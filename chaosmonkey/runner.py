"""Runs scenarios against one operator build and applies the generic invariants to each."""

from __future__ import annotations

import json
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from collections import Counter
from pathlib import Path

from .cluster import K3dCluster
from .kube import now_rfc3339, pod_ready, wait_until
from .model import ClusterSpec, ScenarioResult
from .operators.base import OperatorAdapter
from .scenarios.base import Context, Scenario
from .workload import Workload

BASELINE_TIMEOUT_S = 900


_print_lock = threading.Lock()


def _log(msg: str) -> None:
    name = threading.current_thread().name
    tag = "" if name == "MainThread" else f"[{name}] "
    with _print_lock:
        print(f"[{time.strftime('%H:%M:%S')}] {tag}{msg}", flush=True)


def drop_namespace(op: OperatorAdapter, ns: str) -> None:
    kube = op.kube
    if not kube.get("namespace", ns):
        return
    for kind in op.cr_kinds:
        kube.run("delete", kind, "--all", "-n", ns, "--wait=false", check=False)
    kube.run("delete", "pods", "--all", "-n", ns, "--grace-period=0", "--force", check=False)
    kube.delete("namespace", ns)
    start = time.time()
    while kube.get("namespace", ns):
        if time.time() - start > 150:
            # a custom resource whose finalizer can no longer be processed would hold the namespace
            for kind in op.cr_kinds:
                for obj in kube.items(kind, ns):
                    kube.patch(kind, obj["metadata"]["name"], ns, {"metadata": {"finalizers": None}})
        kube.run("delete", "pods", "--all", "-n", ns, "--grace-period=0", "--force", check=False)
        time.sleep(3)


def restore_infrastructure(op: OperatorAdapter, cluster: K3dCluster) -> None:
    """Undo anything a scenario could leave behind at cluster level, whatever happened."""
    for node in op.kube.items("nodes"):
        name = node["metadata"]["name"]
        if node["spec"].get("unschedulable"):
            op.kube.run("uncordon", name, check=False)
    for c in [f"k3d-{cluster.name}-agent-{i}" for i in range(cluster.agents)]:
        cluster.start_node(c)
    wait_until(lambda: len(cluster.ready_nodes()) == cluster.agents + 1, timeout=180)
    if not op.operator_pods():
        try:
            op.kube.run("-n", op.operator_namespace, "scale", f"deployment/{op.default_deployment}", "--replicas=1")
        except Exception:  # noqa: BLE001 - best effort, reinstall covers the rest
            op.install()
    op.wait_operator_ready()


def baseline_healthy(op: OperatorAdapter, spec: ClusterSpec) -> bool:
    return (op.ready_servers(spec) == spec.hosts and op.ready_keepers(spec) == spec.keepers
            and op.state(spec).reconciled)


def run_scenario(op: OperatorAdapter, cluster: K3dCluster, scenario: Scenario, base: ClusterSpec,
                 out_dir: Path) -> ScenarioResult:
    result = ScenarioResult(scenario.id, scenario.category, op.name, op.version,
                            related_issues=list(scenario.related_issues))
    spec = scenario.spec_for(base).copy(namespace=f"cm-{scenario.id}"[:63])
    started = time.time()
    started_rfc = now_rfc3339()
    ctx: Context | None = None
    missing = scenario.requires - op.capabilities
    if missing:
        result.verdict = "SKIPPED"
        result.summary = f"{op.name} lacks what the trigger needs: {', '.join(sorted(missing))}"
        return result
    try:
        cluster.import_images(sorted({spec.server_image, spec.keeper_image, *getattr(scenario, "images", [])}))
        drop_namespace(op, spec.namespace)
        op.kube.run("create", "namespace", spec.namespace)

        _log(f"  baseline {spec.shards}x{spec.replicas} + {spec.keepers} keepers")
        t0 = time.time()
        op.apply(spec)
        created = wait_until(lambda: baseline_healthy(op, spec), BASELINE_TIMEOUT_S, interval=3)
        result.measure("baseline_create_s", created, "s", f"{spec.shards}x{spec.replicas}+{spec.keepers}")
        if created is None:
            result.verdict, result.summary = "ERROR", f"baseline never became healthy ({op.state(spec).phase})"
            return result

        workload = Workload(op, spec)
        for f in workload.setup() + workload.write(1000):
            result.findings.append(f)
        if any(f.severity == "fail" for f in result.findings):
            result.verdict, result.summary = "ERROR", "workload setup failed on a healthy baseline"
            return result
        workload.start_probes()
        time.sleep(10)

        ctx = Context(op, cluster, spec, workload, result)
        ctx.notes["expect"] = scenario.expect
        ctx.scenario = scenario
        ctx.mark_inject()
        audit_since = ctx.t_inject_rfc
        _log(f"  inject: {scenario.title}")
        scenario.inject(ctx)
        _log("  waiting for recovery")
        scenario.recover(ctx)
        t_end = time.time()
        spec = ctx.spec
        exp = scenario.expect

        # recovery
        if not exp.cluster_survives:
            pass
        elif ctx.recovered_at is None:
            result.add("fail", "recovery", f"cluster not healthy {int(t_end - ctx.t_inject)}s after the failure "
                                           f"({op.ready_servers(spec)}/{spec.hosts} servers, "
                                           f"{op.ready_keepers(spec)}/{spec.keepers} keepers Ready)")
        else:
            result.measure("time_to_recover_s", ctx.recovered_at, "s")
            if ctx.recovered_at > exp.recover_slo_s:
                result.add("warn", "recovery time", f"{ctx.recovered_at:.0f}s, target {exp.recover_slo_s}s")
        if exp.status_must_converge and exp.cluster_survives and ctx.recovered_at is not None:
            if ctx.status_converged_at is None:
                result.add("warn", "status truthfulness",
                           f"every pod Ready but the operator still reports {op.state(spec).phase}")
            else:
                result.measure("time_to_status_healthy_s", ctx.status_converged_at, "s")
        lying = [s for s in ctx.samples if s["reconciled"] and not ctx.healthy(s)]
        if lying:
            seconds = 3 * len(lying)
            result.measure("status_healthy_while_pods_down_s", seconds, "s",
                           "status reported healthy while pods were not Ready")
            # A brief overlap is normal while the operator is still observing; a sustained one means
            # the reported status cannot be trusted to decide whether a cluster is whole.
            if seconds > 60:
                worst = min(s["servers_ready"] for s in lying)
                result.add("warn", "status truthfulness",
                           f"reported reconciled for {seconds}s while only {worst}/{spec.hosts} servers were Ready")

        # data and writes after recovery
        if ctx.recovered_at is not None and exp.cluster_survives:
            for f in workload.write(200):
                f.check = "writes after recovery"
                result.findings.append(f)
            data = workload.verify()
            if not exp.data_must_survive:
                for f in data:
                    f.severity = "warn" if f.severity == "fail" else f.severity
            result.findings.extend(data)

        # availability during the scenario. The probes sample once a second, so a short scenario is
        # given a minimum window before the ratios are computed.
        MIN_PROBE_WINDOW_S = 60
        remaining = MIN_PROBE_WINDOW_S - (time.time() - ctx.t_inject)
        if remaining > 0:
            time.sleep(remaining)
            t_end = time.time()
        avail = workload.query_availability(ctx.t_inject, t_end)
        result.measure("read_availability_pct", avail["read_pct"], "%")
        result.measure("write_availability_pct", avail["write_pct"], "%")
        result.measure("longest_read_outage_s", avail["read_longest_outage_s"], "s")
        result.measure("longest_write_outage_s", avail["write_longest_outage_s"], "s")
        result.measure("probe_samples", avail["samples"], "count")
        for kind, gap_limit, pct_limit in (("read", exp.max_read_outage_s, exp.min_read_availability_pct),
                                           ("write", exp.max_write_outage_s, exp.min_write_availability_pct)):
            gap = avail[f"{kind}_longest_outage_s"]
            pct = avail[f"{kind}_pct"]
            if gap_limit is not None and gap > gap_limit:
                result.add("warn", "availability", f"longest {kind} outage {gap}s, limit {gap_limit}s")
            elif pct_limit is not None and pct is not None and avail["samples"] >= exp.min_samples_for_pct \
                    and pct < pct_limit:
                result.add("warn", "availability", f"{kind} availability {pct:.1f}% below {pct_limit}% "
                                                   f"over {avail['samples']} samples")
        quorum = workload.keeper_quorum(ctx.t_inject, t_end)
        result.measure("keeper_min_serving", quorum["min_serving"], "members")
        result.measure("keeper_longest_below_quorum_s", quorum["longest_below_quorum_s"], "s")
        if exp.keeper_quorum_must_hold and quorum["longest_below_quorum_s"] > 10:
            result.add("fail", "keeper quorum", f"below quorum for {quorum['longest_below_quorum_s']}s")

        # operator-specific checks: a reconcile the scenario triggered may still be finishing, so
        # only findings still present after a second look count
        first = op.verify_operator_state(spec)
        if first:
            time.sleep(30)
            again = {(f.check, f.detail) for f in op.verify_operator_state(spec)}
            result.findings.extend(f for f in first if (f.check, f.detail) in again)
        scenario.verify(ctx)

        # API cost of handling the scenario
        # per namespace, so scenarios running side by side don't count each other's requests
        events = [e for e in cluster.audit_events(audit_since, op.operator_username)
                  if (e.get("objectRef") or {}).get("namespace") == spec.namespace]
        verbs = Counter(e.get("verb") for e in events)
        result.measure("api_requests", len(events), "requests")
        result.measure("api_requests_per_host", len(events) / max(spec.hosts, 1), "requests")
        result.measure("api_conflicts_409", sum(1 for e in events if e.get("responseStatus", {}).get("code") == 409), "requests")
        result.measure("api_writes", sum(verbs[v] for v in ("create", "update", "patch", "delete")), "requests")

        result.decide()
        return result
    except Exception as exc:  # noqa: BLE001 - one broken scenario must not stop the run
        result.verdict = "ERROR"
        result.summary = f"{type(exc).__name__}: {exc}"
        result.add("info", "traceback", traceback.format_exc()[-2000:])
        return result
    finally:
        result.duration_s = round(time.time() - started, 1)
        if ctx is not None:
            (out_dir / f"{scenario.id}.samples.json").write_text(json.dumps(ctx.samples, indent=1))
        # Only this scenario's window, and split per custom resource: one controller's reconcile
        # loop can otherwise bury the lines that explain the scenario.
        try:
            log = op.operator_logs(since_time=started_rfc)
            (out_dir / f"{scenario.id}.operator.log").write_text(log[-4_000_000:])
            lines = [ln for ln in log.splitlines() if spec.namespace in ln]
            (out_dir / f"{scenario.id}.operator.namespace.log").write_text("\n".join(lines)[-4_000_000:])
        except Exception:  # noqa: BLE001
            pass
        try:
            (out_dir / f"{scenario.id}.events.txt").write_text(
                op.kube.run("get", "events", "-n", spec.namespace, "--sort-by=.lastTimestamp", check=False))
        except Exception:  # noqa: BLE001
            pass
        if scenario.runs_alone:
            restore_infrastructure(op, cluster)
        drop_namespace(op, spec.namespace)


def run_target(op: OperatorAdapter, cluster: K3dCluster, scenarios: list[Scenario], base: ClusterSpec,
               out_dir: Path, env: dict[str, str] | None = None, concurrency: int = 3) -> list[ScenarioResult]:
    """Namespace-local scenarios run `concurrency` at a time; the ones that touch the operator, a
    node or cluster-wide load run one by one afterwards, with nothing else in flight."""
    out_dir.mkdir(parents=True, exist_ok=True)
    _log(f"cluster {cluster.name}: creating")
    cluster.create()
    _log(f"operator {op.name} {op.version}: installing")
    op.install(env)
    shared = [s for s in scenarios if not s.runs_alone]
    alone = [s for s in scenarios if s.runs_alone]
    results: dict[str, ScenarioResult] = {}
    lock = threading.Lock()

    def one(scenario: Scenario) -> None:
        threading.current_thread().name = f"{op.name} {scenario.id}"
        _log("start")
        r = run_scenario(op, cluster, scenario, base, out_dir)
        _log(f"-> {r.verdict} {r.summary} ({r.duration_s:.0f}s)")
        for f in r.findings:
            if f.severity in ("fail", "warn"):
                _log(f"   {f.severity}: {f.check}: {f.detail}")
        with lock:
            results[scenario.id] = r
            ordered = [results[s.id] for s in scenarios if s.id in results]
            (out_dir / "results.json").write_text(json.dumps([x.to_dict() for x in ordered], indent=1))

    if shared:
        _log(f"{len(shared)} scenarios in parallel ({concurrency} at a time), then {len(alone)} alone")
        with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
            list(pool.map(one, shared))
    for scenario in alone:
        one(scenario)
    threading.current_thread().name = "MainThread"
    return [results[s.id] for s in scenarios if s.id in results]
