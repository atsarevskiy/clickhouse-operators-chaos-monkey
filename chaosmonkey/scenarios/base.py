"""Scenario contract and the context a scenario runs in.

A scenario describes one failure. The runner owns everything around it:

    fresh namespace ─► cluster from spec_for(base) ─► baseline Ready + data + probes
        ─► inject(ctx) ─► ctx.await_recovery() (or the scenario's own wait)
        ─► generic invariants + scenario.verify(ctx) + adapter.verify_operator_state()
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from ..cluster import K3dCluster
from ..kube import Kube, now_rfc3339, pod_ready
from ..model import ClusterSpec, ScenarioResult
from ..operators.base import OperatorAdapter
from ..workload import Workload


@dataclass
class Expectations:
    """What a well-behaved operator should achieve. Missing the target is DEGRADED, not FAIL,
    except where noted."""

    recover_within_s: int = 300          # past this, not recovering at all is FAIL
    recover_slo_s: int = 120             # recovering but slower than this is DEGRADED
    # Longest continuous outage the client may see, in seconds. This is the primary availability
    # criterion: a percentage over a short scenario swings wildly on one failed sample.
    max_read_outage_s: int | None = 10
    max_write_outage_s: int | None = 20
    # Secondary, and only applied once there are enough samples to mean anything.
    min_read_availability_pct: float | None = 95.0
    min_write_availability_pct: float | None = 90.0
    min_samples_for_pct: int = 40
    keeper_quorum_must_hold: bool = True
    data_must_survive: bool = True       # FAIL on any lost row
    status_must_converge: bool = True    # status reports healthy once everything is healthy
    # False for scenarios that end with the cluster deliberately gone: there is nothing left to
    # write to or count, so the data and post-recovery write checks do not apply.
    cluster_survives: bool = True


@dataclass
class Scenario:
    id: str = ""
    category: str = ""
    title: str = ""
    description: str = ""
    related_issues: list[str] = field(default_factory=list)
    expect: Expectations = field(default_factory=Expectations)
    #: minimum topology the scenario needs; the runner scales the base spec up to it
    min_shards: int = 1
    min_replicas: int = 1
    min_keepers: int = 3
    #: scenarios that measure speed rather than resilience
    performance: bool = False
    #: adapter capabilities the trigger needs (see OperatorAdapter.capabilities)
    requires: frozenset = frozenset()
    #: seconds of continuous health before "recovered" counts; raise it for changes an
    #: operator applies host by host without a done() predicate, so a gap between hosts can't pass
    stable_samples: int = 6
    #: give up waiting when nothing changes for this long while unhealthy (see await_recovery)
    stall_s: int = 180
    #: touches something shared by every namespace (the operator, a node, cluster-wide API load),
    #: so it runs alone after the parallel batch
    exclusive: bool = False

    @property
    def runs_alone(self) -> bool:
        return self.exclusive or self.category in ("operator", "infrastructure", "performance")

    def spec_for(self, base: ClusterSpec) -> ClusterSpec:
        return base.copy(shards=max(base.shards, self.min_shards),
                         replicas=max(base.replicas, self.min_replicas),
                         keepers=max(base.keepers, self.min_keepers))

    def inject(self, ctx: "Context") -> None:
        raise NotImplementedError

    def recover(self, ctx: "Context") -> None:
        """Wait for (or help) recovery. The default waits for the operator to heal the cluster."""
        ctx.await_recovery()

    def verify(self, ctx: "Context") -> None:
        """Scenario-specific checks, recorded on ctx.result."""

    def done(self, ctx: "Context") -> bool:
        """Scenario-specific completion on top of "every pod Ready", e.g. a roll that reached every
        pod. Without it a roll looks recovered in the gap between two host restarts."""
        return True


class Context:
    def __init__(self, op: OperatorAdapter, cluster: K3dCluster, spec: ClusterSpec,
                 workload: Workload, result: ScenarioResult):
        self.op = op
        self.cluster = cluster
        self.kube: Kube = cluster.kube
        self.spec = spec
        self.workload = workload
        self.result = result
        self.t_inject: float = 0.0
        self.t_inject_rfc: str = ""
        self.t_cleared: float | None = None
        self.scenario: Scenario | None = None
        self.recovered_at: float | None = None
        self.status_converged_at: float | None = None
        self.status_grace_s = 0
        self.samples: list[dict] = []
        self.notes: dict = {}

    # ---------- markers ----------

    def mark_inject(self) -> None:
        self.t_inject = time.time()
        self.t_inject_rfc = now_rfc3339()

    def mark_cleared(self) -> None:
        """The fault itself is over (partition lifted, node back, bad spec reverted). Recovery is
        timed from here; availability still covers the whole scenario."""
        self.t_cleared = time.time()

    def since_inject(self) -> float:
        return time.time() - self.t_inject

    def since_reference(self) -> float:
        return time.time() - (self.t_cleared or self.t_inject)

    # ---------- observing ----------

    def snapshot(self) -> dict:
        servers = self.op.server_pods(self.spec)
        keepers = self.op.keeper_pods(self.spec)
        state = self.op.state(self.spec)
        snap = {
            "t": round(self.since_inject(), 1),
            "servers_ready": sum(1 for p in servers if pod_ready(p)),
            "keepers_ready": sum(1 for p in keepers if pod_ready(p)),
            "reconciled": state.reconciled,
            "phase": state.phase,
            "per_shard_ready": [sum(1 for p in self.op.server_pods(self.spec, shard=s) if pod_ready(p))
                                for s in range(self.spec.shards)],
        }
        self.samples.append(snap)
        return snap

    def healthy(self, snap: dict) -> bool:
        return snap["servers_ready"] == self.spec.hosts and snap["keepers_ready"] == self.spec.keepers

    def progress_signature(self) -> tuple:
        """Everything that changes while an operator is working: pods (identity, readiness,
        restarts), StatefulSets (existence, replicas, generation) and the reported phase."""
        ns = self.spec.namespace
        pods = tuple(sorted(
            (p["metadata"]["name"], p["metadata"]["uid"], pod_ready(p), p["status"].get("phase"),
             sum(c.get("restartCount", 0) for c in p["status"].get("containerStatuses") or []))
            for p in self.kube.items("pods", ns) if not p["metadata"]["labels"].get("chaosmonkey")))
        sts = tuple(sorted((s["metadata"]["name"], s["spec"].get("replicas"), s["metadata"].get("generation"),
                            s["status"].get("readyReplicas")) for s in self.kube.items("statefulsets", ns)))
        return pods, sts, self.op.state(self.spec).phase

    def observe(self, max_s: int, settle_s: int = 45, min_s: int = 30) -> None:
        """Watch a held fault and record samples, ending once nothing has changed for settle_s:
        the operator has made its decision, so a longer hold only repeats the same samples."""
        start = time.time()
        last_sig, last_change = None, time.time()
        while time.time() - start < max_s:
            self.snapshot()
            sig = self.progress_signature()
            if sig != last_sig:
                last_sig, last_change = sig, time.time()
            elif time.time() - start >= min_s and time.time() - last_change >= settle_s:
                break
            time.sleep(1)

    def await_recovery(self, timeout: int | None = None, status_grace_s: int = 300,
                       stall_s: int | None = None) -> bool:
        """Poll until every pod is Ready and the scenario is done, then give the reported status
        status_grace_s to agree.

        Stops early when the cluster is unhealthy and nothing at all has changed for stall_s: an
        operator that has stopped acting will not recover in the remaining time, and waiting out a
        20-minute limit for it is most of a run's wall-clock.

        Records: time until healthy (from when the fault was cleared, or from injection), time
        until the status says reconciled, and every sample where the status claimed healthy while
        pods were not."""
        timeout = timeout or self.result_expect().recover_within_s
        self.status_grace_s = status_grace_s
        stall_s = stall_s or (self.scenario.stall_s if self.scenario else 180)
        deadline = time.time() + timeout
        streak, streak_start = 0, None
        last_sig, last_change = None, time.time()
        # the status grace runs past the recovery deadline: a cluster that heals late still gets it
        while time.time() < deadline or self.recovered_at is not None:
            snap = self.snapshot()
            ok = self.healthy(snap) and (self.scenario is None or self.scenario.done(self))
            if ok and streak == 0:
                streak_start = time.time()
            streak = streak + 1 if ok else 0
            need = self.scenario.stable_samples if self.scenario else 6
            if self.recovered_at is None and streak >= 1 and time.time() - (streak_start or time.time()) >= need:
                # recovered when health began, not when the stability window closed
                self.recovered_at = streak_start - (self.t_cleared or self.t_inject)
                self.recovered_wall = time.time()
            if self.recovered_at is not None:
                if snap["reconciled"]:
                    self.status_converged_at = self.since_reference()
                    return True
                if time.time() - self.recovered_wall > status_grace_s:
                    return True
            else:
                sig = self.progress_signature()
                if sig != last_sig:
                    last_sig, last_change = sig, time.time()
                elif time.time() - last_change > stall_s:
                    self.notes["stalled_after_s"] = round(time.time() - last_change)
                    return False
            time.sleep(1)
        return self.recovered_at is not None

    def result_expect(self) -> Expectations:
        return self.notes.get("expect")  # set by the runner

    # ---------- common actions ----------

    def kill_pods(self, pods: list[dict], grace: int = 0) -> list[str]:
        names = [p["metadata"]["name"] for p in pods]
        for name in names:
            self.kube.delete("pod", name, namespace=self.spec.namespace, grace=grace, force=grace == 0)
        return names

    def reapply(self, spec: ClusterSpec) -> None:
        self.spec = spec
        self.workload.spec = spec
        self.op.apply(spec)
