"""Failure scenarios that break something at runtime and expect the operator to heal it."""

from __future__ import annotations

import calendar
import time

from ..kube import pod_ready, wait_until
from .base import Context, Expectations, Scenario


def _pick(ctx: Context, shard: int = 0, replica: int = 0) -> dict:
    pods = ctx.op.server_pods(ctx.spec, shard=shard, replica=replica)
    if not pods:
        raise RuntimeError(f"no server pod for shard {shard} replica {replica}")
    return pods[0]


def rolled(ctx: Context) -> bool:
    """Every server and Keeper pod runs the template carrying the latest roll annotation."""
    want = ctx.spec.pod_annotations.get("chaosmonkey/roll")
    pods = ctx.op.server_pods(ctx.spec) + ctx.op.keeper_pods(ctx.spec)
    return all(p["metadata"].get("annotations", {}).get("chaosmonkey/roll") == want for p in pods)


def recreated_since_inject(ctx: Context) -> bool:
    """Every server StatefulSet was created after the injection."""
    for pod in ctx.op.server_pods(ctx.spec):
        sts = _owner_statefulset(pod)
        obj = ctx.kube.get("statefulset", sts, ctx.spec.namespace) if sts else None
        if not obj:
            return False
        created = calendar.timegm(time.strptime(obj["metadata"]["creationTimestamp"], "%Y-%m-%dT%H:%M:%SZ"))
        if created < ctx.t_inject - 1:
            return False
    return True


def container_restarts(pod: dict) -> int:
    return sum(c.get("restartCount", 0) for c in (pod.get("status") or {}).get("containerStatuses") or [])


def _owner_statefulset(pod: dict) -> str | None:
    for ref in pod["metadata"].get("ownerReferences", []):
        if ref.get("kind") == "StatefulSet":
            return ref["name"]
    return None


# ---------------------------------------------------------------- pods


class ServerPodKill(Scenario):
    def __init__(self):
        super().__init__(
            id="server-pod-kill", category="pods", min_replicas=2,
            title="Kill one ClickHouse pod",
            description="Force-delete one replica's pod. The StatefulSet brings it back; queries keep working on the other replica.",
            expect=Expectations(recover_slo_s=90))

    def inject(self, ctx: Context) -> None:
        ctx.notes["victim"] = ctx.kill_pods([_pick(ctx)])


class ShardReplicasKill(Scenario):
    def __init__(self):
        super().__init__(
            id="shard-all-replicas-kill", category="pods", min_replicas=2,
            title="Kill every replica of one shard",
            description="The shard has no serving replica until the pods restart. Data must survive.",
            expect=Expectations(recover_slo_s=120, max_read_outage_s=None, max_write_outage_s=None,
                                min_read_availability_pct=None, min_write_availability_pct=None))

    def inject(self, ctx: Context) -> None:
        ctx.kill_pods(ctx.op.server_pods(ctx.spec, shard=0))


class AllServersKill(Scenario):
    def __init__(self):
        super().__init__(
            id="all-servers-kill", category="pods",
            title="Kill every ClickHouse pod",
            description="Full ClickHouse outage with Keeper intact.",
            expect=Expectations(recover_slo_s=180, max_read_outage_s=None, max_write_outage_s=None,
                                min_read_availability_pct=None, min_write_availability_pct=None))

    def inject(self, ctx: Context) -> None:
        ctx.kill_pods(ctx.op.server_pods(ctx.spec))


class ServerProcessCrash(Scenario):
    def __init__(self):
        super().__init__(
            id="server-process-crash", category="pods", min_replicas=2,
            title="Crash the clickhouse-server process in place",
            description="SIGKILL the server process inside its container; kubelet restarts the container without the pod being replaced.",
            expect=Expectations(recover_slo_s=60))

    def inject(self, ctx: Context) -> None:
        pod = _pick(ctx)
        name = pod["metadata"]["name"]
        ctx.notes["victim"], ctx.notes["restarts_before"] = name, container_restarts(pod)
        if not ctx.cluster.signal_container(pod, ctx.op.server_container, "KILL"):
            raise RuntimeError(f"could not signal the server process of {name}")

    def verify(self, ctx: Context) -> None:
        # the fault happened only if the container actually restarted
        pod = ctx.kube.get("pod", ctx.notes["victim"], ctx.spec.namespace) or {}
        restarts = container_restarts(pod) - ctx.notes["restarts_before"]
        ctx.result.measure("victim_container_restarts", restarts, "count")
        if restarts <= 0:
            ctx.result.add("fail", "fault not applied", "the server container never restarted")


class BadConfigRollout(Scenario):
    """A change that makes the server fail to start. A careful operator stops after the first
    host instead of rolling the broken config across every replica of a shard."""

    def __init__(self):
        super().__init__(
            id="bad-config-rollout", category="spec", min_replicas=2,
            title="Roll out a config that crashes the server",
            description="Apply a server setting ClickHouse rejects at startup, watch the blast radius, then revert.",
            expect=Expectations(recover_within_s=600, recover_slo_s=300,
                                max_read_outage_s=None, max_write_outage_s=None,
                                min_read_availability_pct=None, min_write_availability_pct=None))
        self.observe_s = 180

    def inject(self, ctx: Context) -> None:
        self.good = ctx.spec
        bad = ctx.spec.copy(server_settings={**ctx.spec.server_settings, "max_concurrent_queries": "not-a-number"})
        ctx.reapply(bad)
        ctx.observe(self.observe_s)
        ctx.notes["bad_phase_samples"] = list(ctx.samples)

    def recover(self, ctx: Context) -> None:
        ctx.reapply(self.good)
        ctx.mark_cleared()
        ctx.await_recovery()

    def verify(self, ctx: Context) -> None:
        bad = ctx.notes.get("bad_phase_samples", [])
        if not bad:
            return
        worst_shard = min(min(s["per_shard_ready"]) for s in bad)
        worst_total = min(s["servers_ready"] for s in bad)
        ctx.result.measure("bad_phase_min_ready_hosts", worst_total, "hosts")
        ctx.result.measure("bad_phase_min_ready_per_shard", worst_shard, "replicas")
        if worst_shard == 0:
            ctx.result.add("fail", "blast radius", "the broken config took every replica of a shard down")
        elif ctx.spec.hosts - worst_total > 1:
            ctx.result.add("warn", "blast radius",
                           f"{ctx.spec.hosts - worst_total} hosts down at once while the config was broken")


# ---------------------------------------------------------------- keeper


class KeeperMemberKill(Scenario):
    def __init__(self):
        super().__init__(
            id="keeper-member-kill", category="keeper",
            title="Kill one Keeper member",
            description="Quorum holds with 2 of 3; writes pause only while sessions move.",
            expect=Expectations(recover_slo_s=90, max_write_outage_s=20))

    def inject(self, ctx: Context) -> None:
        ctx.kill_pods(ctx.op.keeper_pods(ctx.spec)[:1])


class KeeperQuorumLoss(Scenario):
    def __init__(self):
        super().__init__(
            id="keeper-quorum-loss", category="keeper",
            title="Kill a Keeper majority",
            description="Kill 2 of 3 members. Replicated tables go read-only until quorum returns; nothing may be lost.",
            expect=Expectations(recover_slo_s=150, keeper_quorum_must_hold=False,
                                max_write_outage_s=None, min_write_availability_pct=None))

    def inject(self, ctx: Context) -> None:
        ctx.kill_pods(ctx.op.keeper_pods(ctx.spec)[:2])


class KeeperAllKill(Scenario):
    def __init__(self):
        super().__init__(
            id="keeper-all-kill", category="keeper",
            title="Kill every Keeper member",
            description="Full coordination outage; the ensemble must re-form from its persisted state.",
            expect=Expectations(recover_slo_s=180, keeper_quorum_must_hold=False,
                                max_write_outage_s=None, min_write_availability_pct=None))

    def inject(self, ctx: Context) -> None:
        ctx.kill_pods(ctx.op.keeper_pods(ctx.spec))


# ---------------------------------------------------------------- operator


class OperatorKillIdle(Scenario):
    def __init__(self):
        super().__init__(
            id="operator-kill-idle", category="operator",
            title="Kill the operator on a healthy cluster",
            description="A restarted operator must not touch a healthy cluster: no pod restarts, status stays healthy.",
            expect=Expectations(recover_slo_s=60))

    def inject(self, ctx: Context) -> None:
        ctx.notes["before"] = self._restarts(ctx)
        ctx.op.kill_operator()
        ctx.op.wait_operator_ready()
        time.sleep(90)
        ctx.mark_cleared()

    def verify(self, ctx: Context) -> None:
        before, after = ctx.notes["before"], self._restarts(ctx)
        touched = [n for n in before if after.get(n) != before[n]]
        if touched:
            ctx.result.add("warn", "idle restart", f"pods replaced or restarted after an operator restart: {', '.join(touched)}")

    @staticmethod
    def _restarts(ctx: Context) -> dict[str, str]:
        out = {}
        for p in ctx.op.server_pods(ctx.spec) + ctx.op.keeper_pods(ctx.spec):
            cs = (p["status"].get("containerStatuses") or [{}])[0]
            out[p["metadata"]["name"]] = f"{p['metadata']['uid']}:{cs.get('restartCount', 0)}"
        return out


class OperatorKillMidRollout(Scenario):
    def __init__(self):
        super().__init__(
            id="operator-kill-mid-rollout", category="operator", min_replicas=2,
            title="Kill the operator in the middle of a rolling change",
            description="The new operator pod must finish the roll and report the cluster healthy.",
            expect=Expectations(recover_within_s=600, recover_slo_s=360))

    def inject(self, ctx: Context) -> None:
        marker = ctx.op.reconcile_marker(ctx.spec)
        ctx.reapply(ctx.spec.copy(pod_annotations={**ctx.spec.pod_annotations, "chaosmonkey/roll": str(int(time.time()))}))
        wait_until(lambda: ctx.op.reconcile_marker(ctx.spec) != marker, timeout=120)
        # wait until at least one pod has been replaced, so the roll is genuinely in flight
        wait_until(lambda: ctx.op.ready_servers(ctx.spec) < ctx.spec.hosts, timeout=180, interval=1)
        ctx.op.kill_operator(force=True)

    def done(self, ctx: Context) -> bool:
        return rolled(ctx)

    def verify(self, ctx: Context) -> None:
        want = ctx.spec.pod_annotations["chaosmonkey/roll"]
        stale = [p["metadata"]["name"] for p in ctx.op.server_pods(ctx.spec)
                 if p["metadata"].get("annotations", {}).get("chaosmonkey/roll") != want]
        if stale:
            ctx.result.add("fail", "rollout completion", f"pods still on the old template: {', '.join(stale)}")


class OperatorDownDuringFailure(Scenario):
    def __init__(self):
        super().__init__(
            id="operator-down-during-failure", category="operator", min_replicas=2,
            title="Lose a StatefulSet while the operator is down",
            description="Scale the operator to zero, delete a server StatefulSet, bring the operator back.",
            expect=Expectations(recover_slo_s=180))

    def inject(self, ctx: Context) -> None:
        deploy = ctx.op.operator_deployment()
        ctx.kube.run("-n", ctx.op.operator_namespace, "scale", f"deployment/{deploy}", "--replicas=0")
        wait_until(lambda: not ctx.op.operator_pods(), timeout=120)
        sts = _owner_statefulset(_pick(ctx))
        ctx.kube.delete("statefulset", sts, namespace=ctx.spec.namespace)
        time.sleep(30)
        ctx.kube.run("-n", ctx.op.operator_namespace, "scale", f"deployment/{deploy}", "--replicas=1")
        ctx.mark_cleared()


class OperatorKillMidDriftRepair(Scenario):
    """The shape behind a status stuck at "in progress": a repair that needs no spec change is
    interrupted after it fixed the drift but before it recorded completion."""

    def __init__(self):
        super().__init__(
            id="operator-kill-mid-keeper-repair", category="operator",
            related_issues=["Altinity/clickhouse-operator#2035"],
            title="Kill the operator while it repairs a deleted Keeper StatefulSet",
            description="Delete one Keeper member's StatefulSet, kill the operator as soon as it's recreated.",
            expect=Expectations(recover_slo_s=120))

    def inject(self, ctx: Context) -> None:
        pod = ctx.op.keeper_pods(ctx.spec)[-1]
        sts = _owner_statefulset(pod)
        old = (ctx.kube.get("statefulset", sts, ctx.spec.namespace) or {}).get("metadata", {}).get("uid")
        ctx.kube.delete("statefulset", sts, namespace=ctx.spec.namespace)

        def replaced() -> bool:
            obj = ctx.kube.get("statefulset", sts, ctx.spec.namespace)
            return bool(obj) and obj["metadata"]["uid"] != old
        wait_until(replaced, timeout=180, interval=1)
        ctx.op.kill_operator(force=True)


# ---------------------------------------------------------------- drift


class ServerStatefulSetDeleted(Scenario):
    def __init__(self):
        super().__init__(
            id="server-statefulset-deleted", category="drift", min_replicas=2,
            title="Delete a ClickHouse StatefulSet",
            description="The operator must notice the missing StatefulSet and recreate it.",
            expect=Expectations(recover_slo_s=120))

    def inject(self, ctx: Context) -> None:
        ctx.kube.delete("statefulset", _owner_statefulset(_pick(ctx)), namespace=ctx.spec.namespace)


class ServiceDeleted(Scenario):
    def __init__(self):
        super().__init__(
            id="query-service-deleted", category="drift",
            title="Delete the cluster's client Service",
            description="Clients lose their entry point until the operator recreates the Service.",
            expect=Expectations(recover_slo_s=120, max_read_outage_s=None, max_write_outage_s=None,
                                min_read_availability_pct=None, min_write_availability_pct=None))

    def inject(self, ctx: Context) -> None:
        ctx.kube.delete("service", ctx.op.query_service(ctx.spec), namespace=ctx.spec.namespace)

    def recover(self, ctx: Context) -> None:
        svc = ctx.op.query_service(ctx.spec)
        t = wait_until(lambda: ctx.kube.get("service", svc, ctx.spec.namespace) is not None,
                       timeout=ctx.result_expect().recover_within_s)
        ctx.result.measure("service_recreated_after_s", t, "s")
        if t is None:
            ctx.result.add("fail", "drift repair", f"Service {svc} was not recreated")
        ctx.await_recovery(timeout=60)


class ConfigMapsDeletedThenRestart(Scenario):
    def __init__(self):
        super().__init__(
            id="configmaps-deleted-then-restart", category="drift", min_replicas=2,
            title="Delete the operator's ConfigMaps, then restart a pod",
            description="A pod restarted after its config was deleted can only start if the operator restores it.",
            expect=Expectations(recover_slo_s=180))

    def inject(self, ctx: Context) -> None:
        cms = [c["metadata"]["name"] for c in ctx.kube.items("configmaps", ctx.spec.namespace)
               if c["metadata"].get("ownerReferences") or c["metadata"]["name"].startswith(("chi-", "chk-"))]
        for name in cms:
            ctx.kube.delete("configmap", name, namespace=ctx.spec.namespace)
        ctx.notes["deleted_configmaps"] = cms
        time.sleep(5)
        ctx.kill_pods([_pick(ctx)])


class PvcDeleted(Scenario):
    """A replica that loses its volume comes back empty. Replication restores data only if the
    table definition is recreated on the new replica, which is the operator's job."""

    def __init__(self):
        super().__init__(
            id="replica-volume-lost", category="storage", min_replicas=2,
            title="Lose one replica's volume",
            description="Delete a replica's PVC and pod. The replica must come back with its schema and data.",
            expect=Expectations(recover_slo_s=240))

    def inject(self, ctx: Context) -> None:
        pod = _pick(ctx, shard=0, replica=1)
        claims = [v["persistentVolumeClaim"]["claimName"] for v in pod["spec"].get("volumes", [])
                  if v.get("persistentVolumeClaim")]
        for claim in claims:
            ctx.kube.delete("pvc", claim, namespace=ctx.spec.namespace)
        ctx.kill_pods([pod])
        ctx.notes["victim"] = pod["metadata"]["name"]
