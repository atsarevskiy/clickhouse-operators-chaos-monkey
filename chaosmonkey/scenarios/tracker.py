"""Scenarios written from bugs reported on the operators' own issue trackers.

Each one states the issue it came from, and each is written as a property any ClickHouse operator
should hold, not as a reproduction of one operator's internals. `related_issues` records where the
behaviour was first reported; a scenario failing on an operator whose tracker does not list it is
still a real result.
"""

from __future__ import annotations

import time

from ..kube import pod_ready, wait_until
from ..workload import DB
from .base import Context, Expectations, Scenario
from .failures import _owner_statefulset, _pick


# ---------------------------------------------------------------- coordination metadata


class ScaleDownLeavesReplicaMetadata(Scenario):
    """Altinity#1943: the drop of a removed replica runs once, before its Keeper session has
    expired, and is never retried, so the replica stays registered. ClickHouse#349 is the same
    property failing for a different reason."""

    def __init__(self):
        super().__init__(
            id="scale-down-replica-metadata", category="metadata", min_shards=1, min_replicas=3,
            related_issues=["Altinity/clickhouse-operator#1943", "ClickHouse/clickhouse-operator#349"],
            title="Removing a replica cleans up its replication metadata",
            description="Scale a shard down by one replica and check the survivors no longer count the removed "
                        "replica, and that its Keeper registration is gone. A long Keeper session timeout makes "
                        "the removed replica look active while the operator tries to drop it.",
            expect=Expectations(recover_within_s=600, recover_slo_s=300))

    def spec_for(self, base):
        # A session that outlives the scale-down is what makes the drop fail rather than race.
        return super().spec_for(base).copy(
            server_settings={"zookeeper/session_timeout_ms": "120000"})

    def inject(self, ctx: Context) -> None:
        pod = ctx.op.server_pods(ctx.spec, shard=0)[0]["metadata"]["name"]
        ctx.notes["zk_path"] = ctx.workload.table_zk_path(pod)
        ctx.notes["before"] = ctx.workload.total_replicas(pod)
        ctx.reapply(ctx.spec.copy(replicas=ctx.spec.replicas - 1))

    def done(self, ctx: Context) -> bool:
        return len(ctx.op.server_pods(ctx.spec)) == ctx.spec.hosts

    def verify(self, ctx: Context) -> None:
        pods = ctx.op.server_pods(ctx.spec, shard=0)
        if not pods:
            return
        pod = pods[0]["metadata"]["name"]
        want = ctx.spec.replicas
        got = ctx.workload.total_replicas(pod)
        ctx.result.measure("total_replicas_after_scale_down", got, "replicas",
                           f"expected {want}, was {ctx.notes.get('before')} before")
        if got is not None and got > want:
            ctx.result.add("fail", "replica metadata",
                           f"system.replicas still counts {got} replicas, expected {want}")
        path = ctx.notes.get("zk_path")
        if path:
            names = ctx.workload.zk_children(pod, f"{path}/replicas")
            if names is not None:
                ctx.result.measure("keeper_registered_replicas", len(names), "entries")
                if len(names) > want:
                    ctx.result.add("fail", "keeper metadata",
                                   f"{len(names)} replicas still registered under the table path: {', '.join(sorted(names))}")


class ScaleDownShardLeavesMetadata(Scenario):
    """Altinity#1927: removing a shard leaves the shard's table paths in Keeper, which then breaks
    adding the shard back."""

    def __init__(self):
        super().__init__(
            id="scale-down-shard-then-up", category="metadata", min_shards=2, min_replicas=2,
            related_issues=["Altinity/clickhouse-operator#1927"],
            title="A removed shard can be added back cleanly",
            description="Remove a shard, then add it back. The new shard's replicas must create their tables "
                        "without colliding with metadata the removed shard left behind.",
            expect=Expectations(recover_within_s=1200, recover_slo_s=600,
                                max_read_outage_s=None, max_write_outage_s=None,
                                min_read_availability_pct=None, min_write_availability_pct=None,
                                data_must_survive=False))

    def inject(self, ctx: Context) -> None:
        self.full = ctx.spec
        # rows found on a re-added shard were written before it was removed: stale data coming back
        ctx.workload.surplus_label = "stale data resurrected on re-added shard"
        ctx.notes["removed_shard"] = ctx.spec.shards - 1
        ctx.reapply(ctx.spec.copy(shards=ctx.spec.shards - 1))
        wait_until(lambda: len(ctx.op.server_pods(ctx.spec)) == ctx.spec.hosts, timeout=600, interval=1)
        ctx.await_recovery(timeout=300)
        ctx.recovered_at = None
        ctx.status_converged_at = None

    def recover(self, ctx: Context) -> None:
        ctx.reapply(self.full)
        ctx.mark_cleared()
        ctx.workload.expected.pop(ctx.notes["removed_shard"], None)
        ctx.workload.expected[ctx.notes["removed_shard"]] = 0
        ctx.await_recovery()

    def done(self, ctx: Context) -> bool:
        return len(ctx.op.server_pods(ctx.spec)) == ctx.spec.hosts

    def verify(self, ctx: Context) -> None:
        shard = ctx.notes["removed_shard"]
        missing, broken = [], []
        for pod in ctx.op.server_pods(ctx.spec, shard=shard):
            name = pod["metadata"]["name"]
            if ctx.workload.table_count(name) in (None, 0):
                missing.append(name)
            rc, out = ctx.op.sql(ctx.spec, name, f"SELECT is_readonly FROM system.replicas "
                                                 f"WHERE database = '{DB}' AND table = 'events'")
            if rc == 0 and out.strip().startswith("1"):
                broken.append(name)
        if missing:
            ctx.result.add("fail", "shard re-added", f"re-added shard has no tables on: {', '.join(missing)}")
        if broken:
            ctx.result.add("fail", "shard re-added", f"re-added replica is read-only: {', '.join(broken)}")
        for name in [p["metadata"]["name"] for p in ctx.op.server_pods(ctx.spec, shard=shard)]:
            rc, out = ctx.op.sql(ctx.spec, name, f"INSERT INTO {DB}.events (id, shard) VALUES (999000001, {shard})")
            if rc != 0:
                ctx.result.add("fail", "shard re-added", f"cannot write to the re-added shard on {name}: {out.strip()[:200]}")
            break


# ---------------------------------------------------------------- publishing a replica too early


class ReplicaServesBeforeSchema(Scenario):
    """Altinity#1567 and ClickHouse#266: a new replica joins the client Service before the operator
    has created its schema, so clients hit an empty host."""

    def __init__(self):
        super().__init__(
            id="new-replica-published-before-schema", category="lifecycle", min_shards=1, min_replicas=2,
            related_issues=["Altinity/clickhouse-operator#1567", "ClickHouse/clickhouse-operator#266"],
            title="A new replica is not published to clients before its schema exists",
            description="Add a replica and watch, every second, whether its address appears among the client "
                        "Service's ready endpoints before the replica actually holds the tables.",
            expect=Expectations(recover_within_s=900, recover_slo_s=400))

    def inject(self, ctx: Context) -> None:
        before = {p["metadata"]["name"] for p in ctx.op.server_pods(ctx.spec)}
        ctx.reapply(ctx.spec.copy(replicas=ctx.spec.replicas + 1))
        published_first = []
        deadline = time.time() + 600
        while time.time() < deadline:
            ctx.snapshot()
            new = [p for p in ctx.op.server_pods(ctx.spec) if p["metadata"]["name"] not in before]
            done = True
            for pod in new:
                name, ip = pod["metadata"]["name"], pod["status"].get("podIP")
                if not ip:
                    done = False
                    continue
                in_endpoints = ip in ctx.op.ready_endpoint_ips(ctx.spec)
                tables = ctx.workload.table_count(name) if pod_ready(pod) else None
                if in_endpoints and not tables:
                    published_first.append(name)
                if not tables:
                    done = False
            if new and done:
                break
            time.sleep(2)
        ctx.notes["published_before_schema"] = sorted(set(published_first))

    def done(self, ctx: Context) -> bool:
        return len(ctx.op.server_pods(ctx.spec)) == ctx.spec.hosts

    def verify(self, ctx: Context) -> None:
        early = ctx.notes.get("published_before_schema") or []
        ctx.result.measure("replicas_published_before_schema", len(early), "count")
        if early:
            ctx.result.add("warn", "premature publishing",
                           f"reachable through the client Service before holding the tables: {', '.join(early)}")
        for pod in ctx.op.server_pods(ctx.spec):
            name = pod["metadata"]["name"]
            if not ctx.workload.table_count(name):
                ctx.result.add("fail", "schema propagation", f"{name} still has no tables")


class MigrationFailureMarkedDone(Scenario):
    """Altinity#2021: schema creation on a new replica fails, the error is only logged, and the host
    is published anyway."""

    def __init__(self):
        super().__init__(
            id="new-replica-schema-blocked", category="lifecycle", min_shards=1, min_replicas=2,
            related_issues=["Altinity/clickhouse-operator#2021"],
            title="A replica whose schema creation failed is not reported as done",
            description="Add a replica while its traffic to Keeper is blocked, so creating replicated tables "
                        "cannot succeed. The operator must not report the cluster healthy with that host in it. "
                        "Then unblock and let it converge.",
            expect=Expectations(recover_within_s=900, recover_slo_s=450,
                                max_read_outage_s=None, max_write_outage_s=None,
                                min_read_availability_pct=None, min_write_availability_pct=None))
        self.block_s = 180

    def inject(self, ctx: Context) -> None:
        # Deny Keeper egress for every ClickHouse pod of the last replica index, which is the one
        # being added. Selecting by the operator's own replica label keeps this operator-neutral.
        sel = ctx.op.server_replica_label(ctx.spec, ctx.spec.replicas)
        key, value = sel.split("=", 1)
        keeper_sel = ctx.op.keeper_selector(ctx.spec)
        ctx.kube.apply([{
            "apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
            "metadata": {"name": "chaos-block-keeper"},
            "spec": {
                "podSelector": {"matchLabels": {key: value}},
                "policyTypes": ["Egress"],
                "egress": [
                    {"to": [{"podSelector": {"matchExpressions": [
                        {"key": k, "operator": "NotIn", "values": [v]}
                        for k, v in (p.split("=", 1) for p in keeper_sel.split(","))]}}]},
                    {"to": [{"namespaceSelector": {"matchExpressions": [
                        {"key": "kubernetes.io/metadata.name", "operator": "NotIn", "values": [ctx.spec.namespace]}]}}]},
                ],
            },
        }], namespace=ctx.spec.namespace)
        ctx.reapply(ctx.spec.copy(replicas=ctx.spec.replicas + 1))
        end = time.time() + self.block_s
        claimed = []
        while time.time() < end:
            snap = ctx.snapshot()
            if snap["reconciled"]:
                for pod in ctx.op.server_pods(ctx.spec, replica=ctx.spec.replicas - 1):
                    if not ctx.workload.table_count(pod["metadata"]["name"]):
                        claimed.append(pod["metadata"]["name"])
            time.sleep(1)
        ctx.notes["claimed_done_without_schema"] = sorted(set(claimed))

    def recover(self, ctx: Context) -> None:
        ctx.kube.delete("networkpolicy", "chaos-block-keeper", namespace=ctx.spec.namespace)
        ctx.mark_cleared()
        ctx.await_recovery()

    def verify(self, ctx: Context) -> None:
        claimed = ctx.notes.get("claimed_done_without_schema") or []
        if claimed:
            ctx.result.add("fail", "reported done without schema",
                           f"cluster reported healthy while these hosts had no tables: {', '.join(claimed)}")
        for pod in ctx.op.server_pods(ctx.spec):
            name = pod["metadata"]["name"]
            if not ctx.workload.table_count(name):
                ctx.result.add("fail", "schema propagation", f"{name} has no tables after Keeper was reachable again")


# ---------------------------------------------------------------- drift and deletion


class KeeperStatefulSetDeleted(Scenario):
    """Altinity#1597: deleting a Keeper StatefulSet is not noticed."""

    def __init__(self):
        super().__init__(
            id="keeper-statefulset-deleted", category="drift",
            related_issues=["Altinity/clickhouse-operator#1597"],
            title="Delete a Keeper StatefulSet",
            description="The operator must notice the missing StatefulSet and recreate it, without a spec change.",
            expect=Expectations(recover_slo_s=120))

    def inject(self, ctx: Context) -> None:
        sts = _owner_statefulset(ctx.op.keeper_pods(ctx.spec)[-1])
        ctx.notes["sts"] = sts
        ctx.kube.delete("statefulset", sts, namespace=ctx.spec.namespace)

    def verify(self, ctx: Context) -> None:
        if not ctx.kube.get("statefulset", ctx.notes["sts"], ctx.spec.namespace):
            ctx.result.add("fail", "drift repair", f"StatefulSet {ctx.notes['sts']} was never recreated")


class ForegroundDeletion(Scenario):
    """ClickHouse operator PR#309: deleting the cluster with foreground cascade livelocked, because
    the operator kept recreating children while the parent waited for them to go."""

    def __init__(self):
        super().__init__(
            id="cluster-deleted-foreground", category="lifecycle",
            related_issues=["ClickHouse/clickhouse-operator#309"],
            title="Delete the cluster with foreground cascade",
            description="kubectl delete --cascade=foreground must complete: the operator has to stop recreating "
                        "children of an object that is being deleted.",
            expect=Expectations(recover_within_s=420, recover_slo_s=180, status_must_converge=False,
                                max_read_outage_s=None, max_write_outage_s=None,
                                min_read_availability_pct=None, min_write_availability_pct=None,
                                data_must_survive=False, keeper_quorum_must_hold=False,
                                cluster_survives=False))

    def inject(self, ctx: Context) -> None:
        for kind in ctx.op.cr_kinds:
            for obj in ctx.kube.items(kind, ctx.spec.namespace):
                ctx.kube.delete(kind, obj["metadata"]["name"], namespace=ctx.spec.namespace,
                                cascade="foreground", wait=False)

    def recover(self, ctx: Context) -> None:
        def gone() -> bool:
            return all(not ctx.kube.items(kind, ctx.spec.namespace) for kind in ctx.op.cr_kinds)
        t = wait_until(gone, timeout=ctx.result_expect().recover_within_s, interval=1)
        ctx.result.measure("foreground_delete_s", t, "s")
        ctx.recovered_at = t if t is not None else None
        if t is None:
            ctx.result.add("fail", "deletion", "custom resources are still present after a foreground delete")

    def verify(self, ctx: Context) -> None:
        left = [s["metadata"]["name"] for s in ctx.kube.items("statefulsets", ctx.spec.namespace)]
        if left:
            ctx.result.add("warn", "orphaned objects", f"StatefulSets left behind: {', '.join(left)}")


class DeletionWithoutOperator(Scenario):
    """Altinity#1775: with the operator gone, a cluster with a finalizer cannot be deleted, and it
    must finish once the operator comes back."""

    def __init__(self):
        super().__init__(
            id="cluster-deleted-while-operator-down", category="lifecycle", exclusive=True,
            related_issues=["Altinity/clickhouse-operator#1775"],
            title="Delete the cluster while the operator is down, then bring it back",
            description="A delete requested with no operator running must complete once the operator returns, "
                        "and must not leave orphaned StatefulSets, Services or PVCs behind.",
            expect=Expectations(recover_within_s=600, recover_slo_s=240, status_must_converge=False,
                                max_read_outage_s=None, max_write_outage_s=None,
                                min_read_availability_pct=None, min_write_availability_pct=None,
                                data_must_survive=False, keeper_quorum_must_hold=False,
                                cluster_survives=False))

    def inject(self, ctx: Context) -> None:
        deploy = ctx.op.operator_deployment()
        ctx.kube.run("-n", ctx.op.operator_namespace, "scale", f"deployment/{deploy}", "--replicas=0")
        wait_until(lambda: not ctx.op.operator_pods(), timeout=120)
        ctx.op.delete_cluster(ctx.spec)
        time.sleep(30)
        still_there = all(ctx.kube.items(kind, ctx.spec.namespace) for kind in ctx.op.cr_kinds)
        ctx.notes["blocked_while_down"] = still_there
        ctx.kube.run("-n", ctx.op.operator_namespace, "scale", f"deployment/{deploy}", "--replicas=1")
        ctx.op.wait_operator_ready()
        ctx.mark_cleared()

    def recover(self, ctx: Context) -> None:
        def gone() -> bool:
            return all(not ctx.kube.items(kind, ctx.spec.namespace) for kind in ctx.op.cr_kinds)
        t = wait_until(gone, timeout=ctx.result_expect().recover_within_s, interval=1)
        ctx.result.measure("delete_completed_after_operator_return_s", t, "s")
        ctx.recovered_at = t if t is not None else None
        if t is None:
            ctx.result.add("fail", "deletion", "the delete never completed after the operator came back")

    def verify(self, ctx: Context) -> None:
        for kind in ("statefulsets", "services", "configmaps", "pvc"):
            left = [o["metadata"]["name"] for o in ctx.kube.items(kind, ctx.spec.namespace)
                    if not o["metadata"]["name"].startswith("kube-root-ca")]
            if left:
                ctx.result.add("warn", "orphaned objects", f"{kind} left behind: {', '.join(sorted(left)[:6])}")


# ---------------------------------------------------------------- restart safety


class KeeperRollingRestart(Scenario):
    """ClickHouse operator#346: a rolling Keeper change restarts the leader without handing over
    leadership first, so the ensemble has no leader for seconds."""

    def __init__(self):
        super().__init__(
            id="keeper-rolling-restart", category="keeper", min_shards=1, min_replicas=2,
            related_issues=["ClickHouse/clickhouse-operator#346"],
            title="Rolling restart of every Keeper member",
            description="Change the Keeper pod template and measure how long the ensemble runs with no leader "
                        "and how long writes are refused.",
            expect=Expectations(recover_within_s=900, recover_slo_s=420, max_write_outage_s=30))
        self.max_leaderless_s = 10

    def inject(self, ctx: Context) -> None:
        marker = ctx.op.reconcile_marker(ctx.spec)
        ctx.reapply(ctx.spec.copy(keeper_settings={**ctx.spec.keeper_settings,
                                                   "keeper_server/coordination_settings/raft_logs_level": "information"}))
        wait_until(lambda: ctx.op.reconcile_marker(ctx.spec) != marker, timeout=180)
        wait_until(lambda: ctx.op.ready_keepers(ctx.spec) < ctx.spec.keepers, timeout=240, interval=1)

    def verify(self, ctx: Context) -> None:
        stats = ctx.workload.keeper_leaderless(ctx.t_inject)
        ctx.result.measure("keeper_longest_leaderless_s", stats["longest_leaderless_s"], "s",
                           f"over {stats['samples']} samples")
        if stats["longest_leaderless_s"] > self.max_leaderless_s:
            ctx.result.add("warn", "keeper leadership",
                           f"no leader for {stats['longest_leaderless_s']}s during the rolling restart, "
                           f"limit {self.max_leaderless_s}s")


class RestartDuringSlowStart(Scenario):
    """Altinity#2053: a restart-requiring change reaches a host whose server has not finished
    starting, and the operator kills it mid-startup, repeatedly."""

    def __init__(self):
        super().__init__(
            id="config-change-during-slow-start", category="spec", min_shards=1, min_replicas=2,
            related_issues=["Altinity/clickhouse-operator#2053"],
            title="Change config while a host is still starting up",
            description="One replica is restarted with a slow start, and a config change arrives while it is "
                        "still coming up. The operator must not keep killing it, and the cluster must converge.",
            expect=Expectations(recover_within_s=1200, recover_slo_s=600,
                                max_read_outage_s=None, max_write_outage_s=None,
                                min_read_availability_pct=None, min_write_availability_pct=None))

    def spec_for(self, base):
        return super().spec_for(base).copy(init_sleep=150)

    def inject(self, ctx: Context) -> None:
        victim = _pick(ctx, shard=0, replica=0)
        ctx.notes["victim_uids"] = {victim["metadata"]["name"]: victim["metadata"]["uid"]}
        ctx.kill_pods([victim])
        time.sleep(20)
        ctx.reapply(ctx.spec.copy(server_settings={**ctx.spec.server_settings,
                                                   "max_concurrent_queries": "77"}))

    def verify(self, ctx: Context) -> None:
        uids = set()
        for snap_pod in ctx.op.server_pods(ctx.spec, shard=0, replica=0):
            uids.add(snap_pod["metadata"]["uid"])
        seen = ctx.notes.get("pod_generations", 0)
        ctx.result.measure("victim_pod_generations", max(seen, len(uids)), "count")
        if not ctx.op.server_pods(ctx.spec, shard=0, replica=0):
            ctx.result.add("fail", "host lost", "the restarted replica has no pod at all")

    def done(self, ctx: Context) -> bool:
        pods = ctx.op.server_pods(ctx.spec, shard=0, replica=0)
        return bool(pods) and all(pod_ready(p) for p in pods)


# ---------------------------------------------------------------- operator-neutral versions
# The scenarios above that need a preStop hook or a volume-template recreate are skipped on
# operators without those features. These three test the same behaviour through triggers every
# operator supports, so the comparison has no gaps.


def _blast_radius(ctx: Context, samples: list[dict], label: str) -> None:
    worst_shard = min((min(s["per_shard_ready"]) for s in samples), default=ctx.spec.replicas)
    worst_keeper = min((s["keepers_ready"] for s in samples), default=ctx.spec.keepers)
    ctx.result.measure("fault_min_ready_per_shard", worst_shard, "replicas")
    ctx.result.measure("fault_min_ready_keepers", worst_keeper, "members")
    if worst_shard == 0:
        ctx.result.add("fail", "blast radius", f"a shard had no Ready replica while {label}")
    if worst_keeper < ctx.spec.keepers // 2 + 1:
        ctx.result.add("fail", "blast radius", f"Keeper fell to {worst_keeper} Ready members while {label}")


class StuckTerminatingPod(Scenario):
    """Operator-neutral version of wedged-shutdown-recreate: a pod that cannot finish terminating
    during a rolling change. A pod finalizer holds it, which no operator can override."""

    def __init__(self):
        super().__init__(
            id="stuck-terminating-pod", category="spec", min_replicas=2,
            related_issues=["Altinity/clickhouse-operator#2078"],
            title="A pod stuck terminating during a rolling change",
            description="Hold one replica's pod in Terminating with a finalizer, then roll the pod template. "
                        "The operator must not take down the stuck pod's peer, and must finish once it's released.",
            expect=Expectations(recover_within_s=900, recover_slo_s=420,
                                max_read_outage_s=None, min_read_availability_pct=None,
                                max_write_outage_s=None, min_write_availability_pct=None))
        self.hold_s = 240

    def inject(self, ctx: Context) -> None:
        pod = _pick(ctx, shard=0, replica=0)["metadata"]["name"]
        ctx.notes["held"] = pod
        ctx.kube.patch("pod", pod, ctx.spec.namespace, {"metadata": {"finalizers": ["chaosmonkey/hold"]}})
        ctx.reapply(ctx.spec.copy(pod_annotations={**ctx.spec.pod_annotations, "chaosmonkey/roll": str(int(time.time()))}))
        ctx.observe(self.hold_s)
        ctx.notes["fault_samples"] = list(ctx.samples)

    def recover(self, ctx: Context) -> None:
        pod = ctx.notes["held"]
        if ctx.kube.get("pod", pod, ctx.spec.namespace):
            ctx.kube.patch("pod", pod, ctx.spec.namespace, {"metadata": {"finalizers": None}})
        ctx.mark_cleared()
        ctx.await_recovery()

    def done(self, ctx: Context) -> bool:
        from .failures import rolled
        return rolled(ctx)

    def verify(self, ctx: Context) -> None:
        _blast_radius(ctx, ctx.notes.get("fault_samples", []), "a pod was stuck terminating")


class UnschedulableRollout(Scenario):
    """Operator-neutral version of unschedulable-replacement: a pod template change whose pods can
    never be scheduled, because no node carries the label it selects."""

    def __init__(self):
        super().__init__(
            id="unschedulable-rollout", category="spec", min_replicas=2,
            related_issues=["Altinity/clickhouse-operator#2069", "Altinity/clickhouse-operator#2078"],
            title="Roll out pods that can never be scheduled",
            description="Add a nodeSelector no node matches. The operator must stop after the first replacement "
                        "stays Pending instead of taking down a shard or the Keeper quorum, then recover on revert.",
            expect=Expectations(recover_within_s=900, recover_slo_s=420,
                                max_read_outage_s=None, min_read_availability_pct=None,
                                max_write_outage_s=None, min_write_availability_pct=None))
        self.hold_s = 300

    def inject(self, ctx: Context) -> None:
        self.good = ctx.spec
        ctx.reapply(ctx.spec.copy(node_selector={"chaosmonkey/no-such-node": "true"}))
        ctx.observe(self.hold_s)
        ctx.notes["fault_samples"] = list(ctx.samples)

    def recover(self, ctx: Context) -> None:
        ctx.reapply(self.good)
        ctx.mark_cleared()
        ctx.await_recovery()

    def verify(self, ctx: Context) -> None:
        _blast_radius(ctx, ctx.notes.get("fault_samples", []), "replacements could not be scheduled")


class StorageClassChange(Scenario):
    """Operator-neutral version of recreate-on-immutable-change: point the data volumes at a
    different StorageClass. A PVC's class is immutable, so each operator either recreates, applies
    it to new volumes only, or reports that it can't; none of those may lose data or a shard."""

    def __init__(self):
        super().__init__(
            id="storage-class-change", category="spec", min_replicas=2, stable_samples=45,
            related_issues=["ClickHouse/clickhouse-operator#133"],
            title="Change the data volumes' StorageClass",
            description="Switch the volume claim to a second StorageClass with the same provisioner. Data must "
                        "survive, no shard may lose every replica, and the status must end up telling the truth.",
            expect=Expectations(recover_within_s=900, recover_slo_s=420))

    def inject(self, ctx: Context) -> None:
        ctx.kube.apply([{
            "apiVersion": "storage.k8s.io/v1", "kind": "StorageClass",
            "metadata": {"name": "chaos-local-path-2"},
            "provisioner": "rancher.io/local-path",
            "volumeBindingMode": "WaitForFirstConsumer", "reclaimPolicy": "Delete",
        }])
        ctx.reapply(ctx.spec.copy(storage_class="chaos-local-path-2"))
        # Some operators apply this without touching a pod; give them time to act either way.
        wait_until(lambda: ctx.op.ready_servers(ctx.spec) < ctx.spec.hosts, timeout=120, interval=2)

    def verify(self, ctx: Context) -> None:
        _blast_radius(ctx, ctx.samples, "the storage class changed")
