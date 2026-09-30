"""Scenarios driven by spec changes, placement and network faults, and performance runs."""

from __future__ import annotations

import time

from ..kube import pod_ready, wait_until
from .base import Context, Expectations, Scenario
from .failures import recreated_since_inject, rolled


def _roll_value() -> str:
    return str(int(time.time()))


# ---------------------------------------------------------------- spec changes


class RollingConfigChange(Scenario):
    def __init__(self):
        super().__init__(
            id="rolling-restart", category="spec", min_replicas=2,
            title="Rolling restart through a pod template change",
            description="Change a pod annotation. Hosts must roll one replica at a time with queries still served.",
            expect=Expectations(recover_within_s=900, recover_slo_s=420))

    def inject(self, ctx: Context) -> None:
        marker = ctx.op.reconcile_marker(ctx.spec)
        ctx.reapply(ctx.spec.copy(pod_annotations={**ctx.spec.pod_annotations, "chaosmonkey/roll": _roll_value()}))
        wait_until(lambda: ctx.op.reconcile_marker(ctx.spec) != marker, timeout=120)
        # a roll hasn't finished until some pod actually went away and came back
        wait_until(lambda: ctx.op.ready_servers(ctx.spec) < ctx.spec.hosts, timeout=180, interval=1)

    def done(self, ctx: Context) -> bool:
        return rolled(ctx)

    def verify(self, ctx: Context) -> None:
        worst = min((min(s["per_shard_ready"]) for s in ctx.samples), default=ctx.spec.replicas)
        ctx.result.measure("min_ready_replicas_per_shard", worst, "replicas")
        if worst == 0:
            ctx.result.add("fail", "rollout safety", "a shard had no Ready replica during the roll")


class ScaleUpReplica(Scenario):
    def __init__(self):
        super().__init__(
            id="scale-up-replica", category="spec", min_replicas=2,
            title="Add a replica to every shard",
            description="New replicas must get the schema and replicate the existing data.",
            expect=Expectations(recover_within_s=600, recover_slo_s=300))

    def inject(self, ctx: Context) -> None:
        ctx.reapply(ctx.spec.copy(replicas=ctx.spec.replicas + 1))


class ScaleUpShard(Scenario):
    def __init__(self):
        super().__init__(
            id="scale-up-shard", category="spec",
            title="Add a shard",
            description="The new shard must come up with the schema so writes to it work.",
            expect=Expectations(recover_within_s=600, recover_slo_s=300))

    def inject(self, ctx: Context) -> None:
        ctx.reapply(ctx.spec.copy(shards=ctx.spec.shards + 1))
        ctx.workload.expected.setdefault(ctx.spec.shards - 1, 0)

    def verify(self, ctx: Context) -> None:
        new = ctx.spec.shards - 1
        missing = []
        for pod in ctx.op.server_pods(ctx.spec, shard=new):
            rc, out = ctx.op.sql(ctx.spec, pod["metadata"]["name"], "EXISTS TABLE chaos.events")
            if rc != 0 or out.strip() != "1":
                missing.append(pod["metadata"]["name"])
        if missing:
            ctx.result.add("warn", "schema propagation", f"new shard hosts without the table: {', '.join(missing)}")


class ScaleDownReplica(Scenario):
    def __init__(self):
        super().__init__(
            id="scale-down-replica", category="spec", min_replicas=3,
            title="Remove a replica from every shard",
            description="Remaining replicas keep the data, and the removed replica is dropped from replication metadata.",
            expect=Expectations(recover_within_s=600, recover_slo_s=300))

    def inject(self, ctx: Context) -> None:
        ctx.reapply(ctx.spec.copy(replicas=ctx.spec.replicas - 1))

    def recover(self, ctx: Context) -> None:
        wait_until(lambda: len(ctx.op.server_pods(ctx.spec)) == ctx.spec.hosts, timeout=300)
        ctx.await_recovery()

    def verify(self, ctx: Context) -> None:
        pod = ctx.op.server_pods(ctx.spec, shard=0)[0]["metadata"]["name"]
        rc, out = ctx.op.sql(ctx.spec, pod, "SELECT total_replicas FROM system.replicas "
                                            "WHERE database = 'chaos' AND table = 'events'")
        if rc == 0 and out.strip().isdigit() and int(out.strip()) != ctx.spec.replicas:
            ctx.result.add("warn", "replica cleanup",
                           f"replication metadata still lists {out.strip()} replicas, expected {ctx.spec.replicas}")


class VersionUpgrade(Scenario):
    def __init__(self, from_image: str = "clickhouse/clickhouse-server:26.3", to_image: str = "clickhouse/clickhouse-server:26.8"):
        super().__init__(
            id="server-version-upgrade", category="spec", min_replicas=2,
            title="Upgrade ClickHouse server version",
            description=f"Rolling upgrade {from_image} -> {to_image} with queries running.",
            expect=Expectations(recover_within_s=900, recover_slo_s=480))
        self.from_image, self.to_image = from_image, to_image
        self.images = [from_image, to_image]

    def spec_for(self, base):
        return super().spec_for(base).copy(server_image=self.from_image)

    def inject(self, ctx: Context) -> None:
        ctx.reapply(ctx.spec.copy(server_image=self.to_image))
        wait_until(lambda: ctx.op.ready_servers(ctx.spec) < ctx.spec.hosts, timeout=180, interval=1)

    def recover(self, ctx: Context) -> None:
        def upgraded() -> bool:
            pods = ctx.op.server_pods(ctx.spec)
            return len(pods) == ctx.spec.hosts and all(
                pod_ready(p) and p["spec"]["containers"][0]["image"].endswith(self.to_image.split("/")[-1]) for p in pods)
        t = wait_until(upgraded, timeout=ctx.result_expect().recover_within_s, interval=3)
        ctx.result.measure("upgrade_completed_after_s", t, "s")
        if t is None:
            ctx.result.add("fail", "upgrade", "not every host is on the new version")
        ctx.await_recovery(timeout=120)


class InvalidSpecRejected(Scenario):
    def __init__(self):
        super().__init__(
            requires=frozenset({"pod-labels"}),
            id="invalid-spec-no-damage", category="spec", min_replicas=1,
            related_issues=["Altinity/clickhouse-operator#1420"],
            title="Apply a spec the API server rejects",
            description="A pod label value Kubernetes refuses. The operator must not destroy running hosts trying to apply it.",
            expect=Expectations(recover_within_s=600, recover_slo_s=300))
        self.observe_s = 150

    def inject(self, ctx: Context) -> None:
        self.good = ctx.spec
        ctx.notes["sts_before"] = self._sts(ctx)
        ctx.reapply(ctx.spec.copy(pod_labels={**ctx.spec.pod_labels, "chaosmonkey-bad": "/metrics"}))
        end, lowest = time.time() + self.observe_s, len(ctx.notes["sts_before"])
        while time.time() < end:
            ctx.snapshot()
            lowest = min(lowest, len(self._sts(ctx)))
            time.sleep(3)
        ctx.notes["sts_lowest"] = lowest
        ctx.notes["bad_phase_samples"] = list(ctx.samples)

    def recover(self, ctx: Context) -> None:
        ctx.reapply(self.good)
        ctx.mark_cleared()
        ctx.await_recovery()

    def verify(self, ctx: Context) -> None:
        before, lowest = len(ctx.notes["sts_before"]), ctx.notes["sts_lowest"]
        ctx.result.measure("statefulsets_destroyed", before - lowest, "count")
        if lowest < before:
            ctx.result.add("fail", "destructive on invalid spec",
                           f"{before - lowest} of {before} server StatefulSets deleted while applying a rejected spec")
        worst = min((s["servers_ready"] for s in ctx.notes.get("bad_phase_samples", [])), default=ctx.spec.hosts)
        ctx.result.measure("bad_phase_min_ready_hosts", worst, "hosts")

    @staticmethod
    def _sts(ctx: Context) -> list[str]:
        """Server StatefulSets that still exist, out of the ones the baseline had."""
        baseline = ctx.notes.get("sts_names")
        if baseline is None:
            baseline = sorted({ref["name"] for p in ctx.op.server_pods(ctx.spec)
                               for ref in p["metadata"].get("ownerReferences", []) if ref["kind"] == "StatefulSet"})
            ctx.notes["sts_names"] = baseline
        return [n for n in baseline if ctx.kube.get("statefulset", n, ctx.spec.namespace)]


class ImmutableFieldChange(Scenario):
    def __init__(self):
        super().__init__(
            requires=frozenset({"volume-recreate"}),
            id="recreate-on-immutable-change", category="spec", min_replicas=2,
            title="Change an immutable StatefulSet field",
            description="Switching the data volume claim template forces every StatefulSet to be deleted and recreated. "
                        "Replicas come back on empty volumes and must regain schema and data from their peers.",
            expect=Expectations(recover_within_s=900, recover_slo_s=480))

    def inject(self, ctx: Context) -> None:
        ctx.reapply(ctx.spec.copy(volume_variant="b"))
        wait_until(lambda: ctx.op.ready_servers(ctx.spec) < ctx.spec.hosts, timeout=180, interval=1)

    def done(self, ctx: Context) -> bool:
        return recreated_since_inject(ctx)

    def verify(self, ctx: Context) -> None:
        worst = min((min(s["per_shard_ready"]) for s in ctx.samples), default=ctx.spec.replicas)
        ctx.result.measure("min_ready_replicas_per_shard", worst, "replicas")
        if worst == 0:
            ctx.result.add("fail", "rollout safety", "a shard had no Ready replica during the recreate")


class UnschedulableReplacement(Scenario):
    def __init__(self):
        super().__init__(
            requires=frozenset({"volume-recreate"}),
            id="unschedulable-replacement", category="spec", min_replicas=2,
            related_issues=["Altinity/clickhouse-operator#2069", "Altinity/clickhouse-operator#2078"],
            title="Recreate into a replacement that can never start",
            description="Force a recreate whose new volume references a missing StorageClass. The operator must stop "
                        "after the first failed host instead of taking down a shard or the Keeper quorum.",
            expect=Expectations(recover_within_s=900, recover_slo_s=480, keeper_quorum_must_hold=True,
                                max_read_outage_s=None, max_write_outage_s=None,
                                min_read_availability_pct=None, min_write_availability_pct=None))
        self.observe_s = 300

    def inject(self, ctx: Context) -> None:
        self.good = ctx.spec
        ctx.reapply(ctx.spec.copy(volume_variant="b", volume_b_storage_class="does-not-exist"))
        end = time.time() + self.observe_s
        while time.time() < end:
            ctx.snapshot()
            time.sleep(3)
        ctx.notes["bad_phase_samples"] = list(ctx.samples)
        ctx.notes["bad_phase_end"] = time.time()

    def recover(self, ctx: Context) -> None:
        ctx.reapply(self.good)
        ctx.mark_cleared()
        # The unschedulable pods keep their Pending PVCs; clearing them is part of the manual fix.
        for pvc in ctx.kube.items("pvc", ctx.spec.namespace):
            if pvc["status"].get("phase") == "Pending":
                ctx.kube.delete("pvc", pvc["metadata"]["name"], namespace=ctx.spec.namespace)
        ctx.await_recovery()

    def verify(self, ctx: Context) -> None:
        bad = ctx.notes.get("bad_phase_samples", [])
        worst_shard = min((min(s["per_shard_ready"]) for s in bad), default=ctx.spec.replicas)
        worst_keeper = min((s["keepers_ready"] for s in bad), default=ctx.spec.keepers)
        ctx.result.measure("bad_phase_min_ready_per_shard", worst_shard, "replicas")
        ctx.result.measure("bad_phase_min_ready_keepers", worst_keeper, "members")
        if worst_shard == 0:
            ctx.result.add("fail", "blast radius", "every replica of a shard was taken down for a replacement that could not start")
        if worst_keeper < ctx.spec.keepers // 2 + 1:
            ctx.result.add("fail", "blast radius", f"Keeper dropped to {worst_keeper} Ready members, below quorum")


class WedgedShutdownRecreate(Scenario):
    def __init__(self):
        super().__init__(
            requires=frozenset({"volume-recreate", "prestop"}),
            id="wedged-shutdown-recreate", category="spec", min_replicas=2,
            related_issues=["Altinity/clickhouse-operator#2078"],
            title="Recreate hosts whose pods hang on shutdown",
            description="Pods sleep in preStop far longer than the operator waits. A recreate must still finish "
                        "without leaving a host at zero replicas.",
            expect=Expectations(recover_within_s=900, recover_slo_s=480))

    def spec_for(self, base):
        return super().spec_for(base).copy(prestop_sleep=900, termination_grace=900)

    def inject(self, ctx: Context) -> None:
        ctx.reapply(ctx.spec.copy(volume_variant="b"))

    def done(self, ctx: Context) -> bool:
        return recreated_since_inject(ctx)


# ---------------------------------------------------------------- placement / infrastructure


class NodeDrain(Scenario):
    def __init__(self):
        super().__init__(
            id="node-drain", category="infrastructure", min_replicas=2,
            title="Cordon a node and evict its ClickHouse pods",
            description="Local volumes pin pods to their node, so evicted replicas wait until the node returns. "
                        "The other replicas must keep serving.",
            expect=Expectations(recover_slo_s=180))
        self.hold_s = 90

    def inject(self, ctx: Context) -> None:
        pod = ctx.op.server_pods(ctx.spec, shard=0, replica=0)[0]
        node = pod["spec"]["nodeName"]
        ctx.notes["node"] = node
        ctx.kube.run("cordon", node)
        victims = [p for p in ctx.op.server_pods(ctx.spec) if p["spec"].get("nodeName") == node]
        ctx.kill_pods(victims, grace=30)
        time.sleep(self.hold_s)
        ctx.kube.run("uncordon", node)
        ctx.mark_cleared()


class NodeStop(Scenario):
    def __init__(self):
        super().__init__(
            id="node-stop", category="infrastructure", min_replicas=2,
            title="Stop a worker node and start it again",
            description="Hard node loss for 90 s, then the node returns.",
            expect=Expectations(recover_within_s=600, recover_slo_s=300, keeper_quorum_must_hold=False))
        self.hold_s = 90

    def inject(self, ctx: Context) -> None:
        pod = ctx.op.server_pods(ctx.spec, shard=0, replica=0)[0]
        node = pod["spec"]["nodeName"]
        if node.endswith("server-0"):
            # never stop the control plane; pick the node of another replica instead
            others = [p for p in ctx.op.server_pods(ctx.spec) if not p["spec"]["nodeName"].endswith("server-0")]
            node = others[0]["spec"]["nodeName"] if others else node
        ctx.notes["node"] = node
        ctx.cluster.stop_node(node)
        time.sleep(self.hold_s)
        ctx.cluster.start_node(node)
        ctx.mark_cleared()


def _selector_to_expressions(selector: str, op: str) -> list[dict]:
    exprs = []
    for part in selector.split(","):
        k, v = part.split("=", 1)
        exprs.append({"key": k, "operator": op, "values": [v]})
    return exprs


class KeeperPartition(Scenario):
    def __init__(self):
        super().__init__(
            id="replica-keeper-partition", category="network", min_replicas=2,
            title="Cut one replica off from Keeper",
            description="A NetworkPolicy blocks one ClickHouse pod's traffic to Keeper for 60 s.",
            expect=Expectations(recover_slo_s=120))
        self.hold_s = 60

    def inject(self, ctx: Context) -> None:
        pod = ctx.op.server_pods(ctx.spec, shard=0, replica=0)[0]
        labels = pod["metadata"]["labels"]
        keeper_sel = ctx.op.keeper_selector(ctx.spec)
        np = {
            "apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
            "metadata": {"name": "chaos-keeper-partition"},
            "spec": {
                "podSelector": {"matchLabels": {"statefulset.kubernetes.io/pod-name": labels["statefulset.kubernetes.io/pod-name"]}},
                "policyTypes": ["Egress"],
                "egress": [{"to": [{"podSelector": {"matchExpressions": _selector_to_expressions(keeper_sel, "NotIn")}},
                                   {"namespaceSelector": {}}]}],
            },
        }
        # namespaceSelector {} alone would allow everything; restrict it to other namespaces (DNS)
        np["spec"]["egress"][0]["to"][1] = {"namespaceSelector": {"matchExpressions": [
            {"key": "kubernetes.io/metadata.name", "operator": "NotIn", "values": [ctx.spec.namespace]}]}}
        ctx.kube.apply([np], namespace=ctx.spec.namespace)
        time.sleep(self.hold_s)
        ctx.kube.delete("networkpolicy", "chaos-keeper-partition", namespace=ctx.spec.namespace)
        ctx.mark_cleared()


class ReplicaIsolation(Scenario):
    def __init__(self):
        super().__init__(
            id="replica-network-isolation", category="network", min_replicas=2,
            title="Isolate one replica from the network",
            description="Deny all ingress and egress for one ClickHouse pod for 60 s; queries must route to the other replica.",
            expect=Expectations(recover_slo_s=120))
        self.hold_s = 60

    def inject(self, ctx: Context) -> None:
        pod = ctx.op.server_pods(ctx.spec, shard=0, replica=0)[0]
        name = pod["metadata"]["labels"]["statefulset.kubernetes.io/pod-name"]
        ctx.kube.apply([{
            "apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
            "metadata": {"name": "chaos-isolate"},
            "spec": {"podSelector": {"matchLabels": {"statefulset.kubernetes.io/pod-name": name}},
                     "policyTypes": ["Ingress", "Egress"]},
        }], namespace=ctx.spec.namespace)
        time.sleep(self.hold_s)
        ctx.kube.delete("networkpolicy", "chaos-isolate", namespace=ctx.spec.namespace)
        ctx.mark_cleared()


# ---------------------------------------------------------------- performance


class PerfRollingRestart(Scenario):
    def __init__(self, shards: int = 4, replicas: int = 2):
        super().__init__(
            id="perf-rolling-restart", category="performance", performance=True,
            min_shards=shards, min_replicas=replicas,
            title=f"Rolling restart of {shards}x{replicas} hosts",
            description="Time and API cost of rolling every host once.",
            expect=Expectations(recover_within_s=1800, recover_slo_s=900))

    def inject(self, ctx: Context) -> None:
        RollingConfigChange.inject(self, ctx)  # same trigger

    def done(self, ctx: Context) -> bool:
        return rolled(ctx)


class PerfScaleOut(Scenario):
    def __init__(self, add: int = 2):
        super().__init__(
            id="perf-scale-out", category="performance", performance=True, min_shards=2, min_replicas=2,
            title=f"Scale out by {add} shards",
            description="Time and API cost to add shards to a running cluster.",
            expect=Expectations(recover_within_s=1200, recover_slo_s=600))
        self.add = add

    def inject(self, ctx: Context) -> None:
        ctx.reapply(ctx.spec.copy(shards=ctx.spec.shards + self.add))
        for s in range(ctx.spec.shards):
            ctx.workload.expected.setdefault(s, 0)


class PerfOperatorRestart(Scenario):
    def __init__(self, shards: int = 4, replicas: int = 2):
        super().__init__(
            id="perf-operator-restart", category="performance", performance=True,
            min_shards=shards, min_replicas=replicas,
            title=f"Operator restart on {shards}x{replicas} hosts",
            description="API requests a freshly started operator makes against a healthy cluster in its first 2 minutes.",
            expect=Expectations(recover_slo_s=120))

    def inject(self, ctx: Context) -> None:
        ctx.op.kill_operator(force=True)
        ctx.op.wait_operator_ready()
        time.sleep(120)
        ctx.mark_cleared()
