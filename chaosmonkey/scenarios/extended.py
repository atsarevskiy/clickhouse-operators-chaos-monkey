"""The second half of the suite: harder versions of the basic faults, and faults the first half
does not reach.

    pods        repeated kills, a frozen server process, memory pressure, back-to-back replica kills
    keeper      the leader specifically, a frozen leader, a member that lost its volume, membership changes
    network     a partitioned replica pair, DNS loss, ClickHouse cut off from Keeper or from the operator
    operator    kills mid scale-up and scale-down, a kill loop, a reinstall, spec changes while it is down
    drift       every StatefulSet or Service gone, a StatefulSet scaled or edited by hand, status wiped
    spec        Keeper scale and upgrade, a server downgrade, live-reloadable settings, quick successive edits
    infra       a frozen node, a node gone past the eviction timeout, a node restart, a Keeper node drained
    lifecycle   a cluster recreated under the same name, its namespace deleted

Every trigger uses only what every adapter exposes: pod and StatefulSet objects, NetworkPolicies,
node containers, and the generic ClusterSpec.
"""

from __future__ import annotations

import time

from ..kube import pod_ready, wait_until
from ..workload import DB
from .base import Context, Expectations, Scenario
from .changes import VersionUpgrade
from .failures import _owner_statefulset, _pick, container_restarts, rolled
from .tracker import _blast_radius

_NO_LIMITS = dict(max_read_outage_s=None, max_write_outage_s=None,
                  min_read_availability_pct=None, min_write_availability_pct=None)


# ---------------------------------------------------------------- helpers


def _labels(selector: str) -> dict[str, str]:
    return dict(part.split("=", 1) for part in selector.split(","))


def _pod_name_selector(pod: dict) -> dict:
    return {"matchLabels": {"statefulset.kubernetes.io/pod-name": pod["metadata"]["name"]}}


def _policy(ctx: Context, name: str, pod_selector: dict, ingress: list | None = None,
            egress: list | None = None) -> None:
    """A NetworkPolicy; a direction given as [] denies all of it, None leaves it alone."""
    types, spec = [], {"podSelector": pod_selector}
    if ingress is not None:
        types.append("Ingress")
        spec["ingress"] = ingress
    if egress is not None:
        types.append("Egress")
        spec["egress"] = egress
    spec["policyTypes"] = types
    ctx.kube.apply([{"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
                     "metadata": {"name": name}, "spec": spec}], namespace=ctx.spec.namespace)


def _unpolicy(ctx: Context, name: str) -> None:
    ctx.kube.delete("networkpolicy", name, namespace=ctx.spec.namespace)


def _other_namespaces(ctx: Context) -> dict:
    return {"namespaceSelector": {"matchExpressions": [
        {"key": "kubernetes.io/metadata.name", "operator": "NotIn", "values": [ctx.spec.namespace]}]}}


def keeper_mode(ctx: Context, pod: str) -> str | None:
    rc, out = ctx.kube.exec(ctx.spec.namespace, pod, ctx.op.keeper_container,
                            ["sh", "-c", f"echo srvr | nc -w1 127.0.0.1 {ctx.op.keeper_port}"], timeout=10)
    for line in out.splitlines() if rc == 0 else []:
        if line.startswith("Mode:"):
            return line.split(":", 1)[1].strip()
    return None


def keeper_leader(ctx: Context) -> dict | None:
    for pod in ctx.op.keeper_pods(ctx.spec):
        if pod_ready(pod) and keeper_mode(ctx, pod["metadata"]["name"]) in ("leader", "standalone"):
            return pod
    return None


def _keeper_followers(ctx: Context) -> int | None:
    """Synced followers as the leader counts them."""
    leader = keeper_leader(ctx)
    if not leader:
        return None
    rc, out = ctx.kube.exec(ctx.spec.namespace, leader["metadata"]["name"], ctx.op.keeper_container,
                            ["sh", "-c", f"echo mntr | nc -w1 127.0.0.1 {ctx.op.keeper_port}"], timeout=10)
    for line in out.splitlines() if rc == 0 else []:
        parts = line.split()
        if len(parts) == 2 and parts[0] == "zk_synced_followers" and parts[1].isdigit():
            return int(parts[1])
    return None


def _check_ensemble(ctx: Context) -> None:
    want = ctx.spec.keepers - 1
    got = _keeper_followers(ctx)
    ctx.result.measure("keeper_synced_followers", got, "members", f"expected {want}")
    if got is None:
        ctx.result.add("fail", "keeper ensemble", "no Keeper member reports itself leader")
    elif got != want:
        ctx.result.add("fail", "keeper ensemble", f"the leader has {got} synced followers, expected {want}")


def _signal(ctx: Context, pod: dict, container: str, sig: str) -> None:
    if not ctx.cluster.signal_container(pod, container, sig):
        raise RuntimeError(f"could not send SIG{sig} to {pod['metadata']['name']}/{container}")


def _worker_node_of(ctx: Context, pods: list[dict]) -> str:
    """The node of the first pod not on the control plane, which no scenario may disturb."""
    for pod in pods:
        node = pod["spec"].get("nodeName", "")
        if node and not node.endswith("server-0"):
            return node
    raise RuntimeError("every candidate pod is on the control-plane node")


def _schema_everywhere(ctx: Context) -> None:
    missing = [p["metadata"]["name"] for p in ctx.op.server_pods(ctx.spec)
               if not ctx.workload.table_count(p["metadata"]["name"])]
    if missing:
        ctx.result.add("fail", "schema propagation", f"hosts without the tables: {', '.join(missing)}")


def _restarts(ctx: Context) -> dict[str, str]:
    """Pod identity and container restarts, to tell whether a change touched a pod."""
    return {p["metadata"]["name"]: f"{p['metadata']['uid']}:{container_restarts(p)}"
            for p in ctx.op.server_pods(ctx.spec) + ctx.op.keeper_pods(ctx.spec)}


def _replica_metadata(ctx: Context) -> None:
    pods = ctx.op.server_pods(ctx.spec, shard=0)
    got = ctx.workload.total_replicas(pods[0]["metadata"]["name"]) if pods else None
    if got is not None and got > ctx.spec.replicas:
        ctx.result.add("warn", "replica cleanup",
                       f"replication metadata still lists {got} replicas, expected {ctx.spec.replicas}")


def _scale_operator(ctx: Context, replicas: int) -> None:
    deploy = ctx.op.operator_deployment() if replicas == 0 else ctx.notes["operator_deployment"]
    ctx.notes["operator_deployment"] = deploy
    ctx.kube.run("-n", ctx.op.operator_namespace, "scale", f"deployment/{deploy}", f"--replicas={replicas}")
    if replicas == 0:
        wait_until(lambda: not ctx.op.operator_pods(), timeout=120)
    else:
        ctx.op.wait_operator_ready()


# ---------------------------------------------------------------- pods


class ServerPodKillRepeated(Scenario):
    def __init__(self):
        super().__init__(
            id="server-pod-kill-repeated", category="pods", min_replicas=2,
            title="Kill the same ClickHouse pod five times in a row",
            description="Force-delete one replica's pod every 20 s, five times. Each replacement must come back "
                        "and catch up, and the other replica must keep serving throughout.",
            expect=Expectations(recover_slo_s=120))

    def inject(self, ctx: Context) -> None:
        for _ in range(5):
            ctx.kill_pods([_pick(ctx)])
            time.sleep(20)


class ServerProcessFreeze(Scenario):
    """A server that stops responding without dying: the pod stays Running while every connection
    to it hangs, until a probe gives up on it."""

    def __init__(self):
        super().__init__(
            id="server-process-freeze", category="pods", min_replicas=2,
            title="Freeze one ClickHouse server process for 60 s",
            description="SIGSTOP the server process of one replica, then SIGCONT it 60 s later. Clients must be "
                        "routed away from the frozen host, and it must rejoin and catch up.",
            expect=Expectations(recover_slo_s=120))
        self.hold_s = 60

    def inject(self, ctx: Context) -> None:
        pod = _pick(ctx)
        ctx.notes["victim"] = pod["metadata"]["name"]
        _signal(ctx, pod, ctx.op.server_container, "STOP")
        ctx.observe(self.hold_s, settle_s=self.hold_s, min_s=self.hold_s)
        ctx.cluster.signal_container(pod, ctx.op.server_container, "CONT")
        ctx.mark_cleared()


class ServerMemoryPressure(Scenario):
    def __init__(self):
        super().__init__(
            id="server-memory-pressure", category="pods", min_replicas=2,
            title="Run a query that wants more memory than the pod has",
            description="A query on one replica allocates without a per-query limit. ClickHouse should refuse "
                        "it at its own server memory limit; if the container is killed instead, the operator "
                        "must bring the replica back.",
            expect=Expectations(recover_slo_s=120))

    def inject(self, ctx: Context) -> None:
        pod = _pick(ctx)
        name = pod["metadata"]["name"]
        ctx.notes["victim"], ctx.notes["restarts_before"] = name, container_restarts(pod)
        # roughly 10 GB of strings in one aggregate state, against a pod limit of a few GiB
        rc, out = ctx.op.sql(ctx.spec, name,
                             "SELECT length(groupArray(randomString(1000))) FROM numbers(10000000) "
                             "SETTINGS max_memory_usage = 0, max_execution_time = 90", timeout=120)
        ctx.notes["query_result"] = "completed" if rc == 0 else " ".join(out.split())[-200:]
        ctx.mark_cleared()

    def verify(self, ctx: Context) -> None:
        result = ctx.notes.get("query_result", "")
        pod = ctx.kube.get("pod", ctx.notes["victim"], ctx.spec.namespace) or {}
        killed = container_restarts(pod) > ctx.notes["restarts_before"]
        refused = "MEMORY_LIMIT_EXCEEDED" in result
        ctx.result.add("info", "memory query", "container killed" if killed else result)
        if not killed and not refused:
            ctx.result.add("fail", "fault not applied", f"the query was neither refused nor killed: {result[:120]}")


class ReplicasKilledBackToBack(Scenario):
    """Ready has to mean serving: the second replica of a shard is killed the moment the first one's
    replacement reports Ready."""

    def __init__(self):
        super().__init__(
            id="replicas-killed-back-to-back", category="pods", min_replicas=2,
            title="Kill a replica, then its peer as soon as the first is Ready",
            description="Kill replica 0 of shard 0, wait for its replacement to be Ready, then kill replica 1. "
                        "If Ready came before the replica could serve, the shard goes dark.",
            expect=Expectations(recover_slo_s=150, max_read_outage_s=20, max_write_outage_s=30))

    def inject(self, ctx: Context) -> None:
        first = _pick(ctx, shard=0, replica=0)
        ctx.kill_pods([first])
        def replaced() -> bool:
            pods = ctx.op.server_pods(ctx.spec, shard=0, replica=0)
            return bool(pods) and pods[0]["metadata"]["uid"] != first["metadata"]["uid"] and pod_ready(pods[0])
        wait_until(replaced, timeout=300, interval=1)
        ctx.kill_pods([_pick(ctx, shard=0, replica=1)])


class AllPodsKill(Scenario):
    def __init__(self):
        super().__init__(
            id="all-pods-kill", category="pods",
            title="Kill every ClickHouse and Keeper pod at once",
            description="Full outage of servers and coordination together. Everything must come back with the data.",
            expect=Expectations(recover_slo_s=240, keeper_quorum_must_hold=False, **_NO_LIMITS))

    def inject(self, ctx: Context) -> None:
        ctx.kill_pods(ctx.op.server_pods(ctx.spec) + ctx.op.keeper_pods(ctx.spec))


class ServerAndKeeperKill(Scenario):
    def __init__(self):
        super().__init__(
            id="server-and-keeper-kill", category="pods", min_replicas=2,
            title="Kill one ClickHouse pod and one Keeper member together",
            description="Quorum holds and the other replica serves; both pods must come back.",
            expect=Expectations(recover_slo_s=120))

    def inject(self, ctx: Context) -> None:
        ctx.kill_pods([_pick(ctx), ctx.op.keeper_pods(ctx.spec)[0]])


# ---------------------------------------------------------------- keeper


class KeeperLeaderKill(Scenario):
    def __init__(self):
        super().__init__(
            id="keeper-leader-kill", category="keeper",
            title="Kill the Keeper leader",
            description="The remaining two elect a new leader; writes pause only for the election.",
            expect=Expectations(recover_slo_s=90, max_write_outage_s=20))

    def inject(self, ctx: Context) -> None:
        leader = keeper_leader(ctx)
        if not leader:
            raise RuntimeError("no Keeper leader found")
        ctx.notes["leader"] = leader["metadata"]["name"]
        ctx.kill_pods([leader])

    def verify(self, ctx: Context) -> None:
        _check_ensemble(ctx)


class KeeperLeaderFreeze(Scenario):
    def __init__(self):
        super().__init__(
            id="keeper-leader-freeze", category="keeper",
            title="Freeze the Keeper leader for 30 s",
            description="SIGSTOP the leader's process, SIGCONT it 30 s later. The others must elect a leader, "
                        "and the old one must rejoin as a follower.",
            expect=Expectations(recover_slo_s=90, max_write_outage_s=30))
        self.hold_s = 30

    def inject(self, ctx: Context) -> None:
        leader = keeper_leader(ctx)
        if not leader:
            raise RuntimeError("no Keeper leader found")
        ctx.notes["leader"] = leader["metadata"]["name"]
        _signal(ctx, leader, ctx.op.keeper_container, "STOP")
        ctx.observe(self.hold_s, settle_s=self.hold_s, min_s=self.hold_s)
        ctx.cluster.signal_container(leader, ctx.op.keeper_container, "CONT")
        ctx.mark_cleared()

    def verify(self, ctx: Context) -> None:
        _check_ensemble(ctx)


class KeeperMemberVolumeLost(Scenario):
    def __init__(self):
        super().__init__(
            id="keeper-member-volume-lost", category="storage",
            title="Lose one Keeper member's volume",
            description="Delete a follower's PVC and pod. It comes back with an empty log and must rejoin the "
                        "ensemble from a snapshot, with quorum held throughout.",
            expect=Expectations(recover_slo_s=180))

    def inject(self, ctx: Context) -> None:
        leader = keeper_leader(ctx)
        follower = next(p for p in ctx.op.keeper_pods(ctx.spec)
                        if not leader or p["metadata"]["name"] != leader["metadata"]["name"])
        for vol in follower["spec"].get("volumes", []):
            if vol.get("persistentVolumeClaim"):
                ctx.kube.delete("pvc", vol["persistentVolumeClaim"]["claimName"], namespace=ctx.spec.namespace)
        ctx.kill_pods([follower])

    def verify(self, ctx: Context) -> None:
        _check_ensemble(ctx)


class KeeperMembersKilledInTurn(Scenario):
    def __init__(self):
        super().__init__(
            id="keeper-members-killed-in-turn", category="keeper",
            title="Kill each Keeper member in turn",
            description="Kill a member, wait until it is Ready again, then the next, through the whole ensemble. "
                        "Only one member is ever down, so quorum must hold the whole time.",
            expect=Expectations(recover_slo_s=90, max_write_outage_s=20))

    def inject(self, ctx: Context) -> None:
        for name in [p["metadata"]["name"] for p in ctx.op.keeper_pods(ctx.spec)]:
            pod = ctx.kube.get("pod", name, ctx.spec.namespace)
            if not pod:
                continue
            ctx.kill_pods([pod])
            wait_until(lambda: (ctx.kube.get("pod", name, ctx.spec.namespace) or {}).get("metadata", {}).get("uid")
                       not in (None, pod["metadata"]["uid"]) and ctx.op.ready_keepers(ctx.spec) == ctx.spec.keepers,
                       timeout=300, interval=1)

    def verify(self, ctx: Context) -> None:
        _check_ensemble(ctx)


class KeeperProcessCrash(Scenario):
    def __init__(self):
        super().__init__(
            id="keeper-process-crash", category="keeper",
            title="Crash one Keeper process in place",
            description="SIGKILL one member's process from its node; the kubelet restarts the container.",
            expect=Expectations(recover_slo_s=60))

    def inject(self, ctx: Context) -> None:
        pod = ctx.op.keeper_pods(ctx.spec)[0]
        ctx.notes["victim"], ctx.notes["restarts_before"] = pod["metadata"]["name"], container_restarts(pod)
        _signal(ctx, pod, ctx.op.keeper_container, "KILL")

    def verify(self, ctx: Context) -> None:
        pod = ctx.kube.get("pod", ctx.notes["victim"], ctx.spec.namespace) or {}
        if container_restarts(pod) <= ctx.notes["restarts_before"]:
            ctx.result.add("fail", "fault not applied", "the Keeper container never restarted")
        _check_ensemble(ctx)


class KeeperScaleUp(Scenario):
    def __init__(self):
        super().__init__(
            id="keeper-scale-up", category="spec", min_shards=1, min_replicas=2,
            title="Grow Keeper from three members to five",
            description="Every new member must join the ensemble, quorum must hold throughout, and ClickHouse "
                        "must keep writing.",
            expect=Expectations(recover_within_s=900, recover_slo_s=420))

    def spec_for(self, base):
        # five members on three nodes can't all be spread one per node
        return super().spec_for(base).copy(spread_replicas=False)

    def inject(self, ctx: Context) -> None:
        ctx.reapply(ctx.spec.copy(keepers=5))

    def done(self, ctx: Context) -> bool:
        return len(ctx.op.keeper_pods(ctx.spec)) == ctx.spec.keepers

    def verify(self, ctx: Context) -> None:
        _check_ensemble(ctx)


class KeeperScaleDown(Scenario):
    def __init__(self):
        super().__init__(
            id="keeper-scale-down", category="spec", min_shards=1, min_replicas=2, min_keepers=5,
            title="Shrink Keeper from five members to three",
            description="Removed members must leave the ensemble cleanly; the remaining three must hold quorum "
                        "and count each other as followers.",
            expect=Expectations(recover_within_s=900, recover_slo_s=420))

    def spec_for(self, base):
        return super().spec_for(base).copy(spread_replicas=False)

    def inject(self, ctx: Context) -> None:
        ctx.reapply(ctx.spec.copy(keepers=3))

    def done(self, ctx: Context) -> bool:
        return len(ctx.op.keeper_pods(ctx.spec)) == ctx.spec.keepers

    def verify(self, ctx: Context) -> None:
        _check_ensemble(ctx)


# ---------------------------------------------------------------- network


class KeeperLeaderIsolation(Scenario):
    def __init__(self):
        super().__init__(
            id="keeper-leader-isolation", category="network",
            title="Isolate the Keeper leader for 60 s",
            description="Deny all traffic to and from the leader. The others elect a new one; the isolated member "
                        "must step down and rejoin when the network comes back.",
            expect=Expectations(recover_slo_s=90, max_write_outage_s=30))
        self.hold_s = 60

    def inject(self, ctx: Context) -> None:
        leader = keeper_leader(ctx)
        if not leader:
            raise RuntimeError("no Keeper leader found")
        ctx.notes["leader"] = leader["metadata"]["name"]
        _policy(ctx, "chaos-isolate-leader", _pod_name_selector(leader), ingress=[], egress=[])
        ctx.observe(self.hold_s, settle_s=self.hold_s, min_s=self.hold_s)
        _unpolicy(ctx, "chaos-isolate-leader")
        ctx.mark_cleared()

    def verify(self, ctx: Context) -> None:
        _check_ensemble(ctx)


class ReplicaPairPartition(Scenario):
    """The two replicas of a shard can't reach each other but both still reach Keeper and clients,
    so both keep accepting writes and replication has to reconcile them afterwards."""

    def __init__(self):
        super().__init__(
            id="replica-pair-partition", category="network", min_replicas=2,
            title="Partition the two replicas of a shard from each other for 90 s",
            description="Each replica of shard 0 refuses connections from the other. Both keep taking writes; "
                        "once the partition lifts they must end up with identical data.",
            expect=Expectations(recover_slo_s=120))
        self.hold_s = 90

    def inject(self, ctx: Context) -> None:
        a, b = _pick(ctx, shard=0, replica=0), _pick(ctx, shard=0, replica=1)
        for name, me, peer in (("chaos-partition-a", a, b), ("chaos-partition-b", b, a)):
            _policy(ctx, name, _pod_name_selector(me), ingress=[
                {"from": [{"podSelector": {"matchExpressions": [
                    {"key": "statefulset.kubernetes.io/pod-name", "operator": "NotIn",
                     "values": [peer["metadata"]["name"]]}]}}, _other_namespaces(ctx)]}])
        ctx.observe(self.hold_s, settle_s=self.hold_s, min_s=self.hold_s)
        _unpolicy(ctx, "chaos-partition-a")
        _unpolicy(ctx, "chaos-partition-b")
        ctx.mark_cleared()


class DnsOutage(Scenario):
    def __init__(self):
        super().__init__(
            id="dns-outage", category="network", min_replicas=2,
            title="Cut the ClickHouse pods off from DNS for 90 s",
            description="Block egress from every ClickHouse pod to other namespaces, which takes out cluster DNS. "
                        "Established connections keep working; anything that resolves a name fails until DNS "
                        "is back, and replication must resume afterwards.",
            expect=Expectations(recover_slo_s=120, **_NO_LIMITS))
        self.hold_s = 90

    def inject(self, ctx: Context) -> None:
        _policy(ctx, "chaos-no-dns", {"matchLabels": _labels(ctx.op.server_selector(ctx.spec))},
                egress=[{"to": [{"podSelector": {}}]}])
        ctx.observe(self.hold_s, settle_s=self.hold_s, min_s=self.hold_s)
        _unpolicy(ctx, "chaos-no-dns")
        ctx.mark_cleared()


class ClickHouseLosesKeeper(Scenario):
    def __init__(self):
        super().__init__(
            id="clickhouse-loses-keeper", category="network",
            title="Cut every ClickHouse pod off from Keeper for 60 s",
            description="Keeper keeps its quorum, but no server can reach it, so sessions expire and replicated "
                        "tables go read-only. Writes must resume once the network is back.",
            expect=Expectations(recover_slo_s=150, max_write_outage_s=None, min_write_availability_pct=None))
        self.hold_s = 60

    def inject(self, ctx: Context) -> None:
        servers = _labels(ctx.op.server_selector(ctx.spec))
        _policy(ctx, "chaos-keeper-unreachable", {"matchLabels": _labels(ctx.op.keeper_selector(ctx.spec))},
                ingress=[{"from": [{"podSelector": {"matchExpressions": [
                    {"key": k, "operator": "NotIn", "values": [v]} for k, v in servers.items()]}},
                    _other_namespaces(ctx)]}])
        ctx.observe(self.hold_s, settle_s=self.hold_s, min_s=self.hold_s)
        _unpolicy(ctx, "chaos-keeper-unreachable")
        ctx.mark_cleared()


class OperatorCutOff(Scenario):
    """The operator can reach the API server but not ClickHouse, while a new replica needs its
    schema: an operator that treats a failed query as done leaves the replica empty."""

    def __init__(self):
        super().__init__(
            id="operator-cut-off-from-clickhouse", category="network", min_shards=1, min_replicas=2,
            title="Add a replica while the operator cannot reach ClickHouse",
            description="Refuse connections from the operator's namespace to every ClickHouse pod for 180 s, and "
                        "add a replica meanwhile. Once the operator can connect again the new replica must get "
                        "its schema.",
            expect=Expectations(recover_within_s=900, recover_slo_s=420))
        self.hold_s = 180

    def inject(self, ctx: Context) -> None:
        _policy(ctx, "chaos-no-operator", {"matchLabels": _labels(ctx.op.server_selector(ctx.spec))},
                ingress=[{"from": [{"podSelector": {}}, {"namespaceSelector": {"matchExpressions": [
                    {"key": "kubernetes.io/metadata.name", "operator": "NotIn",
                     "values": [ctx.op.operator_namespace]}]}}]}])
        ctx.reapply(ctx.spec.copy(replicas=ctx.spec.replicas + 1))
        ctx.observe(self.hold_s, settle_s=self.hold_s, min_s=self.hold_s)
        _unpolicy(ctx, "chaos-no-operator")
        ctx.mark_cleared()

    def done(self, ctx: Context) -> bool:
        return len(ctx.op.server_pods(ctx.spec)) == ctx.spec.hosts

    def verify(self, ctx: Context) -> None:
        _schema_everywhere(ctx)


# ---------------------------------------------------------------- operator


class OperatorKillLoop(Scenario):
    def __init__(self):
        super().__init__(
            id="operator-kill-loop", category="operator", min_shards=1, min_replicas=2,
            title="Kill the operator every 20 s while it adds a replica",
            description="Request a new replica, then kill the operator pod nine times, 20 s apart. Every restart "
                        "has to pick up where the last one stopped.",
            expect=Expectations(recover_within_s=900, recover_slo_s=420))

    def inject(self, ctx: Context) -> None:
        ctx.reapply(ctx.spec.copy(replicas=ctx.spec.replicas + 1))
        for _ in range(9):
            time.sleep(20)
            ctx.op.kill_operator(force=True)
        ctx.mark_cleared()

    def done(self, ctx: Context) -> bool:
        return len(ctx.op.server_pods(ctx.spec)) == ctx.spec.hosts

    def verify(self, ctx: Context) -> None:
        _schema_everywhere(ctx)


class OperatorKillMidScaleUp(Scenario):
    def __init__(self):
        super().__init__(
            id="operator-kill-mid-scale-up", category="operator", min_shards=1, min_replicas=2,
            title="Kill the operator as soon as a new replica's pod appears",
            description="The new operator pod must finish the scale-up, including the schema on the new replica.",
            expect=Expectations(recover_within_s=900, recover_slo_s=420))

    def inject(self, ctx: Context) -> None:
        before = len(ctx.op.server_pods(ctx.spec))
        ctx.reapply(ctx.spec.copy(replicas=ctx.spec.replicas + 1))
        wait_until(lambda: len(ctx.op.server_pods(ctx.spec)) > before, timeout=300, interval=1)
        ctx.op.kill_operator(force=True)

    def done(self, ctx: Context) -> bool:
        return len(ctx.op.server_pods(ctx.spec)) == ctx.spec.hosts

    def verify(self, ctx: Context) -> None:
        _schema_everywhere(ctx)


class OperatorKillMidScaleDown(Scenario):
    def __init__(self):
        super().__init__(
            id="operator-kill-mid-scale-down", category="operator", min_shards=1, min_replicas=3,
            title="Kill the operator in the middle of removing a replica",
            description="The new operator pod must finish removing the replica and drop it from replication "
                        "metadata.",
            expect=Expectations(recover_within_s=900, recover_slo_s=420))

    def inject(self, ctx: Context) -> None:
        ctx.reapply(ctx.spec.copy(replicas=ctx.spec.replicas - 1))
        time.sleep(10)
        ctx.op.kill_operator(force=True)

    def done(self, ctx: Context) -> bool:
        return len(ctx.op.server_pods(ctx.spec)) == ctx.spec.hosts

    def verify(self, ctx: Context) -> None:
        _replica_metadata(ctx)


class OperatorReinstall(Scenario):
    def __init__(self):
        super().__init__(
            id="operator-reinstall", category="operator",
            title="Delete and reinstall the operator under a running cluster",
            description="Remove the operator Deployment and install it again. A healthy cluster must not be "
                        "restarted or reconfigured by the fresh install.",
            expect=Expectations(recover_slo_s=60))

    def inject(self, ctx: Context) -> None:
        ctx.notes["before"] = _restarts(ctx)
        ctx.kube.delete("deployment", ctx.op.operator_deployment(), namespace=ctx.op.operator_namespace, wait=True)
        wait_until(lambda: not ctx.op.operator_pods(), timeout=120)
        ctx.op.install()
        time.sleep(90)
        ctx.mark_cleared()

    def verify(self, ctx: Context) -> None:
        after = _restarts(ctx)
        touched = [n for n, v in ctx.notes["before"].items() if after.get(n) != v]
        ctx.result.measure("pods_touched_by_reinstall", len(touched), "pods")
        if touched:
            ctx.result.add("warn", "idle restart", f"pods replaced or restarted by the reinstall: {', '.join(touched)}")


class OperatorDownDuringSpecChange(Scenario):
    def __init__(self):
        super().__init__(
            id="operator-down-during-spec-change", category="operator", min_replicas=2,
            title="Change the spec while the operator is down",
            description="Scale the operator to zero, roll the pod template, then bring it back. The change made "
                        "while it was away must be applied.",
            expect=Expectations(recover_within_s=900, recover_slo_s=420))

    def inject(self, ctx: Context) -> None:
        _scale_operator(ctx, 0)
        ctx.reapply(ctx.spec.copy(pod_annotations={**ctx.spec.pod_annotations, "chaosmonkey/roll": str(int(time.time()))}))
        time.sleep(30)
        _scale_operator(ctx, 1)
        ctx.mark_cleared()

    def done(self, ctx: Context) -> bool:
        return rolled(ctx)


class OperatorDownServiceDeleted(Scenario):
    def __init__(self):
        super().__init__(
            id="operator-down-service-deleted", category="operator",
            title="Delete the client Service while the operator is down",
            description="Scale the operator to zero, delete the client Service, bring the operator back. The "
                        "returning operator must recreate it.",
            expect=Expectations(recover_slo_s=120, **_NO_LIMITS))

    def inject(self, ctx: Context) -> None:
        _scale_operator(ctx, 0)
        ctx.kube.delete("service", ctx.op.query_service(ctx.spec), namespace=ctx.spec.namespace)
        time.sleep(30)
        _scale_operator(ctx, 1)
        ctx.mark_cleared()

    def recover(self, ctx: Context) -> None:
        svc = ctx.op.query_service(ctx.spec)
        t = wait_until(lambda: ctx.kube.get("service", svc, ctx.spec.namespace) is not None, timeout=300)
        ctx.result.measure("service_recreated_after_s", t, "s")
        if t is None:
            ctx.result.add("fail", "drift repair", f"Service {svc} was not recreated")
        ctx.await_recovery(timeout=60)


# ---------------------------------------------------------------- drift


class AllServerStatefulSetsDeleted(Scenario):
    def __init__(self):
        super().__init__(
            id="all-server-statefulsets-deleted", category="drift",
            title="Delete every ClickHouse StatefulSet",
            description="Every server pod goes with them. The operator must recreate all of them on their "
                        "existing volumes, with the data.",
            expect=Expectations(recover_slo_s=240, **_NO_LIMITS))

    def inject(self, ctx: Context) -> None:
        names = sorted({_owner_statefulset(p) for p in ctx.op.server_pods(ctx.spec)} - {None})
        ctx.notes["statefulsets"] = names
        for name in names:
            ctx.kube.delete("statefulset", name, namespace=ctx.spec.namespace)

    def verify(self, ctx: Context) -> None:
        missing = [n for n in ctx.notes["statefulsets"] if not ctx.kube.get("statefulset", n, ctx.spec.namespace)]
        if missing:
            ctx.result.add("fail", "drift repair", f"StatefulSets never recreated: {', '.join(missing)}")


class StatefulSetScaledToZero(Scenario):
    def __init__(self):
        super().__init__(
            id="statefulset-scaled-to-zero", category="drift", min_replicas=2,
            title="Scale a ClickHouse StatefulSet to zero by hand",
            description="kubectl scale --replicas=0 on one host's StatefulSet. The operator owns that field and "
                        "must put the host back.",
            expect=Expectations(recover_slo_s=120))

    def inject(self, ctx: Context) -> None:
        sts = _owner_statefulset(_pick(ctx))
        ctx.notes["sts"] = sts
        ctx.kube.run("-n", ctx.spec.namespace, "scale", f"statefulset/{sts}", "--replicas=0")


class StatefulSetTemplateEdited(Scenario):
    def __init__(self):
        super().__init__(
            id="statefulset-template-edited", category="drift", min_replicas=2,
            title="Edit a ClickHouse StatefulSet's pod template by hand",
            description="Add an environment variable to one host's StatefulSet. The StatefulSet controller rolls "
                        "the pod; the operator should notice the edit and restore its own template.",
            expect=Expectations(recover_slo_s=180))

    def inject(self, ctx: Context) -> None:
        sts = _owner_statefulset(_pick(ctx))
        ctx.notes["sts"] = sts
        ctx.kube.patch("statefulset", sts, ctx.spec.namespace,
                       [{"op": "add", "path": "/spec/template/spec/containers/0/env/-",
                         "value": {"name": "CHAOSMONKEY_DRIFT", "value": "1"}}]
                       if self._has_env(ctx, sts) else
                       [{"op": "add", "path": "/spec/template/spec/containers/0/env",
                         "value": [{"name": "CHAOSMONKEY_DRIFT", "value": "1"}]}], patch_type="json")
        time.sleep(30)

    @staticmethod
    def _has_env(ctx: Context, sts: str) -> bool:
        obj = ctx.kube.get("statefulset", sts, ctx.spec.namespace) or {}
        return bool(obj.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [{}])[0].get("env"))

    def verify(self, ctx: Context) -> None:
        time.sleep(60)
        obj = ctx.kube.get("statefulset", ctx.notes["sts"], ctx.spec.namespace) or {}
        env = obj.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [{}])[0].get("env") or []
        kept = any(e.get("name") == "CHAOSMONKEY_DRIFT" for e in env)
        ctx.result.measure("hand_edit_still_present", int(kept), "bool")
        if kept:
            ctx.result.add("warn", "drift repair", f"the hand edit to {ctx.notes['sts']} is still in its template")


class KeeperServicesDeleted(Scenario):
    def __init__(self):
        super().__init__(
            id="keeper-services-deleted", category="drift",
            title="Delete every Service in front of Keeper",
            description="Keeper keeps running, but new client and peer connections resolve nothing. The operator "
                        "must recreate the Services before sessions start failing.",
            expect=Expectations(recover_slo_s=120, **_NO_LIMITS))

    def inject(self, ctx: Context) -> None:
        keeper_labels = ctx.op.keeper_pods(ctx.spec)[0]["metadata"]["labels"]
        names = [s["metadata"]["name"] for s in ctx.kube.items("services", ctx.spec.namespace)
                 if (s["spec"].get("selector") or {})
                 and all(keeper_labels.get(k) == v for k, v in s["spec"]["selector"].items())]
        ctx.notes["services"] = names
        for name in names:
            ctx.kube.delete("service", name, namespace=ctx.spec.namespace)

    def recover(self, ctx: Context) -> None:
        names = ctx.notes["services"]
        t = wait_until(lambda: all(ctx.kube.get("service", n, ctx.spec.namespace) for n in names), timeout=300)
        ctx.result.measure("services_recreated_after_s", t, "s")
        if t is None:
            missing = [n for n in names if not ctx.kube.get("service", n, ctx.spec.namespace)]
            ctx.result.add("fail", "drift repair", f"Keeper Services not recreated: {', '.join(missing)}")
        ctx.await_recovery(timeout=120)


class AllServicesDeleted(Scenario):
    def __init__(self):
        super().__init__(
            id="all-services-deleted", category="drift",
            title="Delete every Service the operator created",
            description="Client, per-host and Keeper Services all go at once. The operator must recreate every one.",
            expect=Expectations(recover_slo_s=180, **_NO_LIMITS))

    def inject(self, ctx: Context) -> None:
        names = [s["metadata"]["name"] for s in ctx.kube.items("services", ctx.spec.namespace)]
        ctx.notes["services"] = names
        for name in names:
            ctx.kube.delete("service", name, namespace=ctx.spec.namespace)

    def recover(self, ctx: Context) -> None:
        names = ctx.notes["services"]
        t = wait_until(lambda: all(ctx.kube.get("service", n, ctx.spec.namespace) for n in names), timeout=300)
        ctx.result.measure("services_recreated_after_s", t, "s")
        if t is None:
            missing = [n for n in names if not ctx.kube.get("service", n, ctx.spec.namespace)]
            ctx.result.add("fail", "drift repair", f"{len(missing)} of {len(names)} Services not recreated: "
                                                   f"{', '.join(missing[:6])}")
        ctx.await_recovery(timeout=120)


class KeeperConfigMapsDeletedThenRestart(Scenario):
    def __init__(self):
        super().__init__(
            id="keeper-configmaps-deleted-then-restart", category="drift",
            title="Delete Keeper's ConfigMaps, then restart a member",
            description="The ConfigMaps a Keeper pod mounts are deleted and the pod restarted. It can only start "
                        "if the operator restores them.",
            expect=Expectations(recover_slo_s=180))

    def inject(self, ctx: Context) -> None:
        pod = ctx.op.keeper_pods(ctx.spec)[0]
        names = sorted({v["configMap"]["name"] for v in pod["spec"].get("volumes", []) if v.get("configMap")
                        and not v["configMap"]["name"].startswith("kube-root-ca")})
        ctx.notes["configmaps"] = names
        for name in names:
            ctx.kube.delete("configmap", name, namespace=ctx.spec.namespace)
        time.sleep(5)
        ctx.kill_pods([pod])


class StatusWiped(Scenario):
    def __init__(self):
        super().__init__(
            id="status-wiped", category="drift",
            title="Wipe the custom resources' status",
            description="Replace the status of every cluster resource with an empty one. The operator must report "
                        "the real state again without touching a single pod.",
            expect=Expectations(recover_slo_s=120))

    def inject(self, ctx: Context) -> None:
        ctx.notes["before"] = _restarts(ctx)
        for kind in ctx.op.cr_kinds:
            for obj in ctx.kube.items(kind, ctx.spec.namespace):
                ctx.kube.patch(kind, obj["metadata"]["name"], ctx.spec.namespace,
                               [{"op": "replace", "path": "/status", "value": {}}],
                               patch_type="json", subresource="status")
        time.sleep(5)

    def done(self, ctx: Context) -> bool:
        return ctx.op.state(ctx.spec).reconciled

    def verify(self, ctx: Context) -> None:
        after = _restarts(ctx)
        touched = [n for n, v in ctx.notes["before"].items() if after.get(n) != v]
        if touched:
            ctx.result.add("warn", "idle restart", f"pods replaced or restarted after the status was wiped: "
                                                   f"{', '.join(touched)}")


# ---------------------------------------------------------------- spec


class ServerVersionDowngrade(VersionUpgrade):
    def __init__(self):
        super().__init__(from_image="clickhouse/clickhouse-server:26.8", to_image="clickhouse/clickhouse-server:26.3")
        self.id, self.title = "server-version-downgrade", "Downgrade ClickHouse server version"
        self.description = "Rolling downgrade clickhouse/clickhouse-server:26.8 -> 26.3 with queries running."


class KeeperVersionUpgrade(Scenario):
    def __init__(self, from_image: str = "clickhouse/clickhouse-keeper:26.3",
                 to_image: str = "clickhouse/clickhouse-keeper:26.8"):
        super().__init__(
            id="keeper-version-upgrade", category="spec", min_replicas=2,
            title="Upgrade Keeper version",
            description=f"Rolling upgrade {from_image} -> {to_image}; quorum must hold and writes continue.",
            expect=Expectations(recover_within_s=900, recover_slo_s=420, max_write_outage_s=30))
        self.from_image, self.to_image = from_image, to_image
        self.images = [from_image, to_image]

    def spec_for(self, base):
        return super().spec_for(base).copy(keeper_image=self.from_image)

    def inject(self, ctx: Context) -> None:
        ctx.reapply(ctx.spec.copy(keeper_image=self.to_image))
        wait_until(lambda: ctx.op.ready_keepers(ctx.spec) < ctx.spec.keepers, timeout=240, interval=1)

    def done(self, ctx: Context) -> bool:
        tag = self.to_image.split(":")[-1]
        return all(p["spec"]["containers"][0]["image"].endswith(f":{tag}") for p in ctx.op.keeper_pods(ctx.spec))

    def verify(self, ctx: Context) -> None:
        _check_ensemble(ctx)


class ReloadableSettingChange(Scenario):
    """ClickHouse applies `max_concurrent_queries` from config.xml without a restart, so an operator
    that rolls every host for it costs a full restart for nothing."""

    def __init__(self):
        super().__init__(
            id="reloadable-setting-change", category="spec", min_replicas=2,
            title="Change a server setting ClickHouse reloads live",
            description="Set max_concurrent_queries through the operator's setting for values that need no "
                        "restart. The new value must reach every host; restarting hosts to apply it is reported.",
            expect=Expectations(recover_within_s=900, recover_slo_s=300))
        self.value = "321"

    def spec_for(self, base):
        # the field is present from the start, so the change is a value change only
        return super().spec_for(base).copy(reloadable_settings={"max_concurrent_queries": "500"})

    def inject(self, ctx: Context) -> None:
        ctx.notes["before"] = _restarts(ctx)
        ctx.reapply(ctx.spec.copy(reloadable_settings={**ctx.spec.reloadable_settings,
                                                       "max_concurrent_queries": self.value}))
        time.sleep(10)

    def done(self, ctx: Context) -> bool:
        for pod in ctx.op.server_pods(ctx.spec):
            rc, out = ctx.op.sql(ctx.spec, pod["metadata"]["name"],
                                 "SELECT value FROM system.server_settings WHERE name = 'max_concurrent_queries'")
            if rc != 0 or out.strip() != self.value:
                return False
        return True

    def verify(self, ctx: Context) -> None:
        after = _restarts(ctx)
        servers = {p["metadata"]["name"] for p in ctx.op.server_pods(ctx.spec)}
        touched = [n for n, v in ctx.notes["before"].items() if n in servers and after.get(n) != v]
        ctx.result.measure("hosts_restarted_for_reloadable_setting", len(touched), "hosts")
        if touched:
            ctx.result.add("warn", "unneeded restart",
                           f"restarted {len(touched)} of {len(servers)} hosts for a setting ClickHouse reloads live")


class MemoryLimitChange(Scenario):
    def __init__(self):
        super().__init__(
            id="memory-limit-change", category="spec", min_replicas=2,
            title="Raise the ClickHouse pods' memory limit",
            description="Change the container memory limit. Hosts roll one replica at a time and every pod ends "
                        "up with the new limit.",
            expect=Expectations(recover_within_s=900, recover_slo_s=420))
        self.limit = "3Gi"

    def inject(self, ctx: Context) -> None:
        ctx.reapply(ctx.spec.copy(server_memory_limit=self.limit))
        wait_until(lambda: ctx.op.ready_servers(ctx.spec) < ctx.spec.hosts, timeout=180, interval=1)

    def done(self, ctx: Context) -> bool:
        return all(p["spec"]["containers"][0].get("resources", {}).get("limits", {}).get("memory") == self.limit
                   for p in ctx.op.server_pods(ctx.spec))

    def verify(self, ctx: Context) -> None:
        _blast_radius(ctx, ctx.samples, "the memory limit rolled")


class RapidSpecChanges(Scenario):
    def __init__(self):
        super().__init__(
            id="rapid-spec-changes", category="spec", min_replicas=2,
            title="Five pod template changes 10 s apart",
            description="The operator must end on the last change, without taking a shard down while it "
                        "replaces a roll that was already in progress.",
            expect=Expectations(recover_within_s=1200, recover_slo_s=600))

    def inject(self, ctx: Context) -> None:
        for i in range(5):
            ctx.reapply(ctx.spec.copy(pod_annotations={**ctx.spec.pod_annotations,
                                                       "chaosmonkey/roll": f"{int(time.time())}-{i}"}))
            time.sleep(10)

    def done(self, ctx: Context) -> bool:
        return rolled(ctx)

    def verify(self, ctx: Context) -> None:
        _blast_radius(ctx, ctx.samples, "changes kept arriving")


class ScaleUpShardAndReplica(Scenario):
    def __init__(self):
        super().__init__(
            id="scale-up-shard-and-replica", category="spec", min_shards=1, min_replicas=2,
            title="Add a shard and a replica in one change",
            description="Every new host, in the new shard and in the existing ones, must get the schema.",
            expect=Expectations(recover_within_s=900, recover_slo_s=420))

    def inject(self, ctx: Context) -> None:
        ctx.reapply(ctx.spec.copy(shards=ctx.spec.shards + 1, replicas=ctx.spec.replicas + 1))
        ctx.workload.expected.setdefault(ctx.spec.shards - 1, 0)

    def done(self, ctx: Context) -> bool:
        return len(ctx.op.server_pods(ctx.spec)) == ctx.spec.hosts

    def verify(self, ctx: Context) -> None:
        _schema_everywhere(ctx)


class ScaleDownToOneReplica(Scenario):
    def __init__(self):
        super().__init__(
            id="scale-down-to-one-replica", category="spec", min_replicas=2,
            title="Remove every replica but one",
            description="Each shard keeps a single replica with all its data, and the removed replicas leave the "
                        "replication metadata.",
            expect=Expectations(recover_within_s=600, recover_slo_s=300))

    def inject(self, ctx: Context) -> None:
        ctx.reapply(ctx.spec.copy(replicas=1))

    def done(self, ctx: Context) -> bool:
        return len(ctx.op.server_pods(ctx.spec)) == ctx.spec.hosts

    def verify(self, ctx: Context) -> None:
        _replica_metadata(ctx)


# ---------------------------------------------------------------- infrastructure


class NodePause(Scenario):
    def __init__(self):
        super().__init__(
            id="node-pause", category="infrastructure", min_replicas=2,
            title="Freeze a worker node for 60 s",
            description="docker pause stops every process on the node: the kubelet goes silent and connections "
                        "to its pods hang instead of failing. Clients must move to the other replicas.",
            expect=Expectations(recover_within_s=600, recover_slo_s=180, keeper_quorum_must_hold=False))
        self.hold_s = 60

    def inject(self, ctx: Context) -> None:
        node = _worker_node_of(ctx, ctx.op.server_pods(ctx.spec, shard=0))
        ctx.notes["node"] = node
        ctx.cluster.pause_node(node)
        ctx.observe(self.hold_s, settle_s=self.hold_s, min_s=self.hold_s)
        ctx.cluster.unpause_node(node)
        ctx.mark_cleared()


class NodeStopLong(Scenario):
    """Past the default 300 s not-ready toleration the node's pods are marked for deletion but can't
    finish terminating, so StatefulSets won't replace them until the node is back."""

    def __init__(self):
        super().__init__(
            id="node-stop-long", category="infrastructure", min_replicas=2,
            title="Stop a worker node for six minutes",
            description="The node stays down past the pod eviction timeout. The other replicas must serve the "
                        "whole time, and the cluster must heal once the node returns.",
            expect=Expectations(recover_within_s=900, recover_slo_s=300, keeper_quorum_must_hold=False))
        self.hold_s = 360

    def inject(self, ctx: Context) -> None:
        node = _worker_node_of(ctx, ctx.op.server_pods(ctx.spec, shard=0))
        ctx.notes["node"] = node
        ctx.cluster.stop_node(node)
        ctx.observe(self.hold_s, settle_s=self.hold_s, min_s=self.hold_s)
        ctx.cluster.start_node(node)
        ctx.mark_cleared()


class NodeRestart(Scenario):
    def __init__(self):
        super().__init__(
            id="node-restart", category="infrastructure", min_replicas=2,
            title="Restart a worker node",
            description="docker restart of a node: every container on it restarts together, and the cluster must "
                        "heal without help.",
            expect=Expectations(recover_within_s=600, recover_slo_s=180, keeper_quorum_must_hold=False))

    def inject(self, ctx: Context) -> None:
        node = _worker_node_of(ctx, ctx.op.server_pods(ctx.spec, shard=0))
        ctx.notes["node"] = node
        ctx.cluster.restart_node(node)
        ctx.mark_cleared()


class KeeperNodeDrain(Scenario):
    def __init__(self):
        super().__init__(
            id="keeper-node-drain", category="infrastructure",
            title="Drain the node of a Keeper member",
            description="Cordon the node of one Keeper member and evict its pods; local volumes keep them waiting "
                        "for the node. Quorum must hold until it is uncordoned.",
            expect=Expectations(recover_slo_s=180))
        self.hold_s = 90

    def inject(self, ctx: Context) -> None:
        node = _worker_node_of(ctx, ctx.op.keeper_pods(ctx.spec))
        ctx.notes["node"] = node
        ctx.kube.run("cordon", node)
        victims = [p for p in ctx.op.server_pods(ctx.spec) + ctx.op.keeper_pods(ctx.spec)
                   if p["spec"].get("nodeName") == node]
        ctx.kill_pods(victims, grace=30)
        time.sleep(self.hold_s)
        ctx.kube.run("uncordon", node)
        ctx.mark_cleared()


# ---------------------------------------------------------------- lifecycle


class ClusterRecreatedSameName(Scenario):
    """Deleting a cluster and creating it again under the same name reuses its Keeper paths, and its
    volumes if they were kept: the new replicas have to create their tables over what is left."""

    def __init__(self):
        super().__init__(
            id="cluster-recreated-same-name", category="lifecycle", stream_check=False,
            title="Delete the cluster and create it again under the same name",
            description="Delete the custom resources, wait until everything is gone, apply the same spec, and "
                        "recreate the schema. Table creation must not collide with what the old cluster left in "
                        "Keeper, and writes must work.",
            expect=Expectations(recover_within_s=900, recover_slo_s=420, data_must_survive=False,
                                keeper_quorum_must_hold=False, status_must_converge=True, **_NO_LIMITS))

    def inject(self, ctx: Context) -> None:
        ctx.op.delete_cluster(ctx.spec)
        # the custom resources outlive their StatefulSets while finalizers run, and an apply that
        # reaches a resource still being deleted is lost with it
        wait_until(lambda: not ctx.kube.items("statefulsets", ctx.spec.namespace)
                   and all(not ctx.kube.items(k, ctx.spec.namespace) for k in ctx.op.cr_kinds),
                   timeout=600, interval=2)
        ctx.workload.expected = {s: 0 for s in range(ctx.spec.shards)}
        ctx.workload.surplus_label = "data from the deleted cluster came back"
        ctx.op.apply(ctx.spec)
        ctx.mark_cleared()

    def recover(self, ctx: Context) -> None:
        ctx.await_recovery()
        if ctx.recovered_at is None:
            return
        for f in ctx.workload.setup():
            f.check = "schema recreated"
            ctx.result.findings.append(f)

    def verify(self, ctx: Context) -> None:
        if ctx.op.keeps_pvcs_on_delete:
            for f in ctx.result.findings:
                if f.check == ctx.workload.surplus_label:
                    f.severity, f.detail = "info", f.detail + " (the operator documents that volumes are reused)"


class NamespaceDeleted(Scenario):
    def __init__(self):
        super().__init__(
            id="namespace-deleted", category="lifecycle", stream_check=False,
            title="Delete the namespace with the cluster running",
            description="kubectl delete namespace. The operator must let its custom resources go so the "
                        "namespace finishes deleting.",
            expect=Expectations(recover_within_s=420, recover_slo_s=180, status_must_converge=False,
                                data_must_survive=False, keeper_quorum_must_hold=False, cluster_survives=False,
                                **_NO_LIMITS))

    def inject(self, ctx: Context) -> None:
        ctx.kube.run("delete", "namespace", ctx.spec.namespace, "--wait=false")

    def recover(self, ctx: Context) -> None:
        t = wait_until(lambda: ctx.kube.get("namespace", ctx.spec.namespace) is None,
                       timeout=ctx.result_expect().recover_within_s, interval=2)
        ctx.result.measure("namespace_deleted_after_s", t, "s")
        ctx.recovered_at = t
        if t is None:
            stuck = [f"{k.split('.')[0]}/{o['metadata']['name']}" for k in ctx.op.cr_kinds
                     for o in ctx.kube.items(k, ctx.spec.namespace)]
            ctx.result.add("fail", "deletion", f"namespace still Terminating; left: {', '.join(stuck) or 'no custom resources'}")


EXTENDED: list[Scenario] = [
    ServerPodKillRepeated(), ServerProcessFreeze(), ServerMemoryPressure(), ReplicasKilledBackToBack(),
    AllPodsKill(), ServerAndKeeperKill(),
    KeeperLeaderKill(), KeeperLeaderFreeze(), KeeperMemberVolumeLost(), KeeperMembersKilledInTurn(),
    KeeperProcessCrash(), KeeperScaleUp(), KeeperScaleDown(),
    KeeperLeaderIsolation(), ReplicaPairPartition(), DnsOutage(), ClickHouseLosesKeeper(), OperatorCutOff(),
    OperatorKillLoop(), OperatorKillMidScaleUp(), OperatorKillMidScaleDown(), OperatorReinstall(),
    OperatorDownDuringSpecChange(), OperatorDownServiceDeleted(),
    AllServerStatefulSetsDeleted(), StatefulSetScaledToZero(), StatefulSetTemplateEdited(), KeeperServicesDeleted(),
    AllServicesDeleted(), KeeperConfigMapsDeletedThenRestart(), StatusWiped(),
    ServerVersionDowngrade(), KeeperVersionUpgrade(), ReloadableSettingChange(), MemoryLimitChange(),
    RapidSpecChanges(), ScaleUpShardAndReplica(), ScaleDownToOneReplica(),
    NodePause(), NodeStopLong(), NodeRestart(), KeeperNodeDrain(),
    ClusterRecreatedSameName(), NamespaceDeleted(),
]
