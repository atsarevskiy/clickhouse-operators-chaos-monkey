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

    def await_recovery(self, timeout: int | None = None, status_grace_s: int = 180) -> bool:
        """Poll until every pod is Ready and the scenario is done, then give the reported status
        status_grace_s to agree.

        Records: time until healthy (from when the fault was cleared, or from injection), time
        until the status says reconciled, and every sample where the status claimed healthy while
        pods were not."""
        timeout = timeout or self.result_expect().recover_within_s
        deadline = time.time() + timeout
        streak = 0
        while time.time() < deadline:
            snap = self.snapshot()
            ok = self.healthy(snap) and (self.scenario is None or self.scenario.done(self))
            streak = streak + 1 if ok else 0
            if streak >= 2 and self.recovered_at is None:
                self.recovered_at = self.since_reference()
                self.recovered_wall = time.time()
            if self.recovered_at is not None:
                if snap["reconciled"]:
                    self.status_converged_at = self.since_reference()
                    return True
                if time.time() - self.recovered_wall > status_grace_s:
                    return True
            time.sleep(3)
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
