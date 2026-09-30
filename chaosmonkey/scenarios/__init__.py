"""Scenario registry and run profiles."""

from __future__ import annotations

from .base import Scenario
from .changes import (ImmutableFieldChange, InvalidSpecRejected, KeeperPartition, NodeDrain, NodeStop,
                      PerfOperatorRestart, PerfRollingRestart, PerfScaleOut, ReplicaIsolation, RollingConfigChange,
                      ScaleDownReplica, ScaleUpReplica, ScaleUpShard, UnschedulableReplacement, VersionUpgrade,
                      WedgedShutdownRecreate)
from .tracker import (DeletionWithoutOperator, ForegroundDeletion, KeeperRollingRestart,
                      KeeperStatefulSetDeleted, MigrationFailureMarkedDone, ReplicaServesBeforeSchema,
                      RestartDuringSlowStart, ScaleDownLeavesReplicaMetadata, ScaleDownShardLeavesMetadata)
from .failures import (AllServersKill, BadConfigRollout, ConfigMapsDeletedThenRestart, KeeperAllKill,
                       KeeperMemberKill, KeeperQuorumLoss, OperatorDownDuringFailure, OperatorKillIdle,
                       OperatorKillMidDriftRepair, OperatorKillMidRollout, PvcDeleted, ServerPodKill,
                       ServerProcessCrash, ServerStatefulSetDeleted, ServiceDeleted, ShardReplicasKill)

ALL: list[Scenario] = [
    ServerPodKill(), ShardReplicasKill(), AllServersKill(), ServerProcessCrash(),
    KeeperMemberKill(), KeeperQuorumLoss(), KeeperAllKill(),
    OperatorKillIdle(), OperatorKillMidRollout(), OperatorDownDuringFailure(), OperatorKillMidDriftRepair(),
    ServerStatefulSetDeleted(), ServiceDeleted(), ConfigMapsDeletedThenRestart(), PvcDeleted(),
    RollingConfigChange(), ScaleUpReplica(), ScaleUpShard(), ScaleDownReplica(), VersionUpgrade(),
    InvalidSpecRejected(), BadConfigRollout(), ImmutableFieldChange(), UnschedulableReplacement(),
    WedgedShutdownRecreate(),
    NodeDrain(), NodeStop(), KeeperPartition(), ReplicaIsolation(),
    PerfRollingRestart(), PerfScaleOut(), PerfOperatorRestart(),
    # from the operators' own issue trackers
    ScaleDownLeavesReplicaMetadata(), ScaleDownShardLeavesMetadata(), ReplicaServesBeforeSchema(),
    MigrationFailureMarkedDone(), KeeperStatefulSetDeleted(), ForegroundDeletion(),
    DeletionWithoutOperator(), KeeperRollingRestart(), RestartDuringSlowStart(),
]
BY_ID = {s.id: s for s in ALL}

_SMOKE = ["server-pod-kill", "keeper-member-kill", "operator-kill-idle", "rolling-restart", "server-statefulset-deleted"]
_STANDARD = _SMOKE + [
    "shard-all-replicas-kill", "all-servers-kill", "server-process-crash",
    "keeper-quorum-loss", "keeper-all-kill",
    "operator-kill-mid-rollout", "operator-down-during-failure", "operator-kill-mid-keeper-repair",
    "query-service-deleted", "configmaps-deleted-then-restart", "replica-volume-lost",
    "scale-up-replica", "scale-up-shard", "invalid-spec-no-damage", "bad-config-rollout",
    "replica-keeper-partition", "replica-network-isolation",
]
_FULL = _STANDARD + [
    "scale-down-replica", "server-version-upgrade", "recreate-on-immutable-change",
    "unschedulable-replacement", "wedged-shutdown-recreate", "node-drain", "node-stop",
]
_TRACKER = [
    "keeper-statefulset-deleted", "cluster-deleted-foreground", "cluster-deleted-while-operator-down",
    "new-replica-published-before-schema", "scale-down-replica-metadata", "keeper-rolling-restart",
    "new-replica-schema-blocked", "config-change-during-slow-start", "scale-down-shard-then-up",
]
_FULL += _TRACKER
_PERF = ["perf-rolling-restart", "perf-scale-out", "perf-operator-restart"]

PROFILES: dict[str, list[str]] = {
    "smoke": _SMOKE,
    "standard": _STANDARD,
    "full": _FULL,
    "tracker": _TRACKER,
    "perf": _PERF,
    "all": _FULL + _PERF,
}


def select(profile: str | None, ids: list[str] | None) -> list[Scenario]:
    wanted = ids or PROFILES[profile or "smoke"]
    unknown = [i for i in wanted if i not in BY_ID]
    if unknown:
        raise SystemExit(f"unknown scenario(s): {', '.join(unknown)}")
    return [BY_ID[i] for i in wanted]
