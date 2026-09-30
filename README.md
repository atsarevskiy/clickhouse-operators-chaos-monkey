# clickhouse-operators-chaos-monkey

Breaks ClickHouse clusters on purpose, on a local k3d cluster, and scores how well a Kubernetes
operator brings them back. One run answers "how good is this operator build?" with a scorecard:
every scenario gets PASS, DEGRADED or FAIL, with recovery time, availability during the failure,
data integrity, Keeper quorum, status truthfulness and the API cost of the operator's response.

It's generic across operators. Scenarios and checks only talk to an adapter interface, so the same
failures run against the [Altinity operator](https://github.com/Altinity/clickhouse-operator) and
the [ClickHouse operator](https://github.com/ClickHouse/clickhouse-operator), at any version or
from a custom image, and results line up side by side.

```
python3 -m chaosmonkey matrix profiles/compare-latest.json
        │
        ▼  per target: fresh k3d cluster (API audit log on), install operator build
runner ── per scenario: fresh namespace
        │   ├─ adapter.render(ClusterSpec) ─► baseline: CR healthy + every pod Ready
        │   ├─ workload: replicated table on every replica, known rows per shard
        │   ├─ probes: read + write through the cluster Service every second,
        │   │          Keeper members serving as leader/follower every second
        │   ├─ inject the failure
        │   ├─ recovery: time until every pod Ready, time until the CR reports healthy
        │   └─ verify: data per replica, writes after recovery, availability, quorum,
        │              API requests / 409s, operator-specific checks, scenario checks
        ▼
results/<run>/<operator>-<version>/results.json + <scenario>.samples.json + operator logs
results/<run>/scorecard.md
```

## What it checks

Every scenario is judged by the same generic invariants, then by its own checks.

| Check | Fails when | Degrades when |
|---|---|---|
| Recovery | the cluster isn't healthy within the scenario's limit | recovery is slower than its target |
| Status truthfulness | | every pod is Ready but the operator still reports the cluster as not healthy |
| Data | a replica holds fewer rows than were written to its shard | a replica holds more rows (duplication) |
| Writes after recovery | a write to any shard fails | |
| Availability | | read or write success during the failure is below the scenario's target |
| Keeper quorum | fewer than a majority of members serve for more than 10 s, where the scenario expects quorum to hold | |
| Operator-specific | | the operator's own bookkeeping contradicts itself (adapter `verify_operator_state`) |

A scenario can also measure blast radius. For example, it can fail if a broken config took every
replica of a shard down, or if applying a spec the API server rejects deleted running hosts.

Measurements recorded for every scenario: `baseline_create_s`, `time_to_recover_s`,
`time_to_status_healthy_s`, read/write availability and longest outage, Keeper minimum serving
members and longest time below quorum, and the operator's API requests, writes and 409 conflicts
from the audit log.

## Scenarios

`python3 -m chaosmonkey list` prints the current list with profiles.

| Category | Scenario | Failure |
|---|---|---|
| pods | `server-pod-kill` | force-delete one replica's pod |
| pods | `shard-all-replicas-kill` | kill every replica of one shard |
| pods | `all-servers-kill` | kill every ClickHouse pod |
| pods | `server-process-crash` | SIGKILL the server process inside its container |
| keeper | `keeper-member-kill` | kill one Keeper member (quorum should hold) |
| keeper | `keeper-quorum-loss` | kill a Keeper majority |
| keeper | `keeper-all-kill` | kill every Keeper member |
| operator | `operator-kill-idle` | restart the operator on a healthy cluster; nothing may be restarted |
| operator | `operator-kill-mid-rollout` | kill the operator in the middle of a rolling change |
| operator | `operator-down-during-failure` | delete a StatefulSet while the operator is scaled to zero |
| operator | `operator-kill-mid-keeper-repair` | kill the operator right after it recreated a deleted Keeper StatefulSet |
| drift | `server-statefulset-deleted` | delete a ClickHouse StatefulSet |
| drift | `query-service-deleted` | delete the client Service |
| drift | `configmaps-deleted-then-restart` | delete the operator's ConfigMaps, then restart a pod |
| storage | `replica-volume-lost` | delete one replica's PVC and pod; schema and data must come back |
| spec | `rolling-restart` | pod template change; hosts roll with queries served |
| spec | `scale-up-replica` / `scale-up-shard` / `scale-down-replica` | topology changes |
| spec | `server-version-upgrade` | rolling ClickHouse upgrade 26.3 to 26.8 |
| spec | `invalid-spec-no-damage` | a pod label value the API server rejects |
| spec | `bad-config-rollout` | a server setting that crashes startup; watch the blast radius, then revert |
| spec | `recreate-on-immutable-change` | change the data volume template, forcing StatefulSet recreation |
| spec | `unschedulable-replacement` | recreate into a volume with a missing StorageClass |
| spec | `wedged-shutdown-recreate` | recreate hosts whose pods hang in preStop |
| infrastructure | `node-drain` / `node-stop` | cordon and evict, or hard-stop a worker node |
| network | `replica-keeper-partition` | NetworkPolicy cuts one replica off from Keeper |
| network | `replica-network-isolation` | NetworkPolicy isolates one replica completely |
| performance | `perf-rolling-restart` / `perf-scale-out` / `perf-operator-restart` | time and API cost at 4x2 hosts |

Profiles: `smoke` (5 scenarios), `standard` (22), `full` (29), `perf` (3), `all`.

Some triggers depend on what an operator supports. A scenario whose requirement an adapter doesn't
declare is reported SKIPPED with the reason, not FAIL. For example, the ClickHouse operator has no
container lifecycle field and never recreates StatefulSets for volume changes, so the
preStop and volume-recreate scenarios are skipped for it.

## Running

Requirements: Docker, k3d v5, kubectl, Helm 3 (for the ClickHouse operator), Python 3.10+. No
Python packages are needed.

```
python3 -m chaosmonkey run --operator altinity --version 0.27.4 --profile smoke
python3 -m chaosmonkey run --operator clickhouse --version 0.0.8 --scenario keeper-quorum-loss --keep-cluster
python3 -m chaosmonkey run --operator altinity --version 0.27.4 --image my-operator:dev --label my-branch
python3 -m chaosmonkey matrix profiles/compare-latest.json --profile standard
python3 -m chaosmonkey report results/20260930-130000
```

A matrix file lists targets; each gets a fresh cluster:

```json
{
  "profile": "standard",
  "targets": [
    {"operator": "altinity", "version": "0.27.3"},
    {"operator": "altinity", "version": "0.27.4"},
    {"operator": "clickhouse", "version": "0.0.8"},
    {"operator": "altinity", "version": "0.27.4", "image": "my-operator:dev", "label": "my-branch",
     "env": {"OPERATOR_K8S_CLIENT_QPS_LIMIT": "200"}}
  ]
}
```

Useful options: `--shards/--replicas/--keepers` for the base topology (default 2x2 plus 3
Keepers), `--server-image/--keeper-image`, `--agents` for worker nodes, `--keep-cluster`.

Each target takes a while: a scenario runs 3 to 10 minutes including a fresh baseline, so
`standard` is roughly two to three hours per operator build.

**Host limits.** Every k3d node runs a kubelet that needs inotify instances. If other local
clusters are running, a new node's kubelet can fail with `inotify_init: too many open files` and
never register. The platform stops after 300 s with that hint. Raise
`fs.inotify.max_user_instances` or use fewer `--agents`.

## Adding an operator

Write one adapter in `chaosmonkey/operators/` implementing `OperatorAdapter`
(`chaosmonkey/operators/base.py`) and register it in `chaosmonkey/operators/__init__.py`:

| Method | Returns |
|---|---|
| `install(env)` / `uninstall()` | installs the operator build from scratch and waits until it runs |
| `render(spec)` | the custom resources for a generic `ClusterSpec` (shards, replicas, Keepers, storage, images, pod template knobs, settings) |
| `state(spec)` | whether the operator reports the cluster fully reconciled, and a readable phase |
| `reconcile_marker(spec)` | a value that changes when a new reconcile starts |
| `server_selector` / `keeper_selector` | label selectors for pods by shard and replica |
| `cluster_name`, `query_service`, `keeper_hosts` | the names the workload and probes connect to |
| `verify_operator_state(spec)` | operator-specific consistency findings |
| `capabilities` | which triggers the operator supports |

The adapter provisions a `chaos` user for the workload. `tests/test_adapters.py` checks every
registered adapter offline (`python3 -m unittest`).

## Adding a scenario

Subclass `Scenario` (`chaosmonkey/scenarios/base.py`): set `id`, `category`, the minimum topology
and `Expectations` (recovery limit and target, availability targets, whether quorum must hold),
implement `inject(ctx)`, and optionally `recover(ctx)` and `verify(ctx)`. Register it in
`chaosmonkey/scenarios/__init__.py` and add it to a profile.
