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
| Recovery | the cluster isn't healthy within the scenario's limit, or nothing (pods, StatefulSets, status) changed for 180 s while unhealthy | recovery is slower than its target |
| Status truthfulness | | the CR reported reconciled for more than 60 s while hosts were not Ready |
| Status stuck | | every pod is Ready but the CR still isn't reconciled 300 s later |
| Data | a replica holds fewer rows than were written to its shard after `SYSTEM SYNC REPLICA` succeeded | a replica holds more rows (duplication) |
| Replica not caught up | a replica is short and `SYSTEM SYNC REPLICA` failed; the finding carries its `system.replicas` state | |
| Ingest stream | an acknowledged batch is missing, or two replicas of a shard hold different batches | a batch is stored twice |
| Writes after recovery | a write to any shard fails | |
| Availability | | the longest read or write outage during the failure is above the scenario's limit |
| Keeper quorum | fewer than a majority of members serve for more than 10 s, where the scenario expects quorum to hold | |
| Operator-specific | | the operator's own bookkeeping contradicts itself (adapter `verify_operator_state`) |

A status that turns healthy some time after the pods is not a finding: the ClickHouse operator,
for example, pushes configuration one replica per reconcile, so its status is honest while it
trails the pods. That delay is recorded as `status_lag_after_ready_s`.

The ingest stream writes one 100-row batch a second through the cluster Service with a synchronous
distributed insert, numbered by wall-clock milliseconds, from a pod pinned to the control-plane
node. After recovery the writer is stopped and every replica is read back batch by batch.

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

Profiles: `smoke` (5 scenarios), `standard` (22), `tracker` (9, written from the operators' issue
trackers), `full` (38), `perf` (3), `all`.

Some triggers depend on what an operator supports. A scenario whose requirement an adapter doesn't
declare is reported SKIPPED with the reason, not FAIL. For example, the ClickHouse operator has no
container lifecycle field and never recreates StatefulSets for volume changes, so the
preStop and volume-recreate scenarios are skipped for it.

## Results

Altinity operator 0.27.4 and ClickHouse operator 0.0.8, ClickHouse and Keeper 26.8, 2 shards x 2 replicas plus 3 Keepers on a three-node k3d cluster with replicas and Keepers spread by each operator's own placement setting, full suite, one run per scenario, 2026-10-01. No run was marked INVALID. The ClickHouse operator's `cluster-deleted-while-operator-down` was rerun after its documented PVC retention was taken into account. Regenerate with `python3 -m chaosmonkey report <run dirs...> --readme-table`.

| Operator | Correctness | PASS | DEGRADED | FAIL | SKIPPED | INVALID |
|---|---|---|---|---|---|---|
| altinity 0.27.4 | **82/100** | 35 | 2 | 7 | 0 | 0 |
| clickhouse 0.0.8 | **94/100** | 36 | 4 | 1 | 3 | 0 |

| Operator | Events | Runs timed | Mean s | p50 s | p90 s | p99 s | Max s | Never healthy | Client outage mean s | Outage p90 s | Outage max s |
|---|---|---|---|---|---|---|---|---|---|---|---|
| altinity 0.27.4 | failures | 22 | 49 | 17 | 104 | 301 | 301 | 5 | 13 | 27 | 263 |
| altinity 0.27.4 | changes | 16 | 141 | 121 | 242 | 364 | 364 | 1 | 0 | 1 | 2 |
| clickhouse 0.0.8 | failures | 25 | 25 | 17 | 49 | 169 | 169 | 2 | 5 | 25 | 49 |
| clickhouse 0.0.8 | changes | 14 | 116 | 17 | 328 | 789 | 789 | 0 | 0 | 1 | 2 |

| Category | altinity correctness / p50 / p90 s | clickhouse correctness / p50 / p90 s |
|---|---|---|
| drift | 25 / 301 / 301 | 100 / 17 / 51 |
| infrastructure | 100 / 16 / 16 | 100 / 17 / 17 |
| keeper | 100 / 17 / 57 | 100 / 18 / 49 |
| lifecycle | 88 / 104 / 104 | 100 / 17 / 17 |
| metadata | 50 / 85 / 85 | 25 / 27 / 27 |
| network | 100 / 1 / 1 | 100 / 0 / 0 |
| operator | 88 / 13 / 214 | 100 / 12 / 169 |
| performance | 100 / 123 / 364 | 100 / 17 / 328 |
| pods | 100 / 14 / 29 | 100 / 14 / 39 |
| spec | 86 / 121 / 238 | 91 / 17 / 150 |
| storage | 0 / 17 / 17 | 100 / 35 / 35 |

**Correctness** asks whether the outcome was right: data kept, every host back, status honest, nothing destroyed. Per scenario it is 100 with no correctness finding, 50 with warnings only, 0 with a failure; a scenario run several times counts once with the average of its runs, and SKIPPED scenarios don't count. **Response time** is seconds from the fault ending (or the change being applied) until every pod is Ready and the change has reached every pod, one sample per run: `failures` are the scenarios that break something, `changes` the spec and performance ones. `Never healthy` counts runs with no time at all, which the percentiles leave out. Client outage is the longest run of failed reads or writes during the run. INVALID runs happened while the host was overloaded (load above 2 per CPU, or under 3 GB free) and are not scored. PASS/DEGRADED/FAIL/SKIPPED/INVALID count runs.

| Category | Scenario | Expected | altinity 0.27.4 | What happened (altinity) | clickhouse 0.0.8 | What happened (clickhouse) |
|---|---|---|---|---|---|---|
| metadata | `scale-down-replica-metadata` | Scale a shard down by one replica and check the survivors no longer count the removed replica, and that its Keeper registration is gone. A long Keeper session timeout makes the removed replica look active while the operator tries to drop it. Targets: healthy within 300 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100)<br>correctness 100, healthy in 85 s | healthy 85 s after the fault ended, status agreed 68 s later; longest outage read 0 s / write 0 s; 0 of 145 acknowledged batches lost | FAIL (0)<br>correctness 0, healthy in 27 s | removed replica still registered: system.replicas still counts 3 replicas, expected 2; removed replica still registered in Keeper: 3 replicas still registered under the table path: 0, 1, 2; healthy 27 s after the fault ended, status agreed 195 s later; longest outage read 0 s / write 0 s; 0 of 208 acknowledged batches lost |
| performance | `perf-rolling-restart` | Time and API cost of rolling every host once. Targets: healthy within 900 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100)<br>correctness 100, healthy in 364 s | healthy 364 s after the fault ended, status agreed 25 s later; longest outage read 0 s / write 1 s; 0 of 346 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 328 s | healthy 328 s after the fault ended, status agreed 8 s later; longest outage read 0 s / write 0 s; 0 of 307 acknowledged batches lost |
| performance | `perf-operator-restart` | API requests a freshly started operator makes against a healthy cluster in its first 2 minutes. Targets: healthy within 120 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100)<br>correctness 100, healthy in 0 s | healthy 0 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 0 s; 0 of 126 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 0 s | healthy 0 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 0 s; 0 of 134 acknowledged batches lost |
| spec | `scale-up-replica` | New replicas must get the schema and replicate the existing data. Targets: healthy within 300 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100)<br>correctness 100, healthy in 95 s | healthy 95 s after the fault ended, status agreed 51 s later; longest outage read 0 s / write 0 s; 0 of 140 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 14 s | healthy 14 s after the fault ended, status agreed 275 s later; longest outage read 0 s / write 0 s; 0 of 268 acknowledged batches lost |
| spec | `invalid-spec-no-damage` | A pod label value Kubernetes refuses. The operator must not destroy running hosts trying to apply it. Targets: healthy within 300 s, read outage up to 10 s, write outage up to 20 s, no row lost. | FAIL (0)<br>correctness 0, healthy in 28 s | destroyed a host applying a rejected spec: 1 of 4 server StatefulSets deleted while applying a rejected spec; healthy 28 s after the fault ended, status agreed 63 s later; longest outage read 0 s / write 0 s; 0 of 189 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 0 s | healthy 0 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 0 s; 0 of 57 acknowledged batches lost |
| spec | `scale-down-replica` | Remaining replicas keep the data, and the removed replica is dropped from replication metadata. Targets: healthy within 300 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100)<br>correctness 100, healthy in 105 s | healthy 105 s after the fault ended, status agreed 66 s later; longest outage read 0 s / write 0 s; 0 of 155 acknowledged batches lost | DEGRADED (50)<br>correctness 50, healthy in 33 s | removed replica still registered: replication metadata still lists 3 replicas, expected 2; healthy 33 s after the fault ended, status agreed 170 s later; longest outage read 0 s / write 0 s; 0 of 185 acknowledged batches lost |
| spec | `recreate-on-immutable-change` | Switching the data volume claim template forces every StatefulSet to be deleted and recreated. Replicas come back on empty volumes and must regain schema and data from their peers. Targets: healthy within 480 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100)<br>correctness 100, healthy in 242 s | healthy 242 s after the fault ended, status agreed 28 s later; longest outage read 0 s / write 1 s; 0 of 236 acknowledged batches lost | SKIPPED | not run: the trigger needs volume-recreate, which this operator lacks |
| spec | `wedged-shutdown-recreate` | Pods sleep in preStop far longer than the operator waits. A recreate must still finish without leaving a host at zero replicas. Targets: healthy within 480 s, read outage up to 10 s, write outage up to 20 s, no row lost. | FAIL (0)<br>correctness 0, never healthy | never got back to healthy: stalled: no pod, StatefulSet or status change for 180s while unhealthy (4/4 servers, 2/3 keepers Ready); longest outage read 0 s / write 0 s | SKIPPED | not run: the trigger needs prestop, volume-recreate, which this operator lacks |
| spec | `unschedulable-rollout` | Add a nodeSelector no node matches. The operator must stop after the first replacement stays Pending instead of taking down a shard or the Keeper quorum, then recover on revert. Targets: healthy within 420 s, no row lost. | PASS (100)<br>correctness 100, healthy in 44 s | healthy 44 s after the fault ended, status agreed 67 s later; longest outage read 0 s / write 0 s; 0 of 188 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 11 s | healthy 11 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 0 s; 0 of 85 acknowledged batches lost |
| spec | `config-change-during-slow-start` | One replica is restarted with a slow start, and a config change arrives while it is still coming up. The operator must not keep killing it, and the cluster must converge. Targets: healthy within 600 s, no row lost. | PASS (100)<br>correctness 100, healthy in 163 s | healthy 163 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 0 s; 0 of 159 acknowledged batches lost | DEGRADED (50)<br>correctness 100, healthy in 789 s | recovered, but slower than the target: 789s, target 600s; healthy 789 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 0 s; 0 of 715 acknowledged batches lost |
| operator | `operator-kill-mid-rollout` | The new operator pod must finish the roll and report the cluster healthy. Targets: healthy within 360 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100)<br>correctness 100, healthy in 214 s | healthy 214 s after the fault ended, status agreed 47 s later; longest outage read 0 s / write 2 s; 0 of 236 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 169 s | healthy 169 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 1 s; 0 of 159 acknowledged batches lost |
| operator | `operator-kill-mid-keeper-repair` | Delete one Keeper member's StatefulSet, kill the operator as soon as it's recreated. Targets: healthy within 120 s, read outage up to 10 s, write outage up to 20 s, no row lost. | DEGRADED (50)<br>correctness 50, healthy in 13 s | status never reported healthy after every pod was Ready: operator still reports CHI Completed / CHK InProgress 300s after every pod was Ready; healthy 13 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 300 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 37 s | healthy 37 s after the fault ended, status agreed 8 s later; longest outage read 0 s / write 0 s; 0 of 43 acknowledged batches lost |
| infrastructure | `node-stop` | Hard node loss for 90 s, then the node returns. Targets: healthy within 300 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100)<br>correctness 100, healthy in 16 s | healthy 16 s after the fault ended, status agreed 64 s later; longest outage read 0 s / write 1 s; 0 of 160 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 17 s | healthy 17 s after the fault ended, status agreed 6 s later; longest outage read 0 s / write 0 s; 0 of 110 acknowledged batches lost |
| keeper | `keeper-member-kill` | Quorum holds with 2 of 3; writes pause only while sessions move. Targets: healthy within 90 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100)<br>correctness 100, healthy in 17 s | healthy 17 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 0 s; 0 of 34 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 16 s | healthy 16 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 0 s; 0 of 32 acknowledged batches lost |
| pods | `shard-all-replicas-kill` | The shard has no serving replica until the pods restart. Data must survive. Targets: healthy within 120 s, no row lost. | PASS (100)<br>correctness 100, healthy in 29 s | healthy 29 s after the fault ended, status agreed 6 s later; longest outage read 3 s / write 1 s; 0 of 35 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 39 s | healthy 39 s after the fault ended, status agreed 6 s later; longest outage read 3 s / write 1 s; 0 of 43 acknowledged batches lost |
| pods | `server-process-crash` | SIGKILL the server process inside its container; kubelet restarts the container without the pod being replaced. Targets: healthy within 60 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100)<br>correctness 100, healthy in 0 s | healthy 0 s after the fault ended, status agreed 6 s later; longest outage read 0 s / write 0 s; 0 of 33 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 1 s | healthy 1 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 0 s; 0 of 32 acknowledged batches lost |
| keeper | `keeper-all-kill` | Full coordination outage; the ensemble must re-form from its persisted state. Targets: healthy within 180 s, read outage up to 10 s, no row lost. | PASS (100)<br>correctness 100, healthy in 17 s | healthy 17 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 4 s; 0 of 16 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 20 s | healthy 20 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 4 s; 0 of 18 acknowledged batches lost |
| drift | `configmaps-deleted-then-restart` | A pod restarted after its config was deleted can only start if the operator restores it. Targets: healthy within 180 s, read outage up to 10 s, write outage up to 20 s, no row lost. | FAIL (0)<br>correctness 0, never healthy | never got back to healthy: stalled: no pod, StatefulSet or status change for 181s while unhealthy (3/4 servers, 3/3 keepers Ready); status reported healthy while hosts were down: reported reconciled for 99s while only 3/4 servers were Ready; clients saw an outage: read availability 76.3% below 95.0% over 152 samples and write availability 87.5% below 90.0% over 152 samples | PASS (100)<br>correctness 100, healthy in 36 s | healthy 36 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 0 s; 0 of 47 acknowledged batches lost |
| network | `replica-keeper-partition` | A NetworkPolicy blocks one ClickHouse pod's traffic to Keeper for 60 s. Targets: healthy within 120 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100)<br>correctness 100, healthy in 1 s | healthy 1 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 0 s; 0 of 65 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 0 s | healthy 0 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 0 s; 0 of 68 acknowledged batches lost |
| drift | `keeper-statefulset-deleted` | The operator must notice the missing StatefulSet and recreate it, without a spec change. Targets: healthy within 120 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100)<br>correctness 100, healthy in 20 s | healthy 20 s after the fault ended, status agreed 6 s later; longest outage read 0 s / write 0 s; 0 of 33 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 17 s | healthy 17 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 1 s; 0 of 28 acknowledged batches lost |
| lifecycle | `cluster-deleted-while-operator-down` | A delete requested with no operator running must complete once the operator returns, and must not leave orphaned StatefulSets, Services or PVCs behind. | DEGRADED (50)<br>correctness 50, never healthy | left objects behind after deletion: pvc left behind: data-a-chk-chaos-keeper-main-0-0-0, data-a-chk-chaos-keeper-main-0-1-0, data-a-chk-chaos-keeper-main-0-2-0; longest outage read 24 s / write 27 s | PASS (100)<br>correctness 100, never healthy | longest outage read 36 s / write 36 s |
| keeper | `keeper-rolling-restart` | Change the Keeper pod template and measure how long the ensemble runs with no leader and how long writes are refused. Targets: healthy within 420 s, read outage up to 10 s, write outage up to 30 s, no row lost. | PASS (100)<br>correctness 100, healthy in 57 s | healthy 57 s after the fault ended, status agreed 6 s later; longest outage read 0 s / write 2 s; 0 of 56 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 49 s | healthy 49 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 1 s; 0 of 54 acknowledged batches lost |
| metadata | `scale-down-shard-then-up` | Remove a shard, then add it back. The new shard's replicas must create their tables without colliding with metadata the removed shard left behind. Targets: healthy within 600 s. | FAIL (0)<br>correctness 0, never healthy | never got back to healthy: stalled: no pod, StatefulSet or status change for 181s while unhealthy (3/4 servers, 3/3 keepers Ready); longest outage read 0 s / write 0 s | DEGRADED (50)<br>correctness 50, healthy in 11 s | old data came back with a re-added shard: chaos-clickhouse-1-0-0 (shard 1) has 1200 rows, expected 200 and chaos-clickhouse-1-1-0 (shard 1) has 1200 rows, expected 200; healthy 11 s after the fault ended, status agreed 55 s later; longest outage read 49 s / write 1 s; 0 of 101 acknowledged batches lost |
| performance | `perf-scale-out` | Time and API cost to add shards to a running cluster. Targets: healthy within 600 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100)<br>correctness 100, healthy in 123 s | healthy 123 s after the fault ended, status agreed 27 s later; longest outage read 0 s / write 0 s; 0 of 147 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 17 s | healthy 17 s after the fault ended, status agreed 244 s later; longest outage read 0 s / write 0 s; 0 of 245 acknowledged batches lost |
| spec | `rolling-restart` | Change a pod annotation. Hosts must roll one replica at a time with queries still served. Targets: healthy within 420 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100)<br>correctness 100, healthy in 238 s | healthy 238 s after the fault ended, status agreed 22 s later; longest outage read 0 s / write 2 s; 0 of 228 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 150 s | healthy 150 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 2 s; 0 of 144 acknowledged batches lost |
| spec | `scale-up-shard` | The new shard must come up with the schema so writes to it work. Targets: healthy within 300 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100)<br>correctness 100, healthy in 121 s | healthy 121 s after the fault ended, status agreed 30 s later; longest outage read 0 s / write 0 s; 0 of 144 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 17 s | healthy 17 s after the fault ended, status agreed 167 s later; longest outage read 0 s / write 0 s; 0 of 173 acknowledged batches lost |
| spec | `bad-config-rollout` | Apply a server setting ClickHouse rejects at startup, watch the blast radius, then revert. Targets: healthy within 300 s, no row lost. | PASS (100)<br>correctness 100, healthy in 0 s | healthy 0 s after the fault ended, status agreed 82 s later; longest outage read 0 s / write 0 s; 0 of 121 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 14 s | healthy 14 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 0 s; 0 of 180 acknowledged batches lost |
| spec | `server-version-upgrade` | Rolling upgrade clickhouse/clickhouse-server:26.3 -> clickhouse/clickhouse-server:26.8 with queries running. Targets: healthy within 480 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100)<br>correctness 100, healthy in 231 s | healthy 231 s after the fault ended, status agreed 31 s later; longest outage read 0 s / write 0 s; 0 of 243 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 121 s | healthy 121 s after the fault ended, status agreed 7 s later; longest outage read 1 s / write 0 s; 0 of 123 acknowledged batches lost |
| spec | `unschedulable-replacement` | Force a recreate whose new volume references a missing StorageClass. The operator must stop after the first failed host instead of taking down a shard or the Keeper quorum. Targets: healthy within 480 s, no row lost. | PASS (100)<br>correctness 100, healthy in 46 s | healthy 46 s after the fault ended, status agreed 94 s later; longest outage read 0 s / write 0 s; 0 of 199 acknowledged batches lost | SKIPPED | not run: the trigger needs volume-recreate, which this operator lacks |
| spec | `stuck-terminating-pod` | Hold one replica's pod in Terminating with a finalizer, then roll the pod template. The operator must not take down the stuck pod's peer, and must finish once it's released. Targets: healthy within 420 s, no row lost. | PASS (100)<br>correctness 100, healthy in 222 s | healthy 222 s after the fault ended, status agreed 23 s later; longest outage read 0 s / write 1 s; 0 of 305 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 11 s | healthy 11 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 0 s; 0 of 180 acknowledged batches lost |
| spec | `storage-class-change` | Switch the volume claim to a second StorageClass with the same provisioner. Data must survive, no shard may lose every replica, and the status must end up telling the truth. Targets: healthy within 420 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100)<br>correctness 100, healthy in 236 s | healthy 236 s after the fault ended, status agreed 46 s later; longest outage read 0 s / write 1 s; 0 of 249 acknowledged batches lost | DEGRADED (50)<br>correctness 50, healthy in 123 s | status never reported healthy after every pod was Ready: operator still reports ClickHouse rollout pending, ConfigurationInSync=ConfigurationChanged / Keeper rollout pending, ConfigurationInSync=ConfigurationChanged 300s after every pod was Ready; healthy 123 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 422 acknowledged batches lost |
| operator | `operator-kill-idle` | A restarted operator must not touch a healthy cluster: no pod restarts, status stays healthy. Targets: healthy within 60 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100)<br>correctness 100, healthy in 0 s | healthy 0 s after the fault ended, status agreed 6 s later; longest outage read 0 s / write 0 s; 0 of 100 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 0 s | healthy 0 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 0 s; 0 of 106 acknowledged batches lost |
| operator | `operator-down-during-failure` | Scale the operator to zero, delete a server StatefulSet, bring the operator back. Targets: healthy within 180 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100)<br>correctness 100, healthy in 33 s | healthy 33 s after the fault ended, status agreed 62 s later; longest outage read 0 s / write 0 s; 0 of 149 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 12 s | healthy 12 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 0 s; 0 of 55 acknowledged batches lost |
| infrastructure | `node-drain` | Local volumes pin pods to their node, so evicted replicas wait until the node returns. The other replicas must keep serving. Targets: healthy within 180 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100)<br>correctness 100, healthy in 13 s | healthy 13 s after the fault ended, status agreed 6 s later; longest outage read 0 s / write 0 s; 0 of 119 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 12 s | healthy 12 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 0 s; 0 of 99 acknowledged batches lost |
| pods | `server-pod-kill` | Force-delete one replica's pod. The StatefulSet brings it back; queries keep working on the other replica. Targets: healthy within 90 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100)<br>correctness 100, healthy in 28 s | healthy 28 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 0 s; 0 of 38 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 14 s | healthy 14 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 0 s; 0 of 33 acknowledged batches lost |
| drift | `server-statefulset-deleted` | The operator must notice the missing StatefulSet and recreate it. Targets: healthy within 120 s, read outage up to 10 s, write outage up to 20 s, no row lost. | FAIL (0)<br>correctness 0, never healthy | never got back to healthy: stalled: no pod, StatefulSet or status change for 181s while unhealthy (3/4 servers, 3/3 keepers Ready); status reported healthy while hosts were down: reported reconciled for 126s while only 3/4 servers were Ready; longest outage read 0 s / write 0 s | PASS (100)<br>correctness 100, healthy in 51 s | healthy 51 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 0 s; 0 of 59 acknowledged batches lost |
| pods | `all-servers-kill` | Full ClickHouse outage with Keeper intact. Targets: healthy within 180 s, no row lost. | PASS (100)<br>correctness 100, healthy in 14 s | healthy 14 s after the fault ended, status agreed 6 s later; longest outage read 10 s / write 10 s; 0 of 21 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 28 s | healthy 28 s after the fault ended, status agreed 7 s later; longest outage read 12 s / write 11 s; 0 of 26 acknowledged batches lost |
| keeper | `keeper-quorum-loss` | Kill 2 of 3 members. Replicated tables go read-only until quorum returns; nothing may be lost. Targets: healthy within 150 s, read outage up to 10 s, no row lost. | PASS (100)<br>correctness 100, healthy in 17 s | healthy 17 s after the fault ended, status agreed 6 s later; longest outage read 0 s / write 1 s; 0 of 29 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 18 s | healthy 18 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 0 s; 0 of 25 acknowledged batches lost |
| drift | `query-service-deleted` | Clients lose their entry point until the operator recreates the Service. Targets: healthy within 120 s, no row lost. | FAIL (0)<br>correctness 0, healthy in 301 s | a deleted object was not recreated: Service clickhouse-chaos was not recreated; recovered, but slower than the target: 301s, target 120s; healthy 301 s after the fault ended, status agreed 6 s later; longest outage read 263 s / write 263 s; 0 of 9 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 1 s | healthy 1 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 0 s; 0 of 32 acknowledged batches lost |
| storage | `replica-volume-lost` | Delete a replica's PVC and pod. The replica must come back with its schema and data. Targets: healthy within 240 s, read outage up to 10 s, write outage up to 20 s, no row lost. | FAIL (0)<br>correctness 0, healthy in 17 s | a replica could not be read: chi-chaos-main-0-1-0: count failed: Database chaos does not exist. (UNKNOWN_DATABASE) (query: SELECT count() FROM cha; a replica could not read the stream table: chi-chaos-main-0-1-0: cannot read the stream table: Database chaos does not exist. (UNKNOWN_DATABASE) (query: SELECT intDiv(id - 2000; healthy 17 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 0 s; 0 of 33 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 35 s | healthy 35 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 0 s; 0 of 46 acknowledged batches lost |
| network | `replica-network-isolation` | Deny all ingress and egress for one ClickHouse pod for 60 s; queries must route to the other replica. Targets: healthy within 120 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100)<br>correctness 100, healthy in 0 s | healthy 0 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 0 s; 0 of 68 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 0 s | healthy 0 s after the fault ended, status agreed 7 s later; longest outage read 0 s / write 0 s; 0 of 69 acknowledged batches lost |
| lifecycle | `cluster-deleted-foreground` | kubectl delete --cascade=foreground must complete: the operator has to stop recreating children of an object that is being deleted. | PASS (100)<br>correctness 100, never healthy | longest outage read 36 s / write 36 s | PASS (100)<br>correctness 100, never healthy | longest outage read 25 s / write 25 s |
| lifecycle | `new-replica-published-before-schema` | Add a replica and watch, every second, whether its address appears among the client Service's ready endpoints before the replica actually holds the tables. Targets: healthy within 400 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100)<br>correctness 100, healthy in 104 s | healthy 104 s after the fault ended, status agreed 74 s later; longest outage read 0 s / write 0 s; 0 of 170 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 17 s | healthy 17 s after the fault ended, status agreed 152 s later; longest outage read 0 s / write 0 s; 0 of 161 acknowledged batches lost |
| lifecycle | `new-replica-schema-blocked` | Add a replica while its traffic to Keeper is blocked, so creating replicated tables cannot succeed. The operator must not report the cluster healthy with that host in it. Then unblock and let it converge. Targets: healthy within 450 s, no row lost. | PASS (100)<br>correctness 100, healthy in 87 s | healthy 87 s after the fault ended, status agreed 50 s later; longest outage read 0 s / write 0 s; 0 of 298 acknowledged batches lost | PASS (100)<br>correctness 100, healthy in 7 s | healthy 7 s after the fault ended, status agreed 112 s later; longest outage read 0 s / write 0 s; 0 of 280 acknowledged batches lost |

### Causes behind each finding

Read from the operator's log after the injection (on an image built from `tracing/` where the stock
log doesn't say enough), from the source at the release tag, and from targeted reproductions.

Operator defects:

- Altinity, `server-statefulset-deleted`, `query-service-deleted`, `configmaps-deleted-then-restart`,
  `replica-volume-lost`: the CHI controller's informers log a deleted StatefulSet, Service or
  ConfigMap with `enqueued=false`, and the 60 s resync builds an action plan with
  `hasActionsToDo=false` because the spec didn't change. No reconcile runs, so nothing is
  recreated. A replica whose PVC was deleted comes back from its StatefulSet on an empty volume
  and never gets its schema (`Database chaos does not exist`).
- Altinity, `invalid-spec-no-damage`: the update the API server rejects goes to
  `onUpdateFailure=Recreate`. The operator deletes the Keeper StatefulSet, then the ClickHouse one,
  and recreating each fails with the same validation error for about 100 s.
- Altinity, `scale-down-shard-then-up`: the replica drops for the removed shard run on a host of
  that same shard while it's still up, so its Keeper registration survives. The re-added shard's
  replication lag grows (201 to 361) and never catches up.
- Altinity, `wedged-shutdown-recreate`: when the scale-to-0 wait times out the operator
  force-deletes the Keeper pod, which removes the API object while the container keeps running
  its preStop. The node then runs two members with the same server id; the leader keeps
  counting the old one (`zk_synced_followers 2`) and the replacement stays a candidate with an
  empty log until the old container exits, 16 minutes later in the reproduction. The operator
  aborts the reconcile in the meantime.
- Altinity Keeper resource with an empty `labels: {}` or `annotations: {}` on its pod template:
  the copy saved in `status.normalizedCompleted` drops the empty maps, so every reconcile sees a
  spec change and requeues after 5 s (44 reconciles in 90 s, status stuck `InProgress`).
- Altinity, `cluster-deleted-while-operator-down`: the Keeper PVCs survive deleting the Keeper
  resource even with the operator running and `reclaimPolicy: Delete` set on its volume template.
  Server PVCs carry the reclaim-policy label and are deleted; Keeper PVCs carry none, and the CHK
  controller has its `pvcDeleter` commented out.
- ClickHouse operator, `scale-down-replica-metadata`: its scaling guide documents dropping each
  Replicated database replica on scale-down; no table-level `SYSTEM DROP REPLICA` is issued, so
  the removed replica stays registered in Keeper and counted in `system.replicas`.
- ClickHouse operator, `storage-class-change`: the operator updates existing PVCs in place, which
  the API server rejects (a PVC's class can't change after creation), and keeps the existing
  StatefulSet's volume templates. The change is never applied and the status stays
  `ConfigurationChanged` with no error.

Documented behaviour, reported but not a defect:

- ClickHouse operator, `scale-down-shard-then-up`: PVCs are never deleted, so a re-added shard
  comes back with the removed shard's rows. The docs say PVCs are kept and can be reused.
- ClickHouse operator, `cluster-deleted-while-operator-down`: the docs say PVCs are not deleted
  on cluster deletion, so leftover volumes are reported as retained, not orphaned.

ClickHouse behaviour, measured but not charged to either operator:

- A ClickHouse server that starts while no Keeper is reachable keeps its replicated tables
  read-only until its own restart thread retries, about a minute after Keeper is back
  (`is_readonly=1 is_session_expired=1` for 65 s in the reproduction). Altinity reports the pod
  Ready during that window. The data check waits it out and records
  `replica_readonly_after_healthy_s`.

Harness artifacts found and removed:

- Placement: without anti-affinity the scheduler stacked a whole shard, or every Keeper member, on
  one node, so node faults measured placement. Clusters now have three nodes and each operator's
  own setting spreads replicas and Keepers (`podDistribution: ShardAntiAffinity`,
  `topologyZoneKey: kubernetes.io/hostname`); with it, Altinity `node-stop` passes.
- Host overload: alongside other local clusters, too many parallel clusters drove load past 5 per
  CPU and API calls timed out. A host monitor now holds each scenario until there is headroom and
  marks any scenario that ran on an overloaded host INVALID instead of scoring it.
- A status that turns healthy after the pods is lag, not a wrong status; it is measured as
  `status_lag_after_ready_s`.

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

A scenario runs 1 to 6 minutes including its own fresh baseline, and most of that is the
operator creating the cluster, not the failure. Waits poll every second and end on an event: the
cluster healthy for the scenario's stability window, a held fault that stopped changing anything,
or a stall. Three options spread the work:

- `--concurrency N`: scenarios that stay inside their own namespace run N at a time on one
  cluster; the ones that touch the operator, a node or cluster-wide API load run alone afterwards.
- `--clusters N`: one operator's scenarios are split over N k3d clusters, longest first, so the
  ones that must run alone run side by side.
- `matrix --parallel-targets`: every operator build at once.

The full suite (44 scenarios) on both operators with `--parallel-targets --clusters 5` takes about
40 minutes, against several hours one scenario at a time. `--repeat N` runs every scenario N times
under ids `scenario#k`, which shows placement-dependent results (node faults) as split verdicts.

**Host limits.** Every k3d node runs a kubelet that needs inotify instances. If other local
clusters are running, a new node's kubelet can fail with `inotify_init: too many open files` and
never register. The platform stops after 300 s with that hint. Raise
`fs.inotify.max_user_instances` or use fewer `--agents`.

## Tracing an operator

`tracing/altinity/build.sh` and `tracing/clickhouse/build.sh` apply a patch to a release and build
an image whose decision points log one `CHAOSTRACE <event> key=value ...` line each: informer
events and whether they enqueued, the reconcile decision and its spec diff, StatefulSet create,
update and delete outcomes, replica drops, schema plans and Keeper quorum checks. Run the image as
a target with `"image": "clickhouse-operator:trace-0.27.4"` and each scenario gets
`<scenario>.trace.log`, cut from the injection on and filtered to its namespace, next to the full
operator log. Some Altinity lines name only a host such as `0-1`, not the namespace, and are in the
full log only; run traced targets with `--concurrency 1` so the full log covers one scenario.

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
