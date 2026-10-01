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

Altinity operator 0.27.4 and ClickHouse operator 0.0.8, ClickHouse and Keeper 26.8, 2 shards x 2 replicas plus 3 Keepers on k3d with one agent node, full suite, 2026-10-01. `node-stop`, `node-drain`, `replica-volume-lost` and five ClickHouse operator scenarios were run twice; both verdicts are shown. Eleven scenarios whose verdict depended on the status checks were rerun after those changed, and their results replace the full run's. Regenerate with `python3 -m chaosmonkey report <run dirs...> --readme-table`.

| Operator | Score | PASS | DEGRADED | FAIL | SKIPPED | drift | infrastructure | keeper | lifecycle | metadata | network | operator | pods | spec | storage |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| altinity 0.27.4 | **77/100** | 30 | 5 | 9 | 0 | 25 | 38 | 100 | 88 | 50 | 100 | 88 | 100 | 86 | 0 |
| clickhouse 0.0.8 | **88/100** | 32 | 10 | 1 | 3 | 100 | 62 | 100 | 88 | 25 | 100 | 100 | 100 | 82 | 100 |

Score per scenario: PASS 100, DEGRADED 50, FAIL 0; the operator score averages the resilience scenarios it ran (performance and SKIPPED excluded), a repeated scenario counting once with the average of its runs. PASS/DEGRADED/FAIL/SKIPPED count runs.

| Category | Scenario | Expected | altinity 0.27.4 | What happened (altinity) | clickhouse 0.0.8 | What happened (clickhouse) |
|---|---|---|---|---|---|---|
| pods | `server-pod-kill` | Force-delete one replica's pod. The StatefulSet brings it back; queries keep working on the other replica. Targets: healthy within 90 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100) | healthy 13 s after the fault ended; longest outage read 4 s / write 1 s; 0 of 28 acknowledged batches lost | PASS (100) | healthy 11 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 30 acknowledged batches lost |
| keeper | `keeper-member-kill` | Quorum holds with 2 of 3; writes pause only while sessions move. Targets: healthy within 90 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100) | healthy 16 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 31 acknowledged batches lost | PASS (100) | healthy 17 s after the fault ended; longest outage read 0 s / write 1 s; 0 of 25 acknowledged batches lost |
| operator | `operator-kill-idle` | A restarted operator must not touch a healthy cluster: no pod restarts, status stays healthy. Targets: healthy within 60 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100) | healthy 0 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 96 acknowledged batches lost | PASS (100) | healthy 0 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 104 acknowledged batches lost |
| spec | `rolling-restart` | Change a pod annotation. Hosts must roll one replica at a time with queries still served. Targets: healthy within 420 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100) | healthy 279 s after the fault ended; longest outage read 1 s / write 1 s; 0 of 263 acknowledged batches lost | PASS (100) | healthy 174 s after the fault ended; longest outage read 0 s / write 1 s; 0 of 152 acknowledged batches lost |
| drift | `server-statefulset-deleted` | The operator must notice the missing StatefulSet and recreate it. Targets: healthy within 120 s, read outage up to 10 s, write outage up to 20 s, no row lost. | FAIL (0) | never got back to healthy: stalled: no pod, StatefulSet or status change for 181s while unhealthy (3/4 servers, 3/3 keepers Ready); status reported healthy while hosts were down: reported reconciled for 117s while only 3/4 servers were Ready; clients saw an outage: longest read outage 17s, limit 10s | PASS (100) | healthy 34 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 39 acknowledged batches lost |
| pods | `shard-all-replicas-kill` | The shard has no serving replica until the pods restart. Data must survive. Targets: healthy within 120 s, no row lost. | PASS (100) | healthy 14 s after the fault ended; longest outage read 2 s / write 0 s; 0 of 30 acknowledged batches lost | PASS (100) | healthy 15 s after the fault ended; longest outage read 3 s / write 1 s; 0 of 26 acknowledged batches lost |
| pods | `all-servers-kill` | Full ClickHouse outage with Keeper intact. Targets: healthy within 180 s, no row lost. | PASS (100) | healthy 13 s after the fault ended; longest outage read 11 s / write 10 s; 0 of 20 acknowledged batches lost | PASS (100) | healthy 16 s after the fault ended; longest outage read 12 s / write 13 s; 0 of 16 acknowledged batches lost |
| pods | `server-process-crash` | SIGKILL the server process inside its container; kubelet restarts the container without the pod being replaced. Targets: healthy within 60 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100) | healthy 1 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 34 acknowledged batches lost | PASS (100) | healthy 1 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 31 acknowledged batches lost |
| keeper | `keeper-quorum-loss` | Kill 2 of 3 members. Replicated tables go read-only until quorum returns; nothing may be lost. Targets: healthy within 150 s, read outage up to 10 s, no row lost. | PASS (100) | healthy 11 s after the fault ended; longest outage read 0 s / write 1 s; 0 of 29 acknowledged batches lost | PASS (100) | healthy 13 s after the fault ended; longest outage read 0 s / write 1 s; 0 of 26 acknowledged batches lost |
| keeper | `keeper-all-kill` | Full coordination outage; the ensemble must re-form from its persisted state. Targets: healthy within 180 s, read outage up to 10 s, no row lost. | PASS (100) | healthy 12 s after the fault ended; longest outage read 0 s / write 3 s; 0 of 22 acknowledged batches lost | PASS (100) | healthy 14 s after the fault ended; longest outage read 0 s / write 2 s; 0 of 24 acknowledged batches lost |
| operator | `operator-kill-mid-rollout` | The new operator pod must finish the roll and report the cluster healthy. Targets: healthy within 360 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100) | healthy 260 s after the fault ended; longest outage read 0 s / write 1 s; 0 of 250 acknowledged batches lost | PASS (100) | healthy 170 s after the fault ended; longest outage read 1 s / write 0 s; 0 of 158 acknowledged batches lost |
| operator | `operator-down-during-failure` | Scale the operator to zero, delete a server StatefulSet, bring the operator back. Targets: healthy within 180 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100) | healthy 30 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 146 acknowledged batches lost | PASS (100) | healthy 14 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 50 acknowledged batches lost |
| drift | `query-service-deleted` | Clients lose their entry point until the operator recreates the Service. Targets: healthy within 120 s, no row lost. | FAIL (0) | a deleted object was not recreated: Service clickhouse-chaos was not recreated; recovered, but slower than the target: 301s, target 120s; healthy 301 s after the fault ended; longest outage read 255 s / write 256 s; 0 of 9 acknowledged batches lost | PASS (100) | healthy 1 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 29 acknowledged batches lost |
| drift | `configmaps-deleted-then-restart` | A pod restarted after its config was deleted can only start if the operator restores it. Targets: healthy within 180 s, read outage up to 10 s, write outage up to 20 s, no row lost. | FAIL (0) | never got back to healthy: stalled: no pod, StatefulSet or status change for 182s while unhealthy (3/4 servers, 3/3 keepers Ready); status reported healthy while hosts were down: reported reconciled for 102s while only 3/4 servers were Ready; longest outage read 0 s / write 0 s | PASS (100) | healthy 25 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 35 acknowledged batches lost |
| spec | `scale-up-replica` | New replicas must get the schema and replicate the existing data. Targets: healthy within 300 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100) | healthy 111 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 141 acknowledged batches lost | PASS (100) / PASS (100) | 2 runs; worst: healthy 15 s after the fault ended, status agreed 213 s later; longest outage read 0 s / write 0 s; 0 of 211 acknowledged batches lost |
| spec | `scale-up-shard` | The new shard must come up with the schema so writes to it work. Targets: healthy within 300 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100) | healthy 119 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 126 acknowledged batches lost | PASS (100) | healthy 14 s after the fault ended, status agreed 198 s later; longest outage read 0 s / write 0 s; 0 of 199 acknowledged batches lost |
| spec | `invalid-spec-no-damage` | A pod label value Kubernetes refuses. The operator must not destroy running hosts trying to apply it. Targets: healthy within 300 s, read outage up to 10 s, write outage up to 20 s, no row lost. | FAIL (0) | destroyed a host applying a rejected spec: 1 of 4 server StatefulSets deleted while applying a rejected spec; healthy 48 s after the fault ended; longest outage read 0 s / write 1 s; 0 of 136 acknowledged batches lost | PASS (100) | healthy 0 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 57 acknowledged batches lost |
| spec | `bad-config-rollout` | Apply a server setting ClickHouse rejects at startup, watch the blast radius, then revert. Targets: healthy within 300 s, no row lost. | PASS (100) | healthy 1 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 106 acknowledged batches lost | PASS (100) | healthy 15 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 169 acknowledged batches lost |
| network | `replica-keeper-partition` | A NetworkPolicy blocks one ClickHouse pod's traffic to Keeper for 60 s. Targets: healthy within 120 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100) | healthy 0 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 69 acknowledged batches lost | PASS (100) | healthy 0 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 68 acknowledged batches lost |
| network | `replica-network-isolation` | Deny all ingress and egress for one ClickHouse pod for 60 s; queries must route to the other replica. Targets: healthy within 120 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100) | healthy 0 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 68 acknowledged batches lost | PASS (100) | healthy 0 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 64 acknowledged batches lost |
| spec | `scale-down-replica` | Remaining replicas keep the data, and the removed replica is dropped from replication metadata. Targets: healthy within 300 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100) | healthy 133 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 176 acknowledged batches lost | DEGRADED (50) / DEGRADED (50) | 2 runs; worst: removed replica still registered: replication metadata still lists 3 replicas, expected 2; healthy 33 s after the fault ended, status agreed 169 s later; longest outage read 0 s / write 0 s; 0 of 189 acknowledged batches lost |
| spec | `server-version-upgrade` | Rolling upgrade clickhouse/clickhouse-server:26.3 -> clickhouse/clickhouse-server:26.8 with queries running. Targets: healthy within 480 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100) | healthy 247 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 241 acknowledged batches lost | DEGRADED (50) | clients saw an outage: read availability 86.5% below 95.0% over 89 samples and write availability 89.9% below 90.0% over 89 samples; healthy 119 s after the fault ended; 0 of 102 acknowledged batches lost |
| spec | `recreate-on-immutable-change` | Switching the data volume claim template forces every StatefulSet to be deleted and recreated. Replicas come back on empty volumes and must regain schema and data from their peers. Targets: healthy within 480 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100) | healthy 310 s after the fault ended; longest outage read 0 s / write 1 s; 0 of 275 acknowledged batches lost | SKIPPED | not run: the trigger needs volume-recreate, which this operator lacks |
| spec | `wedged-shutdown-recreate` | Pods sleep in preStop far longer than the operator waits. A recreate must still finish without leaving a host at zero replicas. Targets: healthy within 480 s, read outage up to 10 s, write outage up to 20 s, no row lost. | FAIL (0) | never got back to healthy: stalled: no pod, StatefulSet or status change for 182s while unhealthy (4/4 servers, 2/3 keepers Ready); longest outage read 0 s / write 0 s | SKIPPED | not run: the trigger needs prestop, volume-recreate, which this operator lacks |
| spec | `stuck-terminating-pod` | Hold one replica's pod in Terminating with a finalizer, then roll the pod template. The operator must not take down the stuck pod's peer, and must finish once it's released. Targets: healthy within 420 s, no row lost. | PASS (100) | healthy 199 s after the fault ended; longest outage read 0 s / write 1 s; 0 of 268 acknowledged batches lost | PASS (100) | healthy 12 s after the fault ended; longest outage read 0 s / write 1 s; 0 of 188 acknowledged batches lost |
| spec | `storage-class-change` | Switch the volume claim to a second StorageClass with the same provisioner. Data must survive, no shard may lose every replica, and the status must end up telling the truth. Targets: healthy within 420 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100) | healthy 267 s after the fault ended; longest outage read 0 s / write 1 s; 0 of 268 acknowledged batches lost | DEGRADED (50) / DEGRADED (50) | 2 runs; worst: status never reported healthy after every pod was Ready: operator still reports ClickHouse rollout pending, ConfigurationInSync=ConfigurationChanged / Keeper rollout pending, ConfigurationInSync=ConfigurationChanged 300s after every pod was Ready; healthy 122 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 433 acknowledged batches lost |
| drift | `keeper-statefulset-deleted` | The operator must notice the missing StatefulSet and recreate it, without a spec change. Targets: healthy within 120 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100) | healthy 15 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 32 acknowledged batches lost | PASS (100) | healthy 17 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 32 acknowledged batches lost |
| lifecycle | `cluster-deleted-foreground` | kubectl delete --cascade=foreground must complete: the operator has to stop recreating children of an object that is being deleted. | PASS (100) | longest outage read 34 s / write 33 s | PASS (100) | longest outage read 23 s / write 23 s |
| lifecycle | `cluster-deleted-while-operator-down` | A delete requested with no operator running must complete once the operator returns, and must not leave orphaned StatefulSets, Services or PVCs behind. | DEGRADED (50) | left objects behind after deletion: pvc left behind: data-a-chk-chaos-keeper-main-0-0-0, data-a-chk-chaos-keeper-main-0-1-0, data-a-chk-chaos-keeper-main-0-2-0; longest outage read 32 s / write 35 s | DEGRADED (50) | left objects behind after deletion: pvc left behind: clickhouse-storage-volume-chaos-clickhouse-0-0-0, clickhouse-storage-volume-chaos-clickhouse-0-1-0, clickhouse-storage-volume-chaos-clickhouse-1-0-0, clickhouse-storage-volume-chaos-clickhouse-1-1-0,...; longest outage read 33 s / write 33 s |
| lifecycle | `new-replica-published-before-schema` | Add a replica and watch, every second, whether its address appears among the client Service's ready endpoints before the replica actually holds the tables. Targets: healthy within 400 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100) | healthy 163 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 178 acknowledged batches lost | PASS (100) | healthy 17 s after the fault ended, status agreed 211 s later; longest outage read 0 s / write 0 s; 0 of 213 acknowledged batches lost |
| metadata | `scale-down-replica-metadata` | Scale a shard down by one replica and check the survivors no longer count the removed replica, and that its Keeper registration is gone. A long Keeper session timeout makes the removed replica look active while the operator tries to drop it. Targets: healthy within 300 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100) | healthy 72 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 122 acknowledged batches lost | FAIL (0) | removed replica still registered: system.replicas still counts 3 replicas, expected 2; removed replica still registered in Keeper: 3 replicas still registered under the table path: 0, 1, 2; healthy 29 s after the fault ended, status agreed 117 s later; longest outage read 0 s / write 0 s; 0 of 141 acknowledged batches lost |
| keeper | `keeper-rolling-restart` | Change the Keeper pod template and measure how long the ensemble runs with no leader and how long writes are refused. Targets: healthy within 420 s, read outage up to 10 s, write outage up to 30 s, no row lost. | PASS (100) | healthy 58 s after the fault ended; longest outage read 0 s / write 1 s; 0 of 55 acknowledged batches lost | PASS (100) | healthy 51 s after the fault ended; longest outage read 0 s / write 1 s; 0 of 53 acknowledged batches lost |
| lifecycle | `new-replica-schema-blocked` | Add a replica while its traffic to Keeper is blocked, so creating replicated tables cannot succeed. The operator must not report the cluster healthy with that host in it. Then unblock and let it converge. Targets: healthy within 450 s, no row lost. | PASS (100) | healthy 120 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 288 acknowledged batches lost | PASS (100) / PASS (100) | 2 runs; worst: healthy 101 s after the fault ended, status agreed 91 s later; longest outage read 0 s / write 0 s; 0 of 350 acknowledged batches lost |
| spec | `config-change-during-slow-start` | One replica is restarted with a slow start, and a config change arrives while it is still coming up. The operator must not keep killing it, and the cluster must converge. Targets: healthy within 600 s, no row lost. | PASS (100) | healthy 163 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 155 acknowledged batches lost | DEGRADED (50) | recovered, but slower than the target: 770s, target 600s; healthy 770 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 667 acknowledged batches lost |
| metadata | `scale-down-shard-then-up` | Remove a shard, then add it back. The new shard's replicas must create their tables without colliding with metadata the removed shard left behind. Targets: healthy within 600 s. | FAIL (0) | never got back to healthy: stalled: no pod, StatefulSet or status change for 182s while unhealthy (3/4 servers, 3/3 keepers Ready); longest outage read 3 s / write 1 s | DEGRADED (50) | old data came back with a re-added shard: chaos-clickhouse-1-0-0 (shard 1) has 1200 rows, expected 200 and chaos-clickhouse-1-1-0 (shard 1) has 1200 rows, expected 200; healthy 14 s after the fault ended; longest outage read 47 s / write 1 s; 0 of 88 acknowledged batches lost |
| performance | `perf-rolling-restart` | Time and API cost of rolling every host once. Targets: healthy within 900 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100) | healthy 378 s after the fault ended; longest outage read 0 s / write 2 s; 0 of 352 acknowledged batches lost | PASS (100) | healthy 322 s after the fault ended; longest outage read 0 s / write 1 s; 0 of 289 acknowledged batches lost |
| performance | `perf-scale-out` | Time and API cost to add shards to a running cluster. Targets: healthy within 600 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100) | healthy 126 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 142 acknowledged batches lost | PASS (100) | healthy 17 s after the fault ended, status agreed 137 s later; longest outage read 0 s / write 0 s; 0 of 149 acknowledged batches lost |
| performance | `perf-operator-restart` | API requests a freshly started operator makes against a healthy cluster in its first 2 minutes. Targets: healthy within 120 s, read outage up to 10 s, write outage up to 20 s, no row lost. | PASS (100) | healthy 1 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 123 acknowledged batches lost | PASS (100) | healthy 1 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 128 acknowledged batches lost |
| operator | `operator-kill-mid-keeper-repair` | Delete one Keeper member's StatefulSet, kill the operator as soon as it's recreated. Targets: healthy within 120 s, read outage up to 10 s, write outage up to 20 s, no row lost. | DEGRADED (50) | status never reported healthy after every pod was Ready: operator still reports CHI Completed / CHK InProgress 300s after every pod was Ready; healthy 15 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 296 acknowledged batches lost | PASS (100) | healthy 20 s after the fault ended; longest outage read 0 s / write 1 s; 0 of 27 acknowledged batches lost |
| storage | `replica-volume-lost` | Delete a replica's PVC and pod. The replica must come back with its schema and data. Targets: healthy within 240 s, read outage up to 10 s, write outage up to 20 s, no row lost. | FAIL (0) / FAIL (0) | 2 runs; worst: a replica could not be read: chi-chaos-main-0-1-0: count failed: Database chaos does not exist. (UNKNOWN_DATABASE) (query: SELECT count() FROM cha; a replica could not read the stream table: chi-chaos-main-0-1-0: cannot read the stream table: Database chaos does not exist. (UNKNOWN_DATABASE) (query: SELECT intDiv(id - 2000; healthy 19 s after the fault ended, status agreed 7 s later; longest outage read 1 s / write 3 s; 0 of 32 acknowledged batches lost | PASS (100) | healthy 38 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 48 acknowledged batches lost |
| spec | `unschedulable-replacement` | Force a recreate whose new volume references a missing StorageClass. The operator must stop after the first failed host instead of taking down a shard or the Keeper quorum. Targets: healthy within 480 s, no row lost. | PASS (100) | healthy 41 s after the fault ended, status agreed 60 s later; longest outage read 0 s / write 1 s; 0 of 170 acknowledged batches lost | SKIPPED | not run: the trigger needs volume-recreate, which this operator lacks |
| infrastructure | `node-drain` | Local volumes pin pods to their node, so evicted replicas wait until the node returns. The other replicas must keep serving. Targets: healthy within 180 s, read outage up to 10 s, write outage up to 20 s, no row lost. | DEGRADED (50) / DEGRADED (50) | 2 runs; worst: clients saw an outage: longest read outage 75s, limit 10s and write availability 57.3% below 90.0% over 89 samples; healthy 14 s after the fault ended, status agreed 6 s later; 0 of 63 acknowledged batches lost | DEGRADED (50) | clients saw an outage: longest read outage 110s, limit 10s and longest write outage 110s, limit 20s; healthy 37 s after the fault ended; 0 of 13 acknowledged batches lost |
| spec | `unschedulable-rollout` | Add a nodeSelector no node matches. The operator must stop after the first replacement stays Pending instead of taking down a shard or the Keeper quorum, then recover on revert. Targets: healthy within 420 s, no row lost. | PASS (100) | healthy 135 s after the fault ended, status agreed 6 s later; longest outage read 9 s / write 1 s; 0 of 208 acknowledged batches lost | PASS (100) | healthy 30 s after the fault ended; longest outage read 0 s / write 0 s; 0 of 103 acknowledged batches lost |
| infrastructure | `node-stop` | Hard node loss for 90 s, then the node returns. Targets: healthy within 300 s, read outage up to 10 s, write outage up to 20 s, no row lost. | FAIL (0) / DEGRADED (50) | 2 runs; worst: a replica did not catch up on replication: chi-chaos-main-0-1-0 (shard 0) has 1000 rows, expected 1200: SYNC REPLICA failed (); is_readonly=1 is_session_expired=1 queue_size=0 last_queue_update_exception= and chi-chaos-main-1-1-0 (shard 1) has 1000 rows, expected 1200: SYNC REPLICA failed (); is_readonly=1 is_session_expired=1 queue_size=0 last_queue_update_exception= and replicas of the same shard differ and could not sync: chi-chaos-main-0-1-0 vs chi-chaos-main-0-0-0 (chi-chaos-main-0-1-0: SYNC REPLICA failed (); is_readonly=1 is_session_expired=1 queue_size=0 last_queue_update_excep...; healthy 16 s after the fault ended, status agreed 6 s later; longest outage read 8 s / write 7 s; 0 of 17 acknowledged batches lost | DEGRADED (50) / PASS (100) | 2 runs; worst: clients saw an outage: longest write outage 25s, limit 20s; healthy 18 s after the fault ended, status agreed 7 s later; 0 of 17 acknowledged batches lost |

### Causes confirmed from traced builds

Read from the operator's log after the injection, on an image built from `tracing/` where the
stock log doesn't say enough, and from `system.replicas` for `node-stop`.

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
- Altinity Keeper resource with an empty `labels: {}` or `annotations: {}` on its pod template:
  the copy saved in `status.normalizedCompleted` drops the empty maps, so every reconcile sees a
  spec change and requeues after 5 s (44 reconciles in 90 s, status stuck `InProgress`).
- Altinity, `node-stop`: no acknowledged batch is lost. When the stopped node holds a replica of
  each shard, those replicas come back Ready but read-only with an expired Keeper session
  (`NO_ZOOKEEPER`) and keep serving a partial copy. Whether a run hits this depends on placement.
- ClickHouse operator, `scale-down-replica-metadata`: cleanup drops the replica from the
  Replicated database but issues no table-level `SYSTEM DROP REPLICA`, so Keeper still lists it.
- ClickHouse operator, `scale-down-shard-then-up`: the removed shard's PVCs are kept and reused when
  the shard comes back, so its old rows return.
- ClickHouse operator, `storage-class-change`: the operator updates existing PVCs in place, which
  the API server rejects (a PVC's class can't change after creation), and keeps the existing
  StatefulSet's volume templates. The change is never applied and the status stays
  `ConfigurationChanged` with no error.

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
