"""Test data and live probes, driven only through the adapter interface.

    setup()    ReplicatedMergeTree table on every server + a Distributed table over the cluster
    write()    known rows into each shard, remembered per shard
    verify()   every replica of every shard holds exactly the rows written to it
    probes     a query pod (read + write through the cluster Service every second) and a Keeper
               pod (members serving as leader/follower every second), logs parsed afterwards
"""

from __future__ import annotations

import time

from .kube import pod_ready, wait_until
from .model import ClusterSpec, Finding
from .operators.base import OperatorAdapter

DB = "chaos"
LOCAL = f"{DB}.events"
DIST = f"{DB}.events_all"
# The availability probe writes to its own table so it never changes the counts verify() checks.
PROBE_LOCAL = f"{DB}.probe"
PROBE_DIST = f"{DB}.probe_all"
# The ingest stream: a batch of STREAM_BATCH rows every second, ids STREAM_BASE + seq * 1000 + n,
# so afterwards every batch the client saw acknowledged can be looked up by its sequence number.
STREAM_LOCAL = f"{DB}.stream"
STREAM_DIST = f"{DB}.stream_all"
STREAM_BASE = 2_000_000_000_000
STREAM_BATCH = 100


class Workload:
    def __init__(self, op: OperatorAdapter, spec: ClusterSpec):
        self.op = op
        self.spec = spec
        self.expected: dict[int, int] = {s: 0 for s in range(spec.shards)}
        self.next_id = 0
        #: what extra rows mean; a scenario that removes and re-adds a shard calls it resurrection
        self.surplus_label = "data duplication"

    # ---------- data ----------

    def _ready_pod(self, shard: int) -> str | None:
        for pod in self.op.server_pods(self.spec, shard=shard):
            if pod_ready(pod):
                return pod["metadata"]["name"]
        return None

    def table_count(self, pod: str) -> int | None:
        rc, out = self.op.sql(self.spec, pod, f"SELECT count() FROM system.tables WHERE database = '{DB}'")
        return int(out.strip()) if rc == 0 and out.strip().isdigit() else None

    def total_replicas(self, pod: str) -> int | None:
        rc, out = self.op.sql(self.spec, pod, "SELECT max(total_replicas) FROM system.replicas "
                                             f"WHERE database = '{DB}' AND table = 'events'")
        return int(out.strip()) if rc == 0 and out.strip().isdigit() else None

    def zk_children(self, pod: str, path: str) -> list[str] | None:
        """Children of a Keeper path as ClickHouse sees them, or None if the query failed."""
        rc, out = self.op.sql(self.spec, pod, f"SELECT name FROM system.zookeeper WHERE path = '{path}'")
        if rc != 0:
            return None
        return [x for x in out.strip().splitlines() if x]

    def table_zk_path(self, pod: str) -> str | None:
        rc, out = self.op.sql(self.spec, pod, "SELECT zookeeper_path FROM system.replicas "
                                             f"WHERE database = '{DB}' AND table = 'events' LIMIT 1")
        return out.strip() or None if rc == 0 else None

    def setup(self) -> list[Finding]:
        findings = []
        engine = self.op.workload_database_engine
        replicated_db = engine.startswith("Replicated")
        tables = [
            f"CREATE TABLE IF NOT EXISTS {LOCAL} (id UInt64, shard UInt8, ts DateTime DEFAULT now()) "
            f"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{DB}/events', '{{replica}}') ORDER BY id",
            f"CREATE TABLE IF NOT EXISTS {DIST} AS {LOCAL} "
            f"ENGINE = Distributed('{self.op.cluster_name(self.spec)}', {DB}, events, shard)",
            f"CREATE TABLE IF NOT EXISTS {PROBE_LOCAL} AS {LOCAL} "
            f"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{DB}/probe', '{{replica}}') ORDER BY id",
            f"CREATE TABLE IF NOT EXISTS {PROBE_DIST} AS {LOCAL} "
            f"ENGINE = Distributed('{self.op.cluster_name(self.spec)}', {DB}, probe, shard)",
            f"CREATE TABLE IF NOT EXISTS {STREAM_LOCAL} AS {LOCAL} "
            f"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{DB}/stream', '{{replica}}') ORDER BY id",
            f"CREATE TABLE IF NOT EXISTS {STREAM_DIST} AS {LOCAL} "
            f"ENGINE = Distributed('{self.op.cluster_name(self.spec)}', {DB}, stream, shard)",
        ]
        if replicated_db:
            # Replicated database: the engine carries DDL to every replica, and it rejects
            # explicit replication paths, so tables use its defaults.
            tables = [q.replace("ReplicatedMergeTree('/clickhouse/tables/{shard}/chaos/events', '{replica}')",
                                "ReplicatedMergeTree")
                       .replace("ReplicatedMergeTree('/clickhouse/tables/{shard}/chaos/probe', '{replica}')",
                                "ReplicatedMergeTree")
                       .replace("ReplicatedMergeTree('/clickhouse/tables/{shard}/chaos/stream', '{replica}')",
                                "ReplicatedMergeTree") for q in tables]
        pods = [p["metadata"]["name"] for p in self.op.server_pods(self.spec)]
        for name in pods:
            rc, out = self.op.sql(self.spec, name, f"CREATE DATABASE IF NOT EXISTS {DB} ENGINE = {engine}")
            if rc != 0:
                findings.append(Finding("fail", "workload setup", f"{name}: {out.strip()[:300]}"))
        targets = pods[:1] if replicated_db else pods
        for name in targets:
            for q in tables:
                rc, out = self.op.sql(self.spec, name, q)
                if rc != 0:
                    findings.append(Finding("fail", "workload setup", f"{name}: {out.strip()[:300]}"))
                    break
        if replicated_db:
            ok = wait_until(lambda: all((self.table_count(n) or 0) >= 6 for n in pods), timeout=120)
            if ok is None:
                findings.append(Finding("fail", "workload setup", "Replicated database did not reach every replica"))
        return findings

    def write(self, rows_per_shard: int = 1000) -> list[Finding]:
        findings = []
        for shard in range(self.spec.shards):
            pod = self._ready_pod(shard)
            if not pod:
                findings.append(Finding("fail", "workload write", f"shard {shard} has no Ready replica"))
                continue
            start = self.next_id
            rc, out = self.op.sql(self.spec, pod,
                                  f"INSERT INTO {LOCAL} (id, shard) SELECT {start} + number, {shard} "
                                  f"FROM numbers({rows_per_shard})")
            if rc != 0:
                findings.append(Finding("fail", "workload write", f"shard {shard} via {pod}: {out.strip()[:300]}"))
                continue
            self.next_id += rows_per_shard
            self.expected[shard] = self.expected.get(shard, 0) + rows_per_shard
        return findings

    def _sync(self, name: str, table: str, timeout: int) -> str | None:
        """SYSTEM SYNC REPLICA; None once the replica has caught up, else why it has not. A replica
        that cannot sync still has its rows on a peer, so a short count is lag, not loss."""
        rc, out = self.op.sql(self.spec, name, f"SYSTEM SYNC REPLICA {table}", timeout=timeout)
        if rc == 0:
            return None
        db, tbl = table.split(".")
        _, state = self.op.sql(self.spec, name,
                               f"SELECT is_readonly, is_session_expired, queue_size, last_queue_update_exception "
                               f"FROM system.replicas WHERE database = '{db}' AND table = '{tbl}' FORMAT TSKV")
        return f"SYNC REPLICA failed ({out.strip()[:120]}); {state.strip()[:200]}"

    def verify(self, sync_timeout: int = 120) -> list[Finding]:
        """Every replica of every shard must hold exactly the rows written to that shard."""
        findings = []
        for shard in range(self.spec.shards):
            for pod in self.op.server_pods(self.spec, shard=shard):
                name = pod["metadata"]["name"]
                if not pod_ready(pod):
                    findings.append(Finding("fail", "data", f"{name} not Ready, cannot verify its data"))
                    continue
                unsynced = self._sync(name, LOCAL, sync_timeout)
                rc, out = self.op.sql(self.spec, name, f"SELECT count() FROM {LOCAL}")
                if rc != 0:
                    findings.append(Finding("fail", "data", f"{name}: count failed: {out.strip()[:200]}"))
                    continue
                got = int(out.strip() or 0)
                want = self.expected.get(shard, 0)
                if got < want and unsynced:
                    findings.append(Finding("fail", "replica not caught up",
                                            f"{name} (shard {shard}) has {got} rows, expected {want}: {unsynced}"))
                elif got < want:
                    findings.append(Finding("fail", "data loss", f"{name} (shard {shard}) has {got} rows, expected {want}"))
                elif got > want:
                    findings.append(Finding("warn", self.surplus_label,
                                            f"{name} (shard {shard}) has {got} rows, expected {want}"))
        return findings

    # ---------- probes ----------

    def start_probes(self) -> None:
        ns, image = self.spec.namespace, self.spec.server_image
        svc = self.op.query_service(self.spec)
        user, pw = self.op.workload_user, self.op.workload_password
        client = f"clickhouse-client -h {svc} --user {user} --password {pw} --connect_timeout 1 --receive_timeout 3 --send_timeout 3"
        ms = "$(($(date +%s%N)/1000000))"
        query_loop = (
            "i=0; while true; do i=$((i+1)); "
            f"t0={ms}; if {client} -q 'SELECT count() FROM {DIST}' >/dev/null 2>&1; then r=ok; else r=fail; fi; t1={ms}; "
            f"if {client} --insert_distributed_sync 1 -q \"INSERT INTO {PROBE_DIST} (id, shard) VALUES ($((1000000000+i)), $((i % {self.spec.shards})))\" >/dev/null 2>&1; then w=ok; else w=fail; fi; "
            "echo \"$(date +%s) read=$r write=$w read_ms=$((t1-t0))\"; sleep 1; done"
        )
        # One batch per second. A batch counts as acknowledged only when the client got success
        # from a synchronous distributed insert; the check afterwards looks each one up.
        # The sequence number is the wall clock in milliseconds, so a writer that gets restarted
        # (its node was stopped, say) can never reuse a number and fake a duplicate.
        stream_loop = (
            f"while true; do s={ms}; "
            f"t0={ms}; if {client} --insert_distributed_sync 1 -q \"INSERT INTO {STREAM_DIST} (id, shard) "
            f"SELECT {STREAM_BASE} + $s * 1000 + number, $((s % {self.spec.shards})) FROM numbers({STREAM_BATCH})\" "
            f">/dev/null 2>&1; then a=ok; else a=fail; fi; t1={ms}; "
            "echo \"$(date +%s) seq=$s ack=$a ms=$((t1-t0))\"; sleep 1; done"
        )
        hosts = " ".join(self.op.keeper_hosts(self.spec))
        port = self.op.keeper_port
        keeper_loop = (
            "while true; do s=0; m=''; "
            f"for h in {hosts}; do x=$(echo srvr | nc -w1 $h {port} 2>/dev/null | sed -n 's/^Mode: //p'); "
            "[ -n \"$x\" ] && [ \"$x\" != observer ] && s=$((s+1)); m=\"$m ${x:-down}\"; done; "
            "echo \"$(date +%s) serving=$s modes=$m\"; sleep 1; done"
        )
        pods = [
            self._probe_pod("query-probe", image, query_loop),
            self._probe_pod("keeper-probe", self.spec.keeper_image, keeper_loop),
            self._probe_pod("ingest-stream", image, stream_loop),
        ]
        self.op.kube.apply(pods, namespace=ns)
        wait_until(lambda: all(pod_ready(p) for p in self.op.kube.items("pods", ns, "chaosmonkey=probe"))
                   and len(self.op.kube.items("pods", ns, "chaosmonkey=probe")) == len(pods), timeout=120)

    @staticmethod
    def _probe_pod(name: str, image: str, loop: str) -> dict:
        return {
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": name, "labels": {"chaosmonkey": "probe"}},
            "spec": {
                "terminationGracePeriodSeconds": 0,
                # On the control plane, which no scenario stops or drains, so a probe measures the
                # cluster instead of dying with the node under test.
                "nodeSelector": {"node-role.kubernetes.io/control-plane": "true"},
                "tolerations": [{"operator": "Exists"}],
                "containers": [{"name": "probe", "image": image, "imagePullPolicy": "IfNotPresent",
                                "command": ["sh", "-c", loop],
                                "resources": {"requests": {"cpu": "20m", "memory": "32Mi"}}}],
            },
        }

    def _probe_lines(self, pod: str, since: float) -> list[list[str]]:
        out = self.op.kube.logs(self.spec.namespace, pod)
        rows = []
        for line in out.splitlines():
            parts = line.split()
            if parts and parts[0].isdigit() and int(parts[0]) >= since:
                rows.append(parts)
        return rows

    def query_availability(self, since: float, until: float | None = None) -> dict:
        rows = [r for r in self._probe_lines("query-probe", since) if until is None or int(r[0]) <= until]
        def stats(key: str) -> tuple[float | None, int]:
            flags = [dict(p.split("=", 1) for p in r[1:]).get(key) == "ok" for r in rows]
            if not flags:
                return None, 0
            longest = run = 0
            for ok in flags:
                run = 0 if ok else run + 1
                longest = max(longest, run)
            return 100.0 * sum(flags) / len(flags), longest
        read_pct, read_gap = stats("read")
        write_pct, write_gap = stats("write")
        return {"samples": len(rows), "read_pct": read_pct, "read_longest_outage_s": read_gap,
                "write_pct": write_pct, "write_longest_outage_s": write_gap}

    @staticmethod
    def _pct(values: list[float], q: float) -> float | None:
        if not values:
            return None
        values = sorted(values)
        return values[min(len(values) - 1, int(q * (len(values) - 1) + 0.5))]

    def stream_stats(self, since: float, until: float) -> dict:
        """Latency and throughput of the ingest stream between since and until."""
        rows = [dict(p.split("=", 1) for p in r[1:]) | {"t": int(r[0])}
                for r in self._probe_lines("ingest-stream", 0) if since <= int(r[0]) <= until]
        acked = [r for r in rows if r.get("ack") == "ok"]
        lat = [float(r["ms"]) for r in acked if r.get("ms", "").lstrip("-").isdigit()]
        # rows acknowledged per 10 s window; the slowest window shows the dip in throughput
        windows: dict[int, int] = {}
        for r in rows:
            windows.setdefault(r["t"] // 10, 0)
            if r.get("ack") == "ok":
                windows[r["t"] // 10] += STREAM_BATCH
        # the first and last windows are partial, so they would read as a dip that isn't there
        if len(windows) > 2:
            for edge in (min(windows), max(windows)):
                windows.pop(edge, None)
        reads = [float(dict(p.split("=", 1) for p in r[1:]).get("read_ms", "nan"))
                 for r in self._probe_lines("query-probe", since) if int(r[0]) <= until]
        reads = [x for x in reads if x == x]
        return {
            "batches": len(rows), "batches_acked": len(acked),
            "insert_p50_ms": self._pct(lat, 0.5), "insert_p99_ms": self._pct(lat, 0.99),
            "insert_max_ms": max(lat) if lat else None,
            "read_p50_ms": self._pct(reads, 0.5), "read_p99_ms": self._pct(reads, 0.99),
            "min_rows_per_10s": min(windows.values()) if windows else None,
            "median_rows_per_10s": self._pct(list(windows.values()), 0.5),
        }

    def stream_integrity(self, sync_timeout: int = 120) -> tuple[dict, list[Finding]]:
        """Every batch the stream saw acknowledged must be stored exactly once, on every replica."""
        findings: list[Finding] = []
        acked, failed = set(), set()
        # Read the log, then stop the writer, so nothing lands while replicas are compared. A batch
        # can be in flight when the writer stops; it is logged by neither outcome, so everything
        # above the last logged sequence number is left out of the comparison.
        lines = self._probe_lines("ingest-stream", 0)
        self.op.kube.delete("pod", "ingest-stream", namespace=self.spec.namespace, grace=0, force=True)
        for r in lines:
            kv = dict(p.split("=", 1) for p in r[1:])
            seq = int(kv.get("seq", "0"))
            (acked if kv.get("ack") == "ok" else failed).add(seq)
        last = max(acked | failed, default=0)
        time.sleep(3)
        ready = [p["metadata"]["name"] for p in self.op.server_pods(self.spec) if pod_ready(p)]
        unsynced = {name: why for name in ready if (why := self._sync(name, STREAM_LOCAL, sync_timeout))}
        # per shard, per replica: count rows per batch on every replica so a replica that is
        # missing batches, or holds extras, is caught even if its peer is complete
        per_seq_best: dict[int, int] = {}
        diverged, lagging = [], []
        for shard in range(self.spec.shards):
            counts_by_replica = []
            for pod in self.op.server_pods(self.spec, shard=shard):
                if not pod_ready(pod):
                    continue
                name = pod["metadata"]["name"]
                rc, out = self.op.sql(self.spec, name,
                                      f"SELECT intDiv(id - {STREAM_BASE}, 1000) AS seq, count() FROM {STREAM_LOCAL} "
                                      f"WHERE id >= {STREAM_BASE} GROUP BY seq FORMAT TSV")
                if rc != 0:
                    findings.append(Finding("fail", "stream", f"{name}: cannot read the stream table: {out.strip()[:200]}"))
                    continue
                counts = {int(a): int(b) for a, b in (ln.split("\t") for ln in out.strip().splitlines() if ln)
                          if int(a) <= last}
                counts_by_replica.append((name, counts))
                for seq, n in counts.items():
                    per_seq_best[seq] = max(per_seq_best.get(seq, 0), n)
            if len(counts_by_replica) > 1:
                ref_name, ref = counts_by_replica[0]
                for name, c in counts_by_replica[1:]:
                    if c != ref:
                        behind = [n for n in (name, ref_name) if n in unsynced]
                        (lagging if behind else diverged).append(
                            f"{name} vs {ref_name}" + (f" ({behind[0]}: {unsynced[behind[0]]})" if behind else ""))
        lost = sorted(s for s in acked if per_seq_best.get(s, 0) < STREAM_BATCH)
        dup = sorted(s for s, n in per_seq_best.items() if n > STREAM_BATCH)
        landed_despite_error = sorted(s for s in failed if per_seq_best.get(s, 0) >= STREAM_BATCH)
        stats = {"batches_acked_total": len(acked), "batches_failed_total": len(failed),
                 "acked_batches_lost": len(lost), "batches_duplicated": len(dup),
                 "failed_batches_stored_anyway": len(landed_despite_error),
                 "replicas_diverged": len(diverged), "replicas_not_caught_up": len(lagging)}
        if lost:
            findings.append(Finding("fail", "acknowledged writes lost",
                                    f"{len(lost)} of {len(acked)} acknowledged batches missing or partial "
                                    f"(first seq {lost[:5]})"))
        if dup:
            findings.append(Finding("warn", "stream duplicates", f"{len(dup)} batches stored more than once (seq {dup[:5]})"))
        if diverged:
            findings.append(Finding("fail", "replica divergence",
                                    f"replicas of the same shard hold different stream data: {', '.join(diverged[:4])}"))
        if lagging:
            findings.append(Finding("fail", "replica not caught up",
                                    f"replicas of the same shard differ and could not sync: {'; '.join(lagging[:2])}"))
        return stats, findings

    def keeper_leaderless(self, since: float, until: float | None = None) -> dict:
        """Longest run of consecutive samples in which no member reported itself leader."""
        rows = [r for r in self._probe_lines("keeper-probe", since) if until is None or int(r[0]) <= until]
        longest = run = 0
        for r in rows:
            modes = " ".join(r[1:]).split("modes=")[-1].split()
            run = 0 if "leader" in modes else run + 1
            longest = max(longest, run)
        return {"samples": len(rows), "longest_leaderless_s": longest}

    def keeper_quorum(self, since: float, until: float | None = None) -> dict:
        quorum = self.spec.keepers // 2 + 1
        rows = [r for r in self._probe_lines("keeper-probe", since) if until is None or int(r[0]) <= until]
        longest = run = 0
        min_serving = None
        for r in rows:
            serving = int(r[1].split("=")[1])
            min_serving = serving if min_serving is None else min(min_serving, serving)
            run = run + 1 if serving < quorum else 0
            longest = max(longest, run)
        return {"samples": len(rows), "min_serving": min_serving, "quorum": quorum,
                "longest_below_quorum_s": longest}


def epoch() -> float:
    return time.time()
