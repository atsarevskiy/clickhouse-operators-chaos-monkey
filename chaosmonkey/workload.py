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


class Workload:
    def __init__(self, op: OperatorAdapter, spec: ClusterSpec):
        self.op = op
        self.spec = spec
        self.expected: dict[int, int] = {s: 0 for s in range(spec.shards)}
        self.next_id = 0

    # ---------- data ----------

    def _ready_pod(self, shard: int) -> str | None:
        for pod in self.op.server_pods(self.spec, shard=shard):
            if pod_ready(pod):
                return pod["metadata"]["name"]
        return None

    def setup(self) -> list[Finding]:
        findings = []
        ddl = [
            f"CREATE DATABASE IF NOT EXISTS {DB}",
            f"CREATE TABLE IF NOT EXISTS {LOCAL} (id UInt64, shard UInt8, ts DateTime DEFAULT now()) "
            f"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{DB}/events', '{{replica}}') ORDER BY id",
            f"CREATE TABLE IF NOT EXISTS {DIST} AS {LOCAL} "
            f"ENGINE = Distributed('{self.op.cluster_name(self.spec)}', {DB}, events, shard)",
            f"CREATE TABLE IF NOT EXISTS {PROBE_LOCAL} AS {LOCAL} "
            f"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{DB}/probe', '{{replica}}') ORDER BY id",
            f"CREATE TABLE IF NOT EXISTS {PROBE_DIST} AS {LOCAL} "
            f"ENGINE = Distributed('{self.op.cluster_name(self.spec)}', {DB}, probe, shard)",
        ]
        for pod in self.op.server_pods(self.spec):
            name = pod["metadata"]["name"]
            for q in ddl:
                rc, out = self.op.sql(self.spec, name, q)
                if rc != 0:
                    findings.append(Finding("fail", "workload setup", f"{name}: {out.strip()[:300]}"))
                    break
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

    def verify(self, sync_timeout: int = 120) -> list[Finding]:
        """Every replica of every shard must hold exactly the rows written to that shard."""
        findings = []
        for shard in range(self.spec.shards):
            for pod in self.op.server_pods(self.spec, shard=shard):
                name = pod["metadata"]["name"]
                if not pod_ready(pod):
                    findings.append(Finding("fail", "data", f"{name} not Ready, cannot verify its data"))
                    continue
                self.op.sql(self.spec, name, f"SYSTEM SYNC REPLICA {LOCAL}", timeout=sync_timeout)
                rc, out = self.op.sql(self.spec, name, f"SELECT count() FROM {LOCAL}")
                if rc != 0:
                    findings.append(Finding("fail", "data", f"{name}: count failed: {out.strip()[:200]}"))
                    continue
                got = int(out.strip() or 0)
                want = self.expected.get(shard, 0)
                if got < want:
                    findings.append(Finding("fail", "data loss", f"{name} (shard {shard}) has {got} rows, expected {want}"))
                elif got > want:
                    findings.append(Finding("warn", "data duplication", f"{name} (shard {shard}) has {got} rows, expected {want}"))
        return findings

    # ---------- probes ----------

    def start_probes(self) -> None:
        ns, image = self.spec.namespace, self.spec.server_image
        svc = self.op.query_service(self.spec)
        user, pw = self.op.workload_user, self.op.workload_password
        client = f"clickhouse-client -h {svc} --user {user} --password {pw} --connect_timeout 1 --receive_timeout 3 --send_timeout 3"
        query_loop = (
            "i=0; while true; do i=$((i+1)); "
            f"if {client} -q 'SELECT count() FROM {DIST}' >/dev/null 2>&1; then r=ok; else r=fail; fi; "
            f"if {client} --insert_distributed_sync 1 -q \"INSERT INTO {PROBE_DIST} (id, shard) VALUES ($((1000000000+i)), $((i % {self.spec.shards})))\" >/dev/null 2>&1; then w=ok; else w=fail; fi; "
            "echo \"$(date +%s) read=$r write=$w\"; sleep 1; done"
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
        ]
        self.op.kube.apply(pods, namespace=ns)
        wait_until(lambda: all(pod_ready(p) for p in self.op.kube.items("pods", ns, "chaosmonkey=probe"))
                   and len(self.op.kube.items("pods", ns, "chaosmonkey=probe")) == 2, timeout=120)

    @staticmethod
    def _probe_pod(name: str, image: str, loop: str) -> dict:
        return {
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": name, "labels": {"chaosmonkey": "probe"}},
            "spec": {
                "terminationGracePeriodSeconds": 0,
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
