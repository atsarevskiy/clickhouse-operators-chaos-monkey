"""The contract every operator adapter implements.

Scenarios and checks only talk to this interface. Anything operator-specific (custom resource
shape, labels, status fields, install method) lives in an adapter, so adding an operator means
adding one file.
"""

from __future__ import annotations

import abc

from ..cluster import K3dCluster
import time

from ..kube import Kube, pod_ready, wait_until
from ..model import ClusterSpec, ClusterState, Finding


class OperatorAdapter(abc.ABC):
    #: short id used on the command line, e.g. "altinity"
    name: str = ""
    #: features a scenario may depend on. A scenario whose requirement an operator lacks is
    #: reported SKIPPED with the reason instead of failing on an unsupported trigger.
    #:   prestop          pod template accepts a container lifecycle preStop hook
    #:   volume-recreate  changing the data volume template deletes and recreates StatefulSets
    #:   pod-labels       arbitrary pod labels can be set
    capabilities: frozenset[str] = frozenset()
    #: resource kinds (kubectl names) of the operator's custom resources, for cleanup
    cr_kinds: list[str] = []
    #: name of the operator Deployment as installed, used to bring it back if a scenario stopped it
    default_deployment: str = ""
    #: container names inside the pods the operator creates
    server_container: str = "clickhouse"
    keeper_container: str = "clickhouse-keeper"

    def __init__(self, cluster: K3dCluster, version: str, image: str | None = None):
        self.cluster = cluster
        self.kube: Kube = cluster.kube
        self.version = version
        self.image_override = image

    # ---------- install ----------

    @property
    @abc.abstractmethod
    def operator_namespace(self) -> str: ...

    @property
    @abc.abstractmethod
    def operator_selector(self) -> str:
        """Label selector for the operator pod(s)."""

    @property
    @abc.abstractmethod
    def operator_username(self) -> str:
        """The API username the operator acts as, for reading its requests from the audit log."""

    @abc.abstractmethod
    def images(self) -> list[str]:
        """Operator images to import into the cluster before installing."""

    @abc.abstractmethod
    def install(self, env: dict[str, str] | None = None) -> None:
        """Install (or reinstall) the operator from scratch and wait until it's running."""

    @abc.abstractmethod
    def uninstall(self) -> None: ...

    # ---------- clusters ----------

    @abc.abstractmethod
    def render(self, spec: ClusterSpec) -> list[dict]:
        """Custom resources (and any helper objects) for spec, as JSON-ready dicts."""

    def apply(self, spec: ClusterSpec) -> None:
        self.kube.apply(self.render(spec), namespace=spec.namespace)

    @abc.abstractmethod
    def delete_cluster(self, spec: ClusterSpec) -> None: ...

    @abc.abstractmethod
    def state(self, spec: ClusterSpec) -> ClusterState:
        """The state the operator reports on its custom resources."""

    @abc.abstractmethod
    def reconcile_marker(self, spec: ClusterSpec) -> str:
        """An opaque value that changes when the operator starts a new reconcile."""

    @abc.abstractmethod
    def server_selector(self, spec: ClusterSpec, shard: int | None = None, replica: int | None = None) -> str: ...

    @abc.abstractmethod
    def keeper_selector(self, spec: ClusterSpec) -> str: ...

    @abc.abstractmethod
    def cluster_name(self, spec: ClusterSpec) -> str:
        """The remote_servers cluster name the operator configures, for Distributed tables."""

    @abc.abstractmethod
    def query_service(self, spec: ClusterSpec) -> str:
        """A Service name that load-balances client queries across the ClickHouse hosts."""

    @abc.abstractmethod
    def server_replica_label(self, spec: ClusterSpec, replica: int) -> str:
        """A single label=value selecting every server pod of one replica index."""

    def ready_endpoint_ips(self, spec: ClusterSpec) -> set[str]:
        """Pod IPs currently published as ready by the client Service."""
        ips: set[str] = set()
        svc = self.query_service(spec)
        for slice_ in self.kube.items("endpointslices", spec.namespace,
                                      f"kubernetes.io/service-name={svc}"):
            for ep in slice_.get("endpoints", []) or []:
                if (ep.get("conditions") or {}).get("ready") is not False:
                    ips.update(ep.get("addresses") or [])
        return ips

    @abc.abstractmethod
    def keeper_hosts(self, spec: ClusterSpec) -> list[str]:
        """Per-member DNS names that answer the Keeper client port, for the quorum probe."""

    @property
    def keeper_port(self) -> int:
        return 2181

    #: database engine for the workload's database. Atomic: tables are created on every replica
    #: and a new replica gets them from the operator. Replicated: DDL runs once and the database
    #: engine carries it to every replica.
    workload_database_engine: str = "Atomic"
    #: the operator documents that deleting a cluster keeps its volumes, so leftover PVCs are policy
    keeps_pvcs_on_delete: bool = False
    #: credentials the adapter provisions for the test workload
    workload_user: str = "chaos"
    workload_password: str = "chaos"

    def verify_operator_state(self, spec: ClusterSpec) -> list[Finding]:
        """Operator-specific consistency checks on top of the generic invariants."""
        return []

    # ---------- helpers shared by all adapters ----------

    def server_pods(self, spec: ClusterSpec, shard: int | None = None, replica: int | None = None) -> list[dict]:
        pods = self.kube.items("pods", spec.namespace, self.server_selector(spec, shard, replica))
        return sorted(pods, key=lambda p: p["metadata"]["name"])

    def keeper_pods(self, spec: ClusterSpec) -> list[dict]:
        pods = self.kube.items("pods", spec.namespace, self.keeper_selector(spec))
        return sorted(pods, key=lambda p: p["metadata"]["name"])

    def ready_servers(self, spec: ClusterSpec) -> int:
        return sum(1 for p in self.server_pods(spec) if pod_ready(p))

    def ready_keepers(self, spec: ClusterSpec) -> int:
        return sum(1 for p in self.keeper_pods(spec) if pod_ready(p))

    def operator_pods(self) -> list[dict]:
        return self.kube.items("pods", self.operator_namespace, self.operator_selector)

    def operator_deployment(self) -> str:
        """Name of the Deployment that owns the operator pod, found through its ReplicaSet."""
        for pod in self.operator_pods():
            for ref in pod["metadata"].get("ownerReferences", []):
                if ref["kind"] == "ReplicaSet":
                    rs = self.kube.get("replicaset", ref["name"], self.operator_namespace) or {}
                    for owner in rs.get("metadata", {}).get("ownerReferences", []):
                        if owner["kind"] == "Deployment":
                            return owner["name"]
        raise RuntimeError("operator Deployment not found")

    def kill_operator(self, force: bool = True) -> None:
        self.kube.delete("pods", namespace=self.operator_namespace, selector=self.operator_selector,
                         grace=0 if force else None, force=force)

    def wait_operator_ready(self, timeout: int = 180) -> bool:
        return wait_until(lambda: any(pod_ready(p) for p in self.operator_pods()), timeout) is not None

    def start_log_capture(self, path) -> None:
        """Follow the operator's log into a file for the whole run, re-attaching when the pod is
        replaced, so a scenario's window can be read back even after rotation or a restart."""
        import subprocess
        import threading

        def follow() -> None:
            with open(path, "a") as out:
                while not getattr(self, "_capture_stop", False):
                    pods = self.operator_pods()
                    if not pods:
                        time.sleep(2)
                        continue
                    name = pods[0]["metadata"]["name"]
                    proc = subprocess.Popen(["kubectl", "--context", self.cluster.context, "logs", "-f",
                                             "-n", self.operator_namespace, name, "--since=1s"],
                                            stdout=out, stderr=subprocess.DEVNULL)
                    self._capture_proc = proc
                    proc.wait()
                    time.sleep(1)

        self._capture_stop = False
        self._capture_path = path
        threading.Thread(target=follow, daemon=True, name=f"{self.name}-log").start()

    def stop_log_capture(self) -> None:
        self._capture_stop = True
        proc = getattr(self, "_capture_proc", None)
        if proc:
            proc.terminate()

    def captured_logs(self, since_rfc: str) -> str | None:
        """Lines of the followed log from since_rfc on, or None when no capture is running."""
        path = getattr(self, "_capture_path", None)
        if not path:
            return None
        keep, out = False, []
        with open(path, errors="replace") as f:
            for line in f:
                if not keep:
                    stamp = line[:20]
                    if stamp[:4].isdigit() and stamp >= since_rfc:
                        keep = True
                    elif line[:1] in "IWE" and line[1:5].isdigit():
                        # klog lines (Altinity): Lmmdd hh:mm:ss.uuuuuu; compare the time of day
                        keep = line[6:14] >= since_rfc[11:19] and line[1:5] == since_rfc[5:7] + since_rfc[8:10]
                if keep:
                    out.append(line)
        return "".join(out)

    def operator_logs(self, since_time: str | None = None) -> str:
        out = []
        for pod in self.operator_pods():
            out.append(self.kube.logs(self.operator_namespace, pod["metadata"]["name"], since_time=since_time))
        return "\n".join(out)

    def sql(self, spec: ClusterSpec, pod: str, query: str, timeout: int = 60) -> tuple[int, str]:
        return self.kube.exec(spec.namespace, pod, self.server_container,
                              ["clickhouse-client", "--user", self.workload_user,
                               "--password", self.workload_password, "-q", query], timeout=timeout)
