"""Adapter for the Altinity clickhouse-operator (ClickHouseInstallation / ClickHouseKeeperInstallation)."""

from __future__ import annotations

import json
import urllib.request
from pathlib import Path

from ..kube import pod_ready, wait_until
from ..model import ClusterSpec, ClusterState, Finding
from .base import OperatorAdapter

CACHE = Path.home() / ".cache" / "chaosmonkey"
CHI_GROUP = "clickhouse.altinity.com"
CHK_GROUP = "clickhouse-keeper.altinity.com"
CLUSTER = "main"


class AltinityOperator(OperatorAdapter):
    name = "altinity"
    cr_kinds = ["clickhouseinstallations.clickhouse.altinity.com",
                "clickhousekeeperinstallations.clickhouse-keeper.altinity.com"]
    capabilities = frozenset({"prestop", "volume-recreate", "pod-labels"})
    default_deployment = "clickhouse-operator"
    server_container = "clickhouse"
    keeper_container = "clickhouse-keeper"

    #: operator settings applied through a ClickHouseOperatorConfiguration. Kept to the minimum a
    #: local run needs and reported with every result, so nothing is silently tuned.
    tuning = {
        "reconcile": {"statefulSet": {"update": {"timeout": 90, "pollInterval": 5}}},
    }

    @property
    def operator_namespace(self) -> str:
        return "kube-system"

    @property
    def operator_selector(self) -> str:
        return "app=clickhouse-operator"

    @property
    def operator_username(self) -> str:
        return "system:serviceaccount:kube-system:clickhouse-operator"

    @property
    def operator_image(self) -> str:
        return self.image_override or f"altinity/clickhouse-operator:{self.version}"

    def images(self) -> list[str]:
        exporter = f"altinity/metrics-exporter:{self.version}"
        return [exporter] + ([] if self.image_override else [self.operator_image])

    def _bundle(self) -> Path:
        CACHE.mkdir(parents=True, exist_ok=True)
        path = CACHE / f"altinity-bundle-{self.version}.yaml"
        if not path.exists():
            url = ("https://raw.githubusercontent.com/Altinity/clickhouse-operator/"
                   f"release-{self.version}/deploy/operator/clickhouse-operator-install-bundle.yaml")
            path.write_bytes(urllib.request.urlopen(url, timeout=60).read())
        return path

    def install(self, env: dict[str, str] | None = None) -> None:
        self.cluster.import_images(self.images())
        if self.image_override:
            self.cluster.import_images([self.operator_image])
        ns = self.operator_namespace
        # Start from the bundle's own Deployment: env set by an earlier run would survive a
        # server-side apply, since it leaves fields other managers own.
        self.kube.delete("deployment", "clickhouse-operator", namespace=ns, wait=True)
        self.kube.run("apply", "--server-side", "--force-conflicts", "-f", str(self._bundle()), timeout=300)
        self.kube.run("-n", ns, "set", "image", "deployment/clickhouse-operator",
                      f"clickhouse-operator={self.operator_image}")
        self.kube.patch("deployment", "clickhouse-operator", ns,
                        [{"op": "replace", "path": "/spec/template/spec/containers/0/imagePullPolicy",
                          "value": "IfNotPresent"}], patch_type="json")
        self.kube.delete("clickhouseoperatorconfigurations", namespace=ns, selector="chaosmonkey=tuning")
        self.kube.apply([{
            "apiVersion": f"{CHI_GROUP}/v1", "kind": "ClickHouseOperatorConfiguration",
            "metadata": {"name": "chaosmonkey-tuning", "namespace": ns, "labels": {"chaosmonkey": "tuning"}},
            "spec": self.tuning,
        }])
        if env:
            self.kube.run("-n", ns, "set", "env", "deployment/clickhouse-operator", "-c", "clickhouse-operator",
                          *[f"{k}={v}" for k, v in env.items()])
        self.kube.run("-n", ns, "rollout", "restart", "deployment/clickhouse-operator")
        self.kube.run("-n", ns, "rollout", "status", "deployment/clickhouse-operator", "--timeout=240s", timeout=260)

    def uninstall(self) -> None:
        self.kube.delete("deployment", "clickhouse-operator", namespace=self.operator_namespace, wait=True)

    # ---------- rendering ----------

    def _keeper_name(self, spec: ClusterSpec) -> str:
        return f"{spec.name}-keeper"

    def _volume_templates(self, spec: ClusterSpec) -> list[dict]:
        def vct(name: str, storage_class: str | None) -> dict:
            s = {"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": spec.storage}}}
            if storage_class:
                s["storageClassName"] = storage_class
            return {"name": name, "spec": s}
        return [vct("data-a", spec.storage_class),
                vct("data-b", spec.volume_b_storage_class or spec.storage_class)]

    def _pod_spec(self, spec: ClusterSpec, container: str, image: str, memory: str) -> dict:
        c: dict = {"name": container, "image": image,
                   "resources": {"requests": {"cpu": "50m", "memory": "200Mi"}, "limits": {"memory": memory}}}
        if spec.prestop_sleep:
            c["lifecycle"] = {"preStop": {"exec": {"command": ["sleep", str(spec.prestop_sleep)]}}}
        pod: dict = {"containers": [c]}
        if spec.termination_grace is not None:
            pod["terminationGracePeriodSeconds"] = spec.termination_grace
        if spec.init_sleep:
            pod["initContainers"] = [{"name": "slow-start", "image": image,
                                      "command": ["sleep", str(spec.init_sleep)]}]
        return pod

    def render(self, spec: ClusterSpec) -> list[dict]:
        keeper = self._keeper_name(spec)
        meta = {"labels": spec.pod_labels, "annotations": spec.pod_annotations}
        chk = {
            "apiVersion": f"{CHK_GROUP}/v1", "kind": "ClickHouseKeeperInstallation",
            "metadata": {"name": keeper, "namespace": spec.namespace},
            "spec": {
                "defaults": {"templates": {"podTemplate": "keeper",
                                           "dataVolumeClaimTemplate": f"data-{spec.volume_variant}"}},
                "configuration": {
                    "clusters": [{"name": CLUSTER, "layout": {"replicasCount": spec.keepers}}],
                    "settings": {"keeper_server/four_letter_word_white_list": "*", **spec.keeper_settings},
                },
                "templates": {
                    "podTemplates": [{"name": "keeper", "metadata": meta,
                                      "spec": self._pod_spec(spec, self.keeper_container, spec.keeper_image, "512Mi")}],
                    "volumeClaimTemplates": self._volume_templates(spec),
                },
            },
        }
        chi = {
            "apiVersion": f"{CHI_GROUP}/v1", "kind": "ClickHouseInstallation",
            "metadata": {"name": spec.name, "namespace": spec.namespace},
            "spec": {
                "defaults": {"templates": {"podTemplate": "server",
                                           "dataVolumeClaimTemplate": f"data-{spec.volume_variant}"}},
                "configuration": {
                    "zookeeper": {"nodes": [{"host": f"keeper-{keeper}.{spec.namespace}.svc.cluster.local",
                                             "port": 2181}]},
                    "users": {
                        f"{self.workload_user}/password": self.workload_password,
                        f"{self.workload_user}/networks/ip": ["::/0"],
                        f"{self.workload_user}/profile": "default",
                    },
                    "settings": spec.server_settings,
                    "clusters": [{"name": CLUSTER, "layout": {"shardsCount": spec.shards,
                                                              "replicasCount": spec.replicas}}],
                },
                "templates": {
                    "podTemplates": [{"name": "server", "metadata": meta,
                                      "spec": self._pod_spec(spec, self.server_container, spec.server_image,
                                                             spec.server_memory_limit)}],
                    "volumeClaimTemplates": self._volume_templates(spec),
                },
            },
        }
        return [chk, chi]

    def delete_cluster(self, spec: ClusterSpec) -> None:
        self.kube.delete("clickhouseinstallations.clickhouse.altinity.com", spec.name, namespace=spec.namespace)
        self.kube.delete("clickhousekeeperinstallations.clickhouse-keeper.altinity.com", self._keeper_name(spec), namespace=spec.namespace)

    # ---------- observing ----------

    def _chi(self, spec: ClusterSpec) -> dict:
        return self.kube.get("clickhouseinstallations.clickhouse.altinity.com", spec.name, spec.namespace) or {}

    def _chk(self, spec: ClusterSpec) -> dict:
        return self.kube.get("clickhousekeeperinstallations.clickhouse-keeper.altinity.com", self._keeper_name(spec), spec.namespace) or {}

    def state(self, spec: ClusterSpec) -> ClusterState:
        chi, chk = self._chi(spec).get("status", {}), self._chk(spec).get("status", {})
        s1, s2 = chi.get("status", "absent"), chk.get("status", "absent")
        return ClusterState(
            reconciled=(s1 == "Completed" and s2 == "Completed"),
            phase=f"CHI {s1} / CHK {s2}",
            message="; ".join(x for x in (chi.get("error"), chk.get("error")) if x),
            raw={"chi": {k: chi.get(k) for k in ("status", "hostsCompleted", "hosts", "error", "errors")},
                 "chk": {k: chk.get(k) for k in ("status", "error")}},
        )

    def reconcile_marker(self, spec: ClusterSpec) -> str:
        chi, chk = self._chi(spec).get("status", {}), self._chk(spec).get("status", {})
        return json.dumps([chi.get("taskIDsStarted"), chk.get("taskIDsStarted")])

    def server_selector(self, spec: ClusterSpec, shard: int | None = None, replica: int | None = None) -> str:
        sel = f"{CHI_GROUP}/chi={spec.name}"
        if shard is not None:
            sel += f",{CHI_GROUP}/shard={shard}"
        if replica is not None:
            sel += f",{CHI_GROUP}/replica={replica}"
        return sel

    def keeper_selector(self, spec: ClusterSpec) -> str:
        return f"{CHK_GROUP}/chk={self._keeper_name(spec)}"

    def cluster_name(self, spec: ClusterSpec) -> str:
        return CLUSTER

    def query_service(self, spec: ClusterSpec) -> str:
        return f"clickhouse-{spec.name}"

    def keeper_hosts(self, spec: ClusterSpec) -> list[str]:
        k = self._keeper_name(spec)
        return [f"chk-{k}-{CLUSTER}-0-{i}" for i in range(spec.keepers)]

    def verify_operator_state(self, spec: ClusterSpec) -> list[Finding]:
        # Task IDs can't be cross-checked: completion records a freshly generated "auto-" ID
        # rather than the one the reconcile started with, so the two lists never share entries.
        findings = []
        chi = self._chi(spec).get("status", {})
        if chi.get("status") == "Completed" and chi.get("hostsCompleted") not in (None, spec.hosts):
            findings.append(Finding("warn", "CHI host counters",
                                    f"Completed with hostsCompleted={chi.get('hostsCompleted')} of {spec.hosts}"))
        for kind, obj in (("CHI", self._chi(spec)), ("CHK", self._chk(spec))):
            st = obj.get("status", {})
            if st.get("status") == "Aborted":
                findings.append(Finding("warn", f"{kind} status", f"Aborted: {st.get('error', '')[:200]}"))
        return findings
