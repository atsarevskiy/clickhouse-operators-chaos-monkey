"""Adapter for the ClickHouse Inc. operator (ClickHouseCluster / KeeperCluster, group clickhouse.com).

Installed from its Helm chart with webhooks and cert-manager disabled: both controllers run the
defaulting and validation code themselves, so only the update-time webhook checks are lost.
"""

from __future__ import annotations

import json
import subprocess

from ..model import ClusterSpec, ClusterState, Finding
from .base import OperatorAdapter

GROUP = "clickhouse.com"
CHC = "clickhouseclusters.clickhouse.com"
KC = "keeperclusters.clickhouse.com"
NAMESPACE = "clickhouse-operator-system"
RELEASE = "clickhouse-operator"

CH_CONDITIONS = ("Ready", "Healthy", "ClusterSizeAligned", "ConfigurationInSync", "SchemaInSync")
KEEPER_CONDITIONS = ("Ready", "Healthy", "ClusterSizeAligned", "ConfigurationInSync")


def _image(ref: str) -> dict:
    repo, _, tag = ref.rpartition(":")
    if "/" in tag or not repo:
        return {"repository": ref}
    return {"repository": repo, "tag": tag}


def _nested(flat: dict) -> dict:
    """Settings are written as slash paths ("zookeeper/session_timeout_ms") across adapters; this
    operator takes a nested config tree."""
    out: dict = {}
    for key, value in flat.items():
        node = out
        parts = key.split("/")
        for part in parts[:-1]:
            node = node.setdefault(part, {})
        node[parts[-1]] = value
    return out


def _version_tuple(v: str) -> tuple[int, ...]:
    return tuple(int(x) for x in v.lstrip("v").split(".") if x.isdigit())


class ClickHouseOperator(OperatorAdapter):
    name = "clickhouse"
    cr_kinds = [CHC, KC]
    default_deployment = "clickhouse-operator-controller-manager"
    server_container = "clickhouse-server"
    keeper_container = "clickhouse-keeper"
    # no container lifecycle field; volume template changes are applied to PVCs in place,
    # so StatefulSets are never deleted and recreated for them
    capabilities = frozenset({"pod-labels"})
    tuning: dict = {}
    # This operator creates and syncs schema for Replicated databases on new replicas; a plain
    # Atomic database is left to the user, so the workload uses the engine the operator manages.
    workload_database_engine = "Replicated('/clickhouse/databases/chaos', '{shard}', '{replica}')"

    @property
    def tag(self) -> str:
        return self.version if self.version.startswith("v") else f"v{self.version}"

    @property
    def operator_namespace(self) -> str:
        return NAMESPACE

    @property
    def operator_selector(self) -> str:
        return "app.kubernetes.io/name=clickhouse-operator,control-plane=controller-manager"

    @property
    def operator_username(self) -> str:
        return f"system:serviceaccount:{NAMESPACE}:clickhouse-operator-controller-manager"

    @property
    def operator_image(self) -> str:
        return self.image_override or f"ghcr.io/clickhouse/clickhouse-operator:{self.tag}"

    def images(self) -> list[str]:
        return [self.operator_image]

    def install(self, env: dict[str, str] | None = None) -> None:
        self.cluster.import_images(self.images())
        repo, _, tag = self.operator_image.rpartition(":")
        chart = (f"https://github.com/ClickHouse/clickhouse-operator/releases/download/{self.tag}/"
                 f"clickhouse-operator-helm-{self.tag.lstrip('v')}.tgz")
        args = ["helm", "--kube-context", self.cluster.context, "upgrade", "--install", RELEASE, chart,
                "-n", NAMESPACE, "--create-namespace", "--wait", "--timeout", "5m",
                "--set", "webhook.enabled=false", "--set", "certManager.enabled=false",
                "--set", "metrics.secure=false",
                "--set", f"manager.image.repository={repo}", "--set", f"manager.image.tag={tag}",
                "--set", "manager.image.pullPolicy=IfNotPresent"]
        # the update checker calls out to the internet; it only affects a status condition
        if _version_tuple(self.tag) >= (0, 0, 8):
            args += ["--set", "manager.envOverrides.DISABLE_VERSION_UPDATE_CHECKS=true"]
        else:
            args += ["--set", "manager.args={--leader-elect,--disable-version-update-checks}"]
        for k, v in (env or {}).items():
            args += ["--set", f"manager.envOverrides.{k}={v}"]
        subprocess.run(args, check=True, capture_output=True, text=True)
        self.kube.run("-n", NAMESPACE, "rollout", "restart", f"deployment/{self.default_deployment}")
        self.kube.run("-n", NAMESPACE, "rollout", "status", f"deployment/{self.default_deployment}",
                      "--timeout=240s", timeout=260)

    def uninstall(self) -> None:
        subprocess.run(["helm", "--kube-context", self.cluster.context, "uninstall", RELEASE, "-n", NAMESPACE],
                       capture_output=True, text=True)

    # ---------- rendering ----------

    def _pvc(self, spec: ClusterSpec) -> dict:
        s: dict = {"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": spec.storage}}}
        sc = spec.volume_b_storage_class if spec.volume_variant == "b" else spec.storage_class
        if sc:
            s["storageClassName"] = sc
        return s

    def _pod_template(self, spec: ClusterSpec, image: str) -> dict:
        pt: dict = {}
        if spec.spread_replicas:
            # replicas of one shard, and the Keeper members, balanced across nodes
            pt["topologyZoneKey"] = "kubernetes.io/hostname"
        if spec.node_selector:
            pt["nodeSelector"] = dict(spec.node_selector)
        if spec.termination_grace is not None:
            pt["terminationGracePeriodSeconds"] = spec.termination_grace
        if spec.init_sleep:
            pt["initContainers"] = [{"name": "slow-start", "image": image, "command": ["sleep", str(spec.init_sleep)]}]
        return pt

    def render(self, spec: ClusterSpec) -> list[dict]:
        common_meta = {}
        if spec.pod_labels:
            common_meta["labels"] = spec.pod_labels
        if spec.pod_annotations:
            common_meta["annotations"] = spec.pod_annotations
        keeper = {
            "apiVersion": f"{GROUP}/v1alpha1", "kind": "KeeperCluster",
            "metadata": {"name": spec.name, "namespace": spec.namespace},
            "spec": {
                "replicas": spec.keepers,
                **common_meta,
                "podTemplate": self._pod_template(spec, spec.keeper_image),
                "containerTemplate": {
                    "image": _image(spec.keeper_image), "imagePullPolicy": "IfNotPresent",
                    "resources": {"requests": {"cpu": "50m", "memory": "200Mi"}, "limits": {"memory": "512Mi"}},
                },
                "dataVolumeClaimSpec": self._pvc(spec),
                "settings": {"extraConfig": _nested({"keeper_server/four_letter_word_white_list": "*",
                                                     **spec.keeper_settings})},
            },
        }
        chc = {
            "apiVersion": f"{GROUP}/v1alpha1", "kind": "ClickHouseCluster",
            "metadata": {"name": spec.name, "namespace": spec.namespace},
            "spec": {
                "shards": spec.shards, "replicas": spec.replicas,
                "keeperClusterRef": {"name": spec.name},
                **common_meta,
                "podTemplate": self._pod_template(spec, spec.server_image),
                "containerTemplate": {
                    "image": _image(spec.server_image), "imagePullPolicy": "IfNotPresent",
                    "resources": {"requests": {"cpu": "50m", "memory": "200Mi"},
                                  "limits": {"memory": spec.server_memory_limit}},
                },
                "dataVolumeClaimSpec": self._pvc(spec),
                "settings": {
                    "extraConfig": _nested(spec.server_settings),
                    "extraUsersConfig": {"users": {self.workload_user: {
                        "password": self.workload_password, "networks": {"ip": "::/0"}, "profile": "default"}}},
                },
            },
        }
        return [keeper, chc]

    def delete_cluster(self, spec: ClusterSpec) -> None:
        self.kube.delete(CHC, spec.name, namespace=spec.namespace)
        self.kube.delete(KC, spec.name, namespace=spec.namespace)

    # ---------- observing ----------

    def _obj(self, kind: str, spec: ClusterSpec) -> dict:
        return self.kube.get(kind, spec.name, spec.namespace) or {}

    @staticmethod
    def _conditions(obj: dict) -> dict[str, dict]:
        return {c["type"]: c for c in obj.get("status", {}).get("conditions", []) or []}

    def _converged(self, obj: dict, expected: int, required: tuple[str, ...]) -> tuple[bool, str]:
        st = obj.get("status", {})
        conds = self._conditions(obj)
        problems = []
        if not obj:
            return False, "absent"
        if obj["metadata"].get("generation") != st.get("observedGeneration"):
            problems.append("generation not observed")
        if st.get("currentRevision") != st.get("updateRevision"):
            problems.append("rollout pending")
        if st.get("readyReplicas") != expected:
            problems.append(f"ready {st.get('readyReplicas')}/{expected}")
        for c in required:
            if conds.get(c, {}).get("status") != "True":
                problems.append(f"{c}={conds.get(c, {}).get('reason', 'missing')}")
        return not problems, ", ".join(problems) or "converged"

    def state(self, spec: ClusterSpec) -> ClusterState:
        chc, kc = self._obj(CHC, spec), self._obj(KC, spec)
        ok1, why1 = self._converged(chc, spec.hosts, CH_CONDITIONS)
        ok2, why2 = self._converged(kc, spec.keepers, KEEPER_CONDITIONS)
        return ClusterState(reconciled=ok1 and ok2, phase=f"ClickHouse {why1} / Keeper {why2}",
                            raw={"clickhouse": chc.get("status", {}), "keeper": kc.get("status", {})})

    def reconcile_marker(self, spec: ClusterSpec) -> str:
        out = []
        for kind in (CHC, KC):
            st = self._obj(kind, spec).get("status", {})
            out.append([st.get("observedGeneration"), st.get("updateRevision"), st.get("currentRevision")])
        return json.dumps(out)

    def server_selector(self, spec: ClusterSpec, shard: int | None = None, replica: int | None = None) -> str:
        sel = f"app={spec.name}-clickhouse"
        if shard is not None:
            sel += f",{GROUP}/shard-id={shard}"
        if replica is not None:
            sel += f",{GROUP}/replica-id={replica}"
        return sel

    def server_replica_label(self, spec: ClusterSpec, replica: int) -> str:
        return f"{GROUP}/replica-id={replica}"

    def keeper_selector(self, spec: ClusterSpec) -> str:
        return f"app={spec.name}-keeper"

    def cluster_name(self, spec: ClusterSpec) -> str:
        return "default"

    def query_service(self, spec: ClusterSpec) -> str:
        return f"{spec.name}-clickhouse-headless"

    def keeper_hosts(self, spec: ClusterSpec) -> list[str]:
        return [f"{spec.name}-keeper-{i}-0.{spec.name}-keeper-headless" for i in range(spec.keepers)]

    def verify_operator_state(self, spec: ClusterSpec) -> list[Finding]:
        findings = []
        for label, kind in (("ClickHouseCluster", CHC), ("KeeperCluster", KC)):
            conds = self._conditions(self._obj(kind, spec))
            rs = conds.get("ReconcileSucceeded", {})
            if rs.get("status") == "False":
                findings.append(Finding("warn", f"{label} ReconcileSucceeded",
                                        f"{rs.get('reason')}: {rs.get('message', '')[:200]}"))
            ss = conds.get("SchemaInSync", {})
            if ss and ss.get("status") == "False":
                findings.append(Finding("warn", f"{label} SchemaInSync", f"{ss.get('reason')}: {ss.get('message', '')[:200]}"))
            rsu = conds.get("ReplicaStartupSucceeded", {})
            if rsu.get("status") == "False":
                findings.append(Finding("warn", f"{label} ReplicaStartupSucceeded", f"{rsu.get('reason')}"))
        return findings
