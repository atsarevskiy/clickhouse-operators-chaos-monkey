"""Local k3d cluster lifecycle: create, import images, read the API audit log, delete."""

from __future__ import annotations

import json
import subprocess
import tempfile
from pathlib import Path

from .kube import Kube, wait_until

AUDIT_LOG = "/var/lib/rancher/k3s/server/logs/audit.log"

# Every request made by any service account, at Metadata level (verb, resource, name, URI, code).
# Filtering by the operator's own service account happens at read time, so the policy doesn't
# need to know which namespace an operator installs into.
AUDIT_POLICY = """apiVersion: audit.k8s.io/v1
kind: Policy
omitStages: [RequestReceived]
rules:
  - level: None
    verbs: [watch]
  - level: Metadata
    userGroups: ["system:serviceaccounts"]
  - level: None
"""


class K3dCluster:
    def __init__(self, name: str, agents: int = 1, k3s_image: str = "rancher/k3s:v1.31.6-k3s1"):
        self.name = name
        self.agents = agents
        self.k3s_image = k3s_image
        self.context = f"k3d-{name}"
        self.kube = Kube(self.context)
        self._policy_dir = Path(tempfile.gettempdir()) / f"chaosmonkey-{name}"

    @property
    def server_node(self) -> str:
        return f"k3d-{self.name}-server-0"

    def exists(self) -> bool:
        out = subprocess.run(["k3d", "cluster", "list", "-o", "json"], capture_output=True, text=True).stdout
        return any(c.get("name") == self.name for c in json.loads(out or "[]"))

    def create(self) -> None:
        if self.exists():
            return
        self._policy_dir.mkdir(parents=True, exist_ok=True)
        policy = self._policy_dir / "audit-policy.yaml"
        policy.write_text(AUDIT_POLICY)
        cmd = [
            "k3d", "cluster", "create", self.name,
            "--image", self.k3s_image,
            "--agents", str(self.agents),
            "--no-lb", "--wait",
            "--volume", f"{policy}:/etc/chaosmonkey/audit-policy.yaml@server:0",
            "--k3s-arg", "--kube-apiserver-arg=audit-policy-file=/etc/chaosmonkey/audit-policy.yaml@server:0",
            "--k3s-arg", f"--kube-apiserver-arg=audit-log-path={AUDIT_LOG}@server:0",
            "--k3s-arg", "--kube-apiserver-arg=audit-log-maxsize=500@server:0",
            "--k3s-arg", "--disable=traefik@server:0",
            "--k3s-arg", "--disable=metrics-server@server:0",
        ]
        # --wait blocks until every node registers, and a node whose kubelet can't start never
        # does, so bound it and report the likely host-side cause instead of hanging.
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=300)
        except subprocess.TimeoutExpired:
            self.delete()
            raise RuntimeError(
                f"k3d cluster {self.name} did not become ready within 300s. A common cause is the host's "
                "inotify limit when several local clusters run at once (kubelet logs 'inotify_init: too many "
                "open files'); raise fs.inotify.max_user_instances or use fewer --agents.") from None
        if wait_until(lambda: len(self.ready_nodes()) == self.agents + 1, timeout=180) is None:
            raise RuntimeError(f"only {len(self.ready_nodes())} of {self.agents + 1} nodes Ready")

    def delete(self) -> None:
        subprocess.run(["k3d", "cluster", "delete", self.name], capture_output=True, text=True)

    def ready_nodes(self) -> list[str]:
        ready = []
        for node in self.kube.items("nodes"):
            for cond in node["status"].get("conditions", []):
                if cond["type"] == "Ready" and cond["status"] == "True":
                    ready.append(node["metadata"]["name"])
        return ready

    def import_images(self, images: list[str]) -> None:
        """Pull on the host once and import, so no pod waits on a registry mid-scenario."""
        present = []
        for image in images:
            if subprocess.run(["docker", "image", "inspect", image], capture_output=True).returncode != 0:
                subprocess.run(["docker", "pull", "-q", image], check=True, capture_output=True)
            present.append(image)
        if present:
            subprocess.run(["k3d", "image", "import", "-c", self.name, *present],
                           check=True, capture_output=True, text=True)

    def audit_events(self, since: str, username: str | None = None) -> list[dict]:
        """ResponseComplete events since an RFC3339 timestamp, optionally for one user."""
        proc = subprocess.run(["docker", "exec", self.server_node, "cat", AUDIT_LOG],
                              capture_output=True, text=True)
        events = []
        for line in proc.stdout.splitlines():
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                continue
            if ev.get("stage") != "ResponseComplete" or ev.get("requestReceivedTimestamp", "") < since:
                continue
            if username and ev.get("user", {}).get("username") != username:
                continue
            events.append(ev)
        return events

    def node_container(self, node: str) -> str:
        """k3d names node containers after the Kubernetes node name."""
        return node

    def stop_node(self, node: str) -> None:
        subprocess.run(["docker", "stop", "-t", "0", self.node_container(node)], capture_output=True)

    def start_node(self, node: str) -> None:
        subprocess.run(["docker", "start", self.node_container(node)], capture_output=True)
