"""Thin kubectl wrapper. Everything goes through kubectl so the platform needs no Python packages."""

from __future__ import annotations

import json
import subprocess
import time
from typing import Any, Callable, Iterable


class KubeError(RuntimeError):
    pass


class Kube:
    def __init__(self, context: str):
        self.context = context

    def run(self, *args: str, stdin: str | None = None, check: bool = True, timeout: int = 180) -> str:
        cmd = ["kubectl", "--context", self.context, *args]
        try:
            proc = subprocess.run(cmd, input=stdin, capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            raise KubeError(f"timeout after {timeout}s: {' '.join(args)}") from exc
        if check and proc.returncode != 0:
            raise KubeError(f"kubectl {' '.join(args)}: {proc.stderr.strip() or proc.stdout.strip()}")
        return proc.stdout

    def json(self, *args: str, check: bool = True) -> Any:
        out = self.run(*args, "-o", "json", check=check)
        return json.loads(out) if out.strip() else None

    def apply(self, objects: Iterable[dict], namespace: str | None = None, server_side: bool = False) -> None:
        doc = {"apiVersion": "v1", "kind": "List", "items": list(objects)}
        args = ["apply", "-f", "-"]
        if namespace:
            args += ["-n", namespace]
        if server_side:
            args += ["--server-side", "--force-conflicts"]
        self.run(*args, stdin=json.dumps(doc))

    def apply_url(self, url: str, server_side: bool = True) -> None:
        args = ["apply", "-f", url]
        if server_side:
            args += ["--server-side", "--force-conflicts"]
        self.run(*args, timeout=300)

    def items(self, kind: str, namespace: str | None = None, selector: str | None = None) -> list[dict]:
        args = ["get", kind]
        args += ["-n", namespace] if namespace else ["-A"]
        if selector:
            args += ["-l", selector]
        data = self.json(*args, check=False)
        return (data or {}).get("items", [])

    def get(self, kind: str, name: str, namespace: str | None = None) -> dict | None:
        args = ["get", kind, name] + (["-n", namespace] if namespace else [])
        out = self.run(*args, "-o", "json", check=False)
        return json.loads(out) if out.strip() else None

    def delete(self, kind: str, name: str | None = None, namespace: str | None = None,
               selector: str | None = None, grace: int | None = None, force: bool = False,
               wait: bool = False, cascade: str | None = None) -> None:
        args = ["delete", kind] + ([name] if name else [])
        if namespace:
            args += ["-n", namespace]
        if selector:
            args += ["-l", selector]
        if grace is not None:
            args += [f"--grace-period={grace}"]
        if force:
            args += ["--force"]
        if cascade:
            args += [f"--cascade={cascade}"]
        args += [f"--wait={'true' if wait else 'false'}", "--ignore-not-found"]
        self.run(*args, check=False)

    def patch(self, kind: str, name: str, namespace: str, patch: dict, patch_type: str = "merge",
              subresource: str | None = None) -> None:
        args = ["patch", kind, name, "-n", namespace, "--type", patch_type, "-p", json.dumps(patch)]
        if subresource:
            args += [f"--subresource={subresource}"]
        self.run(*args)

    def exec(self, namespace: str, pod: str, container: str | None, command: list[str],
             timeout: int = 60) -> tuple[int, str]:
        args = ["exec", "-n", namespace, pod]
        if container:
            args += ["-c", container]
        args += ["--", *command]
        cmd = ["kubectl", "--context", self.context, *args]
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            return 124, "timeout"
        return proc.returncode, (proc.stdout if proc.returncode == 0 else proc.stderr + proc.stdout)

    def logs(self, namespace: str, pod: str, container: str | None = None, since_time: str | None = None) -> str:
        args = ["logs", "-n", namespace, pod]
        if container:
            args += ["-c", container]
        if since_time:
            args += [f"--since-time={since_time}"]
        return self.run(*args, check=False, timeout=120)


def wait_until(predicate: Callable[[], bool], timeout: float, interval: float = 1.0) -> float | None:
    """Poll until predicate() is true. Returns elapsed seconds, or None on timeout."""
    start = time.monotonic()
    while True:
        try:
            if predicate():
                return time.monotonic() - start
        except KubeError:
            pass
        if time.monotonic() - start >= timeout:
            return None
        time.sleep(interval)


def pod_ready(pod: dict) -> bool:
    if pod.get("metadata", {}).get("deletionTimestamp"):
        return False
    for cond in pod.get("status", {}).get("conditions", []) or []:
        if cond.get("type") == "Ready":
            return cond.get("status") == "True"
    return False


def now_rfc3339() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
