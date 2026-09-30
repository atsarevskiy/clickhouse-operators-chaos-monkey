"""Operator-neutral description of a cluster, of its observed state, and of a scenario result."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class ClusterSpec:
    """What a scenario asks for. Each operator adapter renders it into its own custom resources."""

    name: str = "chaos"
    namespace: str = "chaos"
    shards: int = 2
    replicas: int = 2
    keepers: int = 3
    storage: str = "1Gi"
    storage_class: str | None = None
    server_image: str = "clickhouse/clickhouse-server:26.8"
    keeper_image: str = "clickhouse/clickhouse-keeper:26.8"
    # Pod template knobs scenarios turn to create a failure. Kept generic: every operator
    # accepts a pod template of some form.
    pod_annotations: dict[str, str] = field(default_factory=dict)
    pod_labels: dict[str, str] = field(default_factory=dict)
    prestop_sleep: int = 0
    termination_grace: int | None = None
    init_sleep: int = 0
    server_memory_limit: str = "1Gi"
    # Switching this to "b" changes the data volume claim template, which is immutable on a
    # StatefulSet, so it forces the operator down its delete-and-recreate path.
    volume_variant: str = "a"
    volume_b_storage_class: str | None = None
    server_settings: dict[str, Any] = field(default_factory=dict)
    keeper_settings: dict[str, Any] = field(default_factory=dict)

    @property
    def hosts(self) -> int:
        return self.shards * self.replicas

    def copy(self, **changes: Any) -> "ClusterSpec":
        data = asdict(self)
        data.update(changes)
        return ClusterSpec(**data)


@dataclass
class ClusterState:
    """The state an operator reports on its custom resources, reduced to what's comparable."""

    reconciled: bool
    phase: str
    message: str = ""
    raw: dict = field(default_factory=dict)


@dataclass
class Finding:
    severity: str  # "fail" | "warn" | "info"
    check: str
    detail: str


@dataclass
class Measurement:
    name: str
    value: float | None
    unit: str
    note: str = ""


@dataclass
class ScenarioResult:
    scenario: str
    category: str
    operator: str
    version: str
    verdict: str = "ERROR"  # PASS | DEGRADED | FAIL | ERROR | SKIPPED
    summary: str = ""
    findings: list[Finding] = field(default_factory=list)
    measurements: list[Measurement] = field(default_factory=list)
    duration_s: float = 0.0
    related_issues: list[str] = field(default_factory=list)

    def add(self, severity: str, check: str, detail: str) -> None:
        self.findings.append(Finding(severity, check, detail))

    def measure(self, name: str, value: float | None, unit: str, note: str = "") -> None:
        self.measurements.append(Measurement(name, None if value is None else round(value, 2), unit, note))

    def decide(self) -> None:
        severities = {f.severity for f in self.findings}
        if self.verdict in ("ERROR", "SKIPPED") and self.summary:
            return
        if "fail" in severities:
            self.verdict = "FAIL"
        elif "warn" in severities:
            self.verdict = "DEGRADED"
        else:
            self.verdict = "PASS"

    def to_dict(self) -> dict:
        return asdict(self)
