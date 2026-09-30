"""Turns results.json files into one scorecard comparing every operator build that was run."""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

POINTS = {"PASS": 100, "DEGRADED": 50, "FAIL": 0}
KEY_MEASURES = [
    ("time_to_recover_s", "recover s"),
    ("time_to_status_healthy_s", "status s"),
    ("read_availability_pct", "read %"),
    ("write_availability_pct", "write %"),
    ("keeper_min_serving", "keeper min"),
    ("api_requests", "API calls"),
]
PERF_MEASURES = [
    ("baseline_create_s", "create s"),
    ("time_to_recover_s", "done s"),
    ("api_requests", "API calls"),
    ("api_requests_per_host", "API calls/host"),
    ("api_conflicts_409", "409s"),
]


def load(paths: list[Path]) -> list[dict]:
    results = []
    for p in paths:
        f = p / "results.json" if p.is_dir() else p
        if f.exists():
            results.extend(json.loads(f.read_text()))
    return results


def _m(r: dict, name: str) -> str:
    for m in r.get("measurements", []):
        if m["name"] == name:
            v = m["value"]
            return "-" if v is None else (f"{v:g}" if isinstance(v, (int, float)) else str(v))
    return "-"


def scorecard(results: list[dict]) -> str:
    targets = sorted({(r["operator"], r["version"]) for r in results})
    lines = ["# Operator chaos scorecard", ""]

    lines += ["## Summary", "", "| Operator | Version | Resilience score | PASS | DEGRADED | FAIL | ERROR |",
              "|---|---|---|---|---|---|---|"]
    for op, ver in targets:
        rs = [r for r in results if (r["operator"], r["version"]) == (op, ver) and r["category"] != "performance"]
        counted = [r for r in rs if r["verdict"] in POINTS]
        score = f"{sum(POINTS[r['verdict']] for r in counted) / len(counted):.0f}/100" if counted else "-"
        c = defaultdict(int)
        for r in rs:
            c[r["verdict"]] += 1
        lines.append(f"| {op} | {ver} | {score} | {c['PASS']} | {c['DEGRADED']} | {c['FAIL']} | {c['ERROR']} |")
    lines += ["", "Score: PASS 100, DEGRADED 50, FAIL 0, averaged over resilience scenarios that ran (ERROR excluded).", ""]

    lines += ["## By category", "", "| Category | " + " | ".join(f"{o} {v}" for o, v in targets) + " |",
              "|---|" + "---|" * len(targets)]
    for cat in sorted({r["category"] for r in results if r["category"] != "performance"}):
        row = [cat]
        for t in targets:
            rs = [r for r in results if (r["operator"], r["version"]) == t and r["category"] == cat and r["verdict"] in POINTS]
            row.append(f"{sum(POINTS[r['verdict']] for r in rs) / len(rs):.0f}" if rs else "-")
        lines.append("| " + " | ".join(row) + " |")
    lines.append("")

    scenarios = []
    for r in results:
        if r["scenario"] not in scenarios and r["category"] != "performance":
            scenarios.append(r["scenario"])
    lines += ["## Scenarios", "", "| Scenario | " + " | ".join(f"{o} {v}" for o, v in targets) + " |",
              "|---|" + "---|" * len(targets)]
    index = {(r["operator"], r["version"], r["scenario"]): r for r in results}
    for s in scenarios:
        row = [s]
        for o, v in targets:
            r = index.get((o, v, s))
            if not r:
                row.append("-")
                continue
            took = _m(r, "time_to_recover_s")
            row.append(f"{r['verdict']} ({took} s)" if took != "-" else r["verdict"])
        lines.append("| " + " | ".join(row) + " |")
    lines += ["", "Time in brackets: seconds from the failure (or from when a held fault was cleared) "
                  "until every pod was Ready and the scenario's change had reached every pod.", ""]

    lines += ["## Speed", "",
              "Seconds; lower is better. `recover` = every pod Ready, `status` = the operator also "
              "reports the cluster healthy, `create` = fresh cluster to healthy baseline.", "",
              "| Scenario | " + " | ".join(f"{o} {v} recover / status / create" for o, v in targets) + " |",
              "|---|" + "---|" * len(targets)]
    for s in scenarios:
        row = [s]
        for o, v in targets:
            r = index.get((o, v, s))
            row.append(" / ".join(_m(r, k) for k in ("time_to_recover_s", "time_to_status_healthy_s", "baseline_create_s"))
                       if r else "-")
        lines.append("| " + " | ".join(row) + " |")
    medians = []
    for o, v in targets:
        vals = sorted(m["value"] for r in results if (r["operator"], r["version"]) == (o, v)
                      for m in r["measurements"] if m["name"] == "time_to_recover_s" and m["value"] is not None)
        medians.append(f"{vals[len(vals) // 2]:g}" if vals else "-")
    lines.append("| **median recover (recovered scenarios only)** | " + " | ".join(medians) + " |")
    lines.append("")

    perf = [r for r in results if r["category"] == "performance"]
    if perf:
        lines += ["## Performance", "", "| Scenario | Operator | Version | " + " | ".join(h for _, h in PERF_MEASURES) + " |",
                  "|---|---|---|" + "---|" * len(PERF_MEASURES)]
        for r in sorted(perf, key=lambda x: (x["scenario"], x["operator"], x["version"])):
            lines.append(f"| {r['scenario']} | {r['operator']} | {r['version']} | "
                         + " | ".join(_m(r, k) for k, _ in PERF_MEASURES) + " |")
        lines.append("")

    lines += ["## Details", ""]
    for o, v in targets:
        lines += [f"### {o} {v}", "", "| Scenario | Verdict | " + " | ".join(h for _, h in KEY_MEASURES) + " | Findings |",
                  "|---|---|" + "---|" * len(KEY_MEASURES) + "---|"]
        for r in [x for x in results if (x["operator"], x["version"]) == (o, v)]:
            notes = "; ".join(f"{f['severity']}: {f['check']}: {f['detail']}" for f in r["findings"]
                              if f["severity"] in ("fail", "warn")) or r.get("summary", "")
            issues = f" ({', '.join(r['related_issues'])})" if r.get("related_issues") else ""
            lines.append(f"| {r['scenario']}{issues} | {r['verdict']} | "
                         + " | ".join(_m(r, k) for k, _ in KEY_MEASURES)
                         + f" | {notes.replace('|', '/')} |")
        lines.append("")
    return "\n".join(lines)
