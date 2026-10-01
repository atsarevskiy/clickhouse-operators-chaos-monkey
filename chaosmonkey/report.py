"""Turns results.json files into one scorecard comparing every operator build that was run."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path

POINTS = {"PASS": 100, "DEGRADED": 50, "FAIL": 0}
KEY_MEASURES = [
    ("time_to_recover_s", "recover s"),
    ("time_to_status_healthy_s", "status s"),
    ("longest_read_outage_s", "read outage s"),
    ("longest_write_outage_s", "write outage s"),
    ("stream_insert_p99_ms", "insert p99 ms"),
    ("stream_read_p99_ms", "read p99 ms"),
    ("stream_acked_batches_lost", "acked lost"),
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


def _base(scenario_id: str) -> str:
    return scenario_id.split("#")[0]


def load(paths: list[Path]) -> list[dict]:
    """Results from every path. A later path's results for a scenario replace an earlier path's
    for the same operator build, so a rerun of a few scenarios can be laid over a full run."""
    results: list[dict] = []
    for p in paths:
        f = p / "results.json" if p.is_dir() else p
        if not f.exists():
            continue
        new = json.loads(f.read_text())
        replaced = {(r["operator"], r["version"], _base(r["scenario"])) for r in new}
        results = [r for r in results if (r["operator"], r["version"], _base(r["scenario"])) not in replaced]
        results.extend(new)
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

    lines += ["## Impact on a live ingest stream", "",
              "A client writes a 100-row batch every second and reads every second through the cluster "
              "Service throughout each scenario. `outage` = longest run of failed seconds; `p99` = latency; "
              "`lost` = batches the client saw acknowledged that are not stored (must be 0).", "",
              "| Scenario | " + " | ".join(f"{o} {v} write outage s / insert p99 ms / read p99 ms / lost" for o, v in targets) + " |",
              "|---|" + "---|" * len(targets)]
    for s in scenarios:
        row = [s]
        for o, v in targets:
            r = index.get((o, v, s))
            row.append(" / ".join(_m(r, k) for k in ("longest_write_outage_s", "stream_insert_p99_ms",
                                                      "stream_read_p99_ms", "stream_acked_batches_lost"))
                       if r else "-")
        lines.append("| " + " | ".join(row) + " |")
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


# ---------------------------------------------------------------- README results table

_PLAIN = {
    "recovery": "never got back to healthy",
    "recovery time": "recovered, but slower than the target",
    "status truthfulness": "status reported healthy while hosts were down",
    "status stuck": "status never reported healthy after every pod was Ready",
    "availability": "clients saw an outage",
    "keeper quorum": "Keeper lost quorum",
    "data loss": "rows were lost",
    "data": "a replica could not be read",
    "stream": "a replica could not read the stream table",
    "replica not caught up": "a replica did not catch up on replication",
    "acknowledged writes lost": "acknowledged writes were lost",
    "replica divergence": "replicas of a shard diverged",
    "stream duplicates": "some writes were stored twice",
    "writes after recovery": "writes failed after recovery",
    "blast radius": "took down more than it should have",
    "destructive on invalid spec": "destroyed a host applying a rejected spec",
    "drift repair": "a deleted object was not recreated",
    "replica metadata": "removed replica still registered",
    "keeper metadata": "removed replica still registered in Keeper",
    "replica cleanup": "removed replica still registered",
    "schema propagation": "a new replica had no schema",
    "premature publishing": "served clients before its schema existed",
    "orphaned objects": "left objects behind after deletion",
    "stale data resurrected on re-added shard": "old data came back with a re-added shard",
    "keeper leadership": "Keeper had no leader for too long",
    "shard re-added": "a re-added shard did not work",
}


def _clean(detail: str, limit: int = 220) -> str:
    """One line of a finding, without the ClickHouse client's exception boilerplate."""
    text = re.sub(r"Received exception from server \(version [^)]*\):\s*", "", detail)
    text = re.sub(r"Code: \d+\. DB::Exception: (Received from \S+ )?(DB::Exception: )?", "", text)
    text = " ".join(text.split())
    return text if len(text) <= limit else text[:limit - 3].rstrip() + "..."


def _value(r: dict, name: str):
    for m in r.get("measurements", []):
        if m["name"] == name:
            return m["value"]
    return None


def _happened(r: dict) -> str:
    """What the run observed: the findings first, then the numbers that back them."""
    if r["verdict"] == "SKIPPED":
        return "not run: the trigger needs " + r.get("summary", "").split(": ", 1)[-1] + ", which this operator lacks"
    if r["verdict"] == "ERROR":
        return "harness error: " + _clean(r.get("summary", ""), 120)
    if r["verdict"] == "INVALID":
        return _clean(r.get("summary", ""), 160)
    grouped: dict[str, list[str]] = {}
    for f in r.get("findings", []):
        if f["severity"] in ("fail", "warn"):
            details = grouped.setdefault(_PLAIN.get(f["check"], f["check"]), [])
            if _clean(f["detail"]) not in details:
                details.append(_clean(f["detail"]))
    parts = [f"{label}: {' and '.join(details)}" for label, details in grouped.items()]
    rec, lag = _value(r, "time_to_recover_s"), _value(r, "status_lag_after_ready_s")
    if rec is not None:
        parts.append(f"healthy {rec:.0f} s after the fault ended"
                     + (f", status agreed {lag:.0f} s later" if lag and lag >= 5 else ""))
    ro, wo = _value(r, "longest_read_outage_s"), _value(r, "longest_write_outage_s")
    if (ro is not None or wo is not None) and "availability" not in {f["check"] for f in r.get("findings", [])}:
        parts.append(f"longest outage read {ro if ro is not None else '-'} s / write {wo if wo is not None else '-'} s")
    ro_wait = _value(r, "replica_readonly_after_healthy_s")
    if ro_wait:
        parts.append(f"a replica's tables stayed read-only {ro_wait:.0f} s after its pod was Ready")
    acked, lost = _value(r, "stream_batches_acked_total"), _value(r, "stream_acked_batches_lost")
    if acked:
        parts.append(f"{lost or 0} of {acked} acknowledged batches lost")
    return "; ".join(parts)


def _expected(scenario_id: str) -> str:
    from . import scenarios
    s = scenarios.BY_ID.get(scenario_id.split("#")[0])
    if s is None:
        return "-"
    e = s.expect
    targets = [f"healthy within {e.recover_slo_s} s"] if e.cluster_survives else []
    if e.max_read_outage_s is not None:
        targets.append(f"read outage up to {e.max_read_outage_s} s")
    if e.max_write_outage_s is not None:
        targets.append(f"write outage up to {e.max_write_outage_s} s")
    if e.data_must_survive and e.cluster_survives:
        targets.append("no row lost")
    return s.description + (" Targets: " + ", ".join(targets) + "." if targets else "")


#: findings about speed rather than about a wrong outcome; the timing tables cover them, so they
#: don't lower the correctness score
PERFORMANCE_CHECKS = {"availability", "recovery time", "keeper leadership"}

#: categories where the operator reacts to something breaking; the rest are changes it applies
CHANGE_CATEGORIES = {"spec", "performance"}


def correctness_score(r: dict) -> float | None:
    """100 with no correctness finding, 50 with warnings only, 0 with any failure."""
    if r["verdict"] not in POINTS:
        return None
    sev = {f["severity"] for f in r.get("findings", []) if f["check"] not in PERFORMANCE_CHECKS}
    return 0.0 if "fail" in sev else 50.0 if "warn" in sev else 100.0


def _mean(xs: list) -> float | None:
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def _pct(xs: list[float], q: float) -> float | None:
    """Nearest-rank percentile."""
    if not xs:
        return None
    xs = sorted(xs)
    return xs[min(len(xs) - 1, max(0, int(round(q * len(xs) + 0.5)) - 1))]


def _per_scenario(rs: list[dict], fn) -> dict[str, tuple[str, float]]:
    """A scenario run several times scores the average of its runs, and counts once."""
    runs: dict[str, list[dict]] = defaultdict(list)
    for r in rs:
        runs[_base(r["scenario"])].append(r)
    out = {}
    for sid, rr in runs.items():
        v = _mean([fn(r) for r in rr])
        if v is not None:
            out[sid] = (rr[0]["category"], v)
    return out


def _fmt(v: float | None) -> str:
    return "-" if v is None else f"{v:.0f}"


def _timing_row(label: str, rs: list[dict]) -> list[str]:
    """Every run is one event: its time to healthy, plus how many runs never got there."""
    ran = [r for r in rs if r["verdict"] in POINTS]
    times = [v for r in ran if (v := _value(r, "time_to_recover_s")) is not None]
    outages = [max(x for x in (_value(r, "longest_read_outage_s"), _value(r, "longest_write_outage_s")) if x is not None)
               for r in ran if _value(r, "longest_read_outage_s") is not None or _value(r, "longest_write_outage_s") is not None]
    return [label, str(len(times)), _fmt(_mean(times)), _fmt(_pct(times, 0.5)), _fmt(_pct(times, 0.9)),
            _fmt(_pct(times, 0.99)), _fmt(max(times) if times else None), str(len(ran) - len(times)),
            _fmt(_mean(outages)), _fmt(_pct(outages, 0.9)), _fmt(max(outages) if outages else None)]


def summary_table(results: list[dict]) -> str:
    targets = sorted({(r["operator"], r["version"]) for r in results})
    cats = sorted({r["category"] for r in results})
    head = ["Operator", "Correctness", "PASS", "DEGRADED", "FAIL", "SKIPPED", "INVALID"]
    lines = ["| " + " | ".join(head) + " |", "|" + "---|" * len(head)]
    per_target = {}
    for t in targets:
        rs = [r for r in results if (r["operator"], r["version"]) == t]
        cor = _per_scenario(rs, correctness_score)
        per_target[t] = cor
        c = defaultdict(int)
        for r in rs:
            c[r["verdict"]] += 1
        lines.append("| " + " | ".join([
            f"{t[0]} {t[1]}", f"**{_fmt(_mean([v for _, v in cor.values()]))}/100**",
            str(c["PASS"]), str(c["DEGRADED"]), str(c["FAIL"]), str(c["SKIPPED"]), str(c["INVALID"])]) + " |")
    head = ["Operator", "Events", "Runs timed", "Mean s", "p50 s", "p90 s", "p99 s", "Max s", "Never healthy",
            "Client outage mean s", "Outage p90 s", "Outage max s"]
    lines += ["", "| " + " | ".join(head) + " |", "|" + "---|" * len(head)]
    for t in targets:
        rs = [r for r in results if (r["operator"], r["version"]) == t]
        for label, group in (("failures", [r for r in rs if r["category"] not in CHANGE_CATEGORIES]),
                             ("changes", [r for r in rs if r["category"] in CHANGE_CATEGORIES])):
            lines.append("| " + " | ".join([f"{t[0]} {t[1]}"] + _timing_row(label, group)) + " |")
    lines += ["", "| Category | " + " | ".join(f"{o} correctness / p50 / p90 s" for o, _ in targets) + " |",
              "|---|" + "---|" * len(targets)]
    for cat in cats:
        row = [cat]
        for t in targets:
            cor = per_target[t]
            times = [v for r in results if (r["operator"], r["version"]) == t and r["category"] == cat
                     and r["verdict"] in POINTS and (v := _value(r, "time_to_recover_s")) is not None]
            row.append(f"{_fmt(_mean([v for k, v in cor.values() if k == cat]))} / "
                       f"{_fmt(_pct(times, 0.5))} / {_fmt(_pct(times, 0.9))}")
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def readme_table(results: list[dict]) -> str:
    targets = sorted({(r["operator"], r["version"]) for r in results})
    index: dict[tuple, list[dict]] = defaultdict(list)
    for r in results:
        index[(r["operator"], r["version"], _base(r["scenario"]))].append(r)
    order = []
    for r in results:
        if _base(r["scenario"]) not in order:
            order.append(_base(r["scenario"]))
    head = ["Category", "Scenario", "Expected"]
    for o, v in targets:
        head += [f"{o} {v}", f"What happened ({o})"]
    lines = [summary_table(results), "",
             "**Correctness** asks whether the outcome was right: data kept, every host back, status honest, "
             "nothing destroyed. Per scenario it is 100 with no correctness finding, 50 with warnings only, 0 with "
             "a failure; a scenario run several times counts once with the average of its runs, and SKIPPED "
             "scenarios don't count. **Response time** is seconds from the fault ending (or the change being "
             "applied) until every pod is Ready and the change has reached every pod, one sample per run: "
             "`failures` are the scenarios that break something, `changes` the spec and performance ones. "
             "`Never healthy` counts runs with no time at all, which the percentiles leave out. Client outage "
             "is the longest run of failed reads or writes during the run. INVALID runs happened while the host "
             "was overloaded (OOM kills, load above 2 per CPU, or under 3 GB free) and are not scored. "
             "PASS/DEGRADED/FAIL/SKIPPED/INVALID count runs.", "",
             "| " + " | ".join(head) + " |", "|" + "---|" * len(head)]
    for s in order:
        runs = [index.get((o, v, s), []) for o, v in targets]
        cat = next(rr[0]["category"] for rr in runs if rr)
        row = [cat, f"`{s}`", _expected(s)]
        for rr in runs:
            if not rr:
                row += ["-", "-"]
                continue
            verdicts = " / ".join(r["verdict"] + (f" ({POINTS[r['verdict']]})" if r["verdict"] in POINTS else "")
                                  for r in rr)
            # with repeats, describe the worst run and say how the others went
            worst = min(rr, key=lambda r: POINTS.get(r["verdict"], 101))
            text = _happened(worst) or "recovered cleanly"
            if len(rr) > 1:
                text = f"{len(rr)} runs; worst: " + text
            took = [v for r in rr if (v := _value(r, "time_to_recover_s")) is not None]
            scores = (f"correctness {_fmt(_mean([correctness_score(r) for r in rr]))}, "
                      + (f"healthy in {' / '.join(_fmt(x) for x in took)} s" if took else "never healthy"))
            row += [verdicts if rr[0]["verdict"] == "SKIPPED" else f"{verdicts}<br>{scores}", text]
        lines.append("| " + " | ".join(c.replace("|", "/") for c in row) + " |")
    return "\n".join(lines)
