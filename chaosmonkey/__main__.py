"""Command line entry point: python3 -m chaosmonkey <command>."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from . import operators, report, scenarios
from .cluster import K3dCluster
from .model import ClusterSpec
from .runner import run_target


def _base_spec(args: argparse.Namespace) -> ClusterSpec:
    return ClusterSpec(shards=args.shards, replicas=args.replicas, keepers=args.keepers,
                       server_image=args.server_image, keeper_image=args.keeper_image)


def _run_one(target: dict, args: argparse.Namespace, out_root: Path) -> list:
    adapter_cls = operators.get(target["operator"])
    cluster = K3dCluster(target.get("cluster", args.cluster), agents=args.agents)
    op = adapter_cls(cluster, target["version"], image=target.get("image"))
    label = target.get("label", target["version"])
    out = out_root / f"{op.name}-{label}"
    chosen = scenarios.select(target.get("profile", args.profile), target.get("scenarios") or args.scenario)
    if args.fresh_cluster and cluster.exists():
        cluster.delete()
    try:
        results = run_target(op, cluster, chosen, _base_spec(args), out, env=target.get("env"))
    finally:
        if not args.keep_cluster:
            cluster.delete()
    for r in results:
        r.version = label
    (out / "results.json").write_text(json.dumps([r.to_dict() for r in results], indent=1))
    return results


def cmd_run(args: argparse.Namespace) -> int:
    out_root = Path(args.out) / time.strftime("%Y%m%d-%H%M%S")
    target = {"operator": args.operator, "version": args.version}
    if args.image:
        target["image"] = args.image
    if args.label:
        target["label"] = args.label
    _run_one(target, args, out_root)
    return _write_report(out_root)


def cmd_matrix(args: argparse.Namespace) -> int:
    config = json.loads(Path(args.config).read_text())
    out_root = Path(args.out) / time.strftime("%Y%m%d-%H%M%S")
    args.profile = config.get("profile", args.profile)
    for target in config["targets"]:
        args.fresh_cluster = True
        _run_one(target, args, out_root)
        _write_report(out_root)
    return _write_report(out_root)


def _write_report(out_root: Path) -> int:
    dirs = sorted(p for p in out_root.iterdir() if p.is_dir())
    md = report.scorecard(report.load(dirs))
    (out_root / "scorecard.md").write_text(md)
    print(f"\nscorecard: {out_root / 'scorecard.md'}")
    return 0


def cmd_report(args: argparse.Namespace) -> int:
    paths = [Path(p) for p in args.paths]
    dirs = []
    for p in paths:
        dirs += sorted(x for x in p.iterdir() if x.is_dir()) if (p.is_dir() and not (p / "results.json").exists()) else [p]
    print(report.scorecard(report.load(dirs)))
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    for s in scenarios.ALL:
        profiles = [n for n, ids in scenarios.PROFILES.items() if s.id in ids]
        issues = f"  [{', '.join(s.related_issues)}]" if s.related_issues else ""
        print(f"{s.id:34} {s.category:15} {','.join(profiles):28} {s.title}{issues}")
    print("\noperators:", ", ".join(sorted(operators.ADAPTERS)))
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="chaosmonkey", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    def common(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--profile", default="smoke", choices=sorted(scenarios.PROFILES))
        sp.add_argument("--scenario", action="append", help="run only these scenario ids (repeatable)")
        sp.add_argument("--cluster", default="chaosmonkey", help="k3d cluster name")
        sp.add_argument("--agents", type=int, default=1, help="k3d agent nodes besides the server")
        sp.add_argument("--shards", type=int, default=2)
        sp.add_argument("--replicas", type=int, default=2)
        sp.add_argument("--keepers", type=int, default=3)
        sp.add_argument("--server-image", default="clickhouse/clickhouse-server:26.8")
        sp.add_argument("--keeper-image", default="clickhouse/clickhouse-keeper:26.8")
        sp.add_argument("--out", default="results")
        sp.add_argument("--keep-cluster", action="store_true", help="leave the k3d cluster running afterwards")
        sp.add_argument("--fresh-cluster", action="store_true", help="delete an existing cluster of the same name first")

    r = sub.add_parser("run", help="run scenarios against one operator build")
    r.add_argument("--operator", required=True, choices=sorted(operators.ADAPTERS))
    r.add_argument("--version", required=True, help="operator release, e.g. 0.27.4")
    r.add_argument("--image", help="operator image to run instead of the release image")
    r.add_argument("--label", help="name for this build in the report (defaults to the version)")
    common(r)
    r.set_defaults(fn=cmd_run)

    m = sub.add_parser("matrix", help="run a list of operator builds from a JSON file, one fresh cluster each")
    m.add_argument("config")
    common(m)
    m.set_defaults(fn=cmd_matrix)

    rep = sub.add_parser("report", help="print a scorecard from one or more result directories")
    rep.add_argument("paths", nargs="+")
    rep.set_defaults(fn=cmd_report)

    ls = sub.add_parser("list", help="list scenarios, profiles and operators")
    ls.set_defaults(fn=cmd_list)

    args = p.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
