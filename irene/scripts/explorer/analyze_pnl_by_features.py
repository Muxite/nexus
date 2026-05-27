"""Slice explorer drift by feature buckets — tier, breaking_score,
hours_to_expiry, market_volume_24h, freshness — to surface which
features predict bigger post-news moves.

Same "drift" semantics as `analyze_pnl_by_confidence.py`. Read-only.

Usage:
  python -m scripts.explorer.analyze_pnl_by_features \\
      --db /data/irene.db --cycle explorer-0
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path
from typing import Callable, Optional

if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.explorer._common import (
    bucket_label,
    compute_drift,
    connect_readonly,
    iter_explorations,
    mean,
    percentile,
)


def _fmt(v: Optional[float]) -> str:
    if v is None:
        return "—"
    return f"{v:+.4f}"


def _section(
    title: str,
    explorations,
    bucket_fn: Callable,
    drift_lookup: dict,
    order: Optional[list[str]] = None,
) -> list[str]:
    out = [f"## {title}", ""]
    buckets: dict[str, dict] = defaultdict(lambda: {
        "n": 0, "n_with_drift": 0,
        "drift_yes_end": [], "drift_abs_max": [],
    })
    for exp in explorations:
        b = bucket_fn(exp)
        buckets[b]["n"] += 1
        d = drift_lookup.get(exp.id)
        if d is not None and d.drift_yes_end is not None:
            buckets[b]["n_with_drift"] += 1
            buckets[b]["drift_yes_end"].append(d.drift_yes_end)
            buckets[b]["drift_abs_max"].append(d.drift_abs_max)
    out.append("| Bucket | N | N with drift | Mean drift_yes_end | "
               "Median drift_yes_end | Mean |drift|_max |")
    out.append("|---|---|---|---|---|---|")
    if order is None:
        order = sorted(buckets.keys(),
                       key=lambda s: (s == "unknown", s))
    for b in order:
        if b not in buckets:
            continue
        v = buckets[b]
        out.append(
            f"| {b} | {v['n']} | {v['n_with_drift']} | "
            f"{_fmt(mean(v['drift_yes_end']))} | "
            f"{_fmt(percentile(v['drift_yes_end'], 0.5))} | "
            f"{_fmt(mean(v['drift_abs_max']))} |"
        )
    out.append("")
    return out


BREAKING_EDGES = [0.3, 0.5, 0.7, 0.9]
HOURS_EDGES = [6, 24, 72, 168]
VOLUME_EDGES = [1_000, 10_000, 100_000, 1_000_000]


def analyze(db_path: str, *, cycle: Optional[str] = None) -> list[str]:
    con = connect_readonly(db_path)
    rows = list(iter_explorations(con, cycle=cycle, only_complete=True))
    # Pre-compute drift for every exploration so each section re-uses it.
    drift_lookup = {r.id: compute_drift(con, r.id, r.market_mid_t0)
                    for r in rows}

    out = ["# Explorer PnL — by features", ""]
    if cycle:
        out.append(f"Cycle: `{cycle}`")
    out.append(f"N explorations: {len(rows)}")
    out.append(f"N with post-news drift: "
               f"{sum(1 for d in drift_lookup.values() if d.n_live > 0)}")
    out.append("")

    out += _section(
        "By article tier",
        rows, lambda e: e.article_tier or "unknown", drift_lookup,
    )
    out += _section(
        "By breaking_score",
        rows, lambda e: bucket_label(e.breaking_score, BREAKING_EDGES),
        drift_lookup,
    )
    out += _section(
        "By hours_to_expiry",
        rows, lambda e: bucket_label(e.hours_to_expiry, HOURS_EDGES),
        drift_lookup,
    )
    out += _section(
        "By market_volume_24h",
        rows, lambda e: bucket_label(e.market_volume_24h, VOLUME_EDGES),
        drift_lookup,
    )
    out += _section(
        "By freshness",
        rows, lambda e: e.freshness or "unknown", drift_lookup,
    )
    return out


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Slice explorer drift by feature buckets."
    )
    p.add_argument("--db", default="/data/irene.db")
    p.add_argument("--cycle", default=None)
    args = p.parse_args(argv)

    for line in analyze(args.db, cycle=args.cycle):
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
