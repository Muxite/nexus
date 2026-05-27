"""Bucket explorer rows by LLM relatedness confidence and report
hypothetical post-news drift.

The explorer doesn't pick a direction — it just records that an article
might be relevant to a market. To turn that into a "would I have made
money" signal, we look at how the market mid moved during the +20m
post-news window relative to `market_mid_t0`:

  - `drift_yes_end`  = mid at end of window − mid_t0
                       (signed, as if we'd bet YES at t0)
  - `drift_abs_max`  = max |mid − mid_t0| during the window
                       (volatility — did the news move the market AT ALL?)

Read-only — does not modify any tables. Output is markdown.

Usage:
  python -m scripts.explorer.analyze_pnl_by_confidence \\
      --db /data/irene.db --cycle explorer-0
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path
from typing import Optional

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

# Default confidence-bucket edges. The relatedness call emits [0, 1].
CONFIDENCE_EDGES = [0.3, 0.5, 0.7, 0.9]


def analyze(
    db_path: str,
    *,
    cycle: Optional[str] = None,
    edges: list[float] = CONFIDENCE_EDGES,
) -> list[str]:
    con = connect_readonly(db_path)
    buckets: dict[str, dict] = defaultdict(lambda: {
        "n": 0,
        "drift_yes_end": [],
        "drift_abs_max": [],
        "n_with_drift": 0,
    })

    for exp in iter_explorations(con, cycle=cycle, only_complete=True):
        b = bucket_label(exp.llm_confidence, edges)
        d = compute_drift(con, exp.id, exp.market_mid_t0)
        buckets[b]["n"] += 1
        if d.drift_yes_end is not None:
            buckets[b]["drift_yes_end"].append(d.drift_yes_end)
            buckets[b]["drift_abs_max"].append(d.drift_abs_max)
            buckets[b]["n_with_drift"] += 1

    out = ["# Explorer PnL — by LLM relatedness confidence", ""]
    if cycle:
        out.append(f"Cycle: `{cycle}`")
    out.append(f"Confidence edges: {edges}")
    out.append("")
    out.append("| Bucket | N | N with drift | Mean drift_yes_end | "
               "Median drift_yes_end | Mean |drift|_max |")
    out.append("|---|---|---|---|---|---|")
    # Sort buckets in a stable order based on edges.
    order: list[str] = [f"< {edges[0]:g}"]
    for i in range(len(edges) - 1):
        order.append(f"{edges[i]:g}-{edges[i+1]:g}")
    order += [f">= {edges[-1]:g}", "unknown"]
    for b in order:
        if b not in buckets:
            continue
        v = buckets[b]
        mean_end = mean(v["drift_yes_end"])
        med_end = percentile(v["drift_yes_end"], 0.5)
        mean_abs = mean(v["drift_abs_max"])
        out.append(
            f"| {b} | {v['n']} | {v['n_with_drift']} | "
            f"{_fmt(mean_end)} | {_fmt(med_end)} | {_fmt(mean_abs)} |"
        )
    return out


def _fmt(v: Optional[float]) -> str:
    if v is None:
        return "—"
    return f"{v:+.4f}"


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Bucket explorations by LLM confidence and "
                    "report post-news drift."
    )
    p.add_argument("--db", default="/data/irene.db")
    p.add_argument("--cycle", default=None,
                   help="filter by cycle name (e.g. explorer-0)")
    p.add_argument("--edges", type=float, nargs="+",
                   default=CONFIDENCE_EDGES,
                   help="confidence bucket edges")
    args = p.parse_args(argv)

    for line in analyze(args.db, cycle=args.cycle, edges=args.edges):
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
