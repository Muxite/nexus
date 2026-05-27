"""Apply a hypothetical filter to the explorer dataset and report the
edge of the surviving cohort.

This is the "if I had only acted on rows where X" backtest tool: the
filter is described on the command line as one or more
`COLUMN OP VALUE` clauses (AND-combined), and the script reports
how many rows pass, plus their post-news drift distribution.

Supported columns:
  llm_confidence, dkg_final_score, cosine_similarity, breaking_score,
  hours_to_expiry, market_volume_24h, market_spread_t0

Supported operators: >=, >, <=, <, ==, !=

Examples:
  python -m scripts.explorer.replay_filter --db /data/irene.db \\
      --cycle explorer-0 \\
      --filter 'llm_confidence >= 0.7' \\
      --filter 'breaking_score >= 0.5' \\
      --filter 'hours_to_expiry >= 12'

Read-only.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Callable, Optional

if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.explorer._common import (
    compute_drift,
    connect_readonly,
    iter_explorations,
    mean,
    percentile,
)


_COLUMNS = {
    "llm_confidence": lambda e: e.llm_confidence,
    "dkg_final_score": lambda e: e.dkg_final_score,
    "cosine_similarity": lambda e: e.cosine_similarity,
    "breaking_score": lambda e: e.breaking_score,
    "hours_to_expiry": lambda e: e.hours_to_expiry,
    "market_volume_24h": lambda e: e.market_volume_24h,
    "market_spread_t0": lambda e: e.market_spread_t0,
}

_OPS = {
    ">=": lambda a, b: a is not None and a >= b,
    ">": lambda a, b: a is not None and a > b,
    "<=": lambda a, b: a is not None and a <= b,
    "<": lambda a, b: a is not None and a < b,
    "==": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
}


def parse_filter(expr: str) -> Callable:
    """Parse a `COLUMN OP VALUE` clause into a predicate over an
    `ExplorationRow`. Raises ValueError on a malformed expr."""
    # Operators sorted longest-first so '>=' matches before '>'.
    for op in sorted(_OPS.keys(), key=len, reverse=True):
        if f" {op} " in expr:
            left, right = expr.split(f" {op} ", 1)
            col = left.strip()
            if col not in _COLUMNS:
                raise ValueError(
                    f"unknown column {col!r}; supported: "
                    f"{sorted(_COLUMNS.keys())}"
                )
            try:
                val = float(right.strip())
            except ValueError as exc:
                raise ValueError(
                    f"could not parse {right!r} as a number"
                ) from exc
            getter = _COLUMNS[col]
            test = _OPS[op]
            return lambda e, g=getter, t=test, v=val: t(g(e), v)
    raise ValueError(
        f"could not parse filter {expr!r} (expected 'COLUMN OP VALUE')"
    )


def _fmt(v: Optional[float]) -> str:
    if v is None:
        return "—"
    return f"{v:+.4f}"


def replay(
    db_path: str,
    *,
    cycle: Optional[str] = None,
    filters: Optional[list[Callable]] = None,
) -> list[str]:
    con = connect_readonly(db_path)
    rows = list(iter_explorations(con, cycle=cycle, only_complete=True))
    survivors = []
    rejected = 0
    for r in rows:
        if all(f(r) for f in (filters or [])):
            survivors.append(r)
        else:
            rejected += 1

    drift_yes_end = []
    drift_abs_max = []
    n_with_drift = 0
    for s in survivors:
        d = compute_drift(con, s.id, s.market_mid_t0)
        if d.drift_yes_end is not None:
            drift_yes_end.append(d.drift_yes_end)
            drift_abs_max.append(d.drift_abs_max)
            n_with_drift += 1

    out = ["# Explorer replay filter", ""]
    if cycle:
        out.append(f"Cycle: `{cycle}`")
    out.append(f"Total explorations evaluated: {len(rows)}")
    out.append(f"Survivors: {len(survivors)}")
    out.append(f"Rejected:  {rejected}")
    out.append("")
    out.append(f"Survivors with post-news drift: {n_with_drift}")
    out.append(f"Mean drift_yes_end:    {_fmt(mean(drift_yes_end))}")
    out.append(f"Median drift_yes_end:  "
               f"{_fmt(percentile(drift_yes_end, 0.5))}")
    out.append(f"P25 drift_yes_end:     "
               f"{_fmt(percentile(drift_yes_end, 0.25))}")
    out.append(f"P75 drift_yes_end:     "
               f"{_fmt(percentile(drift_yes_end, 0.75))}")
    out.append(f"Mean |drift|_max:      {_fmt(mean(drift_abs_max))}")
    return out


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Replay-filter the explorer dataset and report "
                    "survivor edge."
    )
    p.add_argument("--db", default="/data/irene.db")
    p.add_argument("--cycle", default=None)
    p.add_argument("--filter", action="append", default=[],
                   help="filter clause like 'llm_confidence >= 0.7'; "
                        "may be repeated (AND).")
    args = p.parse_args(argv)

    filters = [parse_filter(f) for f in args.filter]
    for line in replay(args.db, cycle=args.cycle, filters=filters):
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
