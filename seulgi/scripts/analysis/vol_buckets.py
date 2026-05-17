"""vol-buckets — outcome by pre-headline market volatility.

The diagnostic that answers "AI wrong vs market too noisy". If losses
concentrate in the high-vol rows, the SL is firing on natural market
movement and `_lowvol` variants should outperform controls. If losses
spread evenly across vol, the AI direction is the failure mode.

Only counts closed trades where `mkt_vol_30m IS NOT NULL` — pre-cycle-11
rows are silently excluded. On snapshots that pre-date the migration,
the script reports an empty Section with a one-line note.
"""
from __future__ import annotations

import sqlite3

from scripts.analysis._common import (
    add_common_args, build_filters, closed_trades_filter, compute_hit_pct,
)
from scripts.analysis._format import Section


_BUCKETS = (
    ("0.000-0.005", 0.000, 0.005),
    ("0.005-0.015", 0.005, 0.015),
    ("0.015-0.030", 0.015, 0.030),
    ("0.030-0.060", 0.030, 0.060),
    ("0.060+",      0.060, 1.000),
)


def add_parser(p) -> None:
    add_common_args(p)
    p.add_argument("--direction", choices=("YES", "NO"), default=None,
                   help="filter to one direction")


def run(args, conn) -> Section:
    extra, params = build_filters(args, prefix="")
    if args.direction:
        extra += " AND direction = ?"
        params.append(args.direction)
    sql = f"""
        SELECT
            mkt_vol_30m AS v,
            realized_pnl,
            mkt_drift_30m
        FROM trades
        WHERE {closed_trades_filter()}
          AND mkt_vol_30m IS NOT NULL
          {extra}
    """
    try:
        records = conn.execute(sql, params).fetchall()
    except sqlite3.OperationalError as exc:
        return Section(
            title="vol-buckets — pre-headline volatility × PnL",
            headers=["bucket", "n", "W", "L", "hit", "sum_pnl", "avg_pnl"],
            rows=[],
            notes=[f"schema missing mkt_vol_30m ({exc}); snapshot predates cycle-11."],
        )

    rows = []
    total_n = 0
    for label, lo, hi in _BUCKETS:
        subset = [r for r in records if lo <= r["v"] < hi]
        n = len(subset)
        if n == 0:
            rows.append([label, 0, 0, 0, "—", "—", "—"])
            continue
        wins = sum(1 for r in subset if r["realized_pnl"] > 0)
        losses = sum(1 for r in subset if r["realized_pnl"] <= 0)
        sum_pnl = sum(r["realized_pnl"] for r in subset)
        avg_pnl = sum_pnl / n
        hit = compute_hit_pct(wins, losses)
        total_n += n
        rows.append([
            label, n, wins, losses,
            f"{hit:.1f}%" if hit is not None else "—",
            round(sum_pnl, 2), round(avg_pnl, 3),
        ])
    notes = []
    if total_n < 30:
        notes.append(f"low sample (n={total_n}); buckets are suggestive only.")
    suffix = f" (direction={args.direction})" if args.direction else ""
    return Section(
        title=f"vol-buckets{suffix} — closed-trade outcomes by 30m pre-headline stddev",
        headers=["bucket", "n", "W", "L", "hit", "sum_pnl", "avg_pnl"],
        rows=rows,
        notes=notes,
    )
