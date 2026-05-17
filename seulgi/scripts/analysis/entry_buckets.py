"""entry-buckets — outcome by entry-price slice (the disaster-zone view)."""
from __future__ import annotations

from scripts.analysis._common import (
    add_common_args, build_filters, closed_trades_filter, compute_hit_pct,
)
from scripts.analysis._format import Section


def add_parser(p) -> None:
    add_common_args(p)
    p.add_argument("--direction", choices=("YES", "NO"), default=None,
                   help="filter to one direction (default: both)")


def run(args, conn) -> Section:
    extra, params = build_filters(args, prefix="")
    if args.direction:
        extra += " AND direction = ?"
        params.append(args.direction)
    sql = f"""
        SELECT
            CASE
                WHEN mid_price < 0.10 THEN '<0.10'
                WHEN mid_price < 0.25 THEN '0.10-0.25'
                WHEN mid_price < 0.50 THEN '0.25-0.50'
                WHEN mid_price < 0.75 THEN '0.50-0.75'
                WHEN mid_price < 0.90 THEN '0.75-0.90'
                ELSE '>0.90'
            END AS bucket,
            COUNT(*)                                           AS n,
            SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END)  AS wins,
            SUM(CASE WHEN realized_pnl <= 0 THEN 1 ELSE 0 END) AS losses,
            ROUND(SUM(realized_pnl), 2)                        AS sum_pnl,
            ROUND(AVG(realized_pnl), 3)                        AS avg_pnl
        FROM trades
        WHERE {closed_trades_filter()}{extra}
        GROUP BY bucket
        ORDER BY bucket
    """
    rows = []
    for r in conn.execute(sql, params).fetchall():
        hit = compute_hit_pct(r["wins"], r["losses"])
        rows.append([r["bucket"], r["n"], r["wins"], r["losses"],
                     f"{hit:.1f}%" if hit is not None else "—",
                     r["sum_pnl"], r["avg_pnl"]])
    suffix = f" (direction={args.direction})" if args.direction else ""
    return Section(
        title=f"entry-price buckets{suffix} — closed-trade outcomes",
        headers=["entry_mid", "n", "W", "L", "hit", "sum_pnl", "avg_pnl"],
        rows=rows,
    )
