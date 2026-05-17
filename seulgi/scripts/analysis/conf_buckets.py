"""conf-buckets — outcome by AI confidence bucket."""
from __future__ import annotations

from scripts.analysis._common import (
    add_common_args, build_filters, closed_trades_filter, compute_hit_pct,
)
from scripts.analysis._format import Section


def add_parser(p) -> None:
    add_common_args(p)


def run(args, conn) -> Section:
    extra, params = build_filters(args, prefix="")
    sql = f"""
        SELECT
            CASE
                WHEN confidence_raw >= 0.85 THEN '0.85-1.0'
                WHEN confidence_raw >= 0.70 THEN '0.70-0.85'
                WHEN confidence_raw >= 0.55 THEN '0.55-0.70'
                WHEN confidence_raw >= 0.40 THEN '0.40-0.55'
                ELSE '0.00-0.40'
            END AS bucket,
            COUNT(*) AS n,
            SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END)  AS wins,
            SUM(CASE WHEN realized_pnl <= 0 THEN 1 ELSE 0 END) AS losses,
            ROUND(SUM(realized_pnl), 2)                        AS sum_pnl,
            ROUND(AVG(realized_pnl), 3)                        AS avg_pnl
        FROM trades
        WHERE {closed_trades_filter()}{extra}
        GROUP BY bucket
        ORDER BY bucket DESC
    """
    rows = []
    for r in conn.execute(sql, params).fetchall():
        hit = compute_hit_pct(r["wins"], r["losses"])
        rows.append([r["bucket"], r["n"], r["wins"], r["losses"],
                     f"{hit:.1f}%" if hit is not None else "—",
                     r["sum_pnl"], r["avg_pnl"]])
    return Section(
        title="confidence buckets — closed-trade outcomes",
        headers=["bucket", "n", "W", "L", "hit", "sum_pnl", "avg_pnl"],
        rows=rows,
    )
