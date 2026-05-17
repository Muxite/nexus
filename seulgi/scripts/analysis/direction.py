"""direction — YES/NO × inverted breakdown."""
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
        SELECT direction, COALESCE(inverted, 0) AS inv,
            COUNT(*) AS n,
            SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END)  AS wins,
            SUM(CASE WHEN realized_pnl <= 0 THEN 1 ELSE 0 END) AS losses,
            ROUND(SUM(realized_pnl), 2)                        AS sum_pnl
        FROM trades
        WHERE {closed_trades_filter()}{extra}
        GROUP BY direction, inv
        ORDER BY sum_pnl DESC
    """
    rows = []
    for r in conn.execute(sql, params).fetchall():
        hit = compute_hit_pct(r["wins"], r["losses"])
        rows.append([r["direction"], r["inv"], r["n"], r["wins"], r["losses"],
                     f"{hit:.1f}%" if hit is not None else "—", r["sum_pnl"]])
    return Section(
        title="direction × inverted — closed-trade outcomes",
        headers=["dir", "inv", "n", "W", "L", "hit", "sum_pnl"],
        rows=rows,
    )
