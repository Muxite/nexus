"""close-reasons — TP/SL/TIMEOUT/TRAIL breakdown."""
from __future__ import annotations

from scripts.analysis._common import add_common_args, build_filters, closed_trades_filter
from scripts.analysis._format import Section


def add_parser(p) -> None:
    add_common_args(p)


def run(args, conn) -> Section:
    extra, params = build_filters(args, prefix="")
    sql = f"""
        SELECT
            COALESCE(close_reason, 'unknown') AS r,
            COUNT(*)                          AS n,
            SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN realized_pnl <= 0 THEN 1 ELSE 0 END) AS losses,
            ROUND(SUM(realized_pnl), 2)       AS sum_pnl,
            ROUND(AVG(realized_pnl), 3)       AS avg_pnl
        FROM trades
        WHERE {closed_trades_filter()}{extra}
        GROUP BY r ORDER BY n DESC
    """
    rows = [[r["r"], r["n"], r["wins"], r["losses"], r["sum_pnl"], r["avg_pnl"]]
            for r in conn.execute(sql, params).fetchall()]
    return Section(
        title="close reasons — TP / SL / TIMEOUT / TRAIL",
        headers=["reason", "n", "W", "L", "sum_pnl", "avg_pnl"],
        rows=rows,
    )
