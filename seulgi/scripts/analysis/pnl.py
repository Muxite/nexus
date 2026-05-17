"""pnl — overall realized PnL summary for executed trades."""
from __future__ import annotations

from scripts.analysis._common import add_common_args, build_filters, closed_trades_filter
from scripts.analysis._format import Section


def add_parser(p) -> None:
    add_common_args(p)


def run(args, conn) -> Section:
    extra, params = build_filters(args, prefix="")
    sql = f"""
        SELECT
            COUNT(*) AS n,
            SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN realized_pnl <= 0 THEN 1 ELSE 0 END) AS losses,
            ROUND(SUM(realized_pnl), 2)                       AS sum_pnl,
            ROUND(AVG(realized_pnl), 4)                       AS avg_pnl,
            ROUND(AVG(CASE WHEN realized_pnl > 0 THEN realized_pnl END), 3) AS avg_win,
            ROUND(AVG(CASE WHEN realized_pnl <= 0 THEN realized_pnl END), 3) AS avg_loss,
            ROUND(MAX(realized_pnl), 2)                       AS best,
            ROUND(MIN(realized_pnl), 2)                       AS worst,
            ROUND(SUM(size_usdc), 2)                          AS staked
        FROM trades
        WHERE {closed_trades_filter()}{extra}
    """
    row = conn.execute(sql, params).fetchone()
    closed = (row["wins"] or 0) + (row["losses"] or 0)
    hit = (row["wins"] / closed * 100.0) if closed else None

    rows = [
        ["closed_n",   row["n"] or 0],
        ["wins",       row["wins"] or 0],
        ["losses",     row["losses"] or 0],
        ["hit_pct",    f"{hit:.1f}%" if hit is not None else "—"],
        ["sum_pnl",    row["sum_pnl"]],
        ["avg_pnl",    row["avg_pnl"]],
        ["avg_win",    row["avg_win"]],
        ["avg_loss",   row["avg_loss"]],
        ["best",       row["best"]],
        ["worst",      row["worst"]],
        ["staked",     row["staked"]],
    ]
    return Section(
        title="PnL — closed trades",
        headers=["metric", "value"],
        rows=rows,
        aligns=["l", "r"],
        summary=f"filters: since={args.since or 'none'}  variant={args.variant or 'all'}",
    )
