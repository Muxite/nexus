"""variants — per-variant leaderboard (closed trades)."""
from __future__ import annotations

from scripts.analysis._common import (
    add_common_args, build_filters, closed_trades_filter, compute_hit_pct,
)
from scripts.analysis._format import Section


def add_parser(p) -> None:
    add_common_args(p)


def run(args, conn) -> Section:
    extra, params = build_filters(args, prefix="")
    limit = args.limit or 50
    sql = f"""
        SELECT
            COALESCE(variant_name, engine_name, 'default') AS variant,
            COUNT(*)                                       AS n,
            SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END)  AS wins,
            SUM(CASE WHEN realized_pnl <= 0 THEN 1 ELSE 0 END) AS losses,
            ROUND(SUM(realized_pnl), 2)                    AS sum_pnl,
            ROUND(AVG(CASE WHEN realized_pnl > 0 THEN realized_pnl END), 3) AS avg_win,
            ROUND(AVG(CASE WHEN realized_pnl <= 0 THEN realized_pnl END), 3) AS avg_loss,
            ROUND(SUM(size_usdc), 2)                       AS staked
        FROM trades
        WHERE {closed_trades_filter()}{extra}
        GROUP BY variant
        ORDER BY sum_pnl DESC
        LIMIT ?
    """
    rows = []
    for r in conn.execute(sql, params + [limit]).fetchall():
        hit = compute_hit_pct(r["wins"], r["losses"])
        rows.append([
            r["variant"], r["n"], r["wins"], r["losses"],
            f"{hit:.1f}%" if hit is not None else "—",
            r["sum_pnl"], r["avg_win"], r["avg_loss"], r["staked"],
        ])
    return Section(
        title="variants — closed trade outcomes (sorted by sum PnL)",
        headers=["variant", "n", "W", "L", "hit", "sum_pnl",
                 "avg_win", "avg_loss", "staked"],
        rows=rows,
    )
