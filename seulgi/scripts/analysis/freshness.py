"""freshness — outcome by freshness label (BREAKING/EVOLVING/REHASH/STALE_RETURN)."""
from __future__ import annotations

from scripts.analysis._common import (
    add_common_args, build_filters, closed_trades_filter, compute_hit_pct,
)
from scripts.analysis._format import Section


def add_parser(p) -> None:
    add_common_args(p)


_ORDER = {"BREAKING": 0, "EVOLVING": 1, "STALE_RETURN": 2, "REHASH": 3, "NONE": 4}


def run(args, conn) -> Section:
    extra, params = build_filters(args, prefix="")
    sql = f"""
        SELECT COALESCE(freshness_label, 'NONE') AS label,
            COUNT(*) AS n,
            SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END)  AS wins,
            SUM(CASE WHEN realized_pnl <= 0 THEN 1 ELSE 0 END) AS losses,
            ROUND(SUM(realized_pnl), 2)                        AS sum_pnl,
            ROUND(AVG(realized_pnl), 3)                        AS avg_pnl
        FROM trades
        WHERE {closed_trades_filter()}{extra}
        GROUP BY label
    """
    raw = [dict(r) for r in conn.execute(sql, params).fetchall()]
    raw.sort(key=lambda r: _ORDER.get(r["label"], 99))
    rows = []
    for r in raw:
        hit = compute_hit_pct(r["wins"], r["losses"])
        rows.append([r["label"], r["n"], r["wins"], r["losses"],
                     f"{hit:.1f}%" if hit is not None else "—",
                     r["sum_pnl"], r["avg_pnl"]])
    notes = []
    if all(r[0] == "NONE" for r in rows):
        notes.append("All closed trades pre-date cycle-9 freshness "
                     "labelling; rerun once new trades close.")
    return Section(
        title="freshness — closed-trade outcomes",
        headers=["label", "n", "W", "L", "hit", "sum_pnl", "avg_pnl"],
        rows=rows,
        notes=notes,
    )
