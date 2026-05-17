"""sources — per-news-source PnL with hit rate."""
from __future__ import annotations

from scripts.analysis._common import (
    add_common_args, build_filters, closed_trades_filter, compute_hit_pct,
)
from scripts.analysis._format import Section


def add_parser(p) -> None:
    add_common_args(p)
    p.add_argument("--min-n", type=int, default=1,
                   help="hide sources with fewer than this many closed trades")


def run(args, conn) -> Section:
    # build_filters expects unprefixed column names; price-impacts join uses
    # t.<col> so we hand-roll the filter here.
    where_extra = ""
    params: list = []
    if args.since:
        from scripts.analysis._common import parse_since
        cutoff = parse_since(args.since)
        if cutoff:
            where_extra += " AND t.decided_at >= ?"
            params.append(cutoff)
    if args.variant:
        where_extra += " AND COALESCE(t.variant_name, t.engine_name, '') LIKE ?"
        params.append(args.variant)

    limit = args.limit or 30
    sql = f"""
        SELECT s.site_url AS src,
            COUNT(*) AS n,
            SUM(CASE WHEN t.realized_pnl > 0 THEN 1 ELSE 0 END)  AS wins,
            SUM(CASE WHEN t.realized_pnl <= 0 THEN 1 ELSE 0 END) AS losses,
            ROUND(SUM(t.realized_pnl), 2)                        AS sum_pnl
        FROM trades t LEFT JOIN signals s ON s.id = t.signal_id
        WHERE {closed_trades_filter("t.")}{where_extra}
        GROUP BY src
        HAVING n >= ?
        ORDER BY sum_pnl DESC
        LIMIT ?
    """
    params = params + [args.min_n, limit]
    rows = []
    for r in conn.execute(sql, params).fetchall():
        hit = compute_hit_pct(r["wins"], r["losses"])
        src = (r["src"] or "unknown").replace("https://", "").replace("http://", "")
        rows.append([src[:46], r["n"], r["wins"], r["losses"],
                     f"{hit:.1f}%" if hit is not None else "—", r["sum_pnl"]])
    return Section(
        title=f"sources — closed-trade outcomes (min_n={args.min_n})",
        headers=["source", "n", "W", "L", "hit", "sum_pnl"],
        rows=rows,
    )
