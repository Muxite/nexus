"""Realised PnL per decision_path (mvp vs dkg).

Surfaces the headline question for P3: is the DKG path actually generating
more (or better) trades than the legacy MVP path? Reads
`trades LEFT JOIN trade_closures` so the same query gives us:
  - per-path counts (total / closed / win%) plus PnL stats on closed rows,
  - per-path open-position counts and mean age,
  - a tail of recent DKG trades (the new path is what we're watching).

Tracer trades are excluded — they are synthetic and would skew stats.
"""

from __future__ import annotations

from ..io import _now, _percentiles

FILENAME = "pnl.txt"


async def render(dash) -> str:
    lines = [
        f"# irene pnl ({_now().isoformat()})",
        "  realised pnl per decision_path; closed trades only; non-tracer",
        "",
    ]

    # ---- per-path summary: counts + PnL on closed trades --------------------
    cur = await dash.db.conn.execute(
        """
        SELECT t.decision_path     AS path,
               t.id                AS trade_id,
               c.realized_pnl_usdc AS pnl
        FROM trades t
        LEFT JOIN trade_closures c ON c.trade_id = t.id
        WHERE t.is_tracer = 0
        """
    )
    rows = await cur.fetchall()
    await cur.close()

    if not rows:
        lines.append("  (no trades yet)")
        return "\n".join(lines) + "\n"

    by_path: dict[str, dict[str, list]] = {}
    for r in rows:
        path = r["path"] or "mvp"
        bucket = by_path.setdefault(path, {"total": [], "pnls": []})
        bucket["total"].append(r["trade_id"])
        if r["pnl"] is not None:
            bucket["pnls"].append(float(r["pnl"]))

    lines.append(
        f"  {'path':<5} {'trades':<6} {'closed':<6} "
        f"{'win%':<6} {'mean':<7} {'median':<8} {'sum':<8}"
    )
    for path in sorted(by_path.keys()):
        bucket = by_path[path]
        total = len(bucket["total"])
        pnls = bucket["pnls"]
        closed = len(pnls)
        if closed:
            wins = sum(1 for p in pnls if p > 0)
            win_pct = wins / closed
            mean = sum(pnls) / closed
            median = _percentiles(pnls)["p50"]
            total_pnl = sum(pnls)
            lines.append(
                f"  {path:<5} {total:<6d} {closed:<6d} "
                f"{win_pct:<6.3f} {mean:<+7.3f} {median:<+8.3f} {total_pnl:<+8.3f}"
            )
        else:
            lines.append(
                f"  {path:<5} {total:<6d} {0:<6d} "
                f"{'-':<6} {'-':<7} {'-':<8} {'-':<8}"
            )

    # ---- open positions per path -------------------------------------------
    cur = await dash.db.conn.execute(
        """
        SELECT t.decision_path AS path,
               (julianday('now') - julianday(t.decided_at)) * 24.0 AS age_h
        FROM trades t
        LEFT JOIN trade_closures c ON c.trade_id = t.id
        WHERE t.is_tracer = 0 AND c.trade_id IS NULL
        """
    )
    open_rows = await cur.fetchall()
    await cur.close()

    lines.append("")
    lines.append("  open positions:")
    lines.append(f"  {'path':<5} {'open':<5} {'mean_age_h':<10}")
    open_by_path: dict[str, list[float]] = {}
    for r in open_rows:
        open_by_path.setdefault(r["path"] or "mvp", []).append(
            float(r["age_h"]) if r["age_h"] is not None else 0.0
        )
    if not open_by_path:
        lines.append("  (no open positions)")
    else:
        for path in sorted(open_by_path.keys()):
            ages = open_by_path[path]
            mean_age = sum(ages) / len(ages) if ages else 0.0
            lines.append(f"  {path:<5} {len(ages):<5d} {mean_age:<10.1f}")

    # ---- recent dkg trades --------------------------------------------------
    cur = await dash.db.conn.execute(
        """
        SELECT t.decided_at, t.market_id, t.direction, t.mid_price, t.edge,
               t.size_usdc, c.realized_pnl_usdc AS pnl, c.trade_id AS closed
        FROM trades t
        LEFT JOIN trade_closures c ON c.trade_id = t.id
        WHERE t.is_tracer = 0 AND t.decision_path = 'dkg'
        ORDER BY t.decided_at DESC
        LIMIT 5
        """
    )
    dkg_rows = await cur.fetchall()
    await cur.close()

    lines.append("")
    lines.append("  recent dkg trades (last 5):")
    if not dkg_rows:
        lines.append("  (no dkg trades yet)")
    else:
        lines.append(
            f"    {'when':<20} {'mkt':<10} {'dir':<4} "
            f"{'mid':<6} {'edge':<7} {'size$':<6} {'pnl':<8} status"
        )
        for r in dkg_rows:
            short_mkt = (r["market_id"] or "")[:8]
            if r["closed"] is not None:
                pnl_str = f"{float(r['pnl']):+.3f}"
                status = "closed"
            else:
                pnl_str = "-"
                status = "open"
            lines.append(
                f"    {r['decided_at'][:19]:<20} "
                f"{short_mkt:<10} "
                f"{r['direction']:<4} "
                f"{r['mid_price']:<6.3f} "
                f"{r['edge']:<7.3f} "
                f"{r['size_usdc']:<6.2f} "
                f"{pnl_str:<8} "
                f"{status}"
            )

    return "\n".join(lines) + "\n"
