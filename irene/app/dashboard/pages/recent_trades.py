"""Last 20 trade decisions with their decision chain."""

from __future__ import annotations

from ..io import _now

FILENAME = "recent_trades.txt"


async def render(dash) -> str:
    lines = [
        f"# irene recent trades ({_now().isoformat()})",
        "  last 20, newest first",
        "",
        f"  {'when':<20} {'mkt':<10} {'path':<5} {'dir':<4} "
        f"{'mid':<6} {'edge':<7} {'size$':<7} {'pnl':<8} status",
    ]
    cur = await dash.db.conn.execute(
        """
        SELECT t.decided_at, t.market_id, t.decision_path, t.direction,
               t.mid_price, t.edge, t.size_usdc,
               c.realized_pnl_usdc AS pnl, c.trade_id AS closed
        FROM trades t
        LEFT JOIN trade_closures c ON c.trade_id = t.id
        ORDER BY t.decided_at DESC
        LIMIT 20
        """
    )
    rows = await cur.fetchall()
    await cur.close()
    if not rows:
        lines.append("  (no trades yet)")
        return "\n".join(lines) + "\n"
    for r in rows:
        short_mkt = (r["market_id"] or "")[:8]
        if r["closed"] is not None:
            pnl_str = f"{float(r['pnl']):+.3f}"
            status = "closed"
        else:
            pnl_str = "-"
            status = "open"
        lines.append(
            f"  {r['decided_at'][:19]:<20} "
            f"{short_mkt:<10} "
            f"{(r['decision_path'] or 'mvp'):<5} "
            f"{r['direction']:<4} "
            f"{r['mid_price']:<6.3f} "
            f"{r['edge']:<7.3f} "
            f"{r['size_usdc']:<7.2f} "
            f"{pnl_str:<8} "
            f"{status}"
        )
    return "\n".join(lines) + "\n"
