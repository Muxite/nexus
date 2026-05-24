"""Cumulative realized PnL line over the current cycle.

Queries the DB directly (heavy cache doesn't carry per-trade rows). Runs
on the RO clone if available; otherwise on the writer connection.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from ._base import figure_to_png, plt

logger = logging.getLogger(__name__)

FILENAME = "equity_curve.png"


async def render(dash, *, loop) -> bytes:
    db = dash._db_ro or dash._db
    if db is None:
        return b""
    cutoff = dash._cycle_started_at
    if cutoff is None:
        # Fallback: last 24h if cycle scope isn't set.
        cutoff = (datetime.now(timezone.utc).replace(microsecond=0)
                  .isoformat()).replace("T", " ")
        cutoff = None  # let the SQL use a 24h window instead

    try:
        if cutoff:
            sql = (
                "SELECT closed_at, realized_pnl FROM trades "
                "WHERE realized_pnl IS NOT NULL AND closed_at >= ? "
                "ORDER BY closed_at ASC"
            )
            cur = await db._conn.execute(sql, (cutoff,))
        else:
            sql = (
                "SELECT closed_at, realized_pnl FROM trades "
                "WHERE realized_pnl IS NOT NULL "
                "  AND closed_at >= datetime('now','-24 hours') "
                "ORDER BY closed_at ASC"
            )
            cur = await db._conn.execute(sql)
        rows = await cur.fetchall()
        await cur.close()
    except Exception as exc:  # noqa: BLE001
        logger.warning("[plot] equity_curve query failed: %s", exc)
        return b""

    if not rows:
        return b""

    xs = [r["closed_at"][:19] for r in rows]
    ys = []
    cum = 0.0
    for r in rows:
        cum += float(r["realized_pnl"] or 0.0)
        ys.append(cum)
    return await loop.run_in_executor(None, _draw, xs, ys, cutoff or "last 24h")


def _draw(xs, ys, cutoff_label: str) -> bytes:
    fig, ax = plt.subplots(figsize=(8, 3.2))
    color = "#5cb085" if (ys and ys[-1] >= 0) else "#d04444"
    ax.plot(range(len(ys)), ys, "-", color=color, linewidth=2)
    ax.fill_between(range(len(ys)), 0, ys, color=color, alpha=0.2)
    ax.axhline(0, color="#888888", linewidth=0.6)
    ax.set_ylabel("cumulative realized PnL ($)")
    ax.set_xlabel(f"closed trade #  (cycle from {cutoff_label})")
    final = ys[-1] if ys else 0.0
    sign = "+" if final >= 0 else ""
    ax.set_title(f"Equity curve — {len(ys)} closed trades, total {sign}${final:.2f}")
    return figure_to_png(fig)
