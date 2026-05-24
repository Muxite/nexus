"""Horizontal bar of rejection reasons over the last 24h."""

from __future__ import annotations

from ._base import figure_to_png, plt

FILENAME = "per_gate_rejection.png"


async def render(dash, *, loop) -> bytes:
    cs = f"AND cycle_id = {int(dash.current_cycle_id)}" if dash.current_cycle_id is not None else ""
    cur = await dash.db.conn.execute(
        f"""
        SELECT reason, COUNT(*) AS n
        FROM decisions
        WHERE outcome = 'reject'
          AND datetime(created_at) >= datetime('now','-24 hours') {cs}
        GROUP BY reason
        ORDER BY n DESC
        LIMIT 18
        """
    )
    rows = await cur.fetchall()
    await cur.close()
    if not rows:
        return b""
    return await loop.run_in_executor(None, _draw, rows)


def _draw(rows) -> bytes:
    labels = [r["reason"] for r in reversed(rows)]
    values = [int(r["n"]) for r in reversed(rows)]
    fig, ax = plt.subplots(figsize=(8, max(2.5, 0.35 * len(rows))))
    ax.barh(labels, values, color="#d09844", edgecolor="#222222")
    ax.set_xlabel("rejections (last 24h)")
    ax.set_title("Per-gate rejection breakdown")
    return figure_to_png(fig)
