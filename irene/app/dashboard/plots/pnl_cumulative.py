"""Cumulative realised PnL over time — one line per decision_path.

The headline P3 visual: does the DKG path's cumulative PnL pull away from
MVP, stay flat, or trail? X axis is close-time, Y axis is the running sum
of `trade_closures.realized_pnl_usdc`. Non-tracer trades only.
"""

from __future__ import annotations

from ._base import figure_to_png, plt

FILENAME = "pnl_cumulative.png"

_PATH_COLOURS = {
    "mvp": "#4488cc",
    "dkg": "#d09844",
}


async def render(dash, *, loop) -> bytes:
    cur = await dash.db.conn.execute(
        """
        SELECT t.decision_path     AS path,
               c.closed_at         AS closed_at,
               c.realized_pnl_usdc AS pnl
        FROM trade_closures c
        JOIN trades t ON t.id = c.trade_id
        WHERE t.is_tracer = 0
        ORDER BY c.closed_at ASC
        """
    )
    rows = await cur.fetchall()
    await cur.close()

    if not rows:
        # Render a small placeholder so the dashboard doesn't 404.
        return await loop.run_in_executor(None, _draw_placeholder)

    by_path: dict[str, list[tuple[str, float]]] = {}
    for r in rows:
        if r["closed_at"] is None or r["pnl"] is None:
            continue
        by_path.setdefault(r["path"] or "mvp", []).append(
            (r["closed_at"], float(r["pnl"]))
        )
    if not by_path:
        return await loop.run_in_executor(None, _draw_placeholder)
    return await loop.run_in_executor(None, _draw, by_path)


def _draw(by_path: dict[str, list[tuple[str, float]]]) -> bytes:
    fig, ax = plt.subplots(figsize=(8, 3.5))
    for path in sorted(by_path.keys()):
        series = by_path[path]
        xs = list(range(len(series)))
        ys: list[float] = []
        running = 0.0
        for _, pnl in series:
            running += pnl
            ys.append(running)
        ax.plot(
            xs, ys, "-o",
            color=_PATH_COLOURS.get(path, "#888888"),
            linewidth=2,
            markersize=3,
            label=f"{path} (n={len(series)}, sum={running:+.2f})",
        )
    ax.axhline(0.0, color="#888888", linestyle="--", linewidth=0.8, alpha=0.6)
    ax.set_xlabel("close index (chronological)")
    ax.set_ylabel("cumulative realised PnL (USDC)")
    ax.set_title("Cumulative realised PnL per decision_path")
    ax.legend(facecolor="#222222", edgecolor="#444444", fontsize=8)
    return figure_to_png(fig)


def _draw_placeholder() -> bytes:
    fig, ax = plt.subplots(figsize=(8, 3.2))
    ax.text(
        0.5, 0.5, "no closed trades yet",
        ha="center", va="center", fontsize=14, color="#888888",
        transform=ax.transAxes,
    )
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_title("Cumulative realised PnL per decision_path")
    return figure_to_png(fig)
