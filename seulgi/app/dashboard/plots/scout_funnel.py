"""Skip-reason bar chart for scout engines (from the 5-min cache)."""

from __future__ import annotations

from ._base import figure_to_png, plt

FILENAME = "scout_funnel.png"


async def render(dash, *, loop) -> bytes:
    cached = dash._heavy_cache.get("scout_funnel")
    if not cached:
        return b""
    stats = cached.get("stats") or {}
    # `compute_funnel` returns a dict of stage → count. Drop the totals row
    # and anything zero so the bar chart isn't dominated by silence.
    rows = [
        (k.replace("_", " "), int(v))
        for k, v in stats.items()
        if isinstance(v, (int, float)) and v > 0 and k != "total"
    ]
    if not rows:
        return b""
    return await loop.run_in_executor(None, _draw, rows)


def _draw(rows: list[tuple[str, int]]) -> bytes:
    rows = sorted(rows, key=lambda r: r[1], reverse=True)[:18]
    labels = [r[0] for r in reversed(rows)]
    values = [r[1] for r in reversed(rows)]

    fig, ax = plt.subplots(figsize=(8, max(2.5, 0.32 * len(rows))))
    ax.barh(labels, values, color="#d09844", edgecolor="#222222")
    ax.set_xlabel("count (cycle-scoped)")
    ax.set_title("Scout funnel — skip-reason breakdown")
    return figure_to_png(fig)
