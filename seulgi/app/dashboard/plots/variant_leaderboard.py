"""Horizontal bar of ROI per active variant, sorted descending."""

from __future__ import annotations

from datetime import datetime, timezone

from ._base import figure_to_png, plt

FILENAME = "variant_leaderboard.png"


async def render(dash, *, loop) -> bytes:
    v_stats = dash._heavy_cache.get("variant_stats") or []
    if dash._active_variants:
        v_stats = [
            r for r in v_stats
            if (r.get("variant") or "") in dash._active_variants
        ]
    if not v_stats:
        return b""

    # ROI = sum_pnl / 100 (per-engine $100 pot) → percent.
    rows = sorted(
        v_stats, key=lambda r: (r.get("sum_pnl") or 0), reverse=True,
    )[:20]
    return await loop.run_in_executor(None, _draw, rows)


def _draw(rows: list[dict]) -> bytes:
    names = [(r.get("variant") or "?")[:34] for r in reversed(rows)]
    rois = [(r.get("sum_pnl") or 0.0) for r in reversed(rows)]
    colors = ["#3aa66d" if v >= 0 else "#d04444" for v in rois]

    fig, ax = plt.subplots(figsize=(8, max(2.5, 0.35 * len(rows))))
    ax.barh(names, rois, color=colors, edgecolor="#222222")
    ax.axvline(0, color="#888888", linewidth=0.7)
    ax.set_xlabel("ROI on $100 pot ($)")
    ax.set_title(
        f"Variant leaderboard — active engines  "
        f"(rendered {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')})"
    )
    return figure_to_png(fig)
