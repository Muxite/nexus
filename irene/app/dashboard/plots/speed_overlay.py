"""Per-stage latency overlay — p50 / p95 / p99 for every adjacent-stage
diff (ingest→dedupe, ingest→match, ingest→trade, match→trade, news-age)
plotted as grouped bars on a single chart with log-scale y.

Why log scale: ingest→dedupe is usually ~50ms; news-age-at-trade can be
hours (3.6M ms). Linear axis would crush the fast stages flat.

Why grouped bars instead of lines: only one snapshot per render — these
are samples over the last 1h, not a time series. Bars per stage cleanly
show the spread between p50, p95, p99 inside each stage.

Cycle 6a (2026-05-25)."""

from __future__ import annotations

from ..db_helpers import fetch_ms, latency_panel_sqls
from ..io import _percentiles
from ._base import figure_to_png, plt

FILENAME = "speed_overlay.png"


async def render(dash, *, loop) -> bytes:
    # Fetch ms samples per stage using the shared SQL helpers.
    stages: list[tuple[str, dict[str, float]]] = []
    for label, sql in latency_panel_sqls(cycle_id=dash.current_cycle_id):
        samples = await fetch_ms(dash.db, sql)
        p = _percentiles(samples)
        if p["count"] == 0:
            continue
        stages.append((label, p))

    if not stages:
        return b""

    return await loop.run_in_executor(None, _draw, stages)


def _draw(stages: list[tuple[str, dict[str, float]]]) -> bytes:
    labels = [s[0] for s in stages]
    p50 = [max(s[1]["p50"], 0.5) for s in stages]
    p95 = [max(s[1]["p95"], 0.5) for s in stages]
    p99 = [max(s[1]["p99"], 0.5) for s in stages]

    n = len(stages)
    fig, ax = plt.subplots(figsize=(9, 3.8))

    bar_w = 0.27
    xs = list(range(n))
    # Three side-by-side bars per stage. Offset by ±bar_w so the trio
    # is centered on its tick.
    ax.bar([x - bar_w for x in xs], p50, width=bar_w, color="#5cb085",
           edgecolor="#222222", label="p50")
    ax.bar(xs,                       p95, width=bar_w, color="#d09844",
           edgecolor="#222222", label="p95")
    ax.bar([x + bar_w for x in xs], p99, width=bar_w, color="#d04444",
           edgecolor="#222222", label="p99")

    ax.set_yscale("log")
    ax.set_xticks(xs)
    ax.set_xticklabels(labels, rotation=20, ha="right")
    ax.set_ylabel("latency (ms, log)")
    ax.set_title("Per-stage latency overlay — p50 / p95 / p99 (last 1h)")
    ax.legend(facecolor="#222222", edgecolor="#444444", fontsize=8, loc="upper left")

    # Annotate each bar with its numeric value so you can read off
    # the actual milliseconds without squinting at the log axis.
    for i, (vp50, vp95, vp99) in enumerate(zip(p50, p95, p99)):
        for offset, v in ((-bar_w, vp50), (0, vp95), (bar_w, vp99)):
            if v >= 1.0:
                ax.text(i + offset, v * 1.15, f"{int(v)}",
                        ha="center", va="bottom", fontsize=7, color="#dddddd")

    return figure_to_png(fig)
