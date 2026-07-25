"""Per-container CPU% and RAM over the last 60 min.

Two stacked axes sharing the x-axis (minutes-ago). One colored line per
container; colors are pinned so repeat renders stay visually stable.
Data comes from `container_stats` (populated by `app/system_stats.py`).
"""

from __future__ import annotations

from datetime import datetime, timezone

from ._base import figure_to_png, plt

FILENAME = "system_stats.png"

# Pinned per-container colors so the plot stays visually stable across
# renders. Anything not listed falls back to the default cycle.
_COLORS = {
    "irene":             "#5cb085",
    "nexus-ujin": "#6aa3d4",
}


async def render(dash, *, loop) -> bytes:
    cur = await dash.db.conn.execute(
        """
        SELECT ts, container, cpu_pct, mem_bytes
        FROM container_stats
        WHERE datetime(ts) >= datetime('now','-60 minutes')
        ORDER BY ts
        """
    )
    rows = [dict(r) for r in await cur.fetchall()]
    await cur.close()
    if not rows:
        return b""

    series: dict[str, list[tuple[datetime, float, float]]] = {}
    for r in rows:
        try:
            ts = datetime.fromisoformat(r["ts"])
        except ValueError:
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        series.setdefault(r["container"], []).append(
            (ts, float(r["cpu_pct"]), float(r["mem_bytes"])),
        )
    if not series:
        return b""

    now = datetime.now(timezone.utc)
    return await loop.run_in_executor(None, _draw, series, now)


def _draw(series, now):
    fig, (ax_cpu, ax_mem) = plt.subplots(2, 1, figsize=(8, 5), sharex=True)
    for name in sorted(series.keys()):
        pts = series[name]
        minutes_ago = [-(now - p[0]).total_seconds() / 60.0 for p in pts]
        cpu = [p[1] for p in pts]
        mem_mb = [p[2] / (1 << 20) for p in pts]
        color = _COLORS.get(name)
        ax_cpu.plot(minutes_ago, cpu, "-", color=color, linewidth=1.6, label=name)
        ax_mem.plot(minutes_ago, mem_mb, "-", color=color, linewidth=1.6, label=name)

    ax_cpu.set_ylabel("CPU %")
    ax_cpu.set_title("Container CPU & RAM (last 60 min)")
    ax_cpu.legend(facecolor="#222222", edgecolor="#444444", loc="upper left")
    ax_cpu.set_ylim(bottom=0)

    ax_mem.set_ylabel("RAM (MB)")
    ax_mem.set_xlabel("minutes ago")
    ax_mem.set_xlim(-60, 0)
    ax_mem.set_ylim(bottom=0)

    fig.tight_layout()
    return figure_to_png(fig)
