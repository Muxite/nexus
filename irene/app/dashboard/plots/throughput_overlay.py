"""Hourly counts overlay — articles / signals / decisions / trades on a
single set of axes over the last 24h. Operators want to see at a glance
whether ingestion volume is keeping pace with downstream stages, and
where dropoffs happen (e.g. a signal-rich hour producing zero trades).

Uses log scale because the per-stage volume spreads several orders of
magnitude — typical hour: 1000 articles → 1000 signals → 200 decisions
→ 3 trades. Linear axis would compress trades to invisibility.

Cycle 6a (2026-05-25)."""

from __future__ import annotations

import asyncio

from ._base import figure_to_png, plt

FILENAME = "throughput_overlay.png"

# Each tuple: (label, color, sql-template). The {cs} placeholder gets a
# cycle filter when one is set. Hour buckets via strftime so we line
# up cross-stage on a common x-axis.
_STAGES = (
    ("articles ingested",  "#5cb085",
     "SELECT strftime('%Y-%m-%dT%H:00', ingested_at) AS bucket, COUNT(*) AS n "
     "FROM articles WHERE datetime(ingested_at) >= datetime('now','-24 hours') {cs} "
     "GROUP BY bucket ORDER BY bucket"),
    ("signals matched",    "#4a90d9",
     "SELECT strftime('%Y-%m-%dT%H:00', created_at) AS bucket, COUNT(*) AS n "
     "FROM signals WHERE datetime(created_at) >= datetime('now','-24 hours') {cs} "
     "GROUP BY bucket ORDER BY bucket"),
    ("decisions",          "#d09844",
     "SELECT strftime('%Y-%m-%dT%H:00', created_at) AS bucket, COUNT(*) AS n "
     "FROM decisions WHERE datetime(created_at) >= datetime('now','-24 hours') {cs} "
     "GROUP BY bucket ORDER BY bucket"),
    ("trades decided",     "#d04444",
     "SELECT strftime('%Y-%m-%dT%H:00', decided_at) AS bucket, COUNT(*) AS n "
     "FROM trades WHERE datetime(decided_at) >= datetime('now','-24 hours') {cs} "
     "GROUP BY bucket ORDER BY bucket"),
    ("trades closed",      "#9b59b6",
     "SELECT strftime('%Y-%m-%dT%H:00', closed_at) AS bucket, COUNT(*) AS n "
     "FROM trade_closures WHERE datetime(closed_at) >= datetime('now','-24 hours') {cs} "
     "GROUP BY bucket ORDER BY bucket"),
)


async def render(dash, *, loop) -> bytes:
    cs = f"AND cycle_id = {int(dash.current_cycle_id)}" if dash.current_cycle_id is not None else ""
    series: dict[str, dict[str, int]] = {}

    for label, color, sql_tpl in _STAGES:
        sql = sql_tpl.format(cs=cs)
        cur = await dash.db.conn.execute(sql)
        rows = await cur.fetchall()
        await cur.close()
        series[label] = {r["bucket"]: int(r["n"]) for r in rows if r["bucket"]}

    # Collect every distinct bucket across stages and sort once.
    all_buckets = sorted({b for s in series.values() for b in s})
    if not all_buckets:
        return b""

    return await loop.run_in_executor(None, _draw, all_buckets, series)


def _draw(buckets: list[str], series: dict[str, dict[str, int]]) -> bytes:
    xs = list(range(len(buckets)))
    fig, ax = plt.subplots(figsize=(9, 3.6))

    plotted = 0
    for label, color, _sql in _STAGES:
        ys = [series.get(label, {}).get(b, 0) for b in buckets]
        # Log-y plotting needs all-positive values. Replace 0 with 0.5
        # so empty hours show as a low (but visible) point, not absent.
        ys_log = [y if y > 0 else 0.5 for y in ys]
        ax.plot(xs, ys_log, "-o", color=color, linewidth=2, markersize=4, label=label)
        plotted += 1

    if plotted == 0:
        plt.close(fig)
        return b""

    ax.set_yscale("log")
    ax.set_xticks(xs)
    ax.set_xticklabels([b[-5:] for b in buckets], rotation=45, ha="right")
    ax.set_ylabel("rows / hour (log)")
    ax.set_title("Per-stage throughput overlay (last 24h, hourly)")
    ax.legend(facecolor="#222222", edgecolor="#444444", fontsize=8, loc="upper left")
    return figure_to_png(fig)
