"""p50 / p95 / p99 of end-to-end latency, hourly buckets over last 24h."""

from __future__ import annotations

from ..io import _percentiles
from ._base import figure_to_png, plt

FILENAME = "latency_timeseries.png"


async def render(dash, *, loop) -> bytes:
    cur = await dash.db.conn.execute(
        """
        SELECT strftime('%Y-%m-%dT%H:00', t.decided_at) AS hour,
               (julianday(t.decided_at) - julianday(a.ingested_at)) * 86400000.0 AS ms
        FROM trades t
        JOIN signals s ON s.id = t.signal_id
        JOIN articles a ON a.id = s.article_id
        WHERE t.decided_at >= datetime('now','-24 hours')
        """
    )
    rows = await cur.fetchall()
    await cur.close()
    if not rows:
        return b""
    by_hour: dict[str, list[float]] = {}
    for r in rows:
        if r["ms"] is None or r["ms"] < 0:
            continue
        by_hour.setdefault(r["hour"], []).append(float(r["ms"]))
    if not by_hour:
        return b""
    return await loop.run_in_executor(None, _draw, by_hour)


def _draw(by_hour: dict[str, list[float]]) -> bytes:
    hours = sorted(by_hour.keys())
    p50, p95, p99 = [], [], []
    for h in hours:
        p = _percentiles(by_hour[h])
        p50.append(p["p50"])
        p95.append(p["p95"])
        p99.append(p["p99"])

    fig, ax = plt.subplots(figsize=(8, 3.2))
    xs = list(range(len(hours)))
    ax.plot(xs, p50, "-o", color="#5cb085", linewidth=2, label="p50")
    ax.plot(xs, p95, "-o", color="#d09844", linewidth=2, label="p95")
    ax.plot(xs, p99, "-o", color="#d04444", linewidth=2, label="p99")
    ax.set_yscale("log")
    ax.set_xticks(xs)
    ax.set_xticklabels([h[-5:] for h in hours], rotation=45, ha="right")
    ax.set_ylabel("ingest → trade (ms, log)")
    ax.set_title("End-to-end latency over time (24h, hourly)")
    ax.legend(facecolor="#222222", edgecolor="#444444")
    return figure_to_png(fig)
