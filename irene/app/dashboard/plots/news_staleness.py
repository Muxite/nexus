"""Histogram of (decided_at - article.published_at), in seconds, last 24h."""

from __future__ import annotations

import math

from ._base import figure_to_png, plt

FILENAME = "news_staleness.png"


async def render(dash, *, loop) -> bytes:
    cur = await dash.db.conn.execute(
        """
        SELECT (julianday(t.decided_at) - julianday(a.published_at)) * 86400.0 AS sec
        FROM trades t
        JOIN signals s ON s.id = t.signal_id
        JOIN articles a ON a.id = s.article_id
        WHERE t.decided_at >= datetime('now','-24 hours')
          AND a.published_at IS NOT NULL
        """
    )
    rows = await cur.fetchall()
    await cur.close()
    samples = [float(r["sec"]) for r in rows if r["sec"] is not None and r["sec"] >= 0]
    if not samples:
        return b""
    return await loop.run_in_executor(None, _draw, samples)


def _draw(samples: list[float]) -> bytes:
    lo = max(min(samples), 0.5)
    hi = max(samples)
    n = 30
    if hi > lo:
        step = (math.log10(hi) - math.log10(lo)) / (n - 1)
        edges = [10 ** (math.log10(lo) + i * step) for i in range(n)]
    else:
        edges = 20

    fig, ax = plt.subplots(figsize=(8, 3.2))
    ax.hist(samples, bins=edges, color="#4488cc", edgecolor="#222222")
    if hi > lo:
        ax.set_xscale("log")
    ax.set_xlabel("news age at trade (seconds, log)")
    ax.set_ylabel("count (last 24h)")
    ax.set_title(
        f"News staleness — n={len(samples)}  "
        f"(speed thesis: small = winning the race)"
    )
    return figure_to_png(fig)
