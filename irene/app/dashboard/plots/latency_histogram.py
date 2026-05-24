"""Log-x histogram of end-to-end latency (article ingest → trade)."""

from __future__ import annotations

import math

from ..db_helpers import end_to_end_latency_ms
from ._base import figure_to_png, plt

FILENAME = "latency_histogram.png"


async def render(dash, *, loop) -> bytes:
    samples = await end_to_end_latency_ms(dash.db)
    if not samples:
        return b""
    return await loop.run_in_executor(None, _draw, samples)


def _draw(samples: list[float]) -> bytes:
    pos = [s for s in samples if s > 0]
    if not pos:
        return b""
    lo = max(min(pos), 1.0)
    hi = max(pos)
    edges = [10 ** x for x in _logspace(math.log10(lo), math.log10(hi), 30)]
    fig, ax = plt.subplots(figsize=(8, 3.2))
    ax.hist(pos, bins=edges, color="#5cb085", edgecolor="#222222")
    ax.set_xscale("log")
    ax.set_xlabel("ingest → trade (ms, log scale)")
    ax.set_ylabel("count (last 1h)")
    ax.set_title(f"End-to-end latency histogram — n={len(samples)}")
    return figure_to_png(fig)


def _logspace(lo: float, hi: float, n: int) -> list[float]:
    if n <= 1 or hi <= lo:
        return [lo, hi]
    step = (hi - lo) / (n - 1)
    return [lo + i * step for i in range(n)]
