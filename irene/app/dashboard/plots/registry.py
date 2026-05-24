"""Single source of truth for irene's PNG plots.

Importing this module loads matplotlib. The Dashboard wrapper catches
ImportError so a missing dep never crashes the text-file tier.
"""

from __future__ import annotations

from . import (
    funnel_dropoff,
    latency_histogram,
    latency_timeseries,
    news_staleness,
    per_gate_rejection,
    pnl_cumulative,
)

PLOTS = [
    latency_histogram,
    latency_timeseries,
    funnel_dropoff,
    news_staleness,
    per_gate_rejection,
    pnl_cumulative,
]
