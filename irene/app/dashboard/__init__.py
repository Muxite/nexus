"""Irene dashboard package.

Periodic snapshot writer for `data/dashboard/*.txt` (operator landing files)
and `data/dashboard/plots/*.png` (visual diagnostics, regenerated every
5 min). Pull-based: operators `cat`, `less`, or `tail -f`; no HTTP, no
push.

Latency is the headline metric — per `docs/philosophy.md`, irene's edge is
time-to-trade and every panel surfaces that.

Files written (all atomic via `.tmp`-then-rename):
  dashboard.txt          — operator landing page: uptime, 1h counts, e2e latency
  cycle_summary.txt      — current cycle context + per-cycle counts
  latency.txt            — per-stage latency table (the headline diagnostic)
  funnel.txt             — decision outcomes in the last hour
  recent_trades.txt      — last 20 trade decisions
  news_staleness.txt     — (traded_at - article.published_at) distribution
  throughput.txt         — rows/min per stage in the last hour
  llm_calls.txt          — LLM call count, latency, error rate, token totals
  entity_extraction.txt  — shadow-DKG extraction stats (P2)
  entity_resolutions.txt — shadow-DKG resolver stats (P3)
  plots/*.png            — matplotlib-rendered diagnostics, 5-min cadence

Public surface (back-compat for `from app.dashboard import ...`):
    Dashboard, _percentiles, _atomic_write, _now
"""

from __future__ import annotations

from .core import Dashboard
from .io import _atomic_write, _now, _percentiles

__all__ = [
    "Dashboard",
    "_percentiles",
    "_atomic_write",
    "_now",
]
