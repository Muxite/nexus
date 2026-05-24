"""Seulgi dashboard package — periodic text-file snapshot writer.

Files written under `data/dashboard/` every `interval_secs` (default 30 s):
  dashboard.txt        — operator landing page
  scout_funnel.txt     — scout engine decision funnel
  db_health.txt        — write health + WAL + diagnostics tiles
  positions_open.txt   — every live position
  trades_executed.txt  — last 50 executed paper trades
  trades_declined.txt  — last 50 declined trade decisions
  near_miss.txt        — positive-edge trades that missed threshold
  news_seen.txt        — last 50 raw headlines
  watcher_status.txt   — per-site watcher health
  engine_comparison.txt — per-engine summary + skip reasons
  signal_journal.txt   — non-NEUTRAL AI decisions with outcomes
  quant_stats.txt      — prediction accuracy + signal latency
  usage.txt            — AI tokens + Brave search usage
  activity_log.txt     — rolling event log
  trace_log.txt        — end-to-end traces
  variant_compare.txt  — per-variant pivot (lifetime, all engines)

All files are written atomically via `.tmp`-then-rename so readers always
see a complete file.

Public surface (back-compat for `from dashboard import ...`):
    Dashboard, NewsEvent, TradeEvent, PositionEvent, ClosedEvent,
    WatcherStatus, SystemHealth
"""

from __future__ import annotations

from .core import Dashboard
from .events import (
    ClosedEvent,
    NewsEvent,
    PositionEvent,
    SystemHealth,
    TradeEvent,
    WatcherStatus,
)

__all__ = [
    "Dashboard",
    "NewsEvent",
    "TradeEvent",
    "PositionEvent",
    "ClosedEvent",
    "WatcherStatus",
    "SystemHealth",
]
