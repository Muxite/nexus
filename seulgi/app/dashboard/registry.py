"""Single source of truth for which pages (and later plots) the dashboard writes.

Each entry is a module exporting `FILENAME: str` and `async def render(dash)`.
Adding a new page = add a module here. Removing one = delete the entry.
"""

from __future__ import annotations

from .pages import (
    activity_log,
    db_health,
    declined,
    engine_comparison,
    executed,
    main,
    near_miss,
    news,
    positions,
    quant_stats,
    scout_funnel,
    signal_journal,
    trace_log,
    usage,
    variant_compare,
    watcher_status,
)

PAGES = [
    main,
    scout_funnel,
    db_health,
    positions,
    executed,
    declined,
    near_miss,
    news,
    watcher_status,
    engine_comparison,
    activity_log,
    trace_log,
    signal_journal,
    quant_stats,
    usage,
    variant_compare,
]

# Files culled in Phase 1 A1.5 (2026-05-22). Any future page registration
# must not reintroduce these names. Enforced by test_no_dead_files.py.
A1_5_CULLED_FILES = frozenset({
    "ai_decisions.txt",
    "market_index.txt",
    "trade_log.jsonl",
    "wins.txt",
    "losses.txt",
    "headline_journal.jsonl",
    "headline_winners.txt",
})
