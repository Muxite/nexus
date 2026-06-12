"""Single source of truth for irene dashboard pages + plots.

Adding a new page = add a module under pages/ and append it here.
Each entry must export FILENAME: str and `async def render(dash)`.
"""

from __future__ import annotations

from .pages import (
    cycle_summary,
    decisions,
    entity_extraction,
    entity_resolutions,
    explorations,
    funnel,
    latency,
    latency_breakdown,
    main,
    near_miss_audit,
    news_staleness,
    pnl,
    pnl_clusters,
    recent_trades,
    system_stats,
    throughput,
)

PAGES = [
    main,
    cycle_summary,
    latency,
    latency_breakdown,
    funnel,
    decisions,
    recent_trades,
    news_staleness,
    throughput,
    explorations,
    entity_extraction,
    entity_resolutions,
    pnl,
    pnl_clusters,
    near_miss_audit,
    system_stats,
]

# Files that have never existed (or have been deliberately removed) and
# should not be reintroduced. Mirrors seulgi's A1_5_CULLED_FILES.
DEAD_FILES = frozenset({
    # Absorbed into dashboard.txt's AI subsystem panel (2026-05-24,
    # chunk-3 follow-up: "fewer files, more info per file").
    "llm_calls.txt",
})
