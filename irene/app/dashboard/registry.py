"""Single source of truth for irene dashboard pages + plots.

Adding a new page = add a module under pages/ and append it here.
Each entry must export FILENAME: str and `async def render(dash)`.
"""

from __future__ import annotations

from .pages import (
    cycle_summary,
    entity_extraction,
    entity_resolutions,
    funnel,
    latency,
    llm_calls,
    main,
    news_staleness,
    pnl,
    recent_trades,
    throughput,
)

PAGES = [
    main,
    cycle_summary,
    latency,
    funnel,
    recent_trades,
    news_staleness,
    throughput,
    llm_calls,
    entity_extraction,
    entity_resolutions,
    pnl,
]

# Files that have never existed and should not be reintroduced. Mirrors
# seulgi's A1_5_CULLED_FILES convention so the regression test pattern
# stays consistent across bots.
DEAD_FILES = frozenset({
    # irene has never had these; if you add one of these names, you are
    # almost certainly duplicating an existing page.
})
