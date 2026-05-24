"""
## Specification

**Purpose.** Single source of truth for bus topic name strings. Importing
constants here prevents typo-publishes to dead topics.

**Public interface.** Top-level string constants only. No functions, no
classes.

**Invariants.**
- A topic name starting with `"telemetry."` is dropped first under
  back-pressure (see `bus.py`); every other topic is trade-path and is
  never dropped.
- Names are stable: a rename here propagates to every publisher and
  subscriber in the same PR.

**Side effects.** None.

**Failure modes.** None — pure constants.

**Non-goals.** Payload schemas (those live in `docs/p1_contracts.md` §2).
"""

from __future__ import annotations

# Trade-path topics (back-pressured; never dropped) -------------------------

TOPIC_ARTICLE_INGESTED = "article.ingested"
TOPIC_ARTICLE_DEDUPED = "article.deduped"
TOPIC_MARKETS_REFRESHED = "markets.refreshed"
TOPIC_SIGNAL_CREATED = "signal.created"
TOPIC_TRADE_DECIDED = "trade.decided"
TOPIC_EXTRACTION_COMPLETED = "extraction.completed"
TOPIC_RESOLUTION_COMPLETED = "resolution.completed"
TOPIC_EVENT_UPDATED = "event.updated"

# Telemetry topics (best-effort; dropped first under back-pressure) ---------

TOPIC_TELEMETRY_TIMING = "telemetry.timing"
TOPIC_TELEMETRY_BUDGET = "telemetry.budget"


TELEMETRY_PREFIX = "telemetry."


def is_telemetry(topic: str) -> bool:
    """Return True for topics that may be dropped under back-pressure."""
    return topic.startswith(TELEMETRY_PREFIX)
