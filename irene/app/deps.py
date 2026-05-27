"""
## Specification

**Purpose.** The `StageDeps` dataclass every Stage's `__init__` accepts.
This is the dependency-injection contract per `docs/modularization.md` §6
"Dependencies are injected, not imported."

**Public interface.** `StageDeps` (frozen dataclass).

**Invariants.**
- Frozen — stages may not mutate the deps after construction.
- Optional deps default to `None`; a stage that needs one asserts it in its
  own `__init__`.

**Side effects.** None — pure data class.

**Failure modes.** None.

**Non-goals.** Construction of the dependencies (that's `main.py`'s job).
Lifecycle of the dependencies (they outlive any individual `StageDeps`).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:  # avoid import cycles at runtime
    from app.bus import Bus
    from app.clob import CLOBClient
    from app.clob_ws_pool import ClobWsPool
    from app.config import Config
    from app.db import DB
    from app.embed import Embedder
    from app.ai import AIClient
    from app.budget import Budget
    from app.timing import ProfileStore
    from app.stages.markets import MarketIndex


@dataclass(frozen=True)
class StageDeps:
    db: "DB"
    bus: "Bus"
    config: "Config"
    embedder: Optional["Embedder"] = None
    ai: Optional["AIClient"] = None
    budget: Optional["Budget"] = None
    timing: Optional["ProfileStore"] = None
    # The MarketIndex is owned by MarketsStage; MatchStage receives it
    # through deps. Same pattern as db / bus — a process-singleton
    # resource, not a stage-to-stage import.
    market_index: Optional["MarketIndex"] = None
    # Polymarket CLOB client — pure resource (no tables). Used by
    # trade.py (entry mid) and closer.py (exit mid).
    clob: Optional["CLOBClient"] = None
    # Cycle 26: sharded WebSocket pool feeding the LiveMarketCache that
    # CLOBClient reads through. MarketsStage owns the subscription set
    # (drives `subscribe`/`unsubscribe` from each `poll_once`). When
    # None (default; `cfg.markets.enable_ws_feed=False`), the cache
    # stays empty and every CLOB read goes to REST.
    ws_pool: Optional["ClobWsPool"] = None
