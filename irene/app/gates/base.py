"""Gate primitives — Candidate, GateDecision, Gate ABC.

Pydantic for the data structures so YAML / JSON IO and Python use share
one schema. Gates are async (LLM-backed gates need to be), even when
trivial; this lets the pipeline run gates without special-casing.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


class GateOutcome(str, Enum):
    PASSED = "passed"
    REJECTED = "rejected"


class Candidate(BaseModel):
    """One (article, market) pair under consideration. Built once at
    `_handle_link` time; threaded unchanged through all gates."""

    model_config = ConfigDict(frozen=True)

    article_id: int
    market_condition_id: str
    final_score: float                     # DKG market_links.final_score
    cosine_similarity: Optional[float] = None
    freshness: Optional[str] = None
    breaking_score: Optional[float] = None
    article_tier: Optional[str] = None
    market_volume_24h: Optional[float] = None
    # Convenience for LLM gates — pre-fetched so each gate doesn't
    # re-query. Populated by the explorer before invoking the pipeline.
    article_title: Optional[str] = None
    article_body: Optional[str] = None
    market_question: Optional[str] = None
    market_description: Optional[str] = None
    market_end_date: Optional[str] = None
    # markets.active flag (1 = open, 0 = closed/soft-deleted). Used by the
    # market_open gate to drop candidates whose market is already dead at
    # t0 — a closed market has no resting book, so odds capture would only
    # yield empty post-news reads. None = unknown (treated as open).
    market_active: Optional[int] = None
    # Cycle 30 (H8 lane) enrichments — populated by the explorer only for
    # the second (lane) pipeline, from data it already has in hand:
    #   market_mid: the t0 book snapshot's mid (YES token), feeds the
    #     midband + already_moving gates.
    #   event_type: the triggering DKG event's type (events.event_type,
    #     e.g. TRIAL / REGULATORY_DECISION), feeds document_heavy.
    #   token_yes: lets already_moving fetch the lookback price history.
    # All None on the main admission pipeline — its gates don't read them.
    market_mid: Optional[float] = None
    event_type: Optional[str] = None
    token_yes: Optional[str] = None


class GateDecision(BaseModel):
    """One gate's verdict on one candidate. Pipeline returns a list per
    candidate (its full history through the pipeline)."""

    model_config = ConfigDict(frozen=True)

    candidate: Candidate
    gate_name: str
    outcome: GateOutcome
    reason: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)


class Gate(ABC):
    """Stateless filter step. Holds config; the `evaluate()` method does
    the work. Implementations should never raise — return a rejected
    decision with reason='gate_error: …' on internal failure so the
    pipeline can carry on for the rest of the batch."""

    name: str = "gate"

    @abstractmethod
    async def evaluate(self, candidates: list[Candidate]) -> list[GateDecision]:
        ...
