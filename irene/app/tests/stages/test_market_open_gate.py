"""Unit tests for the market_open gate — pure-Python liveness filter.

A closed/ended market has no resting order book, so odds capture against
it yields only empty post-news reads. The gate drops those candidates
before the paid LLM gate and before odds capture.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.gates.base import Candidate, GateOutcome
from app.gates.dkg import DKGThresholdGate
from app.gates.market_open import MarketOpenGate
from app.gates.pipeline import build_pipeline
from app.config import DKGGateConfig, MarketOpenGateConfig


def _candidate(**overrides) -> Candidate:
    base = dict(
        article_id=1,
        market_condition_id="cond-1",
        final_score=0.9,
    )
    base.update(overrides)
    return Candidate(**base)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


_FUTURE = _iso(datetime.now(timezone.utc) + timedelta(days=30))
_PAST = _iso(datetime.now(timezone.utc) - timedelta(days=1))


@pytest.mark.asyncio
async def test_open_market_with_future_end_date_passes():
    gate = MarketOpenGate()
    [d] = await gate.evaluate([_candidate(market_active=1, market_end_date=_FUTURE)])
    assert d.outcome == GateOutcome.PASSED
    assert d.reason == ""


@pytest.mark.asyncio
async def test_open_market_with_null_end_date_passes():
    gate = MarketOpenGate()
    [d] = await gate.evaluate([_candidate(market_active=1, market_end_date=None)])
    assert d.outcome == GateOutcome.PASSED


@pytest.mark.asyncio
async def test_unknown_active_treated_as_open():
    gate = MarketOpenGate()
    [d] = await gate.evaluate([_candidate(market_active=None, market_end_date=_FUTURE)])
    assert d.outcome == GateOutcome.PASSED


@pytest.mark.asyncio
async def test_inactive_market_rejected():
    gate = MarketOpenGate()
    [d] = await gate.evaluate([_candidate(market_active=0, market_end_date=_FUTURE)])
    assert d.outcome == GateOutcome.REJECTED
    assert d.reason == "market_inactive"
    assert d.metadata["active"] == 0


@pytest.mark.asyncio
async def test_ended_market_rejected():
    gate = MarketOpenGate()
    [d] = await gate.evaluate([_candidate(market_active=1, market_end_date=_PAST)])
    assert d.outcome == GateOutcome.REJECTED
    assert d.reason.startswith("market_ended")
    assert d.metadata["end_date"] == _PAST


@pytest.mark.asyncio
async def test_unparseable_end_date_does_not_reject_active_market():
    # A garbage end_date shouldn't false-reject an otherwise-open market.
    gate = MarketOpenGate()
    [d] = await gate.evaluate([_candidate(market_active=1, market_end_date="not-a-date")])
    assert d.outcome == GateOutcome.PASSED


@pytest.mark.asyncio
async def test_runs_before_dkg_in_pipeline():
    """market_open is first, so a dead market is rejected by it (not dkg)
    even when the DKG score would pass."""
    pipeline = build_pipeline([
        MarketOpenGateConfig(),
        DKGGateConfig(min_final_score=0.5),
    ])
    assert pipeline.gate_names == ["market_open", "dkg"]
    cand = _candidate(final_score=0.99, market_active=0, market_end_date=_FUTURE)
    history_map = await pipeline.run([cand])
    history = history_map["1:cond-1"]
    assert pipeline.rejected_by(history) == "market_open"
    # DKG never ran — only the market_open decision is recorded.
    assert [d.gate_name for d in history] == ["market_open"]


@pytest.mark.asyncio
async def test_live_market_flows_through_to_dkg():
    pipeline = build_pipeline([
        MarketOpenGateConfig(),
        DKGGateConfig(min_final_score=0.5),
    ])
    cand = _candidate(final_score=0.99, market_active=1, market_end_date=_FUTURE)
    history_map = await pipeline.run([cand])
    history = history_map["1:cond-1"]
    assert pipeline.passed(history)
    assert [d.gate_name for d in history] == ["market_open", "dkg"]
