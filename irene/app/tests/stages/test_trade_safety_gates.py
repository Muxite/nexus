"""Cycle 23 — pre-trade safety gates.

Validates the four hardening changes that landed in cycle 23 to stop the
"buying blind" pattern surfaced by the cycle-22 audit (21/39 trades had
mid_price=0.5 because `_resolve_mid` fell back silently):

1. `_gate_mid_unavailable` — rejects when CLOB returns no price.
2. `_gate_market_state` — rejects on closed/inactive/near-expiry markets.
3. Kill switch — `cfg.trade.execution_enabled=False` early-rejects.
4. `_insert_trade` post-condition asserts on a degenerate stored mid.

Tests use the same fixtures + helpers as `test_trade.py` rather than
re-rolling them, so any future refactor to the cascade plumbing flows
through one place.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.cycle import cycle_scope

from app.tests.stages.test_trade import (
    _Config,
    _FakeAI,
    _FakeBus,
    _FakeCLOB,
    _make_stage,
    _payload,
    _single_decision,
    db_seeded,  # fixture, re-exported by import
)

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Gate 1 — mid_unavailable
# ---------------------------------------------------------------------------


async def test_mid_unavailable_rejects_when_clob_returns_none(db_seeded):
    """Empty CLOB → get_mid returns None → reject with mid_unavailable."""
    bus = _FakeBus()
    ai = _FakeAI()
    clob = _FakeCLOB(prices={})  # default_mid=None
    stage = _make_stage(db_seeded, bus, ai, clob=clob)
    with cycle_scope(1):
        await stage.handle(_payload())
    d = await _single_decision(db_seeded)
    assert d["outcome"] == "reject"
    assert d["reason"] == "mid_unavailable"


async def test_mid_unavailable_rejects_when_token_yes_missing(db_seeded):
    """Market with NULL token_yes → can't price YES → reject."""
    async with db_seeded.batch() as cur:
        await cur.execute("UPDATE markets SET token_yes=NULL WHERE condition_id='m1'")
    bus = _FakeBus()
    ai = _FakeAI()  # default YES@0.7
    stage = _make_stage(db_seeded, bus, ai)  # default clob has default_mid=0.5
    with cycle_scope(1):
        await stage.handle(_payload())
    d = await _single_decision(db_seeded)
    assert d["outcome"] == "reject"
    assert d["reason"] == "mid_unavailable"


# ---------------------------------------------------------------------------
# Gate 2 — market_state
# ---------------------------------------------------------------------------


async def test_market_closed_rejects(db_seeded):
    """closed_at IS NOT NULL → reject with market_closed."""
    async with db_seeded.batch() as cur:
        await cur.execute(
            "UPDATE markets SET closed_at='2026-05-26T00:00:00Z' WHERE condition_id='m1'"
        )
    bus = _FakeBus()
    ai = _FakeAI()
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload())
    d = await _single_decision(db_seeded)
    assert d["outcome"] == "reject"
    assert d["reason"] == "market_closed"


async def test_market_inactive_rejects(db_seeded):
    """active=0 → reject with market_inactive (priority over closed)."""
    async with db_seeded.batch() as cur:
        await cur.execute("UPDATE markets SET active=0 WHERE condition_id='m1'")
    bus = _FakeBus()
    ai = _FakeAI()
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload())
    d = await _single_decision(db_seeded)
    assert d["outcome"] == "reject"
    assert d["reason"] == "market_inactive"


async def test_market_near_expiry_rejects(db_seeded):
    """end_date 4h from now → inside 6h buffer → reject."""
    end_dt = datetime.now(timezone.utc) + timedelta(hours=4)
    async with db_seeded.batch() as cur:
        await cur.execute(
            "UPDATE markets SET end_date=? WHERE condition_id='m1'",
            (end_dt.isoformat().replace("+00:00", "Z"),),
        )
    bus = _FakeBus()
    ai = _FakeAI()
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload())
    d = await _single_decision(db_seeded)
    assert d["outcome"] == "reject"
    assert d["reason"] == "market_near_expiry"


async def test_market_far_enough_to_expiry_passes_gate(db_seeded):
    """end_date 12h from now → outside 6h buffer → market_state gate passes
    (downstream gates may still reject; we only assert no market_state reject)."""
    end_dt = datetime.now(timezone.utc) + timedelta(hours=12)
    async with db_seeded.batch() as cur:
        await cur.execute(
            "UPDATE markets SET end_date=? WHERE condition_id='m1'",
            (end_dt.isoformat().replace("+00:00", "Z"),),
        )
    bus = _FakeBus()
    ai = _FakeAI()
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload())
    d = await _single_decision(db_seeded)
    # The decision may still reject downstream (e.g. market_news_imbalance
    # for a ~12h-out market), but NOT with one of the market_state reasons.
    assert d["reason"] not in (
        "market_closed", "market_inactive", "market_near_expiry",
    )


async def test_market_state_gate_is_permissive_on_missing_end_date(db_seeded):
    """end_date NULL → no expiry knowledge → don't reject on near-expiry."""
    async with db_seeded.batch() as cur:
        await cur.execute("UPDATE markets SET end_date=NULL WHERE condition_id='m1'")
    bus = _FakeBus()
    ai = _FakeAI()
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload())
    d = await _single_decision(db_seeded)
    assert d["reason"] != "market_near_expiry"


# ---------------------------------------------------------------------------
# Kill switch — execution_enabled
# ---------------------------------------------------------------------------


async def test_execution_disabled_short_circuits_with_no_llm_call(db_seeded):
    """cfg.trade.execution_enabled=False → reject before any LLM call."""
    bus = _FakeBus()
    ai = _FakeAI()
    cfg = _Config()
    cfg.trade.execution_enabled = False
    stage = _make_stage(db_seeded, bus, ai, cfg=cfg)
    with cycle_scope(1):
        await stage.handle(_payload())
    d = await _single_decision(db_seeded)
    assert d["outcome"] == "reject"
    assert d["reason"] == "execution_disabled"
    # No LLM call burned tokens — the gate fires at the head of handle().
    assert ai.calls == []


async def test_execution_enabled_default_true_keeps_trades_flowing(db_seeded):
    """Default cfg.trade.execution_enabled=True does NOT add a reject."""
    bus = _FakeBus()
    ai = _FakeAI()  # default YES@0.7
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload())
    d = await _single_decision(db_seeded)
    assert d["reason"] != "execution_disabled"


# ---------------------------------------------------------------------------
# Post-condition assert
# ---------------------------------------------------------------------------


async def test_insert_trade_rejects_degenerate_mid_via_assert(db_seeded):
    """`_insert_trade` is the last line of defense — even if a regression
    re-introduces a silent fallback upstream, the assert here refuses to
    persist a degenerate mid. The assert fires BEFORE the SQL INSERT so
    FK constraints never run; that's why this test doesn't need to seed a
    valid decision row."""
    bus = _FakeBus()
    ai = _FakeAI()
    stage = _make_stage(db_seeded, bus, ai)
    # mid=None → AssertionError.
    with cycle_scope(1):
        with pytest.raises(AssertionError, match="degenerate mid"):
            await stage._insert_trade(
                signal_id=2, decision_id=999, prediction_id=2, market_id="m1",
                direction="YES", confidence=0.7, similarity=0.8, freshness="BREAKING",
                p_hat=0.7, mid=None, edge=0.18, kelly_fraction=0.1, size_usdc=100.0,
                features_snapshot={}, cycle_id=1, is_tracer=False, tracer_id=None,
            )
        # mid=0.0 → AssertionError (boundary).
        with pytest.raises(AssertionError, match="degenerate mid"):
            await stage._insert_trade(
                signal_id=3, decision_id=999, prediction_id=3, market_id="m1",
                direction="YES", confidence=0.7, similarity=0.8, freshness="BREAKING",
                p_hat=0.7, mid=0.0, edge=0.18, kelly_fraction=0.1, size_usdc=100.0,
                features_snapshot={}, cycle_id=1, is_tracer=False, tracer_id=None,
            )
        # mid=1.0 → AssertionError (other boundary; never possible under
        # cfg.trade.max_mid=0.98 but the assert is strict by design).
        with pytest.raises(AssertionError, match="degenerate mid"):
            await stage._insert_trade(
                signal_id=4, decision_id=999, prediction_id=4, market_id="m1",
                direction="YES", confidence=0.7, similarity=0.8, freshness="BREAKING",
                p_hat=0.7, mid=1.0, edge=0.18, kelly_fraction=0.1, size_usdc=100.0,
                features_snapshot={}, cycle_id=1, is_tracer=False, tracer_id=None,
            )
