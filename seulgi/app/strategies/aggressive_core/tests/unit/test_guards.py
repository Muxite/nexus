"""Unit tests for every Guard in guards.py — pass/skip/edge cases."""
from __future__ import annotations

import time

import pytest

from ...guards import (
    CashGuard,
    DirectionGuard,
    DrawdownGuard,
    DuplicateGuard,
    EdgeGuard,
    ExposureGuard,
    FrequencyGuard,
    GuardContext,
    HardRailGuard,
    MarketClosedGuard,
    PositionCountGuard,
    SoftRailGuard,
    SpreadGuard,
    TokenBudgetGuard,
    VolGuard,
    WatcherHealthGuard,
)


def _ctx(core_cfg, tmp_db, bus, **overrides) -> GuardContext:
    base = dict(
        cfg=core_cfg, db=tmp_db, bus=bus,
        cash=core_cfg.bankroll_usdc, open_positions=0, unique_markets=0,
        committed_usdc=0.0,
        direction="YES", confidence=0.7, agreement=1.0,
        quoted_price=0.5, signed_edge=0.05,
        condition_id="0xCOND",
    )
    base.update(overrides)
    return GuardContext(**base)


# ------------------------- Direction -----------------------------------

@pytest.mark.asyncio
async def test_direction_neutral_skips(core_cfg, tmp_db, bus):
    ctx = _ctx(core_cfg, tmp_db, bus, direction="NEUTRAL")
    r = await DirectionGuard().check(ctx)
    assert not r.passed and r.skip_reason == "neutral"


@pytest.mark.asyncio
async def test_direction_low_confidence_skips(core_cfg, tmp_db, bus):
    ctx = _ctx(core_cfg, tmp_db, bus, confidence=core_cfg.min_confidence - 0.01)
    r = await DirectionGuard().check(ctx)
    assert not r.passed and r.skip_reason.startswith("low_confidence")


@pytest.mark.asyncio
async def test_direction_at_threshold_passes(core_cfg, tmp_db, bus):
    ctx = _ctx(core_cfg, tmp_db, bus, confidence=core_cfg.min_confidence)
    r = await DirectionGuard().check(ctx)
    assert r.passed


# ------------------------- Rails ---------------------------------------

@pytest.mark.asyncio
async def test_hard_rail_low_skip(core_cfg, tmp_db, bus):
    ctx = _ctx(core_cfg, tmp_db, bus, quoted_price=0.01)
    r = await HardRailGuard().check(ctx)
    assert not r.passed


@pytest.mark.asyncio
async def test_hard_rail_high_skip(core_cfg, tmp_db, bus):
    ctx = _ctx(core_cfg, tmp_db, bus, quoted_price=0.99)
    r = await HardRailGuard().check(ctx)
    assert not r.passed


@pytest.mark.asyncio
async def test_soft_rail_disabled_passes(core_cfg, tmp_db, bus):
    # rail_skip_threshold=0.0 → guard always passes
    ctx = _ctx(core_cfg, tmp_db, bus, quoted_price=0.99)
    r = await SoftRailGuard().check(ctx)
    assert r.passed


# ------------------------- Vol -----------------------------------------

@pytest.mark.asyncio
async def test_vol_over_cap_skips(core_cfg, tmp_db, bus):
    ctx = _ctx(core_cfg, tmp_db, bus, mkt_vol_30m=core_cfg.max_market_vol_30m + 0.001)
    r = await VolGuard().check(ctx)
    assert not r.passed


@pytest.mark.asyncio
async def test_vol_unknown_passes(core_cfg, tmp_db, bus):
    ctx = _ctx(core_cfg, tmp_db, bus, mkt_vol_30m=None)
    assert (await VolGuard().check(ctx)).passed


# ------------------------- Market closed -------------------------------

@pytest.mark.asyncio
async def test_market_closed_skips(core_cfg, tmp_db, bus):
    ctx = _ctx(core_cfg, tmp_db, bus, market_closed=True)
    r = await MarketClosedGuard().check(ctx)
    assert not r.passed and r.skip_reason == "market_closed"


# ------------------------- Spread --------------------------------------

@pytest.mark.asyncio
async def test_spread_over_cap_skips(core_cfg, tmp_db, bus):
    ctx = _ctx(core_cfg, tmp_db, bus,
               quoted_spread_bps=core_cfg.max_entry_spread_bps + 1)
    r = await SpreadGuard().check(ctx)
    assert not r.passed


@pytest.mark.asyncio
async def test_spread_zero_unknown_passes(core_cfg, tmp_db, bus):
    ctx = _ctx(core_cfg, tmp_db, bus, quoted_spread_bps=0.0)
    assert (await SpreadGuard().check(ctx)).passed


# ------------------------- Edge ----------------------------------------

@pytest.mark.asyncio
async def test_edge_too_small_skips(core_cfg, tmp_db, bus):
    ctx = _ctx(core_cfg, tmp_db, bus, signed_edge=core_cfg.min_edge / 2)
    assert not (await EdgeGuard().check(ctx)).passed


# ------------------------- Duplicate / counts --------------------------

@pytest.mark.asyncio
async def test_duplicate_position_skips(core_cfg, tmp_db, bus):
    await tmp_db.open_position(
        trade_id=1, condition_id="0xCOND", question="q", token_id="t",
        direction="YES", size_usdc=1.0, entry_price=0.5, p_hat=0.5,
        engine_name=core_cfg.engine_name,
    )
    ctx = _ctx(core_cfg, tmp_db, bus)
    r = await DuplicateGuard().check(ctx)
    assert not r.passed and r.skip_reason == "duplicate_position"


@pytest.mark.asyncio
async def test_position_count_skips_at_max(core_cfg, tmp_db, bus):
    ctx = _ctx(core_cfg, tmp_db, bus, open_positions=core_cfg.max_open_positions)
    assert not (await PositionCountGuard().check(ctx)).passed


# ------------------------- Cash / exposure -----------------------------

@pytest.mark.asyncio
async def test_cash_below_min_trade_skips(core_cfg, tmp_db, bus):
    ctx = _ctx(core_cfg, tmp_db, bus, cash=core_cfg.min_trade_usdc - 0.01)
    assert not (await CashGuard().check(ctx)).passed


@pytest.mark.asyncio
async def test_exposure_cap_skips(core_cfg, tmp_db, bus):
    committed = core_cfg.bankroll_usdc * core_cfg.max_exposure_pct + 0.01
    ctx = _ctx(core_cfg, tmp_db, bus, committed_usdc=committed)
    assert not (await ExposureGuard().check(ctx)).passed


# ------------------------- Frequency -----------------------------------

@pytest.mark.asyncio
async def test_frequency_too_fast_skips(core_cfg, tmp_db, bus):
    core_cfg.min_seconds_between_entries = 30.0
    ctx = _ctx(core_cfg, tmp_db, bus, recent_entry_ts=[time.monotonic() - 1.0])
    assert not (await FrequencyGuard().check(ctx)).passed


@pytest.mark.asyncio
async def test_frequency_hourly_cap_skips(core_cfg, tmp_db, bus):
    core_cfg.min_seconds_between_entries = 0.0
    core_cfg.max_entries_per_hour = 2
    now = time.monotonic()
    ctx = _ctx(core_cfg, tmp_db, bus, recent_entry_ts=[now - 100, now - 50])
    assert not (await FrequencyGuard().check(ctx)).passed


# ------------------------- Drawdown ------------------------------------

@pytest.mark.asyncio
async def test_drawdown_24h_trips(core_cfg, tmp_db, bus):
    # Insert a synthetic losing closed trade large enough to trip 15%-of-$50
    huge_loss = -10.0  # > 15% of 50 = 7.50
    await tmp_db._conn.execute(
        "INSERT INTO trades (signal_id, condition_id, question, direction, "
        "similarity, p_hat, mid_price, size_usdc, decided_at, "
        "engine_name, realized_pnl, closed_at) "
        "VALUES (0,'0xA','q','YES',0.9,0.5,0.5,5.0,datetime('now'),?,?,datetime('now'))",
        (core_cfg.engine_name, huge_loss),
    )
    await tmp_db._conn.commit()
    ctx = _ctx(core_cfg, tmp_db, bus)
    r = await DrawdownGuard().check(ctx)
    assert not r.passed and "drawdown" in (r.skip_reason or "")


# ------------------------- Token budget --------------------------------

@pytest.mark.asyncio
async def test_token_budget_exceeded_skips(core_cfg, tmp_db, bus):
    ctx = _ctx(core_cfg, tmp_db, bus,
               daily_ai_spend_usd=core_cfg.daily_ai_token_budget_usd + 0.01)
    r = await TokenBudgetGuard().check(ctx)
    assert not r.passed and r.skip_reason == "token_budget_exceeded"


# ------------------------- Watcher health ------------------------------

@pytest.mark.asyncio
async def test_watcher_degraded_skips(core_cfg, tmp_db, bus):
    ctx = _ctx(core_cfg, tmp_db, bus, watcher_degraded_fraction=0.6)
    assert not (await WatcherHealthGuard().check(ctx)).passed
