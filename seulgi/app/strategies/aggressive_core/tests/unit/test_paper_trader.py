"""PaperTrader: friction, fill, cash accounting, reconciliation."""
from __future__ import annotations

import pytest

from ...trader_base import OrderRequest


def _req(**overrides) -> OrderRequest:
    base = dict(
        condition_id="0xCOND", question="q", token_id="t1",
        direction="YES", size_usdc=5.0, quoted_price=0.50,
        signal_id=0,
    )
    base.update(overrides)
    return OrderRequest(**base)


@pytest.mark.asyncio
async def test_place_order_fills_and_debits_cash(paper_trader):
    cash0 = paper_trader.cash
    res = await paper_trader.place_order(_req())
    assert res.filled
    assert res.trade_id is not None
    assert res.fill_price > 0.50    # friction crosses to ask
    assert paper_trader.cash == cash0 - 5.0


@pytest.mark.asyncio
async def test_place_order_refuses_insufficient_cash(paper_trader):
    res = await paper_trader.place_order(_req(size_usdc=paper_trader.cash + 1.0))
    assert not res.filled
    assert res.skip_reason == "insufficient_cash"


@pytest.mark.asyncio
async def test_place_order_volume_cap_refuses_stacking(core_cfg, paper_trader):
    core_cfg.max_size_per_market_per_hour_usdc = 3.0
    first = await paper_trader.place_order(_req(size_usdc=2.0))
    assert first.filled
    second = await paper_trader.place_order(_req(size_usdc=2.0))
    assert not second.filled and second.skip_reason == "volume_cap"


@pytest.mark.asyncio
async def test_close_order_credits_stake_plus_pnl(paper_trader):
    open_res = await paper_trader.place_order(_req(size_usdc=5.0))
    assert open_res.filled
    cash_after_open = paper_trader.cash
    # Exit at 0.50 → essentially break-even minus spread cost
    close = await paper_trader.close_order(
        trade_id=open_res.trade_id, condition_id="0xCOND", token_id="t1",
        direction="YES", size_usdc=5.0, entry_price=open_res.fill_price,
        current_price=0.50, close_reason="TIMEOUT",
    )
    # Cash returned: stake + realized
    assert paper_trader.cash == pytest.approx(cash_after_open + 5.0 + close.realized_pnl, abs=1e-6)
    # Realized should be slightly negative (we crossed the spread twice + spread_cost)
    assert close.realized_pnl < 0


@pytest.mark.asyncio
async def test_verify_balance_round_trip(paper_trader):
    bal0 = await paper_trader.verify_balance()
    assert bal0.cash_usdc == 50.0 and bal0.open_positions == 0
    await paper_trader.place_order(_req(size_usdc=5.0))
    bal1 = await paper_trader.verify_balance()
    assert bal1.cash_usdc == 45.0
    assert bal1.committed_usdc == 5.0
    assert bal1.open_positions == 1
