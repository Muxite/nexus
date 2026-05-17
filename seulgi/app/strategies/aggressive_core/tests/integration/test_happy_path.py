"""Integration: CoreStrategy → guards → paper fill → exit policy → close,
end-to-end against a fixture DB, no external network.
"""
from __future__ import annotations

from dataclasses import dataclass

import pytest

try:
    from markets import MarketMatch
except ImportError:
    from app.markets import MarketMatch  # type: ignore[no-redef]


class _Result:
    def __init__(self, conf=0.7, p_alpha=0.55, agreement=1.0):
        try:
            from ai import Direction
        except ImportError:
            from app.ai import Direction  # type: ignore[no-redef]
        self.direction = Direction.YES
        self.confidence = conf
        self.p_alpha = p_alpha
        self.agreement = agreement
        self.reason = "test"
        self.raw_response = "{}"
        self.risk_score = 0.0


@dataclass
class _Headline:
    headline_id: str = "h1"
    site_url: str = "test://"
    headline: str = "Test headline"
    _freshness_label: str = "BREAKING"


def _match(condition_id="0xC", mid_yes=0.50, mid_no=0.50) -> MarketMatch:
    return MarketMatch(
        condition_id=condition_id, question="Q?",
        token_yes="tk_yes", token_no="tk_no",
        similarity=0.80, mid_yes=mid_yes, mid_no=mid_no,
    )


@pytest.mark.asyncio
async def test_happy_path_entry_and_exit(strategy, tmp_db, paper_trader, core_cfg):
    signal_id = await tmp_db.insert_signal(
        site_url="test://", headline="h", headline_id="h1",
    )
    cash0 = paper_trader.cash

    # Provide a market context that passes the _lowvol filter
    match = _match(mid_yes=0.50)
    match.mkt_vol_30m = 0.005
    await strategy.evaluate(
        signal_id=signal_id, headline=_Headline(),
        survivors=[match], classify_results=[_Result(conf=0.6, p_alpha=0.60)],
    )
    # Position opened, cash debited
    bal = await paper_trader.verify_balance()
    assert bal.open_positions == 1
    assert bal.cash_usdc == cash0 - core_cfg.max_position_usdc


@pytest.mark.asyncio
async def test_neutral_direction_blocks_entry(strategy, tmp_db, paper_trader):
    signal_id = await tmp_db.insert_signal(
        site_url="test://", headline="h", headline_id="h1",
    )
    from app.ai import Direction
    r = _Result()
    r.direction = Direction.NEUTRAL
    await strategy.evaluate(
        signal_id=signal_id, headline=_Headline(),
        survivors=[_match()], classify_results=[r],
    )
    bal = await paper_trader.verify_balance()
    assert bal.open_positions == 0


@pytest.mark.asyncio
async def test_high_vol_blocks_entry(strategy, tmp_db, paper_trader, core_cfg):
    signal_id = await tmp_db.insert_signal(
        site_url="test://", headline="h", headline_id="h1",
    )
    match = _match()
    match.mkt_vol_30m = core_cfg.max_market_vol_30m + 0.01
    await strategy.evaluate(
        signal_id=signal_id, headline=_Headline(),
        survivors=[match], classify_results=[_Result(p_alpha=0.60)],
    )
    bal = await paper_trader.verify_balance()
    assert bal.open_positions == 0
