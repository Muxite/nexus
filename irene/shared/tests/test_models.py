"""Unit tests for irene_shared.models — serialisation and validation."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from irene_shared.models import (
    BrierScore,
    Direction,
    FillResult,
    MappedSignal,
    MarketEvent,
    MarketEventType,
    MarketRecord,
    OpenPosition,
    Orderbook,
    Outcome,
    RawSignal,
    SignalSource,
    TradeDecision,
    TradeMode,
)


def _dt(s: str = "2026-01-01T12:00:00+00:00") -> datetime:
    return datetime.fromisoformat(s)


# ---------------------------------------------------------------------------
# MarketRecord
# ---------------------------------------------------------------------------


class TestMarketRecord:
    def test_round_trip(self):
        m = MarketRecord(
            condition_id="abc123",
            slug="will-x-happen",
            question="Will X happen by 2026-01-01?",
            end_date=_dt(),
            first_seen_at=_dt(),
            updated_at=_dt(),
        )
        data = m.model_dump()
        m2 = MarketRecord.model_validate(data)
        assert m2.condition_id == "abc123"
        assert m2.active is True

    def test_optional_fields_default_none(self):
        m = MarketRecord(
            condition_id="x",
            question="Q?",
            end_date=_dt(),
            first_seen_at=_dt(),
            updated_at=_dt(),
        )
        assert m.token_yes is None
        assert m.token_no is None
        assert m.category is None


# ---------------------------------------------------------------------------
# MappedSignal
# ---------------------------------------------------------------------------


class TestMappedSignal:
    def _make(self, **kwargs) -> MappedSignal:
        defaults: dict[str, Any] = dict(
            signal_id=1,
            condition_id="cond1",
            token_yes="tok_yes",
            token_no="tok_no",
            question="Will it rain?",
            similarity_score=0.85,
            direction=Direction.YES,
            direction_confidence=0.75,
            signal_title="Rain forecast up",
            signal_source="newsapi",
            mapped_at=_dt(),
        )
        defaults.update(kwargs)
        return MappedSignal(**defaults)

    def test_valid(self):
        s = self._make()
        assert s.direction == Direction.YES
        assert s.similarity_score == 0.85

    def test_similarity_score_bounds(self):
        with pytest.raises(Exception):
            self._make(similarity_score=1.5)
        with pytest.raises(Exception):
            self._make(similarity_score=-0.1)

    def test_direction_confidence_bounds(self):
        with pytest.raises(Exception):
            self._make(direction_confidence=1.1)

    def test_json_round_trip(self):
        s = self._make()
        s2 = MappedSignal.model_validate_json(s.model_dump_json())
        assert s2.condition_id == s.condition_id
        assert s2.direction == Direction.YES


# ---------------------------------------------------------------------------
# Orderbook
# ---------------------------------------------------------------------------


class TestOrderbook:
    def test_valid(self):
        ob = Orderbook(
            token_id="tok",
            best_bid=0.45,
            best_ask=0.50,
            mid_price=0.475,
            bid_depth_usdc=500.0,
            ask_depth_usdc=300.0,
        )
        assert ob.spread == pytest.approx(0.05)

    def test_ask_must_be_gte_bid(self):
        with pytest.raises(Exception):
            Orderbook(
                token_id="tok",
                best_bid=0.55,
                best_ask=0.50,
                mid_price=0.52,
                bid_depth_usdc=100,
                ask_depth_usdc=100,
            )

    def test_prices_bounded(self):
        with pytest.raises(Exception):
            Orderbook(
                token_id="tok",
                best_bid=1.1,
                best_ask=1.2,
                mid_price=1.15,
                bid_depth_usdc=100,
                ask_depth_usdc=100,
            )


# ---------------------------------------------------------------------------
# TradeDecision
# ---------------------------------------------------------------------------


class TestTradeDecision:
    def test_round_trip(self):
        td = TradeDecision(
            mapped_signal_id=7,
            condition_id="cond1",
            token_id="tok_yes",
            direction=Direction.YES,
            p_hat=0.65,
            mid_price=0.50,
            edge=0.15,
            kelly_size_usdc=10.0,
            executed=False,
            skip_reason=None,
            decided_at=_dt(),
        )
        data = td.model_dump()
        td2 = TradeDecision.model_validate(data)
        assert td2.edge == 0.15
        assert td2.executed is False


# ---------------------------------------------------------------------------
# OpenPosition
# ---------------------------------------------------------------------------


class TestOpenPosition:
    def test_unrealized_pnl_pct(self):
        pos = OpenPosition(
            trade_id=1,
            condition_id="c",
            token_id="t",
            direction=Direction.YES,
            size_usdc=100.0,
            entry_price=0.50,
            opened_at=_dt(),
            last_price=0.60,
        )
        assert pos.unrealized_pnl_pct == pytest.approx(0.20)

    def test_unrealized_pnl_pct_none_when_no_last_price(self):
        pos = OpenPosition(
            trade_id=2,
            condition_id="c",
            token_id="t",
            direction=Direction.YES,
            size_usdc=50.0,
            entry_price=0.50,
            opened_at=_dt(),
        )
        assert pos.unrealized_pnl_pct is None


# ---------------------------------------------------------------------------
# BrierScore
# ---------------------------------------------------------------------------


class TestBrierScore:
    def test_valid(self):
        bs = BrierScore(
            trade_id=1,
            signal_source="newsapi",
            p_hat=0.70,
            actual_outcome=1.0,
            brier_component=(0.70 - 1.0) ** 2,
            recorded_at=_dt(),
        )
        assert bs.brier_component == pytest.approx(0.09)

    def test_p_hat_bounded(self):
        with pytest.raises(Exception):
            BrierScore(
                trade_id=1,
                signal_source="newsapi",
                p_hat=1.5,
                actual_outcome=1.0,
                brier_component=0.25,
                recorded_at=_dt(),
            )


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class TestEnums:
    def test_direction_values(self):
        assert Direction.YES == "YES"
        assert Direction.NO == "NO"
        assert Direction.NEUTRAL == "NEUTRAL"

    def test_trade_mode_values(self):
        assert TradeMode.PAPER == "paper"
        assert TradeMode.LIVE == "live"

    def test_market_event_type_values(self):
        assert MarketEventType.SIGNAL_MATCH == "signal_match"
        assert MarketEventType.TRADE_DECIDED == "trade_decided"
