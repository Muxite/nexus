"""Unit tests for decision.py — DecisionEngine evaluate()."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from irene_shared.config import TradingSection
from irene_shared.models import Direction, MappedSignal, Orderbook
from decision import DecisionEngine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _make_signal(direction: Direction = Direction.YES) -> MappedSignal:
    return MappedSignal(
        signal_id=1,
        condition_id="cond_abc",
        token_yes="tok_yes",
        token_no="tok_no",
        question="Will X happen?",
        similarity_score=0.85,
        direction=direction,
        direction_confidence=0.70,
        signal_title="Test headline",
        signal_source="newsapi",
        mapped_at=_now(),
    )


def _make_ob(
    best_bid: float = 0.48,
    best_ask: float = 0.52,
    bid_depth: float = 1000.0,
    ask_depth: float = 1000.0,
) -> Orderbook:
    mid = (best_bid + best_ask) / 2.0
    return Orderbook(
        token_id="tok_yes",
        best_bid=best_bid,
        best_ask=best_ask,
        mid_price=mid,
        bid_depth_usdc=bid_depth,
        ask_depth_usdc=ask_depth,
    )


def _cfg(**overrides) -> TradingSection:
    defaults = dict(
        balance_usdc=1000.0,
        max_position_fraction=0.20,
        kelly_fraction=0.25,
        min_edge=0.04,
        fee_buffer=0.01,
        max_liquidity_fraction=0.15,
        max_open_positions=8,
        order_type="FOK",
        position_timeout_hours=48.0,
        edge_increment_yes=0.12,
        edge_increment_no=0.12,
    )
    defaults.update(overrides)
    return TradingSection(**defaults)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestDecisionEngineHappyPath:
    def test_returns_decision_with_no_skip_reason(self):
        engine = DecisionEngine()
        decision = engine.evaluate(
            signal=_make_signal(Direction.YES),
            orderbook=_make_ob(),
            balance_usdc=1000.0,
            open_positions=0,
            cfg=_cfg(),
        )
        assert decision.skip_reason is None
        assert decision.kelly_size_usdc > 0

    def test_decision_for_no_direction(self):
        engine = DecisionEngine()
        decision = engine.evaluate(
            signal=_make_signal(Direction.NO),
            orderbook=_make_ob(),
            balance_usdc=1000.0,
            open_positions=0,
            cfg=_cfg(),
        )
        assert decision.skip_reason is None
        assert decision.direction == Direction.NO

    def test_kelly_size_clamped_by_max_position_fraction(self):
        # Use very high edge_increment to get large Kelly
        engine = DecisionEngine()
        decision = engine.evaluate(
            signal=_make_signal(Direction.YES),
            orderbook=_make_ob(),
            balance_usdc=1000.0,
            open_positions=0,
            cfg=_cfg(edge_increment_yes=0.49, kelly_fraction=1.0),
        )
        assert decision.skip_reason is None
        assert decision.kelly_size_usdc <= 0.20 * 1000.0 + 1e-6  # max_position_fraction

    def test_p_hat_clamped_to_range(self):
        # Even with very high increment, p_hat stays below 0.99
        engine = DecisionEngine()
        decision = engine.evaluate(
            signal=_make_signal(Direction.YES),
            orderbook=_make_ob(best_bid=0.90, best_ask=0.95),
            balance_usdc=1000.0,
            open_positions=0,
            cfg=_cfg(edge_increment_yes=0.40),
        )
        # p_hat = clamp(0.925 + 0.40, 0.01, 0.99) = 0.99
        assert decision.p_hat <= 0.99

    def test_decided_at_is_set(self):
        engine = DecisionEngine()
        before = _now()
        decision = engine.evaluate(
            signal=_make_signal(),
            orderbook=_make_ob(),
            balance_usdc=1000.0,
            open_positions=0,
            cfg=_cfg(),
        )
        assert decision.decided_at >= before

    def test_token_id_is_yes_token_for_yes_direction(self):
        engine = DecisionEngine()
        decision = engine.evaluate(
            signal=_make_signal(Direction.YES),
            orderbook=_make_ob(),
            balance_usdc=1000.0,
            open_positions=0,
            cfg=_cfg(),
        )
        assert decision.token_id == "tok_yes"

    def test_token_id_is_no_token_for_no_direction(self):
        engine = DecisionEngine()
        signal = _make_signal(Direction.NO)
        decision = engine.evaluate(
            signal=signal,
            orderbook=_make_ob(),
            balance_usdc=1000.0,
            open_positions=0,
            cfg=_cfg(),
        )
        assert decision.token_id == "tok_no"


# ---------------------------------------------------------------------------
# Skip reasons
# ---------------------------------------------------------------------------


class TestDecisionEngineSkips:
    def test_skips_when_net_edge_is_negative(self):
        # edge_increment_yes=0.01 → edge < fee_cost → net_edge < 0
        engine = DecisionEngine()
        decision = engine.evaluate(
            signal=_make_signal(Direction.YES),
            orderbook=_make_ob(),
            balance_usdc=1000.0,
            open_positions=0,
            cfg=_cfg(edge_increment_yes=0.01, fee_buffer=0.05),
        )
        assert decision.skip_reason is not None
        assert "edge" in decision.skip_reason

    def test_skips_when_net_edge_below_min_edge(self):
        # net_edge just barely positive but below min_edge
        engine = DecisionEngine()
        decision = engine.evaluate(
            signal=_make_signal(Direction.YES),
            orderbook=_make_ob(best_bid=0.49, best_ask=0.51),  # spread=0.02
            balance_usdc=1000.0,
            open_positions=0,
            cfg=_cfg(
                edge_increment_yes=0.05,  # edge=0.05
                fee_buffer=0.01,          # fee_cost=0.01+0.01=0.02
                min_edge=0.10,            # net_edge=0.03 < 0.10
            ),
        )
        assert decision.skip_reason is not None
        assert "min_edge" in decision.skip_reason

    def test_skips_when_at_position_cap(self):
        engine = DecisionEngine()
        decision = engine.evaluate(
            signal=_make_signal(Direction.YES),
            orderbook=_make_ob(),
            balance_usdc=1000.0,
            open_positions=8,  # at max_open_positions=8
            cfg=_cfg(max_open_positions=8),
        )
        assert decision.skip_reason is not None
        assert "cap" in decision.skip_reason

    def test_skips_when_kelly_size_too_small(self):
        # Tiny balance so kelly_size < $1.00
        engine = DecisionEngine()
        decision = engine.evaluate(
            signal=_make_signal(Direction.YES),
            orderbook=_make_ob(),
            balance_usdc=0.50,  # very small balance
            open_positions=0,
            cfg=_cfg(balance_usdc=0.50),
        )
        assert decision.skip_reason is not None
        assert "kelly" in decision.skip_reason.lower()

    def test_skips_when_insufficient_liquidity(self):
        engine = DecisionEngine()
        decision = engine.evaluate(
            signal=_make_signal(Direction.YES),
            orderbook=_make_ob(ask_depth=5.0),  # only $5 depth
            balance_usdc=1000.0,
            open_positions=0,
            cfg=_cfg(max_liquidity_fraction=0.15),  # max_tradeable = 0.15 * 5 = 0.75 < $1
        )
        assert decision.skip_reason is not None
        assert "depth" in decision.skip_reason

    def test_kelly_capped_by_liquidity_when_large_enough(self):
        # ask_depth=100 → max_tradeable=15. Kelly > 15 should be capped, not skipped.
        engine = DecisionEngine()
        decision = engine.evaluate(
            signal=_make_signal(Direction.YES),
            orderbook=_make_ob(ask_depth=100.0),
            balance_usdc=10000.0,
            open_positions=0,
            cfg=_cfg(
                balance_usdc=10000.0,
                edge_increment_yes=0.20,
                kelly_fraction=1.0,
                max_liquidity_fraction=0.15,
            ),
        )
        # Should not skip — should cap at max_liquidity_fraction * 100 = 15
        assert decision.skip_reason is None
        assert decision.kelly_size_usdc <= 0.15 * 100.0 + 1e-6
