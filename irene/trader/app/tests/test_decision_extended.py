"""
Extended unit tests for decision.py — boundary arithmetic and edge cases
not covered by the basic test suite.
"""

from __future__ import annotations

import pytest

from irene_shared.config import TradingSection
from irene_shared.models import Direction, MappedSignal, Orderbook
from datetime import datetime, timezone
from decision import DecisionEngine


# ---------------------------------------------------------------------------
# Helpers (mirrors test_decision.py to keep this file self-contained)
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _sig(direction: Direction = Direction.YES) -> MappedSignal:
    return MappedSignal(
        signal_id=99,
        condition_id="cond_ext",
        token_yes="tok_yes",
        token_no="tok_no",
        question="Extended edge-case market?",
        similarity_score=0.90,
        direction=direction,
        direction_confidence=0.80,
        signal_title="Extended test headline",
        signal_source="rss",
        mapped_at=_now(),
    )


def _ob(
    best_bid: float = 0.48,
    best_ask: float = 0.52,
    bid_depth: float = 1000.0,
    ask_depth: float = 1000.0,
) -> Orderbook:
    return Orderbook(
        token_id="tok_yes",
        best_bid=best_bid,
        best_ask=best_ask,
        mid_price=(best_bid + best_ask) / 2.0,
        bid_depth_usdc=bid_depth,
        ask_depth_usdc=ask_depth,
    )


def _cfg(**kw) -> TradingSection:
    base = dict(
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
    base.update(kw)
    return TradingSection(**base)


engine = DecisionEngine()


# ---------------------------------------------------------------------------
# p_hat arithmetic
# ---------------------------------------------------------------------------


class TestPHatArithmetic:
    def test_yes_p_hat_equals_mid_plus_increment(self):
        ob = _ob(best_bid=0.40, best_ask=0.44)  # mid = 0.42
        d = engine.evaluate(_sig(Direction.YES), ob, 1000.0, 0, _cfg(edge_increment_yes=0.10))
        # p_hat = 0.42 + 0.10 = 0.52
        assert d.p_hat == pytest.approx(0.52, abs=1e-5)

    def test_no_p_hat_equals_mid_minus_increment(self):
        ob = _ob(best_bid=0.40, best_ask=0.44)  # mid = 0.42
        d = engine.evaluate(_sig(Direction.NO), ob, 1000.0, 0, _cfg(edge_increment_no=0.10))
        # p_hat = 0.42 - 0.10 = 0.32
        assert d.p_hat == pytest.approx(0.32, abs=1e-5)

    def test_p_hat_clamped_at_0_99_when_mid_near_1(self):
        ob = _ob(best_bid=0.96, best_ask=0.98)  # mid = 0.97
        d = engine.evaluate(_sig(Direction.YES), ob, 1000.0, 0, _cfg(edge_increment_yes=0.10))
        assert d.p_hat <= 0.99

    def test_p_hat_clamped_at_0_01_when_mid_near_0(self):
        ob = _ob(best_bid=0.02, best_ask=0.04)  # mid = 0.03
        d = engine.evaluate(_sig(Direction.NO), ob, 1000.0, 0, _cfg(edge_increment_no=0.10))
        assert d.p_hat >= 0.01


# ---------------------------------------------------------------------------
# Edge check exact boundary
# ---------------------------------------------------------------------------


class TestEdgeBoundary:
    """
    net_edge = |p_hat - mid| - (spread/2 + fee_buffer)

    With mid=0.50, bid=0.48, ask=0.52:
      spread = 0.04 → spread/2 = 0.02
      fee_cost = fee_buffer + 0.02 = fee_buffer + 0.02
    """

    def test_net_edge_well_above_min_edge_is_accepted(self):
        # edge_increment_yes=0.20 → edge = 0.20
        # fee_cost = spread/2 + fee_buffer = 0.02 + 0.01 = 0.03
        # net_edge = 0.17 >> min_edge = 0.04
        ob = _ob(best_bid=0.48, best_ask=0.52)
        d = engine.evaluate(
            _sig(Direction.YES), ob, 1000.0, 0,
            _cfg(edge_increment_yes=0.20, fee_buffer=0.01, min_edge=0.04),
        )
        assert d.skip_reason is None

    def test_net_edge_one_epsilon_below_min_edge_is_skipped(self):
        ob = _ob(best_bid=0.48, best_ask=0.52)
        # edge=0.10, fee_cost=0.02+0.04=0.06, net_edge=0.04, min_edge=0.041
        d = engine.evaluate(
            _sig(Direction.YES), ob, 1000.0, 0,
            _cfg(edge_increment_yes=0.10, fee_buffer=0.04, min_edge=0.041),
        )
        assert d.skip_reason is not None
        assert "min_edge" in d.skip_reason

    def test_net_edge_negative_skips_at_edge_check_not_min_edge(self):
        # Make spread huge so fee_cost > edge → net_edge < 0
        ob = _ob(best_bid=0.20, best_ask=0.80)  # spread=0.60, spread/2=0.30
        # edge=0.12, fee_cost = 0.30 + 0.01 = 0.31 → net_edge = -0.19
        d = engine.evaluate(_sig(Direction.YES), ob, 1000.0, 0, _cfg())
        assert d.skip_reason is not None
        # Should say "edge too small" not "min_edge"
        assert "edge too small" in d.skip_reason or "net_edge" in d.skip_reason


# ---------------------------------------------------------------------------
# Position cap exact boundary
# ---------------------------------------------------------------------------


class TestPositionCapBoundary:
    def test_one_below_cap_is_accepted(self):
        d = engine.evaluate(_sig(), _ob(), 1000.0, open_positions=7, cfg=_cfg(max_open_positions=8))
        assert d.skip_reason is None

    def test_exactly_at_cap_is_skipped(self):
        d = engine.evaluate(_sig(), _ob(), 1000.0, open_positions=8, cfg=_cfg(max_open_positions=8))
        assert d.skip_reason is not None
        assert "cap" in d.skip_reason

    def test_over_cap_is_skipped(self):
        d = engine.evaluate(_sig(), _ob(), 1000.0, open_positions=64, cfg=_cfg(max_open_positions=8))
        assert d.skip_reason is not None


# ---------------------------------------------------------------------------
# Kelly sizing arithmetic
# ---------------------------------------------------------------------------


class TestKellySizing:
    def test_kelly_formula_exact(self):
        """
        kelly_raw = net_edge / (1 - price_to_buy)
        kelly_size = kelly_fraction * kelly_raw * balance

        With YES: price_to_buy = best_ask = 0.52
        mid=0.50, edge_increment_yes=0.12 → p_hat=0.62
        fee_cost = 0.02 + 0.01 = 0.03
        net_edge = 0.12 - 0.03 = 0.09
        kelly_raw = 0.09 / (1 - 0.52) = 0.09 / 0.48 = 0.1875
        kelly_size = 0.25 * 0.1875 * 1000 = 46.875
        capped at max_position_fraction*balance = 0.20*1000=200 → not capped
        """
        d = engine.evaluate(_sig(Direction.YES), _ob(), 1000.0, 0, _cfg())
        assert d.skip_reason is None
        assert d.kelly_size_usdc == pytest.approx(46.875, abs=0.01)

    def test_kelly_for_no_uses_best_bid_side(self):
        """
        NO direction: price_to_buy = 1 - best_bid = 1 - 0.48 = 0.52
        p_hat = mid - edge_increment_no = 0.50 - 0.12 = 0.38
        edge = |0.38 - 0.50| = 0.12
        fee_cost = 0.02 + 0.01 = 0.03
        net_edge = 0.09
        kelly_raw = 0.09 / (1 - 0.52) = 0.1875
        kelly_size = 0.25 * 0.1875 * 1000 = 46.875
        """
        d = engine.evaluate(_sig(Direction.NO), _ob(), 1000.0, 0, _cfg())
        assert d.skip_reason is None
        assert d.kelly_size_usdc == pytest.approx(46.875, abs=0.01)

    def test_max_position_fraction_cap_applied(self):
        # kelly_fraction=1.0, large edge → raw kelly exceeds max_position_fraction cap
        # With edge=0.12, price=0.52, net_edge~0.09 → kelly_raw=0.1875 → kelly*1.0*1000=187.5
        # max_position_fraction=0.05 → cap = 50 → should be capped
        d = engine.evaluate(
            _sig(Direction.YES), _ob(), 1000.0, 0,
            _cfg(kelly_fraction=1.0, max_position_fraction=0.05),
        )
        assert d.skip_reason is None
        assert d.kelly_size_usdc <= 0.05 * 1000.0 + 1e-6

    def test_liquidity_cap_applied_when_kelly_exceeds_depth(self):
        # ask_depth=200, max_liquidity_fraction=0.10 → cap=20
        # kelly with fraction=1.0 and edge=0.12 → ~187 → capped at 20
        d = engine.evaluate(
            _sig(Direction.YES), _ob(ask_depth=200.0), 1000.0, 0,
            _cfg(kelly_fraction=1.0, max_liquidity_fraction=0.10, max_position_fraction=1.0),
        )
        assert d.skip_reason is None
        assert d.kelly_size_usdc <= 0.10 * 200.0 + 1e-6

    def test_kelly_size_is_below_min_when_balance_tiny(self):
        # Balance = $0.10 → kelly_size ≈ 0.25 * 0.1875 * 0.10 ≈ $0.005 < $1 → skipped
        d = engine.evaluate(_sig(), _ob(), 0.10, 0, _cfg())
        assert d.skip_reason is not None
        assert "kelly" in d.skip_reason.lower()


# ---------------------------------------------------------------------------
# Liquidity boundary
# ---------------------------------------------------------------------------


class TestLiquidityBoundary:
    def test_exactly_1_usdc_depth_cap_allows_trade(self):
        # max_liquidity_fraction=1.0, depth=1.0 → cap=1.0 → not skipped if kelly>=1
        d = engine.evaluate(
            _sig(Direction.YES), _ob(ask_depth=10.0), 1000.0, 0,
            _cfg(max_liquidity_fraction=0.10),
            # cap = 0.10 * 10 = 1.0 — exactly $1 → allowed
        )
        # kelly_size >= 1.0, cap = 1.0 → should be capped to 1.0, not skipped
        assert d.skip_reason is None
        assert d.kelly_size_usdc == pytest.approx(1.0, abs=0.01)

    def test_depth_cap_below_1_skips(self):
        # max_liquidity_fraction=0.10, depth=5 → cap=0.5 < $1 → skip
        d = engine.evaluate(
            _sig(Direction.YES), _ob(ask_depth=5.0), 1000.0, 0,
            _cfg(max_liquidity_fraction=0.10),
        )
        assert d.skip_reason is not None
        assert "depth" in d.skip_reason

    def test_no_direction_uses_bid_depth(self):
        # YES: uses ask_depth. NO: uses bid_depth.
        # bid_depth=500 → cap = 0.15 * 500 = 75
        d = engine.evaluate(
            _sig(Direction.NO), _ob(bid_depth=500.0, ask_depth=5.0), 1000.0, 0,
            _cfg(max_liquidity_fraction=0.15),
        )
        # With bid_depth=500 there's plenty of liquidity — should NOT skip
        assert d.skip_reason is None


# ---------------------------------------------------------------------------
# Output fields
# ---------------------------------------------------------------------------


class TestOutputFields:
    def test_condition_id_matches_signal(self):
        d = engine.evaluate(_sig(), _ob(), 1000.0, 0, _cfg())
        assert d.condition_id == "cond_ext"

    def test_mapped_signal_id_matches_signal(self):
        d = engine.evaluate(_sig(), _ob(), 1000.0, 0, _cfg())
        assert d.mapped_signal_id == 99

    def test_order_type_set_from_cfg(self):
        d = engine.evaluate(_sig(), _ob(), 1000.0, 0, _cfg(order_type="FAK"))
        assert d.skip_reason is None
        assert d.order_type == "FAK"

    def test_executed_false_on_decide(self):
        d = engine.evaluate(_sig(), _ob(), 1000.0, 0, _cfg())
        assert d.executed is False

    def test_skip_sets_kelly_size_to_zero(self):
        d = engine.evaluate(_sig(), _ob(), 1000.0, 8, _cfg(max_open_positions=8))
        assert d.kelly_size_usdc == 0.0

    def test_skip_sets_order_type_to_none(self):
        d = engine.evaluate(_sig(), _ob(), 1000.0, 8, _cfg(max_open_positions=8))
        assert d.order_type is None
