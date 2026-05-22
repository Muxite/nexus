"""
Tests for seulgi/app/trader.py

Edge model reminder:
    edge = edge_increment × (2 × confidence − 1)
    p_hat = mid_price + edge

With the defaults (edge_increment=0.12, min_edge=0.04):
    conf=0.85 → edge=0.084 → passes
    conf=0.80 → edge=0.072 → passes
    conf=0.75 → edge=0.060 → passes
    conf=0.67 → edge=0.041 → barely passes
    conf=0.50 → edge=0.000 → always skipped
"""

import asyncio
import sys
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ai import Direction, ClassifyResult
from markets import MarketMatch
from trader import PaperTrader
from watcher import RawHeadline


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_cfg(
    balance=1000.0,
    max_position=50.0,
    min_trade=5.0,
    min_edge=0.04,
    edge_increment=0.12,
    timeout_hours=24.0,
    max_open=20,
    max_unique_markets=15,
    cold_position_hours=12.0,
):
    cfg = MagicMock()
    cfg.balance_usdc = balance
    cfg.max_position_usdc = max_position
    cfg.min_trade_usdc = min_trade
    cfg.min_edge = min_edge
    cfg.edge_increment = edge_increment
    cfg.position_timeout_hours = timeout_hours
    cfg.max_open_positions = max_open
    cfg.max_unique_markets = max_unique_markets
    cfg.cold_position_hours = cold_position_hours
    cfg.infinite_balance = False
    # Explicit defaults — without these MagicMock returns truthy child mocks
    # and `>=` against a Mock raises TypeError.
    cfg.invert_high_conf = False
    cfg.invert_conf_threshold = 0.85
    cfg.rail_skip_threshold = 0.0
    cfg.entry_rail_skip_threshold = 0.0
    cfg.take_profit = 0.0
    cfg.stop_loss = 0.0
    cfg.trailing_stop = 0.0
    cfg.min_hold_seconds = 0.0
    cfg.spread_cost_bps = 0.0
    # Cycle 10+ additions — must be explicit so MagicMock comparisons don't TypeError.
    cfg.min_entry_yes_mid = 0.0
    cfg.stop_loss_pct = 0.0
    cfg.max_market_vol_30m = 0.0
    cfg.simulate_fees = None
    cfg.simulate_sizing = None
    cfg.simulate_slippage = None
    # M4 variant-level filter defaults
    cfg.direction_filter = ""
    cfg.freshness_filter = ""
    cfg.mid_yes_min = 0.0
    cfg.mid_yes_max = 1.0
    return cfg


def _make_ai_cfg(min_confidence=0.45):
    ai_cfg = MagicMock()
    ai_cfg.min_confidence = min_confidence
    ai_cfg.min_agreement = 0.0  # disable ensemble gate in unit tests
    return ai_cfg


def _make_trader(balance=1000.0, min_edge=0.04, max_open=20):
    db = MagicMock()
    db.insert_signal = AsyncMock(return_value=1)
    db.insert_trade = AsyncMock(return_value=10)
    db.open_position = AsyncMock()
    db.close_position = AsyncMock()
    db.close_trade = AsyncMock()
    db.count_open_positions = AsyncMock(return_value=0)
    db.has_open_position = AsyncMock(return_value=False)
    db.count_unique_open_markets = AsyncMock(return_value=0)
    db.get_cold_losing_positions = AsyncMock(return_value=[])
    db.update_position_price = AsyncMock()

    cfg = _make_cfg(balance=balance, min_edge=min_edge, max_open=max_open)
    ai_cfg = _make_ai_cfg()

    trader = PaperTrader(db=db, classifier=MagicMock(), cfg=cfg, ai_cfg=ai_cfg)
    trader._balance = balance

    mock_session = MagicMock()
    mock_session.closed = False
    trader._session = mock_session

    return trader, db


def _make_headline():
    return RawHeadline(
        site_url="https://apnews.com",
        headline="Fed cuts rates by 50 basis points amid slowdown",
        article_url="https://apnews.com/article/fed-rate-cut",
        detected_at=datetime.now(timezone.utc),
    )


def _make_match(similarity=0.75, token_yes="ty1", token_no="tn1"):
    return MarketMatch(
        condition_id="cid_abc",
        question="Will the Fed cut rates in 2025?",
        token_yes=token_yes,
        token_no=token_no,
        similarity=similarity,
    )


# ---------------------------------------------------------------------------
# Edge model sanity
# ---------------------------------------------------------------------------


class TestEdgeModel:
    """Verify edge = edge_increment × (2 × confidence − 1)."""

    @pytest.mark.asyncio
    async def test_edge_scales_with_confidence(self):
        trader, db = _make_trader()
        trader._classifier.classify_batch = AsyncMock(
            return_value=[ClassifyResult(Direction.YES, 0.85, "bullish")]
        )
        mid = 0.45
        # expected: edge = 0.12 * (2*0.85 - 1) = 0.12 * 0.7 = 0.084
        expected_edge = 0.12 * (2 * 0.85 - 1)
        with patch("trader._fetch_mid_price", AsyncMock(return_value=mid)):
            decisions = await trader.process(_make_headline(), [_make_match()])
        assert decisions[0].executed
        assert decisions[0].edge == pytest.approx(expected_edge, abs=1e-9)
        assert decisions[0].p_hat == pytest.approx(mid + expected_edge, abs=1e-9)

    @pytest.mark.asyncio
    async def test_low_confidence_gives_zero_edge(self):
        """confidence=0.5 → edge=0 → always fails min_edge check."""
        trader, db = _make_trader()
        # Force past the min_confidence filter (set it to 0)
        trader._ai_cfg.min_confidence = 0.0
        trader._classifier.classify_batch = AsyncMock(
            return_value=[ClassifyResult(Direction.YES, 0.5, "meh")]
        )
        with patch("trader._fetch_mid_price", AsyncMock(return_value=0.45)):
            decisions = await trader.process(_make_headline(), [_make_match()])
        assert not decisions[0].executed
        assert decisions[0].edge == pytest.approx(0.0, abs=1e-9)

    @pytest.mark.asyncio
    async def test_no_direction_uses_no_token(self):
        """NO direction should use token_no, not token_yes."""
        trader, db = _make_trader()
        trader._classifier.classify_batch = AsyncMock(
            return_value=[ClassifyResult(Direction.NO, 0.80, "bearish")]
        )
        calls = []
        async def _track_price(session, token_id):
            calls.append(token_id)
            return 0.40

        with patch("trader._fetch_mid_price", _track_price):
            decisions = await trader.process(_make_headline(), [_make_match()])

        assert decisions[0].executed
        assert calls[0] == "tn1"  # NO token used

    @pytest.mark.asyncio
    async def test_yes_direction_uses_yes_token(self):
        trader, db = _make_trader()
        trader._classifier.classify_batch = AsyncMock(
            return_value=[ClassifyResult(Direction.YES, 0.80, "bullish")]
        )
        calls = []
        async def _track_price(session, token_id):
            calls.append(token_id)
            return 0.40

        with patch("trader._fetch_mid_price", _track_price):
            await trader.process(_make_headline(), [_make_match()])

        assert calls[0] == "ty1"  # YES token used


# ---------------------------------------------------------------------------
# Skip scenarios
# ---------------------------------------------------------------------------


class TestProcessSkips:
    @pytest.mark.asyncio
    async def test_neutral_skipped(self):
        trader, db = _make_trader()
        trader._classifier.classify_batch = AsyncMock(
            return_value=[ClassifyResult(Direction.NEUTRAL, 0.5, "unclear")]
        )
        decisions = await trader.process(_make_headline(), [_make_match()])
        assert not decisions[0].executed
        assert decisions[0].skip_reason == "neutral"
        db.open_position.assert_not_called()

    @pytest.mark.asyncio
    async def test_low_confidence_skipped(self):
        trader, db = _make_trader()
        trader._classifier.classify_batch = AsyncMock(
            return_value=[ClassifyResult(Direction.YES, 0.30, "not sure")]  # < 0.45
        )
        decisions = await trader.process(_make_headline(), [_make_match()])
        assert not decisions[0].executed
        assert decisions[0].skip_reason.startswith("low_confidence")

    @pytest.mark.asyncio
    async def test_no_token_skipped(self):
        trader, db = _make_trader()
        trader._classifier.classify_batch = AsyncMock(
            return_value=[ClassifyResult(Direction.YES, 0.8, "bullish")]
        )
        decisions = await trader.process(_make_headline(), [_make_match(token_yes=None, token_no=None)])
        assert not decisions[0].executed
        assert decisions[0].skip_reason == "no_token"

    @pytest.mark.asyncio
    async def test_no_price_skipped(self):
        trader, db = _make_trader()
        trader._classifier.classify_batch = AsyncMock(
            return_value=[ClassifyResult(Direction.YES, 0.8, "bullish")]
        )
        with patch("trader._fetch_mid_price", AsyncMock(return_value=None)):
            decisions = await trader.process(_make_headline(), [_make_match()])
        assert not decisions[0].executed
        assert decisions[0].skip_reason == "no_price"

    @pytest.mark.asyncio
    async def test_edge_too_small_skipped(self):
        # conf=0.8, edge_increment=0.12 → edge = 0.12*(1.6-1) = 0.072
        # min_edge=0.20 → skip
        trader, db = _make_trader(min_edge=0.20)
        trader._classifier.classify_batch = AsyncMock(
            return_value=[ClassifyResult(Direction.YES, 0.80, "bullish")]
        )
        with patch("trader._fetch_mid_price", AsyncMock(return_value=0.45)):
            decisions = await trader.process(_make_headline(), [_make_match()])
        assert not decisions[0].executed
        assert decisions[0].skip_reason.startswith("edge_too_small")

    @pytest.mark.asyncio
    async def test_no_balance_skipped(self):
        trader, db = _make_trader(balance=0.0)
        trader._classifier.classify_batch = AsyncMock(
            return_value=[ClassifyResult(Direction.YES, 0.8, "bullish")]
        )
        with patch("trader._fetch_mid_price", AsyncMock(return_value=0.45)):
            decisions = await trader.process(_make_headline(), [_make_match()])
        assert not decisions[0].executed
        # either "no_balance" or "size_too_small"
        reason = decisions[0].skip_reason
        assert "balance" in reason or "small" in reason

    @pytest.mark.asyncio
    async def test_max_positions_skipped(self):
        trader, db = _make_trader(max_open=2)
        db.count_open_positions = AsyncMock(return_value=2)  # already at limit
        trader._classifier.classify_batch = AsyncMock(
            return_value=[ClassifyResult(Direction.YES, 0.85, "bullish")]
        )
        with patch("trader._fetch_mid_price", AsyncMock(return_value=0.45)):
            decisions = await trader.process(_make_headline(), [_make_match()])
        assert not decisions[0].executed
        assert decisions[0].skip_reason.startswith("max_positions")

    @pytest.mark.asyncio
    async def test_duplicate_position_skipped(self):
        trader, db = _make_trader()
        db.has_open_position = AsyncMock(return_value=True)  # already open
        trader._classifier.classify_batch = AsyncMock(
            return_value=[ClassifyResult(Direction.YES, 0.85, "bullish")]
        )
        with patch("trader._fetch_mid_price", AsyncMock(return_value=0.45)):
            decisions = await trader.process(_make_headline(), [_make_match()])
        assert not decisions[0].executed
        assert decisions[0].skip_reason == "duplicate_position"


# ---------------------------------------------------------------------------
# Successful execution
# ---------------------------------------------------------------------------


class TestProcessExecution:
    @pytest.mark.asyncio
    async def test_yes_trade_executed(self):
        trader, db = _make_trader(balance=1000.0)
        trader._classifier.classify_batch = AsyncMock(
            return_value=[ClassifyResult(Direction.YES, 0.85, "bullish")]
        )
        with patch("trader._fetch_mid_price", AsyncMock(return_value=0.45)):
            decisions = await trader.process(_make_headline(), [_make_match()])

        assert decisions[0].executed
        assert decisions[0].direction == Direction.YES
        assert decisions[0].size_usdc == 50.0
        assert trader.balance == 950.0
        db.open_position.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_trade_executed(self):
        trader, db = _make_trader()
        trader._classifier.classify_batch = AsyncMock(
            return_value=[ClassifyResult(Direction.NO, 0.75, "bearish")]
        )
        with patch("trader._fetch_mid_price", AsyncMock(return_value=0.40)):
            decisions = await trader.process(_make_headline(), [_make_match()])
        assert decisions[0].executed
        assert decisions[0].direction == Direction.NO

    @pytest.mark.asyncio
    async def test_empty_matches_returns_empty(self):
        trader, db = _make_trader()
        decisions = await trader.process(_make_headline(), [])
        assert decisions == []
        db.insert_signal.assert_not_called()

    @pytest.mark.asyncio
    async def test_multiple_matches_evaluated(self):
        trader, db = _make_trader(balance=1000.0)
        trader._classifier.classify_batch = AsyncMock(
            return_value=[
                ClassifyResult(Direction.YES, 0.85, "bullish"),
                ClassifyResult(Direction.YES, 0.85, "bullish"),
            ]
        )
        matches = [
            MarketMatch("cid1", "Q1?", "ty1", "tn1", 0.80),
            MarketMatch("cid2", "Q2?", "ty2", "tn2", 0.70),
        ]
        with patch("trader._fetch_mid_price", AsyncMock(return_value=0.45)):
            decisions = await trader.process(_make_headline(), matches)

        assert len(decisions) == 2
        # Both should execute since they have different condition_ids
        executed = [d for d in decisions if d.executed]
        assert len(executed) == 2

    @pytest.mark.asyncio
    async def test_ai_calls_are_batched(self):
        """All matches for a headline should be classified in one batch call."""
        trader, db = _make_trader()
        matches = [
            MarketMatch(f"cid{i}", f"Q{i}?", f"ty{i}", f"tn{i}", 0.7)
            for i in range(5)
        ]
        batch_results = [ClassifyResult(Direction.NEUTRAL, 0.5, "neutral")] * 5
        trader._classifier.classify_batch = AsyncMock(return_value=batch_results)

        decisions = await trader.process(_make_headline(), matches)

        # classify_batch should be called once for the whole batch
        trader._classifier.classify_batch.assert_called_once()
        assert len(decisions) == 5


# ---------------------------------------------------------------------------
# monitor_positions()
# ---------------------------------------------------------------------------


class TestMonitorPositions:
    def _make_position(self, price=0.50, age_hours=1.0, direction="YES"):
        opened_at = datetime.now(timezone.utc) - timedelta(hours=age_hours)
        return {
            "trade_id": 10,
            "condition_id": "cid_abc",
            "token_id": "tok_yes_1",
            "direction": direction,
            "size_usdc": 50.0,
            "entry_price": 0.50,
            "opened_at": opened_at.isoformat(),
            "last_price": price,
            "p_hat": 0.584,
        }

    @pytest.mark.asyncio
    async def test_win_closes_position(self):
        trader, db = _make_trader(balance=950.0)
        db.get_open_positions = AsyncMock(return_value=[self._make_position()])

        with patch("trader._fetch_mid_price", AsyncMock(return_value=0.97)):
            await trader.monitor_positions()

        db.close_position.assert_called_once_with(10)
        db.close_trade.assert_called_once()
        assert trader.balance > 950.0

    @pytest.mark.asyncio
    async def test_loss_closes_position(self):
        trader, db = _make_trader(balance=950.0)
        db.get_open_positions = AsyncMock(return_value=[self._make_position()])

        with patch("trader._fetch_mid_price", AsyncMock(return_value=0.03)):
            await trader.monitor_positions()

        db.close_position.assert_called_once_with(10)
        db.close_trade.assert_called_once()
        assert trader.balance < 1000.0

    @pytest.mark.asyncio
    async def test_timeout_closes_position(self):
        trader, db = _make_trader(balance=950.0)
        trader._cfg.position_timeout_hours = 1.0

        db.get_open_positions = AsyncMock(
            return_value=[self._make_position(price=0.52, age_hours=2.0)]
        )

        with patch("trader._fetch_mid_price", AsyncMock(return_value=0.52)):
            await trader.monitor_positions()

        db.close_position.assert_called_once_with(10)
        db.close_trade.assert_called_once()

    @pytest.mark.asyncio
    async def test_healthy_position_not_closed(self):
        trader, db = _make_trader(balance=950.0)
        trader._cfg.position_timeout_hours = 24.0
        db.get_open_positions = AsyncMock(return_value=[self._make_position(price=0.55)])

        with patch("trader._fetch_mid_price", AsyncMock(return_value=0.55)):
            await trader.monitor_positions()

        db.close_position.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_price_skips_position(self):
        trader, db = _make_trader()
        db.get_open_positions = AsyncMock(return_value=[self._make_position()])

        with patch("trader._fetch_mid_price", AsyncMock(return_value=None)):
            await trader.monitor_positions()

        db.close_position.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_positions_is_noop(self):
        trader, db = _make_trader()
        db.get_open_positions = AsyncMock(return_value=[])

        await trader.monitor_positions()
        db.close_position.assert_not_called()

    @pytest.mark.asyncio
    async def test_realized_pnl_correct_on_win(self):
        trader, db = _make_trader(balance=950.0)
        db.get_open_positions = AsyncMock(return_value=[self._make_position()])

        exit_price = 0.97
        entry_price = 0.50
        size = 50.0
        expected_pnl = (exit_price - entry_price) * (size / entry_price)

        pnl_recorded = []
        db.close_trade = AsyncMock(side_effect=lambda tid, pnl, **_kw: pnl_recorded.append(pnl))

        with patch("trader._fetch_mid_price", AsyncMock(return_value=exit_price)):
            await trader.monitor_positions()

        assert len(pnl_recorded) == 1
        assert pnl_recorded[0] == pytest.approx(expected_pnl, abs=0.01)

    @pytest.mark.asyncio
    async def test_balance_returned_correctly_on_close(self):
        trader, db = _make_trader(balance=950.0)
        db.get_open_positions = AsyncMock(return_value=[self._make_position()])

        exit_price = 0.97
        entry = 0.50
        size = 50.0
        expected_pnl = (exit_price - entry) * (size / entry)

        with patch("trader._fetch_mid_price", AsyncMock(return_value=exit_price)):
            await trader.monitor_positions()

        assert trader.balance == pytest.approx(950.0 + size + expected_pnl, abs=0.01)

    @pytest.mark.asyncio
    async def test_close_persists_exit_price_and_entry_price(self):
        """FIX-2B: close_trade must receive exit_price + entry_price_at_close
        on every close path so postmortems can reconcile from trades alone.
        """
        trader, db = _make_trader(balance=950.0)
        # Force a WIN close at mid 0.97 (paper mode default).
        position = self._make_position(price=0.97, age_hours=1.0, direction="YES")
        db.get_open_positions = AsyncMock(return_value=[position])
        captured = {}
        db.close_trade = AsyncMock(side_effect=lambda *a, **kw: captured.update(kw) or captured.setdefault("_args", a))

        with patch("trader._fetch_mid_price", AsyncMock(return_value=0.97)):
            await trader.monitor_positions()

        # exit_price falls through from the polled mid in paper mode.
        assert captured.get("exit_price") == pytest.approx(0.97)
        assert captured.get("entry_price_at_close") == pytest.approx(position["entry_price"])
        assert captured.get("close_reason") == "WIN"

    @pytest.mark.asyncio
    async def test_tp_gate_fee_aware_suppresses_net_loss(self):
        """FIX-2A: TP must not fire when realized net PnL after fees would be < 0.

        Reproduces 2026-05-22-trader-exit-bugs.md Bug 2.1: a 2 ¢ TP on a
        $1.40 stake at mid 0.695 books a net loss because round-trip
        fees absorb the gross gain. The legacy gross gate would close;
        the fix must skip.
        """
        from config import SimulateFeesConfig, SimulateSlippageConfig
        trader, db = _make_trader(balance=900.0)
        trader._cfg.mode = "simulate"
        # Realistic Polymarket fee model from config defaults.
        trader._cfg.simulate_fees = SimulateFeesConfig()
        trader._cfg.simulate_slippage = SimulateSlippageConfig()
        trader._cfg.take_profit = 0.020          # 2 ¢ TP knob
        trader._cfg.min_hold_seconds = 0.0       # bypass hold
        # Small-stake position: 1.97 shares at 0.695 entry, $1.4 stake.
        shares = 1.97
        entry_price = 0.695
        size_usdc = shares * entry_price + 0.026  # include the entry_fee
        opened_at = datetime.now(timezone.utc) - timedelta(hours=2)
        pos = {
            "trade_id": 42, "condition_id": "cid_fee_blind",
            "token_id": "tok_yes_1", "direction": "YES",
            "size_usdc": size_usdc, "entry_price": entry_price,
            "shares": shares,
            "opened_at": opened_at.isoformat(),
            "last_price": entry_price, "p_hat": 0.75,
            "market_type": None,
        }
        db.get_open_positions = AsyncMock(return_value=[pos])

        # Current mid is 2 ¢ above entry — legacy gross gate would fire TP.
        exit_mid = entry_price + 0.020
        with patch("trader._fetch_mid_price", AsyncMock(return_value=exit_mid)):
            await trader.monitor_positions()

        # Net MTM = gross $0.0394 − entry_fee $0.026 − est_exit_fee ≈ $0.025
        # ⇒ < 0; gate suppresses TP.
        db.close_position.assert_not_called()
        db.close_trade.assert_not_called()

    @pytest.mark.asyncio
    async def test_tp_gate_fee_aware_fires_when_net_positive(self):
        """FIX-2A: TP still fires when the move is large enough to clear fees."""
        from config import SimulateFeesConfig, SimulateSlippageConfig
        trader, db = _make_trader(balance=900.0)
        trader._cfg.mode = "simulate"
        trader._cfg.simulate_fees = SimulateFeesConfig()
        trader._cfg.simulate_slippage = SimulateSlippageConfig()
        trader._cfg.take_profit = 0.020
        trader._cfg.min_hold_seconds = 0.0
        # Larger move (10 ¢) on the same small stake produces net gain.
        shares = 1.97
        entry_price = 0.695
        size_usdc = shares * entry_price + 0.026
        opened_at = datetime.now(timezone.utc) - timedelta(hours=2)
        pos = {
            "trade_id": 43, "condition_id": "cid_net_win",
            "token_id": "tok_yes_1", "direction": "YES",
            "size_usdc": size_usdc, "entry_price": entry_price,
            "shares": shares,
            "opened_at": opened_at.isoformat(),
            "last_price": entry_price, "p_hat": 0.75,
            "market_type": None,
        }
        db.get_open_positions = AsyncMock(return_value=[pos])

        exit_mid = entry_price + 0.10  # well clear of round-trip fees
        with patch("trader._fetch_mid_price", AsyncMock(return_value=exit_mid)):
            await trader.monitor_positions()

        db.close_position.assert_called_once_with(43)
        db.close_trade.assert_called_once()
        # close_reason kwarg should be TP
        call_kwargs = db.close_trade.call_args.kwargs
        assert call_kwargs.get("close_reason") == "TP"

    @pytest.mark.asyncio
    async def test_trail_gate_fee_aware_suppresses_net_loss(self):
        """FIX-2A: TRAIL must not fire when the post-fee MTM is < 0.

        HWM was high enough to arm the trail, price retreated below
        entry on a net-of-fees basis — fix lets it fall through to
        SL / TIMEOUT instead of locking in a fee-blind loss.
        """
        from config import SimulateFeesConfig, SimulateSlippageConfig
        trader, db = _make_trader(balance=900.0)
        trader._cfg.mode = "simulate"
        trader._cfg.simulate_fees = SimulateFeesConfig()
        trader._cfg.simulate_slippage = SimulateSlippageConfig()
        trader._cfg.trailing_stop = 0.010
        trader._cfg.min_hold_seconds = 0.0
        shares = 1.97
        entry_price = 0.695
        size_usdc = shares * entry_price + 0.026
        opened_at = datetime.now(timezone.utc) - timedelta(hours=2)
        # Armed HWM 3 ¢ above entry; price drifted back to entry. Gross
        # `mtm >= 0` would have closed; net (after fees) is negative.
        pos = {
            "trade_id": 44, "condition_id": "cid_trail_blind",
            "token_id": "tok_yes_1", "direction": "YES",
            "size_usdc": size_usdc, "entry_price": entry_price,
            "shares": shares,
            "opened_at": opened_at.isoformat(),
            "last_price": entry_price, "p_hat": 0.75,
            "high_water_price": entry_price + 0.030,
            "market_type": None,
        }
        db.get_open_positions = AsyncMock(return_value=[pos])

        with patch("trader._fetch_mid_price", AsyncMock(return_value=entry_price)):
            await trader.monitor_positions()

        db.close_position.assert_not_called()
        db.close_trade.assert_not_called()

    @pytest.mark.asyncio
    async def test_prices_fetched_concurrently(self):
        """Price fetches for all open positions must be concurrent."""
        trader, db = _make_trader(balance=900.0)

        positions = [
            {
                "trade_id": i,
                "condition_id": f"cid{i}",
                "token_id": f"tok{i}",
                "direction": "YES",
                "size_usdc": 10.0,
                "entry_price": 0.50,
                "opened_at": (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
                "last_price": 0.50,
                "p_hat": 0.584,
            }
            for i in range(5)
        ]
        db.get_open_positions = AsyncMock(return_value=positions)

        import time
        t0 = time.monotonic()

        async def _slow_price(session, token_id):
            await asyncio.sleep(0.01)
            return 0.55  # no close trigger

        with patch("trader._fetch_mid_price", _slow_price):
            await trader.monitor_positions()

        elapsed = time.monotonic() - t0
        assert elapsed < 0.04, f"Price fetches appear sequential ({elapsed:.3f}s)"


# ---------------------------------------------------------------------------
# FIX-1: market_type plumbing into exit-leg fee/slippage
# ---------------------------------------------------------------------------


class TestExitFeeMarketType:
    """The exit-leg fee compute must use the per-category rate from the
    market_type column, not the default rate (Polymarket "General" tier).

    Regression: prior to FIX-1, trader.py passed market_type=None on every
    exit, so geopolitics (0%) trades paid 5% and sports (3%) paid 5% on
    the close — biasing the simulate-mode PnL versus the live tape.
    """

    def _simulate_cfg(self):
        """Cfg with simulate-mode fees/slippage wired up (no MagicMock — the
        sim.compute_fee path expects real dataclass attributes)."""
        import simulate as sim  # noqa: F401 — ensure module importable
        from config import (
            SimulateFeesConfig,
            SimulateSlippageConfig,
        )

        cfg = _make_cfg(balance=100.0)
        cfg.mode = "simulate"
        cfg.simulate_fees = SimulateFeesConfig()
        cfg.simulate_slippage = SimulateSlippageConfig()
        cfg.position_timeout_hours = 24.0
        cfg.take_profit = 0.0
        cfg.stop_loss = 0.0
        cfg.trailing_stop = 0.0
        cfg.min_hold_seconds = 0.0
        return cfg

    def _make_pos(self, market_type, direction="YES"):
        opened_at = datetime.now(timezone.utc) - timedelta(hours=2)
        # entry_price 0.50, size 5.0 → 10 shares — well above the rail
        # bounds (0.01..0.99) so neither WIN nor LOSS branches trigger.
        return {
            "trade_id": 42,
            "condition_id": "cid_fix1",
            "token_id": "tok_yes_fix1",
            "direction": direction,
            "size_usdc": 5.0,
            "entry_price": 0.50,
            "opened_at": opened_at.isoformat(),
            "last_price": 0.50,
            "unrealized_pnl": 0.0,
            "p_hat": 0.55,
            "shares": 10.0,
            "market_type": market_type,
            "engine_name": "default",
            "high_water_price": None,
        }

    @pytest.mark.asyncio
    async def test_cold_cull_uses_geopolitics_rate(self):
        """COLD_CULL on a geopolitics market must pay 0% taker fee
        (rate=0.0 → only the gas drag remains)."""
        import simulate as sim
        trader, db = _make_trader()
        trader._cfg = self._simulate_cfg()

        pos = self._make_pos(market_type="geopolitics")
        # Cold path requires the position to look stale-losing.
        db.get_cold_losing_positions = AsyncMock(return_value=[pos])
        db.get_open_positions = AsyncMock(return_value=[])

        recorded = {}
        async def _capture(trade_id, pnl, **kwargs):
            recorded.update(kwargs)
            recorded["pnl"] = pnl
        db.close_trade = AsyncMock(side_effect=_capture)

        exit_price = 0.40  # losing exit, off the rails
        with patch("trader._fetch_mid_price", AsyncMock(return_value=exit_price)):
            await trader.monitor_positions()

        # Expected exit fee: shares * 0.0 * p*(1-p) = 0, plus gas drag.
        expected = sim.compute_fee(
            shares=10.0,
            price=exit_price,  # slippage disabled for geopolitics
            market_type="geopolitics",
            cfg=trader._cfg.simulate_fees,
        ).fee + trader._cfg.simulate_fees.gas_cost_per_trade_usdc
        assert recorded.get("close_reason") == "COLD_CULL"
        assert recorded.get("exit_fee_usdc") == pytest.approx(expected, abs=1e-9)
        # Geopolitics rate is 0% — exit_fee must equal exactly the gas drag,
        # NOT the default 0.05 rate that the pre-fix code would have applied.
        default_rate_fee = sim.compute_fee(
            shares=10.0, price=exit_price, market_type=None,
            cfg=trader._cfg.simulate_fees,
        ).fee + trader._cfg.simulate_fees.gas_cost_per_trade_usdc
        assert recorded["exit_fee_usdc"] < default_rate_fee

    @pytest.mark.asyncio
    async def test_main_exit_uses_sports_rate(self):
        """TIMEOUT exit on a sports market must use the sports rate (0.03),
        not the default 0.05."""
        import simulate as sim
        trader, db = _make_trader()
        trader._cfg = self._simulate_cfg()
        trader._cfg.position_timeout_hours = 1.0  # force TIMEOUT

        pos = self._make_pos(market_type="sports")
        db.get_cold_losing_positions = AsyncMock(return_value=[])
        db.get_open_positions = AsyncMock(return_value=[pos])

        recorded = {}
        async def _capture(trade_id, pnl, **kwargs):
            recorded.update(kwargs)
            recorded["pnl"] = pnl
        db.close_trade = AsyncMock(side_effect=_capture)

        exit_price = 0.55  # off the rails → walks the book branch
        with patch("trader._fetch_mid_price", AsyncMock(return_value=exit_price)):
            await trader.monitor_positions()

        # Slippage applies to non-whitelisted categories — replicate it
        # here so the expected fee matches the trader's effective price.
        slip = sim.apply_slippage(
            mid=exit_price,
            notional_usdc=10.0 * exit_price,
            side="NO",  # selling YES
            market_type="sports",
            cfg=trader._cfg.simulate_slippage,
        )
        expected = sim.compute_fee(
            shares=10.0,
            price=slip.effective_price,
            market_type="sports",
            cfg=trader._cfg.simulate_fees,
        ).fee + trader._cfg.simulate_fees.gas_cost_per_trade_usdc
        # Default-rate (buggy) fee — what the pre-fix code recorded.
        default = sim.compute_fee(
            shares=10.0, price=slip.effective_price, market_type=None,
            cfg=trader._cfg.simulate_fees,
        ).fee + trader._cfg.simulate_fees.gas_cost_per_trade_usdc

        assert recorded.get("close_reason") == "TIMEOUT"
        assert recorded.get("exit_fee_usdc") == pytest.approx(expected, abs=1e-9)
        # 3% rate is strictly less than the 5% default — regression guard.
        assert recorded["exit_fee_usdc"] < default
