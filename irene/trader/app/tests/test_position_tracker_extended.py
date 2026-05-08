"""
Extended tests for position_tracker.py — resolution threshold boundaries,
NO-position win/loss logic, Brier score actual values, multi-position handling.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from irene_shared.models import Direction, Orderbook
from position_tracker import (
    PositionTracker,
    _ANTI_RESOLVED_THRESHOLD,
    _RESOLVED_THRESHOLD,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _pos(
    trade_id: int = 1,
    direction: str = Direction.YES.value,
    size_usdc: float = 100.0,
    entry_price: float = 0.50,
    opened_at: datetime | None = None,
    condition_id: str = "cond_ext",
    token_id: str = "tok_yes",
    signal_source: str = "rss",
    p_hat: float = 0.70,
) -> dict:
    return {
        "trade_id": trade_id,
        "condition_id": condition_id,
        "token_id": token_id,
        "direction": direction,
        "size_usdc": size_usdc,
        "entry_price": entry_price,
        "opened_at": opened_at or (_now() - timedelta(hours=1)),
        "last_price": None,
        "unrealized_pnl": None,
        "signal_source": signal_source,
        "p_hat": p_hat,
    }


def _ob(mid: float) -> Orderbook:
    bid = max(mid - 0.01, 0.001)
    ask = min(mid + 0.01, 0.999)
    return Orderbook(
        token_id="tok_yes",
        best_bid=bid,
        best_ask=ask,
        mid_price=mid,
        bid_depth_usdc=500.0,
        ask_depth_usdc=500.0,
    )


def _tracker(timeout_hours: float = 48.0) -> PositionTracker:
    db = MagicMock()
    db.get_open_positions = AsyncMock(return_value=[])
    db.upsert_position = AsyncMock()
    db.close_trade = AsyncMock()
    db.delete_position = AsyncMock()
    db.log_event = AsyncMock()
    db.insert_brier_score = AsyncMock()

    cfg = MagicMock()
    cfg.get.return_value.trading.position_timeout_hours = timeout_hours
    cfg.get.return_value.polymarket.clob_url = "https://clob.polymarket.com"
    cfg.get.return_value.timing.jitter_fraction = 0.0
    cfg.get.return_value.rate_limits.clob_max_per_day = 20000

    redis = MagicMock()
    redis.try_consume_daily_budget = AsyncMock(return_value=True)

    return PositionTracker(cfg, db, MagicMock(), redis)


def _clob(mid: float | None) -> MagicMock:
    clob = MagicMock()
    ob = _ob(mid) if mid is not None else None
    clob.get_orderbook = AsyncMock(return_value=ob)
    return clob


# ---------------------------------------------------------------------------
# Resolution threshold boundary tests
# ---------------------------------------------------------------------------


class TestResolutionThresholdBoundary:
    @pytest.mark.asyncio
    async def test_price_exactly_at_resolved_threshold_triggers_close(self):
        t = _tracker()
        pos = _pos(direction=Direction.YES.value)
        await t._check_one(pos, _clob(_RESOLVED_THRESHOLD), timeout_hours=48.0, now=_now())
        t._db.close_trade.assert_called_once()

    @pytest.mark.asyncio
    async def test_price_just_below_resolved_threshold_does_not_close(self):
        t = _tracker()
        pos = _pos(direction=Direction.YES.value)
        price = _RESOLVED_THRESHOLD - 0.001  # 0.969
        await t._check_one(pos, _clob(price), timeout_hours=48.0, now=_now())
        t._db.close_trade.assert_not_called()

    @pytest.mark.asyncio
    async def test_price_exactly_at_anti_resolved_threshold_triggers_close(self):
        t = _tracker()
        pos = _pos(direction=Direction.NO.value)
        await t._check_one(pos, _clob(_ANTI_RESOLVED_THRESHOLD), timeout_hours=48.0, now=_now())
        t._db.close_trade.assert_called_once()

    @pytest.mark.asyncio
    async def test_price_just_above_anti_resolved_threshold_does_not_close(self):
        t = _tracker()
        pos = _pos(direction=Direction.NO.value)
        price = _ANTI_RESOLVED_THRESHOLD + 0.001  # 0.031
        await t._check_one(pos, _clob(price), timeout_hours=48.0, now=_now())
        t._db.close_trade.assert_not_called()


# ---------------------------------------------------------------------------
# YES/NO win/loss outcome logic
# ---------------------------------------------------------------------------


class TestOutcomeLogic:
    @pytest.mark.asyncio
    async def test_yes_position_at_resolved_threshold_is_win(self):
        t = _tracker()
        pos = _pos(direction=Direction.YES.value)
        await t._close(1, pos, _RESOLVED_THRESHOLD, "resolved", Direction.YES.value, 100.0, 0.50)
        kw = t._db.close_trade.call_args.kwargs
        assert kw["outcome"] == "WIN"

    @pytest.mark.asyncio
    async def test_yes_position_at_anti_threshold_is_loss(self):
        t = _tracker()
        pos = _pos(direction=Direction.YES.value)
        await t._close(1, pos, _ANTI_RESOLVED_THRESHOLD, "resolved", Direction.YES.value, 100.0, 0.50)
        kw = t._db.close_trade.call_args.kwargs
        assert kw["outcome"] == "LOSS"

    @pytest.mark.asyncio
    async def test_no_position_at_anti_threshold_is_win(self):
        t = _tracker()
        pos = _pos(direction=Direction.NO.value, token_id="tok_no")
        await t._close(1, pos, _ANTI_RESOLVED_THRESHOLD, "resolved", Direction.NO.value, 100.0, 0.50)
        kw = t._db.close_trade.call_args.kwargs
        assert kw["outcome"] == "WIN"

    @pytest.mark.asyncio
    async def test_no_position_at_resolved_threshold_is_loss(self):
        t = _tracker()
        pos = _pos(direction=Direction.NO.value, token_id="tok_no")
        await t._close(1, pos, _RESOLVED_THRESHOLD, "resolved", Direction.NO.value, 100.0, 0.50)
        kw = t._db.close_trade.call_args.kwargs
        assert kw["outcome"] == "LOSS"

    @pytest.mark.asyncio
    async def test_mid_market_exit_on_timeout_is_push(self):
        t = _tracker()
        pos = _pos(direction=Direction.YES.value)
        await t._close(1, pos, 0.52, "timeout", Direction.YES.value, 100.0, 0.50)
        kw = t._db.close_trade.call_args.kwargs
        assert kw["outcome"] == "PUSH"


# ---------------------------------------------------------------------------
# Brier score actual outcome values
# ---------------------------------------------------------------------------


class TestBrierScoreActualValues:
    @pytest.mark.asyncio
    async def test_yes_win_brier_actual_is_1(self):
        t = _tracker()
        pos = _pos(direction=Direction.YES.value, p_hat=0.75, signal_source="brave")
        await t._close(1, pos, _RESOLVED_THRESHOLD, "resolved", Direction.YES.value, 100.0, 0.50)
        kw = t._db.insert_brier_score.call_args.kwargs
        assert kw["actual_outcome"] == pytest.approx(1.0)

    @pytest.mark.asyncio
    async def test_yes_loss_brier_actual_is_0(self):
        t = _tracker()
        pos = _pos(direction=Direction.YES.value, p_hat=0.75, signal_source="newsapi")
        await t._close(1, pos, _ANTI_RESOLVED_THRESHOLD, "resolved", Direction.YES.value, 100.0, 0.50)
        kw = t._db.insert_brier_score.call_args.kwargs
        assert kw["actual_outcome"] == pytest.approx(0.0)

    @pytest.mark.asyncio
    async def test_brier_score_uses_p_hat_from_pos(self):
        t = _tracker()
        pos = _pos(direction=Direction.YES.value, p_hat=0.88)
        await t._close(1, pos, _RESOLVED_THRESHOLD, "resolved", Direction.YES.value, 100.0, 0.50)
        kw = t._db.insert_brier_score.call_args.kwargs
        assert kw["p_hat"] == pytest.approx(0.88)

    @pytest.mark.asyncio
    async def test_brier_score_uses_signal_source_from_pos(self):
        t = _tracker()
        pos = _pos(direction=Direction.YES.value, signal_source="watched_pages")
        await t._close(1, pos, _RESOLVED_THRESHOLD, "resolved", Direction.YES.value, 100.0, 0.50)
        kw = t._db.insert_brier_score.call_args.kwargs
        assert kw["signal_source"] == "watched_pages"

    @pytest.mark.asyncio
    async def test_no_brier_on_timeout_regardless_of_price(self):
        t = _tracker()
        pos = _pos(direction=Direction.YES.value)
        # Even if exit_price looks resolved, timeout → no Brier
        await t._close(1, pos, _RESOLVED_THRESHOLD, "timeout", Direction.YES.value, 100.0, 0.50)
        t._db.insert_brier_score.assert_not_called()


# ---------------------------------------------------------------------------
# P&L arithmetic
# ---------------------------------------------------------------------------


class TestPnlArithmetic:
    @pytest.mark.asyncio
    async def test_realized_pnl_formula(self):
        """shares = size_usdc / entry_price; pnl = (exit - entry) * shares"""
        t = _tracker()
        pos = _pos(entry_price=0.40, size_usdc=80.0, direction=Direction.YES.value)
        # shares = 80 / 0.40 = 200
        # pnl = (0.80 - 0.40) * 200 = 80
        await t._close(1, pos, 0.80, "resolved", Direction.YES.value, 80.0, 0.40)
        kw = t._db.close_trade.call_args.kwargs
        assert kw["realized_pnl"] == pytest.approx(80.0, abs=0.01)

    @pytest.mark.asyncio
    async def test_realized_pnl_negative_on_loss(self):
        t = _tracker()
        pos = _pos(entry_price=0.60, size_usdc=60.0, direction=Direction.YES.value)
        # shares = 60 / 0.60 = 100; pnl = (0.10 - 0.60) * 100 = -50
        await t._close(1, pos, 0.10, "resolved", Direction.YES.value, 60.0, 0.60)
        kw = t._db.close_trade.call_args.kwargs
        assert kw["realized_pnl"] == pytest.approx(-50.0, abs=0.01)

    @pytest.mark.asyncio
    async def test_unrealized_pnl_updated_before_close_decision(self):
        t = _tracker()
        pos = _pos(entry_price=0.50, size_usdc=100.0)
        # current_price = 0.60 → unrealized = (0.60 - 0.50) * 200 = 20
        clob = _clob(0.60)
        await t._check_one(pos, clob, timeout_hours=48.0, now=_now())
        kw = t._db.upsert_position.call_args.kwargs
        assert kw["unrealized_pnl"] == pytest.approx(20.0, abs=0.01)


# ---------------------------------------------------------------------------
# Rate limiting: skip when CLOB budget exhausted
# ---------------------------------------------------------------------------


class TestClobRateLimit:
    @pytest.mark.asyncio
    async def test_skips_check_when_clob_budget_exhausted(self):
        t = _tracker()
        t._redis.try_consume_daily_budget = AsyncMock(return_value=False)
        pos = _pos()
        clob = _clob(0.60)
        # Should not call clob.get_orderbook at all
        await t._check_one(pos, clob, timeout_hours=48.0, now=_now())
        clob.get_orderbook.assert_not_awaited()
        t._db.close_trade.assert_not_called()


# ---------------------------------------------------------------------------
# Multiple positions
# ---------------------------------------------------------------------------


class TestMultiplePositions:
    @pytest.mark.asyncio
    async def test_exception_in_one_position_does_not_stop_others(self):
        """_check_all should catch per-position errors and continue."""
        t = _tracker()
        pos1 = _pos(trade_id=1, token_id="bad_token")
        pos2 = _pos(trade_id=2, token_id="good_token")
        t._db.get_open_positions = AsyncMock(return_value=[pos1, pos2])

        clob = MagicMock()

        async def _side_effect(token_id):
            if token_id == "bad_token":
                raise RuntimeError("CLOB error for bad token")
            return _ob(0.98)

        clob.get_orderbook = AsyncMock(side_effect=_side_effect)
        clob.__aenter__ = AsyncMock(return_value=clob)
        clob.__aexit__ = AsyncMock(return_value=False)

        from unittest.mock import patch
        with patch("position_tracker.ClobClient", return_value=clob):
            await t._check_all()

        # pos2 should be closed (price 0.98 >= 0.97)
        t._db.close_trade.assert_called_once()
        kw = t._db.close_trade.call_args.kwargs
        assert kw["exit_reason"] == "resolved"
