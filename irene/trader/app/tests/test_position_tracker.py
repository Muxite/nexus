"""Unit tests for position_tracker.py — close logic, resolution, timeout."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from irene_shared.models import Direction, MarketEventType, Orderbook
from position_tracker import PositionTracker, _RESOLVED_THRESHOLD, _ANTI_RESOLVED_THRESHOLD


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _make_pos(
    trade_id: int = 1,
    direction: str = Direction.YES.value,
    size_usdc: float = 100.0,
    entry_price: float = 0.50,
    opened_at: datetime | None = None,
    condition_id: str = "cond_abc",
    token_id: str = "tok_yes",
    signal_source: str = "newsapi",
    p_hat: float = 0.65,
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


def _make_ob(mid_price: float) -> Orderbook:
    bid = max(mid_price - 0.01, 0.01)
    ask = min(mid_price + 0.01, 0.99)
    return Orderbook(
        token_id="tok_yes",
        best_bid=bid,
        best_ask=ask,
        mid_price=mid_price,
        bid_depth_usdc=500.0,
        ask_depth_usdc=500.0,
    )


def _make_tracker(
    positions: list[dict] | None = None,
    timeout_hours: float = 48.0,
) -> PositionTracker:
    db = MagicMock()
    db.get_open_positions = AsyncMock(return_value=positions or [])
    db.upsert_position = AsyncMock()
    db.close_trade = AsyncMock()
    db.delete_position = AsyncMock()
    db.log_event = AsyncMock()
    db.insert_brier_score = AsyncMock()

    cfg = MagicMock()
    cfg.get.return_value.trading.position_timeout_hours = timeout_hours
    cfg.get.return_value.polymarket.clob_url = "https://clob.polymarket.com"
    cfg.get.return_value.timing.jitter_fraction = 0.0

    tracker = PositionTracker(cfg, db, MagicMock())
    return tracker


def _make_clob(mid_price: float | None) -> MagicMock:
    clob = MagicMock()
    ob = _make_ob(mid_price) if mid_price is not None else None
    clob.get_orderbook = AsyncMock(return_value=ob)
    return clob


# ---------------------------------------------------------------------------
# _check_one — no close triggered
# ---------------------------------------------------------------------------


class TestCheckOneNoClose:
    @pytest.mark.asyncio
    async def test_updates_unrealized_pnl_when_price_available(self):
        tracker = _make_tracker()
        pos = _make_pos(entry_price=0.50)
        clob = _make_clob(0.60)

        await tracker._check_one(pos, clob, timeout_hours=48.0, now=_now())

        tracker._db.upsert_position.assert_called_once()
        call_kwargs = tracker._db.upsert_position.call_args.kwargs
        assert call_kwargs["last_price"] == pytest.approx(0.60)
        # unrealized = (0.60 - 0.50) * (100 / 0.50) = 0.10 * 200 = 20
        assert call_kwargs["unrealized_pnl"] == pytest.approx(20.0)

    @pytest.mark.asyncio
    async def test_no_close_for_mid_market_price(self):
        tracker = _make_tracker()
        pos = _make_pos(entry_price=0.50)
        clob = _make_clob(0.55)  # not resolved

        await tracker._check_one(pos, clob, timeout_hours=48.0, now=_now())

        tracker._db.close_trade.assert_not_called()
        tracker._db.delete_position.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_close_when_clob_returns_none(self):
        tracker = _make_tracker()
        pos = _make_pos()
        clob = _make_clob(None)

        await tracker._check_one(pos, clob, timeout_hours=48.0, now=_now())

        tracker._db.close_trade.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_close_when_within_timeout(self):
        tracker = _make_tracker()
        pos = _make_pos(opened_at=_now() - timedelta(hours=24))
        clob = _make_clob(0.55)

        await tracker._check_one(pos, clob, timeout_hours=48.0, now=_now())

        tracker._db.close_trade.assert_not_called()


# ---------------------------------------------------------------------------
# _check_one — resolution triggers close
# ---------------------------------------------------------------------------


class TestCheckOneResolution:
    @pytest.mark.asyncio
    async def test_yes_resolved_closes_position(self):
        tracker = _make_tracker()
        pos = _make_pos(direction=Direction.YES.value)
        clob = _make_clob(_RESOLVED_THRESHOLD)

        await tracker._check_one(pos, clob, timeout_hours=48.0, now=_now())

        tracker._db.close_trade.assert_called_once()
        call_kwargs = tracker._db.close_trade.call_args.kwargs
        assert call_kwargs["exit_reason"] == "resolved"

    @pytest.mark.asyncio
    async def test_no_resolved_closes_position(self):
        tracker = _make_tracker()
        pos = _make_pos(direction=Direction.NO.value)
        clob = _make_clob(_ANTI_RESOLVED_THRESHOLD)

        await tracker._check_one(pos, clob, timeout_hours=48.0, now=_now())

        tracker._db.close_trade.assert_called_once()
        call_kwargs = tracker._db.close_trade.call_args.kwargs
        assert call_kwargs["exit_reason"] == "resolved"

    @pytest.mark.asyncio
    async def test_resolved_deletes_position(self):
        tracker = _make_tracker()
        pos = _make_pos()
        clob = _make_clob(_RESOLVED_THRESHOLD)

        await tracker._check_one(pos, clob, timeout_hours=48.0, now=_now())

        tracker._db.delete_position.assert_called_once_with(pos["trade_id"])

    @pytest.mark.asyncio
    async def test_resolved_logs_event(self):
        tracker = _make_tracker()
        pos = _make_pos()
        clob = _make_clob(_RESOLVED_THRESHOLD)

        await tracker._check_one(pos, clob, timeout_hours=48.0, now=_now())

        tracker._db.log_event.assert_called_once()
        call_kwargs = tracker._db.log_event.call_args.kwargs
        assert call_kwargs["event_type"] == MarketEventType.POSITION_CLOSED


# ---------------------------------------------------------------------------
# _check_one — timeout triggers close
# ---------------------------------------------------------------------------


class TestCheckOneTimeout:
    @pytest.mark.asyncio
    async def test_timeout_closes_position(self):
        tracker = _make_tracker()
        pos = _make_pos(opened_at=_now() - timedelta(hours=50))
        clob = _make_clob(0.55)

        await tracker._check_one(pos, clob, timeout_hours=48.0, now=_now())

        tracker._db.close_trade.assert_called_once()
        call_kwargs = tracker._db.close_trade.call_args.kwargs
        assert call_kwargs["exit_reason"] == "timeout"

    @pytest.mark.asyncio
    async def test_timeout_at_exact_boundary(self):
        tracker = _make_tracker()
        now = _now()
        pos = _make_pos(opened_at=now - timedelta(hours=48))
        clob = _make_clob(0.55)

        await tracker._check_one(pos, clob, timeout_hours=48.0, now=now)

        tracker._db.close_trade.assert_called_once()

    @pytest.mark.asyncio
    async def test_timeout_uses_entry_price_when_no_clob_price(self):
        tracker = _make_tracker()
        pos = _make_pos(entry_price=0.50, opened_at=_now() - timedelta(hours=50))
        clob = _make_clob(None)

        await tracker._check_one(pos, clob, timeout_hours=48.0, now=_now())

        tracker._db.close_trade.assert_called_once()
        call_kwargs = tracker._db.close_trade.call_args.kwargs
        assert call_kwargs["exit_price"] == pytest.approx(0.50)  # falls back to entry_price


# ---------------------------------------------------------------------------
# _close — P&L and outcome calculation
# ---------------------------------------------------------------------------


class TestClose:
    @pytest.mark.asyncio
    async def test_realized_pnl_win(self):
        tracker = _make_tracker()
        pos = _make_pos(entry_price=0.50, size_usdc=100.0, direction=Direction.YES.value)

        await tracker._close(
            trade_id=1,
            pos=pos,
            exit_price=_RESOLVED_THRESHOLD,
            exit_reason="resolved",
            direction=Direction.YES.value,
            size_usdc=100.0,
            entry_price=0.50,
        )

        call_kwargs = tracker._db.close_trade.call_args.kwargs
        # shares = 100/0.50 = 200, pnl = (0.97-0.50)*200 = 94
        assert call_kwargs["realized_pnl"] == pytest.approx((_RESOLVED_THRESHOLD - 0.50) * 200)
        assert call_kwargs["outcome"] == "WIN"

    @pytest.mark.asyncio
    async def test_realized_pnl_loss(self):
        tracker = _make_tracker()
        pos = _make_pos(entry_price=0.50, size_usdc=100.0, direction=Direction.YES.value)

        await tracker._close(
            trade_id=1,
            pos=pos,
            exit_price=_ANTI_RESOLVED_THRESHOLD,  # YES position resolved NO → LOSS
            exit_reason="resolved",
            direction=Direction.YES.value,
            size_usdc=100.0,
            entry_price=0.50,
        )

        call_kwargs = tracker._db.close_trade.call_args.kwargs
        assert call_kwargs["realized_pnl"] < 0
        assert call_kwargs["outcome"] == "LOSS"

    @pytest.mark.asyncio
    async def test_outcome_push_on_timeout(self):
        tracker = _make_tracker()
        pos = _make_pos(entry_price=0.50, size_usdc=100.0, direction=Direction.YES.value)

        await tracker._close(
            trade_id=1,
            pos=pos,
            exit_price=0.55,  # mid-market, not resolved
            exit_reason="timeout",
            direction=Direction.YES.value,
            size_usdc=100.0,
            entry_price=0.50,
        )

        call_kwargs = tracker._db.close_trade.call_args.kwargs
        assert call_kwargs["outcome"] == "PUSH"

    @pytest.mark.asyncio
    async def test_inserts_brier_score_on_resolution(self):
        tracker = _make_tracker()
        pos = _make_pos(p_hat=0.65, signal_source="newsapi", direction=Direction.YES.value)

        await tracker._close(
            trade_id=1,
            pos=pos,
            exit_price=_RESOLVED_THRESHOLD,
            exit_reason="resolved",
            direction=Direction.YES.value,
            size_usdc=100.0,
            entry_price=0.50,
        )

        tracker._db.insert_brier_score.assert_called_once()
        call_kwargs = tracker._db.insert_brier_score.call_args.kwargs
        assert call_kwargs["p_hat"] == pytest.approx(0.65)
        assert call_kwargs["signal_source"] == "newsapi"

    @pytest.mark.asyncio
    async def test_no_brier_score_on_timeout(self):
        tracker = _make_tracker()
        pos = _make_pos(direction=Direction.YES.value)

        await tracker._close(
            trade_id=1,
            pos=pos,
            exit_price=0.55,
            exit_reason="timeout",
            direction=Direction.YES.value,
            size_usdc=100.0,
            entry_price=0.50,
        )

        tracker._db.insert_brier_score.assert_not_called()

    @pytest.mark.asyncio
    async def test_naive_opened_at_gets_utc_timezone(self):
        """opened_at without tzinfo should be treated as UTC."""
        tracker = _make_tracker()
        now = _now()
        pos = _make_pos(opened_at=datetime(2025, 1, 1, 0, 0, 0))  # naive
        clob = _make_clob(0.55)

        # Should not raise even with naive datetime
        await tracker._check_one(pos, clob, timeout_hours=0.01, now=now)
        tracker._db.close_trade.assert_called_once()
