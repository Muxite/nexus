"""Unit tests for executor.py — paper and live mode execution."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from irene_shared.models import Direction, FillResult, OrderType, TradeDecision, TradeMode
from executor import Executor


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _make_decision(
    kelly_size: float = 50.0,
    direction: Direction = Direction.YES,
    skip_reason: str | None = None,
) -> TradeDecision:
    return TradeDecision(
        mapped_signal_id=1,
        condition_id="cond_abc",
        token_id="tok_yes",
        direction=direction,
        p_hat=0.62,
        mid_price=0.50,
        edge=0.08,
        kelly_size_usdc=kelly_size,
        executed=False,
        order_type=OrderType.FOK,
        skip_reason=skip_reason,
        decided_at=_now(),
    )


def _make_cfg(mode: str = "paper") -> MagicMock:
    cfg = MagicMock()
    cfg.irene.mode = TradeMode.LIVE if mode == "live" else TradeMode.PAPER
    cfg.trading.order_type = "FOK"
    cfg.polymarket.clob_url = "https://clob.polymarket.com"
    cfg.polymarket.api_key = "key"
    cfg.polymarket.api_secret = "secret"
    cfg.polymarket.api_passphrase = "pass"
    cfg.polymarket.private_key = "0xprivkey"
    cfg.polymarket.chain_id = 137
    return cfg


def _make_db(trade_id: int = 42) -> MagicMock:
    db = MagicMock()
    db.insert_trade = AsyncMock(return_value=trade_id)
    db.record_fill = AsyncMock()
    db.upsert_position = AsyncMock()
    return db


# ---------------------------------------------------------------------------
# Paper mode
# ---------------------------------------------------------------------------


class TestExecutorPaperMode:
    @pytest.mark.asyncio
    async def test_paper_mode_inserts_trade(self):
        db = _make_db()
        executor = Executor(db)
        result = await executor.execute(_make_decision(), _make_cfg("paper"))

        db.insert_trade.assert_called_once()
        call_kwargs = db.insert_trade.call_args.kwargs
        assert call_kwargs["condition_id"] == "cond_abc"
        assert call_kwargs["executed"] is False

    @pytest.mark.asyncio
    async def test_paper_mode_returns_none(self):
        db = _make_db()
        executor = Executor(db)
        result = await executor.execute(_make_decision(), _make_cfg("paper"))
        assert result is None

    @pytest.mark.asyncio
    async def test_paper_mode_does_not_call_record_fill(self):
        db = _make_db()
        executor = Executor(db)
        await executor.execute(_make_decision(), _make_cfg("paper"))
        db.record_fill.assert_not_called()

    @pytest.mark.asyncio
    async def test_paper_mode_does_not_upsert_position(self):
        db = _make_db()
        executor = Executor(db)
        await executor.execute(_make_decision(), _make_cfg("paper"))
        db.upsert_position.assert_not_called()

    @pytest.mark.asyncio
    async def test_paper_mode_passes_kelly_size_to_insert(self):
        db = _make_db()
        executor = Executor(db)
        await executor.execute(_make_decision(kelly_size=123.45), _make_cfg("paper"))

        call_kwargs = db.insert_trade.call_args.kwargs
        assert call_kwargs["kelly_size_usdc"] == pytest.approx(123.45)

    @pytest.mark.asyncio
    async def test_paper_mode_passes_direction_to_insert(self):
        db = _make_db()
        executor = Executor(db)
        await executor.execute(_make_decision(direction=Direction.NO), _make_cfg("paper"))

        call_kwargs = db.insert_trade.call_args.kwargs
        assert call_kwargs["direction"] == Direction.NO.value


# ---------------------------------------------------------------------------
# Live mode — _submit_sync returns None (no fill)
# ---------------------------------------------------------------------------


class TestExecutorLiveModeNoFill:
    @pytest.mark.asyncio
    async def test_live_inserts_trade_with_executed_true(self):
        db = _make_db()
        executor = Executor(db)

        with patch.object(executor, "_submit_live_order", new_callable=AsyncMock, return_value=None):
            await executor.execute(_make_decision(), _make_cfg("live"))

        call_kwargs = db.insert_trade.call_args.kwargs
        assert call_kwargs["executed"] is True

    @pytest.mark.asyncio
    async def test_live_no_fill_returns_none(self):
        db = _make_db()
        executor = Executor(db)

        with patch.object(executor, "_submit_live_order", new_callable=AsyncMock, return_value=None):
            result = await executor.execute(_make_decision(), _make_cfg("live"))

        assert result is None

    @pytest.mark.asyncio
    async def test_live_no_fill_does_not_call_record_fill(self):
        db = _make_db()
        executor = Executor(db)

        with patch.object(executor, "_submit_live_order", new_callable=AsyncMock, return_value=None):
            await executor.execute(_make_decision(), _make_cfg("live"))

        db.record_fill.assert_not_called()


# ---------------------------------------------------------------------------
# Live mode — successful fill
# ---------------------------------------------------------------------------


class TestExecutorLiveModeWithFill:
    def _make_fill(self) -> FillResult:
        return FillResult(
            order_id="order_abc",
            token_id="tok_yes",
            direction=Direction.YES,
            fill_price=0.52,
            fill_size_usdc=50.0,
            fill_at=_now(),
        )

    @pytest.mark.asyncio
    async def test_live_fill_calls_record_fill(self):
        db = _make_db(trade_id=7)
        executor = Executor(db)
        fill = self._make_fill()

        with patch.object(executor, "_submit_live_order", new_callable=AsyncMock, return_value=fill):
            await executor.execute(_make_decision(), _make_cfg("live"))

        db.record_fill.assert_called_once()
        call_kwargs = db.record_fill.call_args.kwargs
        assert call_kwargs["trade_id"] == 7
        assert call_kwargs["order_id"] == "order_abc"
        assert call_kwargs["fill_price"] == pytest.approx(0.52)

    @pytest.mark.asyncio
    async def test_live_fill_upserts_position(self):
        db = _make_db(trade_id=7)
        executor = Executor(db)
        fill = self._make_fill()

        with patch.object(executor, "_submit_live_order", new_callable=AsyncMock, return_value=fill):
            await executor.execute(_make_decision(), _make_cfg("live"))

        db.upsert_position.assert_called_once()
        call_kwargs = db.upsert_position.call_args.kwargs
        assert call_kwargs["trade_id"] == 7
        assert call_kwargs["entry_price"] == pytest.approx(0.52)

    @pytest.mark.asyncio
    async def test_live_fill_returns_fill_result(self):
        db = _make_db()
        executor = Executor(db)
        fill = self._make_fill()

        with patch.object(executor, "_submit_live_order", new_callable=AsyncMock, return_value=fill):
            result = await executor.execute(_make_decision(), _make_cfg("live"))

        assert result is fill


# ---------------------------------------------------------------------------
# _submit_sync — graceful ImportError
# ---------------------------------------------------------------------------


class TestSubmitSyncImportError:
    def test_returns_none_when_py_clob_client_not_installed(self):
        import sys
        import builtins

        original_import = builtins.__import__

        def _mock_import(name, *args, **kwargs):
            if name.startswith("py_clob_client"):
                raise ImportError("not installed")
            return original_import(name, *args, **kwargs)

        decision = _make_decision()
        cfg = _make_cfg("live")

        with patch("builtins.__import__", side_effect=_mock_import):
            result = Executor._submit_sync(decision, cfg)

        assert result is None
