"""Unit tests for trader/app/main.py — Trader._handle_signal pipeline."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from irene_shared.models import (
    Direction,
    MappedSignal,
    Orderbook,
    TradeDecision,
    OrderType,
)
from main import Trader


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _make_cfg(mode: str = "paper"):
    cfg = MagicMock()
    cfg.polymarket.clob_url = "https://clob.polymarket.com"
    cfg.trading.balance_usdc = 1000.0
    cfg.trading.max_open_positions = 10
    cfg.rate_limits.clob_max_per_day = 20000
    return cfg


def _make_config_manager(cfg=None):
    m = MagicMock()
    m.get.return_value = cfg or _make_cfg()
    return m


def _make_db(open_positions: int = 0):
    db = MagicMock()
    db.count_open_positions = AsyncMock(return_value=open_positions)
    db.log_event = AsyncMock()
    return db


def _make_redis():
    redis = MagicMock()
    redis.subscribe_signals = MagicMock()
    redis.set_last_seen = AsyncMock()
    redis.try_consume_daily_budget = AsyncMock(return_value=True)
    return redis


def _make_mapped_signal(
    direction: Direction = Direction.YES,
    token_yes: str = "tok_yes",
    token_no: str = "tok_no",
) -> MappedSignal:
    return MappedSignal(
        signal_id=1,
        condition_id="cond_abc",
        token_yes=token_yes,
        token_no=token_no,
        question="Will rates exceed 5%?",
        similarity_score=0.82,
        direction=direction,
        direction_confidence=0.75,
        signal_title="Fed raises rates",
        signal_source="newsapi",
        mapped_at=_now(),
    )


def _make_orderbook(
    token_id: str = "tok_yes",
    mid_price: float = 0.50,
) -> Orderbook:
    return Orderbook(
        token_id=token_id,
        best_bid=mid_price - 0.01,
        best_ask=mid_price + 0.01,
        mid_price=mid_price,
        bid_depth_usdc=500.0,
        ask_depth_usdc=500.0,
    )


def _make_decision(skip_reason: str | None = None) -> TradeDecision:
    return TradeDecision(
        mapped_signal_id=1,
        condition_id="cond_abc",
        token_id="tok_yes",
        direction=Direction.YES,
        p_hat=0.62,
        mid_price=0.50,
        edge=0.08,
        kelly_size_usdc=50.0,
        executed=False,
        order_type=OrderType.FOK,
        skip_reason=skip_reason,
        decided_at=_now(),
    )


def _make_clob_cm(orderbook):
    """Return a mock GammaClient-style async context manager."""
    client = MagicMock()
    client.get_orderbook = AsyncMock(return_value=orderbook)

    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=client)
    cm.__aexit__ = AsyncMock(return_value=False)

    klass = MagicMock(return_value=cm)
    return klass, client


def _make_trader(db=None, redis=None, cfg=None) -> Trader:
    with patch("main.DecisionEngine"), patch("main.Executor") as MockExecutor, \
         patch("main.PositionTracker"):
        # execute() is awaited in _handle_signal — must be AsyncMock
        MockExecutor.return_value.execute = AsyncMock()
        trader = Trader(
            _make_config_manager(cfg),
            db or _make_db(),
            redis or _make_redis(),
        )
    return trader


# ---------------------------------------------------------------------------
# _handle_signal — orderbook fetch
# ---------------------------------------------------------------------------


class TestHandleSignalOrderbook:

    @pytest.mark.asyncio
    async def test_skips_when_orderbook_none(self):
        db = _make_db()
        trader = _make_trader(db=db)

        clob_class, clob_client = _make_clob_cm(None)

        with patch("main.ClobClient", clob_class):
            await trader._handle_signal(_make_mapped_signal())

        # No decision logged if orderbook is unavailable
        db.log_event.assert_not_called()

    @pytest.mark.asyncio
    async def test_uses_yes_token_for_yes_direction(self):
        db = _make_db()
        trader = _make_trader(db=db)
        trader._engine.evaluate.return_value = _make_decision(skip_reason="test_skip")

        ob = _make_orderbook("tok_yes")
        clob_class, clob_client = _make_clob_cm(ob)

        with patch("main.ClobClient", clob_class):
            await trader._handle_signal(
                _make_mapped_signal(direction=Direction.YES, token_yes="tok_yes")
            )

        clob_client.get_orderbook.assert_called_once_with("tok_yes")

    @pytest.mark.asyncio
    async def test_uses_no_token_for_no_direction(self):
        db = _make_db()
        trader = _make_trader(db=db)
        trader._engine.evaluate.return_value = _make_decision(skip_reason="test_skip")

        ob = _make_orderbook("tok_no")
        clob_class, clob_client = _make_clob_cm(ob)

        with patch("main.ClobClient", clob_class):
            await trader._handle_signal(
                _make_mapped_signal(direction=Direction.NO, token_no="tok_no")
            )

        clob_client.get_orderbook.assert_called_once_with("tok_no")


# ---------------------------------------------------------------------------
# _handle_signal — decision evaluation
# ---------------------------------------------------------------------------


class TestHandleSignalDecision:

    @pytest.mark.asyncio
    async def test_evaluates_decision(self):
        db = _make_db(open_positions=2)
        trader = _make_trader(db=db)
        trader._engine.evaluate.return_value = _make_decision(skip_reason="too_small")

        ob = _make_orderbook()
        clob_class, _ = _make_clob_cm(ob)

        with patch("main.ClobClient", clob_class):
            await trader._handle_signal(_make_mapped_signal())

        trader._engine.evaluate.assert_called_once()
        call_kwargs = trader._engine.evaluate.call_args.kwargs
        assert call_kwargs["open_positions"] == 2

    @pytest.mark.asyncio
    async def test_logs_trade_decided_event(self):
        db = _make_db()
        trader = _make_trader(db=db)
        trader._engine.evaluate.return_value = _make_decision(skip_reason="too_small")

        ob = _make_orderbook()
        clob_class, _ = _make_clob_cm(ob)

        with patch("main.ClobClient", clob_class):
            await trader._handle_signal(_make_mapped_signal())

        from irene_shared.models import MarketEventType
        db.log_event.assert_called_once()
        assert db.log_event.call_args.kwargs["event_type"] == MarketEventType.TRADE_DECIDED

    @pytest.mark.asyncio
    async def test_log_event_payload_contains_skip_reason(self):
        db = _make_db()
        trader = _make_trader(db=db)
        trader._engine.evaluate.return_value = _make_decision(skip_reason="edge_too_small")

        ob = _make_orderbook()
        clob_class, _ = _make_clob_cm(ob)

        with patch("main.ClobClient", clob_class):
            await trader._handle_signal(_make_mapped_signal())

        payload = db.log_event.call_args.kwargs["payload"]
        assert payload["skip_reason"] == "edge_too_small"

    @pytest.mark.asyncio
    async def test_log_event_payload_contains_p_hat(self):
        db = _make_db()
        trader = _make_trader(db=db)
        decision = _make_decision()
        trader._engine.evaluate.return_value = decision

        ob = _make_orderbook()
        clob_class, _ = _make_clob_cm(ob)

        with patch("main.ClobClient", clob_class):
            await trader._handle_signal(_make_mapped_signal())

        payload = db.log_event.call_args.kwargs["payload"]
        assert payload["p_hat"] == pytest.approx(0.62)


# ---------------------------------------------------------------------------
# _handle_signal — execution gating
# ---------------------------------------------------------------------------


class TestHandleSignalExecution:

    @pytest.mark.asyncio
    async def test_skips_execution_when_skip_reason_set(self):
        trader = _make_trader()
        trader._engine.evaluate.return_value = _make_decision(skip_reason="edge_too_small")

        ob = _make_orderbook()
        clob_class, _ = _make_clob_cm(ob)

        with patch("main.ClobClient", clob_class):
            await trader._handle_signal(_make_mapped_signal())

        trader._executor.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_executes_when_no_skip_reason(self):
        redis = _make_redis()
        trader = _make_trader(redis=redis)
        trader._engine.evaluate.return_value = _make_decision(skip_reason=None)
        trader._executor.execute = AsyncMock()

        ob = _make_orderbook()
        clob_class, _ = _make_clob_cm(ob)

        with patch("main.ClobClient", clob_class):
            await trader._handle_signal(_make_mapped_signal())

        trader._executor.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_updates_last_seen_after_execute(self):
        redis = _make_redis()
        trader = _make_trader(redis=redis)
        trader._engine.evaluate.return_value = _make_decision(skip_reason=None)
        trader._executor.execute = AsyncMock()

        ob = _make_orderbook()
        clob_class, _ = _make_clob_cm(ob)

        with patch("main.ClobClient", clob_class):
            await trader._handle_signal(_make_mapped_signal())

        redis.set_last_seen.assert_called_once()
        call_args = redis.set_last_seen.call_args
        assert call_args.args[0] == "trader"

    @pytest.mark.asyncio
    async def test_no_last_seen_update_when_skipped(self):
        redis = _make_redis()
        trader = _make_trader(redis=redis)
        trader._engine.evaluate.return_value = _make_decision(skip_reason="no_edge")

        ob = _make_orderbook()
        clob_class, _ = _make_clob_cm(ob)

        with patch("main.ClobClient", clob_class):
            await trader._handle_signal(_make_mapped_signal())

        redis.set_last_seen.assert_not_called()


# ---------------------------------------------------------------------------
# _signal_loop — control flow
# ---------------------------------------------------------------------------


class TestSignalLoop:

    @pytest.mark.asyncio
    async def test_loop_exits_when_not_running(self):
        trader = _make_trader()
        trader._running = False

        handle_calls = []

        async def _fake_handle(ms):
            handle_calls.append(ms)

        async def _empty_subscribe():
            return
            yield  # make it a generator

        trader._redis.subscribe_signals.return_value = _empty_subscribe()
        trader._handle_signal = _fake_handle

        await trader._signal_loop()
        assert handle_calls == []

    @pytest.mark.asyncio
    async def test_loop_handles_each_signal(self):
        trader = _make_trader()
        trader._running = True

        ms = _make_mapped_signal()
        handled = []

        async def _fake_handle(signal):
            handled.append(signal)
            trader._running = False

        async def _one_signal():
            yield ms.model_dump_json()

        trader._redis.subscribe_signals.return_value = _one_signal()
        trader._handle_signal = _fake_handle

        await trader._signal_loop()
        assert len(handled) == 1

    @pytest.mark.asyncio
    async def test_loop_continues_after_exception(self):
        trader = _make_trader()
        trader._running = True

        call_count = 0
        ms = _make_mapped_signal()

        async def _raising_handle(signal):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("boom")
            trader._running = False

        async def _two_signals():
            yield ms.model_dump_json()
            yield ms.model_dump_json()

        trader._redis.subscribe_signals.return_value = _two_signals()
        trader._handle_signal = _raising_handle

        await trader._signal_loop()
        assert call_count == 2


# ---------------------------------------------------------------------------
# _profile_loop — Phase 7 profile publishing
# ---------------------------------------------------------------------------


class TestProfileLoop:

    @pytest.mark.asyncio
    async def test_skips_publish_when_interval_zero(self):
        redis = _make_redis()
        redis.set_profile = AsyncMock()
        trader = _make_trader(redis=redis)
        trader._running = True

        cfg = _make_cfg()
        cfg.timing = MagicMock()
        cfg.timing.publish_interval_seconds = 0

        async def _fake_sleep(secs):
            trader._running = False

        with patch.object(trader._cfg, "get", return_value=cfg), \
             patch("asyncio.sleep", side_effect=_fake_sleep):
            await trader._profile_loop()

        redis.set_profile.assert_not_called()

    @pytest.mark.asyncio
    async def test_publishes_when_interval_positive(self):
        redis = _make_redis()
        redis.set_profile = AsyncMock()
        trader = _make_trader(redis=redis)
        trader._running = True

        cfg = _make_cfg()
        cfg.timing = MagicMock()
        cfg.timing.publish_interval_seconds = 60

        async def _fake_sleep(secs):
            trader._running = False

        with patch.object(trader._cfg, "get", return_value=cfg), \
             patch("asyncio.sleep", side_effect=_fake_sleep):
            await trader._profile_loop()

        redis.set_profile.assert_called_once()
        args = redis.set_profile.call_args.args
        assert args[0] == "trader"
        assert isinstance(args[1], dict)
