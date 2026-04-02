"""Unit tests for irene_shared/db.py — IreneDB with mocked asyncpg pool."""

from __future__ import annotations

import pickle
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import numpy as np
import pytest

from irene_shared.db import IreneDB


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _make_conn() -> MagicMock:
    """Return a mock asyncpg connection with the common query methods."""
    conn = MagicMock()
    conn.execute = AsyncMock(return_value="UPDATE 0")
    conn.fetchrow = AsyncMock(return_value=None)
    conn.fetchval = AsyncMock(return_value=0)
    conn.fetch = AsyncMock(return_value=[])
    return conn


def _make_pool(conn: MagicMock | None = None) -> MagicMock:
    """Return a mock asyncpg pool that yields *conn* from acquire()."""
    if conn is None:
        conn = _make_conn()
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=conn)
    cm.__aexit__ = AsyncMock(return_value=False)

    pool = MagicMock()
    pool.acquire = MagicMock(return_value=cm)
    return pool


def _make_db(conn: MagicMock | None = None) -> tuple[IreneDB, MagicMock]:
    """Return a pre-connected IreneDB (pool injected) and the underlying conn mock."""
    if conn is None:
        conn = _make_conn()
    db = IreneDB("postgresql://test/test")
    db._pool = _make_pool(conn)
    return db, conn


# ---------------------------------------------------------------------------
# Pool lifecycle
# ---------------------------------------------------------------------------


class TestPoolRequired:

    def test_raises_when_not_connected(self):
        db = IreneDB("postgresql://test/test")
        # _pool is None before connect()
        with pytest.raises(RuntimeError, match="connect"):
            db._pool_required()


# ---------------------------------------------------------------------------
# upsert_market
# ---------------------------------------------------------------------------


class TestUpsertMarket:

    @pytest.mark.asyncio
    async def test_calls_execute(self):
        db, conn = _make_db()
        emb = np.zeros(384, dtype=np.float32)
        await db.upsert_market(
            condition_id="cond_1",
            slug="my-market",
            question="Will it rain?",
            description="desc",
            category="weather",
            end_date=_now(),
            volume_usdc=10000.0,
            token_yes="tok_yes",
            token_no="tok_no",
            embedding=emb,
        )
        conn.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_serializes_embedding_as_bytes(self):
        db, conn = _make_db()
        emb = np.ones(384, dtype=np.float32) * 0.5
        await db.upsert_market(
            condition_id="cond_1",
            slug=None,
            question="Q",
            description=None,
            category=None,
            end_date=_now(),
            volume_usdc=None,
            token_yes=None,
            token_no=None,
            embedding=emb,
        )
        call_args = conn.execute.call_args.args
        # Find the embedding bytes argument (should be pickle of float32 array)
        emb_bytes = call_args[10]  # 11th positional arg (0-indexed 10)
        recovered = pickle.loads(emb_bytes)
        assert np.allclose(recovered, emb)

    @pytest.mark.asyncio
    async def test_none_embedding_passes_none(self):
        db, conn = _make_db()
        await db.upsert_market(
            condition_id="cond_1",
            slug=None,
            question="Q",
            description=None,
            category=None,
            end_date=_now(),
            volume_usdc=None,
            token_yes=None,
            token_no=None,
            embedding=None,
        )
        call_args = conn.execute.call_args.args
        assert call_args[10] is None  # embedding_bytes is None


# ---------------------------------------------------------------------------
# insert_signal
# ---------------------------------------------------------------------------


class TestInsertSignal:

    @pytest.mark.asyncio
    async def test_returns_id_from_fetchrow(self):
        conn = _make_conn()
        conn.fetchrow = AsyncMock(return_value={"id": 7})
        db, _ = _make_db(conn)

        result = await db.insert_signal(
            source="newsapi",
            source_detail="newsapi",
            url="https://news.com/a",
            title="Title",
            description="Desc",
            published_at=_now(),
        )
        assert result == 7

    @pytest.mark.asyncio
    async def test_returns_none_on_conflict(self):
        conn = _make_conn()
        conn.fetchrow = AsyncMock(return_value=None)
        db, _ = _make_db(conn)

        result = await db.insert_signal(
            source="newsapi",
            source_detail="newsapi",
            url="https://news.com/duplicate",
            title="Title",
            description=None,
            published_at=None,
        )
        assert result is None


# ---------------------------------------------------------------------------
# deactivate_expired_markets
# ---------------------------------------------------------------------------


class TestDeactivateExpiredMarkets:

    @pytest.mark.asyncio
    async def test_returns_count_from_update(self):
        conn = _make_conn()
        conn.execute = AsyncMock(return_value="UPDATE 5")
        db, _ = _make_db(conn)

        result = await db.deactivate_expired_markets(before=_now())
        assert result == 5

    @pytest.mark.asyncio
    async def test_returns_zero_when_none_expired(self):
        conn = _make_conn()
        conn.execute = AsyncMock(return_value="UPDATE 0")
        db, _ = _make_db(conn)

        result = await db.deactivate_expired_markets(before=_now())
        assert result == 0


# ---------------------------------------------------------------------------
# count_open_positions
# ---------------------------------------------------------------------------


class TestCountOpenPositions:

    @pytest.mark.asyncio
    async def test_returns_fetchval_result(self):
        conn = _make_conn()
        conn.fetchval = AsyncMock(return_value=3)
        db, _ = _make_db(conn)

        result = await db.count_open_positions()
        assert result == 3

    @pytest.mark.asyncio
    async def test_returns_zero_when_no_positions(self):
        conn = _make_conn()
        conn.fetchval = AsyncMock(return_value=0)
        db, _ = _make_db(conn)

        assert await db.count_open_positions() == 0


# ---------------------------------------------------------------------------
# log_event
# ---------------------------------------------------------------------------


class TestLogEvent:

    @pytest.mark.asyncio
    async def test_calls_execute_with_condition_and_type(self):
        db, conn = _make_db()
        await db.log_event(
            condition_id="cond_abc",
            event_type="signal_match",
            payload={"key": "value"},
        )
        conn.execute.assert_called_once()
        sql, *args = conn.execute.call_args.args
        assert "market_events" in sql
        assert args[0] == "cond_abc"
        assert args[1] == "signal_match"

    @pytest.mark.asyncio
    async def test_serializes_payload_as_json(self):
        import json
        db, conn = _make_db()
        payload = {"score": 0.95, "nested": {"k": 1}}
        await db.log_event(
            condition_id="cond_1",
            event_type="trade_decided",
            payload=payload,
        )
        _, _, payload_json, _ = conn.execute.call_args.args[1:]
        recovered = json.loads(payload_json)
        assert recovered["score"] == pytest.approx(0.95)
        assert recovered["nested"]["k"] == 1


# ---------------------------------------------------------------------------
# insert_trade
# ---------------------------------------------------------------------------


class TestInsertTrade:

    @pytest.mark.asyncio
    async def test_returns_trade_id(self):
        conn = _make_conn()
        conn.fetchrow = AsyncMock(return_value={"id": 42})
        db, _ = _make_db(conn)

        result = await db.insert_trade(
            mapped_signal_id=1,
            condition_id="cond_abc",
            token_id="tok_yes",
            direction="YES",
            p_hat=0.62,
            mid_price=0.50,
            edge=0.08,
            kelly_size_usdc=50.0,
            executed=False,
        )
        assert result == 42

    @pytest.mark.asyncio
    async def test_calls_fetchrow(self):
        conn = _make_conn()
        conn.fetchrow = AsyncMock(return_value={"id": 1})
        db, _ = _make_db(conn)

        await db.insert_trade(
            mapped_signal_id=None,
            condition_id="cond_abc",
            token_id="tok_no",
            direction="NO",
            p_hat=0.38,
            mid_price=0.50,
            edge=0.06,
            kelly_size_usdc=30.0,
            executed=True,
            order_type="FOK",
        )
        conn.fetchrow.assert_called_once()


# ---------------------------------------------------------------------------
# insert_mapped_signal
# ---------------------------------------------------------------------------


class TestInsertMappedSignal:

    @pytest.mark.asyncio
    async def test_returns_id(self):
        conn = _make_conn()
        conn.fetchrow = AsyncMock(return_value={"id": 3})
        db, _ = _make_db(conn)

        result = await db.insert_mapped_signal(
            signal_id=1,
            condition_id="cond_abc",
            similarity_score=0.82,
            direction="YES",
            direction_confidence=0.75,
        )
        assert result == 3


# ---------------------------------------------------------------------------
# insert_brier_score
# ---------------------------------------------------------------------------


class TestInsertBrierScore:

    @pytest.mark.asyncio
    async def test_computes_brier_component(self):
        db, conn = _make_db()
        await db.insert_brier_score(
            trade_id=1,
            signal_source="newsapi",
            p_hat=0.7,
            actual_outcome=1.0,
        )
        conn.execute.assert_called_once()
        call_args = conn.execute.call_args.args
        # brier_component = (0.7 - 1.0)^2 = 0.09 — positional arg index 5
        brier_component = call_args[5]
        assert brier_component == pytest.approx(0.09)

    @pytest.mark.asyncio
    async def test_brier_perfect_prediction_is_zero(self):
        db, conn = _make_db()
        await db.insert_brier_score(
            trade_id=1,
            signal_source="newsapi",
            p_hat=1.0,
            actual_outcome=1.0,
        )
        brier_component = conn.execute.call_args.args[5]
        assert brier_component == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# upsert_position
# ---------------------------------------------------------------------------


class TestUpsertPosition:

    @pytest.mark.asyncio
    async def test_calls_execute(self):
        db, conn = _make_db()
        await db.upsert_position(
            trade_id=1,
            condition_id="cond_abc",
            token_id="tok_yes",
            direction="YES",
            size_usdc=50.0,
            entry_price=0.52,
            opened_at=_now(),
            last_price=0.54,
            unrealized_pnl=1.0,
        )
        conn.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_position_calls_execute(self):
        db, conn = _make_db()
        await db.delete_position(trade_id=7)
        conn.execute.assert_called_once()
        sql = conn.execute.call_args.args[0]
        assert "DELETE" in sql
