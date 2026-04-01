"""
Unit tests for monitor route handlers.

Each route handler is an async function that takes a `Request` object and
reads `request.app.state.db` / `request.app.state.redis`. Tests inject mocks
directly into a lightweight mock request — no ASGI server overhead required.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from main import (
    calibration,
    health,
    market_events,
    markets,
    metrics,
    positions,
    signals,
    trades,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_request(db: MagicMock | None = None, redis: MagicMock | None = None) -> MagicMock:
    request = MagicMock()
    request.app.state.db = db or MagicMock()
    request.app.state.redis = redis or MagicMock()
    return request


def _make_db(
    positions_count: int = 0,
    market_rows: list | None = None,
    trade_rows: list | None = None,
    signal_rows: list | None = None,
    event_rows: list | None = None,
    brier_rows: list | None = None,
) -> MagicMock:
    db = MagicMock()
    db.count_open_positions = AsyncMock(return_value=positions_count)
    db.get_active_markets = AsyncMock(return_value=market_rows or [])
    db.get_recent_trades = AsyncMock(return_value=trade_rows or [])
    db.get_recent_signals = AsyncMock(return_value=signal_rows or [])
    db.get_market_events = AsyncMock(return_value=event_rows or [])
    db.get_open_positions = AsyncMock(return_value=[])
    db.get_brier_summary = AsyncMock(return_value=brier_rows or [])
    return db


def _make_redis(
    last_seen_map: dict | None = None,
    profiles: dict | None = None,
) -> MagicMock:
    redis = MagicMock()
    _lsm = last_seen_map or {}
    redis.get_last_seen = AsyncMock(side_effect=lambda svc: _lsm.get(svc))
    _prof = profiles or {}
    redis.get_profile = AsyncMock(side_effect=lambda svc: _prof.get(svc))
    return redis


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------


class TestHealth:
    @pytest.mark.asyncio
    async def test_returns_ok_status(self):
        db = _make_db(positions_count=3)
        redis = _make_redis(last_seen_map={"scanner": "t1", "feeder": "t2", "trader": "t3"})
        result = await health(request=_make_request(db, redis))
        assert result["status"] == "ok"

    @pytest.mark.asyncio
    async def test_open_positions_count(self):
        db = _make_db(positions_count=5)
        result = await health(request=_make_request(db, _make_redis()))
        assert result["open_positions"] == 5

    @pytest.mark.asyncio
    async def test_last_seen_all_services(self):
        redis = _make_redis({"scanner": "ts1", "feeder": "ts2", "trader": "ts3"})
        result = await health(request=_make_request(_make_db(), redis))
        ls = result["last_seen"]
        assert ls["scanner"] == "ts1"
        assert ls["feeder"] == "ts2"
        assert ls["trader"] == "ts3"

    @pytest.mark.asyncio
    async def test_last_seen_none_when_service_absent(self):
        redis = _make_redis(last_seen_map={})  # no services seen
        result = await health(request=_make_request(_make_db(), redis))
        assert result["last_seen"]["scanner"] is None
        assert result["last_seen"]["trader"] is None

    @pytest.mark.asyncio
    async def test_queries_correct_services(self):
        redis = _make_redis()
        await health(request=_make_request(_make_db(), redis))
        called_services = [call.args[0] for call in redis.get_last_seen.call_args_list]
        assert "scanner" in called_services
        assert "feeder" in called_services
        assert "trader" in called_services


# ---------------------------------------------------------------------------
# /markets
# ---------------------------------------------------------------------------


class TestMarkets:
    @pytest.mark.asyncio
    async def test_returns_market_list(self):
        rows = [{"condition_id": "c1", "question": "Will X happen?", "trade_count": 2}]
        db = _make_db(market_rows=rows)
        result = await markets(request=_make_request(db), limit=200)
        assert result["markets"] == rows
        assert result["count"] == 1

    @pytest.mark.asyncio
    async def test_empty_markets_returns_zero_count(self):
        db = _make_db(market_rows=[])
        result = await markets(request=_make_request(db), limit=200)
        assert result["count"] == 0
        assert result["markets"] == []

    @pytest.mark.asyncio
    async def test_passes_limit_to_db(self):
        db = _make_db()
        await markets(request=_make_request(db), limit=42)
        db.get_active_markets.assert_called_once_with(limit=42)

    @pytest.mark.asyncio
    async def test_market_events_returns_events(self):
        rows = [{"event_type": "signal_match", "payload": {}}]
        db = _make_db(event_rows=rows)
        result = await market_events(
            condition_id="cond_abc",
            request=_make_request(db),
            limit=100,
            offset=0,
        )
        assert result["condition_id"] == "cond_abc"
        assert result["events"] == rows
        assert result["count"] == 1

    @pytest.mark.asyncio
    async def test_market_events_passes_pagination(self):
        db = _make_db()
        await market_events(
            condition_id="cond_xyz",
            request=_make_request(db),
            limit=25,
            offset=50,
        )
        db.get_market_events.assert_called_once_with("cond_xyz", limit=25, offset=50)


# ---------------------------------------------------------------------------
# /trades
# ---------------------------------------------------------------------------


class TestTrades:
    @pytest.mark.asyncio
    async def test_returns_trades_list(self):
        rows = [{"id": 1, "condition_id": "c1", "outcome": "WIN"}]
        db = _make_db(trade_rows=rows)
        result = await trades(
            request=_make_request(db),
            limit=100, offset=0,
            condition_id=None, outcome=None,
        )
        assert result["trades"] == rows
        assert result["count"] == 1

    @pytest.mark.asyncio
    async def test_passes_filters_to_db(self):
        db = _make_db()
        await trades(
            request=_make_request(db),
            limit=50, offset=10,
            condition_id="cond_abc",
            outcome="WIN",
        )
        db.get_recent_trades.assert_called_once_with(
            limit=50, offset=10,
            condition_id="cond_abc",
            outcome="WIN",
        )

    @pytest.mark.asyncio
    async def test_no_filters_passes_none(self):
        db = _make_db()
        await trades(
            request=_make_request(db),
            limit=100, offset=0,
            condition_id=None, outcome=None,
        )
        call_kwargs = db.get_recent_trades.call_args.kwargs
        assert call_kwargs["condition_id"] is None
        assert call_kwargs["outcome"] is None

    @pytest.mark.asyncio
    async def test_empty_result(self):
        db = _make_db(trade_rows=[])
        result = await trades(
            request=_make_request(db),
            limit=100, offset=0,
            condition_id=None, outcome=None,
        )
        assert result["count"] == 0


# ---------------------------------------------------------------------------
# /signals
# ---------------------------------------------------------------------------


class TestSignals:
    @pytest.mark.asyncio
    async def test_returns_signal_list(self):
        rows = [{"id": 1, "title": "Test headline", "match_count": 3}]
        db = _make_db(signal_rows=rows)
        result = await signals(request=_make_request(db), limit=100, offset=0)
        assert result["signals"] == rows
        assert result["count"] == 1

    @pytest.mark.asyncio
    async def test_passes_pagination(self):
        db = _make_db()
        await signals(request=_make_request(db), limit=25, offset=75)
        db.get_recent_signals.assert_called_once_with(limit=25, offset=75)

    @pytest.mark.asyncio
    async def test_empty_signals(self):
        db = _make_db(signal_rows=[])
        result = await signals(request=_make_request(db), limit=100, offset=0)
        assert result["count"] == 0


# ---------------------------------------------------------------------------
# /positions
# ---------------------------------------------------------------------------


class TestPositions:
    @pytest.mark.asyncio
    async def test_returns_positions(self):
        rows = [{"trade_id": 7, "condition_id": "c1", "size_usdc": 50.0}]
        db = _make_db()
        db.get_open_positions = AsyncMock(return_value=rows)
        result = await positions(request=_make_request(db))
        assert result["positions"] == rows
        assert result["count"] == 1

    @pytest.mark.asyncio
    async def test_empty_positions(self):
        db = _make_db()
        db.get_open_positions = AsyncMock(return_value=[])
        result = await positions(request=_make_request(db))
        assert result["count"] == 0


# ---------------------------------------------------------------------------
# /calibration
# ---------------------------------------------------------------------------


class TestCalibration:
    @pytest.mark.asyncio
    async def test_returns_brier_summary(self):
        rows = [{"signal_source": "newsapi", "mean_brier": 0.15, "sample_count": 42}]
        db = _make_db(brier_rows=rows)
        result = await calibration(request=_make_request(db))
        assert result["calibration"] == rows

    @pytest.mark.asyncio
    async def test_empty_calibration(self):
        db = _make_db(brier_rows=[])
        result = await calibration(request=_make_request(db))
        assert result["calibration"] == []


# ---------------------------------------------------------------------------
# /metrics
# ---------------------------------------------------------------------------


class TestMetrics:
    @pytest.mark.asyncio
    async def test_returns_available_profiles(self):
        profiles = {
            "scanner": {"gamma_fetch": {"mean_ms": 312.4}},
            "feeder": {"signal_embed": {"p95_ms": 45.0}},
        }
        redis = _make_redis(profiles=profiles)
        result = await metrics(request=_make_request(redis=redis))
        assert "scanner" in result["metrics"]
        assert "feeder" in result["metrics"]

    @pytest.mark.asyncio
    async def test_excludes_services_without_profile(self):
        # Only feeder has a profile
        redis = _make_redis(profiles={"feeder": {"op": {"mean_ms": 10.0}}})
        result = await metrics(request=_make_request(redis=redis))
        assert "scanner" not in result["metrics"]
        assert "trader" not in result["metrics"]
        assert "feeder" in result["metrics"]

    @pytest.mark.asyncio
    async def test_empty_when_no_profiles(self):
        redis = _make_redis(profiles={})
        result = await metrics(request=_make_request(redis=redis))
        assert result["metrics"] == {}

    @pytest.mark.asyncio
    async def test_queries_all_three_services(self):
        redis = _make_redis()
        await metrics(request=_make_request(redis=redis))
        called = [call.args[0] for call in redis.get_profile.call_args_list]
        assert "scanner" in called
        assert "feeder" in called
        assert "trader" in called
