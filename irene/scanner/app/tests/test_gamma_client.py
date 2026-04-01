"""Unit tests for gamma_client — HTTP mocking, pagination, field extraction."""

from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gamma_client import GammaClient, parse_market, _parse_dt, _safe_float


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_raw_market(**overrides) -> dict:
    """Return a minimal valid raw Gamma API market dict."""
    base = {
        "conditionId": "cond_abc",
        "slug": "will-x-happen",
        "question": "Will X happen by 2026-01-01?",
        "description": "Resolves YES if X happens.",
        "endDate": "2026-01-01T12:00:00Z",
        "volumeNum": 10000.0,
        "category": "Politics",
        "tokens": [
            {"token_id": "tok_yes_1"},
            {"token_id": "tok_no_1"},
        ],
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# parse_market
# ---------------------------------------------------------------------------


class TestParseMarket:
    def test_valid_market(self):
        raw = _make_raw_market()
        result = parse_market(raw)
        assert result is not None
        assert result["condition_id"] == "cond_abc"
        assert result["question"] == "Will X happen by 2026-01-01?"
        assert result["token_yes"] == "tok_yes_1"
        assert result["token_no"] == "tok_no_1"
        assert result["volume_usdc"] == 10000.0
        assert result["category"] == "Politics"

    def test_missing_condition_id_returns_none(self):
        raw = _make_raw_market()
        raw.pop("conditionId")
        assert parse_market(raw) is None

    def test_missing_question_returns_none(self):
        raw = _make_raw_market(question="")
        assert parse_market(raw) is None

    def test_missing_end_date_returns_none(self):
        raw = _make_raw_market()
        raw.pop("endDate")
        assert parse_market(raw) is None

    def test_invalid_end_date_returns_none(self):
        raw = _make_raw_market(endDate="not-a-date")
        assert parse_market(raw) is None

    def test_no_tokens_gives_none_token_fields(self):
        raw = _make_raw_market(tokens=[])
        result = parse_market(raw)
        assert result is not None
        assert result["token_yes"] is None
        assert result["token_no"] is None

    def test_camel_case_condition_id(self):
        raw = _make_raw_market()
        raw.pop("conditionId")
        raw["condition_id"] = "snake_case_id"
        result = parse_market(raw)
        assert result["condition_id"] == "snake_case_id"

    def test_volume_fallback_fields(self):
        raw = _make_raw_market()
        raw.pop("volumeNum")
        raw["usdcLiquidity"] = "5000"
        result = parse_market(raw)
        assert result["volume_usdc"] == 5000.0

    def test_end_date_is_utc_aware(self):
        raw = _make_raw_market(endDate="2026-06-01T00:00:00Z")
        result = parse_market(raw)
        assert result["end_date"].tzinfo is not None


# ---------------------------------------------------------------------------
# _parse_dt
# ---------------------------------------------------------------------------


class TestParseDt:
    def test_z_suffix(self):
        dt = _parse_dt("2026-01-01T12:00:00Z")
        assert dt is not None
        assert dt.tzinfo is not None

    def test_offset_suffix(self):
        dt = _parse_dt("2026-01-01T12:00:00+00:00")
        assert dt is not None

    def test_invalid_returns_none(self):
        assert _parse_dt("not-a-date") is None
        assert _parse_dt("") is None


# ---------------------------------------------------------------------------
# _safe_float
# ---------------------------------------------------------------------------


class TestSafeFloat:
    def test_numeric_string(self):
        assert _safe_float("1234.5") == pytest.approx(1234.5)

    def test_int(self):
        assert _safe_float(100) == pytest.approx(100.0)

    def test_none_returns_none(self):
        assert _safe_float(None) is None

    def test_invalid_string_returns_none(self):
        assert _safe_float("abc") is None


# ---------------------------------------------------------------------------
# GammaClient — HTTP mocking
# ---------------------------------------------------------------------------


class TestGammaClient:
    def _make_response(self, data: list) -> MagicMock:
        """Return a mock aiohttp response."""
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json = AsyncMock(return_value=data)
        resp.__aenter__ = AsyncMock(return_value=resp)
        resp.__aexit__ = AsyncMock(return_value=False)
        return resp

    @pytest.mark.asyncio
    async def test_fetch_single_page(self):
        market = _make_raw_market()
        session = MagicMock()
        session.get = MagicMock(return_value=self._make_response([market]))
        session.close = AsyncMock()

        async with GammaClient(session=session) as client:
            results = await client.fetch_open_markets(page_size=200)

        assert len(results) == 1
        assert results[0]["conditionId"] == "cond_abc"

    @pytest.mark.asyncio
    async def test_empty_response_stops_pagination(self):
        session = MagicMock()
        session.get = MagicMock(return_value=self._make_response([]))
        session.close = AsyncMock()

        async with GammaClient(session=session) as client:
            results = await client.fetch_open_markets(page_size=10)

        assert results == []

    @pytest.mark.asyncio
    async def test_http_error_returns_empty(self):
        from aiohttp import ClientResponseError

        resp = MagicMock()
        resp.raise_for_status = MagicMock(
            side_effect=ClientResponseError(None, None, status=503)
        )
        resp.__aenter__ = AsyncMock(return_value=resp)
        resp.__aexit__ = AsyncMock(return_value=False)

        session = MagicMock()
        session.get = MagicMock(return_value=resp)
        session.close = AsyncMock()

        async with GammaClient(session=session) as client:
            results = await client.fetch_open_markets()

        assert results == []

    @pytest.mark.asyncio
    async def test_pagination_stops_when_batch_smaller_than_page_size(self):
        """When batch < page_size, we're on the last page — stop paginating."""
        page1 = [_make_raw_market(conditionId=f"cond_{i}") for i in range(3)]
        # page_size=5 but only 3 returned → last page
        call_count = 0

        resp_mock = MagicMock()
        resp_mock.raise_for_status = MagicMock()
        resp_mock.__aenter__ = AsyncMock(return_value=resp_mock)
        resp_mock.__aexit__ = AsyncMock(return_value=False)

        async def _json():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return page1
            return []  # should not be reached

        resp_mock.json = _json

        session = MagicMock()
        session.get = MagicMock(return_value=resp_mock)
        session.close = AsyncMock()

        async with GammaClient(session=session) as client:
            results = await client.fetch_open_markets(page_size=5)

        assert len(results) == 3
        assert call_count == 1
