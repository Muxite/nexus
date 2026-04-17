"""Unit tests for feeder/app/sources/brave.py — BraveNewsSource."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sources.brave import BraveNewsSource, _parse_age


# ---------------------------------------------------------------------------
# _parse_age
# ---------------------------------------------------------------------------


class TestParseAge:

    def test_relative_hours(self):
        now = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = _parse_age("2 hours ago", now)
        expected = now - timedelta(hours=2)
        assert result == expected

    def test_relative_minutes(self):
        now = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = _parse_age("30 minutes ago", now)
        expected = now - timedelta(minutes=30)
        assert result == expected

    def test_relative_days(self):
        now = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = _parse_age("1 day ago", now)
        expected = now - timedelta(days=1)
        assert result == expected

    def test_empty_string_returns_none(self):
        now = datetime.now(timezone.utc)
        assert _parse_age("", now) is None

    def test_iso_string(self):
        now = datetime.now(timezone.utc)
        result = _parse_age("2026-03-25T10:00:00Z", now)
        assert result is not None
        assert result.year == 2026
        assert result.month == 3


# ---------------------------------------------------------------------------
# BraveNewsSource.fetch
# ---------------------------------------------------------------------------


def _make_session(status: int = 200, body: dict | None = None) -> MagicMock:
    """Return a mock aiohttp.ClientSession with a GET that returns body."""
    if body is None:
        body = {
            "results": [
                {
                    "url": "https://reuters.com/article/1",
                    "title": "Fed raises rates to 5.5%",
                    "description": "Federal Reserve raises benchmark rate",
                    "age": "1 hour ago",
                },
                {
                    "url": "https://reuters.com/article/2",
                    "title": "Bitcoin hits new high",
                    "description": "Crypto markets surge",
                    "age": "2 hours ago",
                },
            ]
        }

    mock_resp = AsyncMock()
    mock_resp.status = status
    mock_resp.json = AsyncMock(return_value=body)
    mock_resp.raise_for_status = MagicMock()
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock(return_value=False)

    session = MagicMock()
    session.get = MagicMock(return_value=mock_resp)
    return session


class TestBraveNewsSourceFetch:

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_key(self):
        source = BraveNewsSource(api_key="", session=MagicMock())
        result = await source.fetch()
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_signals_for_each_result(self):
        session = _make_session()
        source = BraveNewsSource(api_key="test_key", session=session)
        signals = await source.fetch()
        assert len(signals) == 2

    @pytest.mark.asyncio
    async def test_signal_url_and_title(self):
        session = _make_session()
        source = BraveNewsSource(api_key="test_key", session=session)
        signals = await source.fetch()
        assert signals[0].url == "https://reuters.com/article/1"
        assert "Fed raises rates" in signals[0].title

    @pytest.mark.asyncio
    async def test_signal_description_populated(self):
        session = _make_session()
        source = BraveNewsSource(api_key="test_key", session=session)
        signals = await source.fetch()
        assert signals[0].description == "Federal Reserve raises benchmark rate"

    @pytest.mark.asyncio
    async def test_since_filter_excludes_old_articles(self):
        now = datetime.now(timezone.utc)
        # Both articles are "N hours ago" → published before `since = 30 min ago`
        session = _make_session()
        source = BraveNewsSource(api_key="test_key", session=session)
        since = now - timedelta(minutes=30)
        signals = await source.fetch(since=since)
        assert len(signals) == 0  # both are >30 min old

    @pytest.mark.asyncio
    async def test_since_filter_passes_recent_articles(self):
        now = datetime.now(timezone.utc)
        body = {
            "results": [
                {
                    "url": "https://reuters.com/article/new",
                    "title": "Breaking news",
                    "description": "Just happened",
                    "age": "5 minutes ago",
                }
            ]
        }
        session = _make_session(body=body)
        source = BraveNewsSource(api_key="test_key", session=session)
        since = now - timedelta(hours=1)
        signals = await source.fetch(since=since)
        assert len(signals) == 1

    @pytest.mark.asyncio
    async def test_skips_results_with_no_url(self):
        body = {
            "results": [
                {"url": "", "title": "No URL article", "description": "test", "age": ""},
                {"url": "https://example.com/article", "title": "Has URL", "description": "ok", "age": ""},
            ]
        }
        session = _make_session(body=body)
        source = BraveNewsSource(api_key="test_key", session=session)
        signals = await source.fetch()
        assert len(signals) == 1
        assert signals[0].url == "https://example.com/article"

    @pytest.mark.asyncio
    async def test_returns_empty_on_http_error(self):
        import aiohttp as _aiohttp

        mock_resp = AsyncMock()
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)
        mock_resp.raise_for_status = MagicMock(
            side_effect=_aiohttp.ClientResponseError(
                request_info=MagicMock(), history=(), status=429
            )
        )

        session = MagicMock()
        session.get = MagicMock(return_value=mock_resp)

        source = BraveNewsSource(api_key="test_key", session=session)
        result = await source.fetch()
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_on_network_error(self):
        import aiohttp as _aiohttp

        mock_resp = AsyncMock()
        mock_resp.__aenter__ = AsyncMock(
            side_effect=_aiohttp.ClientConnectorError(connection_key=MagicMock(), os_error=OSError())
        )
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        session = MagicMock()
        session.get = MagicMock(return_value=mock_resp)

        source = BraveNewsSource(api_key="test_key", session=session)
        result = await source.fetch()
        assert result == []

    @pytest.mark.asyncio
    async def test_rotates_queries_across_calls(self):
        session = _make_session(body={"results": []})
        source = BraveNewsSource(api_key="test_key", session=session)

        await source.fetch()
        await source.fetch()
        await source.fetch()

        assert source._query_index == 3

    @pytest.mark.asyncio
    async def test_source_detail_is_brave_news(self):
        session = _make_session()
        source = BraveNewsSource(api_key="test_key", session=session)
        signals = await source.fetch()
        assert all(s.source_detail == "brave_news" for s in signals)
