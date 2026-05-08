"""Unit tests for sources/newsapi.py — HTTP mocking, dedup, field extraction."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from sources.newsapi import NewsAPISource, _parse_published
from irene_shared.models import SignalSource


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_article(**overrides) -> dict:
    base = {
        "url": "https://example.com/article/1",
        "title": "Fed raises rates by 25 basis points",
        "description": "The Federal Reserve announced a quarter-point rate increase.",
        "publishedAt": "2026-01-01T12:00:00Z",
        "content": None,
    }
    base.update(overrides)
    return base


def _mock_session(articles: list) -> MagicMock:
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json = AsyncMock(return_value={"status": "ok", "articles": articles})
    resp.__aenter__ = AsyncMock(return_value=resp)
    resp.__aexit__ = AsyncMock(return_value=False)
    session = MagicMock()
    session.get = MagicMock(return_value=resp)
    return session


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestNewsAPISource:
    @pytest.mark.asyncio
    async def test_returns_signals_from_articles(self):
        session = _mock_session([_make_article()])
        source = NewsAPISource(api_key="key123", session=session)
        signals = await source.fetch()
        assert len(signals) == 1
        assert signals[0].source == SignalSource.NEWSAPI
        assert signals[0].title == "Fed raises rates by 25 basis points"
        assert signals[0].url == "https://example.com/article/1"

    @pytest.mark.asyncio
    async def test_skips_removed_articles(self):
        session = _mock_session([_make_article(title="[Removed]")])
        source = NewsAPISource(api_key="key", session=session)
        signals = await source.fetch()
        assert signals == []

    @pytest.mark.asyncio
    async def test_skips_articles_with_no_url(self):
        session = _mock_session([_make_article(url="")])
        source = NewsAPISource(api_key="key", session=session)
        signals = await source.fetch()
        assert signals == []

    @pytest.mark.asyncio
    async def test_skips_articles_with_no_title(self):
        session = _mock_session([_make_article(title="")])
        source = NewsAPISource(api_key="key", session=session)
        signals = await source.fetch()
        assert signals == []

    @pytest.mark.asyncio
    async def test_empty_api_key_returns_empty(self):
        session = _mock_session([_make_article()])
        source = NewsAPISource(api_key="", session=session)
        signals = await source.fetch()
        assert signals == []
        session.get.assert_not_called()

    @pytest.mark.asyncio
    async def test_published_at_parsed(self):
        session = _mock_session([_make_article(publishedAt="2026-06-01T09:30:00Z")])
        source = NewsAPISource(api_key="key", session=session)
        signals = await source.fetch()
        assert signals[0].published_at is not None
        assert signals[0].published_at.year == 2026

    @pytest.mark.asyncio
    async def test_description_falls_back_to_content(self):
        article = _make_article(description=None, content="Full article content here.")
        session = _mock_session([article])
        source = NewsAPISource(api_key="key", session=session)
        signals = await source.fetch()
        assert signals[0].description == "Full article content here."

    @pytest.mark.asyncio
    async def test_http_error_returns_empty(self):
        from aiohttp import ClientResponseError

        resp = MagicMock()
        resp.raise_for_status = MagicMock(
            side_effect=ClientResponseError(None, None, status=429)
        )
        resp.__aenter__ = AsyncMock(return_value=resp)
        resp.__aexit__ = AsyncMock(return_value=False)
        session = MagicMock()
        session.get = MagicMock(return_value=resp)

        source = NewsAPISource(api_key="key", session=session)
        signals = await source.fetch()
        assert signals == []

    @pytest.mark.asyncio
    async def test_multiple_articles(self):
        articles = [
            _make_article(url=f"https://example.com/{i}", title=f"Article {i}")
            for i in range(5)
        ]
        session = _mock_session(articles)
        source = NewsAPISource(api_key="key", session=session)
        signals = await source.fetch()
        assert len(signals) == 5

    @pytest.mark.asyncio
    async def test_source_detail_set(self):
        session = _mock_session([_make_article()])
        source = NewsAPISource(api_key="key", session=session, source_detail="newsapi")
        signals = await source.fetch()
        assert signals[0].source_detail == "newsapi"


class TestParsePublished:
    def test_z_suffix(self):
        dt = _parse_published("2026-01-01T12:00:00Z")
        assert dt is not None
        assert dt.tzinfo is not None
        assert dt.year == 2026

    def test_none_input(self):
        assert _parse_published(None) is None

    def test_empty_string(self):
        assert _parse_published("") is None

    def test_invalid_string(self):
        assert _parse_published("not-a-date") is None
