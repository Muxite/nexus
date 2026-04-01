"""Unit tests for sources/rss.py — mock HTTP, feedparser parsing."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from sources.rss import RSSSource, _parse_entry_date, _strip_html
from irene_shared.models import SignalSource


# ---------------------------------------------------------------------------
# Sample RSS XML
# ---------------------------------------------------------------------------

_SAMPLE_RSS = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Test Feed</title>
    <link>https://example.com</link>
    <item>
      <title>Fed raises rates by 25 basis points</title>
      <link>https://example.com/article/1</link>
      <description>The Fed announced a quarter-point rate increase.</description>
      <pubDate>Tue, 01 Jan 2026 12:00:00 +0000</pubDate>
    </item>
    <item>
      <title>Bitcoin surpasses $100k milestone</title>
      <link>https://example.com/article/2</link>
      <description>BTC price hit an all-time high.</description>
      <pubDate>Tue, 01 Jan 2026 11:00:00 +0000</pubDate>
    </item>
  </channel>
</rss>"""

_EMPTY_RSS = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><title>Empty</title></channel></rss>"""


def _mock_session(xml: bytes = _SAMPLE_RSS, status: int = 200) -> MagicMock:
    resp = MagicMock()
    if status != 200:
        from aiohttp import ClientResponseError
        resp.raise_for_status = MagicMock(
            side_effect=ClientResponseError(None, None, status=status)
        )
    else:
        resp.raise_for_status = MagicMock()
    resp.read = AsyncMock(return_value=xml)
    resp.__aenter__ = AsyncMock(return_value=resp)
    resp.__aexit__ = AsyncMock(return_value=False)
    session = MagicMock()
    session.get = MagicMock(return_value=resp)
    return session


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestRSSSource:
    @pytest.mark.asyncio
    async def test_parses_articles(self):
        session = _mock_session()
        source = RSSSource("https://example.com/feed", session)
        signals = await source.fetch()
        assert len(signals) == 2
        assert signals[0].source == SignalSource.RSS
        assert signals[0].title == "Fed raises rates by 25 basis points"

    @pytest.mark.asyncio
    async def test_source_detail_is_url(self):
        session = _mock_session()
        url = "https://feeds.reuters.com/reuters/topNews"
        source = RSSSource(url, session)
        signals = await source.fetch()
        assert all(s.source_detail == url for s in signals)

    @pytest.mark.asyncio
    async def test_since_filter(self):
        session = _mock_session()
        source = RSSSource("https://example.com/feed", session)
        # since after both articles → empty result
        since = datetime(2026, 1, 2, tzinfo=timezone.utc)
        signals = await source.fetch(since=since)
        assert signals == []

    @pytest.mark.asyncio
    async def test_since_filter_passes_recent(self):
        session = _mock_session()
        source = RSSSource("https://example.com/feed", session)
        # since before both articles → both pass
        since = datetime(2025, 12, 31, tzinfo=timezone.utc)
        signals = await source.fetch(since=since)
        assert len(signals) == 2

    @pytest.mark.asyncio
    async def test_empty_feed_returns_empty(self):
        session = _mock_session(_EMPTY_RSS)
        source = RSSSource("https://example.com/feed", session)
        signals = await source.fetch()
        assert signals == []

    @pytest.mark.asyncio
    async def test_http_error_returns_empty(self):
        session = _mock_session(status=503)
        source = RSSSource("https://example.com/feed", session)
        signals = await source.fetch()
        assert signals == []

    @pytest.mark.asyncio
    async def test_network_exception_returns_empty(self):
        resp = MagicMock()
        resp.__aenter__ = AsyncMock(side_effect=Exception("Connection refused"))
        resp.__aexit__ = AsyncMock(return_value=False)
        session = MagicMock()
        session.get = MagicMock(return_value=resp)
        source = RSSSource("https://example.com/feed", session)
        signals = await source.fetch()
        assert signals == []

    @pytest.mark.asyncio
    async def test_description_populated(self):
        session = _mock_session()
        source = RSSSource("https://example.com/feed", session)
        signals = await source.fetch()
        assert signals[0].description is not None
        assert len(signals[0].description) > 0


class TestStripHtml:
    def test_strips_tags(self):
        result = _strip_html("<p>Hello <b>world</b></p>")
        assert result == "Hello  world"

    def test_none_returns_none(self):
        assert _strip_html(None) is None

    def test_empty_returns_none(self):
        assert _strip_html("") is None

    def test_plain_text_unchanged(self):
        assert _strip_html("plain text") == "plain text"
