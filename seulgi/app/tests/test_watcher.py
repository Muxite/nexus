"""
Tests for seulgi/app/watcher.py (scraper-backed polling)

Covers:
- _clean_url: tracking param stripping, fragment removal
- _clean_title / _is_junk helpers
- RawHeadline dataclass
- SiteWatcher._fetch_headlines: link filtering, dedup, scraper error handling
- SiteWatcher.run: enqueue on new headlines, failure counting, cancellation
"""

import asyncio
import sys
import os
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from watcher import SiteWatcher, RawHeadline, _clean_url, _clean_title, _is_junk


# ---------------------------------------------------------------------------
# _clean_url
# ---------------------------------------------------------------------------


class TestCleanUrl:
    def test_strips_utm_params(self):
        url = "https://example.com/story?utm_source=twitter&utm_medium=social&id=123"
        result = _clean_url(url)
        assert "utm_source" not in result
        assert "utm_medium" not in result
        assert "id=123" in result

    def test_strips_fragment(self):
        url = "https://example.com/story#section1"
        assert _clean_url(url) == "https://example.com/story"

    def test_strips_ref_param(self):
        url = "https://example.com/story?ref=homepage&p=1"
        result = _clean_url(url)
        assert "ref=" not in result
        assert "p=1" in result

    def test_no_params(self):
        url = "https://example.com/story"
        assert _clean_url(url) == url

    def test_preserves_non_tracking_params(self):
        url = "https://example.com/search?q=news&page=2"
        result = _clean_url(url)
        assert "q=news" in result
        assert "page=2" in result

    def test_fragment_and_utm(self):
        url = "https://example.com/story?utm_campaign=foo#top"
        result = _clean_url(url)
        assert "#" not in result
        assert "utm_campaign" not in result


# ---------------------------------------------------------------------------
# _clean_title / _is_junk
# ---------------------------------------------------------------------------


class TestHelpers:
    def test_clean_title_collapses_whitespace(self):
        assert _clean_title("  Breaking  news  today  ") == "Breaking news today"

    def test_clean_title_removes_comment_count(self):
        title = "Major earthquake strikes city 1.2k comments"
        result = _clean_title(title)
        assert "1.2k" not in result
        assert "Major earthquake" in result

    def test_is_junk_sign_in(self):
        assert _is_junk("Sign in to continue reading")

    def test_is_junk_subscribe(self):
        assert _is_junk("Subscribe to our newsletter today")

    def test_not_junk(self):
        assert not _is_junk("Fed raises interest rates amid inflation concerns")


# ---------------------------------------------------------------------------
# RawHeadline
# ---------------------------------------------------------------------------


class TestRawHeadline:
    def test_fields(self):
        now = datetime.now(timezone.utc)
        h = RawHeadline(
            site_url="https://apnews.com",
            headline="Breaking: major earthquake strikes city center today",
            article_url="https://apnews.com/article/earthquake-123",
            detected_at=now,
        )
        assert h.site_url == "https://apnews.com"
        assert h.detected_at == now


# ---------------------------------------------------------------------------
# Helpers to mock aiohttp session responses
# ---------------------------------------------------------------------------


def _make_scraper_response(links: list[dict], status: int = 200):
    """Return a mock aiohttp response whose .json() returns a scraper payload."""
    resp = MagicMock()
    resp.status = status
    resp.json = AsyncMock(return_value={
        "extraction": {"links": links}
    })
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=resp)
    cm.__aexit__ = AsyncMock(return_value=False)
    return cm


def _make_watcher(**kwargs):
    w = SiteWatcher(
        url="https://example.com",
        change_wait_seconds=1,
        max_failures=3,
        scraper_url="http://fake-scraper:9000",
        **kwargs,
    )
    w._session = MagicMock()
    return w


# ---------------------------------------------------------------------------
# SiteWatcher._fetch_headlines
# ---------------------------------------------------------------------------


class TestFetchHeadlines:
    @pytest.mark.asyncio
    async def test_returns_headlines_for_valid_links(self):
        w = _make_watcher()
        links = [{"url": "https://example.com/article-one", "text": "This is a valid headline about something important"}]
        w._session.post.return_value = _make_scraper_response(links)

        results = await w._fetch_headlines()
        assert len(results) == 1
        assert results[0].headline == "This is a valid headline about something important"

    @pytest.mark.asyncio
    async def test_deduplicates_urls(self):
        w = _make_watcher()
        links = [
            {"url": "https://example.com/article-one", "text": "This is a valid headline about something important"},
            {"url": "https://example.com/article-one", "text": "This is a valid headline about something important"},
        ]
        w._session.post.return_value = _make_scraper_response(links)
        results = await w._fetch_headlines()
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_filters_short_titles(self):
        w = _make_watcher()
        links = [{"url": "https://example.com/article", "text": "Short"}]
        w._session.post.return_value = _make_scraper_response(links)
        results = await w._fetch_headlines()
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_filters_non_http_urls(self):
        w = _make_watcher()
        links = [{"url": "javascript:void(0)", "text": "This is a valid looking title of enough length"}]
        w._session.post.return_value = _make_scraper_response(links)
        results = await w._fetch_headlines()
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_filters_nav_junk(self):
        w = _make_watcher()
        links = [{"url": "https://example.com/subscribe", "text": "Subscribe to our newsletter today for free access"}]
        w._session.post.return_value = _make_scraper_response(links)
        results = await w._fetch_headlines()
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_strips_tracking_from_url(self):
        w = _make_watcher()
        links = [{
            "url": "https://example.com/article?utm_source=twitter&id=42",
            "text": "This is a valid headline with enough characters for filter",
        }]
        w._session.post.return_value = _make_scraper_response(links)
        results = await w._fetch_headlines()
        assert len(results) == 1
        assert "utm_source" not in results[0].article_url
        assert "id=42" in results[0].article_url

    @pytest.mark.asyncio
    async def test_cross_call_dedup(self):
        """Second call should not re-yield the same URL."""
        w = _make_watcher()
        links = [{"url": "https://example.com/article-once", "text": "This headline should appear only once here"}]
        w._session.post.return_value = _make_scraper_response(links)
        first = await w._fetch_headlines()
        w._session.post.return_value = _make_scraper_response(links)
        second = await w._fetch_headlines()
        assert len(first) == 1
        assert len(second) == 0

    @pytest.mark.asyncio
    async def test_raises_on_500(self):
        from watcher import _FetchError
        w = _make_watcher()
        w._session.post.return_value = _make_scraper_response([], status=500)
        with pytest.raises(_FetchError):
            await w._fetch_headlines()

    @pytest.mark.asyncio
    async def test_raises_on_connection_error(self):
        import aiohttp
        from watcher import _FetchError
        w = _make_watcher()
        w._session.post.side_effect = aiohttp.ClientConnectorError(
            MagicMock(), OSError("connection refused")
        )
        with pytest.raises(_FetchError):
            await w._fetch_headlines()

    @pytest.mark.asyncio
    async def test_empty_links_returns_empty(self):
        w = _make_watcher()
        w._session.post.return_value = _make_scraper_response([])
        results = await w._fetch_headlines()
        assert results == []


# ---------------------------------------------------------------------------
# SiteWatcher.run loop
# ---------------------------------------------------------------------------


class TestRunLoop:
    @pytest.mark.asyncio
    async def test_enqueues_headlines_on_new_links(self):
        w = SiteWatcher(url="https://example.com", change_wait_seconds=1, max_failures=3, scraper_url="http://fake:9000")
        queue = asyncio.Queue()

        links = [{"url": "https://example.com/breaking-news-article", "text": "Breaking: major event shakes the world today everywhere"}]
        call_count = 0
        mock_session = MagicMock()
        mock_session.post.return_value = _make_scraper_response(links)
        mock_session.close = AsyncMock()

        async def fake_sleep(secs):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError()

        with patch("watcher.aiohttp.ClientSession", return_value=mock_session), \
             patch("asyncio.sleep", side_effect=fake_sleep):
            try:
                await w.run(queue)
            except asyncio.CancelledError:
                pass

        assert not queue.empty()
        h = queue.get_nowait()
        assert "Breaking" in h.headline

    @pytest.mark.asyncio
    async def test_no_headlines_when_no_links(self):
        w = SiteWatcher(url="https://example.com", change_wait_seconds=1, max_failures=3, scraper_url="http://fake:9000")
        queue = asyncio.Queue()
        call_count = 0
        mock_session = MagicMock()
        mock_session.post.return_value = _make_scraper_response([])
        mock_session.close = AsyncMock()

        async def fake_sleep(secs):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError()

        with patch("watcher.aiohttp.ClientSession", return_value=mock_session), \
             patch("asyncio.sleep", side_effect=fake_sleep):
            try:
                await w.run(queue)
            except asyncio.CancelledError:
                pass

        assert queue.empty()

    @pytest.mark.asyncio
    async def test_marks_unreachable_after_max_failures(self):
        import aiohttp
        w = SiteWatcher(url="https://example.com", max_failures=2, scraper_url="http://fake:9000")
        queue = asyncio.Queue()
        call_count = 0
        mock_session = MagicMock()
        mock_session.post.side_effect = aiohttp.ClientConnectorError(
            MagicMock(), OSError("refused")
        )
        mock_session.close = AsyncMock()

        async def fake_sleep(secs):
            nonlocal call_count
            call_count += 1
            if call_count > 12:
                raise asyncio.CancelledError()

        with patch("watcher.aiohttp.ClientSession", return_value=mock_session), \
             patch("asyncio.sleep", side_effect=fake_sleep):
            try:
                await w.run(queue)
            except asyncio.CancelledError:
                pass

        assert w.is_unreachable

    @pytest.mark.asyncio
    async def test_status_reflects_watcher_state(self):
        w = _make_watcher()
        s = w.status
        assert s["url"] == "https://example.com"
        assert s["reachable"] is True
        assert s["failures"] == 0
        assert s["total_headlines"] == 0
        assert s["is_unreachable"] is False


# ---------------------------------------------------------------------------
# RawHeadline.headline_id
# ---------------------------------------------------------------------------


def _make_raw_headline(**kwargs) -> RawHeadline:
    from datetime import datetime, timezone
    defaults = dict(
        site_url="https://apnews.com",
        headline="Test headline",
        article_url="https://apnews.com/article/1",
        detected_at=datetime.now(timezone.utc),
    )
    defaults.update(kwargs)
    return RawHeadline(**defaults)


class TestHeadlineId:
    def test_headline_id_is_12_char_hex(self):
        h = _make_raw_headline(headline="Fed cuts rates by 25bp")
        assert isinstance(h.headline_id, str)
        assert len(h.headline_id) == 12
        # Must be valid hex
        int(h.headline_id, 16)

    def test_two_headlines_have_different_ids(self):
        h1 = _make_raw_headline(headline="First headline", article_url="https://apnews.com/1")
        h2 = _make_raw_headline(headline="Second headline", article_url="https://apnews.com/2")
        assert h1.headline_id != h2.headline_id

    def test_headline_id_stable_after_creation(self):
        h = _make_raw_headline(headline="Stable headline")
        first = h.headline_id
        assert h.headline_id == first
