"""
Unit tests for sources/watched.py — WatchedPageSource.

Playwright is mocked throughout so no browser is launched.
Tests verify:
  - DOM change detection → RawSignal extraction
  - In-session URL dedup
  - 304-style "no changes" path (ready=False)
  - Page reload triggered after silence window
  - Jitter is applied to reload interval
  - _clean_url helper
"""

from __future__ import annotations

import asyncio
import random
from datetime import timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sources.watched import WatchedPageSource, _clean_url


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_source(reload_seconds: int = 300, jitter_fraction: float = 0.0) -> WatchedPageSource:
    src = WatchedPageSource(
        url="https://example.com/news",
        reload_seconds=reload_seconds,
        jitter_fraction=jitter_fraction,
        change_wait_seconds=5,
        rng=random.Random(42),
    )
    # Inject a mock page so _ensure_page is not needed
    src._page = MagicMock()
    src._browser = MagicMock()
    src._playwright = MagicMock()
    return src


def _make_page_with_changes(items: list[dict]) -> MagicMock:
    """Return a mock page whose evaluate() behaves as if observer has changes."""
    page = MagicMock()

    async def _evaluate(js):
        # _DRAIN_JS is a function that returns/resets _ireneChanges — check it first
        # because _DRAIN_JS also mentions _ireneReady (to reset it)
        if "_ireneChanges" in js:
            return items
        if "window._ireneReady" in js:
            return True
        return None

    page.evaluate = AsyncMock(side_effect=_evaluate)
    page.is_closed = MagicMock(return_value=False)
    return page


def _make_page_silent() -> MagicMock:
    """Return a mock page whose observer never fires."""
    page = MagicMock()

    async def _evaluate(js):
        if "window._ireneReady" in js:
            return False
        return []

    page.evaluate = AsyncMock(side_effect=_evaluate)
    page.reload = AsyncMock()
    page.is_closed = MagicMock(return_value=False)
    return page


# ---------------------------------------------------------------------------
# _drain_changes
# ---------------------------------------------------------------------------

class TestDrainChanges:

    @pytest.mark.asyncio
    async def test_returns_signals_for_valid_links(self):
        src = _make_source()
        src._page = _make_page_with_changes([
            {"url": "https://example.com/story/1", "title": "Fed raises interest rates sharply", "description": "Details here"},
            {"url": "https://example.com/story/2", "title": "Bitcoin hits all time high record", "description": None},
        ])
        signals = await src._drain_changes()
        assert len(signals) == 2
        assert signals[0].url == "https://example.com/story/1"
        assert signals[1].title == "Bitcoin hits all time high record"

    @pytest.mark.asyncio
    async def test_skips_short_titles(self):
        src = _make_source()
        src._page = _make_page_with_changes([
            {"url": "https://example.com/x", "title": "Short", "description": None},
        ])
        signals = await src._drain_changes()
        assert signals == []

    @pytest.mark.asyncio
    async def test_skips_non_http_urls(self):
        src = _make_source()
        src._page = _make_page_with_changes([
            {"url": "javascript:void(0)", "title": "Click here to read the full article", "description": None},
            {"url": "mailto:news@example.com", "title": "Subscribe to our newsletter today", "description": None},
        ])
        signals = await src._drain_changes()
        assert signals == []

    @pytest.mark.asyncio
    async def test_in_session_dedup(self):
        """Same URL returned twice across two drains → only processed once."""
        items = [{"url": "https://example.com/story/1", "title": "Fed raises interest rates sharply", "description": None}]
        src = _make_source()
        src._page = _make_page_with_changes(items)

        first = await src._drain_changes()
        assert len(first) == 1

        # Second drain with same URL
        src._page = _make_page_with_changes(items)
        second = await src._drain_changes()
        assert second == []

    @pytest.mark.asyncio
    async def test_signal_source_detail_prefix(self):
        src = _make_source()
        src._page = _make_page_with_changes([
            {"url": "https://example.com/story/99", "title": "Congress passes major spending bill today", "description": None},
        ])
        signals = await src._drain_changes()
        assert signals[0].source_detail.startswith("watched:")

    @pytest.mark.asyncio
    async def test_evaluate_exception_returns_empty(self):
        src = _make_source()
        src._page = MagicMock()
        src._page.evaluate = AsyncMock(side_effect=Exception("page closed"))
        signals = await src._drain_changes()
        assert signals == []


# ---------------------------------------------------------------------------
# run() loop behaviour
# ---------------------------------------------------------------------------

class TestRunLoop:

    @pytest.mark.asyncio
    async def test_enqueues_signals_on_dom_change(self):
        src = _make_source(reload_seconds=600, jitter_fraction=0.0)
        src._page = _make_page_with_changes([
            {"url": "https://example.com/breaking-news-article-here", "title": "Breaking major political election result announced", "description": "Full story"},
        ])
        queue: asyncio.Queue = asyncio.Queue()

        # Cancel on the second sleep so we get exactly one loop iteration
        call_count = 0
        async def _fake_sleep(secs):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError()

        # Bypass HTTP reachability check and Playwright launch so we use our mock page
        with patch.object(src, "_check_reachable", AsyncMock(return_value=True)), \
             patch.object(src, "_ensure_page", AsyncMock()), \
             patch("asyncio.sleep", side_effect=_fake_sleep):
            try:
                await src.run(queue)
            except asyncio.CancelledError:
                pass

        assert not queue.empty()
        sig = queue.get_nowait()
        assert "example.com" in sig.url

    @pytest.mark.asyncio
    async def test_no_signals_when_no_changes(self):
        src = _make_source(reload_seconds=600, jitter_fraction=0.0)
        src._page = _make_page_silent()
        queue: asyncio.Queue = asyncio.Queue()

        call_count = 0
        async def _fake_sleep(secs):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError()

        # Bypass HTTP reachability check and Playwright launch so we use our mock page
        with patch.object(src, "_check_reachable", AsyncMock(return_value=True)), \
             patch.object(src, "_ensure_page", AsyncMock()), \
             patch("asyncio.sleep", side_effect=_fake_sleep):
            try:
                await src.run(queue)
            except asyncio.CancelledError:
                pass

        assert queue.empty()

    @pytest.mark.asyncio
    async def test_reload_triggered_after_silence_window(self):
        src = _make_source(reload_seconds=0, jitter_fraction=0.0)  # instant timeout
        src._page = _make_page_silent()
        src._page.reload = AsyncMock()

        queue: asyncio.Queue = asyncio.Queue()

        # Patch sleep so loop doesn't actually wait
        call_count = 0
        async def _fake_sleep(secs):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError()

        # Bypass HTTP reachability check and Playwright launch so we use our mock page
        with patch.object(src, "_check_reachable", AsyncMock(return_value=True)), \
             patch.object(src, "_ensure_page", AsyncMock()), \
             patch("asyncio.sleep", side_effect=_fake_sleep):
            try:
                await src.run(queue)
            except asyncio.CancelledError:
                pass

        src._page.reload.assert_called()


# ---------------------------------------------------------------------------
# Jitter
# ---------------------------------------------------------------------------

class TestJitter:

    def test_jitter_varies_reload_deadline(self):
        rng = random.Random(0)
        src = WatchedPageSource(
            url="https://example.com",
            reload_seconds=300,
            jitter_fraction=0.20,
            rng=rng,
        )
        deadlines = {src._jittered_reload() for _ in range(20)}
        # With 20% jitter on 300s the range is [240, 360]; values should vary
        assert len(deadlines) > 1
        assert all(240 <= d <= 360 for d in deadlines)

    def test_zero_jitter_is_exact(self):
        src = WatchedPageSource(
            url="https://example.com",
            reload_seconds=300,
            jitter_fraction=0.0,
        )
        assert src._jittered_reload() == 300.0


# ---------------------------------------------------------------------------
# _clean_url
# ---------------------------------------------------------------------------

class TestCleanUrl:

    def test_strips_fragment(self):
        assert _clean_url("https://example.com/story#comments") == "https://example.com/story"

    def test_strips_utm_params(self):
        result = _clean_url("https://example.com/story?utm_source=twitter&utm_medium=social")
        assert "utm_source" not in result
        assert "utm_medium" not in result

    def test_preserves_non_tracking_params(self):
        url = "https://example.com/search?q=election&page=2"
        result = _clean_url(url)
        assert "q=election" in result

    def test_strips_ref_param(self):
        result = _clean_url("https://example.com/story?ref=homepage")
        assert "ref=" not in result

    def test_clean_url_unchanged(self):
        url = "https://example.com/story/breaking-news"
        assert _clean_url(url) == url
