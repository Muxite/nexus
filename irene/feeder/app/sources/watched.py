"""
Watched-page signal source using a persistent Playwright browser tab.

Instead of polling a URL repeatedly, this source:
  1. Performs a quick HTTP reachability check before launching the browser.
  2. Opens a headless Chromium tab and navigates to the target page once.
  3. Injects a MutationObserver that accumulates newly added DOM nodes.
  4. Waits — with jitter — for the observer to signal changes.
  5. Extracts content from changed nodes and yields it as RawSignals.
  6. If nothing changes within `reload_seconds` (±jitter), reloads the page
     and re-injects the observer to avoid stale content.

Two extraction modes:
  - Link mode (default): extracts <a href> links from DOM mutations.
    Best for news sites (AP, Reuters, BBC, etc.)
  - Post mode (post_mode=True): extracts text content from article/status
    elements. Best for social media (Truth Social, etc.)

Health tracking:
  - Tracks consecutive failures (navigate / evaluate errors).
  - After `max_failures` consecutive failures, marks the source as
    unreachable so the manager loop can apply a cooldown.
  - Reachability resets on the next successful interaction.

One WatchedPageSource instance per URL. Call run() to start the watch loop;
it is designed to run as a long-lived asyncio task via asyncio.create_task().
New signals are enqueued on the provided asyncio.Queue for the feeder to drain.
"""

from __future__ import annotations

import asyncio
import logging
import random
import re
from datetime import datetime, timezone
from typing import Optional

from irene_shared.models import RawSignal, SignalSource

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Observer JS — link mode (news sites)
# ---------------------------------------------------------------------------

_OBSERVER_JS = r"""
(function() {
    if (window._ireneObserverActive) return;
    window._ireneObserverActive = true;
    window._ireneReady = false;
    window._ireneChanges = [];

    var _NAV_BLOCKLIST = [
        'privacy', 'cookie', 'terms', 'accessibility', 'press freedom',
        'advertise', 'subscribe', 'sign in', 'log in', 'register',
        'contact us', 'about us', 'careers', 'sitemap', 'rss feed',
        'newsletter', 'download app', 'skip to', 'close menu', 'back to top'
    ];

    function isNavJunk(title) {
        var t = title.toLowerCase();
        for (var i = 0; i < _NAV_BLOCKLIST.length; i++) {
            if (t.indexOf(_NAV_BLOCKLIST[i]) !== -1) return true;
        }
        return false;
    }

    function extractLinks(node) {
        if (node.nodeType !== 1) return;
        var anchors = [];
        if (node.tagName === 'A' && node.href) {
            anchors.push(node);
        }
        anchors = anchors.concat(Array.from(node.querySelectorAll('a[href]')));
        anchors.forEach(function(a) {
            var url = a.href || '';
            var title = (a.innerText || a.textContent || '').trim();
            // Strip trailing comment counts and metadata (e.g. "... 1.28k")
            title = title.replace(/\s+[\d.,]+[kKmM]?\s*$/, '').trim();
            // Strip "A trending article titled ..." prefix from AP News
            title = title.replace(/^A trending article titled [""](.+)[""].*$/, '$1').trim();
            var desc = '';
            var parent = a.closest('article, li, div[class*="story"], div[class*="item"], div[class*="card"]');
            if (parent) {
                var p = parent.querySelector('p, [class*="summary"], [class*="desc"]');
                if (p) desc = (p.innerText || p.textContent || '').trim();
            }
            if (url && title && title.length > 20 && !isNavJunk(title)) {
                window._ireneChanges.push({url: url, title: title, description: desc || null});
            }
        });
    }

    var observer = new MutationObserver(function(mutations) {
        mutations.forEach(function(m) {
            m.addedNodes.forEach(extractLinks);
        });
        if (window._ireneChanges.length > 0) {
            window._ireneReady = true;
        }
    });
    observer.observe(document.body, {childList: true, subtree: true});
})();
"""

# ---------------------------------------------------------------------------
# Observer JS — post mode (social media / Truth Social)
# ---------------------------------------------------------------------------

_POST_OBSERVER_JS = r"""
(function() {
    if (window._ireneObserverActive) return;
    window._ireneObserverActive = true;
    window._ireneReady = false;
    window._ireneChanges = [];

    var _seenTexts = new Set();

    function extractPost(node) {
        if (node.nodeType !== 1) return;
        // Mastodon / Truth Social selectors
        var selectors = [
            '.status__content p',
            '.status-content',
            '[class*="StatusBody"] p',
            '[class*="post-content"]',
            '[class*="toot"] p',
            'article [class*="content"] p',
            'article p',
        ];
        for (var i = 0; i < selectors.length; i++) {
            var els;
            try { els = node.querySelectorAll(selectors[i]); } catch(e) { continue; }
            els.forEach(function(el) {
                var text = (el.innerText || el.textContent || '').trim();
                if (text && text.length > 20 && !_seenTexts.has(text)) {
                    _seenTexts.add(text);
                    window._ireneChanges.push({
                        url: window.location.href,
                        title: text.substring(0, 250),
                        description: text.length > 250 ? text.substring(250, 600) : null
                    });
                }
            });
        }
        // Also look at the node itself
        if (node.matches && selectors.some(function(s) { try { return node.matches(s); } catch(e) { return false; } })) {
            var text = (node.innerText || node.textContent || '').trim();
            if (text && text.length > 20 && !_seenTexts.has(text)) {
                _seenTexts.add(text);
                window._ireneChanges.push({
                    url: window.location.href,
                    title: text.substring(0, 250),
                    description: text.length > 250 ? text.substring(250, 600) : null
                });
            }
        }
    }

    var observer = new MutationObserver(function(mutations) {
        mutations.forEach(function(m) {
            m.addedNodes.forEach(extractPost);
        });
        if (window._ireneChanges.length > 0) {
            window._ireneReady = true;
        }
    });
    observer.observe(document.body || document.documentElement, {childList: true, subtree: true});
})();
"""

# JS to drain accumulated changes and reset state
_DRAIN_JS = """
(function() {
    var out = window._ireneChanges || [];
    window._ireneChanges = [];
    window._ireneReady = false;
    return out;
})()
"""

# JS to fully reset the observer (called before a page reload)
_RESET_JS = """
window._ireneObserverActive = false;
window._ireneReady = false;
window._ireneChanges = [];
"""


class WatchedPageSource:
    """
    Persistent headless browser tab that watches a single page for DOM changes.

    Usage:
        source = WatchedPageSource(url="https://apnews.com/", ...)
        task = asyncio.create_task(source.run(queue))
        # drain queue elsewhere
        task.cancel()
        await source.close()
    """

    def __init__(
        self,
        url: str,
        reload_seconds: int = 300,
        jitter_fraction: float = 0.20,
        change_wait_seconds: int = 30,
        post_mode: bool = False,
        max_failures: int = 5,
        rng: Optional[random.Random] = None,
    ) -> None:
        self._url = url
        self._reload_seconds = reload_seconds
        self._jitter_fraction = jitter_fraction
        self._change_wait_seconds = change_wait_seconds
        self._post_mode = post_mode
        self._max_failures = max_failures
        self._rng = rng or random.Random()
        self._seen_urls: set[str] = set()
        self._page = None
        self._browser = None
        self._playwright = None
        self._consecutive_failures: int = 0
        self._unreachable: bool = False

    @property
    def is_unreachable(self) -> bool:
        return self._unreachable

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def run(self, queue: asyncio.Queue) -> None:
        """
        Watch loop. Runs forever until cancelled.
        Enqueues RawSignal objects onto `queue` as new content is detected.
        """
        if not await self._check_reachable():
            logger.warning("WatchedPage(%s): unreachable — aborting watch loop", self._url)
            self._unreachable = True
            return

        try:
            await self._ensure_page()
        except Exception as exc:
            logger.error("WatchedPage(%s): failed to open page: %s", self._url, exc)
            self._record_failure()
            return

        logger.info("WatchedPage(%s): watching started (post_mode=%s)", self._url, self._post_mode)

        silent_since = asyncio.get_event_loop().time()
        reload_deadline = self._jittered_reload()

        while True:
            try:
                await asyncio.sleep(self._change_wait_seconds)

                now = asyncio.get_event_loop().time()

                try:
                    ready = await self._page.evaluate("window._ireneReady === true")
                except Exception as exc:
                    logger.warning("WatchedPage(%s): evaluate error: %s", self._url, exc)
                    self._record_failure()
                    if self._unreachable:
                        break
                    await self._reload_page()
                    silent_since = now
                    reload_deadline = self._jittered_reload()
                    continue

                if ready:
                    signals = await self._drain_changes()
                    for sig in signals:
                        await queue.put(sig)
                    if signals:
                        logger.info(
                            "WatchedPage(%s): %d new items detected via MutationObserver",
                            self._url, len(signals),
                        )
                    self._consecutive_failures = 0
                    silent_since = now
                    reload_deadline = self._jittered_reload()

                elif (now - silent_since) >= reload_deadline:
                    logger.debug(
                        "WatchedPage(%s): silent for %.0fs, reloading",
                        self._url, now - silent_since,
                    )
                    await self._reload_page()
                    silent_since = now
                    reload_deadline = self._jittered_reload()

            except asyncio.CancelledError:
                logger.info("WatchedPage(%s): watch loop cancelled", self._url)
                break
            except Exception as exc:
                logger.error("WatchedPage(%s): unexpected error: %s", self._url, exc)
                self._record_failure()
                if self._unreachable:
                    break
                try:
                    await self._reload_page()
                except Exception:
                    pass
                silent_since = asyncio.get_event_loop().time()
                reload_deadline = self._jittered_reload()

    async def close(self) -> None:
        """Shut down the browser tab and Playwright instance."""
        try:
            if self._page and not self._page.is_closed():
                await self._page.close()
        except Exception:
            pass
        try:
            if self._browser:
                await self._browser.close()
        except Exception:
            pass
        try:
            if self._playwright:
                await self._playwright.stop()
        except Exception:
            pass
        logger.info("WatchedPage(%s): closed", self._url)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _check_reachable(self) -> bool:
        """Quick HTTP HEAD check before spinning up Playwright."""
        import aiohttp
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.head(
                    self._url,
                    allow_redirects=True,
                    headers={"User-Agent": "Mozilla/5.0 (compatible; Irene/1.0)"},
                ) as resp:
                    reachable = resp.status < 500
                    if not reachable:
                        logger.warning(
                            "WatchedPage(%s): HTTP %d — marking unreachable",
                            self._url, resp.status,
                        )
                    return reachable
        except Exception as exc:
            logger.warning("WatchedPage(%s): reachability check failed: %s", self._url, exc)
            return False

    def _record_failure(self) -> None:
        self._consecutive_failures += 1
        if self._consecutive_failures >= self._max_failures:
            logger.error(
                "WatchedPage(%s): %d consecutive failures — marking unreachable",
                self._url, self._consecutive_failures,
            )
            self._unreachable = True

    async def _ensure_page(self) -> None:
        """Start Playwright + Chromium and navigate to the target URL."""
        from playwright.async_api import async_playwright
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-extensions",
                "--blink-settings=imagesEnabled=false",
            ],
        )
        context = await self._browser.new_context(
            user_agent="Mozilla/5.0 (compatible; Irene/1.0; news aggregator)",
            java_script_enabled=True,
            bypass_csp=True,
        )
        self._page = await context.new_page()
        await self._page.goto(self._url, wait_until="domcontentloaded", timeout=30_000)
        await self._inject_observer()
        # Drain any links already present at page load
        await self._drain_changes()

    async def _reload_page(self) -> None:
        """Full page reload then re-inject observer."""
        try:
            await self._page.evaluate(_RESET_JS)
            await self._page.reload(wait_until="domcontentloaded", timeout=30_000)
            await self._inject_observer()
            logger.debug("WatchedPage(%s): reloaded", self._url)
        except Exception as exc:
            logger.warning("WatchedPage(%s): reload failed (%s), re-navigating", self._url, exc)
            try:
                await self._page.goto(self._url, wait_until="domcontentloaded", timeout=30_000)
                await self._inject_observer()
            except Exception as exc2:
                logger.error("WatchedPage(%s): re-navigate also failed: %s", self._url, exc2)
                self._record_failure()

    async def _inject_observer(self) -> None:
        """Inject the MutationObserver script into the current page."""
        try:
            js = _POST_OBSERVER_JS if self._post_mode else _OBSERVER_JS
            await self._page.evaluate(js)
        except Exception as exc:
            logger.warning("WatchedPage(%s): observer inject failed: %s", self._url, exc)

    async def _drain_changes(self) -> list[RawSignal]:
        """Drain accumulated changes from the observer, return as RawSignals."""
        try:
            items = await self._page.evaluate(_DRAIN_JS)
        except Exception as exc:
            logger.warning("WatchedPage(%s): drain failed: %s", self._url, exc)
            return []

        if not items:
            return []

        now = datetime.now(timezone.utc)
        signals: list[RawSignal] = []
        for item in items:
            url = _clean_url(item.get("url", ""))
            title = (item.get("title") or "").strip()
            if not url or not title or len(title) < 20:
                continue
            # In post_mode the URL is the page URL — allow duplicates by title
            if not self._post_mode:
                if url in self._seen_urls:
                    continue
                if not url.startswith("http"):
                    continue
                self._seen_urls.add(url)
            else:
                # Dedupe by title prefix in post mode
                title_key = title[:100]
                if title_key in self._seen_urls:
                    continue
                self._seen_urls.add(title_key)

            signals.append(
                RawSignal(
                    source=SignalSource.RSS,
                    source_detail=f"watched:{self._url}",
                    url=url,
                    title=title,
                    description=item.get("description") or None,
                    published_at=now,
                    ingested_at=now,
                )
            )
        return signals

    def _jittered_reload(self) -> float:
        """Return reload_seconds with ±jitter applied."""
        jitter = self._jitter_fraction * self._reload_seconds
        return self._reload_seconds + self._rng.uniform(-jitter, jitter)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TRACKING_PARAMS = frozenset({
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "utm_id", "utm_reader", "ref", "source",
})


def _clean_url(url: str) -> str:
    """Strip fragment and tracking params; return clean absolute URL."""
    from urllib.parse import urlparse, urlencode, parse_qs, urlunparse
    url = url.split("#")[0].strip()
    parsed = urlparse(url)
    if not parsed.query:
        return url
    params = parse_qs(parsed.query, keep_blank_values=True)
    filtered = {k: v for k, v in params.items() if k.lower() not in _TRACKING_PARAMS}
    return urlunparse(parsed._replace(query=urlencode(filtered, doseq=True)))
