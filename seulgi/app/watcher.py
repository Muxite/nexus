"""
Seulgi news site watcher — scraper-backed polling.

Instead of running a headless Chromium tab, each SiteWatcher periodically
calls the jennie scraper container (POST /fetch) to fetch the page and extract
links. This is lighter and avoids running Playwright inside seulgi.

Flow:
  1. Poll the scraper every `change_wait_seconds`.
  2. Extract links from resp.extraction.links.
  3. Filter junk, dedup against _seen set.
  4. Emit new RawHeadline objects into the shared queue.
  5. On repeated failures, mark the site unreachable and stop.

The scraper container address is read from the SCRAPER_URL env var
(default: http://scraper:9000).
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import aiohttp

logger = logging.getLogger(__name__)

_SCRAPER_URL = os.getenv("SCRAPER_URL", "http://scraper:9000")

# ---------------------------------------------------------------------------
# Output type
# ---------------------------------------------------------------------------


@dataclass
class RawHeadline:
    """A newly detected headline from a watched news site."""
    site_url: str
    headline: str       # the link title / article headline
    article_url: str    # the link href (cleaned)
    detected_at: datetime


# ---------------------------------------------------------------------------
# Internal exceptions
# ---------------------------------------------------------------------------


class _FetchError(Exception):
    """Raised by _fetch_headlines when the scraper is unavailable or returns an error."""


# ---------------------------------------------------------------------------
# Junk filter
# ---------------------------------------------------------------------------

_NAV_JUNK = frozenset({
    "sign in", "log in", "subscribe", "newsletter", "cookie", "privacy",
    "terms", "contact", "about us", "advertise", "accessibility",
    "search", "menu", "home", "sports", "weather",
})

_COMMENT_RE = re.compile(r"\s+\d[\d,.]*[km]?\s*(comments?|replies?)?$", re.I)


def _is_junk(text: str) -> bool:
    lower = text.lower()
    return any(j in lower for j in _NAV_JUNK)


def _clean_title(text: str) -> str:
    text = " ".join(text.split())
    return _COMMENT_RE.sub("", text).strip()


# ---------------------------------------------------------------------------
# URL cleaner
# ---------------------------------------------------------------------------

_TRACKING_PARAMS = frozenset({
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "utm_id", "ref", "source",
})


def _clean_url(url: str) -> str:
    """Strip fragment and tracking params from a URL."""
    url = url.split("#")[0].strip()
    parsed = urlparse(url)
    if not parsed.query:
        return url
    params = parse_qs(parsed.query, keep_blank_values=True)
    filtered = {k: v for k, v in params.items() if k.lower() not in _TRACKING_PARAMS}
    return urlunparse(parsed._replace(query=urlencode(filtered, doseq=True)))


# ---------------------------------------------------------------------------
# Watcher
# ---------------------------------------------------------------------------


class SiteWatcher:
    """
    Watches a single news site by polling the jennie scraper container.

    Lifecycle:
        watcher = SiteWatcher(url="https://apnews.com", ...)
        asyncio.create_task(watcher.run(queue))
        # later:
        await watcher.close()
    """

    def __init__(
        self,
        url: str,
        change_wait_seconds: int = 30,
        reload_seconds: int = 300,   # kept for API compatibility, not used
        max_failures: int = 5,
        scraper_url: str = _SCRAPER_URL,
    ) -> None:
        self._url = url
        self._change_wait = change_wait_seconds
        self._max_failures = max_failures
        self._scraper_url = scraper_url.rstrip("/")

        self._session: Optional[aiohttp.ClientSession] = None
        self._seen: set[str] = set()
        self._failures = 0
        self._unreachable = False
        self._last_headline_at: Optional[datetime] = None
        self._last_headline: str = ""
        self._total_headlines: int = 0

    @property
    def is_unreachable(self) -> bool:
        return self._unreachable

    @property
    def status(self) -> dict:
        return {
            "url": self._url,
            "reachable": not self._unreachable,
            "failures": self._failures,
            "last_headline_at": self._last_headline_at,
            "last_headline": self._last_headline,
            "total_headlines": self._total_headlines,
            "is_unreachable": self._unreachable,
        }

    async def run(self, queue: asyncio.Queue) -> None:
        """
        Main poll loop. Puts RawHeadline objects into `queue`.
        Runs until cancelled or the site becomes unreachable.
        """
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60),
        )
        logger.info("[watcher] %s: starting (scraper=%s, interval=%ds)",
                    self._url, self._scraper_url, self._change_wait)

        while True:
            try:
                await asyncio.sleep(self._change_wait)
                headlines = await self._fetch_headlines()  # raises _FetchError on scraper failure
                for h in headlines:
                    await queue.put(h)
                if headlines:
                    logger.info("[watcher] %s: %d new headline(s)", self._url, len(headlines))
                    self._last_headline_at = headlines[-1].detected_at
                    self._last_headline = headlines[-1].headline
                    self._total_headlines += len(headlines)
                self._failures = 0

            except asyncio.CancelledError:
                logger.info("[watcher] %s: cancelled", self._url)
                break
            except _FetchError as exc:
                logger.warning("[watcher] %s: fetch error: %s", self._url, exc)
                self._fail()
                if self._unreachable:
                    break
            except Exception as exc:
                logger.warning("[watcher] %s: loop error: %s", self._url, exc)
                self._fail()
                if self._unreachable:
                    break

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _fetch_headlines(self) -> list[RawHeadline]:
        """Call the scraper and return new headlines."""
        payload = {
            "url": self._url,
            "extract_links": True,
            "extract_text": False,
        }
        try:
            async with self._session.post(
                f"{self._scraper_url}/fetch",
                json=payload,
            ) as resp:
                if resp.status >= 500:
                    raise _FetchError(f"scraper returned HTTP {resp.status}")
                data = await resp.json()
        except _FetchError:
            raise
        except aiohttp.ClientConnectorError as exc:
            raise _FetchError(f"scraper unreachable: {exc}") from exc
        except Exception as exc:
            raise _FetchError(str(exc)) from exc

        links = []
        try:
            extraction = data.get("extraction") or {}
            links = extraction.get("links") or []
        except Exception:
            pass

        if not links:
            return []

        now = datetime.now(timezone.utc)
        results = []
        for link in links:
            url = _clean_url((link.get("url") or "").strip())
            title = _clean_title((link.get("text") or "").strip())
            if not url.startswith("http"):
                continue
            if len(title) < 20 or _is_junk(title):
                continue
            if url in self._seen:
                continue
            self._seen.add(url)
            results.append(RawHeadline(
                site_url=self._url,
                headline=title,
                article_url=url,
                detected_at=now,
            ))
        return results

    def _fail(self) -> None:
        self._failures += 1
        if self._failures >= self._max_failures:
            logger.error("[watcher] %s: %d consecutive failures — unreachable",
                         self._url, self._failures)
            self._unreachable = True
