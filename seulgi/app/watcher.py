"""
Seulgi news site watcher — scraper-backed polling.

Instead of running a headless Chromium tab, each SiteWatcher periodically
calls the jennie scraper container (POST /fetch) to extract links.

Flow:
  1. Poll the scraper every ~change_wait_seconds (Gaussian jitter ±20%).
  2. Extract links from resp.extraction.links.
  3. Filter junk, dedup against _seen set.
  4. Emit new RawHeadline objects into the shared queue.
  5. On repeated failures, enter exponential backoff (up to 30 min).
  6. Recover automatically when the scraper becomes reachable again.

Recovery model:
  - 5 consecutive failures → backoff level 1 (30 s wait between retries)
  - Each further bout of 5 failures → backoff doubles (60 s, 120 s … 1800 s)
  - Any successful poll resets backoff to level 0 (normal polling)

This means a temporary scraper outage no longer permanently kills watchers.

The scraper container address is read from SCRAPER_URL env var
(default: http://scraper:9000).
"""

from __future__ import annotations

import asyncio
import collections
import logging
import os
import random
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import aiohttp

from profiler import prof

logger = logging.getLogger(__name__)

_SCRAPER_URL = os.getenv("SCRAPER_URL", "http://scraper:9000")

# Exponential backoff sequence (seconds) applied after max_failures are hit.
# Each additional bout of failures moves one level up; success resets to 0.
_BACKOFF_LEVELS = [30, 60, 120, 240, 480, 900, 1800]


# ---------------------------------------------------------------------------
# Search budget
# ---------------------------------------------------------------------------


class SearchBudget:
    """
    Rolling 1-hour token bucket for Brave Search API calls.

    Shared across all TwitterAccountWatcher instances so the total call rate
    stays within the configured cap regardless of how many accounts are watched.

    Not thread-safe — designed for single-event-loop use (all asyncio).
    """

    def __init__(self, per_hour: int = 20) -> None:
        self._per_hour = per_hour
        self._calls: collections.deque[float] = collections.deque()

    def can_spend(self) -> bool:
        """Return True if a call fits within the current hourly budget."""
        self._evict()
        return len(self._calls) < self._per_hour

    def spend(self) -> None:
        """Record that one call was made right now."""
        self._calls.append(time.monotonic())

    def _evict(self) -> None:
        cutoff = time.monotonic() - 3600.0
        while self._calls and self._calls[0] < cutoff:
            self._calls.popleft()

    @property
    def usage(self) -> tuple[int, int]:
        """Return (calls_in_last_hour, per_hour_limit)."""
        self._evict()
        return len(self._calls), self._per_hour


# ---------------------------------------------------------------------------
# Output type
# ---------------------------------------------------------------------------

@dataclass
class RawHeadline:
    """A newly detected headline from a watched news site or social post."""
    site_url:    str
    headline:    str       # link title / article headline
    article_url: str       # cleaned href
    detected_at: datetime
    headline_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    source:      str = "news"   # "news" | "twitter" | "truth_social"


# ---------------------------------------------------------------------------
# Internal exceptions
# ---------------------------------------------------------------------------

class _FetchError(Exception):
    """Raised by _fetch_headlines when the scraper is unavailable or errors."""


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
    """Strip fragment and common tracking params from a URL."""
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

    The task runs forever until cancelled; it never exits due to scraper errors.
    Use is_degraded / backoff_level to observe current health from outside.
    """

    def __init__(
        self,
        url: str,
        change_wait_seconds: int = 30,
        reload_seconds: int = 300,   # kept for API compatibility, unused
        max_failures: int = 5,
        scraper_url: str = _SCRAPER_URL,
    ) -> None:
        self._url          = url
        self._change_wait  = change_wait_seconds
        self._max_failures = max_failures
        self._scraper_url  = scraper_url.rstrip("/")

        self._session: Optional[aiohttp.ClientSession] = None
        self._seen:    set[str] = set()

        # Failure / backoff state
        self._consecutive_failures: int = 0
        self._backoff_level:        int = 0  # index into _BACKOFF_LEVELS

        # Metrics
        self._last_headline_at: Optional[datetime] = None
        self._last_headline:    str  = ""
        self._total_headlines:  int  = 0

    # ------------------------------------------------------------------
    # Public properties
    # ------------------------------------------------------------------

    @property
    def is_unreachable(self) -> bool:
        """True when at maximum backoff level (scraper consistently failing)."""
        return self._backoff_level >= len(_BACKOFF_LEVELS) - 1

    @property
    def backoff_level(self) -> int:
        return self._backoff_level

    @property
    def status(self) -> dict:
        return {
            "url":              self._url,
            "reachable":        self._backoff_level == 0,
            "failures":         self._consecutive_failures,
            "backoff_level":    self._backoff_level,
            "backoff_wait":     _BACKOFF_LEVELS[self._backoff_level] if self._backoff_level else 0,
            "last_headline_at": self._last_headline_at,
            "last_headline":    self._last_headline,
            "total_headlines":  self._total_headlines,
            "is_unreachable":   self.is_unreachable,
        }

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self, queue: asyncio.Queue) -> None:
        """
        Poll loop — runs until cancelled.  Never exits due to scraper errors.

        Poll interval uses Gaussian jitter (±20% of base wait) to avoid
        thundering-herd when many watchers start simultaneously.
        """
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60),
        )
        logger.info(
            "[watcher] %s: starting (scraper=%s, interval=%ds)",
            self._url, self._scraper_url, self._change_wait,
        )

        while True:
            await asyncio.sleep(self._next_wait())
            try:
                with prof.span("watcher/fetch"):
                    headlines = await self._fetch_headlines()
                for h in headlines:
                    await queue.put(h)
                if headlines:
                    for h in headlines:
                        logger.info(
                            "[watcher] [hid=%s] new headline site=%s url=%s title=%s",
                            h.headline_id, self._url, h.article_url, h.headline[:80],
                        )
                    self._last_headline_at = headlines[-1].detected_at
                    self._last_headline    = headlines[-1].headline
                    self._total_headlines += len(headlines)
                # Reset backoff on any successful poll
                if self._backoff_level > 0:
                    logger.info(
                        "[watcher] %s: recovered (was backoff level %d)",
                        self._url, self._backoff_level,
                    )
                self._consecutive_failures = 0
                self._backoff_level        = 0

            except asyncio.CancelledError:
                logger.info("[watcher] %s: cancelled", self._url)
                break

            except (_FetchError, Exception) as exc:
                self._consecutive_failures += 1
                logger.warning(
                    "[watcher] %s: failure #%d: %s",
                    self._url, self._consecutive_failures, exc,
                )
                if self._consecutive_failures >= self._max_failures:
                    self._backoff_level = min(
                        self._backoff_level + 1, len(_BACKOFF_LEVELS) - 1
                    )
                    self._consecutive_failures = 0
                    logger.warning(
                        "[watcher] %s: backoff level %d — next retry in ~%ds",
                        self._url, self._backoff_level,
                        _BACKOFF_LEVELS[self._backoff_level],
                    )

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _next_wait(self) -> float:
        """Return the next sleep duration with Gaussian jitter.

        Normal polling:  Gaussian(change_wait_seconds, 20% sigma)
        Backoff polling: Gaussian(_BACKOFF_LEVELS[level],   20% sigma)
        Minimum is always 5 seconds to avoid tight loops.
        """
        if self._backoff_level > 0:
            base = float(_BACKOFF_LEVELS[self._backoff_level])
        else:
            base = float(self._change_wait)
        return max(5.0, random.gauss(base, base * 0.2))

    async def _fetch_headlines(self) -> list[RawHeadline]:
        """Call the scraper and return new, unseen headlines."""
        payload = {
            "url":           self._url,
            "extract_links": True,
            "extract_text":  False,
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
            url   = _clean_url((link.get("url")  or "").strip())
            title = _clean_title((link.get("text") or "").strip())
            if not url.startswith("http"):
                continue
            if len(title) < 20 or _is_junk(title):
                continue
            if url in self._seen:
                continue
            self._seen.add(url)
            results.append(RawHeadline(
                site_url    = self._url,
                headline    = title,
                article_url = url,
                detected_at = now,
            ))
        return results


# ---------------------------------------------------------------------------
# Twitter / X account watcher
# ---------------------------------------------------------------------------

class TwitterAccountWatcher:
    """
    Polls the scraper's /twitter/search endpoint for tweets from a specific
    account. Deduplicates by tweet URL so each tweet is emitted exactly once.

    Uses the Brave Search API via the scraper — no Twitter API key needed.

    Interval: polls every `interval_seconds` (default: 120 s).

    Usage:
        watcher = TwitterAccountWatcher(username="elonmusk", interval_seconds=60)
        asyncio.create_task(watcher.run(queue))
    """

    def __init__(
        self,
        username: str,
        interval_seconds: int = 120,
        scraper_url: str = _SCRAPER_URL,
        max_failures: int = 5,
        budget: Optional["SearchBudget"] = None,
    ) -> None:
        self._username = username.lstrip("@")
        self._interval = interval_seconds
        self._scraper_url = scraper_url.rstrip("/")
        self._max_failures = max_failures
        self._budget = budget
        self._seen: set[str] = set()  # tweet URLs
        self._session: Optional[aiohttp.ClientSession] = None
        self._consecutive_failures = 0
        self._backoff_level = 0
        self._total_tweets = 0
        self._last_tweet_at: Optional[datetime] = None

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    @property
    def status(self) -> dict:
        used, limit = self._budget.usage if self._budget else (0, 0)
        return {
            "username": self._username,
            "type": "twitter",
            "total_tweets": self._total_tweets,
            "last_tweet_at": self._last_tweet_at.isoformat() if self._last_tweet_at else None,
            "failures": self._consecutive_failures,
            "budget_used": used,
            "budget_limit": limit,
        }

    async def run(self, queue: asyncio.Queue) -> None:
        logger.info("[twitter] @%s: starting (scraper=%s, interval=%ds)",
                    self._username, self._scraper_url, self._interval)
        while True:
            try:
                # Budget check: skip this poll if Brave API hourly cap is reached
                if self._budget is not None and not self._budget.can_spend():
                    used, limit = self._budget.usage
                    logger.info("[twitter] @%s: budget %d/%d/hr — skipping poll",
                                self._username, used, limit)
                    wait = max(10, random.gauss(self._interval, self._interval * 0.15))
                    await asyncio.sleep(wait)
                    continue

                tweets = await self._fetch_tweets()
                if self._budget is not None:
                    self._budget.spend()
                self._consecutive_failures = 0
                self._backoff_level = 0

                for tweet in tweets:
                    self._total_tweets += 1
                    self._last_tweet_at = tweet.detected_at
                    logger.info(
                        "[twitter] [hid=%s] @%s: %s",
                        tweet.headline_id, self._username, tweet.headline[:80]
                    )
                    await queue.put(tweet)

            except Exception as exc:
                self._consecutive_failures += 1
                if self._consecutive_failures >= self._max_failures:
                    self._backoff_level = min(
                        self._backoff_level + 1, len(_BACKOFF_LEVELS) - 1
                    )
                    self._consecutive_failures = 0
                backoff = _BACKOFF_LEVELS[self._backoff_level]
                logger.warning("[twitter] @%s error (backoff=%ds): %s",
                               self._username, backoff, exc)
                await asyncio.sleep(backoff)
                continue

            wait = max(10, random.gauss(self._interval, self._interval * 0.15))
            await asyncio.sleep(wait)

    async def _fetch_tweets(self) -> list[RawHeadline]:
        session = self._get_session()
        url = f"{self._scraper_url}/twitter/search"
        payload = {"username": self._username, "count": 10}
        try:
            async with session.post(
                url, json=payload, timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                if resp.status != 200:
                    raise _FetchError(f"scraper {resp.status}")
                data = await resp.json()
        except aiohttp.ClientError as exc:
            raise _FetchError(str(exc)) from exc

        posts = data.get("posts") or []
        now = datetime.now(timezone.utc)
        results = []
        for post in posts:
            tweet_url = post.get("url", "")
            text = (post.get("text") or "").strip()
            if not tweet_url or tweet_url in self._seen:
                continue
            if len(text) < 15:
                continue
            self._seen.add(tweet_url)
            headline = RawHeadline(
                site_url=f"https://x.com/@{self._username}",
                headline=text,
                article_url=tweet_url,
                detected_at=now,
                source="twitter",
            )
            results.append(headline)
        return results


# ---------------------------------------------------------------------------
# Truth Social watcher (via RSS feed)
# ---------------------------------------------------------------------------

class TruthSocialWatcher:
    """
    Polls Truth Social via the public RSS feed available for individual accounts.

    Truth Social exposes an RSS feed at:
        https://truthsocial.com/@{username}/feed.rss

    No authentication needed. The scraper's /feed endpoint handles RSS parsing.

    Usage:
        watcher = TruthSocialWatcher(username="realDonaldTrump", interval_seconds=60)
        asyncio.create_task(watcher.run(queue))
    """

    _RSS_BASE = "https://truthsocial.com/@{username}/feed.rss"

    def __init__(
        self,
        username: str,
        interval_seconds: int = 60,
        scraper_url: str = _SCRAPER_URL,
        max_failures: int = 5,
    ) -> None:
        self._username = username.lstrip("@")
        self._interval = interval_seconds
        self._scraper_url = scraper_url.rstrip("/")
        self._max_failures = max_failures
        self._feed_url = self._RSS_BASE.format(username=self._username)
        self._seen: set[str] = set()
        self._session: Optional[aiohttp.ClientSession] = None
        self._consecutive_failures = 0
        self._backoff_level = 0
        self._total_posts = 0
        self._last_post_at: Optional[datetime] = None

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    @property
    def status(self) -> dict:
        return {
            "username": self._username,
            "type": "truth_social",
            "total_posts": self._total_posts,
            "last_post_at": self._last_post_at.isoformat() if self._last_post_at else None,
            "failures": self._consecutive_failures,
        }

    async def run(self, queue: asyncio.Queue) -> None:
        logger.info("[truth] @%s: starting (scraper=%s, interval=%ds)",
                    self._username, self._scraper_url, self._interval)
        while True:
            try:
                posts = await self._fetch_posts()
                self._consecutive_failures = 0
                self._backoff_level = 0

                for post in posts:
                    self._total_posts += 1
                    self._last_post_at = post.detected_at
                    logger.info(
                        "[truth] [hid=%s] @%s: %s",
                        post.headline_id, self._username, post.headline[:80]
                    )
                    await queue.put(post)

            except Exception as exc:
                self._consecutive_failures += 1
                if self._consecutive_failures >= self._max_failures:
                    self._backoff_level = min(
                        self._backoff_level + 1, len(_BACKOFF_LEVELS) - 1
                    )
                    self._consecutive_failures = 0
                backoff = _BACKOFF_LEVELS[self._backoff_level]
                logger.warning("[truth] @%s error (backoff=%ds): %s",
                               self._username, backoff, exc)
                await asyncio.sleep(backoff)
                continue

            wait = max(10, random.gauss(self._interval, self._interval * 0.15))
            await asyncio.sleep(wait)

    async def _fetch_posts(self) -> list[RawHeadline]:
        session = self._get_session()
        url = f"{self._scraper_url}/feed"
        payload = {"url": self._feed_url}
        try:
            async with session.post(
                url, json=payload, timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                if resp.status != 200:
                    raise _FetchError(f"scraper /feed {resp.status}")
                data = await resp.json()
        except aiohttp.ClientError as exc:
            raise _FetchError(str(exc)) from exc

        items = data.get("items") or []
        now = datetime.now(timezone.utc)
        results = []
        for item in items:
            item_url = (item.get("url") or "").strip()
            title = _clean_title((item.get("title") or item.get("summary") or "").strip())
            if not item_url or item_url in self._seen:
                continue
            if len(title) < 10:
                continue
            self._seen.add(item_url)
            post = RawHeadline(
                site_url=f"https://truthsocial.com/@{self._username}",
                headline=title,
                article_url=item_url,
                detected_at=now,
                source="truth_social",
            )
            results.append(post)
        return results
