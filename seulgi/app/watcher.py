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

    If a `db` is provided, every spend() is also logged as a `brave_search`
    usage event for lifetime accounting (kept independent of the in-memory
    rolling counter, which only reflects the last hour).

    Not thread-safe — designed for single-event-loop use (all asyncio).
    """

    def __init__(self, per_hour: int = 20, db=None) -> None:
        self._per_hour = per_hour
        self._calls: collections.deque[float] = collections.deque()
        self._db = db
        self._lifetime: int = 0   # in-process counter; DB is the persisted source of truth

    def can_spend(self) -> bool:
        """Return True if a call fits within the current hourly budget."""
        self._evict()
        return len(self._calls) < self._per_hour

    def spend(self, username: Optional[str] = None) -> None:
        """Record that one call was made right now. Fire-and-forget DB log."""
        self._calls.append(time.monotonic())
        self._lifetime += 1
        if self._db is not None:
            meta = f'{{"username":"{username}"}}' if username else None
            asyncio.create_task(self._db.log_usage(
                kind="brave_search", count=1.0, provider="brave", meta=meta,
            ))

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
        db=None,
    ) -> None:
        self._url          = url
        self._change_wait  = change_wait_seconds
        self._max_failures = max_failures
        self._scraper_url  = scraper_url.rstrip("/")
        # Cycle-8: optional DB ref so we can persist per-site health (the
        # `watcher_health` schema was added in cycle 4 but nothing wrote to
        # it until now). Persistence lets the dashboard surface stability
        # scores across container restarts and lets postmortems answer "did
        # outlet X actually deliver?" without grepping logs.
        self._db = db
        self._last_content_hash: Optional[str] = None
        self._short_circuit_hits: int = 0

        self._session: Optional[aiohttp.ClientSession] = None
        self._seen:    set[str] = set()

        # Failure / backoff state
        self._consecutive_failures: int = 0
        self._backoff_level:        int = 0  # index into _BACKOFF_LEVELS
        self._last_error:           str = ""

        # Metrics
        self._last_headline_at: Optional[datetime] = None
        self._last_headline:    str  = ""
        self._total_headlines:  int  = 0
        # Adaptive interval: scale base wait by `interval_multiplier` based on
        # recent churn. Three successful polls with zero new headlines doubles
        # the multiplier (up to 5x); any poll with new headlines resets to 1.0.
        # This keeps fast sites fast and stops us hammering quiet sites.
        self._idle_streak: int = 0
        self._interval_multiplier: float = 1.0
        self._max_interval_multiplier: float = 5.0
        # EMA of fetch latency in ms (for health surface)
        self._ema_latency_ms: Optional[float] = None
        # Per-fetch counters
        self._total_fetches: int = 0
        self._total_successes: int = 0

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
            "last_error":       self._last_error,
            # Adaptive scheduling + reliability surface (cycle-5 additions).
            "interval_multiplier": self._interval_multiplier,
            "current_interval_s":  self._change_wait * self._interval_multiplier,
            "ema_latency_ms":      self._ema_latency_ms,
            "total_fetches":       self._total_fetches,
            "total_successes":     self._total_successes,
            "success_rate":        (self._total_successes / self._total_fetches) if self._total_fetches else None,
            "short_circuit_hits":  self._short_circuit_hits,
            "content_hash":        self._last_content_hash,
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
            self._total_fetches += 1
            t0 = time.monotonic()
            content_hash_now: Optional[str] = None
            try:
                with prof.span("watcher/fetch"):
                    headlines, content_hash_now = await self._fetch_headlines_with_hash()
                # Update latency EMA (alpha=0.2)
                dt_ms = (time.monotonic() - t0) * 1000.0
                self._ema_latency_ms = (
                    dt_ms if self._ema_latency_ms is None
                    else 0.8 * self._ema_latency_ms + 0.2 * dt_ms
                )
                self._total_successes += 1

                # Content-hash short-circuit: if the response's deduped link
                # set hashes to the same value as the previous successful
                # fetch, the page hasn't changed. We still bump the idle
                # streak (so adaptive interval extends) but skip ALL
                # downstream work — engine dispatch, AI tokens, the lot.
                if (
                    content_hash_now is not None
                    and content_hash_now == self._last_content_hash
                    and not headlines
                ):
                    self._short_circuit_hits += 1
                    self._idle_streak += 1
                    if self._idle_streak >= 3 and self._interval_multiplier < self._max_interval_multiplier:
                        self._interval_multiplier = min(
                            self._interval_multiplier * 2.0,
                            self._max_interval_multiplier,
                        )
                        self._idle_streak = 0
                    self._consecutive_failures = 0
                    self._backoff_level = 0
                    if self._db is not None:
                        try:
                            await self._db.upsert_watcher_health(
                                self._url, success=True,
                                ema_latency_ms=self._ema_latency_ms,
                                current_interval_s=self._change_wait * self._interval_multiplier,
                                new_headlines=0,
                                content_hash=content_hash_now,
                            )
                        except Exception:  # noqa: BLE001
                            pass
                    continue

                self._last_content_hash = content_hash_now

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
                    # Active site — slot back to base interval
                    self._idle_streak = 0
                    if self._interval_multiplier > 1.0:
                        logger.info(
                            "[watcher] %s: churn detected, restoring base interval (was ×%.1f)",
                            self._url, self._interval_multiplier,
                        )
                    self._interval_multiplier = 1.0
                else:
                    # Quiet poll — back off the polling interval gradually so we
                    # stop hammering sites that aren't producing.
                    self._idle_streak += 1
                    if self._idle_streak >= 3 and self._interval_multiplier < self._max_interval_multiplier:
                        self._interval_multiplier = min(
                            self._interval_multiplier * 2.0,
                            self._max_interval_multiplier,
                        )
                        self._idle_streak = 0
                        logger.debug(
                            "[watcher] %s: idle, extending interval ×%.1f",
                            self._url, self._interval_multiplier,
                        )
                # Reset backoff on any successful poll
                if self._backoff_level > 0:
                    logger.info(
                        "[watcher] %s: recovered (was backoff level %d)",
                        self._url, self._backoff_level,
                    )
                self._consecutive_failures = 0
                self._backoff_level        = 0
                self._last_error           = ""

                # Persist watcher health on every successful fetch so the
                # dashboard can render lifetime success rate and the
                # postmortem can identify silently-degraded sources.
                if self._db is not None:
                    try:
                        await self._db.upsert_watcher_health(
                            self._url, success=True,
                            ema_latency_ms=self._ema_latency_ms,
                            current_interval_s=self._change_wait * self._interval_multiplier,
                            new_headlines=len(headlines),
                            content_hash=content_hash_now,
                        )
                    except Exception:  # noqa: BLE001
                        pass

            except asyncio.CancelledError:
                logger.info("[watcher] %s: cancelled", self._url)
                break

            except (_FetchError, Exception) as exc:
                self._consecutive_failures += 1
                self._last_error = str(exc)[:80]
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
                if self._db is not None:
                    try:
                        await self._db.upsert_watcher_health(
                            self._url, success=False,
                            ema_latency_ms=self._ema_latency_ms,
                            current_interval_s=self._change_wait * self._interval_multiplier,
                            new_headlines=0,
                        )
                    except Exception:  # noqa: BLE001
                        pass

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
            base = float(self._change_wait) * self._interval_multiplier
        return max(5.0, random.gauss(base, base * 0.2))

    async def _fetch_headlines(self) -> list[RawHeadline]:
        """Backward-compat shim — discards the content hash."""
        results, _ = await self._fetch_headlines_with_hash()
        return results

    async def _fetch_headlines_with_hash(self) -> tuple[list[RawHeadline], Optional[str]]:
        """Call the scraper and return (new headlines, content fingerprint).

        The fingerprint is a SHA-1 over the sorted, deduped link URLs in the
        response. Stable across calls when the page hasn't changed; lets the
        outer loop short-circuit identical responses before any downstream
        work happens.
        """
        import hashlib
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

        # Fingerprint the sorted URL set so identical responses → identical
        # hashes regardless of link order. Skips entirely if no links.
        clean_urls = sorted({
            _clean_url((l.get("url") or "").strip())
            for l in links
            if isinstance(l, dict)
        })
        clean_urls = [u for u in clean_urls if u.startswith("http")]
        content_hash = (
            hashlib.sha1("\n".join(clean_urls).encode()).hexdigest()[:16]
            if clean_urls else None
        )

        if not links:
            return [], content_hash

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
        return results, content_hash


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
        self._last_error: str = ""
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
            "last_tweet_at": self._last_tweet_at,
            "failures": self._consecutive_failures,
            "budget_used": used,
            "budget_limit": limit,
            "last_error": self._last_error,
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
                    self._budget.spend(self._username)
                self._consecutive_failures = 0
                self._backoff_level = 0
                self._last_error = ""

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
                self._last_error = str(exc)[:80]
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
        self._last_error: str = ""
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
            "last_post_at": self._last_post_at,
            "failures": self._consecutive_failures,
            "last_error": self._last_error,
        }

    async def run(self, queue: asyncio.Queue) -> None:
        logger.info("[truth] @%s: starting (scraper=%s, interval=%ds)",
                    self._username, self._scraper_url, self._interval)
        while True:
            try:
                posts = await self._fetch_posts()
                self._consecutive_failures = 0
                self._backoff_level = 0
                self._last_error = ""

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
                self._last_error = str(exc)[:80]
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


# ---------------------------------------------------------------------------
# Generic RSS / Atom FeedWatcher (cycle 8)
# ---------------------------------------------------------------------------


class FeedWatcher:
    """
    Subscribes to a site's RSS/Atom feed via the scraper's /feed endpoint.

    Why exists: cycle 4's preflight identified 26 outlets that publish proper
    RSS — Times of Israel, The Hindu, Al Jazeera, Le Monde, SCMP, etc. — and
    we were rendering every page with Playwright (~3–10s per fetch) when a
    feed fetch is ~50ms. This watcher polls the feed instead, leaving the
    Playwright budget for sites that don't expose feeds.

    Behaviour mirrors SiteWatcher's API surface (status dict, run/close, db
    persistence) so the dashboard and watcher_status loop don't need to
    special-case it.
    """

    def __init__(
        self,
        site_url: str,
        feed_url: str,
        *,
        interval_seconds: int = 60,
        scraper_url: str = _SCRAPER_URL,
        max_failures: int = 5,
        db=None,
    ) -> None:
        self._url      = site_url      # canonical display URL (e.g. https://www.aljazeera.com/news/)
        self._feed_url = feed_url
        self._interval = interval_seconds
        self._max_failures = max_failures
        self._scraper_url = scraper_url.rstrip("/")
        self._db = db
        self._session: Optional[aiohttp.ClientSession] = None
        self._seen: set[str] = set()
        self._consecutive_failures = 0
        self._backoff_level = 0
        self._last_error: str = ""
        self._last_headline_at: Optional[datetime] = None
        self._last_headline = ""
        self._total_headlines = 0
        self._total_fetches = 0
        self._total_successes = 0
        self._ema_latency_ms: Optional[float] = None

    @property
    def is_unreachable(self) -> bool:
        return self._backoff_level >= len(_BACKOFF_LEVELS) - 1

    @property
    def backoff_level(self) -> int:
        return self._backoff_level

    @property
    def status(self) -> dict:
        return {
            "url":              self._url,
            "feed_url":         self._feed_url,
            "type":             "feed",
            "reachable":        self._backoff_level == 0,
            "failures":         self._consecutive_failures,
            "backoff_level":    self._backoff_level,
            "last_headline_at": self._last_headline_at,
            "last_headline":    self._last_headline,
            "total_headlines":  self._total_headlines,
            "is_unreachable":   self.is_unreachable,
            "last_error":       self._last_error,
            "ema_latency_ms":   self._ema_latency_ms,
            "total_fetches":    self._total_fetches,
            "total_successes":  self._total_successes,
            "success_rate":     (self._total_successes / self._total_fetches) if self._total_fetches else None,
        }

    async def run(self, queue: asyncio.Queue) -> None:
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        logger.info(
            "[feed] %s ← %s starting (interval=%ds)",
            self._url, self._feed_url, self._interval,
        )
        while True:
            await asyncio.sleep(max(5, random.gauss(self._interval, self._interval * 0.15)))
            self._total_fetches += 1
            t0 = time.monotonic()
            try:
                items = await self._fetch_feed()
                self._ema_latency_ms = (time.monotonic() - t0) * 1000.0 if self._ema_latency_ms is None \
                    else 0.8 * self._ema_latency_ms + 0.2 * (time.monotonic() - t0) * 1000.0
                self._total_successes += 1
                new_count = 0
                for h in items:
                    await queue.put(h)
                    new_count += 1
                    logger.info(
                        "[feed] [hid=%s] new headline site=%s url=%s title=%s",
                        h.headline_id, self._url, h.article_url, h.headline[:80],
                    )
                    self._last_headline_at = h.detected_at
                    self._last_headline    = h.headline
                self._total_headlines += new_count
                if self._backoff_level > 0:
                    logger.info("[feed] %s: recovered (was backoff %d)", self._url, self._backoff_level)
                self._consecutive_failures = 0
                self._backoff_level = 0
                self._last_error = ""
                if self._db is not None:
                    try:
                        await self._db.upsert_watcher_health(
                            self._url, success=True,
                            ema_latency_ms=self._ema_latency_ms,
                            current_interval_s=float(self._interval),
                            new_headlines=new_count,
                        )
                    except Exception:  # noqa: BLE001
                        pass

            except asyncio.CancelledError:
                logger.info("[feed] %s: cancelled", self._url)
                break

            except Exception as exc:
                self._consecutive_failures += 1
                self._last_error = str(exc)[:80]
                logger.warning("[feed] %s: failure #%d: %s",
                               self._url, self._consecutive_failures, exc)
                if self._consecutive_failures >= self._max_failures:
                    self._backoff_level = min(self._backoff_level + 1, len(_BACKOFF_LEVELS) - 1)
                    self._consecutive_failures = 0
                if self._db is not None:
                    try:
                        await self._db.upsert_watcher_health(
                            self._url, success=False,
                            ema_latency_ms=self._ema_latency_ms,
                            current_interval_s=float(self._interval),
                            new_headlines=0,
                        )
                    except Exception:  # noqa: BLE001
                        pass

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def _fetch_feed(self) -> list[RawHeadline]:
        url = f"{self._scraper_url}/feed"
        payload = {"url": self._feed_url}
        async with self._session.post(url, json=payload) as resp:
            if resp.status != 200:
                raise _FetchError(f"scraper /feed {resp.status}")
            data = await resp.json()

        items = data.get("items") or []
        now = datetime.now(timezone.utc)
        results: list[RawHeadline] = []
        for item in items:
            item_url = (item.get("url") or item.get("link") or "").strip()
            title = _clean_title((item.get("title") or "").strip())
            if not item_url or item_url in self._seen:
                continue
            if len(title) < 10 or _is_junk(title):
                continue
            self._seen.add(item_url)
            results.append(RawHeadline(
                site_url=self._url,
                headline=title,
                article_url=item_url,
                detected_at=now,
                source="news_rss",
            ))
        return results


# ---------------------------------------------------------------------------
# Scraper health poller
# ---------------------------------------------------------------------------


class ScraperHealthPoller:
    """
    Probes the scraper's GET /health endpoint on a fixed interval and exposes
    last-known reachability for the dashboard.

    All watchers share a single scraper service; when it goes down every
    watcher fails identically. This poller surfaces that root cause once,
    instead of forcing the operator to guess from 17 identical failures.
    """

    def __init__(
        self,
        scraper_url: str = _SCRAPER_URL,
        interval_seconds: float = 15.0,
        on_status_change=None,   # Optional[Callable[[dict], None]]
    ) -> None:
        self._url = scraper_url.rstrip("/") + "/health"
        self._interval = interval_seconds
        self._on_status_change = on_status_change

        self._session: Optional[aiohttp.ClientSession] = None
        self._reachable: bool = False
        self._last_ok_at: Optional[datetime] = None
        self._last_checked_at: Optional[datetime] = None
        self._last_error: str = "never probed"
        self._latency_ms: Optional[float] = None
        self._running = False

    @property
    def status(self) -> dict:
        return {
            "scraper_url":     self._url,
            "reachable":       self._reachable,
            "last_ok_at":      self._last_ok_at,
            "last_checked_at": self._last_checked_at,
            "last_error":      self._last_error,
            "latency_ms":      self._latency_ms,
        }

    async def start(self) -> None:
        self._running = True
        asyncio.create_task(self._loop(), name="scraper_health_poller")

    async def close(self) -> None:
        self._running = False
        if self._session and not self._session.closed:
            await self._session.close()

    async def probe_once(self) -> dict:
        """Single probe; returns the same shape as `status` after running."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        prev_reachable = self._reachable
        t0 = time.monotonic()
        try:
            async with self._session.get(
                self._url, timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                self._latency_ms = (time.monotonic() - t0) * 1000.0
                self._last_checked_at = datetime.now(timezone.utc)
                if resp.status == 200:
                    self._reachable = True
                    self._last_ok_at = self._last_checked_at
                    self._last_error = ""
                else:
                    self._reachable = False
                    self._last_error = f"HTTP {resp.status}"
        except Exception as exc:
            self._latency_ms = (time.monotonic() - t0) * 1000.0
            self._last_checked_at = datetime.now(timezone.utc)
            self._reachable = False
            self._last_error = str(exc)[:80]

        if prev_reachable != self._reachable:
            if self._reachable:
                logger.warning("[scraper_health] recovered (latency=%.0fms)", self._latency_ms or 0)
            else:
                logger.error(
                    "[scraper_health] UNREACHABLE: %s (url=%s)",
                    self._last_error, self._url,
                )
            if self._on_status_change is not None:
                try:
                    self._on_status_change(self.status)
                except Exception:
                    pass
        return self.status

    async def _loop(self) -> None:
        while self._running:
            try:
                await self.probe_once()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.debug("[scraper_health] probe loop error: %s", exc)
            await asyncio.sleep(self._interval)
