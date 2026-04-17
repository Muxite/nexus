"""
RSS feed signal source.

Fetches and parses RSS/Atom feeds using feedparser.
One RSSSource instance per feed URL — keep the same instance alive across
polls so HTTP caching state (ETag / Last-Modified) is preserved.

HTTP conditional GET:
  On the first fetch the server's ETag and Last-Modified headers (if present)
  are stored on the instance.  Subsequent fetches send If-None-Match /
  If-Modified-Since.  A 304 response means nothing changed; the method returns
  an empty list immediately without parsing.  This eliminates redundant XML
  downloads when feeds publish infrequently.

In-memory dedup:
  Seen article URLs are tracked in a per-instance set so we never attempt a
  DB insert for the same URL twice within a session, even if the entry's
  published_at date falls inside the `since` window on multiple polls.
"""

from __future__ import annotations

import logging
import re
import calendar
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Optional

import aiohttp
import feedparser  # type: ignore

from irene_shared.models import RawSignal, SignalSource

logger = logging.getLogger(__name__)


class RSSSource:
    """
    Async RSS/Atom feed source with HTTP caching and in-session dedup.

    Keep a single instance per feed URL across polls — the ETag / Last-Modified
    state stored on the instance is what enables 304 short-circuits.

    Usage:
        source = RSSSource(url="https://...", session=session)
        signals = await source.fetch(since=last_poll_time)
    """

    def __init__(self, url: str, session: aiohttp.ClientSession) -> None:
        self._url = url
        self._session = session
        # HTTP cache headers from the last successful (200) response
        self._etag: str | None = None
        self._last_modified: str | None = None
        # URLs seen this session — skip DB insert attempts for these
        self._seen_urls: set[str] = set()

    async def fetch(self, since: Optional[datetime] = None) -> list[RawSignal]:
        """
        Fetch and parse the RSS feed.

        Returns only entries not yet seen this session and (if `since` is given)
        published after that datetime.  Returns an empty list immediately on a
        304 Not Modified response.

        Args:
            since: Only include entries published after this datetime (UTC).
        """
        result = await self._fetch_xml()
        if result is None:
            # 304 Not Modified or network error — nothing to process
            return []

        raw_xml, etag, last_modified = result
        # Store cache headers for the next request
        if etag:
            self._etag = etag
        if last_modified:
            self._last_modified = last_modified

        feed = feedparser.parse(raw_xml)
        signals = []
        ingested_at = datetime.now(timezone.utc)

        for entry in feed.entries:
            url = entry.get("link") or ""
            title = entry.get("title") or ""
            if not url or not title:
                continue

            # In-memory dedup — skip without any DB round-trip
            if url in self._seen_urls:
                continue

            published_at = _parse_entry_date(entry)
            if since and published_at and published_at <= since:
                self._seen_urls.add(url)  # remember it so future polls skip it too
                continue

            description = (
                entry.get("summary")
                or entry.get("description")
                or entry.get("content", [{}])[0].get("value")
            )

            self._seen_urls.add(url)
            signals.append(
                RawSignal(
                    source=SignalSource.RSS,
                    source_detail=self._url,
                    url=url,
                    title=title,
                    description=_strip_html(description),
                    published_at=published_at,
                    ingested_at=ingested_at,
                )
            )

        if signals:
            logger.info("RSSSource(%s): %d new entries", self._url, len(signals))
        else:
            logger.debug("RSSSource(%s): 0 new entries", self._url)
        return signals

    async def _fetch_xml(self) -> tuple[bytes, str | None, str | None] | None:
        """
        Perform the HTTP GET with conditional headers.

        Returns:
            (body, etag, last_modified) on a 200 response.
            None on 304 Not Modified or any error.
        """
        headers: dict[str, str] = {
            "User-Agent": "Irene/1.0 (news aggregator)",
            "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml",
        }
        if self._etag:
            headers["If-None-Match"] = self._etag
        if self._last_modified:
            headers["If-Modified-Since"] = self._last_modified

        try:
            async with self._session.get(
                self._url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10),
                allow_redirects=True,
            ) as resp:
                if resp.status == 304:
                    logger.debug("RSSSource(%s): 304 Not Modified — skipping", self._url)
                    return None
                resp.raise_for_status()
                body = await resp.read()
                etag = resp.headers.get("ETag")
                last_modified = resp.headers.get("Last-Modified")
                return body, etag, last_modified

        except aiohttp.ClientResponseError as exc:
            logger.error("RSS HTTP %d for %s: %s", exc.status, self._url, exc.message)
            return None
        except Exception as exc:
            logger.error("RSS fetch failed for %s: %s", self._url, exc)
            return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_entry_date(entry) -> Optional[datetime]:
    """Extract publication datetime from a feedparser entry."""
    for attr in ("published_parsed", "updated_parsed"):
        parsed = getattr(entry, attr, None)
        if parsed:
            try:
                ts = calendar.timegm(parsed)
                return datetime.fromtimestamp(ts, tz=timezone.utc)
            except Exception:
                continue

    for attr in ("published", "updated"):
        raw = getattr(entry, attr, None)
        if raw:
            try:
                return parsedate_to_datetime(raw).astimezone(timezone.utc)
            except Exception:
                continue

    return None


def _strip_html(text: Optional[str]) -> Optional[str]:
    """Remove HTML tags from a string for cleaner embedding text."""
    if not text:
        return None
    return re.sub(r"<[^>]+>", " ", text).strip() or None
