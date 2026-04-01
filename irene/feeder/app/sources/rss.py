"""
RSS feed signal source.

Fetches and parses RSS/Atom feeds using feedparser.
One RSSSource instance per feed URL.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Optional

import aiohttp
import feedparser  # type: ignore

from irene_shared.models import RawSignal, SignalSource

logger = logging.getLogger(__name__)


class RSSSource:
    """
    Async RSS/Atom feed source.

    Usage:
        async with aiohttp.ClientSession() as session:
            source = RSSSource(url="https://...", session=session)
            signals = await source.fetch(since=...)
    """

    def __init__(self, url: str, session: aiohttp.ClientSession) -> None:
        self._url = url
        self._session = session

    async def fetch(self, since: Optional[datetime] = None) -> list[RawSignal]:
        """
        Fetch and parse the RSS feed. Returns RawSignal list.

        Args:
            since: Only include entries published after this datetime (UTC).
                   If None, all entries are returned.
        """
        raw_xml = await self._fetch_xml()
        if raw_xml is None:
            return []

        feed = feedparser.parse(raw_xml)
        signals = []
        ingested_at = datetime.now(timezone.utc)

        for entry in feed.entries:
            url = entry.get("link") or ""
            title = entry.get("title") or ""
            if not url or not title:
                continue

            published_at = _parse_entry_date(entry)
            if since and published_at and published_at < since:
                continue

            description = (
                entry.get("summary")
                or entry.get("description")
                or entry.get("content", [{}])[0].get("value")
            )

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

        logger.info("RSSSource(%s): fetched %d entries", self._url, len(signals))
        return signals

    async def _fetch_xml(self) -> Optional[bytes]:
        try:
            async with self._session.get(
                self._url,
                timeout=aiohttp.ClientTimeout(total=10),
                headers={"User-Agent": "Irene/1.0 (news aggregator)"},
            ) as resp:
                resp.raise_for_status()
                return await resp.read()
        except aiohttp.ClientResponseError as exc:
            logger.error("RSS HTTP error %d for %s: %s", exc.status, self._url, exc.message)
            return None
        except Exception as exc:
            logger.error("RSS fetch failed for %s: %s", self._url, exc)
            return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_entry_date(entry) -> Optional[datetime]:
    """Extract publication datetime from a feedparser entry."""
    # feedparser normalises most date fields to a time.struct_time in
    # entry.published_parsed or entry.updated_parsed
    for attr in ("published_parsed", "updated_parsed"):
        parsed = getattr(entry, attr, None)
        if parsed:
            try:
                import calendar
                ts = calendar.timegm(parsed)
                return datetime.fromtimestamp(ts, tz=timezone.utc)
            except Exception:
                continue

    # Fallback: parse raw string
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
    import re
    return re.sub(r"<[^>]+>", " ", text).strip() or None
