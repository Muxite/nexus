"""
Brave Search News API signal source.

Uses the Brave Search News endpoint (`/res/v1/news/search`) to fetch recent
news articles matching prediction-market-relevant queries.

Quota is plan-specific — check the Brave Search API dashboard. Defaults in
``config.yaml`` (``rate_limits.brave_max_per_day``) assume a **paid** plan
(e.g. Basic), not the free tier. Lower ``brave_max_per_day`` if you downgrade.

Brave API reference:
  Endpoint: GET https://api.search.brave.com/res/v1/news/search
  Auth:     X-Subscription-Token: <key>
  Params:   q, count (max 20), freshness=pd (past day)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from irene_shared.models import RawSignal, SignalSource

logger = logging.getLogger(__name__)

_BRAVE_NEWS_URL = "https://api.search.brave.com/res/v1/news/search"

# Multiple focused queries instead of one broad query — each call returns up
# to 20 results, so we rotate through topics to maximise signal diversity
# while staying within rate limits.
_QUERIES = [
    (
        "election OR referendum OR appointed OR resigned OR indicted OR convicted"
    ),
    (
        "federal reserve OR interest rates OR tariff OR sanctions OR ceasefire"
    ),
    (
        "bitcoin OR ethereum OR crypto OR SEC OR legislation OR signed OR vetoed"
    ),
    (
        "war OR invasion OR treaty OR summit OR coup OR protest OR verdict"
    ),
]


class BraveNewsSource:
    """
    Async Brave Search News client.

    Rotates through a list of queries on each fetch() call so a single
    poll covers multiple topic areas without blowing through the rate limit.

    Usage:
        async with aiohttp.ClientSession() as session:
            source = BraveNewsSource(api_key="...", session=session)
            signals = await source.fetch(since=datetime.now(utc) - timedelta(hours=1))
    """

    def __init__(
        self,
        api_key: str,
        session: aiohttp.ClientSession,
        queries: list[str] | None = None,
        count_per_query: int = 20,
        source_detail: str = "brave_news",
    ) -> None:
        self._api_key = api_key
        self._session = session
        self._queries = queries or _QUERIES
        self._count = min(count_per_query, 20)  # API max
        self._source_detail = source_detail
        self._query_index = 0  # rotate queries across fetches
        # In-memory dedup: skip URLs already processed this session
        self._seen_urls: set[str] = set()

    async def fetch(self, since: Optional[datetime] = None) -> list[RawSignal]:
        """
        Fetch news articles from Brave Search, rotating through queries.

        Fetches one query per call (rotating) to keep per-call overhead low
        and spread query diversity over time.  Returns a list of RawSignal
        objects; does NOT deduplicate against the DB (caller handles that).
        """
        if not self._api_key:
            logger.warning("BraveNewsSource: no API key configured, skipping")
            return []

        query = self._queries[self._query_index % len(self._queries)]
        self._query_index += 1

        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "X-Subscription-Token": self._api_key,
        }
        params = {
            "q": query,
            "count": str(self._count),
            "freshness": "pd",  # past day
            "search_lang": "en",
        }

        try:
            async with self._session.get(
                _BRAVE_NEWS_URL,
                headers=headers,
                params=params,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
        except aiohttp.ClientResponseError as exc:
            logger.warning("BraveNewsSource HTTP %d for query %r: %s", exc.status, query, exc)
            return []
        except Exception as exc:
            logger.warning("BraveNewsSource fetch error for query %r: %s", query, exc)
            return []

        results = data.get("results", [])
        signals: list[RawSignal] = []
        now = datetime.now(timezone.utc)

        for item in results:
            url = item.get("url", "")
            if not url:
                continue
            if url in self._seen_urls:
                continue

            title = (item.get("title") or "").strip()
            description = (item.get("description") or "").strip() or None
            published_at = _parse_age(item.get("age", ""), now) or now

            if since and published_at <= since:
                self._seen_urls.add(url)
                continue

            self._seen_urls.add(url)
            signals.append(
                RawSignal(
                    source=SignalSource.RSS,  # reuse RSS enum value for "web news"
                    source_detail=self._source_detail,
                    url=url,
                    title=title,
                    description=description,
                    published_at=published_at,
                    ingested_at=now,
                )
            )

        if signals:
            logger.info("BraveNewsSource: query=%r → %d new results", query, len(signals))
        else:
            logger.debug("BraveNewsSource: query=%r → 0 new results", query)
        return signals


# ---------------------------------------------------------------------------
# Age parsing
# ---------------------------------------------------------------------------

import re
from datetime import timedelta

_RELATIVE_RE = re.compile(
    r"^(\d+)\s+(second|minute|hour|day|week|month|year)s?\s+ago$",
    re.IGNORECASE,
)
_UNIT_SECONDS = {
    "second": 1,
    "minute": 60,
    "hour": 3600,
    "day": 86400,
    "week": 604800,
    "month": 2592000,
    "year": 31536000,
}


def _parse_age(age_str: str, now: datetime) -> datetime | None:
    """Convert Brave's ``age`` string to a UTC datetime."""
    if not age_str:
        return None
    clean = age_str.strip()
    m = _RELATIVE_RE.match(clean)
    if m:
        n = int(m.group(1))
        unit = m.group(2).lower()
        seconds = _UNIT_SECONDS.get(unit, 0) * n
        return now - timedelta(seconds=seconds)
    # Try ISO / dateutil
    try:
        from dateutil import parser as du_parser
        dt = du_parser.parse(clean)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None
