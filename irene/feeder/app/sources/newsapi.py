"""
NewsAPI signal source.

Polls newsapi.org /v2/everything for recent articles.
Requires a NewsAPI key (free tier: 100 req/day, developer tier: unlimited).

Deduplication is by URL — the caller should check the DB before inserting.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from irene_shared.models import RawSignal, SignalSource

logger = logging.getLogger(__name__)

_NEWSAPI_URL = "https://newsapi.org/v2/everything"

# Broad query covering major prediction market categories
_DEFAULT_QUERY = (
    "election OR policy OR legislation OR rates OR tariff OR verdict OR "
    "appointed OR signed OR approved OR rejected OR resigned OR launched OR "
    "bitcoin OR ethereum OR crypto OR sanctions OR ceasefire OR treaty"
)


class NewsAPISource:
    """
    Async NewsAPI client.

    Usage:
        async with aiohttp.ClientSession() as session:
            source = NewsAPISource(api_key="...", session=session)
            signals = await source.fetch(since=datetime.now(utc) - timedelta(hours=1))
    """

    def __init__(
        self,
        api_key: str,
        session: aiohttp.ClientSession,
        page_size: int = 100,
        query: str = _DEFAULT_QUERY,
        source_detail: str = "newsapi",
    ) -> None:
        self._api_key = api_key
        self._session = session
        self._page_size = min(page_size, 100)  # API max is 100
        self._query = query
        self._source_detail = source_detail
        # In-memory dedup: skip URLs already processed this session
        self._seen_urls: set[str] = set()

    async def fetch(self, since: Optional[datetime] = None) -> list[RawSignal]:
        """
        Fetch articles published after `since`.

        Returns a list of RawSignal objects. Does NOT deduplicate against the DB
        — the caller is responsible for that.
        """
        if not self._api_key:
            logger.warning("NewsAPI key not configured — skipping")
            return []

        params = {
            "q": self._query,
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": str(self._page_size),
            "apiKey": self._api_key,
        }
        if since:
            params["from"] = since.strftime("%Y-%m-%dT%H:%M:%S")

        try:
            async with self._session.get(
                _NEWSAPI_URL,
                params=params,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
        except aiohttp.ClientResponseError as exc:
            logger.error("NewsAPI HTTP error %d: %s", exc.status, exc.message)
            return []
        except Exception as exc:
            logger.error("NewsAPI request failed: %s", exc)
            return []

        articles = data.get("articles") or []
        signals = []
        ingested_at = datetime.now(timezone.utc)

        for article in articles:
            url = article.get("url") or ""
            title = article.get("title") or ""
            if not url or not title or title == "[Removed]":
                continue
            if url in self._seen_urls:
                continue

            published_at = _parse_published(article.get("publishedAt"))
            description = article.get("description") or article.get("content")

            self._seen_urls.add(url)
            signals.append(
                RawSignal(
                    source=SignalSource.NEWSAPI,
                    source_detail=self._source_detail,
                    url=url,
                    title=title,
                    description=description,
                    published_at=published_at,
                    ingested_at=ingested_at,
                )
            )

        if signals:
            logger.info("NewsAPI: %d new articles", len(signals))
        else:
            logger.debug("NewsAPI: 0 new articles")
        return signals


def _parse_published(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, AttributeError):
        return None
