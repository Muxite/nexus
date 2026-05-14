"""
Async article body fetcher.

Pulls the first ~500 characters of meaningful body text from an article URL
so future scripts can re-classify direction with more context than just the
headline. Fire-and-forget: never blocks the trading pipeline. Failures are
recorded in headline_articles.fetch_error and silently move on.

Design
──────
- Single in-process aiohttp session, bounded by an asyncio.Semaphore (8 concurrent).
- 6s total timeout per URL. Paywalls / Cloudflare-challenged pages just return None.
- HTML parsed by stdlib html.parser; we look for <article>, <main>, or the first
  <p> tag with non-trivial text. No external dep.
- Result is stored to DB by the caller via SeulgiDB.upsert_headline_article.

Usage
─────
    fetcher = ArticleFetcher(db)
    asyncio.create_task(fetcher.fetch_and_store(headline_id, article_url, site_url))
"""

from __future__ import annotations

import asyncio
import logging
from html.parser import HTMLParser
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = aiohttp.ClientTimeout(total=6.0)
_MAX_CONCURRENT = 8
_TARGET_CHARS = 600
_MIN_PARA_CHARS = 80   # ignore nav blurbs and image captions

# Light UA so generic sites don't 403 us. Most paywalled outlets still block
# but at least free sources (Reuters, AP, AlJazeera, Politico, BBC) succeed.
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
}


class _ArticleParser(HTMLParser):
    """
    Walk HTML and accumulate the first long-enough paragraph(s) inside
    <article>, <main>, or — failing those — any top-level <p>.
    """

    _SKIP_TAGS = frozenset({"script", "style", "noscript", "svg", "header", "footer", "nav", "aside"})

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._depth_skip = 0
        self._in_p = 0
        self._in_article = 0
        self._in_main = 0
        self._buf: list[str] = []
        self._paras_article: list[str] = []
        self._paras_main: list[str] = []
        self._paras_any: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag in self._SKIP_TAGS:
            self._depth_skip += 1
            return
        if self._depth_skip:
            return
        if tag == "article":
            self._in_article += 1
        elif tag == "main":
            self._in_main += 1
        elif tag == "p":
            self._in_p += 1
            self._buf = []

    def handle_endtag(self, tag):
        if tag in self._SKIP_TAGS:
            if self._depth_skip:
                self._depth_skip -= 1
            return
        if self._depth_skip:
            return
        if tag == "p" and self._in_p:
            text = " ".join("".join(self._buf).split())
            self._buf = []
            self._in_p -= 1
            if len(text) >= _MIN_PARA_CHARS:
                if self._in_article:
                    self._paras_article.append(text)
                elif self._in_main:
                    self._paras_main.append(text)
                else:
                    self._paras_any.append(text)
        elif tag == "article" and self._in_article:
            self._in_article -= 1
        elif tag == "main" and self._in_main:
            self._in_main -= 1

    def handle_data(self, data):
        if self._depth_skip or not self._in_p:
            return
        self._buf.append(data)

    def best_body(self, target: int) -> Optional[str]:
        for pool in (self._paras_article, self._paras_main, self._paras_any):
            if not pool:
                continue
            joined = " ".join(pool)
            return joined[:target] if len(joined) > target else joined
        return None


class ArticleFetcher:
    def __init__(self, db, target_chars: int = _TARGET_CHARS) -> None:
        self._db = db
        self._target = target_chars
        self._session: Optional[aiohttp.ClientSession] = None
        self._sem = asyncio.Semaphore(_MAX_CONCURRENT)

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(headers=_HEADERS)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def fetch_and_store(
        self, headline_id: str, article_url: str, site_url: Optional[str] = None,
    ) -> None:
        """Fire-and-forget: fetch the article, extract first paragraph(s), persist."""
        if not headline_id or not article_url:
            return
        try:
            body, error = await self._fetch(article_url)
        except Exception as exc:
            body, error = None, f"unexpected:{type(exc).__name__}:{exc}"
        try:
            await self._db.upsert_headline_article(
                headline_id=headline_id,
                article_url=article_url,
                site_url=site_url,
                body=body,
                fetch_error=error,
            )
        except Exception as exc:
            logger.debug("[article] db write failed for %s: %s", headline_id, exc)

    async def _fetch(self, url: str) -> tuple[Optional[str], Optional[str]]:
        async with self._sem:
            session = self._get_session()
            try:
                async with session.get(
                    url, timeout=_DEFAULT_TIMEOUT, allow_redirects=True,
                ) as resp:
                    if resp.status != 200:
                        return None, f"http:{resp.status}"
                    ctype = (resp.headers.get("content-type") or "").lower()
                    if "html" not in ctype and "xml" not in ctype:
                        return None, f"ctype:{ctype[:40]}"
                    # Cap body to 512 KB — articles never need more.
                    raw = await resp.content.read(512 * 1024)
                    html = raw.decode(resp.charset or "utf-8", errors="replace")
            except asyncio.TimeoutError:
                return None, "timeout"
            except aiohttp.ClientError as exc:
                return None, f"client:{type(exc).__name__}"
        parser = _ArticleParser()
        try:
            parser.feed(html)
        except Exception as exc:
            return None, f"parse:{type(exc).__name__}"
        body = parser.best_body(self._target)
        return (body, None) if body else (None, "no_paragraph")
