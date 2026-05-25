"""
## Specification

**Purpose.** News ingest stage — polls the shared scraper-v2 HTTP service
on a fixed cadence, writes new articles to the `articles` table, and publishes
`article.ingested` on the bus so downstream stages (dedupe) can pick them up.

**Two ingestion paths (cycle 4).**
- RSS feeds via `/feed` (shape-compatible across v1 and v2). Source tag `rss`.
- HTML homepages via `/scrape` mode=`links` (v2 only). The endpoint
  returns the boilerplate-stripped headline link-set for a site so we
  can pick up Reuters/AP/WaPo/etc. whose RSS is dead. Source tag `html`.

**Public interface.**
- `IngestStage`:
  - `IngestStage(deps: StageDeps)` — pure assembly; reads
    `deps.config.scraper.*` and `deps.config.ingest.*` and stashes the db /
    bus refs.
  - `await stage.start()` — launches one background poll task; spreads the
    initial fires with a random jitter (seulgi lesson: synchronised feed
    polls thunder).
  - `await stage.stop()` — cancels the background task and closes the httpx
    client. Idempotent.
  - `await stage.handle(payload)` — raises `NotImplementedError`. This stage
    is a pure poller; the bus never calls `handle()` on it.

**Owned tables** (`docs/modularization.md` §4): `articles`. DDL is registered
with `db.register_schema()` at import time.

**Bus contract.** Publishes `TOPIC_ARTICLE_INGESTED` per row actually
inserted, payload shape per `docs/p1_contracts.md` §2:
`{article_id: int, url: str, is_tracer: bool, tracer_id: str | None}`. P1 real
ingestion always sets `is_tracer=False` / `tracer_id=None`; the tracer code
path lives in `scripts/tracer/fire.py` (future PR).

**Invariants.**
- Every `articles` row stamps `cycle_id` from `cycle.current_cycle_id()`.
- Dedupe by `url`: `INSERT OR IGNORE` against the UNIQUE constraint; we only
  publish when `cur.rowcount == 1` so a re-poll of the same feed produces
  exactly one bus message per unique URL.
- An HTTP error in a poll cycle is **logged and swallowed**; the loop
  continues. A transient scraper outage must not crash the stage.
- No module-level mutable state. The httpx client and the background task
  are stage-instance attributes.

**Side effects.** HTTP POSTs to `{scraper.url}/feed` and `/scrape`.
SQLite writes to `articles`. Bus publishes on `TOPIC_ARTICLE_INGESTED`.

**Failure modes.**
- `httpx.HTTPError` during a poll: warning logged, this feed/site skipped,
  loop continues.
- Malformed JSON (missing `items` for RSS, `links` for HTML, etc.): the
  payload is skipped with a warning. Other items in the batch still process.
- 429 (host cooldown on /scrape): single warning log, item skipped.
- `cycle.current_cycle_id()` unset: `LookupError` propagates — treat as
  programmer error (main.py must set it before any stage starts).

**Non-goals.**
- Twitter polling (TBD — wired via `/social/twitter` in a future PR).
- Article body extraction (TBD — would call `/scrape mode=article` per
  link; for now we just take the title from the link-set).
- Feed parsing (the scraper does it; we just consume its JSON, so we do NOT
  import `feedparser`).
"""

from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timezone
from typing import Any, Optional

import httpx

from app import db as db_module
from app.cycle import current_cycle_id
from app.deps import StageDeps
from app.topics import TOPIC_ARTICLE_INGESTED

logger = logging.getLogger(__name__)


_INGEST_DDL = [
    """
    CREATE TABLE IF NOT EXISTS articles (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        source          TEXT NOT NULL,
        source_detail   TEXT,
        url             TEXT NOT NULL UNIQUE,
        title           TEXT NOT NULL,
        summary         TEXT,
        published_at    TEXT,
        ingested_at     TEXT NOT NULL,
        cycle_id        INTEGER NOT NULL,
        is_tracer       INTEGER NOT NULL DEFAULT 0,
        tracer_id       TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_articles_ingested_at ON articles(ingested_at)",
    "CREATE INDEX IF NOT EXISTS idx_articles_tracer ON articles(is_tracer) WHERE is_tracer=1",
]
db_module.register_schema("app/stages/ingest.py", _INGEST_DDL)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


_BOILERPLATE_TITLE_BLOCKLIST = {
    "read more", "subscribe", "sign in", "log in", "newsletter",
    "skip to content", "skip to main content", "menu", "search",
    "home", "more", "share", "advertisement", "ad", "ads",
    "see all", "view all", "next", "previous",
}

# URLs whose path matches ANY of these substrings are NOT news articles.
# Catches NYT puzzles/crosswords/games, podcasts, account/help/about
# pages, jobs boards, etc. Match is plain substring, case-insensitive.
_NON_ARTICLE_URL_PATTERNS = (
    "/puzzles/", "/crosswords/", "/crossword/", "/games/", "/wordle",
    "/spelling-bee", "/connections", "/video/", "/videos/", "/podcasts/",
    "/podcast/", "/audio/", "/newsletter", "/newsletters/",
    "/account/", "/accounts/", "/login", "/signin", "/sign-in",
    "/signup", "/sign-up", "/subscribe", "/help/", "/about/",
    "/contact", "/cookies", "/privacy", "/terms", "/jobs/", "/careers/",
    "/store/", "/shop/", "/games", "/cooking/", "/recipes/", "/games?",
)


class IngestStage:
    def __init__(self, deps: StageDeps) -> None:
        self._db = deps.db
        self._bus = deps.bus
        self._scraper_url = deps.config.scraper.url.rstrip("/")
        self._scraper_timeout = deps.config.scraper.timeout_seconds
        self._feeds = list(deps.config.ingest.feeds)
        self._poll_seconds = deps.config.ingest.poll_seconds
        self._html_sites = list(getattr(deps.config.ingest, "html_sites", []) or [])
        self._html_poll_seconds = int(
            getattr(deps.config.ingest, "html_poll_seconds", 300)
        )
        # Cycle 5: combined RSS+HTML source list — polled via /scrape mode=combined.
        self._combined_sources = list(
            getattr(deps.config.ingest, "combined_sources", []) or []
        )
        self._combined_poll_seconds = int(
            getattr(deps.config.ingest, "combined_poll_seconds", 180)
        )
        self._combined_enrich_top_n = int(
            getattr(deps.config.ingest, "combined_enrich_html_top_n", 0)
        )
        self._client: Optional[httpx.AsyncClient] = None
        self._feed_task: Optional[asyncio.Task] = None
        self._html_task: Optional[asyncio.Task] = None
        self._combined_task: Optional[asyncio.Task] = None
        self._stopped = False

    async def start(self) -> None:
        if (
            self._feed_task is not None
            or self._html_task is not None
            or self._combined_task is not None
        ):
            return
        self._client = httpx.AsyncClient(timeout=self._scraper_timeout)
        self._feed_task = asyncio.create_task(self._run_feed_loop())
        # Only start the HTML loop when sites are configured; idle is fine.
        if self._html_sites:
            self._html_task = asyncio.create_task(self._run_html_loop())
        if self._combined_sources:
            self._combined_task = asyncio.create_task(self._run_combined_loop())

    async def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        for task in (self._feed_task, self._html_task, self._combined_task):
            if task is None:
                continue
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._feed_task = None
        self._html_task = None
        self._combined_task = None
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def handle(self, payload: dict) -> None:
        raise NotImplementedError("IngestStage is a poller, not a subscriber")

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _run_feed_loop(self) -> None:
        """One task that round-robins all feeds. Jitters the first fire per
        feed so a startup with N feeds doesn't stampede the scraper."""
        if not self._feeds:
            return
        # Random initial delays per feed.
        last_polled: dict[str, float] = {}
        loop = asyncio.get_running_loop()
        now = loop.time()
        for feed in self._feeds:
            last_polled[feed] = now - random.uniform(0, self._poll_seconds)
        while True:
            for feed in self._feeds:
                if loop.time() - last_polled[feed] < self._poll_seconds:
                    continue
                last_polled[feed] = loop.time()
                try:
                    await self._poll_feed(feed)
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001 — loop must survive any feed failure
                    logger.exception("ingest: unexpected error polling feed %r", feed)
            await asyncio.sleep(1.0)

    async def _poll_feed(self, feed_url: str) -> None:
        items = await self._fetch_feed(feed_url)
        if items is None:
            return
        for item in items:
            await self._ingest_item(source="rss", source_detail=feed_url, item=item)

    async def _fetch_feed(self, feed_url: str) -> Optional[list[dict[str, Any]]]:
        """POST {scraper.url}/feed with `{"url": feed_url}`. Returns the
        items list, or None on any HTTP-layer error (logged, swallowed)."""
        assert self._client is not None
        try:
            resp = await self._client.post(
                f"{self._scraper_url}/feed",
                json={"url": feed_url},
            )
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPError as exc:
            logger.warning(
                "ingest: scraper /feed failed for %r (%s) — skipping",
                feed_url,
                exc.__class__.__name__,
            )
            return None
        except ValueError as exc:  # JSON decode
            logger.warning(
                "ingest: scraper /feed returned non-JSON for %r (%s) — skipping",
                feed_url,
                exc,
            )
            return None
        items = data.get("items") if isinstance(data, dict) else None
        if not isinstance(items, list):
            logger.warning("ingest: scraper /feed missing 'items' for %r", feed_url)
            return None
        return items

    # ---- HTML watcher loop (cycle 4) ----------------------------------

    async def _run_html_loop(self) -> None:
        """Mirror of `_run_feed_loop` but for HTML homepages via /scrape."""
        if not self._html_sites:
            return
        last_polled: dict[str, float] = {}
        loop = asyncio.get_running_loop()
        now = loop.time()
        for site in self._html_sites:
            last_polled[site] = now - random.uniform(0, self._html_poll_seconds)
        while True:
            for site in self._html_sites:
                if loop.time() - last_polled[site] < self._html_poll_seconds:
                    continue
                last_polled[site] = loop.time()
                try:
                    await self._poll_html_site(site)
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001
                    logger.exception("ingest: unexpected error polling site %r", site)
            await asyncio.sleep(1.0)

    async def _poll_html_site(self, site_url: str) -> None:
        links = await self._fetch_html_links(site_url)
        if links is None:
            return
        for link in links:
            url = link.get("url")
            text = link.get("text") or ""
            if not isinstance(url, str) or not url:
                continue
            title = text.strip()
            # Boilerplate filter — three guards:
            # 1. minimum length (30 chars catches short labels)
            # 2. title blocklist (UI strings like "Subscribe")
            # 3. URL pattern blocklist (puzzles/games/podcasts/account/...)
            if len(title) < 30 or title.lower() in _BOILERPLATE_TITLE_BLOCKLIST:
                continue
            url_lower = url.lower()
            if any(pat in url_lower for pat in _NON_ARTICLE_URL_PATTERNS):
                continue
            await self._ingest_item(
                source="html",
                source_detail=site_url,
                item={"url": url, "title": title},
            )

    async def _fetch_html_links(self, site_url: str) -> Optional[list[dict[str, Any]]]:
        """POST {scraper.url}/scrape with `{"url": site, "mode": "links"}`.
        Returns the link-set, or None on any HTTP-layer error."""
        assert self._client is not None
        try:
            resp = await self._client.post(
                f"{self._scraper_url}/scrape",
                json={"url": site_url, "mode": "links"},
            )
            # 429 = scraper-v2 host cooldown; log once and move on.
            if resp.status_code == 429:
                logger.warning(
                    "ingest: scraper /scrape 429 (cooldown) for %r — skipping",
                    site_url,
                )
                return None
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPError as exc:
            logger.warning(
                "ingest: scraper /scrape failed for %r (%s) — skipping",
                site_url,
                exc.__class__.__name__,
            )
            return None
        except ValueError as exc:
            logger.warning(
                "ingest: scraper /scrape returned non-JSON for %r (%s) — skipping",
                site_url,
                exc,
            )
            return None
        if not isinstance(data, dict):
            return None
        # Defensive: only `kind=="links"` carries headline link items.
        if data.get("kind") != "links":
            return None
        links = data.get("links")
        if not isinstance(links, list):
            return None
        return links

    # ---- Combined RSS+HTML loop (cycle 5) -----------------------------

    async def _run_combined_loop(self) -> None:
        """Per-source loop driving /scrape mode=combined. The scraper
        fetches the host's pinned RSS feed (per_host.yaml) and the HTML
        homepage in parallel and returns the merged link-set. Each
        merged entry carries title (always), summary + published_at
        (when RSS contributed) and a `seen_in` list.
        """
        if not self._combined_sources:
            return
        last_polled: dict[str, float] = {}
        loop = asyncio.get_running_loop()
        now = loop.time()
        for source in self._combined_sources:
            last_polled[source] = now - random.uniform(0, self._combined_poll_seconds)
        while True:
            for source in self._combined_sources:
                if loop.time() - last_polled[source] < self._combined_poll_seconds:
                    continue
                last_polled[source] = loop.time()
                try:
                    await self._poll_combined_source(source)
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001
                    logger.exception("ingest: unexpected error polling combined %r", source)
            await asyncio.sleep(1.0)

    async def _poll_combined_source(self, source_url: str) -> None:
        links = await self._fetch_combined_links(source_url)
        if links is None:
            return
        for link in links:
            url = link.get("url")
            text = link.get("text") or ""
            if not isinstance(url, str) or not url:
                continue
            title = text.strip()
            # Boilerplate filter — same triple guard as the HTML loop:
            # length + title blocklist + URL pattern blocklist. The
            # scraper already drops most cruft, but defence in depth.
            if len(title) < 30 or title.lower() in _BOILERPLATE_TITLE_BLOCKLIST:
                continue
            url_lower = url.lower()
            if any(pat in url_lower for pat in _NON_ARTICLE_URL_PATTERNS):
                continue
            summary = link.get("summary") or None
            published = link.get("published") or None
            seen_in = link.get("seen_in") or []
            # source_detail records which sub-strategies surfaced this
            # so chunked decision audits can attribute back to RSS/HTML.
            if isinstance(seen_in, list) and seen_in:
                tag_suffix = "+" + "+".join(sorted(seen_in))
            else:
                tag_suffix = ""
            await self._ingest_item(
                source="combined",
                source_detail=f"{source_url}{tag_suffix}",
                item={
                    "url": url,
                    "title": title,
                    "summary": summary if isinstance(summary, str) else None,
                    "published_at": published if isinstance(published, str) else None,
                },
            )

    async def _fetch_combined_links(
        self, source_url: str
    ) -> Optional[list[dict[str, Any]]]:
        """POST /scrape with mode=combined. Returns the merged link list
        (each item is `{url, text, summary, published, seen_in}`), or
        None on any HTTP-layer error."""
        assert self._client is not None
        try:
            resp = await self._client.post(
                f"{self._scraper_url}/scrape",
                json={
                    "url": source_url,
                    "mode": "combined",
                    "enrich_html_top_n": self._combined_enrich_top_n,
                },
            )
            if resp.status_code == 429:
                logger.warning(
                    "ingest: scraper /scrape mode=combined 429 for %r — skipping",
                    source_url,
                )
                return None
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPError as exc:
            logger.warning(
                "ingest: scraper /scrape combined failed for %r (%s) — skipping",
                source_url,
                exc.__class__.__name__,
            )
            return None
        except ValueError as exc:
            logger.warning(
                "ingest: scraper /scrape combined non-JSON for %r (%s)",
                source_url, exc,
            )
            return None
        if not isinstance(data, dict):
            return None
        # `kind=='empty'` is fine — just no links this tick.
        if data.get("kind") not in ("links", "empty"):
            return None
        links = data.get("links")
        if not isinstance(links, list):
            return None
        return links

    async def _ingest_item(
        self,
        *,
        source: str,
        source_detail: str,
        item: dict[str, Any],
    ) -> None:
        url = item.get("url")
        title = item.get("title")
        if not isinstance(url, str) or not url or not isinstance(title, str) or not title:
            logger.warning("ingest: skipping item with missing url/title from %r", source_detail)
            return
        summary = item.get("summary")
        published_at = item.get("published_at")
        cycle_id = current_cycle_id()
        ingested_at = _now_iso()

        async with self._db.batch() as cur:
            await cur.execute(
                """
                INSERT OR IGNORE INTO articles
                    (source, source_detail, url, title, summary, published_at,
                     ingested_at, cycle_id, is_tracer, tracer_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, NULL)
                """,
                (
                    source,
                    source_detail,
                    url,
                    title,
                    summary if isinstance(summary, str) else None,
                    published_at if isinstance(published_at, str) else None,
                    ingested_at,
                    cycle_id,
                ),
            )
            inserted = cur.rowcount == 1
            article_id: Optional[int] = None
            if inserted:
                # lastrowid is reliable here because we just inserted in this cursor.
                article_id = cur.lastrowid

        if inserted and article_id is not None:
            await self._bus.publish(
                TOPIC_ARTICLE_INGESTED,
                {
                    "article_id": int(article_id),
                    "url": url,
                    "is_tracer": False,
                    "tracer_id": None,
                },
            )


if __name__ == "__main__":
    # Independent runnability per modularization rule 8. Minimal smoke:
    # spins the stage against an in-memory DB and a no-op bus, prints OK.
    import asyncio as _asyncio

    from app.bus import InProcBus
    from app.config import Config
    from app.cycle import set_cycle_id
    from app.db import DB

    async def _smoke() -> None:
        db = DB.ephemeral()
        await db.connect()
        set_cycle_id(1)
        bus = InProcBus()
        await bus.start()
        cfg = Config()
        deps = StageDeps(db=db, bus=bus, config=cfg)
        stage = IngestStage(deps)
        await stage.start()
        await _asyncio.sleep(0.1)
        await stage.stop()
        await bus.stop()
        await db.close()
        print("ingest smoke OK")

    _asyncio.run(_smoke())
