"""
## Specification

**Purpose.** News ingest stage — polls the shared `scraper:9000` HTTP service
on a fixed cadence, writes new articles to the `articles` table, and publishes
`article.ingested` on the bus so downstream stages (dedupe) can pick them up.

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

**Side effects.** HTTP GETs to `{scraper.url}/feed?url=…`. SQLite writes to
`articles`. Bus publishes on `TOPIC_ARTICLE_INGESTED`.

**Failure modes.**
- `httpx.HTTPError` during a poll: warning logged, this feed skipped, loop
  continues.
- Malformed `/feed` JSON (missing `items` / item missing `url` or `title`):
  the item is skipped with a warning. Other items in the batch still process.
- `cycle.current_cycle_id()` unset: `LookupError` propagates — treat as
  programmer error (main.py must set it before any stage starts).

**Non-goals.**
- Twitter polling (TBD — wired via `/twitter` in a future PR).
- Direct HTML scraping (TBD — the scraper's `/fetch` endpoint may be used by
  downstream dedupe; ingest does not call it).
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


class IngestStage:
    def __init__(self, deps: StageDeps) -> None:
        self._db = deps.db
        self._bus = deps.bus
        self._scraper_url = deps.config.scraper.url.rstrip("/")
        self._scraper_timeout = deps.config.scraper.timeout_seconds
        self._feeds = list(deps.config.ingest.feeds)
        self._poll_seconds = deps.config.ingest.poll_seconds
        self._client: Optional[httpx.AsyncClient] = None
        self._task: Optional[asyncio.Task] = None
        self._stopped = False

    async def start(self) -> None:
        if self._task is not None:
            return
        self._client = httpx.AsyncClient(timeout=self._scraper_timeout)
        self._task = asyncio.create_task(self._run_feed_loop())

    async def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
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
