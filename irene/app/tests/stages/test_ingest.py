"""IngestStage tests — DDL registration, the happy poll path, idempotency on
re-ingest of the same URL, HTTP-error resilience, cycle stamping, and the
poller's `handle()` contract."""

from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, Optional

import httpx
import pytest

# Importing app.stages.ingest registers the articles DDL.
import app.stages.ingest  # noqa: F401
# main.py registers the cycles DDL; importing it is needed before db.connect().
import app.main  # noqa: F401
from app.config import Config, IngestSection, IreneSection, ScraperSection
from app.cycle import cycle_scope
from app.db import DB
from app.deps import StageDeps
from app.stages.ingest import IngestStage
from app.topics import TOPIC_ARTICLE_INGESTED


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeBus:
    """Minimal Bus-protocol stand-in for unit tests."""

    def __init__(self) -> None:
        self.published: list[tuple[str, dict]] = []

    async def publish(self, topic: str, payload: dict) -> None:
        self.published.append((topic, payload))

    def subscribe(self, topic: str, handler: Callable[[dict], Awaitable[None]]) -> None:
        pass

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


def _make_config(
    feeds: list[str] | None = None,
    poll_seconds: int = 60,
    html_sites: list[str] | None = None,
    html_poll_seconds: int = 300,
) -> Config:
    return Config(
        irene=IreneSection(),
        scraper=ScraperSection(url="http://scraper-v2:9001", timeout_seconds=5.0),
        ingest=IngestSection(
            feeds=feeds or [],
            poll_seconds=poll_seconds,
            html_sites=html_sites or [],
            html_poll_seconds=html_poll_seconds,
        ),
    )


def _item(url: str, title: str = "T", summary: str | None = "S",
          published_at: str | None = "2026-05-23T12:00:00+00:00") -> dict[str, Any]:
    return {"url": url, "title": title, "summary": summary, "published_at": published_at}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def db_ready():
    db = DB.ephemeral()
    await db.connect()
    try:
        yield db
    finally:
        await db.close()


@pytest.fixture
def bus() -> _FakeBus:
    return _FakeBus()


def _make_stage(
    db: DB,
    bus: _FakeBus,
    feeds: list[str] | None = None,
    html_sites: list[str] | None = None,
) -> IngestStage:
    deps = StageDeps(
        db=db, bus=bus,
        config=_make_config(feeds=feeds, html_sites=html_sites),
    )
    return IngestStage(deps)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_articles_table_exists_after_connect(db_ready):
    cur = await db_ready.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='articles'"
    )
    assert (await cur.fetchone()) is not None
    cur = await db_ready.conn.execute("PRAGMA table_info(articles)")
    cols = {row["name"] for row in await cur.fetchall()}
    expected = {
        "id", "source", "source_detail", "url", "title", "summary",
        "published_at", "ingested_at", "cycle_id", "is_tracer", "tracer_id",
    }
    assert expected.issubset(cols)


async def test_handle_raises_not_implemented(db_ready, bus):
    stage = _make_stage(db_ready, bus)
    with pytest.raises(NotImplementedError):
        await stage.handle({"any": "payload"})


async def test_poll_feed_happy_path_inserts_and_publishes(db_ready, bus, monkeypatch):
    stage = _make_stage(db_ready, bus, feeds=["https://example.com/rss"])

    async def stub_fetch(self, feed_url: str) -> Optional[list[dict[str, Any]]]:
        assert feed_url == "https://example.com/rss"
        return [
            _item("https://example.com/a1"),
            _item("https://example.com/a2"),
            _item("https://example.com/a3"),
        ]

    monkeypatch.setattr(IngestStage, "_fetch_feed", stub_fetch)

    with cycle_scope(1):
        await stage._poll_feed("https://example.com/rss")

    cur = await db_ready.conn.execute("SELECT COUNT(*) FROM articles")
    assert (await cur.fetchone())[0] == 3
    assert len(bus.published) == 3
    topics = {t for t, _ in bus.published}
    assert topics == {TOPIC_ARTICLE_INGESTED}
    urls = {p["url"] for _, p in bus.published}
    assert urls == {
        "https://example.com/a1",
        "https://example.com/a2",
        "https://example.com/a3",
    }
    for _, payload in bus.published:
        assert payload["is_tracer"] is False
        assert payload["tracer_id"] is None
        assert isinstance(payload["article_id"], int)


async def test_repeat_url_is_idempotent(db_ready, bus, monkeypatch):
    stage = _make_stage(db_ready, bus, feeds=["https://example.com/rss"])

    async def stub_fetch(self, feed_url: str) -> Optional[list[dict[str, Any]]]:
        return [_item("https://example.com/same")]

    monkeypatch.setattr(IngestStage, "_fetch_feed", stub_fetch)

    with cycle_scope(1):
        await stage._poll_feed("https://example.com/rss")
        await stage._poll_feed("https://example.com/rss")

    cur = await db_ready.conn.execute("SELECT COUNT(*) FROM articles")
    assert (await cur.fetchone())[0] == 1
    assert len(bus.published) == 1


async def test_http_error_skips_feed_no_crash(db_ready, bus, monkeypatch, caplog):
    """A real httpx.ConnectError out of the client must be swallowed inside
    `_fetch_feed`. The poll loop continues; no rows inserted, no publishes."""
    stage = _make_stage(db_ready, bus, feeds=["https://example.com/rss"])
    # Force start() to create the real client, then swap the .get coroutine
    # so it raises like a transient scraper outage.
    await stage.start()
    try:
        async def boom(*args, **kwargs):
            raise httpx.ConnectError("connection refused")

        monkeypatch.setattr(stage._client, "get", boom)

        with caplog.at_level(logging.WARNING, logger="app.stages.ingest"):
            with cycle_scope(1):
                await stage._poll_feed("https://example.com/rss")

        cur = await db_ready.conn.execute("SELECT COUNT(*) FROM articles")
        assert (await cur.fetchone())[0] == 0
        assert bus.published == []
        # At least one WARNING entry mentioning the feed URL.
        assert any("example.com/rss" in r.message for r in caplog.records)
    finally:
        await stage.stop()


async def test_cycle_id_stamped_on_inserted_row(db_ready, bus, monkeypatch):
    stage = _make_stage(db_ready, bus, feeds=["https://example.com/rss"])

    async def stub_fetch(self, feed_url: str) -> Optional[list[dict[str, Any]]]:
        return [_item("https://example.com/cycle-check")]

    monkeypatch.setattr(IngestStage, "_fetch_feed", stub_fetch)

    with cycle_scope(42):
        await stage._poll_feed("https://example.com/rss")

    cur = await db_ready.conn.execute(
        "SELECT cycle_id, is_tracer, tracer_id, source, source_detail "
        "FROM articles WHERE url=?",
        ("https://example.com/cycle-check",),
    )
    row = await cur.fetchone()
    assert row is not None
    assert row["cycle_id"] == 42
    assert row["is_tracer"] == 0
    assert row["tracer_id"] is None
    assert row["source"] == "rss"
    assert row["source_detail"] == "https://example.com/rss"


async def test_item_missing_url_or_title_is_skipped(db_ready, bus, monkeypatch):
    stage = _make_stage(db_ready, bus, feeds=["https://example.com/rss"])

    async def stub_fetch(self, feed_url: str) -> Optional[list[dict[str, Any]]]:
        return [
            {"title": "no url"},
            {"url": "https://example.com/no-title"},
            _item("https://example.com/good"),
        ]

    monkeypatch.setattr(IngestStage, "_fetch_feed", stub_fetch)

    with cycle_scope(1):
        await stage._poll_feed("https://example.com/rss")

    cur = await db_ready.conn.execute("SELECT url FROM articles")
    rows = [r["url"] for r in await cur.fetchall()]
    assert rows == ["https://example.com/good"]
    assert len(bus.published) == 1


async def test_html_path_inserts_articles_with_source_html(db_ready, bus, monkeypatch):
    """Cycle 4: /scrape mode=links returns LinkItem rows; HTML watcher
    converts them to source='html' article rows."""
    stage = _make_stage(db_ready, bus, html_sites=["https://apnews.com"])

    async def stub_fetch(self, site_url: str):
        assert site_url == "https://apnews.com"
        return [
            {"url": "https://apnews.com/a", "text": "Senate passes spending bill in late-night vote"},
            {"url": "https://apnews.com/b", "text": "Fed signals possible rate cut by year-end"},
        ]

    monkeypatch.setattr(IngestStage, "_fetch_html_links", stub_fetch)

    with cycle_scope(1):
        await stage._poll_html_site("https://apnews.com")

    cur = await db_ready.conn.execute(
        "SELECT source, source_detail, title FROM articles ORDER BY id"
    )
    rows = await cur.fetchall()
    await cur.close()
    assert len(rows) == 2
    assert all(r["source"] == "html" for r in rows)
    assert all(r["source_detail"] == "https://apnews.com" for r in rows)
    assert rows[0]["title"].startswith("Senate passes")
    # Bus publishes one article.ingested per row.
    assert len(bus.published) == 2


async def test_html_path_filters_boilerplate_titles(db_ready, bus, monkeypatch):
    """Boilerplate UI strings ('Subscribe', short links) MUST be skipped."""
    stage = _make_stage(db_ready, bus, html_sites=["https://apnews.com"])

    async def stub_fetch(self, site_url: str):
        return [
            {"url": "https://apnews.com/sub",   "text": "Subscribe"},                     # blocked: blocklist
            {"url": "https://apnews.com/short", "text": "Hi"},                            # blocked: too short
            {"url": "https://apnews.com/nav",   "text": "FT Leaders Academy"},            # blocked: 30-char floor
            # URL pattern blocks (titles look real but URL is a non-article path):
            {"url": "https://www.nytimes.com/crosswords/abc", "text": "The Crossword Get clued in with wordplay every day"},
            {"url": "https://www.nytimes.com/podcasts/ezra-klein", "text": "Listen to the latest podcast episodes here on demand"},
            {"url": "https://www.example.com/account/settings",  "text": "Manage your account settings and notification preferences"},
            {"url": "https://apnews.com/real",  "text": "Russia launches new attack on Kyiv overnight"},
            {"url": "https://apnews.com/menu",  "text": "Menu"},                          # blocked: blocklist
        ]

    monkeypatch.setattr(IngestStage, "_fetch_html_links", stub_fetch)

    with cycle_scope(1):
        await stage._poll_html_site("https://apnews.com")

    cur = await db_ready.conn.execute("SELECT title FROM articles")
    rows = await cur.fetchall()
    await cur.close()
    titles = [r["title"] for r in rows]
    assert len(titles) == 1
    assert titles[0].startswith("Russia launches")


async def test_html_path_handles_429_cooldown_gracefully(db_ready, bus, monkeypatch, caplog):
    """When scraper-v2 returns 429 (host cooldown), log + skip — no crash."""
    stage = _make_stage(db_ready, bus, html_sites=["https://apnews.com"])

    class _Resp:
        status_code = 429
        def raise_for_status(self): pass
        def json(self): return {}

    class _Client:
        async def post(self, url, json):
            return _Resp()

    stage._client = _Client()  # type: ignore[assignment]

    with cycle_scope(1):
        result = await stage._fetch_html_links("https://apnews.com")
    assert result is None
    # No articles inserted, no bus publishes.
    cur = await db_ready.conn.execute("SELECT COUNT(*) FROM articles")
    assert (await cur.fetchone())[0] == 0
    assert bus.published == []


async def test_html_path_skips_response_with_wrong_kind(db_ready, bus, monkeypatch):
    """Defensive: if /scrape returns kind='empty' or 'article' (not 'links')
    we drop the response — link extraction is only valid for kind='links'."""
    stage = _make_stage(db_ready, bus, html_sites=["https://apnews.com"])

    class _Resp:
        status_code = 200
        def raise_for_status(self): pass
        def json(self):
            return {"kind": "empty", "links": []}

    class _Client:
        async def post(self, url, json):
            return _Resp()

    stage._client = _Client()  # type: ignore[assignment]

    with cycle_scope(1):
        result = await stage._fetch_html_links("https://apnews.com")
    assert result is None


async def test_stop_is_idempotent(db_ready, bus):
    stage = _make_stage(db_ready, bus, feeds=[])
    await stage.start()
    await stage.stop()
    await stage.stop()  # second call must not raise
