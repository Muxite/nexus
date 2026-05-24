"""Tests for app/stages/link.py — the event → market linker."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import tempfile
from pathlib import Path

import numpy as np
import pytest

import app.main  # noqa: F401
import app.stages.dedupe  # noqa: F401
import app.stages.eventize  # noqa: F401
import app.stages.extract  # noqa: F401
import app.stages.ingest  # noqa: F401
import app.stages.link  # noqa: F401 — registers market_links DDL
import app.stages.markets  # noqa: F401
import app.stages.match  # noqa: F401
import app.stages.resolve  # noqa: F401
import app.stages.trade  # noqa: F401
import app.ai  # noqa: F401
from app.db import DB
from app.deps import StageDeps
from app.stages.link import (
    LinkerStage,
    deadline_fit_score,
    directness_score,
    final_score,
    novelty_score,
    resolution_fit_score,
)
from app.topics import TOPIC_MARKET_LINKS_UPDATED


# ---------------------------------------------------------------------------
# Pure scoring math
# ---------------------------------------------------------------------------


def test_directness_with_overlap_full_cosine():
    v = np.array([1.0, 0.0], dtype=np.float32)
    assert directness_score(v, v, entity_overlap=True) == pytest.approx(1.0)


def test_directness_without_overlap_half_cosine():
    v = np.array([1.0, 0.0], dtype=np.float32)
    assert directness_score(v, v, entity_overlap=False) == pytest.approx(0.5)


def test_directness_clamps_negative_cosine_to_zero():
    v1 = np.array([1.0, 0.0], dtype=np.float32)
    v2 = np.array([-1.0, 0.0], dtype=np.float32)
    assert directness_score(v1, v2, entity_overlap=True) == 0.0


def test_novelty_at_one_is_max():
    assert novelty_score(1) == 1.0


def test_novelty_decays_with_log():
    s_3 = novelty_score(3)
    s_10 = novelty_score(10)
    s_100 = novelty_score(100)
    assert 0.0 < s_100 < s_10 < s_3 < 1.0


def test_deadline_fit_near_deadline_is_one():
    df = deadline_fit_score("2026-05-20T00:00:00+00:00", "2026-05-21T00:00:00+00:00")
    assert df == pytest.approx(1.0 - 1.0/30.0, rel=1e-3)


def test_deadline_fit_far_out_is_zero():
    df = deadline_fit_score("2026-05-20T00:00:00+00:00", "2026-12-31T00:00:00+00:00")
    assert df == 0.0


def test_deadline_fit_past_deadline_is_one():
    df = deadline_fit_score("2026-05-20T00:00:00+00:00", "2026-05-15T00:00:00+00:00")
    assert df == 1.0


def test_deadline_fit_unknown_market_end_is_neutral():
    df = deadline_fit_score("2026-05-20T00:00:00+00:00", None)
    assert df == 0.5


def test_resolution_fit_table_values():
    assert resolution_fit_score("ELECTION") > resolution_fit_score("UNCLASSIFIED")
    assert resolution_fit_score("REGULATORY_DECISION") == 0.9
    assert resolution_fit_score("UNCLASSIFIED") == 0.5
    assert resolution_fit_score("UNKNOWN_TYPE") == 0.5


def test_final_score_is_mean_of_four():
    assert final_score(1.0, 1.0, 1.0, 1.0) == pytest.approx(1.0)
    assert final_score(0.5, 0.5, 0.5, 0.5) == pytest.approx(0.5)
    assert final_score(0.4, 0.6, 0.8, 0.2) == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# Stage smoke tests (small ephemeral DB)
# ---------------------------------------------------------------------------


@pytest.fixture
def irene_db_path():
    tmp = tempfile.NamedTemporaryFile(prefix="test_link_", suffix=".db", delete=False)
    tmp.close()
    Path(tmp.name).unlink()
    asyncio.run(_bootstrap(tmp.name))
    yield tmp.name
    Path(tmp.name).unlink(missing_ok=True)


async def _bootstrap(path: str) -> None:
    db = DB(path=path)
    await db.connect()
    await db.close()


class _StubBus:
    def __init__(self):
        self.published: list[tuple[str, dict]] = []
    async def publish(self, topic, payload):
        self.published.append((topic, payload))
    def subscribe(self, topic, handler): pass
    async def start(self): pass
    async def stop(self): pass


class _StubConfig:
    pass


class _FakeEmbedder:
    embedder_id = "fake@4"
    def embed(self, text):
        v = np.zeros(4, dtype=np.float32)
        for w in text.split():
            v[hash(w) % 4] += 1.0
        n = np.linalg.norm(v)
        return (v / n).astype(np.float32) if n > 0 else v


def _seed_markets(sync_conn, *, n=3):
    """Seed a few markets with hand-built embeddings + questions."""
    # Embeddings designed so test events match cleanly.
    vecs = {
        "iran_market": np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32),
        "election_market": np.array([0.0, 1.0, 0.0, 0.0], dtype=np.float32),
        "earnings_market": np.array([0.0, 0.0, 1.0, 0.0], dtype=np.float32),
    }
    sync_conn.execute(
        "INSERT INTO markets (condition_id, question, embedding, embedder_id, "
        "embedding_dim, active, first_seen_at, updated_at, end_date) "
        "VALUES ('cond-iran', 'Will Trump strike Iran?', ?, 'fake@4', 4, 1, "
        "'2026-05-20T00:00:00+00:00', '2026-05-20T00:00:00+00:00', '2026-06-01T00:00:00+00:00')",
        (vecs["iran_market"].tobytes(),),
    )
    sync_conn.execute(
        "INSERT INTO markets (condition_id, question, embedding, embedder_id, "
        "embedding_dim, active, first_seen_at, updated_at, end_date) "
        "VALUES ('cond-elec', 'Who wins the election?', ?, 'fake@4', 4, 1, "
        "'2026-05-20T00:00:00+00:00', '2026-05-20T00:00:00+00:00', '2026-11-01T00:00:00+00:00')",
        (vecs["election_market"].tobytes(),),
    )
    sync_conn.execute(
        "INSERT INTO markets (condition_id, question, embedding, embedder_id, "
        "embedding_dim, active, first_seen_at, updated_at, end_date) "
        "VALUES ('cond-earn', 'Apple Q3 earnings beat?', ?, 'fake@4', 4, 1, "
        "'2026-05-20T00:00:00+00:00', '2026-05-20T00:00:00+00:00', '2026-08-01T00:00:00+00:00')",
        (vecs["earnings_market"].tobytes(),),
    )


def _seed_event(sync_conn, *, summary: str, event_type: str = "REGULATORY_DECISION",
                support_count: int = 1) -> int:
    sync_conn.execute(
        "INSERT INTO cycles (name, opened_at) VALUES ('c', '2026-05-20T00:00:00+00:00')"
    )
    sync_conn.execute(
        "INSERT INTO articles (source, url, title, ingested_at, cycle_id, published_at) "
        "VALUES ('test', 'http://a/1', 'A', '2026-05-20T00:00:00+00:00', 1, "
        "'2026-05-20T12:00:00+00:00')"
    )
    sync_conn.execute(
        """
        INSERT INTO events
            (event_type, status, canonical_summary, support_count,
             first_seen_at, last_seen_at, cycle_id, eventize_version)
        VALUES (?, 'emerging', ?, ?, '2026-05-20T12:00:00+00:00',
                '2026-05-20T12:00:00+00:00', 1, 'v1')
        """,
        (event_type, summary, support_count),
    )
    eid = sync_conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    # Need a fact tied to article 1 with event_id = eid so the linker's
    # query finds the event.
    sync_conn.execute(
        "INSERT INTO entities (entity_type, canonical_name, first_seen_at, last_seen_at) "
        "VALUES ('ORG', 'Test', '2026-05-20T00:00:00+00:00', '2026-05-20T00:00:00+00:00')"
    )
    sync_conn.execute(
        """
        INSERT INTO extraction_runs
            (article_id, model, prompt_version, ontology_version, status,
             fact_count, entity_count, dropped_fact_count, cycle_id, created_at)
        VALUES (1, 'm', 'v3', 'v1', 'ok', 1, 1, 0, 1, '2026-05-20T00:00:00+00:00')
        """
    )
    sync_conn.execute(
        """
        INSERT INTO facts
            (extraction_run_id, article_id, subj_entity_id, relation,
             confidence, observed_at, evidence_span, cycle_id, event_id)
        VALUES (1, 1, 1, 'decides', 1.0, '2026-05-20T12:00:00+00:00',
                'span', 1, ?)
        """,
        (eid,),
    )
    return eid


@pytest.mark.asyncio
async def test_start_fails_without_markets(irene_db_path):
    """If the markets table is empty, start() must raise — otherwise
    every score is silently zero."""
    db = DB(path=irene_db_path)
    await db.connect()
    deps = StageDeps(db=db, bus=_StubBus(), config=_StubConfig(),
                     embedder=_FakeEmbedder())
    stage = LinkerStage(deps)
    try:
        with pytest.raises(RuntimeError, match="no active markets"):
            await stage.start()
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_score_event_against_markets_picks_top_k(irene_db_path):
    sync = sqlite3.connect(irene_db_path, isolation_level=None)
    _seed_markets(sync)
    # Event whose embedding matches the Iran market.
    eid = _seed_event(sync, summary="Trump decides Iran strike")
    sync.close()

    db = DB(path=irene_db_path)
    await db.connect()
    deps = StageDeps(db=db, bus=_StubBus(), config=_StubConfig(),
                     embedder=_FakeEmbedder())
    stage = LinkerStage(deps)
    await stage.start()
    try:
        await stage.handle({"article_id": 1, "event_id": eid})
        # Some links should land.
        cur = await db.conn.execute(
            "SELECT COUNT(*) FROM market_links WHERE event_id = ?", (eid,),
        )
        n = (await cur.fetchone())[0]
        await cur.close()
        assert n >= 1
    finally:
        await stage.stop()
        await db.close()


@pytest.mark.asyncio
async def test_publishes_market_links_updated(irene_db_path):
    sync = sqlite3.connect(irene_db_path, isolation_level=None)
    _seed_markets(sync)
    eid = _seed_event(sync, summary="Trump decides Iran strike")
    sync.close()

    db = DB(path=irene_db_path)
    await db.connect()
    bus = _StubBus()
    deps = StageDeps(db=db, bus=bus, config=_StubConfig(),
                     embedder=_FakeEmbedder())
    stage = LinkerStage(deps)
    await stage.start()
    try:
        await stage.handle({"article_id": 1, "event_id": eid})
        # Filter to TOPIC_MARKET_LINKS_UPDATED only.
        msgs = [m for m in bus.published if m[0] == TOPIC_MARKET_LINKS_UPDATED]
        assert len(msgs) >= 1
        topic, payload = msgs[0]
        assert payload["event_id"] == eid
        assert payload["links_written"] >= 1
        assert payload["top_final_score"] > 0.0
    finally:
        await stage.stop()
        await db.close()


@pytest.mark.asyncio
async def test_idempotent_re_link(irene_db_path):
    """Re-running on the same article doesn't duplicate market_links rows."""
    sync = sqlite3.connect(irene_db_path, isolation_level=None)
    _seed_markets(sync)
    eid = _seed_event(sync, summary="Trump decides Iran strike")
    sync.close()

    db = DB(path=irene_db_path)
    await db.connect()
    deps = StageDeps(db=db, bus=_StubBus(), config=_StubConfig(),
                     embedder=_FakeEmbedder())
    stage = LinkerStage(deps)
    await stage.start()
    try:
        await stage.handle({"article_id": 1, "event_id": eid})
        cur = await db.conn.execute("SELECT COUNT(*) FROM market_links")
        n_first = (await cur.fetchone())[0]
        await cur.close()
        await stage.handle({"article_id": 1, "event_id": eid})
        cur = await db.conn.execute("SELECT COUNT(*) FROM market_links")
        n_second = (await cur.fetchone())[0]
        await cur.close()
        assert n_first == n_second
    finally:
        await stage.stop()
        await db.close()


def test_market_links_in_forbidden_lint_for_now():
    """Until the dual-path trader (chunk 3) lands, the trade path must
    not read market_links. Once chunk 3 wires up `decision_path='dkg'`
    this assertion is expected to flip."""
    from scripts.lint.no_facts_in_trade_path import FORBIDDEN_TABLES
    # NOTE: market_links is NOT currently in the forbidden list because
    # this is the bridge from DKG to the trade path. The trade-path
    # lint scope was last reviewed in P3 ch2a. Keep this test as a
    # tripwire that flags the decision when someone tries to add it.
    assert "market_links" not in FORBIDDEN_TABLES


# ---------------------------------------------------------------------------
# Live-bus integration (P3 chunk 3 workstream A)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_link_live_bus_on_event_updated(irene_db_path):
    """End-to-end via the live InProcBus: publishing
    `TOPIC_EVENT_UPDATED` makes the subscribed `LinkerStage` score the
    event against active markets and write `market_links` rows. Also
    asserts the downstream `TOPIC_MARKET_LINKS_UPDATED` publication so
    the dkg trader (workstream B) has the signal it'll later
    subscribe to."""
    from app.bus import InProcBus
    from app.topics import TOPIC_EVENT_UPDATED

    sync = sqlite3.connect(irene_db_path, isolation_level=None)
    _seed_markets(sync)
    eid = _seed_event(sync, summary="Trump decides Iran strike")
    sync.close()

    db = DB(path=irene_db_path)
    await db.connect()
    bus = InProcBus()
    deps = StageDeps(db=db, bus=bus, config=_StubConfig(),
                     embedder=_FakeEmbedder())
    stage = LinkerStage(deps)

    received: list[dict] = []

    async def _downstream(payload: dict) -> None:
        received.append(payload)

    # Subscriptions BEFORE bus.start() — same invariant main.py enforces.
    bus.subscribe(TOPIC_EVENT_UPDATED, stage.handle)
    bus.subscribe(TOPIC_MARKET_LINKS_UPDATED, _downstream)

    await stage.start()
    await bus.start()
    try:
        await bus.publish(
            TOPIC_EVENT_UPDATED,
            {
                "extraction_run_id": None,
                "article_id": 1,
                "events_created": 1,
                "events_confirmed": 0,
                "is_tracer": False,
                "tracer_id": None,
            },
        )
        for _ in range(50):
            if received:
                break
            await asyncio.sleep(0.02)
        assert received, "linker did not publish market_links.updated on the live bus"
        payload = received[0]
        assert payload["event_id"] == eid
        assert payload["links_written"] >= 1
        cur = await db.conn.execute(
            "SELECT COUNT(*) FROM market_links WHERE event_id = ?", (eid,),
        )
        n_links = (await cur.fetchone())[0]
        await cur.close()
        assert n_links >= 1
        assert bus.counters.handler_exceptions == 0
    finally:
        await bus.stop()
        await stage.stop()
        await db.close()


def test_wire_pipeline_subscribes_link_to_event_updated():
    """The wiring contract: `app.main.wire_pipeline()` must register
    `LinkerStage.handle` on `TOPIC_EVENT_UPDATED`. Source-level
    assertion so the test is robust to runtime config — main.py is the
    single point that flips link from batch-only to live."""
    from pathlib import Path
    src = (Path(__file__).resolve().parent.parent.parent.parent
           / "app" / "main.py").read_text()
    assert "bus.subscribe(TOPIC_EVENT_UPDATED, link.handle)" in src
