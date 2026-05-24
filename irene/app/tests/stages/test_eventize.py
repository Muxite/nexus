"""Schema-only smoke for the eventize stage stub.

The full eventize tests land alongside the clustering logic in chunk 2
proper. For now we verify: the DDL applies cleanly through the
schema-registry path, the new tables exist, the facts.event_id column
materialises, and the stage's contract (handle raises until research
session lands) holds.
"""

from __future__ import annotations

import asyncio
import sqlite3
import tempfile
from pathlib import Path

import numpy as np
import pytest

import app.main  # noqa: F401
import app.stages.dedupe  # noqa: F401
import app.stages.eventize  # noqa: F401 — registers events / event_edges DDL
import app.stages.extract  # noqa: F401
import app.stages.ingest  # noqa: F401
import app.stages.markets  # noqa: F401
import app.stages.match  # noqa: F401
import app.stages.resolve  # noqa: F401
import app.stages.trade  # noqa: F401
import app.ai  # noqa: F401
from app.db import DB
from app.deps import StageDeps
from app.stages.eventize import EventizeStage
from app.topics import TOPIC_EVENT_UPDATED


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def irene_db_path():
    tmp = tempfile.NamedTemporaryFile(prefix="test_eventize_", suffix=".db", delete=False)
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
    async def publish(self, topic: str, payload: dict) -> None: pass
    def subscribe(self, topic: str, handler) -> None: pass
    async def start(self) -> None: pass
    async def stop(self) -> None: pass


class _StubConfig:
    pass


class _FakeEmbedder:
    embedder_id = "fake@4"
    def embed(self, text: str) -> np.ndarray:
        v = np.zeros(4, dtype=np.float32)
        v[abs(hash(text)) % 4] = 1.0
        return v


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def test_events_table_created(irene_db_path):
    conn = sqlite3.connect(irene_db_path)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(events)")]
    assert "id" in cols
    assert "event_type" in cols
    assert "status" in cols
    assert "canonical_summary" in cols
    assert "novelty_z" in cols
    assert "support_count" in cols
    conn.close()


def test_event_edges_table_created(irene_db_path):
    conn = sqlite3.connect(irene_db_path)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(event_edges)")]
    assert "from_event_id" in cols
    assert "to_event_id" in cols
    assert "edge_kind" in cols
    conn.close()


def test_events_status_check_constraint(irene_db_path):
    conn = sqlite3.connect(irene_db_path)
    # Valid status.
    conn.execute(
        "INSERT INTO events (event_type, status, first_seen_at, last_seen_at, cycle_id) "
        "VALUES ('REGULATORY', 'emerging', '2026-05-24T00:00:00+00:00', '2026-05-24T00:00:00+00:00', 1)"
    )
    # Invalid status should raise.
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO events (event_type, status, first_seen_at, last_seen_at, cycle_id) "
            "VALUES ('X', 'made_up_status', '2026-05-24T00:00:00+00:00', '2026-05-24T00:00:00+00:00', 1)"
        )
    conn.close()


def test_event_edges_edge_kind_check(irene_db_path):
    conn = sqlite3.connect(irene_db_path)
    conn.execute(
        "INSERT INTO events (event_type, status, first_seen_at, last_seen_at, cycle_id) "
        "VALUES ('A', 'emerging', '2026-05-24T00:00:00+00:00', '2026-05-24T00:00:00+00:00', 1)"
    )
    conn.execute(
        "INSERT INTO events (event_type, status, first_seen_at, last_seen_at, cycle_id) "
        "VALUES ('B', 'emerging', '2026-05-24T00:00:00+00:00', '2026-05-24T00:00:00+00:00', 1)"
    )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO event_edges (from_event_id, to_event_id, edge_kind, "
            "confidence, observed_at) "
            "VALUES (1, 2, 'unknown_kind', 0.5, '2026-05-24T00:00:00+00:00')"
        )
    conn.close()


def test_event_edges_unique_constraint(irene_db_path):
    conn = sqlite3.connect(irene_db_path)
    conn.execute(
        "INSERT INTO events (event_type, status, first_seen_at, last_seen_at, cycle_id) "
        "VALUES ('A', 'emerging', '2026-05-24T00:00:00+00:00', '2026-05-24T00:00:00+00:00', 1)"
    )
    conn.execute(
        "INSERT INTO events (event_type, status, first_seen_at, last_seen_at, cycle_id) "
        "VALUES ('B', 'emerging', '2026-05-24T00:00:00+00:00', '2026-05-24T00:00:00+00:00', 1)"
    )
    conn.execute(
        "INSERT INTO event_edges (from_event_id, to_event_id, edge_kind, "
        "confidence, observed_at) "
        "VALUES (1, 2, 'confirms', 0.9, '2026-05-24T00:00:00+00:00')"
    )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO event_edges (from_event_id, to_event_id, edge_kind, "
            "confidence, observed_at) "
            "VALUES (1, 2, 'confirms', 0.5, '2026-05-24T00:00:00+00:00')"
        )
    conn.close()


@pytest.mark.asyncio
async def test_facts_event_id_column_present(irene_db_path):
    """EventizeStage.start() ensures facts.event_id exists (idempotent
    column-add for DBs that bootstrapped before this DDL was registered)."""
    db = DB(path=irene_db_path)
    await db.connect()
    deps = StageDeps(db=db, bus=_StubBus(), config=_StubConfig(),
                     embedder=_FakeEmbedder())
    stage = EventizeStage(deps)
    await stage.start()
    cur = await db.conn.execute("PRAGMA table_info(facts)")
    cols = [r[1] async for r in cur]
    await cur.close()
    await db.close()
    assert "event_id" in cols


# ---------------------------------------------------------------------------
# Stage contract
# ---------------------------------------------------------------------------


def test_event_updated_topic_constant():
    assert TOPIC_EVENT_UPDATED == "event.updated"


def test_eventize_requires_embedder(irene_db_path):
    async def go():
        db = DB(path=irene_db_path)
        await db.connect()
        deps = StageDeps(db=db, bus=_StubBus(), config=_StubConfig(), embedder=None)
        try:
            with pytest.raises(RuntimeError, match="embedder"):
                EventizeStage(deps)
        finally:
            await db.close()
    asyncio.run(go())


@pytest.mark.asyncio
async def test_handle_raises_not_implemented_until_research_lands(irene_db_path):
    """Documents the contract-first stub: until the research session
    deliverable lands, handle() must error rather than silently no-op
    and accumulate stale state."""
    db = DB(path=irene_db_path)
    await db.connect()
    deps = StageDeps(db=db, bus=_StubBus(), config=_StubConfig(),
                     embedder=_FakeEmbedder())
    stage = EventizeStage(deps)
    await stage.start()
    try:
        with pytest.raises(NotImplementedError, match="research-session"):
            await stage.handle({
                "extraction_run_id": 1, "article_id": 1,
                "entities_resolved": 0, "merges": 0,
                "is_tracer": False, "tracer_id": None,
            })
    finally:
        await stage.stop()
        await db.close()


def test_eventize_tables_in_forbidden_lint_list():
    """events + event_edges must be in the trade-path lint's forbidden
    list so we never accidentally read them from the trade path."""
    from scripts.lint.no_facts_in_trade_path import FORBIDDEN_TABLES
    assert "events" in FORBIDDEN_TABLES
    assert "event_edges" in FORBIDDEN_TABLES
