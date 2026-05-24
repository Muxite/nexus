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
async def test_handle_missing_article_id_no_crash(irene_db_path):
    """A malformed payload (missing article_id) is logged and ignored,
    not raised — the bus dispatcher would otherwise lose the whole
    chain on one bad message."""
    db = DB(path=irene_db_path)
    await db.connect()
    deps = StageDeps(db=db, bus=_StubBus(), config=_StubConfig(),
                     embedder=_FakeEmbedder())
    stage = EventizeStage(deps)
    await stage.start()
    try:
        await stage.handle({})  # no article_id
        # No event row should have been written.
        cur = await db.conn.execute("SELECT COUNT(*) FROM events")
        assert (await cur.fetchone())[0] == 0
        await cur.close()
    finally:
        await stage.stop()
        await db.close()


def test_eventize_tables_in_forbidden_lint_list():
    """events + event_edges must be in the trade-path lint's forbidden
    list so we never accidentally read them from the trade path."""
    from scripts.lint.no_facts_in_trade_path import FORBIDDEN_TABLES
    assert "events" in FORBIDDEN_TABLES
    assert "event_edges" in FORBIDDEN_TABLES


# ---------------------------------------------------------------------------
# Real-world clustering behaviour
# ---------------------------------------------------------------------------


class _ScriptedEmbedder:
    """Test embedder that returns hand-controlled vectors keyed by text.
    Falls back to one-hot-by-hash for unknown text."""

    embedder_id = "scripted@4"

    def __init__(self, mapping: dict[str, np.ndarray], dim: int = 4) -> None:
        self._mapping = mapping
        self._dim = dim

    def embed(self, text: str) -> np.ndarray:
        if text in self._mapping:
            return self._mapping[text].astype(np.float32)
        v = np.zeros(self._dim, dtype=np.float32)
        v[abs(hash(text)) % self._dim] = 1.0
        return v

    def embed_batch(self, texts: list[str]) -> np.ndarray:
        return np.stack([self.embed(t) for t in texts])


def _seed_minimal_dkg(conn) -> dict:
    """Seeds: 1 cycle, 2 articles (different published_at), 3 entities
    (FDA, Pfizer, Apple), and the FK glue. Returns the ids."""
    conn.execute(
        "INSERT INTO cycles (name, opened_at) VALUES ('t', '2026-05-23T00:00:00+00:00')"
    )
    conn.execute(
        "INSERT INTO articles (source, url, title, ingested_at, cycle_id, published_at) "
        "VALUES ('test', 'http://a/1', 'A1', '2026-05-23T00:00:00+00:00', 1, "
        "'2026-05-20T12:00:00+00:00')"
    )
    conn.execute(
        "INSERT INTO articles (source, url, title, ingested_at, cycle_id, published_at) "
        "VALUES ('test', 'http://a/2', 'A2', '2026-05-23T00:00:00+00:00', 1, "
        "'2026-05-20T18:00:00+00:00')"
    )
    conn.execute(
        "INSERT INTO entities (entity_type, canonical_name, first_seen_at, last_seen_at) "
        "VALUES ('ORG', 'FDA', '2026-05-20T00:00:00+00:00', '2026-05-20T00:00:00+00:00')"
    )
    conn.execute(
        "INSERT INTO entities (entity_type, canonical_name, first_seen_at, last_seen_at) "
        "VALUES ('COMPANY', 'Pfizer', '2026-05-20T00:00:00+00:00', '2026-05-20T00:00:00+00:00')"
    )
    conn.execute(
        "INSERT INTO entities (entity_type, canonical_name, first_seen_at, last_seen_at) "
        "VALUES ('COMPANY', 'Apple', '2026-05-20T00:00:00+00:00', '2026-05-20T00:00:00+00:00')"
    )
    return {"article1": 1, "article2": 2, "fda": 1, "pfizer": 2, "apple": 3}


def _insert_fact(conn, *, run_id, article_id, subj_id, relation, obj_id, observed_at, obj_value=None):
    conn.execute(
        """
        INSERT INTO facts
            (extraction_run_id, article_id, subj_entity_id, relation,
             obj_entity_id, obj_value, confidence, observed_at,
             evidence_span, cycle_id)
        VALUES (?, ?, ?, ?, ?, ?, 1.0, ?, 'span', 1)
        """,
        (run_id, article_id, subj_id, relation, obj_id, obj_value, observed_at),
    )
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def _insert_extraction_run(conn, *, article_id, cycle_id=1):
    conn.execute(
        """
        INSERT INTO extraction_runs
            (article_id, model, prompt_version, ontology_version, status,
             fact_count, entity_count, dropped_fact_count, cycle_id, created_at)
        VALUES (?, 'm', 'v3', 'v1', 'ok', 1, 1, 0, ?, '2026-05-23T00:00:00+00:00')
        """,
        (article_id, cycle_id),
    )
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


@pytest.mark.asyncio
async def test_handle_creates_event_on_cluster_miss(irene_db_path):
    """First fact in an empty DKG → one new event, fact carries event_id."""
    sync = sqlite3.connect(irene_db_path, isolation_level=None)
    ids = _seed_minimal_dkg(sync)
    run_id = _insert_extraction_run(sync, article_id=ids["article1"])
    _insert_fact(
        sync, run_id=run_id, article_id=ids["article1"],
        subj_id=ids["fda"], relation="decides", obj_id=ids["pfizer"],
        observed_at="2026-05-20T12:00:00+00:00",
    )
    sync.close()

    db = DB(path=irene_db_path)
    await db.connect()
    deps = StageDeps(db=db, bus=_StubBus(), config=_StubConfig(),
                     embedder=_FakeEmbedder())
    stage = EventizeStage(deps)
    await stage.start()
    try:
        await stage.handle({
            "article_id": ids["article1"], "extraction_run_id": run_id,
            "is_tracer": False, "tracer_id": None,
        })
        cur = await db.conn.execute("SELECT COUNT(*) FROM events")
        assert (await cur.fetchone())[0] == 1
        await cur.close()
        cur = await db.conn.execute(
            "SELECT event_id FROM facts WHERE article_id=?", (ids["article1"],),
        )
        row = await cur.fetchone()
        await cur.close()
        assert row[0] is not None
        # Event type lookup: (ORG, decides, COMPANY) → REGULATORY_DECISION
        cur = await db.conn.execute("SELECT event_type FROM events LIMIT 1")
        assert (await cur.fetchone())[0] == "REGULATORY_DECISION"
        await cur.close()
    finally:
        await stage.stop()
        await db.close()


@pytest.mark.asyncio
async def test_handle_confirms_when_clusters_match(irene_db_path):
    """Two facts with overlapping entities + same templated triple →
    one event with support_count=2 + a confirms edge."""
    sync = sqlite3.connect(irene_db_path, isolation_level=None)
    ids = _seed_minimal_dkg(sync)
    run1 = _insert_extraction_run(sync, article_id=ids["article1"])
    _insert_fact(
        sync, run_id=run1, article_id=ids["article1"],
        subj_id=ids["fda"], relation="decides", obj_id=ids["pfizer"],
        observed_at="2026-05-20T12:00:00+00:00",
    )
    run2 = _insert_extraction_run(sync, article_id=ids["article2"])
    _insert_fact(
        sync, run_id=run2, article_id=ids["article2"],
        subj_id=ids["fda"], relation="decides", obj_id=ids["pfizer"],
        observed_at="2026-05-20T18:00:00+00:00",
    )
    sync.close()

    db = DB(path=irene_db_path)
    await db.connect()
    deps = StageDeps(db=db, bus=_StubBus(), config=_StubConfig(),
                     embedder=_FakeEmbedder())
    stage = EventizeStage(deps)
    await stage.start()
    try:
        await stage.handle({"article_id": ids["article1"], "extraction_run_id": run1,
                            "is_tracer": False, "tracer_id": None})
        await stage.handle({"article_id": ids["article2"], "extraction_run_id": run2,
                            "is_tracer": False, "tracer_id": None})
        cur = await db.conn.execute("SELECT COUNT(*), MAX(support_count) FROM events")
        n_events, max_support = await cur.fetchone()
        await cur.close()
        assert n_events == 1
        assert max_support == 2
        # A confirms self-edge should exist.
        cur = await db.conn.execute("SELECT COUNT(*) FROM event_edges WHERE edge_kind='confirms'")
        assert (await cur.fetchone())[0] == 1
        await cur.close()
    finally:
        await stage.stop()
        await db.close()


@pytest.mark.asyncio
async def test_handle_does_not_cluster_without_entity_overlap(irene_db_path):
    """Disjoint entity sets → two events even with identical cosine."""
    sync = sqlite3.connect(irene_db_path, isolation_level=None)
    ids = _seed_minimal_dkg(sync)
    # Add a 4th entity (SEC) so we can build two facts that share NO
    # entities at all.
    sync.execute(
        "INSERT INTO entities (entity_type, canonical_name, first_seen_at, last_seen_at) "
        "VALUES ('ORG', 'SEC', '2026-05-20T00:00:00+00:00', '2026-05-20T00:00:00+00:00')"
    )
    sec_id = sync.execute(
        "SELECT id FROM entities WHERE canonical_name='SEC'"
    ).fetchone()[0]
    run1 = _insert_extraction_run(sync, article_id=ids["article1"])
    _insert_fact(
        sync, run_id=run1, article_id=ids["article1"],
        subj_id=ids["fda"], relation="decides", obj_id=ids["pfizer"],
        observed_at="2026-05-20T12:00:00+00:00",
    )
    run2 = _insert_extraction_run(sync, article_id=ids["article2"])
    _insert_fact(
        sync, run_id=run2, article_id=ids["article2"],
        subj_id=sec_id, relation="decides", obj_id=ids["apple"],
        observed_at="2026-05-20T18:00:00+00:00",
    )
    sync.close()

    db = DB(path=irene_db_path)
    await db.connect()
    # Custom embedder that maps the two triples to the SAME vector so we
    # know the only thing preventing the merge is entity overlap.
    same_vec = np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32)
    emb = _ScriptedEmbedder({
        "FDA decides Pfizer": same_vec,
        "SEC decides Apple": same_vec,
    }, dim=4)
    deps = StageDeps(db=db, bus=_StubBus(), config=_StubConfig(), embedder=emb)
    stage = EventizeStage(deps)
    await stage.start()
    try:
        await stage.handle({"article_id": ids["article1"], "extraction_run_id": run1,
                            "is_tracer": False, "tracer_id": None})
        await stage.handle({"article_id": ids["article2"], "extraction_run_id": run2,
                            "is_tracer": False, "tracer_id": None})
        cur = await db.conn.execute("SELECT COUNT(*) FROM events")
        assert (await cur.fetchone())[0] == 2
        await cur.close()
    finally:
        await stage.stop()
        await db.close()


@pytest.mark.asyncio
async def test_handle_idempotent_on_already_eventized_fact(irene_db_path):
    """Re-firing the same article with a fact whose event_id is set
    creates no new events and no new edges."""
    sync = sqlite3.connect(irene_db_path, isolation_level=None)
    ids = _seed_minimal_dkg(sync)
    run_id = _insert_extraction_run(sync, article_id=ids["article1"])
    _insert_fact(
        sync, run_id=run_id, article_id=ids["article1"],
        subj_id=ids["fda"], relation="decides", obj_id=ids["pfizer"],
        observed_at="2026-05-20T12:00:00+00:00",
    )
    sync.close()

    db = DB(path=irene_db_path)
    await db.connect()
    deps = StageDeps(db=db, bus=_StubBus(), config=_StubConfig(),
                     embedder=_FakeEmbedder())
    stage = EventizeStage(deps)
    await stage.start()
    try:
        await stage.handle({"article_id": ids["article1"], "extraction_run_id": run_id,
                            "is_tracer": False, "tracer_id": None})
        await stage.handle({"article_id": ids["article1"], "extraction_run_id": run_id,
                            "is_tracer": False, "tracer_id": None})
        cur = await db.conn.execute("SELECT COUNT(*) FROM events")
        assert (await cur.fetchone())[0] == 1
        await cur.close()
        cur = await db.conn.execute("SELECT COUNT(*) FROM event_edges")
        assert (await cur.fetchone())[0] == 0  # no second-pass confirm
        await cur.close()
    finally:
        await stage.stop()
        await db.close()


@pytest.mark.asyncio
async def test_handle_publishes_event_updated(irene_db_path):
    sync = sqlite3.connect(irene_db_path, isolation_level=None)
    ids = _seed_minimal_dkg(sync)
    run_id = _insert_extraction_run(sync, article_id=ids["article1"])
    _insert_fact(
        sync, run_id=run_id, article_id=ids["article1"],
        subj_id=ids["fda"], relation="decides", obj_id=ids["pfizer"],
        observed_at="2026-05-20T12:00:00+00:00",
    )
    sync.close()

    class RecordingBus(_StubBus):
        def __init__(self): self.published = []
        async def publish(self, topic, payload): self.published.append((topic, payload))

    db = DB(path=irene_db_path)
    await db.connect()
    bus = RecordingBus()
    deps = StageDeps(db=db, bus=bus, config=_StubConfig(),
                     embedder=_FakeEmbedder())
    stage = EventizeStage(deps)
    await stage.start()
    try:
        await stage.handle({"article_id": ids["article1"], "extraction_run_id": run_id,
                            "is_tracer": False, "tracer_id": None})
        assert len(bus.published) == 1
        topic, payload = bus.published[0]
        assert topic == TOPIC_EVENT_UPDATED
        assert payload["article_id"] == ids["article1"]
        assert payload["events_created"] == 1
        assert payload["events_confirmed"] == 0
    finally:
        await stage.stop()
        await db.close()


# ---------------------------------------------------------------------------
# YAML classification
# ---------------------------------------------------------------------------


def test_classify_org_decides_company_is_regulatory_decision():
    from app.stages.eventize import _classify
    rules = [
        {"subj": "ORG", "relation": "decides", "obj": "COMPANY",
         "type": "REGULATORY_DECISION"},
    ]
    assert _classify(rules, "ORG", "decides", "COMPANY") == "REGULATORY_DECISION"


def test_classify_wildcard_subj():
    from app.stages.eventize import _classify
    rules = [{"subj": "*", "relation": "announces", "obj": "*", "type": "ANNOUNCEMENT"}]
    assert _classify(rules, "PERSON", "announces", "COUNTRY") == "ANNOUNCEMENT"


def test_classify_null_obj_requires_no_obj():
    from app.stages.eventize import _classify
    rules = [{"subj": "PERSON", "relation": "resigns", "obj": None,
              "type": "RESIGNATION"}]
    assert _classify(rules, "PERSON", "resigns", None) == "RESIGNATION"
    # If there's an obj, the rule doesn't match.
    assert _classify(rules, "PERSON", "resigns", "ORG") == "UNCLASSIFIED"


def test_classify_unmatched_returns_unclassified():
    from app.stages.eventize import _classify
    rules = [{"subj": "ORG", "relation": "decides", "obj": "COMPANY",
              "type": "REGULATORY_DECISION"}]
    assert _classify(rules, "PERSON", "decides", "COUNTRY") == "UNCLASSIFIED"


def test_event_types_yaml_loads_cleanly():
    """The shipped event_types.yaml parses without exception."""
    from app.stages.eventize import _load_event_types_yaml
    repo_root = Path(__file__).resolve().parent.parent.parent.parent
    config = _load_event_types_yaml(repo_root / "app/stages/data/event_types.yaml")
    assert config["default_window_hours"] == 24
    assert config["default_stale_hours"] == 72
    assert len(config["rules"]) > 0


# ---------------------------------------------------------------------------
# EventIndex
# ---------------------------------------------------------------------------


def test_event_index_register_and_best_match():
    from app.stages.eventize import EventIndex
    idx = EventIndex()
    v1 = np.array([1.0, 0.0], dtype=np.float32)
    v2 = np.array([0.0, 1.0], dtype=np.float32)
    idx.register(event_id=10, event_type="REGULATORY_DECISION",
                 centroid=v1, last_seen_at_iso="2026-05-20T12:00:00+00:00")
    idx.register(event_id=11, event_type="REGULATORY_DECISION",
                 centroid=v2, last_seen_at_iso="2026-05-20T18:00:00+00:00")
    eid, cos = idx.best_match("REGULATORY_DECISION", v1,
                              staleness_cutoff_iso="2026-05-19T00:00:00+00:00")
    assert eid == 10
    assert cos == pytest.approx(1.0)


def test_event_index_stale_exclusion():
    from app.stages.eventize import EventIndex
    idx = EventIndex()
    v = np.array([1.0, 0.0], dtype=np.float32)
    idx.register(event_id=10, event_type="X", centroid=v,
                 last_seen_at_iso="2026-05-15T00:00:00+00:00")
    # Cutoff after the registered event's last_seen → excluded.
    eid, _cos = idx.best_match("X", v,
                               staleness_cutoff_iso="2026-05-20T00:00:00+00:00")
    assert eid is None


def test_event_index_update_on_confirm_bumps_support():
    from app.stages.eventize import EventIndex
    idx = EventIndex()
    v1 = np.array([1.0, 0.0], dtype=np.float32)
    v2 = np.array([0.0, 1.0], dtype=np.float32)
    idx.register(event_id=5, event_type="X", centroid=v1,
                 last_seen_at_iso="2026-05-20T12:00:00+00:00")
    idx.update_on_confirm(event_id=5, event_type="X",
                          new_fact_vec=v2,
                          new_last_seen_at_iso="2026-05-20T18:00:00+00:00")
    assert idx._support_counts_by_type["X"][0] == 2
    # Centroid is now (v1 + v2)/2 normalised → roughly (0.707, 0.707).
    centroid = idx._centroids_by_type["X"][0]
    assert centroid[0] == pytest.approx(0.7071, abs=0.01)
    assert centroid[1] == pytest.approx(0.7071, abs=0.01)


def test_event_index_dim_inferred_lazily():
    from app.stages.eventize import EventIndex
    idx = EventIndex()
    assert idx._dim is None
    idx.register(event_id=1, event_type="X",
                 centroid=np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32),
                 last_seen_at_iso="2026-05-20T12:00:00+00:00")
    assert idx._dim == 4

