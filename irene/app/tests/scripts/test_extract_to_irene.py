"""Tests for scripts/dkg/extract_to_irene.py.

Covers the pure-compute layer (parsing/normalization), the article
selection (idempotent resume), and the DB persist flow. The async
worker pool isn't exercised end-to-end — testing it would require
mocking httpx + the OpenRouter response shape, which is already
exercised in the sandbox bulk-sample tests. Here we focus on the
new logic this script adds: SBERT-via-find_or_create_entity, the
extraction_runs UNIQUE resume, and the perf-timer wiring.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pytest

# Register DDLs.
import app.main  # noqa: F401
import app.stages.dedupe  # noqa: F401
import app.stages.extract  # noqa: F401
import app.stages.ingest  # noqa: F401
import app.stages.markets  # noqa: F401
import app.stages.match  # noqa: F401
import app.stages.resolve  # noqa: F401
import app.stages.trade  # noqa: F401
import app.ai  # noqa: F401
from app.db import DB
from app.timing import ProfileStore, SampledTimer
from app.sbert_dedupe import EntityIndex
from scripts.dkg.extract_to_irene import (
    CallResult,
    _extract_content,
    _normalise_facts,
    _persist_extraction,
    _select_articles_to_process,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def irene_db_path():
    tmp = tempfile.NamedTemporaryFile(prefix="test_irene_extract_", suffix=".db", delete=False)
    tmp.close()
    Path(tmp.name).unlink()
    asyncio.run(_bootstrap(tmp.name))
    yield tmp.name
    Path(tmp.name).unlink(missing_ok=True)


async def _bootstrap(path: str) -> None:
    db = DB(path=path)
    await db.connect()
    await db.close()


@pytest.fixture
def irene_conn(irene_db_path):
    conn = sqlite3.connect(irene_db_path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    # Need a cycle row to point articles at.
    conn.execute(
        "INSERT INTO cycles (name, opened_at) VALUES ('test-cycle', '2026-05-24T00:00:00+00:00')"
    )
    yield conn
    conn.close()


class _FakeEmbedder:
    """Deterministic embedder that returns one-hot vectors keyed by name.
    Same-surface inputs land same vector (cosine 1.0); different inputs
    cosine 0.0 — far below SBERT threshold so each new surface inserts
    its own entity row."""

    embedder_id = "fake-embedder@8"

    def embed(self, text: str) -> np.ndarray:
        vec = np.zeros(8, dtype=np.float32)
        vec[abs(hash(text)) % 8] = 1.0
        return vec


# ---------------------------------------------------------------------------
# Parsing layer
# ---------------------------------------------------------------------------


def test_extract_content_handles_normal_chat_response():
    body = json.dumps({"choices": [{"message": {"content": "hello"}}]})
    assert _extract_content(body) == "hello"


def test_extract_content_returns_none_on_garbage():
    assert _extract_content("not json") is None
    assert _extract_content('{"foo": "bar"}') is None


def test_normalise_facts_accepts_top_level_array():
    facts = _normalise_facts([
        {"subj": "FDA", "subj_type": "ORG", "relation": "decides",
         "obj": "Pfizer", "obj_type": "COMPANY", "obj_value": None},
    ])
    assert facts is not None and len(facts) == 1
    assert facts[0]["relation"] == "decides"


def test_normalise_facts_accepts_facts_key_dict():
    facts = _normalise_facts({"facts": [
        {"subject": "Fed", "subj_type": "ORG", "relation": "changes",
         "object": None, "obj_value": "rate -25bp"},
    ]})
    assert facts is not None and len(facts) == 1
    assert facts[0]["subj"] == "Fed"
    assert facts[0]["obj"] is None
    assert facts[0]["obj_value"] == "rate -25bp"


def test_normalise_facts_drops_off_ontology_relations():
    facts = _normalise_facts({"facts": [
        {"subj": "X", "subj_type": "ORG", "relation": "visits", "obj": "Y", "obj_type": "ORG"},
        {"subj": "X", "subj_type": "ORG", "relation": "announces", "obj": None},
    ]})
    assert facts is not None and len(facts) == 1
    assert facts[0]["relation"] == "announces"


def test_normalise_facts_drops_off_ontology_entity_types():
    facts = _normalise_facts({"facts": [
        {"subj": "Paris", "subj_type": "CITY", "relation": "announces", "obj": None},
        {"subj": "Apple", "subj_type": "COMPANY", "relation": "announces", "obj": None},
    ]})
    assert facts is not None and len(facts) == 1
    assert facts[0]["subj"] == "Apple"


def test_normalise_facts_returns_none_on_unrecognised_shape():
    assert _normalise_facts("not a structure") is None
    assert _normalise_facts({"unknown": "shape"}) is None


# ---------------------------------------------------------------------------
# Article selection — idempotent resume
# ---------------------------------------------------------------------------


def _insert_article(conn, *, url, title, cycle_id=1):
    cur = conn.execute(
        "INSERT INTO articles (source, url, title, ingested_at, cycle_id) "
        "VALUES ('seulgi_migration', ?, ?, '2026-05-24T00:00:00+00:00', ?)",
        (url, title, cycle_id),
    )
    return cur.lastrowid


def _insert_extraction_run(conn, *, article_id, model, cycle_id=1):
    conn.execute(
        """
        INSERT INTO extraction_runs
            (article_id, model, prompt_version, ontology_version, status,
             cycle_id, fact_count, entity_count, dropped_fact_count, created_at)
        VALUES (?, ?, 'v3', 'v1', 'ok', ?, 0, 0, 0, '2026-05-24T00:00:00+00:00')
        """,
        (article_id, model, cycle_id),
    )


def test_select_articles_excludes_already_processed(irene_conn):
    aid1 = _insert_article(irene_conn, url="http://a.example", title="A")
    aid2 = _insert_article(irene_conn, url="http://b.example", title="B")
    _insert_extraction_run(irene_conn, article_id=aid1, model="m-1")
    rows = _select_articles_to_process(
        irene_conn, source_filter=None, model="m-1", limit=None,
    )
    ids = [r["id"] for r in rows]
    assert ids == [aid2]


def test_select_articles_different_model_returns_all(irene_conn):
    aid1 = _insert_article(irene_conn, url="http://a.example", title="A")
    _insert_extraction_run(irene_conn, article_id=aid1, model="m-1")
    rows = _select_articles_to_process(
        irene_conn, source_filter=None, model="m-2", limit=None,
    )
    assert len(rows) == 1
    assert rows[0]["id"] == aid1


def test_select_articles_honors_source_filter(irene_conn):
    aid = _insert_article(irene_conn, url="http://a.example", title="A")
    # Insert one with a different source.
    irene_conn.execute(
        "INSERT INTO articles (source, url, title, ingested_at, cycle_id) "
        "VALUES ('live', 'http://b.example', 'B', '2026-05-24T00:00:00+00:00', 1)"
    )
    rows = _select_articles_to_process(
        irene_conn,
        source_filter="source='seulgi_migration'",
        model="m-1", limit=None,
    )
    assert [r["id"] for r in rows] == [aid]


def test_select_articles_limit(irene_conn):
    for i in range(5):
        _insert_article(irene_conn, url=f"http://{i}.example", title=f"H{i}")
    rows = _select_articles_to_process(
        irene_conn, source_filter=None, model="m-1", limit=2,
    )
    assert len(rows) == 2


# ---------------------------------------------------------------------------
# Persist — writes entities + facts + extraction_runs
# ---------------------------------------------------------------------------


def _make_timers():
    store = ProfileStore()
    sbert = SampledTimer("sbert", store, sample_rate=0.0)
    db = SampledTimer("db", store, sample_rate=0.0)
    return sbert, db


def test_persist_ok_writes_entities_and_facts(irene_conn):
    aid = _insert_article(irene_conn, url="http://a.example", title="A")
    result = CallResult(
        status="ok",
        raw_response='{"choices":[{"message":{"content":"ok"}}]}',
        parsed_facts=[
            {"subj": "FDA", "subj_type": "ORG", "relation": "decides",
             "obj": "Pfizer", "obj_type": "COMPANY", "obj_value": None},
            {"subj": "FDA", "subj_type": "ORG", "relation": "announces",
             "obj": None, "obj_type": None, "obj_value": None},
        ],
        latency_ms=120.0,
        tokens_in=200, tokens_out=30, cost_usd=0.0001,
    )
    sbert, db = _make_timers()
    run_id = _persist_extraction(
        irene_conn, embedder=_FakeEmbedder(), entity_index=None,
        article_id=aid, cycle_id=1, is_tracer=0, tracer_id=None,
        model="m-1", result=result,
        sbert_timer=sbert, db_timer=db,
    )
    assert run_id > 0
    # Two distinct entities.
    n_ent = irene_conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    assert n_ent == 2
    # Two facts.
    n_facts = irene_conn.execute(
        "SELECT COUNT(*) FROM facts WHERE article_id=?", (aid,)
    ).fetchone()[0]
    assert n_facts == 2
    # Extraction run reflects the metrics.
    er = irene_conn.execute(
        "SELECT status, fact_count, entity_count, dropped_fact_count, latency_ms "
        "FROM extraction_runs WHERE article_id=?", (aid,)
    ).fetchone()
    assert er["status"] == "ok"
    assert er["fact_count"] == 2
    assert er["entity_count"] == 2
    assert er["dropped_fact_count"] == 0
    assert er["latency_ms"] == 120.0


def test_persist_duplicate_returns_zero_and_no_writes(irene_conn):
    aid = _insert_article(irene_conn, url="http://a.example", title="A")
    result = CallResult(
        status="ok",
        parsed_facts=[
            {"subj": "FDA", "subj_type": "ORG", "relation": "decides",
             "obj": None, "obj_type": None, "obj_value": None},
        ],
    )
    sbert, db = _make_timers()
    run_id_1 = _persist_extraction(
        irene_conn, embedder=_FakeEmbedder(), entity_index=None,
        article_id=aid, cycle_id=1, is_tracer=0, tracer_id=None,
        model="m-1", result=result,
        sbert_timer=sbert, db_timer=db,
    )
    assert run_id_1 > 0
    # Second call with same (article, model, prompt_v, ontology_v) is a no-op.
    run_id_2 = _persist_extraction(
        irene_conn, embedder=_FakeEmbedder(), entity_index=None,
        article_id=aid, cycle_id=1, is_tracer=0, tracer_id=None,
        model="m-1", result=result,
        sbert_timer=sbert, db_timer=db,
    )
    assert run_id_2 == 0
    # No duplicate fact rows.
    n_facts = irene_conn.execute("SELECT COUNT(*) FROM facts").fetchone()[0]
    assert n_facts == 1


def test_persist_empty_status_writes_run_no_facts(irene_conn):
    aid = _insert_article(irene_conn, url="http://a.example", title="A")
    result = CallResult(status="empty", parsed_facts=[])
    sbert, db = _make_timers()
    run_id = _persist_extraction(
        irene_conn, embedder=_FakeEmbedder(), entity_index=None,
        article_id=aid, cycle_id=1, is_tracer=0, tracer_id=None,
        model="m-1", result=result,
        sbert_timer=sbert, db_timer=db,
    )
    assert run_id > 0
    assert irene_conn.execute("SELECT COUNT(*) FROM facts").fetchone()[0] == 0
    er = irene_conn.execute("SELECT status FROM extraction_runs WHERE article_id=?", (aid,)).fetchone()
    assert er["status"] == "empty"


def test_persist_with_index_warms_and_registers(irene_conn):
    """When an EntityIndex is passed in, new entities land in both the
    DB and the in-memory index — and subsequent persist calls hit the
    index fast path instead of the SQL."""
    aid1 = _insert_article(irene_conn, url="http://a.example", title="A")
    aid2 = _insert_article(irene_conn, url="http://b.example", title="B")
    result_a = CallResult(
        status="ok",
        parsed_facts=[
            {"subj": "FDA", "subj_type": "ORG", "relation": "decides",
             "obj": "Pfizer", "obj_type": "COMPANY", "obj_value": None},
        ],
    )
    result_b = CallResult(
        status="ok",
        parsed_facts=[
            {"subj": "FDA", "subj_type": "ORG", "relation": "announces",
             "obj": None, "obj_type": None, "obj_value": None},
        ],
    )
    sbert, db = _make_timers()
    index = EntityIndex()
    _persist_extraction(
        irene_conn, embedder=_FakeEmbedder(), entity_index=index,
        article_id=aid1, cycle_id=1, is_tracer=0, tracer_id=None,
        model="m-1", result=result_a,
        sbert_timer=sbert, db_timer=db,
    )
    # Index now contains FDA and Pfizer.
    assert index.by_canonical("ORG", "FDA") is not None
    assert index.by_canonical("COMPANY", "Pfizer") is not None
    # Second article re-uses FDA.
    _persist_extraction(
        irene_conn, embedder=_FakeEmbedder(), entity_index=index,
        article_id=aid2, cycle_id=1, is_tracer=0, tracer_id=None,
        model="m-1", result=result_b,
        sbert_timer=sbert, db_timer=db,
    )
    # Still only one FDA row in the DB.
    n = irene_conn.execute(
        "SELECT COUNT(*) FROM entities WHERE entity_type='ORG' AND canonical_name='FDA'"
    ).fetchone()[0]
    assert n == 1


def test_persist_parse_error_writes_only_run(irene_conn):
    aid = _insert_article(irene_conn, url="http://a.example", title="A")
    result = CallResult(
        status="parse_error",
        raw_response="garbage",
        error_detail="inner_json_decode",
    )
    sbert, db = _make_timers()
    run_id = _persist_extraction(
        irene_conn, embedder=_FakeEmbedder(), entity_index=None,
        article_id=aid, cycle_id=1, is_tracer=0, tracer_id=None,
        model="m-1", result=result,
        sbert_timer=sbert, db_timer=db,
    )
    assert run_id > 0
    assert irene_conn.execute("SELECT COUNT(*) FROM facts").fetchone()[0] == 0
    assert irene_conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0] == 0
