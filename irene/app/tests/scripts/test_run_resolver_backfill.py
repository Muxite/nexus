"""Tests for scripts/dkg/run_resolver_backfill.py.

Strategy: build an in-memory irene DB with a small seeded DKG state
(articles + entities + facts + extraction_runs), run the backfill, and
assert that `entity_resolutions` rows appear with the right
`resolution_kind`.
"""

from __future__ import annotations

import asyncio
import sqlite3
import tempfile
from pathlib import Path

import pytest

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
from scripts.dkg.run_resolver_backfill import run_inproc


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def irene_db_path():
    tmp = tempfile.NamedTemporaryFile(prefix="test_backfill_", suffix=".db", delete=False)
    tmp.close()
    Path(tmp.name).unlink()
    asyncio.run(_bootstrap(tmp.name))
    yield tmp.name
    Path(tmp.name).unlink(missing_ok=True)


async def _bootstrap(path: str) -> None:
    db = DB(path=path)
    await db.connect()
    await db.close()


def _seed_simple_corpus(db_path: str):
    """Seed: one cycle, one article, two entities + one fact, and an
    extraction_runs row that ties them together."""
    conn = sqlite3.connect(db_path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute(
        "INSERT INTO cycles (name, opened_at) "
        "VALUES ('test-backfill-cycle', '2026-05-24T00:00:00+00:00')"
    )
    cycle_id = conn.execute("SELECT id FROM cycles").fetchone()[0]
    conn.execute(
        "INSERT INTO articles (source, url, title, ingested_at, cycle_id) "
        "VALUES ('test', 'http://t.example/1', 'A', '2026-05-24T00:00:00+00:00', ?)",
        (cycle_id,),
    )
    aid = conn.execute("SELECT id FROM articles").fetchone()[0]
    # FDA + its long-form spell-out — the acronym rule should fire.
    conn.execute(
        "INSERT INTO entities (entity_type, canonical_name, first_seen_at, last_seen_at) "
        "VALUES ('ORG', 'FDA', '2026-05-24T00:00:00+00:00', '2026-05-24T00:00:00+00:00')"
    )
    fda_id = conn.execute("SELECT id FROM entities WHERE canonical_name='FDA'").fetchone()[0]
    conn.execute(
        "INSERT INTO entities (entity_type, canonical_name, first_seen_at, last_seen_at) "
        "VALUES ('ORG', 'U.S. Food and Drug Administration', "
        " '2026-05-24T00:00:00+00:00', '2026-05-24T00:00:00+00:00')"
    )
    fda_long_id = conn.execute(
        "SELECT id FROM entities WHERE canonical_name LIKE 'U.S. Food%'"
    ).fetchone()[0]
    conn.execute(
        """
        INSERT INTO extraction_runs
            (article_id, model, prompt_version, ontology_version, status,
             fact_count, entity_count, dropped_fact_count, cycle_id, created_at)
        VALUES (?, 'm-test', 'v3', 'v1', 'ok', 1, 2, 0, ?, '2026-05-24T00:00:00+00:00')
        """,
        (aid, cycle_id),
    )
    run_id = conn.execute("SELECT id FROM extraction_runs").fetchone()[0]
    conn.execute(
        """
        INSERT INTO facts
            (extraction_run_id, article_id, subj_entity_id, relation,
             obj_entity_id, confidence, observed_at, evidence_span, cycle_id)
        VALUES (?, ?, ?, 'decides', ?, 1.0, '2026-05-24T00:00:00+00:00',
                'FDA decides on ...', ?)
        """,
        (run_id, aid, fda_id, fda_long_id, cycle_id),
    )
    conn.close()
    return {"article_id": aid, "fda_id": fda_id, "fda_long_id": fda_long_id}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_backfill_resolves_acronym_pair(irene_db_path):
    ids = _seed_simple_corpus(irene_db_path)
    summary = run_inproc(irene_db=irene_db_path)
    assert summary["stats"]["articles_with_unresolved"] == 1
    assert summary["stats"]["entities_resolved"] == 2

    conn = sqlite3.connect(irene_db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT entity_id, resolved_to_id, resolution_kind FROM entity_resolutions ORDER BY entity_id"
    ).fetchall()
    conn.close()
    # Both entities resolved; one of them is an 'acronym' resolution (the
    # FDA↔long-form pair). The other resolves to whichever the acronym
    # rule picks as the canonical (deterministic from acronyms.yaml).
    kinds = {r["resolution_kind"] for r in rows}
    assert "acronym" in kinds


def test_backfill_idempotent(irene_db_path):
    _seed_simple_corpus(irene_db_path)
    run_inproc(irene_db=irene_db_path)
    conn = sqlite3.connect(irene_db_path)
    n_first = conn.execute("SELECT COUNT(*) FROM entity_resolutions").fetchone()[0]
    conn.close()
    # Second run should be a no-op (UNIQUE on entity_id; resolver's own
    # SELECT excludes already-resolved entities).
    run_inproc(irene_db=irene_db_path)
    conn = sqlite3.connect(irene_db_path)
    n_second = conn.execute("SELECT COUNT(*) FROM entity_resolutions").fetchone()[0]
    conn.close()
    assert n_first == n_second


def test_backfill_skips_non_ok_extraction_runs(irene_db_path):
    """An extraction_runs row with status != 'ok' has no facts to
    resolve; the SELECT excludes it."""
    conn = sqlite3.connect(irene_db_path, isolation_level=None)
    conn.execute(
        "INSERT INTO cycles (name, opened_at) "
        "VALUES ('test', '2026-05-24T00:00:00+00:00')"
    )
    conn.execute(
        "INSERT INTO articles (source, url, title, ingested_at, cycle_id) "
        "VALUES ('test', 'http://t.example/parse', 'PE', '2026-05-24T00:00:00+00:00', 1)"
    )
    conn.execute(
        """
        INSERT INTO extraction_runs
            (article_id, model, prompt_version, ontology_version, status,
             fact_count, entity_count, dropped_fact_count, cycle_id, created_at)
        VALUES (1, 'm', 'v3', 'v1', 'parse_error',
                0, 0, 0, 1, '2026-05-24T00:00:00+00:00')
        """
    )
    conn.close()
    summary = run_inproc(irene_db=irene_db_path)
    assert summary["stats"]["articles_seen"] == 0
    assert summary["stats"]["articles_with_unresolved"] == 0


def test_backfill_handles_empty_db(irene_db_path):
    summary = run_inproc(irene_db=irene_db_path)
    assert summary["stats"]["articles_seen"] == 0
    assert summary["stats"]["entities_resolved"] == 0


def test_backfill_self_resolves_unmatched_entities(irene_db_path):
    """An entity with no acronym / alias / suffix match resolves to
    itself."""
    conn = sqlite3.connect(irene_db_path, isolation_level=None)
    conn.execute(
        "INSERT INTO cycles (name, opened_at) "
        "VALUES ('test', '2026-05-24T00:00:00+00:00')"
    )
    conn.execute(
        "INSERT INTO articles (source, url, title, ingested_at, cycle_id) "
        "VALUES ('test', 'http://t.example/x', 'X', '2026-05-24T00:00:00+00:00', 1)"
    )
    conn.execute(
        "INSERT INTO entities (entity_type, canonical_name, first_seen_at, last_seen_at) "
        "VALUES ('PERSON', 'Unique Person', "
        "        '2026-05-24T00:00:00+00:00', '2026-05-24T00:00:00+00:00')"
    )
    conn.execute(
        """
        INSERT INTO extraction_runs
            (article_id, model, prompt_version, ontology_version, status,
             fact_count, entity_count, dropped_fact_count, cycle_id, created_at)
        VALUES (1, 'm', 'v3', 'v1', 'ok',
                1, 1, 0, 1, '2026-05-24T00:00:00+00:00')
        """
    )
    conn.execute(
        """
        INSERT INTO facts
            (extraction_run_id, article_id, subj_entity_id, relation,
             confidence, observed_at, evidence_span, cycle_id)
        VALUES (1, 1, 1, 'announces', 1.0, '2026-05-24T00:00:00+00:00',
                'Unique Person announces ...', 1)
        """
    )
    conn.close()
    summary = run_inproc(irene_db=irene_db_path)
    assert summary["by_kind"].get("self", 0) == 1

    conn = sqlite3.connect(irene_db_path)
    row = conn.execute(
        "SELECT entity_id, resolved_to_id FROM entity_resolutions"
    ).fetchone()
    conn.close()
    assert row[0] == row[1]  # resolved to itself


def test_backfill_respects_max_articles(irene_db_path):
    """`--max-articles N` caps how many articles we try to resolve."""
    # Seed 3 articles, each with one unresolved entity.
    conn = sqlite3.connect(irene_db_path, isolation_level=None)
    conn.execute(
        "INSERT INTO cycles (name, opened_at) "
        "VALUES ('c', '2026-05-24T00:00:00+00:00')"
    )
    for i in range(3):
        conn.execute(
            "INSERT INTO articles (source, url, title, ingested_at, cycle_id) "
            "VALUES ('test', ?, ?, '2026-05-24T00:00:00+00:00', 1)",
            (f"http://t.example/{i}", f"Article {i}"),
        )
        conn.execute(
            "INSERT INTO entities (entity_type, canonical_name, first_seen_at, last_seen_at) "
            "VALUES ('PERSON', ?, '2026-05-24T00:00:00+00:00', '2026-05-24T00:00:00+00:00')",
            (f"Person_{i}",),
        )
    for i in range(3):
        conn.execute(
            """
            INSERT INTO extraction_runs
                (article_id, model, prompt_version, ontology_version, status,
                 fact_count, entity_count, dropped_fact_count, cycle_id, created_at)
            VALUES (?, 'm', 'v3', 'v1', 'ok', 1, 1, 0, 1, '2026-05-24T00:00:00+00:00')
            """,
            (i + 1,),
        )
        conn.execute(
            """
            INSERT INTO facts
                (extraction_run_id, article_id, subj_entity_id, relation,
                 confidence, observed_at, evidence_span, cycle_id)
            VALUES (?, ?, ?, 'announces', 1.0, '2026-05-24T00:00:00+00:00',
                    'p announces ...', 1)
            """,
            (i + 1, i + 1, i + 1),
        )
    conn.close()
    summary = run_inproc(irene_db=irene_db_path, max_articles=2)
    assert summary["stats"]["articles_seen"] == 2
