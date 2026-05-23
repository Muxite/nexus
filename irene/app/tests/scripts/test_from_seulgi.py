"""Tests for scripts/migrate/from_seulgi.py.

Builds two ephemeral SQLite DBs (a stripped seulgi schema + a real
irene schema via DB.connect()) and runs the importer against them.
"""

from __future__ import annotations

import asyncio
import sqlite3
import tempfile
from pathlib import Path

import numpy as np
import pytest

# Register all irene DDLs by importing schema-bearing modules.
import app.main  # noqa: F401
import app.stages.dedupe  # noqa: F401
import app.stages.extract  # noqa: F401
import app.stages.ingest  # noqa: F401
import app.stages.markets  # noqa: F401
import app.stages.match  # noqa: F401
import app.stages.resolve  # noqa: F401
import app.stages.trade  # noqa: F401
import app.ai  # noqa: F401
import scripts.migrate.from_seulgi as fromseulgi  # noqa: F401 — registers its own DDL
from app.db import DB, IreneDBPathError
from scripts.migrate.from_seulgi import (
    _CYCLE_NAME,
    _SEULGI_EMBEDDER_ID,
    _SEULGI_EMBEDDING_DIM,
    run_inproc,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


_SEULGI_SCHEMA = [
    """
    CREATE TABLE signals (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        site_url        TEXT,
        headline        TEXT,
        detected_at     TEXT,
        headline_id     TEXT UNIQUE,
        freshness_label TEXT,
        nearest_age_secs INTEGER
    )
    """,
    """
    CREATE TABLE headline_articles (
        headline_id     TEXT PRIMARY KEY,
        article_url     TEXT,
        site_url        TEXT,
        body            TEXT,
        body_chars      INTEGER,
        fetched_at      TEXT,
        fetch_error     TEXT
    )
    """,
    """
    CREATE TABLE markets (
        condition_id    TEXT PRIMARY KEY,
        question        TEXT,
        token_yes       TEXT,
        token_no        TEXT,
        end_date        TEXT,
        volume_usdc     REAL,
        embedding       BLOB,
        fetched_at      TEXT,
        match_count     INTEGER,
        last_matched_at TEXT,
        market_type     TEXT,
        closed_at       TEXT,
        source          TEXT
    )
    """,
]


def _make_seulgi_db(rows_signals=None, rows_articles=None, rows_markets=None):
    """Returns a path to a temp seulgi-shaped sqlite file. Caller is
    responsible for deleting if they care."""
    tmp = tempfile.NamedTemporaryFile(prefix="test_seulgi_", suffix=".db", delete=False)
    tmp.close()
    conn = sqlite3.connect(tmp.name)
    for stmt in _SEULGI_SCHEMA:
        conn.execute(stmt)
    if rows_signals:
        conn.executemany(
            "INSERT INTO signals (site_url, headline, detected_at, headline_id, freshness_label) "
            "VALUES (?, ?, ?, ?, ?)",
            rows_signals,
        )
    if rows_articles:
        conn.executemany(
            "INSERT INTO headline_articles (headline_id, article_url, site_url, body, fetched_at) "
            "VALUES (?, ?, ?, ?, ?)",
            rows_articles,
        )
    if rows_markets:
        conn.executemany(
            "INSERT INTO markets (condition_id, question, token_yes, token_no, end_date, "
            "volume_usdc, embedding, fetched_at, market_type, closed_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows_markets,
        )
    conn.commit()
    conn.close()
    return tmp.name


def _make_irene_db_path():
    """Return a fresh temp path for irene.db. The importer creates +
    bootstraps the schema."""
    tmp = tempfile.NamedTemporaryFile(prefix="test_irene_", suffix=".db", delete=False)
    tmp.close()
    Path(tmp.name).unlink()  # let DB.connect() create it
    return tmp.name


def _make_embedding(dim=_SEULGI_EMBEDDING_DIM, seed=0):
    rng = np.random.default_rng(seed)
    v = rng.standard_normal(dim).astype(np.float32)
    v /= np.linalg.norm(v) + 1e-9
    return v.tobytes()


# ---------------------------------------------------------------------------
# Articles
# ---------------------------------------------------------------------------


def test_articles_basic_join():
    """A signal with a matching headline_articles row produces a fully-
    populated irene.articles row."""
    seulgi = _make_seulgi_db(
        rows_signals=[
            ("https://example.com", "FDA Approves Drug X",
             "2026-05-20T12:00:00+00:00", "hl-001", "BREAKING"),
        ],
        rows_articles=[
            ("hl-001", "https://example.com/article/123", "https://example.com",
             "FDA approved drug X for treating Y.", "2026-05-20T12:00:01+00:00"),
        ],
    )
    irene = _make_irene_db_path()
    summary = run_inproc(seulgi_db=seulgi, irene_db=irene)
    assert summary["tables"]["articles"]["rows_inserted"] == 1
    assert summary["tables"]["articles"]["rows_skipped"] == 0

    conn = sqlite3.connect(irene)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM articles").fetchone()
    assert row["title"] == "FDA Approves Drug X"
    assert row["summary"] == "FDA approved drug X for treating Y."
    assert row["url"] == "https://example.com/article/123"
    assert row["source"] == "seulgi_migration"
    assert row["published_at"] == "2026-05-20T12:00:00+00:00"
    assert row["is_tracer"] == 0
    assert row["cycle_id"] is not None
    conn.close()


def test_articles_no_headline_articles_row():
    """A signal with NO matching headline_articles synthesizes a unique
    URL from headline_id (NOT signal.site_url, which is just the
    publisher and would collapse different headlines from the same
    publisher into one row)."""
    seulgi = _make_seulgi_db(
        rows_signals=[
            ("https://example.com/topic", "Headline without body",
             "2026-05-20T12:00:00+00:00", "hl-002", "EVOLVING"),
            ("https://example.com/topic", "A second headline same publisher",
             "2026-05-20T12:05:00+00:00", "hl-002b", "EVOLVING"),
        ],
        rows_articles=[],  # no body rows
    )
    irene = _make_irene_db_path()
    summary = run_inproc(seulgi_db=seulgi, irene_db=irene)
    # Both headlines should land — synthetic URL keeps them distinct.
    assert summary["tables"]["articles"]["rows_inserted"] == 2

    conn = sqlite3.connect(irene)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT url, title, summary FROM articles ORDER BY id").fetchall()
    assert rows[0]["url"] == "seulgi-headline://hl-002"
    assert rows[0]["title"] == "Headline without body"
    assert rows[0]["summary"] is None
    assert rows[1]["url"] == "seulgi-headline://hl-002b"
    assert rows[1]["title"] == "A second headline same publisher"
    conn.close()


def test_articles_dedup_on_url():
    """Two signals pointing at the same article_url collapse to one row."""
    seulgi = _make_seulgi_db(
        rows_signals=[
            ("https://nytimes.com", "Same article seen twice (sig 1)",
             "2026-05-20T12:00:00+00:00", "hl-003", "BREAKING"),
            ("https://nytimes.com", "Same article seen twice (sig 2)",
             "2026-05-20T12:05:00+00:00", "hl-004", "BREAKING"),
        ],
        rows_articles=[
            ("hl-003", "https://nytimes.com/2026/05/20/article.html",
             "https://nytimes.com", "Body text", "2026-05-20T12:00:01+00:00"),
            ("hl-004", "https://nytimes.com/2026/05/20/article.html",
             "https://nytimes.com", "Body text", "2026-05-20T12:05:01+00:00"),
        ],
    )
    irene = _make_irene_db_path()
    summary = run_inproc(seulgi_db=seulgi, irene_db=irene)
    assert summary["tables"]["articles"]["rows_seen"] == 2
    assert summary["tables"]["articles"]["rows_inserted"] == 1
    assert summary["tables"]["articles"]["rows_skipped"] == 1


def test_articles_drop_empty_headline():
    """A signal with empty/NULL headline is filtered out by the SELECT."""
    seulgi = _make_seulgi_db(
        rows_signals=[
            ("https://example.com", None,
             "2026-05-20T12:00:00+00:00", "hl-005", "EVOLVING"),
            ("https://example.com/2", "Valid headline",
             "2026-05-20T12:00:00+00:00", "hl-006", "EVOLVING"),
        ],
        rows_articles=[],
    )
    irene = _make_irene_db_path()
    summary = run_inproc(seulgi_db=seulgi, irene_db=irene)
    # Only the valid-headline row makes it through.
    assert summary["tables"]["articles"]["rows_seen"] == 1
    assert summary["tables"]["articles"]["rows_inserted"] == 1


# ---------------------------------------------------------------------------
# Markets
# ---------------------------------------------------------------------------


def test_markets_basic_with_embedding():
    blob = _make_embedding(seed=1)
    seulgi = _make_seulgi_db(
        rows_markets=[
            ("cond-001", "Will X happen?", "tok-yes-001", "tok-no-001",
             "2026-12-31", 12345.67, blob, "2026-05-01T00:00:00+00:00",
             "binary", None),
        ],
    )
    irene = _make_irene_db_path()
    summary = run_inproc(seulgi_db=seulgi, irene_db=irene)
    assert summary["tables"]["markets"]["rows_inserted"] == 1

    conn = sqlite3.connect(irene)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM markets WHERE condition_id='cond-001'").fetchone()
    assert row["question"] == "Will X happen?"
    assert row["embedding"] == blob
    assert row["embedder_id"] == _SEULGI_EMBEDDER_ID
    assert row["embedding_dim"] == _SEULGI_EMBEDDING_DIM
    assert row["active"] == 1
    assert row["closed_at"] is None
    conn.close()


def test_markets_closed_sets_active_zero():
    seulgi = _make_seulgi_db(
        rows_markets=[
            ("cond-002", "Closed market?", "y", "n", "2025-01-01", 0.0,
             _make_embedding(seed=2), "2025-01-01T00:00:00+00:00",
             "binary", "2025-01-02T00:00:00+00:00"),
        ],
    )
    irene = _make_irene_db_path()
    run_inproc(seulgi_db=seulgi, irene_db=irene)
    conn = sqlite3.connect(irene)
    row = conn.execute("SELECT active, closed_at FROM markets").fetchone()
    assert row[0] == 0
    assert row[1] == "2025-01-02T00:00:00+00:00"
    conn.close()


def test_markets_null_embedding_passes_through():
    seulgi = _make_seulgi_db(
        rows_markets=[
            ("cond-003", "No-embedding market", "y", "n", None, None,
             None, "2026-05-01T00:00:00+00:00", "binary", None),
        ],
    )
    irene = _make_irene_db_path()
    run_inproc(seulgi_db=seulgi, irene_db=irene)
    conn = sqlite3.connect(irene)
    row = conn.execute("SELECT embedding, embedder_id, embedding_dim FROM markets").fetchone()
    assert row[0] is None
    assert row[1] is None
    assert row[2] is None
    conn.close()


def test_markets_wrong_embedding_dim_raises():
    """A 256-d blob (not 384-d) trips the dim check."""
    bad_blob = np.zeros(256, dtype=np.float32).tobytes()
    seulgi = _make_seulgi_db(
        rows_markets=[
            ("cond-004", "Wrong dim", "y", "n", None, None, bad_blob,
             None, "binary", None),
        ],
    )
    irene = _make_irene_db_path()
    with pytest.raises(RuntimeError, match="embedding dim"):
        run_inproc(seulgi_db=seulgi, irene_db=irene)


# ---------------------------------------------------------------------------
# Cycle + batch log
# ---------------------------------------------------------------------------


def test_cycle_row_is_created_once():
    seulgi = _make_seulgi_db(
        rows_signals=[
            ("https://example.com", "X", "2026-05-20T00:00:00+00:00", "hl-cy1", "BREAKING"),
        ],
    )
    irene = _make_irene_db_path()
    run_inproc(seulgi_db=seulgi, irene_db=irene)
    run_inproc(seulgi_db=seulgi, irene_db=irene)  # re-run: idempotent

    conn = sqlite3.connect(irene)
    rows = conn.execute("SELECT name FROM cycles WHERE name=?", (_CYCLE_NAME,)).fetchall()
    assert len(rows) == 1
    conn.close()


def test_batch_log_records_each_table():
    seulgi = _make_seulgi_db(
        rows_signals=[
            ("https://example.com", "Hi", "2026-05-20T00:00:00+00:00", "hl-bl1", "BREAKING"),
        ],
        rows_markets=[
            ("cond-bl1", "Q?", "y", "n", None, None, _make_embedding(seed=3),
             None, "binary", None),
        ],
    )
    irene = _make_irene_db_path()
    summary = run_inproc(seulgi_db=seulgi, irene_db=irene)

    conn = sqlite3.connect(irene)
    rows = conn.execute(
        "SELECT table_name, rows_inserted FROM seulgi_migration_batches WHERE batch_id=?",
        (summary["batch_id"],),
    ).fetchall()
    by_table = {r[0]: r[1] for r in rows}
    assert by_table == {"articles": 1, "markets": 1}
    conn.close()


# ---------------------------------------------------------------------------
# Resumability
# ---------------------------------------------------------------------------


def test_resumability_second_run_inserts_zero():
    seulgi = _make_seulgi_db(
        rows_signals=[
            ("https://example.com", "Once", "2026-05-20T00:00:00+00:00", "hl-r1", "BREAKING"),
        ],
        rows_markets=[
            ("cond-r1", "Q?", "y", "n", None, None, _make_embedding(seed=4),
             None, "binary", None),
        ],
    )
    irene = _make_irene_db_path()
    first = run_inproc(seulgi_db=seulgi, irene_db=irene)
    second = run_inproc(seulgi_db=seulgi, irene_db=irene)
    assert first["tables"]["articles"]["rows_inserted"] == 1
    assert second["tables"]["articles"]["rows_inserted"] == 0
    assert second["tables"]["articles"]["rows_skipped"] == 1
    assert first["tables"]["markets"]["rows_inserted"] == 1
    assert second["tables"]["markets"]["rows_inserted"] == 0


# ---------------------------------------------------------------------------
# Seulgi-path assertion
# ---------------------------------------------------------------------------


def test_irene_db_must_not_be_seulgi_path():
    seulgi = _make_seulgi_db(
        rows_signals=[
            ("https://example.com", "X", "2026-05-20T00:00:00+00:00", "hl-sp1", "BREAKING"),
        ],
    )
    # Path containing 'seulgi' triggers the assertion in DB.connect().
    bad_path = "/tmp/data/seulgi/something.db"
    with pytest.raises(IreneDBPathError):
        run_inproc(seulgi_db=seulgi, irene_db=bad_path)
