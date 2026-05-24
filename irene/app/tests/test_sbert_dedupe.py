"""Tests for app/sbert_dedupe.py.

Two layers:
- `score_against_existing` is pure-compute and exercised against
  hand-built vectors (boundary tests at threshold).
- `find_or_create_entity` runs against an in-memory sqlite with the
  real `entities`/`entity_aliases` schema registered via the existing
  extract.py DDL. The embedder is a deterministic fake so cosine
  outcomes are predictable.
"""

from __future__ import annotations

import sqlite3
import sys
import types
from unittest.mock import MagicMock

import numpy as np
import pytest

# Import DDL-bearing module so the schema is registered for our in-memory DB.
import app.stages.extract  # noqa: F401
from app.embed import Embedder
from app.sbert_dedupe import (
    EntityIndex,
    find_or_create_entity,
    score_against_existing,
)


# ---------------------------------------------------------------------------
# In-memory DB fixture with the entities + entity_aliases schema
# ---------------------------------------------------------------------------


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.execute(
        """
        CREATE TABLE entities (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_type     TEXT NOT NULL CHECK(entity_type IN
                                ('COMPANY','PERSON','ORG','COUNTRY',
                                 'ECON_INDICATOR','ASSET','EVENT')),
            canonical_name  TEXT NOT NULL,
            external_refs   TEXT,
            status          TEXT NOT NULL DEFAULT 'active',
            embedding       BLOB,
            embedder_id     TEXT,
            first_seen_at   TEXT NOT NULL,
            last_seen_at    TEXT NOT NULL,
            UNIQUE(entity_type, canonical_name)
        )
        """
    )
    c.execute(
        """
        CREATE TABLE entity_aliases (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_id   INTEGER NOT NULL REFERENCES entities(id),
            alias       TEXT NOT NULL,
            alias_kind  TEXT NOT NULL CHECK(alias_kind IN
                            ('synonym','ticker','slug','wikidata','other')),
            UNIQUE(entity_id, alias, alias_kind)
        )
        """
    )
    yield c
    c.close()


# ---------------------------------------------------------------------------
# Fake sentence-transformers wiring (same shape as test_embed.py)
# ---------------------------------------------------------------------------


class _FakeModel:
    """Returns a one-hot float32 vector indexed by `hash(text) % out_dim`.

    Same-hash inputs get cosine 1.0; different-hash inputs cosine 0.0.
    For threshold-boundary tests we bypass this and use raw vectors.
    """

    def __init__(self, out_dim: int) -> None:
        self.out_dim = out_dim
        self.encode = MagicMock(side_effect=self._encode)

    def _encode(self, text, **kwargs):
        if isinstance(text, list):
            arr = np.zeros((len(text), self.out_dim), dtype=np.float32)
            for i, t in enumerate(text):
                arr[i, hash(t) % self.out_dim] = 1.0
            return arr
        vec = np.zeros(self.out_dim, dtype=np.float32)
        vec[hash(text) % self.out_dim] = 1.0
        return vec


def _install_fake_embedder(monkeypatch, out_dim: int = 16):
    fake_model = _FakeModel(out_dim)
    fake_module = types.ModuleType("sentence_transformers")
    fake_module.SentenceTransformer = MagicMock(return_value=fake_model)  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "sentence_transformers", fake_module)
    return Embedder("fake-model", out_dim)


# A small class that overrides Embedder.embed to return whatever vector we
# hand it — used for the cosine-threshold boundary tests where the natural
# one-hot fake can't produce 0.84/0.86 outputs.
class _ScriptedEmbedder:
    def __init__(self, dim: int, embeddings: dict[str, np.ndarray]) -> None:
        self.embedder_id = f"scripted@{dim}"
        self._embeddings = embeddings
        self._dim = dim

    def embed(self, text: str) -> np.ndarray:
        if text not in self._embeddings:
            raise KeyError(
                f"_ScriptedEmbedder has no vector for {text!r}; "
                f"declare one in the test"
            )
        return self._embeddings[text].astype(np.float32)


# ---------------------------------------------------------------------------
# Pure-compute layer
# ---------------------------------------------------------------------------


def test_score_against_empty_candidates_returns_negative_one():
    q = np.array([1.0, 0.0], dtype=np.float32)
    cand = np.zeros((0, 2), dtype=np.float32)
    idx, cos = score_against_existing(q, cand)
    assert idx == -1
    assert cos == 0.0


def test_score_picks_max_cosine():
    q = np.array([1.0, 0.0, 0.0], dtype=np.float32)
    cand = np.stack([
        np.array([0.0, 1.0, 0.0], dtype=np.float32),   # cos 0.0
        np.array([0.8, 0.6, 0.0], dtype=np.float32),   # cos 0.8
        np.array([0.6, 0.0, 0.8], dtype=np.float32),   # cos 0.6
    ])
    idx, cos = score_against_existing(q, cand)
    assert idx == 1
    assert cos == pytest.approx(0.8, rel=1e-6)


def test_score_dim_mismatch_raises():
    q = np.array([1.0, 0.0], dtype=np.float32)
    cand = np.zeros((2, 3), dtype=np.float32)
    with pytest.raises(ValueError):
        score_against_existing(q, cand)


# ---------------------------------------------------------------------------
# find_or_create_entity — fast path
# ---------------------------------------------------------------------------


def test_exact_canonical_match_no_embedder_call(conn, monkeypatch):
    """If the same (type, canonical) already exists, the embedder is not
    consulted and no new row is inserted. The existing row's last_seen_at
    is bumped."""
    emb = _install_fake_embedder(monkeypatch, out_dim=16)
    cur = conn.cursor()
    # Seed an entity with NO embedding — exact-canonical hits should still
    # work for legacy rows.
    cur.execute(
        "INSERT INTO entities (entity_type, canonical_name, first_seen_at, last_seen_at) "
        "VALUES ('COMPANY', 'Apple', '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')"
    )
    seeded_id = cur.lastrowid

    eid, kind = find_or_create_entity(
        cur,
        entity_type="COMPANY",
        surface_name="Apple",
        embedder=emb,
        now_iso="2026-05-23T00:00:00+00:00",
    )
    assert eid == seeded_id
    assert kind == "exact_canonical"
    # Embedder was never called.
    assert sys.modules["sentence_transformers"].SentenceTransformer.call_count == 0
    # No alias was created (surface == canonical).
    cur.execute("SELECT COUNT(*) FROM entity_aliases")
    assert cur.fetchone()[0] == 0
    # last_seen_at bumped.
    cur.execute("SELECT last_seen_at FROM entities WHERE id = ?", (seeded_id,))
    assert cur.fetchone()[0] == "2026-05-23T00:00:00+00:00"


# ---------------------------------------------------------------------------
# find_or_create_entity — insert new
# ---------------------------------------------------------------------------


def test_insert_new_stamps_embedding_and_embedder_id(conn, monkeypatch):
    emb = _install_fake_embedder(monkeypatch, out_dim=16)
    cur = conn.cursor()
    eid, kind = find_or_create_entity(
        cur,
        entity_type="COMPANY",
        surface_name="Pfizer",
        embedder=emb,
        now_iso="2026-05-23T00:00:00+00:00",
    )
    assert kind == "inserted_new"
    cur.execute(
        "SELECT canonical_name, embedding, embedder_id FROM entities WHERE id = ?",
        (eid,),
    )
    name, blob, eid_str = cur.fetchone()
    assert name == "Pfizer"
    assert blob is not None
    assert eid_str == "fake-model@16"
    vec = np.frombuffer(blob, dtype=np.float32)
    assert vec.shape == (16,)


# ---------------------------------------------------------------------------
# find_or_create_entity — SBERT merge path
# ---------------------------------------------------------------------------


def test_sbert_merge_above_threshold(conn):
    """Two surface forms whose embeddings cosine ≥ threshold merge."""
    vec_putin = np.array([0.9, 0.4, 0.0, 0.0], dtype=np.float32)
    vec_putin /= np.linalg.norm(vec_putin)
    vec_vladimir_putin = np.array([0.95, 0.31, 0.0, 0.0], dtype=np.float32)
    vec_vladimir_putin /= np.linalg.norm(vec_vladimir_putin)
    # cosine(Putin, Vladimir Putin) ≈ 0.998 — well above 0.85
    emb = _ScriptedEmbedder(4, {
        "Putin": vec_putin,
        "Vladimir Putin": vec_vladimir_putin,
    })
    cur = conn.cursor()

    eid1, kind1 = find_or_create_entity(
        cur, entity_type="PERSON", surface_name="Putin",
        embedder=emb, now_iso="2026-05-23T00:00:00+00:00",
    )
    assert kind1 == "inserted_new"

    eid2, kind2 = find_or_create_entity(
        cur, entity_type="PERSON", surface_name="Vladimir Putin",
        embedder=emb, now_iso="2026-05-23T00:00:01+00:00",
    )
    assert kind2 == "sbert_merge"
    assert eid2 == eid1
    # The surface form is preserved as an alias.
    cur.execute(
        "SELECT alias, alias_kind FROM entity_aliases WHERE entity_id = ?",
        (eid1,),
    )
    aliases = cur.fetchall()
    assert ("Vladimir Putin", "synonym") in aliases


def test_sbert_no_merge_below_threshold(conn):
    """Two different entities (orthogonal vectors) do not merge."""
    vec_apple = np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32)
    vec_microsoft = np.array([0.0, 1.0, 0.0, 0.0], dtype=np.float32)
    emb = _ScriptedEmbedder(4, {
        "Apple": vec_apple,
        "Microsoft": vec_microsoft,
    })
    cur = conn.cursor()
    eid1, _ = find_or_create_entity(
        cur, entity_type="COMPANY", surface_name="Apple",
        embedder=emb, now_iso="2026-05-23T00:00:00+00:00",
    )
    eid2, kind2 = find_or_create_entity(
        cur, entity_type="COMPANY", surface_name="Microsoft",
        embedder=emb, now_iso="2026-05-23T00:00:01+00:00",
    )
    assert kind2 == "inserted_new"
    assert eid1 != eid2


def test_threshold_boundary_just_below(conn):
    """Cosine 0.84 must NOT merge under default threshold 0.85."""
    # Construct two unit vectors with exact cosine 0.84.
    v1 = np.array([1.0, 0.0], dtype=np.float32)
    v2 = np.array([0.84, np.sqrt(1.0 - 0.84 ** 2)], dtype=np.float32)
    emb = _ScriptedEmbedder(2, {"A": v1, "B": v2})
    cur = conn.cursor()
    find_or_create_entity(
        cur, entity_type="COMPANY", surface_name="A",
        embedder=emb, now_iso="2026-05-23T00:00:00+00:00",
    )
    _, kind = find_or_create_entity(
        cur, entity_type="COMPANY", surface_name="B",
        embedder=emb, now_iso="2026-05-23T00:00:01+00:00",
    )
    assert kind == "inserted_new"


def test_threshold_boundary_just_above(conn):
    """Cosine 0.86 must merge under default threshold 0.85."""
    v1 = np.array([1.0, 0.0], dtype=np.float32)
    v2 = np.array([0.86, np.sqrt(1.0 - 0.86 ** 2)], dtype=np.float32)
    emb = _ScriptedEmbedder(2, {"A": v1, "B": v2})
    cur = conn.cursor()
    find_or_create_entity(
        cur, entity_type="COMPANY", surface_name="A",
        embedder=emb, now_iso="2026-05-23T00:00:00+00:00",
    )
    _, kind = find_or_create_entity(
        cur, entity_type="COMPANY", surface_name="B",
        embedder=emb, now_iso="2026-05-23T00:00:01+00:00",
    )
    assert kind == "sbert_merge"


# ---------------------------------------------------------------------------
# Cross-type isolation
# ---------------------------------------------------------------------------


def test_cross_type_does_not_merge(conn):
    """Same surface name in two types stays distinct even with identical vectors."""
    v = np.array([1.0, 0.0], dtype=np.float32)
    emb = _ScriptedEmbedder(2, {"Apple": v})
    cur = conn.cursor()
    eid_company, _ = find_or_create_entity(
        cur, entity_type="COMPANY", surface_name="Apple",
        embedder=emb, now_iso="2026-05-23T00:00:00+00:00",
    )
    eid_event, kind_event = find_or_create_entity(
        cur, entity_type="EVENT", surface_name="Apple",
        embedder=emb, now_iso="2026-05-23T00:00:01+00:00",
    )
    assert kind_event == "inserted_new"
    assert eid_company != eid_event


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


def test_idempotent_same_surface_twice(conn, monkeypatch):
    """Calling twice with the same surface name returns the same id and writes
    only one row — the second is an exact-canonical hit."""
    emb = _install_fake_embedder(monkeypatch, out_dim=16)
    cur = conn.cursor()
    eid1, kind1 = find_or_create_entity(
        cur, entity_type="ORG", surface_name="FDA",
        embedder=emb, now_iso="2026-05-23T00:00:00+00:00",
    )
    eid2, kind2 = find_or_create_entity(
        cur, entity_type="ORG", surface_name="FDA",
        embedder=emb, now_iso="2026-05-23T00:00:05+00:00",
    )
    assert eid1 == eid2
    assert kind1 == "inserted_new"
    assert kind2 == "exact_canonical"
    cur.execute("SELECT COUNT(*) FROM entities")
    assert cur.fetchone()[0] == 1


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def test_empty_surface_name_raises(conn, monkeypatch):
    emb = _install_fake_embedder(monkeypatch, out_dim=16)
    cur = conn.cursor()
    with pytest.raises(ValueError):
        find_or_create_entity(
            cur, entity_type="COMPANY", surface_name="   ",
            embedder=emb, now_iso="2026-05-23T00:00:00+00:00",
        )


def test_empty_entity_type_raises(conn, monkeypatch):
    emb = _install_fake_embedder(monkeypatch, out_dim=16)
    cur = conn.cursor()
    with pytest.raises(ValueError):
        find_or_create_entity(
            cur, entity_type="", surface_name="Apple",
            embedder=emb, now_iso="2026-05-23T00:00:00+00:00",
        )


# ---------------------------------------------------------------------------
# Mixed-dim safety
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# EntityIndex — in-memory cache + indexed find_or_create_entity
# ---------------------------------------------------------------------------


def test_entity_index_load_empty(conn):
    idx = EntityIndex()
    idx.load(conn.cursor(), "COMPANY")
    assert idx.size("COMPANY") == 0
    eid_match, canonical, cos = idx.best_match("COMPANY", np.zeros(4, dtype=np.float32))
    assert eid_match is None
    assert canonical == ""
    assert cos == 0.0


def test_entity_index_load_from_seeded_rows(conn):
    v = np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO entities (entity_type, canonical_name, embedding, embedder_id, "
        "first_seen_at, last_seen_at) "
        "VALUES ('COMPANY', 'Apple', ?, 'fake@4', '2026-01-01', '2026-01-01')",
        (v.tobytes(),),
    )
    idx = EntityIndex()
    idx.load(cur, "COMPANY")
    assert idx.size("COMPANY") == 1
    assert idx.by_canonical("COMPANY", "Apple") is not None
    eid, name, cos = idx.best_match("COMPANY", v)
    assert name == "Apple"
    assert cos == pytest.approx(1.0)


def test_entity_index_register_appends_and_searchable(conn):
    idx = EntityIndex()
    idx.load(conn.cursor(), "PERSON")
    v1 = np.array([1.0, 0.0], dtype=np.float32)
    v2 = np.array([0.0, 1.0], dtype=np.float32)
    idx.register(entity_id=10, entity_type="PERSON",
                 canonical_name="Putin", embedding=v1)
    idx.register(entity_id=11, entity_type="PERSON",
                 canonical_name="Trump", embedding=v2)
    assert idx.size("PERSON") == 2
    eid, name, _cos = idx.best_match("PERSON", v1)
    assert eid == 10 and name == "Putin"
    eid2, name2, _cos2 = idx.best_match("PERSON", v2)
    assert eid2 == 11 and name2 == "Trump"


def test_entity_index_dim_mismatch_on_register_raises(conn):
    idx = EntityIndex()
    idx.register(entity_id=1, entity_type="ORG",
                 canonical_name="A", embedding=np.zeros(4, dtype=np.float32))
    with pytest.raises(RuntimeError, match="dim mismatch"):
        idx.register(entity_id=2, entity_type="ORG",
                     canonical_name="B", embedding=np.zeros(8, dtype=np.float32))


def test_find_or_create_with_index_exact_canonical(conn, monkeypatch):
    emb = _install_fake_embedder(monkeypatch, out_dim=16)
    cur = conn.cursor()
    idx = EntityIndex()
    # First call: insert + register.
    eid1, kind1 = find_or_create_entity(
        cur, entity_type="ORG", surface_name="FDA",
        embedder=emb, now_iso="2026-05-24T00:00:00+00:00", index=idx,
    )
    assert kind1 == "inserted_new"
    # Second call same surface: hits the index fast path, no SBERT, no SQL SELECT.
    eid2, kind2 = find_or_create_entity(
        cur, entity_type="ORG", surface_name="FDA",
        embedder=emb, now_iso="2026-05-24T00:00:01+00:00", index=idx,
    )
    assert eid1 == eid2
    assert kind2 == "exact_canonical"


def test_find_or_create_with_index_sbert_merge(conn):
    vp = np.array([0.9, 0.4, 0.0, 0.0], dtype=np.float32)
    vp /= np.linalg.norm(vp)
    vvp = np.array([0.95, 0.31, 0.0, 0.0], dtype=np.float32)
    vvp /= np.linalg.norm(vvp)
    emb = _ScriptedEmbedder(4, {"Putin": vp, "Vladimir Putin": vvp})
    cur = conn.cursor()
    idx = EntityIndex()
    eid1, _ = find_or_create_entity(
        cur, entity_type="PERSON", surface_name="Putin",
        embedder=emb, now_iso="2026-05-24T00:00:00+00:00", index=idx,
    )
    eid2, kind2 = find_or_create_entity(
        cur, entity_type="PERSON", surface_name="Vladimir Putin",
        embedder=emb, now_iso="2026-05-24T00:00:01+00:00", index=idx,
    )
    assert kind2 == "sbert_merge"
    assert eid2 == eid1
    # Alias persists in entity_aliases.
    rows = cur.execute(
        "SELECT alias FROM entity_aliases WHERE entity_id = ?", (eid1,)
    ).fetchall()
    assert ("Vladimir Putin",) in rows


def test_indexed_and_unindexed_agree(conn):
    """Indexed and unindexed paths must produce the same final state for
    the same sequence of inserts. (Behavioural equivalence guard.)"""
    cur = conn.cursor()
    vecs = {
        "A": np.array([1.0, 0.0], dtype=np.float32),
        "B": np.array([0.0, 1.0], dtype=np.float32),
        "C": np.array([0.8, 0.6], dtype=np.float32) / np.linalg.norm([0.8, 0.6]),
    }
    emb = _ScriptedEmbedder(2, vecs)
    idx = EntityIndex()
    for surface in ("A", "B", "C", "A"):
        find_or_create_entity(
            cur, entity_type="COMPANY", surface_name=surface,
            embedder=emb, now_iso="2026-05-24T00:00:00+00:00", index=idx,
        )
    cur.execute("SELECT canonical_name FROM entities WHERE entity_type='COMPANY' ORDER BY id")
    indexed_names = [r[0] for r in cur.fetchall()]

    # Reset, run again without the index.
    cur.execute("DELETE FROM entities")
    cur.execute("DELETE FROM entity_aliases")
    for surface in ("A", "B", "C", "A"):
        find_or_create_entity(
            cur, entity_type="COMPANY", surface_name=surface,
            embedder=emb, now_iso="2026-05-24T00:00:00+00:00", index=None,
        )
    cur.execute("SELECT canonical_name FROM entities WHERE entity_type='COMPANY' ORDER BY id")
    unindexed_names = [r[0] for r in cur.fetchall()]
    assert indexed_names == unindexed_names


def test_mixed_dim_existing_rows_raises(conn):
    """If the entities table has rows with mixed-dim embeddings (different
    embedder versions), the lookup fails loudly rather than silently mixing
    vector spaces."""
    v_short = np.array([1.0, 0.0], dtype=np.float32)
    v_long = np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO entities (entity_type, canonical_name, embedding, embedder_id, "
        "first_seen_at, last_seen_at) "
        "VALUES ('COMPANY', 'A', ?, 'v1@2', '2026-01-01', '2026-01-01')",
        (v_short.tobytes(),),
    )
    cur.execute(
        "INSERT INTO entities (entity_type, canonical_name, embedding, embedder_id, "
        "first_seen_at, last_seen_at) "
        "VALUES ('COMPANY', 'B', ?, 'v2@4', '2026-01-01', '2026-01-01')",
        (v_long.tobytes(),),
    )
    emb = _ScriptedEmbedder(4, {"C": v_long})
    with pytest.raises(RuntimeError, match="mixed-dimensional"):
        find_or_create_entity(
            cur, entity_type="COMPANY", surface_name="C",
            embedder=emb, now_iso="2026-05-23T00:00:00+00:00",
        )
