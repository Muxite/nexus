"""
## Specification

**Purpose.** Cosine-based entity deduplication. Resolves a surface-form
name to an existing `entities.id` when the surface form is semantically
close enough (cosine ≥ threshold, partitioned by `entity_type`) to an
already-stored entity. Inserts a new row otherwise. Same canonical-name
hits still use the existing UNIQUE constraint as a fast path; SBERT
fires only when that misses.

The motivation comes from the 1k bulk-extraction validation
(`reports/2026-05-23-irene-dkg-bulk-1k-validation.md`): the model
emits surface variants like `Putin`/`Vladimir Putin`, `US`/`U.S.`,
`Trump` (no obvious dup yet but the long tail will explode at 50k+).
Strict UNIQUE-on-canonical lets these proliferate; SBERT cosine
collapses them while preserving the original surface form as an
alias.

**Public interface.**
- `find_or_create_entity(cur, *, entity_type, surface_name, embedder,
  now_iso, threshold=0.85, alias_kind='synonym') -> (entity_id, match_kind)`
  — sync, takes a `sqlite3.Cursor`. Returns the `entities.id` plus a
  string in `{'exact_canonical', 'sbert_merge', 'inserted_new'}`
  describing how the lookup resolved. The caller is responsible for
  the transaction boundary (we don't commit).
- `score_against_existing(query_vec, candidate_vecs) -> (best_idx, best_cos)`
  — pure-compute helper used by `find_or_create_entity` and exposed
  separately so callers (and tests) can score without a DB.

**Why sync.** The replay script is sync; sharing this code with the
live (async) ExtractStage will require either a thread-offload wrapper
or an async sibling at step 6 of the bulk-population plan. Keeping the
sync core single-sourced now avoids divergence later.

**Invariants.**
- `entity_type` partitions the cosine search. A surface form never
  merges across types; an "Apple" COMPANY and an "Apple" EVENT stay
  distinct (locked decision in
  `reports/2026-05-23-irene-dkg-prompt-tuning.md` §"Open questions").
- The first match — fast-path exact canonical — does not require an
  embedding. The DB ALREADY guarantees uniqueness on
  `(entity_type, canonical_name)`; we just look it up before computing
  any vectors. This means rows written before SBERT was introduced
  (no embedding) are still findable by exact name.
- A new entity row written by this function carries an embedding AND
  an `embedder_id`. The two together are the cheap guard against
  embedder-version drift (same lesson as `entities.embedder_id` on the
  markets table).
- When SBERT merges to an existing entity whose canonical differs from
  the surface form, the surface form is written to `entity_aliases`
  with `alias_kind=alias_kind` so future strict lookups still resolve.
- Cosine math runs in `float32`. Embeddings are stored as
  `np.float32.tobytes()` and read back via `np.frombuffer`.

**Side effects.** Reads `entities`. Writes `entities` (one INSERT on
`inserted_new`) and `entity_aliases` (one INSERT OR IGNORE on
`sbert_merge` when the surface form differs from the canonical).

**Failure modes.**
- `ValueError` if `surface_name` is empty after strip, or
  `entity_type` is empty.
- `EmbedderDimMismatch` propagates from the embedder if the loaded
  model's dim doesn't match the configured dim.
- `sqlite3.OperationalError` propagates if the `entities` table is
  missing the `embedding`/`embedder_id` columns (forgot to register
  the updated DDL).

**Non-goals.**
- Not a stage. Owns no tables. The `embedding`/`embedder_id` columns
  live on `entities`, owned by `app/stages/extract.py`.
- Doesn't do alias-based or acronym-based resolution. That's
  `app/stages/resolve.py`'s job — and it runs over the merged
  entities AFTER this function has done its work.
- Doesn't normalise the surface form (no suffix stripping, no
  lowercasing). The caller passes whatever the LLM emitted; we let
  the embedder do the semantic work.
"""

from __future__ import annotations

import logging
import sqlite3
from typing import TYPE_CHECKING, Iterable, Literal, Optional

import numpy as np

if TYPE_CHECKING:
    from app.embed import Embedder

logger = logging.getLogger(__name__)


MatchKind = Literal["exact_canonical", "sbert_merge", "inserted_new"]


def score_against_existing(
    query_vec: np.ndarray,
    candidate_vecs: np.ndarray,
) -> tuple[int, float]:
    """Return `(best_idx, best_cosine)` of `query_vec` against `candidate_vecs`.

    `query_vec` is shape `(dim,)`, `candidate_vecs` is shape `(N, dim)`.
    Both are assumed normalised (sentence-transformers `Embedder` ships
    them that way by default — `normalize_embeddings=True`). If `N == 0`,
    returns `(-1, 0.0)`.

    The cosine is computed as a dot product because both sides are
    already unit-length; this saves a divide and matches the math used
    elsewhere in the codebase (see `app/stages/markets.py` `MarketIndex`).
    """
    if candidate_vecs.shape[0] == 0:
        return -1, 0.0
    if query_vec.shape[0] != candidate_vecs.shape[1]:
        raise ValueError(
            f"dim mismatch: query {query_vec.shape[0]} vs "
            f"candidates {candidate_vecs.shape[1]}"
        )
    sims = candidate_vecs @ query_vec
    best_idx = int(np.argmax(sims))
    return best_idx, float(sims[best_idx])


def _fetch_candidates(
    cur: sqlite3.Cursor, entity_type: str
) -> tuple[list[int], list[str], np.ndarray]:
    """Pull every entity of `entity_type` that carries an embedding.

    Entities without an embedding (legacy rows from before SBERT, or
    rows whose insertion path didn't stamp one) are excluded from the
    cosine pool — they're still findable via the exact-canonical fast
    path. Returns `(ids, canonicals, vecs)` where `vecs.shape == (N, dim)`.
    """
    cur.execute(
        "SELECT id, canonical_name, embedding, embedder_id FROM entities "
        "WHERE entity_type = ? AND embedding IS NOT NULL",
        (entity_type,),
    )
    rows = cur.fetchall()
    if not rows:
        return [], [], np.zeros((0, 0), dtype=np.float32)
    ids: list[int] = []
    names: list[str] = []
    vec_list: list[np.ndarray] = []
    dim: Optional[int] = None
    for row in rows:
        # sqlite3.Row indexing accepts both int and str; fall back to int
        # so this works whether the caller set row_factory or not.
        eid = row[0]
        canonical = row[1]
        blob = row[2]
        vec = np.frombuffer(blob, dtype=np.float32)
        if dim is None:
            dim = vec.shape[0]
        elif vec.shape[0] != dim:
            # Embedder change is a deployment-level event; we'd rather
            # crash and re-embed than silently mix vector spaces.
            raise RuntimeError(
                f"entities table has mixed-dimensional embeddings for "
                f"entity_type={entity_type!r}: saw {dim} then "
                f"{vec.shape[0]} on id={eid}"
            )
        ids.append(int(eid))
        names.append(str(canonical))
        vec_list.append(vec)
    return ids, names, np.stack(vec_list)


def find_or_create_entity(
    cur: sqlite3.Cursor,
    *,
    entity_type: str,
    surface_name: str,
    embedder: "Embedder",
    now_iso: str,
    threshold: float = 0.85,
    alias_kind: str = "synonym",
) -> tuple[int, MatchKind]:
    """Resolve `surface_name` to an existing `entities.id` or insert a new row.

    See module docstring for full semantics. The caller owns the
    transaction (no commit here)."""
    if not entity_type:
        raise ValueError("entity_type must be non-empty")
    name = surface_name.strip()
    if not name:
        raise ValueError("surface_name must be non-empty after strip")

    # ---- fast path: exact canonical_name within this type --------------
    cur.execute(
        "SELECT id FROM entities WHERE entity_type = ? AND canonical_name = ?",
        (entity_type, name),
    )
    row = cur.fetchone()
    if row is not None:
        eid = int(row[0])
        cur.execute(
            "UPDATE entities SET last_seen_at = ? WHERE id = ?",
            (now_iso, eid),
        )
        return eid, "exact_canonical"

    # ---- SBERT path: embed + cosine vs same-type pool ------------------
    query_vec = embedder.embed(name).astype(np.float32, copy=False)
    ids, names, candidate_vecs = _fetch_candidates(cur, entity_type)
    if candidate_vecs.shape[0] > 0:
        best_idx, best_cos = score_against_existing(query_vec, candidate_vecs)
        if best_cos >= threshold:
            eid = ids[best_idx]
            cur.execute(
                "UPDATE entities SET last_seen_at = ? WHERE id = ?",
                (now_iso, eid),
            )
            # Surface form differs from the canonical → preserve it as an
            # alias so future strict (non-SBERT) lookups still resolve.
            canonical = names[best_idx]
            if name != canonical:
                cur.execute(
                    "INSERT OR IGNORE INTO entity_aliases "
                    "(entity_id, alias, alias_kind) VALUES (?, ?, ?)",
                    (eid, name, alias_kind),
                )
            return eid, "sbert_merge"

    # ---- no match → insert -------------------------------------------
    cur.execute(
        """
        INSERT INTO entities
            (entity_type, canonical_name, embedding, embedder_id,
             first_seen_at, last_seen_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            entity_type,
            name,
            query_vec.tobytes(),
            embedder.embedder_id,
            now_iso,
            now_iso,
        ),
    )
    new_id = cur.lastrowid
    if new_id is None:
        # sqlite3 sets lastrowid on INSERT; defensive only.
        raise RuntimeError(
            f"INSERT into entities returned no rowid for "
            f"({entity_type!r}, {name!r})"
        )
    return int(new_id), "inserted_new"
