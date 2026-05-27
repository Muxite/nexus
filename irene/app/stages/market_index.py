"""In-process cosine-search index over active market embeddings.

Extracted from `app/stages/markets.py` so the stage module can shrink and
the index can be unit-tested in isolation. Re-exported from `markets.py`
for backwards-compatible imports.

**Documented exception to modularization rule 5** ("no shared mutable
state across messages"). `MarketIndex` is process-local mutable state by
design: cosine math wants ndarray locality, and rebuilding the matrix per
query would dominate the match-stage budget. The state lives on a
`MarketIndex` INSTANCE — not at module scope — and mutation is guarded by
an `asyncio.Lock`. This exception is named in `docs/modularization.md`
rule 5; no other stage adds a similar exception without a reports/ entry.

**Invariants.**
- Every market row gets an `embedder_id` of the form `"<model>@<dim>"` the
  moment its embedding is written; rows with `embedding IS NULL` have a
  NULL `embedder_id`. Mixed `embedder_id` values across the index is the
  signature of an in-flight model rotation — `MarketIndex.load()` rejects
  that state explicitly rather than silently mixing vector spaces.
- `MarketIndex.topk()` returns at most `k` results, every result has
  `similarity >= floor`, sorted by descending similarity. Empty index ⇒ `[]`.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Optional

import numpy as np

from app.db import DB

logger = logging.getLogger(__name__)


class MarketIndexEmbedderMismatch(RuntimeError):
    """Raised by `MarketIndex.load()` when rows carry mixed `embedder_id`s.

    A mixed state is the signature of a mid-flight model rotation; mixing
    vector spaces in a cosine search silently returns garbage (a seulgi
    lesson). The stage catches this, re-runs the embedding refresh, and
    retries.
    """


@dataclass
class _IndexSnapshot:
    """Immutable view of the index used by `topk()` so a concurrent
    `load()` can swap the live state without corrupting an in-flight search."""

    condition_ids: tuple[str, ...]
    matrix: Optional[np.ndarray]   # shape (N, dim) or None when empty
    embedder_id: Optional[str]


class MarketIndex:
    """In-process cosine-search index over active market embeddings.

    Process-local mutable state by design — see the module docstring for
    the rule-5 exception.
    """

    def __init__(self, embedder, dim: int) -> None:
        self._embedder = embedder
        self._dim = dim
        # The snapshot pattern: `load()` builds a fresh snapshot under the
        # lock and atomically replaces `self._snapshot`. `topk()` reads the
        # reference once (atomic in CPython) and works off that view.
        self._snapshot = _IndexSnapshot(condition_ids=(), matrix=None, embedder_id=None)
        self._lock = asyncio.Lock()

    @property
    def size(self) -> int:
        return len(self._snapshot.condition_ids)

    @property
    def loaded_embedder_id(self) -> Optional[str]:
        return self._snapshot.embedder_id

    async def load(self, db: DB) -> int:
        """Read every active embedded market into memory.

        Returns the number of rows loaded. Raises
        `MarketIndexEmbedderMismatch` if the active set spans more than one
        `embedder_id` value.
        """
        async with self._lock:
            cur = await db.conn.execute(
                """
                SELECT condition_id, embedding, embedder_id
                FROM markets
                WHERE active = 1
                  AND embedding IS NOT NULL
                  AND embedder_id IS NOT NULL
                """
            )
            rows = await cur.fetchall()
            await cur.close()

            if not rows:
                self._snapshot = _IndexSnapshot(condition_ids=(), matrix=None, embedder_id=None)
                return 0

            seen_ids = {r["embedder_id"] for r in rows}
            if len(seen_ids) > 1:
                raise MarketIndexEmbedderMismatch(
                    f"markets table holds {len(seen_ids)} embedder_id values "
                    f"({sorted(seen_ids)!r}); re-embed before loading the index"
                )
            (the_id,) = seen_ids

            condition_ids: list[str] = []
            vectors: list[np.ndarray] = []
            for r in rows:
                blob = r["embedding"]
                vec = np.frombuffer(blob, dtype=np.float32)
                if vec.shape[0] != self._dim:
                    # Defensive: if a row's blob doesn't match dim, the table
                    # has rotted somehow. Skip the row and log loudly rather
                    # than build a matrix with a ragged stack.
                    logger.warning(
                        "markets: skipping %s — embedding dim=%d, expected %d",
                        r["condition_id"], vec.shape[0], self._dim,
                    )
                    continue
                condition_ids.append(r["condition_id"])
                vectors.append(vec)

            if not vectors:
                self._snapshot = _IndexSnapshot(condition_ids=(), matrix=None, embedder_id=the_id)
                return 0

            matrix = np.stack(vectors).astype(np.float32, copy=False)
            self._snapshot = _IndexSnapshot(
                condition_ids=tuple(condition_ids),
                matrix=matrix,
                embedder_id=the_id,
            )
            return len(condition_ids)

    def topk(
        self,
        query_embedding: np.ndarray,
        k: int,
        floor: float,
    ) -> list[tuple[str, float]]:
        """Return up to `k` `(condition_id, similarity)` pairs above `floor`.

        Reads the live snapshot once — concurrent `load()` calls produce a
        new snapshot reference rather than mutating the one we're reading.
        """
        snap = self._snapshot  # single read; atomic in CPython
        if snap.matrix is None or len(snap.condition_ids) == 0:
            return []

        q = np.asarray(query_embedding, dtype=np.float32).reshape(-1)
        if q.shape[0] != self._dim:
            raise ValueError(
                f"query embedding dim {q.shape[0]} does not match index dim {self._dim}"
            )

        scores = snap.matrix @ q
        order = np.argsort(-scores)
        out: list[tuple[str, float]] = []
        for idx in order:
            sim = float(scores[idx])
            if sim < floor:
                break
            out.append((snap.condition_ids[idx], sim))
            if len(out) >= k:
                break
        return out
