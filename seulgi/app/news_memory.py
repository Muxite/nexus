"""
Semantic news deduplication using ChromaDB.

Maintains a persistent vector store of every headline seen. When a new headline
arrives, it is checked against near-neighbours. If a similar headline has already
been seen (cosine distance < threshold), the new headline is marked as a republish.

The AI classifier receives this context and can choose to return NEUTRAL for
republishes, preventing the system from acting on stale news.

Collection: "headlines"
Distance:   cosine (HNSW)
Capacity:   ~500k documents (ChromaDB handles this via HNSW index)

Thread safety: check() is synchronous and serialises writes via a threading.Lock.
               It is safe to call from asyncio.run_in_executor().

Usage:
    memory = NewsMemory(
        path="/data/news_chroma",
        embedder=embedder,       # reuse seulgi's existing Embedder instance
        threshold=0.12,          # cosine distance; 0.12 ≈ cosine sim 0.88
    )
    is_republish, similar_to = memory.check(
        headline="Fed raises rates by 25bp",
        embedding=vec,
        site_url="https://reuters.com",
        article_url="https://reuters.com/article/123",
    )
"""

from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal, Optional

import numpy as np

logger = logging.getLogger(__name__)


FreshnessLabel = Literal["BREAKING", "EVOLVING", "REHASH", "STALE_RETURN"]


@dataclass
class FreshnessResult:
    """Output of NewsMemory.assess — labels every headline by freshness.

    Used by the runner to short-circuit Stage-2/3 on REHASH and to inject
    prior-coverage context into Stage-2 / Stage-3 prompts.
    """
    label: FreshnessLabel = "BREAKING"
    is_republish: bool = False              # back-compat with check()
    nearest_dist: Optional[float] = None    # cosine distance to closest prior
    nearest_age_secs: Optional[float] = None  # age of closest prior (seconds)
    similar_to: Optional[str] = None        # nearest headline text
    # Top-N semantically similar headlines from the last recent window:
    # [{"headline": str, "site_url": str, "age_secs": float, "dist": float}]
    recent_context: list[dict] = field(default_factory=list)


def _parse_iso(s: str) -> Optional[datetime]:
    """Best-effort ISO-8601 parse; returns tz-aware UTC datetime or None."""
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


class NewsMemory:
    """
    Persistent semantic deduplication store for news headlines.

    Reuses the existing Embedder instance — no second model load.
    """

    def __init__(
        self,
        path: str,
        embedder,           # markets.Embedder — avoids circular import
        threshold: float = 0.12,
        fresh_dist_threshold: float = 0.30,
        rehash_age_secs: float = 21600.0,
        stale_age_secs: float = 86400.0,
        recent_context_window_secs: float = 86400.0,
        recent_context_n: int = 3,
        short_circuit_labels: Optional[list] = None,
    ) -> None:
        self._embedder = embedder
        self._threshold = threshold
        self._fresh_dist_threshold = fresh_dist_threshold
        self._rehash_age_secs = rehash_age_secs
        self._stale_age_secs = stale_age_secs
        self._recent_context_window_secs = recent_context_window_secs
        self._recent_context_n = recent_context_n
        # Cycle-13: which labels make the runner short-circuit Stage 2 + 3.
        # Stored under a name the runner can read via attribute access.
        self._short_circuit_labels = list(
            short_circuit_labels if short_circuit_labels is not None
            else ["REHASH", "STALE_RETURN"]
        )
        self._lock = threading.Lock()
        self._collection = None
        self._enabled = False
        self._path = path
        self._init(path)

    def _init(self, path: str) -> None:
        try:
            import chromadb  # type: ignore
            client = chromadb.PersistentClient(path=path)
            self._collection = client.get_or_create_collection(
                name="headlines",
                metadata={"hnsw:space": "cosine"},
            )
            count = self._collection.count()
            self._enabled = True
            logger.info(
                "[news_memory] ChromaDB ready at %s — %d headlines stored",
                path, count,
            )
        except ImportError:
            logger.warning(
                "[news_memory] chromadb not installed — dedup disabled. "
                "Add chromadb to seulgi/requirements.txt."
            )
        except Exception as exc:
            logger.warning("[news_memory] init failed (%s) — dedup disabled", exc)

    def check(
        self,
        headline: str,
        embedding: np.ndarray,
        site_url: str,
        article_url: str,
    ) -> tuple[bool, Optional[str]]:
        """Back-compat shim — see `assess()` for the full freshness label."""
        r = self.assess(headline, embedding, site_url, article_url)
        return r.is_republish, r.similar_to

    def assess(
        self,
        headline: str,
        embedding: np.ndarray,
        site_url: str,
        article_url: str,
    ) -> FreshnessResult:
        """
        Label a headline by freshness and surface prior-coverage context.

        Decision table — using nearest-neighbour cosine distance + age:

          nearest_dist > fresh_dist_threshold (0.30)
              → BREAKING   (no close match in store)
          dedup_threshold (0.12) ≤ dist ≤ 0.30
              if age <  rehash_age_secs (6h)   → EVOLVING (new angle, fresh window)
              if age >= rehash_age_secs        → BREAKING (topic resurfaced)
          dist < dedup_threshold (0.12)
              if age <  rehash_age_secs        → REHASH (true republish, recent)
              if age >= stale_age_secs (24h)   → STALE_RETURN
              else (6h–24h)                    → EVOLVING (conservative middle)

        Side effect: adds the headline to the collection unless it's a REHASH.
        REHASH headlines are NOT added — otherwise every duplicate would
        become its own nearest-neighbour anchor and degrade future labels.
        """
        result = FreshnessResult(label="BREAKING")
        if not self._enabled or self._collection is None:
            return result

        vec = embedding.tolist()
        now = datetime.now(timezone.utc)

        with self._lock:
            try:
                count = self._collection.count()
                n_results = min(10, max(1, count))
                if count == 0:
                    # First headline ever — definitionally BREAKING.
                    self._add(headline, vec, site_url, article_url, now)
                    return result
                qres = self._collection.query(
                    query_embeddings=[vec],
                    n_results=n_results,
                    include=["documents", "distances", "metadatas"],
                )
            except Exception as exc:
                logger.debug("[news_memory] query error: %s", exc)
                return result

            distances = qres.get("distances", [[]])[0] or []
            documents = qres.get("documents", [[]])[0] or []
            metadatas = qres.get("metadatas", [[]])[0] or []

            if not distances:
                self._add(headline, vec, site_url, article_url, now)
                return result

            # Pull the freshness signal from the nearest neighbour.
            nearest_dist = float(distances[0])
            nearest_doc = documents[0] if documents else None
            nearest_meta = metadatas[0] if metadatas else {}
            nearest_at = _parse_iso(nearest_meta.get("detected_at", ""))
            nearest_age = (now - nearest_at).total_seconds() if nearest_at else None

            result.nearest_dist = nearest_dist
            result.nearest_age_secs = nearest_age
            result.similar_to = nearest_doc

            # Build recent_context: top-N within the recent window (skip self
            # — the nearest itself if it's effectively identical we still
            # want to surface as context, but cap at recent_context_n).
            ctx: list[dict] = []
            window = self._recent_context_window_secs
            for doc, dist, meta in zip(documents, distances, metadatas):
                at = _parse_iso((meta or {}).get("detected_at", ""))
                if at is None:
                    continue
                age = (now - at).total_seconds()
                if age > window:
                    continue
                ctx.append({
                    "headline": doc,
                    "site_url": (meta or {}).get("site_url"),
                    "age_secs": age,
                    "dist": float(dist),
                })
                if len(ctx) >= self._recent_context_n:
                    break
            result.recent_context = ctx

            # Apply the decision table.
            if nearest_dist > self._fresh_dist_threshold:
                result.label = "BREAKING"
            elif nearest_dist < self._threshold:
                if nearest_age is None or nearest_age < self._rehash_age_secs:
                    result.label = "REHASH"
                    result.is_republish = True
                elif nearest_age >= self._stale_age_secs:
                    result.label = "STALE_RETURN"
                else:
                    result.label = "EVOLVING"
            else:  # in [threshold, fresh_dist_threshold]
                if nearest_age is None or nearest_age < self._rehash_age_secs:
                    result.label = "EVOLVING"
                else:
                    result.label = "BREAKING"

            # Persist all non-REHASH; REHASH would self-anchor and degrade signal.
            if result.label != "REHASH":
                self._add(headline, vec, site_url, article_url, now)
            else:
                logger.debug(
                    "[news_memory] REHASH dist=%.3f age=%ss orig=%s new=%s",
                    nearest_dist, int(nearest_age or 0),
                    (nearest_doc or "")[:60], headline[:60],
                )

        return result

    def _add(
        self,
        headline: str,
        vec: list,
        site_url: str,
        article_url: str,
        when: datetime,
    ) -> None:
        try:
            self._collection.add(
                documents=[headline],
                embeddings=[vec],
                ids=[uuid.uuid4().hex],
                metadatas=[{
                    "site_url": site_url,
                    "article_url": article_url,
                    "detected_at": when.isoformat(),
                }],
            )
        except Exception as exc:
            logger.debug("[news_memory] add error: %s", exc)

    @property
    def size(self) -> int:
        """Number of headlines stored."""
        if not self._enabled or self._collection is None:
            return 0
        try:
            return self._collection.count()
        except Exception:
            return 0

    async def close(self) -> None:
        """No-op — ChromaDB PersistentClient flushes automatically."""
        pass
