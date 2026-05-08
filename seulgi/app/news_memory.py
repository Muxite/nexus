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
from datetime import datetime, timezone
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)


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
    ) -> None:
        self._embedder = embedder
        self._threshold = threshold
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
        """
        Check if a headline is a republish of something already seen.

        Returns:
            (is_republish, similar_headline_text)
            is_republish=False and similar_to=None when dedup is disabled or
            collection is empty.

        Side effect: adds the headline to the collection if it is NOT a republish.
        """
        if not self._enabled or self._collection is None:
            return False, None

        vec = embedding.tolist()

        with self._lock:
            # Query for top-3 nearest neighbours
            try:
                n_results = min(3, max(1, self._collection.count()))
                results = self._collection.query(
                    query_embeddings=[vec],
                    n_results=n_results,
                    include=["documents", "distances"],
                )
            except Exception as exc:
                logger.debug("[news_memory] query error: %s", exc)
                return False, None

            distances = results.get("distances", [[]])[0]
            documents = results.get("documents", [[]])[0]

            if distances and distances[0] < self._threshold:
                similar = documents[0] if documents else None
                logger.debug(
                    "[news_memory] republish detected dist=%.3f  orig=%s  new=%s",
                    distances[0], (similar or "")[:60], headline[:60],
                )
                return True, similar

            # Not a republish — add to collection
            try:
                self._collection.add(
                    documents=[headline],
                    embeddings=[vec],
                    ids=[uuid.uuid4().hex],
                    metadatas=[{
                        "site_url": site_url,
                        "article_url": article_url,
                        "detected_at": datetime.now(timezone.utc).isoformat(),
                    }],
                )
            except Exception as exc:
                logger.debug("[news_memory] add error: %s", exc)

        return False, None

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
