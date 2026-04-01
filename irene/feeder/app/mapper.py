"""
Signal → Market mapper.

Loads active market embeddings from PostgreSQL (cached, refreshed every
CACHE_TTL seconds) and finds markets that are semantically similar to an
incoming signal via cosine similarity.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Optional

import numpy as np

from irene_shared.config import SignalsSection
from irene_shared.db import IreneDB
from irene_shared.models import Direction, MappedSignal, RawSignal

from direction import DirectionClassifier, DirectionResult

logger = logging.getLogger(__name__)

_CACHE_TTL = 60  # seconds between market index refreshes


class _MarketCache:
    """In-memory cache of active market embeddings."""

    def __init__(self) -> None:
        self._records: list[dict] = []
        self._matrix: Optional[np.ndarray] = None  # shape (N, dim)
        self._loaded_at: float = 0.0

    def is_stale(self) -> bool:
        return time.monotonic() - self._loaded_at > _CACHE_TTL

    def load(self, records: list[dict]) -> None:
        """
        Populate cache from DB records.

        Each record must have: condition_id, question, token_yes, token_no,
        end_date, category, embedding (np.ndarray).
        """
        valid = [r for r in records if r.get("embedding") is not None]
        if not valid:
            self._records = []
            self._matrix = None
            self._loaded_at = time.monotonic()
            return

        self._records = valid
        self._matrix = np.stack([r["embedding"] for r in valid])  # (N, dim)
        self._loaded_at = time.monotonic()
        logger.debug("MarketCache: loaded %d markets", len(valid))

    def find_matches(
        self,
        query_embedding: np.ndarray,
        threshold: float,
        top_k: int,
    ) -> list[tuple[dict, float]]:
        """
        Return up to top_k markets with cosine similarity >= threshold.

        Returns list of (market_record, similarity_score) sorted descending.
        """
        if self._matrix is None or len(self._records) == 0:
            return []

        sims: np.ndarray = self._matrix @ query_embedding  # (N,)
        # Get indices above threshold, sorted descending
        above = np.where(sims >= threshold)[0]
        if len(above) == 0:
            return []

        sorted_idx = above[np.argsort(sims[above])[::-1]][:top_k]
        return [(self._records[i], float(sims[i])) for i in sorted_idx]


class SignalMapper:
    """
    Maps a RawSignal to zero or more MappedSignal objects.

    Maintains an in-memory market embedding cache, refreshed from the DB
    every _CACHE_TTL seconds.
    """

    def __init__(self, db: IreneDB) -> None:
        self._db = db
        self._cache = _MarketCache()
        self._classifier = DirectionClassifier()
        self._refresh_lock = asyncio.Lock()

    async def map(
        self,
        signal: RawSignal,
        signal_id: int,
        query_embedding: np.ndarray,
        cfg: SignalsSection,
    ) -> list[MappedSignal]:
        """
        Find markets relevant to `signal` and classify direction.

        Args:
            signal:          The raw signal to map.
            signal_id:       DB id of the stored signal row.
            query_embedding: Pre-computed embedding for the signal text.
            cfg:             Current SignalsSection config (for thresholds).

        Returns:
            List of MappedSignal (may be empty if no market matches).
        """
        await self._maybe_refresh()

        matches = self._cache.find_matches(
            query_embedding,
            threshold=cfg.similarity_threshold,
            top_k=cfg.top_k_markets,
        )
        if not matches:
            return []

        # Classify direction once for the signal text
        signal_text = f"{signal.title} {signal.description or ''}".strip()
        direction_result: DirectionResult = self._classifier.classify(
            signal_text,
            extra_yes_keywords=cfg.yes_keywords,
            extra_no_keywords=cfg.no_keywords,
            min_confidence=cfg.direction_min_confidence,
        )

        now = datetime.now(timezone.utc)
        results = []

        for market, score in matches:
            token_yes = market.get("token_yes")
            token_no = market.get("token_no")

            # Skip if we don't have token IDs — can't trade without them
            if not token_yes or not token_no:
                logger.debug(
                    "Skipping market %s: missing token IDs", market["condition_id"]
                )
                continue

            results.append(
                MappedSignal(
                    signal_id=signal_id,
                    condition_id=market["condition_id"],
                    token_yes=token_yes,
                    token_no=token_no,
                    question=market["question"],
                    similarity_score=score,
                    direction=direction_result.direction,
                    direction_confidence=direction_result.confidence,
                    signal_title=signal.title,
                    signal_source=signal.source_detail,
                    signal_published_at=signal.published_at,
                    mapped_at=now,
                )
            )

        return results

    # ------------------------------------------------------------------
    # Cache management
    # ------------------------------------------------------------------

    async def _maybe_refresh(self) -> None:
        if not self._cache.is_stale():
            return
        async with self._refresh_lock:
            if not self._cache.is_stale():
                return  # another coroutine refreshed while we waited
            records = await self._db.get_active_markets_with_embeddings()
            self._cache.load(records)
