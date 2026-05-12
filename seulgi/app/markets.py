"""
Market index for Seulgi.

Responsibilities:
  1. Fetch all open Polymarket markets from the Gamma REST API.
  2. Embed each market question with all-MiniLM-L6-v2.
  3. Persist markets + embeddings to SQLite.
  4. Maintain an in-memory cosine similarity index (numpy matrix).
  5. Expose find_matches(headline_embedding) → list of top-K markets.

The index is rebuilt every `refresh_hours` hours so new markets are picked up
without a restart. Embedding is the only slow step; it runs in a thread
executor to avoid blocking the event loop.

Similarity search is a simple matrix dot-product (O(N * dim)); at typical
Polymarket scale (~2000 markets, dim=384) this takes <5ms.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import aiohttp
import numpy as np

from config import Config
from profiler import prof
from db import SeulgiDB

logger = logging.getLogger(__name__)

_GAMMA_URL = "https://gamma-api.polymarket.com/markets"

# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass
class Market:
    """A Polymarket market with its embedding."""
    condition_id: str
    question: str
    token_yes: Optional[str]
    token_no: Optional[str]


@dataclass
class MarketMatch:
    """A market that matched a headline, with its similarity score."""
    condition_id: str
    question: str
    token_yes: Optional[str]
    token_no: Optional[str]
    similarity: float


# ---------------------------------------------------------------------------
# Gamma API helpers
# ---------------------------------------------------------------------------


async def _fetch_gamma_markets(session: aiohttp.ClientSession) -> list[dict]:
    """
    Fetch all open markets from the Gamma API, paginating automatically.

    Returns raw dicts straight from the API. Stops early if a page is empty.
    """
    markets = []
    offset = 0
    page_size = 200

    while True:
        params = {
            "active": "true",
            "closed": "false",
            "limit": str(page_size),
            "offset": str(offset),
        }
        try:
            async with session.get(_GAMMA_URL, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                resp.raise_for_status()
                page = await resp.json()
        except Exception as exc:
            logger.warning("Gamma API error at offset %d: %s", offset, exc)
            break

        if not isinstance(page, list) or not page:
            break

        markets.extend(page)
        logger.debug("Fetched %d markets (offset=%d)", len(page), offset)

        if len(page) < page_size:
            break
        offset += page_size

    logger.info("Gamma API: fetched %d total open markets", len(markets))
    return markets


async def _fetch_gamma_markets_capped(
    session: aiohttp.ClientSession, max_markets: int = 0
) -> list[dict]:
    """Fetch markets and cap to top-N by volume (0 = no cap)."""
    markets = await _fetch_gamma_markets(session)
    if max_markets > 0 and len(markets) > max_markets:
        markets.sort(key=lambda r: float(r.get("volume", 0) or 0), reverse=True)
        markets = markets[:max_markets]
        logger.info("Gamma API: trimmed to top %d markets by volume", max_markets)
    return markets


def _parse_market(raw: dict) -> Optional[Market]:
    """
    Extract the fields we care about from a raw Gamma API market dict.

    Returns None if condition_id or question is missing.
    Token IDs come from clobTokenIds (the correct field — tokens[] is always empty).
    """
    condition_id = raw.get("conditionId") or raw.get("condition_id") or ""
    question = raw.get("question") or ""
    if not condition_id or not question:
        return None

    # clobTokenIds is the authoritative source (tokens[] is always [])
    clob_ids = raw.get("clobTokenIds") or []
    token_yes = str(clob_ids[0]) if len(clob_ids) >= 1 and clob_ids[0] else None
    token_no = str(clob_ids[1]) if len(clob_ids) >= 2 and clob_ids[1] else None

    return Market(
        condition_id=condition_id,
        question=question,
        token_yes=token_yes,
        token_no=token_no,
    )


# ---------------------------------------------------------------------------
# Embedder (thin wrapper around sentence-transformers)
# ---------------------------------------------------------------------------


class Embedder:
    """
    Synchronous sentence-transformer embedder.

    Load once at startup. Call embed() from a thread executor inside asyncio:
        vec = await loop.run_in_executor(None, embedder.embed, text)
    """

    _MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

    def __init__(self) -> None:
        self._model = None

    def load(self) -> None:
        """Load the model. Blocks for ~2s on first run (downloads ~80MB)."""
        try:
            from sentence_transformers import SentenceTransformer  # type: ignore
        except ImportError:
            raise ImportError("Install sentence-transformers: pip install sentence-transformers")
        logger.info("Loading embedding model %s …", self._MODEL_NAME)
        self._model = SentenceTransformer(self._MODEL_NAME)
        logger.info("Embedding model ready (dim=%d)", self._model.get_sentence_embedding_dimension())

    @prof.traced("markets/embed")
    def embed(self, text: str) -> np.ndarray:
        """Embed a single string → normalised float32 vector of shape (384,)."""
        if self._model is None:
            self.load()
        vec = self._model.encode(text, normalize_embeddings=True, show_progress_bar=False)
        return vec.astype(np.float32)

    def embed_batch(self, texts: list[str]) -> np.ndarray:
        """Embed multiple strings at once → shape (N, 384)."""
        if not texts:
            return np.zeros((0, 384), dtype=np.float32)
        if self._model is None:
            self.load()
        vecs = self._model.encode(
            texts, normalize_embeddings=True, show_progress_bar=False, batch_size=64
        )
        return vecs.astype(np.float32)


# ---------------------------------------------------------------------------
# Market index
# ---------------------------------------------------------------------------


class MarketIndex:
    """
    In-memory cosine similarity index over all embedded Polymarket markets.

    The index is a numpy matrix of shape (N, 384). Finding the top-K matches
    for a headline is a single matrix multiply: scores = matrix @ query.

    Rebuilt periodically by calling refresh() from the background loop.
    """

    def __init__(self, db: SeulgiDB, embedder: Embedder, cfg: Config) -> None:
        self._db = db
        self._embedder = embedder
        self._cfg = cfg

        # These are updated atomically on each refresh
        self._markets: list[Market] = []
        self._matrix: Optional[np.ndarray] = None  # shape (N, 384)
        self._last_refresh: float = 0.0
        self._heat_scores: dict[str, int] = {}  # condition_id → match_count

    @property
    def market_count(self) -> int:
        return len(self._markets)

    @property
    def needs_refresh(self) -> bool:
        refresh_interval = self._cfg.markets.refresh_hours * 3600
        return (time.monotonic() - self._last_refresh) >= refresh_interval

    async def refresh(self) -> int:
        """
        Fetch markets from Gamma API, embed questions, store in DB, rebuild index.

        Returns the number of markets in the updated index.

        This is the heavy operation — runs once at startup and then every
        `refresh_hours` hours. Embedding is done in a thread executor.
        """
        logger.info("MarketIndex: refreshing …")

        async with aiohttp.ClientSession() as session:
            raw_markets = await _fetch_gamma_markets_capped(
                session, max_markets=self._cfg.markets.max_tracked_markets
            )

        markets: list[Market] = []
        for raw in raw_markets:
            m = _parse_market(raw)
            if m and (m.token_yes or m.token_no):
                markets.append(m)

        logger.info("MarketIndex: %d markets with tokens (of %d fetched)", len(markets), len(raw_markets))

        if not markets:
            logger.warning("MarketIndex: no valid markets — keeping old index")
            return self.market_count

        # Embed all questions in one batch (fast: ~2s for 2000 markets)
        loop = asyncio.get_event_loop()
        questions = [m.question for m in markets]
        embeddings = await loop.run_in_executor(None, self._embedder.embed_batch, questions)

        # Persist to DB and build in-memory matrix
        for market, emb in zip(markets, embeddings):
            await self._db.upsert_market(
                condition_id=market.condition_id,
                question=market.question,
                token_yes=market.token_yes,
                token_no=market.token_no,
                end_date=None,
                volume_usdc=None,
                embedding=emb,
            )

        self._markets = markets
        self._matrix = embeddings  # shape (N, 384)
        self._last_refresh = time.monotonic()

        logger.info("MarketIndex: index built with %d markets", len(markets))
        return len(markets)

    async def load_heat_from_db(self) -> None:
        """Load market heat scores (match_count) from DB into memory."""
        async with self._db._conn.execute(
            "SELECT condition_id, match_count FROM markets WHERE match_count > 0"
        ) as cur:
            rows = await cur.fetchall()
        self._heat_scores = {r["condition_id"]: r["match_count"] for r in rows}

    async def load_from_db(self) -> int:
        """
        Load the market index from the database (used on startup if DB exists).

        Falls back to an empty index if the DB has no markets.
        """
        rows = await self._db.get_markets_with_embeddings()
        if not rows:
            return 0

        self._markets = [
            Market(
                condition_id=r["condition_id"],
                question=r["question"],
                token_yes=r["token_yes"],
                token_no=r["token_no"],
            )
            for r in rows
        ]
        self._matrix = np.stack([r["embedding"] for r in rows])  # (N, 384)
        logger.info("MarketIndex: loaded %d markets from DB", len(self._markets))
        return len(self._markets)

    @prof.traced("markets/search")
    def find_matches(self, query_embedding: np.ndarray) -> list[MarketMatch]:
        """
        Find the top-K markets most similar to the given embedding.

        Returns up to cfg.markets.top_k results above cfg.markets.similarity_threshold,
        sorted by descending similarity.

        O(N * dim) — negligible at typical Polymarket scale.
        """
        if self._matrix is None or len(self._markets) == 0:
            return []

        # Single matrix multiply: shape (N,)
        scores = self._matrix @ query_embedding

        # Apply heat boost — frequently-matched markets score slightly higher (capped at +5%)
        if self._heat_scores:
            for i, m in enumerate(self._markets):
                heat = self._heat_scores.get(m.condition_id, 0)
                if heat > 0:
                    scores[i] = min(1.0, float(scores[i]) + min(heat / 100.0, 0.05))

        # Filter and sort
        threshold = self._cfg.markets.similarity_threshold
        top_k = self._cfg.markets.top_k

        # Get indices sorted by score descending
        order = np.argsort(-scores)
        results = []
        for idx in order:
            sim = float(scores[idx])
            if sim < threshold:
                break
            market = self._markets[idx]
            results.append(MarketMatch(
                condition_id=market.condition_id,
                question=market.question,
                token_yes=market.token_yes,
                token_no=market.token_no,
                similarity=sim,
            ))
            if len(results) >= top_k:
                break

        return results
