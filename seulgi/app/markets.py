"""
Market index for Seulgi.

## Specification
**Purpose.** Polymarket gamma-api adapter + SBERT-based MarketIndex with
cosine-similarity lookup for headline → market matching. Single source of
truth for the open-market universe; sole entry point for news→market
similarity scoring.

**Public interface.**
  - `Market`, `MarketMatch` dataclasses.
  - `Embedder` (sentence-transformers wrapper; load once, share with
    `news_memory.NewsMemory`).
  - `MarketIndex.refresh()` — fetch+embed+persist+rebuild matrix.
  - `MarketIndex.load_from_db()` — boot path; rebuilds matrix from
    persisted embeddings (no network).
  - `MarketIndex.load_heat_from_db()` — populates the `match_count` heat
    dict used for the cosine boost.
  - `MarketIndex.find_matches(query_embedding) -> list[MarketMatch]` —
    sole consumer surface (engine.process at engine.py:380).
  - Module-level `_fetch_gamma_markets`, `_fetch_gamma_markets_capped`,
    `_parse_market` — used directly only by `refresh()` today.

**Invariants.**
  - `find_matches` returns ≤ `cfg.markets.top_k` results, every result has
    `similarity >= cfg.markets.similarity_threshold`, sorted by score
    descending (stable).
  - Heat boost is additive at most +0.05 (capped) and is applied BEFORE
    the threshold filter and BEFORE the argsort — so it can change which
    markets clear the threshold and the order they land in.
  - `self._matrix.shape == (len(self._markets), 384)`; rows align by
    index with `self._markets`.
  - On `refresh()`, markets not in the fresh fetch are marked
    `closed_at=now`; on `upsert_market` they're reopened
    (`closed_at = NULL`). `load_from_db` filters `closed_at IS NULL`.

**Side effects.**
  - HTTP GET to `https://gamma-api.polymarket.com/markets` (paged 100/req).
  - Loads `sentence-transformers/all-MiniLM-L6-v2` (~80MB, ~2s first run)
    on first `embed()` call.
  - Writes to `markets` table (`upsert_market`, `mark_markets_closed`,
    `prune_closed_markets` — closed > 30d hard-deleted).
  - Reads `markets.match_count` (heat). Bumped externally by
    `db.bump_market_heat` in engine.process.

**Failure modes.**
  - Gamma API 422 on offset ≥ 10100 (universe ceiling) is silently
    caught in `_fetch_gamma_markets` and treated as end-of-pagination —
    the index caps out at ~10.1k markets. There is no Kalshi/Predictit
    fallback today (Phase 4E.3).
  - Empty refresh keeps the previous in-memory matrix (log "no valid
    markets — keeping old index"); does NOT touch DB closed_at.
  - Network/timeout from gamma is logged as a warning and ends
    pagination early; partial fetches may incorrectly mark markets
    closed.

**Non-goals.**
  - Per-market quality scoring (volume/liquidity/spread gates) — Phase 4E.3.
  - Concept-link or tag-based routing — NOT IMPLEMENTED (Phase 4D.2 / 4E.2).
  - Market discovery beyond polymarket (kalshi, predictit) — Phase 4E.3.
  - Re-embedding after model swap — embeddings are written once per
    refresh from whatever model `Embedder._MODEL_NAME` is set to.
"""

from __future__ import annotations

import asyncio
import json
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
    # Coarse classification populated by scripts/postmortem/tag_market_types.py.
    # None on legacy rows or freshly-fetched markets the tagger hasn't seen yet.
    market_type: Optional[str] = None


@dataclass
class MarketMatch:
    """A market that matched a headline, with its similarity score."""
    condition_id: str
    question: str
    token_yes: Optional[str]
    token_no: Optional[str]
    similarity: float
    # Pre-fetched mid prices (populated in EngineRunner BEFORE the AI call so
    # the model sees real prices and so the trader doesn't re-fetch per engine).
    mid_yes: Optional[float] = None
    mid_no:  Optional[float] = None
    # Forwarded from `Market.market_type` so the rules engine can gate without
    # hitting the DB per match.
    market_type: Optional[str] = None
    # Phase 4D.2: when the tag-based router augmented this match in (rather
    # than the cosine top-K), `tag` is the macro category that caught it
    # (e.g. "interest_rates"). NULL for cosine-only matches. Persisted on
    # `engine_results.tag` so per-tag counterfactuals can attribute PnL.
    tag: Optional[str] = None


# ---------------------------------------------------------------------------
# Gamma API helpers
# ---------------------------------------------------------------------------


async def _fetch_gamma_markets(session: aiohttp.ClientSession) -> list[dict]:
    """
    Fetch all open markets from the Gamma API, paginating automatically.

    Page size of 100 — Gamma silently caps every response at 100 items
    regardless of the `limit` we ask for (verified 2026-05-14; previously the
    cap appeared higher). The loop terminates only when a page returns *zero*
    items; using `len(page) < page_size` as the exit condition (which we used
    to, with page_size=200) caused the very first page to short-circuit and
    return only 100 markets total, which made nearly every market in our DB
    get marked closed and the index collapse to ~93 rows.

    A hard safety cap of `max_pages` rounds prevents an unbounded loop if
    Gamma's pagination ever misbehaves.
    """
    markets = []
    offset = 0
    page_size = 100
    max_pages = 1000  # 100k markets — generous; current universe is ~50k

    for _ in range(max_pages):
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
        offset += len(page)

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


def _parse_market(raw: dict) -> Optional[tuple[Market, Optional[str]]]:
    """
    Extract the fields we care about from a raw Gamma API market dict.

    Returns (Market, end_date_iso) or None if condition_id or question is missing.
    Token IDs come from clobTokenIds (the correct field — tokens[] is always empty).
    """
    condition_id = raw.get("conditionId") or raw.get("condition_id") or ""
    question = raw.get("question") or ""
    if not condition_id or not question:
        return None

    # CRITICAL: Gamma returns clobTokenIds as a JSON-encoded *string*, not a
    # native JSON array (e.g. '["171...","987..."]'). Old code did `clob_ids[0]`
    # on that string and got just the character "[", which then fed the CLOB
    # midpoint endpoint and returned 404 for every market → 100% no_price skips.
    clob_raw = raw.get("clobTokenIds")
    clob_ids: list = []
    if isinstance(clob_raw, list):
        clob_ids = clob_raw
    elif isinstance(clob_raw, str) and clob_raw.strip().startswith("["):
        try:
            parsed_ids = json.loads(clob_raw)
            if isinstance(parsed_ids, list):
                clob_ids = parsed_ids
        except (json.JSONDecodeError, ValueError):
            clob_ids = []
    token_yes = str(clob_ids[0]) if len(clob_ids) >= 1 and clob_ids[0] else None
    token_no = str(clob_ids[1]) if len(clob_ids) >= 2 and clob_ids[1] else None
    end_date = raw.get("endDate") or raw.get("end_date") or None

    return (
        Market(
            condition_id=condition_id,
            question=question,
            token_yes=token_yes,
            token_no=token_no,
        ),
        end_date,
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
        end_dates: list[Optional[str]] = []
        for raw in raw_markets:
            parsed = _parse_market(raw)
            if parsed is None:
                continue
            m, end_date = parsed
            if m.token_yes or m.token_no:
                markets.append(m)
                end_dates.append(end_date)

        logger.info("MarketIndex: %d markets with tokens (of %d fetched)", len(markets), len(raw_markets))

        if not markets:
            logger.warning("MarketIndex: no valid markets — keeping old index")
            return self.market_count

        # Embed all questions in one batch (fast: ~2s for 2000 markets)
        loop = asyncio.get_event_loop()
        questions = [m.question for m in markets]
        embeddings = await loop.run_in_executor(None, self._embedder.embed_batch, questions)

        # Persist to DB and build in-memory matrix
        for market, emb, end_date in zip(markets, embeddings, end_dates):
            await self._db.upsert_market(
                condition_id=market.condition_id,
                question=market.question,
                token_yes=market.token_yes,
                token_no=market.token_no,
                end_date=end_date,
                volume_usdc=None,
                embedding=emb,
            )

        # Prune: anything in the DB that wasn't in this fetch is now closed
        open_ids = [m.condition_id for m in markets]
        closed_n = await self._db.mark_markets_closed(open_ids)
        if closed_n:
            logger.info("MarketIndex: marked %d markets closed (dropped from Gamma open list)", closed_n)
        # Hard-delete markets that have been closed > 30 days
        pruned = await self._db.prune_closed_markets(older_than_days=30)
        if pruned:
            logger.info("MarketIndex: hard-pruned %d markets closed > 30d", pruned)

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

        A4E.3 (2026-05-23): when `cfg.markets.min_quality_score > 0`,
        non-polymarket rows whose recomputed `discovery.quality_score`
        falls below the floor are dropped at load time. Polymarket rows
        are exempt — the gamma adapter doesn't capture liquidity/spread
        on every row so their derived score would be 0 across the board.
        """
        rows = await self._db.get_markets_with_embeddings()
        if not rows:
            return 0

        min_q = getattr(self._cfg.markets, "min_quality_score", 0.0) or 0.0
        kept_rows = rows
        if min_q > 0.0:
            try:
                from discovery import DiscoveredMarket, quality_score
                kept_rows = []
                for r in rows:
                    src = r.get("source") or "polymarket"
                    if src == "polymarket":
                        kept_rows.append(r)
                        continue
                    dm = DiscoveredMarket(
                        source=src, cond_id=r["condition_id"],
                        question=r["question"],
                        volume_24h_usdc=r.get("volume_usdc"),
                    )
                    if quality_score(dm) >= min_q:
                        kept_rows.append(r)
            except Exception as exc:  # noqa: BLE001
                logger.warning("min_quality_score filter skipped: %s", exc)
                kept_rows = rows

        self._markets = [
            Market(
                condition_id=r["condition_id"],
                question=r["question"],
                token_yes=r["token_yes"],
                token_no=r["token_no"],
                market_type=(r["market_type"] if "market_type" in r.keys() else None),
            )
            for r in kept_rows
        ]
        self._matrix = np.stack([r["embedding"] for r in rows])  # (N, 384)
        logger.info("MarketIndex: loaded %d markets from DB", len(self._markets))
        return len(self._markets)

    @prof.traced("markets/search")
    def find_matches(
        self,
        query_embedding: np.ndarray,
        headline: Optional[str] = None,
    ) -> list[MarketMatch]:
        """
        Find the top-K markets most similar to the given embedding.

        Returns up to cfg.markets.top_k results above cfg.markets.similarity_threshold,
        sorted by descending similarity.

        O(N * dim) — negligible at typical Polymarket scale.

        Phase 4D.2 tag-augment step: when `headline` is non-empty AND the
        tagger config is enabled (default), each match is run through
        `tagger.tag_headline`; any tag whose `route_market_bucket` keyword
        appears in a market's question text is unioned into the result
        (after the cosine pass). Tag-routed matches are marked with their
        triggering tag in `MarketMatch.tag`. Cosine matches keep `tag=None`.
        The total result is still capped at `cfg.markets.top_k`.
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
        results: list[MarketMatch] = []
        seen_ids: set[str] = set()
        for idx in order:
            sim = float(scores[idx])
            if sim < threshold:
                break
            market = self._markets[idx]
            seen_ids.add(market.condition_id)
            results.append(MarketMatch(
                condition_id=market.condition_id,
                question=market.question,
                token_yes=market.token_yes,
                token_no=market.token_no,
                similarity=sim,
                market_type=market.market_type,
            ))
            if len(results) >= top_k:
                break

        # ── Phase 4D.2 tag-augment ────────────────────────────────────────
        # When the headline is supplied AND the tagger is enabled, scan
        # market questions for any tag's bucket keywords. Union with cosine
        # matches (dedup by condition_id). Capped at top_k total.
        tagger_cfg = getattr(self._cfg, "tagger", None)
        if (
            headline
            and tagger_cfg is not None
            and getattr(tagger_cfg, "enabled", False)
            and len(results) < top_k
        ):
            try:
                from tagger import tag_headline, route_market_bucket
            except ImportError:
                tag_headline = None  # type: ignore[assignment]
                route_market_bucket = None  # type: ignore[assignment]
            if tag_headline is not None and route_market_bucket is not None:
                tag_result = tag_headline(headline)
                if tag_result.tags:
                    keywords = route_market_bucket(tag_result.tags)
                    if keywords:
                        max_routed = int(getattr(
                            tagger_cfg, "max_tag_routed_per_headline", 10))
                        added = 0
                        # Lowercase keyword scan over every market question.
                        # Cheap: ~10-50us per market on a ~1k-market index.
                        for i, market in enumerate(self._markets):
                            if len(results) >= top_k or added >= max_routed:
                                break
                            if market.condition_id in seen_ids:
                                continue
                            q_lower = market.question.lower()
                            matched_tag: Optional[str] = None
                            for tag in tag_result.tags:
                                bucket = [
                                    kw for kw in route_market_bucket([tag])
                                    if kw in q_lower
                                ]
                                if bucket:
                                    matched_tag = tag
                                    break
                            if matched_tag is None:
                                continue
                            seen_ids.add(market.condition_id)
                            results.append(MarketMatch(
                                condition_id=market.condition_id,
                                question=market.question,
                                token_yes=market.token_yes,
                                token_no=market.token_no,
                                similarity=float(scores[i]),
                                market_type=market.market_type,
                                tag=matched_tag,
                            ))
                            added += 1

        return results
