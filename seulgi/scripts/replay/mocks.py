"""
Deterministic stand-ins for the runner's external dependencies so a
replay run costs zero LLM tokens, makes zero CLOB API calls, and
produces reproducible failures.

Every mock matches the signature of the production class it replaces
so `EngineRunner` doesn't need to know it's being replayed.
"""
from __future__ import annotations

import asyncio
import sys
import os
from dataclasses import dataclass, field
from typing import Optional

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "app"))

from ai import ClassifyResult, Direction
from markets import MarketMatch
from news_memory import FreshnessResult


# ---------------------------------------------------------------------------
# ReplayTriple — one stored headline's full payload, ready to inject
# ---------------------------------------------------------------------------


@dataclass
class HistoricalTrade:
    """Subset of `trades` row fields needed to replay one (market, engine)
    decision deterministically."""
    engine_name: str
    condition_id: str
    question: str
    token_id: Optional[str]
    direction: str            # "YES" | "NO" | "NEUTRAL"
    confidence_raw: float
    p_alpha: float
    mid_yes: Optional[float]
    mid_no: Optional[float]
    similarity: float
    relevance_score: Optional[str]
    relevance_reason: Optional[str]


@dataclass
class ReplayTriple:
    """Everything needed to re-run one historical headline through the live
    runner against a fresh DB. Built by harness.load_headlines."""
    signal_id_orig: int       # source DB row, for debug only
    site_url: str
    headline: str
    article_url: str
    headline_id: str
    detected_at_iso: str
    freshness_label: Optional[str]
    nearest_age_secs: Optional[float]
    trades: list[HistoricalTrade] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Mock embedder — returns a deterministic vector keyed on text
# ---------------------------------------------------------------------------


class MockEmbedder:
    """Drop-in for `markets.Embedder`. The vector content doesn't matter for
    replay because MockMarketIndex.find_matches ignores it — but the shape
    must be 384-dim float32 normalised so downstream numpy paths work."""

    _DIM = 384

    def embed(self, text: str) -> np.ndarray:
        # Deterministic per-text vector via hash → seed → normal draw.
        seed = abs(hash(text)) % (2**32)
        rng = np.random.default_rng(seed)
        v = rng.standard_normal(self._DIM).astype(np.float32)
        v /= np.linalg.norm(v) or 1.0
        return v

    def embed_batch(self, texts: list[str]) -> np.ndarray:
        return np.stack([self.embed(t) for t in texts]) if texts else np.zeros(
            (0, self._DIM), dtype=np.float32
        )


# ---------------------------------------------------------------------------
# Mock market index — return the markets each headline historically matched
# ---------------------------------------------------------------------------


class MockMarketIndex:
    """Drop-in for `markets.MarketIndex`. Pre-loaded with a {headline_id ->
    [MarketMatch]} map built from historical `trades` rows."""

    def __init__(self, matches_by_headline: dict[str, list[MarketMatch]]) -> None:
        self._map = matches_by_headline
        # process() reads index._current_hid via a side channel; we use a
        # threadlocal-ish single slot since each replay coroutine sets it
        # before calling find_matches.
        self._next_hid: Optional[str] = None

    def stage(self, hid: str) -> None:
        """Tell the index which headline's matches to return on the next
        find_matches call. Replay-only; in production the index returns
        based purely on cosine similarity."""
        self._next_hid = hid

    def find_matches(self, query_embedding: np.ndarray) -> list[MarketMatch]:
        hid = self._next_hid
        self._next_hid = None
        if hid is None:
            return []
        return list(self._map.get(hid, []))


# ---------------------------------------------------------------------------
# Mock direction classifier — replay historical AI verdicts
# ---------------------------------------------------------------------------


class MockDirectionClassifier:
    """Drop-in for `ai.DirectionClassifier`. Pre-loaded with a
    {(headline_id, condition_id) -> HistoricalTrade} map so each market
    gets back its original AI verdict."""

    def __init__(self, verdicts: dict[tuple[str, str], HistoricalTrade]) -> None:
        self._verdicts = verdicts
        # Track tokens reported to caller so log_usage still fires once
        # per replay headline (matches production write count).
        self._tokens_per_call = 10  # nominal — non-zero so log_usage writes

    async def classify_batch(
        self,
        headline: str,
        matches: list,
        is_republish: bool = False,
        similar_to: Optional[str] = None,
        source: Optional[str] = None,
        freshness=None,
        relevance_verdicts: Optional[list] = None,
    ) -> tuple[list[ClassifyResult], dict]:
        results: list[ClassifyResult] = []
        # The runner doesn't pass headline_id, but the matches carry the
        # condition_ids we need to look up. We resolve by (hid, cid) using
        # a per-call hid hint set via stage_hid().
        hid = getattr(self, "_next_hid", None)
        for m in matches:
            t = self._verdicts.get((hid, m.condition_id)) if hid else None
            if t is None:
                # No historical decision — treat as NEUTRAL so the trader
                # records a skip row but doesn't fabricate a trade.
                results.append(ClassifyResult(
                    direction=Direction.NEUTRAL, confidence=0.0,
                    reason="replay:no_historical_verdict",
                ))
                continue
            try:
                d = Direction(t.direction)
            except ValueError:
                d = Direction.NEUTRAL
            results.append(ClassifyResult(
                direction=d,
                confidence=t.confidence_raw or 0.0,
                reason="replay",
                tokens_used=0,
                p_yes=0.0, p_no=0.0, p_neutral=0.0,
                agreement=1.0,
                p_alpha=t.p_alpha or 0.0,
                risk_score=0.0,
            ))
        self._next_hid = None
        return results, {
            "model": "replay-mock",
            "tokens": self._tokens_per_call,
            "n_in": len(matches),
        }

    def stage_hid(self, hid: str) -> None:
        """Set the headline_id whose verdicts to use on the next call."""
        self._next_hid = hid

    async def close(self) -> None:
        return None


# ---------------------------------------------------------------------------
# Mock news memory — return stored freshness label
# ---------------------------------------------------------------------------


class MockNewsMemory:
    """Drop-in for `news_memory.NewsMemory`. Returns the historical
    freshness label from the signals row instead of running ChromaDB."""

    def __init__(self) -> None:
        # Replay never short-circuits — we want every gate to fire and the
        # writer to see every write path. Override per test if needed.
        self._short_circuit_labels: list[str] = []
        self._next: Optional[FreshnessResult] = None
        self.size = 0

    def stage(self, label: Optional[str], age_secs: Optional[float]) -> None:
        if label is None:
            self._next = FreshnessResult(label="BREAKING")
            return
        self._next = FreshnessResult(
            label=label,  # type: ignore[arg-type]
            is_republish=(label == "REHASH"),
            nearest_dist=0.2 if label != "BREAKING" else 0.5,
            nearest_age_secs=age_secs,
            similar_to=None,
            recent_context=[],
        )

    def assess(self, headline, embedding, site_url, article_url) -> FreshnessResult:
        r = self._next or FreshnessResult(label="BREAKING")
        self._next = None
        return r


# ---------------------------------------------------------------------------
# No-op article fetcher
# ---------------------------------------------------------------------------


class MockArticleFetcher:
    async def fetch_and_store(self, headline_id, article_url, site_url) -> None:
        return None


# ---------------------------------------------------------------------------
# Mock fetch_mid_price — replaces engine._fetch_mid_price
# ---------------------------------------------------------------------------


def make_mock_fetch_mid_price(prices: dict[str, float]):
    """Returns an async function with the same signature as
    `engine._fetch_mid_price(session, token_id)` that resolves to the
    historical mid for that token_id, or None if unknown."""

    async def _fetch(session, token_id):
        return prices.get(token_id)

    return _fetch
