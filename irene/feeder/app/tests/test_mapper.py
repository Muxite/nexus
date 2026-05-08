"""Unit tests for mapper.py — embedding cache, similarity, mapping logic."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest

from irene_shared.config import SignalsSection
from irene_shared.models import Direction, MappedSignal, RawSignal, SignalSource
from mapper import SignalMapper, _MarketCache


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_vec(dim: int = 384, seed: int = 0) -> np.ndarray:
    rng = np.random.default_rng(seed)
    v = rng.random(dim).astype(np.float32)
    return v / np.linalg.norm(v)


def _make_market(condition_id: str, seed: int, question: str = "Will X happen?") -> dict:
    return {
        "condition_id": condition_id,
        "question": question,
        "token_yes": f"tok_yes_{condition_id}",
        "token_no": f"tok_no_{condition_id}",
        "end_date": datetime(2026, 1, 2, tzinfo=timezone.utc),
        "category": "Politics",
        "embedding": _make_vec(seed=seed),
    }


def _make_signal(title: str = "Senate passes the bill") -> RawSignal:
    return RawSignal(
        source=SignalSource.NEWSAPI,
        source_detail="newsapi",
        url="https://example.com/1",
        title=title,
        description=None,
        published_at=None,
        ingested_at=datetime.now(timezone.utc),
    )


def _cfg(**overrides) -> SignalsSection:
    defaults = dict(
        similarity_threshold=0.70,
        top_k_markets=3,
        direction_min_confidence=0.30,
        yes_keywords=[],
        no_keywords=[],
    )
    defaults.update(overrides)
    return SignalsSection(**defaults)


# ---------------------------------------------------------------------------
# _MarketCache
# ---------------------------------------------------------------------------


class TestMarketCache:
    def test_is_stale_initially(self):
        cache = _MarketCache()
        assert cache.is_stale()

    def test_not_stale_after_load(self):
        cache = _MarketCache()
        cache.load([_make_market("c1", 0)])
        assert not cache.is_stale()

    def test_load_empty_gives_no_results(self):
        cache = _MarketCache()
        cache.load([])
        query = _make_vec()
        assert cache.find_matches(query, threshold=0.0, top_k=5) == []

    def test_load_markets_without_embeddings_skipped(self):
        market = _make_market("c1", 0)
        market["embedding"] = None
        cache = _MarketCache()
        cache.load([market])
        assert cache.find_matches(_make_vec(), threshold=0.0, top_k=5) == []

    def test_find_exact_match(self):
        query = _make_vec(seed=42)
        market = _make_market("exact", 42)
        cache = _MarketCache()
        cache.load([market])
        matches = cache.find_matches(query, threshold=0.0, top_k=1)
        assert len(matches) == 1
        cond_id, score = matches[0][0]["condition_id"], matches[0][1]
        assert cond_id == "exact"
        assert score == pytest.approx(1.0, abs=1e-5)

    def test_threshold_filters_low_similarity(self):
        query = _make_vec(seed=0)
        # Use a very different vector for the market
        market = _make_market("low_sim", 99)
        cache = _MarketCache()
        cache.load([market])
        matches = cache.find_matches(query, threshold=0.99, top_k=5)
        assert matches == []

    def test_top_k_limits_results(self):
        query = _make_vec(seed=0)
        markets = [_make_market(f"c{i}", i) for i in range(10)]
        cache = _MarketCache()
        cache.load(markets)
        matches = cache.find_matches(query, threshold=0.0, top_k=3)
        assert len(matches) <= 3

    def test_results_sorted_descending(self):
        query = _make_vec(seed=0)
        markets = [_make_market(f"c{i}", i) for i in range(5)]
        cache = _MarketCache()
        cache.load(markets)
        matches = cache.find_matches(query, threshold=0.0, top_k=5)
        scores = [m[1] for m in matches]
        assert scores == sorted(scores, reverse=True)


# ---------------------------------------------------------------------------
# SignalMapper
# ---------------------------------------------------------------------------


class TestSignalMapper:
    def _make_db(self, markets: list[dict]) -> MagicMock:
        db = MagicMock()
        db.get_active_markets_with_embeddings = AsyncMock(return_value=markets)
        return db

    @pytest.mark.asyncio
    async def test_maps_signal_to_matching_market(self):
        query = _make_vec(seed=42)
        market = _make_market("cond_42", 42, question="Will the Fed raise rates?")

        db = self._make_db([market])
        mapper = SignalMapper(db)

        signal = _make_signal("Fed raises interest rates by 25 basis points")
        results = await mapper.map(
            signal=signal,
            signal_id=1,
            query_embedding=query,
            cfg=_cfg(similarity_threshold=0.001, top_k_markets=3),
        )

        assert len(results) == 1
        assert results[0].condition_id == "cond_42"

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_markets_in_db(self):
        db = self._make_db([])
        mapper = SignalMapper(db)
        signal = _make_signal()
        results = await mapper.map(
            signal=signal,
            signal_id=1,
            query_embedding=_make_vec(),
            cfg=_cfg(similarity_threshold=0.001),
        )
        assert results == []

    @pytest.mark.asyncio
    async def test_skips_market_without_tokens(self):
        market = _make_market("c1", 42)
        market["token_yes"] = None
        db = self._make_db([market])
        mapper = SignalMapper(db)
        results = await mapper.map(
            signal=_make_signal(),
            signal_id=1,
            query_embedding=_make_vec(seed=42),
            cfg=_cfg(similarity_threshold=0.001),
        )
        assert results == []

    @pytest.mark.asyncio
    async def test_direction_attached_from_classifier(self):
        query = _make_vec(seed=7)
        market = _make_market("c7", 7)
        db = self._make_db([market])
        mapper = SignalMapper(db)

        # YES signal
        signal = _make_signal("Senate passes the infrastructure bill")
        results = await mapper.map(
            signal=signal,
            signal_id=1,
            query_embedding=query,
            cfg=_cfg(similarity_threshold=0.001),
        )
        assert len(results) == 1
        assert results[0].direction == Direction.YES

    @pytest.mark.asyncio
    async def test_neutral_direction_still_included_in_results(self):
        """Mapper returns NEUTRAL — it's the feeder main that skips NEUTRAL dispatches."""
        query = _make_vec(seed=5)
        market = _make_market("c5", 5)
        db = self._make_db([market])
        mapper = SignalMapper(db)

        signal = _make_signal("Markets open on Monday morning")  # no direction keywords
        results = await mapper.map(
            signal=signal,
            signal_id=1,
            query_embedding=query,
            cfg=_cfg(similarity_threshold=0.001),
        )
        assert len(results) == 1
        assert results[0].direction == Direction.NEUTRAL

    @pytest.mark.asyncio
    async def test_top_k_limits_number_of_results(self):
        query = _make_vec(seed=0)
        markets = [_make_market(f"c{i}", i) for i in range(10)]
        db = self._make_db(markets)
        mapper = SignalMapper(db)
        results = await mapper.map(
            signal=_make_signal("Senate passes the bill"),
            signal_id=1,
            query_embedding=query,
            cfg=_cfg(similarity_threshold=0.001, top_k_markets=2),
        )
        assert len(results) <= 2

    @pytest.mark.asyncio
    async def test_cache_not_refreshed_when_fresh(self):
        query = _make_vec(seed=0)
        market = _make_market("c1", 0)
        db = self._make_db([market])
        mapper = SignalMapper(db)

        # First call loads cache
        await mapper.map(_make_signal(), 1, query, _cfg(similarity_threshold=0.001))
        initial_calls = db.get_active_markets_with_embeddings.call_count

        # Second call should not re-query DB (cache fresh)
        await mapper.map(_make_signal(), 2, query, _cfg(similarity_threshold=0.001))
        assert db.get_active_markets_with_embeddings.call_count == initial_calls
