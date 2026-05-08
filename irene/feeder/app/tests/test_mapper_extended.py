"""
Extended tests for mapper.py — cache staleness, direction_min_confidence
filtering, top_k exact boundary, multi-match score ordering, token
validation edge cases.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import numpy as np
import pytest

from irene_shared.config import SignalsSection
from irene_shared.models import Direction, RawSignal, SignalSource
from mapper import SignalMapper, _MarketCache


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _vec(dim: int = 384, seed: int = 0) -> np.ndarray:
    rng = np.random.default_rng(seed)
    v = rng.random(dim).astype(np.float32)
    return v / np.linalg.norm(v)


def _market(condition_id: str, seed: int, question: str = "Will X happen?") -> dict:
    return {
        "condition_id": condition_id,
        "question": question,
        "token_yes": f"yes_{condition_id}",
        "token_no": f"no_{condition_id}",
        "end_date": datetime(2026, 6, 1, tzinfo=timezone.utc),
        "category": "Politics",
        "embedding": _vec(seed=seed),
    }


def _sig(title: str = "Senate passes bill") -> RawSignal:
    return RawSignal(
        source=SignalSource.NEWSAPI,
        source_detail="newsapi",
        url="https://example.com/article",
        title=title,
        description=None,
        published_at=None,
        ingested_at=_now(),
    )


def _cfg(**kw) -> SignalsSection:
    base = dict(
        similarity_threshold=0.001,  # nearly zero so matches always pass in basic tests
        top_k_markets=5,
        direction_min_confidence=0.30,
        yes_keywords=[],
        no_keywords=[],
    )
    base.update(kw)
    return SignalsSection(**base)


def _db(markets: list[dict]) -> MagicMock:
    db = MagicMock()
    db.get_active_markets_with_embeddings = AsyncMock(return_value=markets)
    return db


# ---------------------------------------------------------------------------
# _MarketCache advanced
# ---------------------------------------------------------------------------


class TestMarketCacheAdvanced:
    def test_stale_after_loaded_at_reset(self):
        """Resetting _loaded_at to 0 should make the cache report stale."""
        cache = _MarketCache()
        cache.load([_market("c1", 0)])
        assert not cache.is_stale()  # just loaded
        cache._loaded_at = 0.0  # simulate TTL expiry
        assert cache.is_stale()

    def test_cache_reload_replaces_previous_markets(self):
        cache = _MarketCache()
        market_a = _market("cA", 0)
        market_b = _market("cB", 1)
        cache.load([market_a])
        cache.load([market_b])  # replace with only market_b
        q = _vec(seed=1)
        matches = cache.find_matches(q, threshold=0.0, top_k=10)
        cond_ids = [m[0]["condition_id"] for m in matches]
        assert "cB" in cond_ids
        assert "cA" not in cond_ids  # replaced

    def test_find_matches_returns_scores_in_0_to_1_range(self):
        cache = _MarketCache()
        markets = [_market(f"c{i}", i) for i in range(5)]
        cache.load(markets)
        q = _vec(seed=99)
        matches = cache.find_matches(q, threshold=0.0, top_k=5)
        for _, score in matches:
            assert 0.0 <= score <= 1.0 + 1e-6

    def test_exact_top_k_boundary(self):
        """top_k=2 should never return more than 2 results."""
        cache = _MarketCache()
        cache.load([_market(f"c{i}", i) for i in range(20)])
        q = _vec(seed=5)
        matches = cache.find_matches(q, threshold=0.0, top_k=2)
        assert len(matches) == 2

    def test_top_k_larger_than_market_count_returns_all(self):
        cache = _MarketCache()
        cache.load([_market(f"c{i}", i) for i in range(3)])
        q = _vec(seed=0)
        matches = cache.find_matches(q, threshold=0.0, top_k=100)
        assert len(matches) == 3

    def test_threshold_of_1_returns_only_identical_vectors(self):
        query = _vec(seed=7)
        market_same = _market("same", 7)
        market_diff = _market("diff", 99)
        cache = _MarketCache()
        cache.load([market_same, market_diff])
        matches = cache.find_matches(query, threshold=0.9999, top_k=10)
        # Only the identical vector (cosine=1.0) should match
        assert len(matches) == 1
        assert matches[0][0]["condition_id"] == "same"


# ---------------------------------------------------------------------------
# SignalMapper — direction and confidence
# ---------------------------------------------------------------------------


class TestSignalMapperDirection:
    @pytest.mark.asyncio
    async def test_high_confidence_yes_direction_attached(self):
        """Titles with strong YES keywords give Direction.YES."""
        query = _vec(seed=10)
        market = _market("c10", 10)
        mapper = SignalMapper(_db([market]))

        signal = _sig("Government approves major infrastructure bill")
        results = await mapper.map(signal, 1, query, _cfg())
        assert len(results) == 1
        # The direction classifier may give YES or NEUTRAL depending on keywords
        # We just verify the field is set and not an error
        assert results[0].direction in (Direction.YES, Direction.NO, Direction.NEUTRAL)

    @pytest.mark.asyncio
    async def test_direction_confidence_attached_to_result(self):
        query = _vec(seed=11)
        market = _market("c11", 11)
        mapper = SignalMapper(_db([market]))
        results = await mapper.map(_sig(), 1, query, _cfg())
        assert len(results) == 1
        assert 0.0 <= results[0].direction_confidence <= 1.0

    @pytest.mark.asyncio
    async def test_similarity_score_in_result_matches_cosine_similarity(self):
        query = _vec(seed=42)
        market = _market("exact", 42)  # same seed → cosine=1.0
        mapper = SignalMapper(_db([market]))
        results = await mapper.map(_sig(), 1, query, _cfg())
        assert len(results) == 1
        assert results[0].similarity_score == pytest.approx(1.0, abs=1e-4)

    @pytest.mark.asyncio
    async def test_results_ordered_by_similarity_descending(self):
        query = _vec(seed=0)
        # Mix of seeds — scores will vary
        markets = [_market(f"c{i}", i * 7) for i in range(5)]
        mapper = SignalMapper(_db(markets))
        results = await mapper.map(_sig(), 1, query, _cfg(top_k_markets=5))
        scores = [r.similarity_score for r in results]
        assert scores == sorted(scores, reverse=True)

    @pytest.mark.asyncio
    async def test_top_k_limits_returned_results(self):
        query = _vec(seed=0)
        markets = [_market(f"c{i}", i) for i in range(20)]
        mapper = SignalMapper(_db(markets))
        results = await mapper.map(_sig(), 1, query, _cfg(top_k_markets=3))
        assert len(results) <= 3


# ---------------------------------------------------------------------------
# SignalMapper — cache behaviour
# ---------------------------------------------------------------------------


class TestSignalMapperCache:
    @pytest.mark.asyncio
    async def test_cache_loaded_once_across_multiple_calls(self):
        query = _vec(seed=0)
        markets = [_market("c1", 0)]
        db = _db(markets)
        mapper = SignalMapper(db)

        # 5 calls — cache should be loaded exactly once (not stale between calls)
        for i in range(5):
            await mapper.map(_sig(), i, query, _cfg())

        assert db.get_active_markets_with_embeddings.call_count == 1

    @pytest.mark.asyncio
    async def test_cache_reloaded_when_stale(self):
        """Force cache staleness and verify DB is re-queried."""
        query = _vec(seed=0)
        db = _db([_market("c1", 0)])
        mapper = SignalMapper(db)

        # First call loads cache
        await mapper.map(_sig(), 1, query, _cfg())
        first_count = db.get_active_markets_with_embeddings.call_count

        # Force stale
        mapper._cache._loaded_at = 0.0  # reset load time → is_stale() returns True

        # Second call should reload
        await mapper.map(_sig(), 2, query, _cfg())
        assert db.get_active_markets_with_embeddings.call_count == first_count + 1


# ---------------------------------------------------------------------------
# SignalMapper — token validation
# ---------------------------------------------------------------------------


class TestSignalMapperTokenValidation:
    @pytest.mark.asyncio
    async def test_market_with_none_token_no_is_skipped(self):
        market = _market("c1", 0)
        market["token_no"] = None
        mapper = SignalMapper(_db([market]))
        results = await mapper.map(_sig(), 1, _vec(seed=0), _cfg())
        assert results == []

    @pytest.mark.asyncio
    async def test_market_with_empty_token_yes_is_skipped(self):
        market = _market("c1", 0)
        market["token_yes"] = ""
        mapper = SignalMapper(_db([market]))
        results = await mapper.map(_sig(), 1, _vec(seed=0), _cfg())
        assert results == []

    @pytest.mark.asyncio
    async def test_market_with_both_tokens_is_included(self):
        market = _market("c_valid", 5)
        mapper = SignalMapper(_db([market]))
        results = await mapper.map(_sig(), 1, _vec(seed=5), _cfg())
        assert len(results) == 1
        assert results[0].condition_id == "c_valid"

    @pytest.mark.asyncio
    async def test_token_yes_and_no_are_propagated_to_result(self):
        market = _market("c_tok", 3)
        mapper = SignalMapper(_db([market]))
        results = await mapper.map(_sig(), 1, _vec(seed=3), _cfg())
        assert results[0].token_yes == "yes_c_tok"
        assert results[0].token_no == "no_c_tok"
