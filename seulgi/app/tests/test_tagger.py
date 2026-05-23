"""Tests for seulgi/app/tagger.py and the tag-augment step in markets.py.

Covers (Phase 4D.2):
  - `tag_headline` deterministic keyword match for each of the 8 categories.
  - `tag_headline` empty / boring-headline → no tags.
  - `tag_headline` multi-tag headline returns both tags.
  - `route_market_bucket` mapping for a known tag.
  - `route_market_bucket` unknown tag → empty list (no KeyError).
  - `MarketIndex.find_matches` augments tag-routed markets in.
  - `MarketIndex.find_matches` on an untagged headline does NOT
    add tag-routed matches (cosine-only behaviour unchanged).
  - `MarketIndex.find_matches` respects `max_tag_routed_per_headline`.
"""

from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from tagger import (  # noqa: E402
    HeadlineTags,
    route_market_bucket,
    supported_tags,
    tag_headline,
)
from markets import Market, MarketIndex  # noqa: E402


# ---------------------------------------------------------------------------
# tag_headline
# ---------------------------------------------------------------------------


class TestTagHeadline:
    def test_interest_rates_fed_cut(self):
        result = tag_headline("Fed cuts rates by 25bps")
        assert result.tags == ["interest_rates"]
        assert result.confidence == {"interest_rates": 1.0}
        assert result.method == "keyword"

    def test_interest_rates_powell(self):
        result = tag_headline("Powell signals dovish pivot at FOMC press conference")
        assert "interest_rates" in result.tags

    def test_geopolitics_invasion(self):
        result = tag_headline("Russia invasion enters second year as NATO ramps up sanctions")
        assert "geopolitics" in result.tags
        assert result.confidence["geopolitics"] == 1.0

    def test_elections(self):
        result = tag_headline("Trump leads in primary poll ahead of Iowa caucuses")
        assert "elections" in result.tags

    def test_sports(self):
        result = tag_headline("NFL playoff schedule released; Super Bowl in February")
        assert "sports" in result.tags

    def test_weather(self):
        result = tag_headline("Hurricane Idalia makes landfall in Florida")
        assert "weather" in result.tags

    def test_tech(self):
        result = tag_headline("Apple unveils new AI chip; Nvidia stock reacts")
        assert "tech" in result.tags

    def test_crypto(self):
        result = tag_headline("Bitcoin tops $70k as Coinbase reports record volume")
        assert "crypto" in result.tags

    def test_earnings(self):
        result = tag_headline("Tesla quarterly report beats EPS estimates; revenue beat")
        assert "earnings" in result.tags

    def test_empty_headline_returns_no_tags(self):
        assert tag_headline("").tags == []
        assert tag_headline("   ").tags == []
        assert tag_headline("Boring local news about the weekend fair").tags == []

    def test_multiple_tags(self):
        result = tag_headline("Russia invades Ukraine; Fed reacts with emergency rate cut")
        assert "geopolitics" in result.tags
        assert "interest_rates" in result.tags
        # Stable ordering: interest_rates is declared before geopolitics in
        # _TAG_KEYWORDS, so it should appear first.
        assert result.tags.index("interest_rates") < result.tags.index("geopolitics")
        assert result.confidence["interest_rates"] == 1.0
        assert result.confidence["geopolitics"] == 1.0

    def test_case_insensitive(self):
        # Mixed case + lower case both fire the regex.
        assert "interest_rates" in tag_headline("fed cuts rates").tags
        assert "interest_rates" in tag_headline("FED CUTS RATES").tags
        assert "geopolitics" in tag_headline("RUSSIA SANCTIONS HEAVY").tags

    def test_word_boundaries(self):
        # "ratel" should NOT trigger "rate cut" — regex is \brate cut\b.
        # And "preside" shouldn't match "president"-style tokens not in the map.
        result = tag_headline("Honeyrate cutbacks at the local bakery")
        assert "interest_rates" not in result.tags

    def test_supported_tags_complete(self):
        tags = supported_tags()
        assert set(tags) == {
            "interest_rates", "geopolitics", "elections",
            "sports", "weather", "tech", "crypto", "earnings",
        }


# ---------------------------------------------------------------------------
# route_market_bucket
# ---------------------------------------------------------------------------


class TestRouteMarketBucket:
    def test_interest_rates_bucket(self):
        kws = route_market_bucket(["interest_rates"])
        # The bucket must contain the canonical keywords from the spec.
        for expected in ("fed", "rate", "fomc", "inflation", "bond"):
            assert expected in kws

    def test_unknown_tag_returns_empty(self):
        assert route_market_bucket(["nonexistent_tag"]) == []

    def test_empty_tag_list(self):
        assert route_market_bucket([]) == []

    def test_multi_tag_dedup(self):
        # Both tags include "bond"-ish overlaps? No — interest_rates has
        # "fed"/"rate" and elections has "trump"/"vote". No overlap by
        # design; assert the lists are concatenated without duplicates.
        kws = route_market_bucket(["interest_rates", "elections"])
        assert len(kws) == len(set(kws)), "keyword list contains duplicates"

    def test_known_tags_all_yield_keywords(self):
        for tag in supported_tags():
            assert route_market_bucket([tag]), f"tag {tag!r} returned empty bucket"


# ---------------------------------------------------------------------------
# MarketIndex.find_matches tag-augment
# ---------------------------------------------------------------------------


class TestFindMatchesTagAugment:
    def _make_index(
        self,
        *,
        tagger_enabled: bool = True,
        sim_threshold: float = 0.5,
        top_k: int = 10,
        max_tag_routed: int = 10,
    ) -> MarketIndex:
        cfg = MagicMock()
        cfg.markets.similarity_threshold = sim_threshold
        cfg.markets.top_k = top_k
        cfg.markets.refresh_hours = 4.0
        cfg.tagger.enabled = tagger_enabled
        cfg.tagger.min_tag_confidence = 1.0
        cfg.tagger.max_tag_routed_per_headline = max_tag_routed
        idx = MarketIndex(db=MagicMock(), embedder=MagicMock(), cfg=cfg)
        return idx

    def _populate(self, idx: MarketIndex, markets_and_vecs):
        """`markets_and_vecs` is a list[(Market, np.ndarray)]."""
        idx._markets = [m for m, _ in markets_and_vecs]
        idx._matrix = np.stack([v for _, v in markets_and_vecs])

    def _unit_vec(self, seed: int) -> np.ndarray:
        rng = np.random.default_rng(seed)
        v = rng.standard_normal(384).astype(np.float32)
        v /= np.linalg.norm(v)
        return v

    def test_tagged_headline_adds_tag_routed_matches(self):
        idx = self._make_index(sim_threshold=0.5, top_k=10)
        # Two markets:
        #  - cid1: cosine-similar to the query embedding (will match cosine).
        #  - cid_fed: question mentions "Fed" but cosine-far — only the
        #    tag-augment can pull it in.
        query_vec = self._unit_vec(seed=1)
        far_vec = self._unit_vec(seed=99)
        self._populate(idx, [
            (Market("cid1", "Will inflation hit target?", "ty1", "tn1"), query_vec),
            (Market("cid_fed", "Will the Fed cut rates in March?", "tyf", "tnf"), far_vec),
        ])
        # Tagged headline.
        results = idx.find_matches(query_vec.copy(),
                                   headline="Powell signals FOMC rate cut")
        cids = {r.condition_id for r in results}
        assert "cid1" in cids       # cosine match
        assert "cid_fed" in cids    # tag-routed
        tag_routed = [r for r in results if r.tag is not None]
        assert len(tag_routed) == 1
        assert tag_routed[0].tag == "interest_rates"
        assert tag_routed[0].condition_id == "cid_fed"

    def test_untagged_headline_no_tag_routing(self):
        idx = self._make_index(sim_threshold=0.5, top_k=10)
        query_vec = self._unit_vec(seed=1)
        far_vec = self._unit_vec(seed=99)
        self._populate(idx, [
            (Market("cid1", "Will inflation hit target?", "ty1", "tn1"), query_vec),
            (Market("cid_fed", "Will the Fed cut rates?", "tyf", "tnf"), far_vec),
        ])
        # Headline has zero macro keywords — no tag, no augment.
        results = idx.find_matches(query_vec.copy(),
                                   headline="Boring local news about a county fair")
        cids = {r.condition_id for r in results}
        assert "cid1" in cids
        assert "cid_fed" not in cids
        assert all(r.tag is None for r in results)

    def test_no_headline_param_legacy_behavior(self):
        # Passing no headline argument keeps cosine-only behaviour.
        idx = self._make_index(sim_threshold=0.5, top_k=10)
        query_vec = self._unit_vec(seed=1)
        far_vec = self._unit_vec(seed=99)
        self._populate(idx, [
            (Market("cid1", "Will inflation hit target?", "ty1", "tn1"), query_vec),
            (Market("cid_fed", "Will the Fed cut rates?", "tyf", "tnf"), far_vec),
        ])
        results = idx.find_matches(query_vec.copy())  # no headline kwarg
        cids = {r.condition_id for r in results}
        assert "cid1" in cids
        assert "cid_fed" not in cids

    def test_tagger_disabled_no_augment(self):
        idx = self._make_index(tagger_enabled=False, sim_threshold=0.5, top_k=10)
        query_vec = self._unit_vec(seed=1)
        far_vec = self._unit_vec(seed=99)
        self._populate(idx, [
            (Market("cid1", "Will inflation hit target?", "ty1", "tn1"), query_vec),
            (Market("cid_fed", "Will the Fed cut rates?", "tyf", "tnf"), far_vec),
        ])
        results = idx.find_matches(query_vec.copy(),
                                   headline="Fed cuts rates at FOMC meeting")
        cids = {r.condition_id for r in results}
        assert "cid_fed" not in cids  # tagger disabled → no augment

    def test_max_tag_routed_per_headline_cap(self):
        # Set max_tag_routed=2 and confirm only 2 tag-routed markets land
        # even when many would match.
        idx = self._make_index(sim_threshold=0.99, top_k=10, max_tag_routed=2)
        # All vectors orthogonal-ish to query; cosine ≈ 0 so they all fall
        # below the 0.99 threshold and only tag-routing can surface them.
        query_vec = self._unit_vec(seed=1)
        many = []
        for i in range(5):
            many.append((
                Market(f"cid_fed_{i}", f"Will the Fed move rates in scenario {i}?",
                       f"ty{i}", f"tn{i}"),
                self._unit_vec(seed=100 + i),
            ))
        self._populate(idx, many)
        results = idx.find_matches(query_vec.copy(), headline="Fed rate hike incoming")
        tag_routed = [r for r in results if r.tag is not None]
        assert len(tag_routed) == 2  # capped


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-v"]))
