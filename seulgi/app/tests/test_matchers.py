"""Tests for seulgi/app/matchers.py and the A4E.2 matcher rerank step
inside `markets.py:find_matches`.

Covers (Phase 4E.2):
  - `Matcher` Protocol contract — both classes are runtime-checkable.
  - `CrossEncoderReranker.rerank_sync` with mocked model: in-band
    candidates rerank, out-of-band pass through with extra_score=0.
  - `CrossEncoderReranker` falls back to zero-score when the model
    can't load (scaffolded path — default state).
  - `LearnedMLPMatcher.fit` on a synthetic separable dataset converges,
    `score` returns plausible values.
  - `LearnedMLPMatcher` returns neutral 0.5 when unfit.
  - `find_matches(matchers=[...])` writes `extra_score` onto each
    MarketMatch and re-sorts by `similarity + extra_score`.
  - `find_matches()` with default kwargs is bit-identical to the
    pre-A4E.2 cosine-only behaviour (no matchers passed).
"""

from __future__ import annotations

import asyncio
import os
import sys
from unittest.mock import MagicMock

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from matchers import (  # noqa: E402
    CrossEncoderReranker,
    LearnedMLPMatcher,
    Matcher,
    MatchScore,
)
from markets import Market, MarketIndex, MarketMatch  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _unit_vec(seed: int, dim: int = 384) -> np.ndarray:
    rng = np.random.default_rng(seed)
    v = rng.standard_normal(dim).astype(np.float32)
    v /= np.linalg.norm(v)
    return v


def _make_index(sim_threshold: float = 0.0, top_k: int = 10) -> MarketIndex:
    cfg = MagicMock()
    cfg.markets.similarity_threshold = sim_threshold
    cfg.markets.top_k = top_k
    cfg.markets.refresh_hours = 4.0
    cfg.tagger.enabled = False  # isolate matcher behaviour from A4D.2
    cfg.tagger.min_tag_confidence = 1.0
    cfg.tagger.max_tag_routed_per_headline = 10
    return MarketIndex(db=MagicMock(), embedder=MagicMock(), cfg=cfg)


# ---------------------------------------------------------------------------
# Protocol contract
# ---------------------------------------------------------------------------


class TestMatcherProtocol:
    def test_cross_encoder_satisfies_protocol(self):
        ce = CrossEncoderReranker()
        # runtime_checkable Protocol — isinstance must accept any class with
        # the right method signature.
        assert isinstance(ce, Matcher)

    def test_learned_mlp_satisfies_protocol(self):
        mlp = LearnedMLPMatcher()
        assert isinstance(mlp, Matcher)


# ---------------------------------------------------------------------------
# CrossEncoderReranker
# ---------------------------------------------------------------------------


class TestCrossEncoderReranker:
    def test_no_model_returns_zero_scores(self):
        """Default scaffolded path: with no model loaded, every candidate
        scores 0.0 — preserving cosine behaviour."""
        ce = CrossEncoderReranker()
        ce._load_failed = True  # force scaffolded mode (no model installed)
        candidates = [
            MarketMatch("cid1", "Will A happen?", "ty", "tn", 0.4),
            MarketMatch("cid2", "Will B happen?", "ty", "tn", 0.8),
        ]
        scores = ce.rerank_sync("breaking news", [0.0] * 384, candidates)
        assert len(scores) == 2
        assert all(s.extra_score == 0.0 for s in scores)
        assert all(s.method == "cross_encoder" for s in scores)
        assert scores[0].cond_id == "cid1"
        assert scores[0].cosine_sim == pytest.approx(0.4)

    def test_in_band_candidates_get_reranked(self):
        """With a mocked model loaded, candidates whose cosine falls inside
        [band_low, band_high] are scored; out-of-band ones pass through 0.0."""
        ce = CrossEncoderReranker(band_low=0.3, band_high=0.55)
        # Inject a stub model that returns a constant high logit for every
        # pair → squashed sigmoid ≈ 0.88.
        stub = MagicMock()
        stub.predict = MagicMock(return_value=np.array([2.0, 2.0]))
        ce._model = stub
        candidates = [
            MarketMatch("cid_low", "Q1", "ty", "tn", 0.10),   # below band
            MarketMatch("cid_mid", "Q2", "ty", "tn", 0.40),   # in band
            MarketMatch("cid_mid2", "Q3", "ty", "tn", 0.50),  # in band
            MarketMatch("cid_high", "Q4", "ty", "tn", 0.90),  # above band
        ]
        scores = ce.rerank_sync("headline", [0.0] * 384, candidates)
        assert scores[0].extra_score == 0.0           # out of band
        assert scores[1].extra_score > 0.5            # in band, predicted
        assert scores[2].extra_score > 0.5            # in band, predicted
        assert scores[3].extra_score == 0.0           # out of band
        # Predict was called once with 2 in-band pairs.
        assert stub.predict.call_count == 1
        assert len(stub.predict.call_args[0][0]) == 2

    def test_model_exception_does_not_tank_cosine(self):
        """If the cross-encoder predict() raises, every candidate gets
        extra_score=0 — the cosine ranking is preserved upstream."""
        ce = CrossEncoderReranker(band_low=0.3, band_high=0.55)
        stub = MagicMock()
        stub.predict = MagicMock(side_effect=RuntimeError("oom"))
        ce._model = stub
        candidates = [MarketMatch("cid_mid", "Q", "ty", "tn", 0.4)]
        scores = ce.rerank_sync("headline", [0.0] * 384, candidates)
        assert scores[0].extra_score == 0.0


# ---------------------------------------------------------------------------
# LearnedMLPMatcher
# ---------------------------------------------------------------------------


class TestLearnedMLPMatcher:
    def test_unfit_returns_neutral(self):
        mlp = LearnedMLPMatcher()
        score = mlp.score(np.zeros(384, dtype=np.float32),
                          np.zeros(384, dtype=np.float32))
        assert score == pytest.approx(0.5)

    def test_fit_on_synthetic_separable_data(self):
        """A clearly-separable synthetic dataset: positive class = headline
        + market vectors that align; negative class = anti-aligned. After
        fitting, the matcher should score positive examples > 0.5 and
        negative examples < 0.5 (within a generous margin)."""
        mlp = LearnedMLPMatcher(min_train_samples=10)
        rng = np.random.default_rng(seed=7)
        n = 60
        X_pos = np.concatenate([
            np.ones((n, 384), dtype=np.float32) + 0.05 * rng.standard_normal((n, 384)),
            np.ones((n, 384), dtype=np.float32) + 0.05 * rng.standard_normal((n, 384)),
        ], axis=1).astype(np.float32)
        X_neg = np.concatenate([
            np.ones((n, 384), dtype=np.float32) + 0.05 * rng.standard_normal((n, 384)),
            -np.ones((n, 384), dtype=np.float32) + 0.05 * rng.standard_normal((n, 384)),
        ], axis=1).astype(np.float32)
        X = np.concatenate([X_pos, X_neg], axis=0)
        y = np.concatenate([np.ones(n), np.zeros(n)]).astype(np.float32)

        fitted = asyncio.new_event_loop().run_until_complete(mlp.fit(X, y))
        assert fitted is True

        # Score a held-out positive and a held-out negative.
        h_pos = np.ones(384, dtype=np.float32)
        m_pos = np.ones(384, dtype=np.float32)
        m_neg = -np.ones(384, dtype=np.float32)
        s_pos = mlp.score(h_pos, m_pos)
        s_neg = mlp.score(h_pos, m_neg)
        # The MLP should learn that aligned > anti-aligned.
        assert s_pos > s_neg
        assert 0.0 <= s_pos <= 1.0
        assert 0.0 <= s_neg <= 1.0

    def test_fit_refuses_below_min_samples(self):
        mlp = LearnedMLPMatcher(min_train_samples=30)
        X = np.zeros((5, 768), dtype=np.float32)
        y = np.zeros(5, dtype=np.float32)
        fitted = asyncio.new_event_loop().run_until_complete(mlp.fit(X, y))
        assert fitted is False
        # And score() still returns neutral.
        assert mlp.score(np.zeros(384, dtype=np.float32),
                         np.zeros(384, dtype=np.float32)) == pytest.approx(0.5)

    def test_rerank_sync_uses_market_embedding(self):
        """When market_embedding is attached to candidates, the MLP scores
        each pair; when absent, neutral 0.5 is emitted."""
        mlp = LearnedMLPMatcher(min_train_samples=10)
        # Force fitted=True with default weights so score() runs.
        mlp._fitted = True
        c1 = MarketMatch("cid1", "Q1", "ty", "tn", 0.4)
        c2 = MarketMatch("cid2", "Q2", "ty", "tn", 0.6)
        # Attach embeddings to one only.
        setattr(c1, "market_embedding", np.ones(384, dtype=np.float32))
        scores = mlp.rerank_sync("headline", [0.0] * 384, [c1, c2])
        assert len(scores) == 2
        assert all(s.method == "learned_mlp" for s in scores)
        # c2 (no embedding) → neutral 0.5.
        assert scores[1].extra_score == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# find_matches integration
# ---------------------------------------------------------------------------


class TestFindMatchesMatchers:
    def _populate(self, idx: MarketIndex, markets_and_vecs):
        idx._markets = [m for m, _ in markets_and_vecs]
        idx._matrix = np.stack([v for _, v in markets_and_vecs])

    def test_default_no_matchers_unchanged(self):
        """Sanity: with no `matchers` kwarg, find_matches returns the same
        result list as before A4E.2 — every candidate has extra_score=0."""
        idx = _make_index(sim_threshold=0.0, top_k=5)
        q = _unit_vec(seed=1)
        self._populate(idx, [
            (Market("c1", "Q1", "ty", "tn"), q),
            (Market("c2", "Q2", "ty", "tn"), _unit_vec(seed=42)),
        ])
        results = idx.find_matches(q.copy())
        assert all(r.extra_score == 0.0 for r in results)
        # Top match should be c1 (perfectly aligned with query).
        assert results[0].condition_id == "c1"

    def test_matcher_rerank_resorts_by_extra_score(self):
        """When a stub matcher emits a high extra_score for a lower-cosine
        candidate, that candidate climbs in the result order."""
        idx = _make_index(sim_threshold=-1.0, top_k=5)
        q = _unit_vec(seed=1)
        v_close = q.copy()                       # cosine = 1.0
        v_mid = 0.5 * q + 0.5 * _unit_vec(seed=99)  # cosine ≈ 0.5
        v_mid /= np.linalg.norm(v_mid)
        self._populate(idx, [
            (Market("c_close", "Close", "ty", "tn"), v_close),
            (Market("c_far", "Far", "ty", "tn"), v_mid),
        ])

        # Matcher that gives c_far a massive extra_score; c_close = 0.
        class _StubMatcher:
            def rerank_sync(self, headline, headline_emb, candidates):
                out = []
                for c in candidates:
                    extra = 1.0 if c.condition_id == "c_far" else 0.0
                    out.append(MatchScore(
                        cond_id=c.condition_id, cosine_sim=c.similarity,
                        extra_score=extra, method="stub"))
                return out

        results = idx.find_matches(
            q.copy(), matchers=[_StubMatcher()], matcher_weights=[10.0])
        # c_far should have a large extra_score and be re-sorted to the top.
        assert results[0].condition_id == "c_far"
        assert results[0].extra_score > 0
        # Raw cosine `similarity` is preserved.
        assert results[1].condition_id == "c_close"
        assert results[1].extra_score == 0.0
        # c_close's similarity is still its cosine, not 0.
        assert results[1].similarity > 0.9

    def test_matcher_exception_isolated(self):
        """A broken matcher must not tank the cosine result — find_matches
        skips that matcher and returns the cosine list unchanged."""
        idx = _make_index(sim_threshold=0.0, top_k=5)
        q = _unit_vec(seed=1)
        self._populate(idx, [(Market("c1", "Q", "ty", "tn"), q)])

        class _BrokenMatcher:
            def rerank_sync(self, headline, headline_emb, candidates):
                raise RuntimeError("boom")

        results = idx.find_matches(q.copy(), matchers=[_BrokenMatcher()])
        assert len(results) == 1
        assert results[0].extra_score == 0.0  # unmodified
        assert results[0].condition_id == "c1"
