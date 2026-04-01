"""
Unit tests for embedder.py.

The sentence-transformer model is NOT loaded in unit tests (too slow, needs
the model file). We mock the SentenceTransformer class. Integration tests
that load the real model are marked @pytest.mark.integration.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from embedder import Embedder, _DEFAULT_MODEL


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_vec(dim: int = 384, seed: int = 0) -> np.ndarray:
    rng = np.random.default_rng(seed)
    v = rng.random(dim).astype(np.float32)
    return v / np.linalg.norm(v)  # normalised


def _mock_model(dim: int = 384) -> MagicMock:
    model = MagicMock()
    model.get_sentence_embedding_dimension.return_value = dim
    model.encode.side_effect = lambda texts, **kw: (
        np.stack([_make_vec(dim, i) for i in range(len(texts))])
        if isinstance(texts, list)
        else _make_vec(dim, 0)
    )
    return model


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestEmbedder:
    @patch("embedder.SentenceTransformer")
    def test_load_calls_sentence_transformer(self, MockST):
        MockST.return_value = _mock_model()
        e = Embedder()
        e.load()
        MockST.assert_called_once_with(_DEFAULT_MODEL)

    @patch("embedder.SentenceTransformer")
    def test_embed_returns_float32_normalised(self, MockST):
        MockST.return_value = _mock_model()
        e = Embedder()
        e.load()
        vec = e.embed("hello world")
        assert vec.dtype == np.float32
        assert vec.ndim == 1
        # Normalised → L2 norm ≈ 1.0
        assert np.linalg.norm(vec) == pytest.approx(1.0, abs=1e-5)

    @patch("embedder.SentenceTransformer")
    def test_embed_batch_returns_list(self, MockST):
        MockST.return_value = _mock_model()
        e = Embedder()
        e.load()
        texts = ["a", "b", "c"]
        vecs = e.embed_batch(texts)
        assert len(vecs) == 3
        assert all(v.dtype == np.float32 for v in vecs)

    @patch("embedder.SentenceTransformer")
    def test_embed_batch_empty_returns_empty(self, MockST):
        MockST.return_value = _mock_model()
        e = Embedder()
        e.load()
        assert e.embed_batch([]) == []

    @patch("embedder.SentenceTransformer")
    def test_dim_property(self, MockST):
        MockST.return_value = _mock_model(dim=384)
        e = Embedder()
        e.load()
        assert e.dim == 384

    def test_dim_before_load_raises(self):
        e = Embedder()
        with pytest.raises(RuntimeError, match="load()"):
            _ = e.dim

    @patch("embedder.SentenceTransformer")
    def test_lazy_load_on_embed(self, MockST):
        MockST.return_value = _mock_model()
        e = Embedder()
        # Don't call e.load() — should auto-load on first embed()
        e.embed("test")
        MockST.assert_called_once()

    # ------------------------------------------------------------------
    # Static similarity methods
    # ------------------------------------------------------------------

    def test_cosine_similarity_identical(self):
        v = _make_vec()
        assert Embedder.cosine_similarity(v, v) == pytest.approx(1.0, abs=1e-5)

    def test_cosine_similarity_orthogonal(self):
        v1 = np.array([1.0, 0.0, 0.0], dtype=np.float32)
        v2 = np.array([0.0, 1.0, 0.0], dtype=np.float32)
        assert Embedder.cosine_similarity(v1, v2) == pytest.approx(0.0, abs=1e-7)

    def test_cosine_similarity_opposite(self):
        v = _make_vec()
        assert Embedder.cosine_similarity(v, -v) == pytest.approx(-1.0, abs=1e-5)

    def test_batch_cosine_similarity_shape(self):
        query = _make_vec(dim=4)
        matrix = np.stack([_make_vec(dim=4, seed=i) for i in range(5)])
        sims = Embedder.batch_cosine_similarity(query, matrix)
        assert sims.shape == (5,)

    def test_batch_cosine_similarity_correct_values(self):
        # matrix row 2 is identical to query → similarity == 1.0
        query = _make_vec(dim=4, seed=0)
        candidates = [_make_vec(dim=4, seed=i) for i in range(5)]
        candidates[2] = query.copy()
        matrix = np.stack(candidates)
        sims = Embedder.batch_cosine_similarity(query, matrix)
        assert sims[2] == pytest.approx(1.0, abs=1e-5)

    def test_batch_cosine_top_k(self):
        """Verify we can get top-K by argsort."""
        query = _make_vec(dim=4, seed=99)
        matrix = np.stack([_make_vec(dim=4, seed=i) for i in range(10)])
        sims = Embedder.batch_cosine_similarity(query, matrix)
        top3 = np.argsort(sims)[::-1][:3]
        assert len(top3) == 3
        assert sims[top3[0]] >= sims[top3[1]] >= sims[top3[2]]


# ---------------------------------------------------------------------------
# Integration test (loads real model, skipped by default)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestEmbedderIntegration:
    def test_real_model_embed(self):
        e = Embedder()
        e.load()
        vec = e.embed("Will it rain in London tomorrow?")
        assert vec.shape == (384,)
        assert np.linalg.norm(vec) == pytest.approx(1.0, abs=1e-5)

    def test_similar_texts_have_high_similarity(self):
        e = Embedder()
        e.load()
        v1 = e.embed("Will the Federal Reserve raise interest rates?")
        v2 = e.embed("Fed rate hike decision announcement")
        v_unrelated = e.embed("Will it snow in Tokyo next week?")
        sim_related = Embedder.cosine_similarity(v1, v2)
        sim_unrelated = Embedder.cosine_similarity(v1, v_unrelated)
        assert sim_related > sim_unrelated
