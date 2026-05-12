"""
Tests for seulgi/app/markets.py

Covers:
- _parse_market: field extraction, missing fields, clobTokenIds
- Embedder: embed/embed_batch shapes
- MarketIndex.find_matches: top-k, threshold filtering, empty index
"""

import sys
import os
from unittest.mock import MagicMock, AsyncMock, patch

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from markets import _parse_market, Market, MarketMatch, Embedder, MarketIndex


# ---------------------------------------------------------------------------
# _parse_market
# ---------------------------------------------------------------------------


class TestParseMarket:
    def test_basic_parse(self):
        raw = {
            "conditionId": "abc123",
            "question": "Will X happen?",
            "clobTokenIds": ["token_yes_1", "token_no_1"],
        }
        parsed = _parse_market(raw)
        assert parsed is not None
        m, _end = parsed
        assert m.condition_id == "abc123"
        assert m.question == "Will X happen?"
        assert m.token_yes == "token_yes_1"
        assert m.token_no == "token_no_1"

    def test_missing_condition_id_returns_none(self):
        raw = {"question": "Will X happen?", "clobTokenIds": ["a", "b"]}
        assert _parse_market(raw) is None

    def test_missing_question_returns_none(self):
        raw = {"conditionId": "abc", "clobTokenIds": ["a", "b"]}
        assert _parse_market(raw) is None

    def test_no_clob_tokens(self):
        raw = {"conditionId": "abc", "question": "Will X happen?", "clobTokenIds": []}
        parsed = _parse_market(raw)
        assert parsed is not None
        m, _end = parsed
        assert m.token_yes is None
        assert m.token_no is None

    def test_one_clob_token(self):
        raw = {"conditionId": "abc", "question": "Will X?", "clobTokenIds": ["yes_only"]}
        parsed = _parse_market(raw)
        assert parsed is not None
        m, _end = parsed
        assert m.token_yes == "yes_only"
        assert m.token_no is None

    def test_condition_id_fallback(self):
        raw = {
            "condition_id": "fallback_id",
            "question": "Will Y happen?",
            "clobTokenIds": ["a", "b"],
        }
        parsed = _parse_market(raw)
        assert parsed is not None
        m, _end = parsed
        assert m.condition_id == "fallback_id"


# ---------------------------------------------------------------------------
# Embedder
# ---------------------------------------------------------------------------


class TestEmbedder:
    def _mock_model(self):
        mock = MagicMock()
        mock.encode.return_value = np.ones((384,), dtype=np.float32)
        mock.get_sentence_embedding_dimension.return_value = 384
        return mock

    def test_embed_returns_float32_vector(self):
        embedder = Embedder()
        embedder._model = self._mock_model()
        vec = embedder.embed("some text")
        assert vec.dtype == np.float32
        assert vec.shape == (384,)

    def test_embed_batch_shape(self):
        embedder = Embedder()
        mock = MagicMock()
        mock.encode.return_value = np.ones((3, 384), dtype=np.float32)
        embedder._model = mock
        result = embedder.embed_batch(["a", "b", "c"])
        assert result.shape == (3, 384)

    def test_embed_batch_empty(self):
        embedder = Embedder()
        result = embedder.embed_batch([])
        assert result.shape == (0, 384)

    def test_load_called_on_first_embed(self):
        embedder = Embedder()
        mock = self._mock_model()
        with patch.object(embedder, "load", side_effect=lambda: setattr(embedder, "_model", mock)):
            embedder.embed("test text for embedding")
        assert embedder._model is mock


# ---------------------------------------------------------------------------
# MarketIndex.find_matches
# ---------------------------------------------------------------------------


class TestMarketIndexFindMatches:
    def _make_index(self):
        db = MagicMock()
        embedder = MagicMock()
        cfg = MagicMock()
        cfg.markets.similarity_threshold = 0.3
        cfg.markets.top_k = 5
        cfg.markets.refresh_hours = 4.0
        return MarketIndex(db=db, embedder=embedder, cfg=cfg)

    def test_returns_empty_when_no_matrix(self):
        idx = self._make_index()
        query = np.ones(384, dtype=np.float32) / np.sqrt(384)
        assert idx.find_matches(query) == []

    def test_returns_top_matches(self):
        idx = self._make_index()
        # Two markets: one similar, one not
        q1 = np.ones(384, dtype=np.float32)
        q1 /= np.linalg.norm(q1)
        q2 = np.zeros(384, dtype=np.float32)
        q2[0] = 1.0  # orthogonal to q1 (mostly)

        idx._markets = [
            Market("cid1", "Similar question", "tok_yes_1", "tok_no_1"),
            Market("cid2", "Different question", "tok_yes_2", "tok_no_2"),
        ]
        idx._matrix = np.stack([q1, q2])

        query = q1.copy()
        results = idx.find_matches(query)
        assert len(results) >= 1
        assert results[0].condition_id == "cid1"
        assert results[0].similarity > 0.9

    def test_threshold_filters_low_similarity(self):
        idx = self._make_index()
        idx._cfg.markets.similarity_threshold = 0.99  # very high threshold

        q = np.ones(384, dtype=np.float32)
        q /= np.linalg.norm(q)
        # Second market nearly orthogonal
        q2 = np.zeros(384, dtype=np.float32)
        q2[0] = 1.0

        idx._markets = [
            Market("cid1", "Q1", "ty1", "tn1"),
            Market("cid2", "Q2", "ty2", "tn2"),
        ]
        idx._matrix = np.stack([q, q2])

        query = q.copy()
        results = idx.find_matches(query)
        # Only q1 passes threshold (similarity=1.0), q2 does not
        assert all(r.similarity >= 0.99 for r in results)

    def test_top_k_limits_results(self):
        idx = self._make_index()
        idx._cfg.markets.top_k = 2
        idx._cfg.markets.similarity_threshold = 0.0

        n = 10
        vecs = np.random.randn(n, 384).astype(np.float32)
        vecs /= np.linalg.norm(vecs, axis=1, keepdims=True)
        idx._markets = [Market(f"cid{i}", f"Q{i}", f"ty{i}", f"tn{i}") for i in range(n)]
        idx._matrix = vecs

        query = vecs[0].copy()
        results = idx.find_matches(query)
        assert len(results) <= 2

    def test_results_sorted_by_similarity_descending(self):
        idx = self._make_index()
        idx._cfg.markets.similarity_threshold = 0.0
        idx._cfg.markets.top_k = 10

        n = 5
        vecs = np.random.randn(n, 384).astype(np.float32)
        vecs /= np.linalg.norm(vecs, axis=1, keepdims=True)
        idx._markets = [Market(f"cid{i}", f"Q{i}", f"ty{i}", f"tn{i}") for i in range(n)]
        idx._matrix = vecs

        query = vecs[2].copy()
        results = idx.find_matches(query)
        sims = [r.similarity for r in results]
        assert sims == sorted(sims, reverse=True)
