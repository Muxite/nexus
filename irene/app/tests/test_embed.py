"""Tests for app/embed.py.

The real sentence-transformers model is heavyweight (~80MB + torch import),
so every test patches `sentence_transformers.SentenceTransformer` with a
deterministic fake. The fake returns a one-hot vector indexed by
`hash(text) % dim` so we can assert on shape, dtype, and cache hits without
ever touching torch.
"""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

import numpy as np
import pytest

from app.embed import Embedder, EmbedderDimMismatch


# ---------------------------------------------------------------------------
# Fake model
# ---------------------------------------------------------------------------


class _FakeModel:
    """Deterministic stand-in for SentenceTransformer.

    `encode()` returns a one-hot float vector of length `out_dim`. The hot
    index is `hash(text) % out_dim`. A list input returns a 2-D array.
    """

    def __init__(self, out_dim: int) -> None:
        self.out_dim = out_dim
        self.encode = MagicMock(side_effect=self._encode)

    def _encode(self, text, **kwargs):
        if isinstance(text, list):
            arr = np.zeros((len(text), self.out_dim), dtype=np.float32)
            for i, t in enumerate(text):
                arr[i, hash(t) % self.out_dim] = 1.0
            return arr
        vec = np.zeros(self.out_dim, dtype=np.float32)
        vec[hash(text) % self.out_dim] = 1.0
        return vec


def _install_fake_module(monkeypatch, out_dim: int) -> _FakeModel:
    """Patch `sentence_transformers.SentenceTransformer` so the lazy import
    inside `Embedder._load` resolves to a factory returning our fake."""
    fake_model = _FakeModel(out_dim)
    fake_module = types.ModuleType("sentence_transformers")
    fake_module.SentenceTransformer = MagicMock(return_value=fake_model)  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "sentence_transformers", fake_module)
    return fake_model


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_embedder_id_format():
    emb = Embedder("sentence-transformers/all-MiniLM-L6-v2", 384)
    assert emb.embedder_id == "sentence-transformers/all-MiniLM-L6-v2@384"


def test_lazy_load_no_import_at_construction(monkeypatch):
    # If construction loaded the model the MagicMock factory below would be
    # invoked even though we never call embed().
    fake = _install_fake_module(monkeypatch, out_dim=8)
    Embedder("dummy", 8)
    assert sys.modules["sentence_transformers"].SentenceTransformer.call_count == 0
    _ = fake  # silence unused warning


def test_embed_returns_shape_and_dtype(monkeypatch):
    _install_fake_module(monkeypatch, out_dim=16)
    emb = Embedder("dummy", 16)
    vec = emb.embed("hello world")
    assert vec.shape == (16,)
    assert vec.dtype == np.float32


def test_embed_batch_shape_and_dtype(monkeypatch):
    _install_fake_module(monkeypatch, out_dim=16)
    emb = Embedder("dummy", 16)
    out = emb.embed_batch(["a", "b", "c"])
    assert out.shape == (3, 16)
    assert out.dtype == np.float32


def test_embed_batch_empty_input(monkeypatch):
    # Empty input must short-circuit without loading the model.
    _install_fake_module(monkeypatch, out_dim=16)
    emb = Embedder("dummy", 16)
    out = emb.embed_batch([])
    assert out.shape == (0, 16)
    assert out.dtype == np.float32
    assert sys.modules["sentence_transformers"].SentenceTransformer.call_count == 0


def test_cache_hit_on_repeated_text(monkeypatch):
    fake = _install_fake_module(monkeypatch, out_dim=8)
    emb = Embedder("dummy", 8)
    v1 = emb.embed("same text")
    v2 = emb.embed("same text")
    assert np.array_equal(v1, v2)
    # The underlying model was invoked exactly once despite two embed() calls.
    assert fake.encode.call_count == 1


def test_cache_hit_in_batch(monkeypatch):
    # Re-embedding the same texts via batch should not re-invoke encode for
    # the cached entries.
    fake = _install_fake_module(monkeypatch, out_dim=8)
    emb = Embedder("dummy", 8)
    emb.embed_batch(["a", "b"])
    assert fake.encode.call_count == 1
    emb.embed_batch(["a", "b"])
    # Second call was a full cache hit — encode() not called again.
    assert fake.encode.call_count == 1


def test_dim_mismatch_raises(monkeypatch):
    # Model returns dim=16 but Embedder was declared with dim=32.
    _install_fake_module(monkeypatch, out_dim=16)
    emb = Embedder("dummy", 32)
    with pytest.raises(EmbedderDimMismatch):
        emb.embed("anything")


def test_dim_mismatch_raises_in_batch(monkeypatch):
    _install_fake_module(monkeypatch, out_dim=16)
    emb = Embedder("dummy", 32)
    with pytest.raises(EmbedderDimMismatch):
        emb.embed_batch(["a", "b"])


def test_cache_eviction_lru(monkeypatch):
    fake = _install_fake_module(monkeypatch, out_dim=4)
    emb = Embedder("dummy", 4, cache_size=2)
    emb.embed("one")   # cache: [one]
    emb.embed("two")   # cache: [one, two]
    emb.embed("three") # cache: [two, three]  — 'one' evicted
    assert fake.encode.call_count == 3
    emb.embed("two")   # cache hit
    assert fake.encode.call_count == 3
    emb.embed("one")   # was evicted — must re-encode
    assert fake.encode.call_count == 4


def test_cache_lru_moves_recent_to_end(monkeypatch):
    # Access pattern: insert A, B; touch A; insert C. B should be evicted,
    # not A, because A was most-recently used.
    fake = _install_fake_module(monkeypatch, out_dim=4)
    emb = Embedder("dummy", 4, cache_size=2)
    emb.embed("A")
    emb.embed("B")
    emb.embed("A")     # cache hit, moves A to end
    assert fake.encode.call_count == 2
    emb.embed("C")     # evicts B (not A)
    assert fake.encode.call_count == 3
    emb.embed("A")     # still cached
    assert fake.encode.call_count == 3
    emb.embed("B")     # evicted — must re-encode
    assert fake.encode.call_count == 4
