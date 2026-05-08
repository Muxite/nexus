"""
Unit tests for news_memory.NewsMemory (ChromaDB semantic deduplication).

All tests use an in-memory or temp-dir ChromaDB instance — no network required.
"""

from __future__ import annotations

import tempfile
import threading
from unittest.mock import MagicMock, patch

import numpy as np
import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_chroma_path(tmp_path):
    return str(tmp_path / "chroma")


@pytest.fixture
def fake_embedder():
    """Returns a mock embedder; embed() returns a fixed 4-d vector."""
    m = MagicMock()
    m.embed.return_value = np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float32)
    return m


def _make_memory(tmp_path: str, embedder, threshold: float = 0.12):
    """Import and construct a NewsMemory using a real ChromaDB PersistentClient."""
    pytest.importorskip("chromadb", reason="chromadb not installed")
    from news_memory import NewsMemory
    return NewsMemory(path=tmp_path, embedder=embedder, threshold=threshold)


def _random_vec(dim: int = 4) -> np.ndarray:
    v = np.random.rand(dim).astype(np.float32)
    return v / np.linalg.norm(v)


# ---------------------------------------------------------------------------
# Basic behaviour
# ---------------------------------------------------------------------------


def test_empty_collection_returns_not_republish(tmp_chroma_path, fake_embedder):
    mem = _make_memory(tmp_chroma_path, fake_embedder)
    is_rep, similar = mem.check(
        headline="Breaking news headline",
        embedding=_random_vec(),
        site_url="https://apnews.com",
        article_url="https://apnews.com/article/1",
    )
    assert is_rep is False
    assert similar is None


def test_identical_headline_detected_as_republish(tmp_chroma_path, fake_embedder):
    """Adding a headline and then checking the same embedding → republish."""
    mem = _make_memory(tmp_chroma_path, fake_embedder, threshold=0.99)
    vec = _random_vec()
    headline = "Fed raises rates by 25bp"

    # First check: not a republish, gets stored
    is_rep, _ = mem.check(headline, vec, "https://reuters.com", "https://reuters.com/1")
    assert is_rep is False

    # Second check with the exact same vector → below threshold → republish
    is_rep2, similar = mem.check(headline, vec, "https://bbc.com", "https://bbc.com/2")
    assert is_rep2 is True
    assert similar == headline


def test_similar_headline_above_threshold_is_republish(tmp_chroma_path, fake_embedder):
    """Two headlines whose vectors are nearly identical → republish when threshold is generous."""
    mem = _make_memory(tmp_chroma_path, fake_embedder, threshold=0.99)

    base_vec = np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32)
    similar_vec = np.array([0.9999, 0.001, 0.001, 0.001], dtype=np.float32)
    similar_vec /= np.linalg.norm(similar_vec)

    # Store the first headline
    mem.check("Original headline", base_vec, "https://reuters.com", "https://reuters.com/1")

    # Check with very similar vector
    is_rep, similar = mem.check("Slightly rephrased headline", similar_vec, "https://bbc.com", "https://bbc.com/2")
    assert is_rep is True


def test_dissimilar_headline_not_republish(tmp_chroma_path, fake_embedder):
    """Orthogonal vectors → not a republish."""
    mem = _make_memory(tmp_chroma_path, fake_embedder, threshold=0.12)

    vec_a = np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32)
    vec_b = np.array([0.0, 1.0, 0.0, 0.0], dtype=np.float32)

    mem.check("Stock market rallies", vec_a, "https://cnbc.com", "https://cnbc.com/1")

    is_rep, similar = mem.check(
        "Earthquake hits Japan", vec_b, "https://bbc.com", "https://bbc.com/2"
    )
    assert is_rep is False
    assert similar is None


# ---------------------------------------------------------------------------
# Storage side-effects
# ---------------------------------------------------------------------------


def test_size_increments_on_new_headline(tmp_chroma_path, fake_embedder):
    mem = _make_memory(tmp_chroma_path, fake_embedder)
    assert mem.size == 0

    for i in range(3):
        mem.check(
            f"Unique headline {i}",
            _random_vec(),
            "https://apnews.com",
            f"https://apnews.com/{i}",
        )
    assert mem.size == 3


def test_republish_not_stored(tmp_chroma_path, fake_embedder):
    """A headline detected as a republish should NOT be added to the collection."""
    mem = _make_memory(tmp_chroma_path, fake_embedder, threshold=0.99)
    vec = np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32)

    mem.check("Original", vec, "https://reuters.com", "https://reuters.com/1")
    size_after_first = mem.size

    # Same vector → republish → should not increment size
    mem.check("Republish", vec, "https://reuters.com", "https://reuters.com/2")
    assert mem.size == size_after_first


# ---------------------------------------------------------------------------
# Disabled / degraded mode
# ---------------------------------------------------------------------------


def test_disabled_when_chromadb_not_installed(tmp_chroma_path, fake_embedder):
    """If chromadb cannot be imported, NewsMemory degrades gracefully."""
    import sys
    with patch.dict(sys.modules, {"chromadb": None}):
        from news_memory import NewsMemory
        mem = NewsMemory(path=tmp_chroma_path, embedder=fake_embedder, threshold=0.12)

    is_rep, similar = mem.check("headline", _random_vec(), "https://x.com", "https://x.com/1")
    assert is_rep is False
    assert similar is None
    assert mem.size == 0


# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------


def test_concurrent_checks_no_exceptions(tmp_chroma_path, fake_embedder):
    """Two threads calling check() concurrently should not raise."""
    mem = _make_memory(tmp_chroma_path, fake_embedder, threshold=0.12)
    errors = []

    def worker(idx):
        try:
            for j in range(5):
                mem.check(
                    f"Thread {idx} headline {j}",
                    _random_vec(),
                    "https://apnews.com",
                    f"https://apnews.com/{idx}/{j}",
                )
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == [], f"Thread errors: {errors}"
