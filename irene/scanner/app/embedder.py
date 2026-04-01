"""
Sentence-transformer embedder for scanner and feeder services.

Loads all-MiniLM-L6-v2 once on init and produces normalised float32 vectors.
CPU-only; no GPU required. Model is ~80 MB and cached in the Docker volume
irene_model_cache.

Embedding computation is synchronous (numpy/torch) and should be called from
asyncio via run_in_executor to avoid blocking the event loop.
"""

from __future__ import annotations

import logging
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)

# Module-level import so tests can patch `embedder.SentenceTransformer`.
# Wrapped in try/except so the module can be imported without the package
# installed (e.g. during unit tests that mock the class).
try:
    from sentence_transformers import SentenceTransformer  # type: ignore
except ImportError:  # pragma: no cover
    SentenceTransformer = None  # type: ignore

_DEFAULT_MODEL = "sentence-transformers/all-MiniLM-L6-v2"


class Embedder:
    """
    Wraps sentence-transformers to produce normalised L2 float32 vectors.

    Usage (async context):
        loop = asyncio.get_event_loop()
        vec = await loop.run_in_executor(None, embedder.embed, text)
    """

    def __init__(self, model_name: str = _DEFAULT_MODEL) -> None:
        self._model_name = model_name
        self._model = None  # lazy-loaded on first embed()

    def load(self) -> None:
        """Explicitly load the model. Call once at startup."""
        if SentenceTransformer is None:  # pragma: no cover
            raise ImportError("sentence-transformers is not installed")
        logger.info("Loading embedding model %s …", self._model_name)
        self._model = SentenceTransformer(self._model_name)
        logger.info("Embedding model loaded (dim=%d)", self.dim)

    @property
    def dim(self) -> int:
        """Embedding dimension (384 for all-MiniLM-L6-v2)."""
        if self._model is None:
            raise RuntimeError("Call load() before accessing dim")
        return self._model.get_sentence_embedding_dimension()

    def embed(self, text: str) -> np.ndarray:
        """
        Embed a single text string.

        Returns a normalised float32 ndarray of shape (dim,).
        Thread-safe after load().
        """
        if self._model is None:
            self.load()
        vec = self._model.encode(text, normalize_embeddings=True, show_progress_bar=False)
        return vec.astype(np.float32)

    def embed_batch(self, texts: list[str]) -> list[np.ndarray]:
        """
        Embed multiple texts in one forward pass.

        Returns a list of normalised float32 ndarrays.
        """
        if not texts:
            return []
        if self._model is None:
            self.load()
        vecs = self._model.encode(
            texts,
            normalize_embeddings=True,
            show_progress_bar=False,
            batch_size=64,
        )
        return [v.astype(np.float32) for v in vecs]

    @staticmethod
    def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
        """
        Cosine similarity between two normalised vectors.

        Since both are L2-normalised, this is just the dot product.
        """
        return float(np.dot(a, b))

    @staticmethod
    def batch_cosine_similarity(
        query: np.ndarray,
        matrix: np.ndarray,
    ) -> np.ndarray:
        """
        Cosine similarity between one query vector and a matrix of candidates.

        Args:
            query:  shape (dim,)
            matrix: shape (N, dim) — rows are normalised candidate vectors

        Returns:
            similarities: shape (N,)

        # PERF: consider C++ extension if N > 50,000 and call frequency
        # is sub-second. numpy dot is sufficient at current market scale (~2000).
        """
        return matrix @ query  # dot product since both are L2-normalised
