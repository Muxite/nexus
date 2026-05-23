"""
## Specification

**Purpose.** Thin wrapper around a `sentence-transformers` model that
exposes a pure-compute embedding surface for the rest of the pipeline.
Owns no tables; written for read-only consumption by stages that pull an
`Embedder` instance out of `StageDeps`.

**Public interface.**
- `Embedder(model_name: str, dim: int, *, cache_size: int = 4096)` —
  declares the model to load and the dimensionality the caller expects.
  The model itself is loaded lazily on first `embed()`/`embed_batch()`
  call so importing this module is cheap (no torch import at top level).
- `Embedder.embedder_id` — `f"{model_name}@{dim}"`. Every embedding row
  written downstream stamps this string. The seulgi lesson: cosine math
  on rows from two model spaces silently returns garbage; the id is the
  cheap guard.
- `Embedder.embed(text: str) -> np.ndarray` — shape `(dim,)`, dtype
  `float32`.
- `Embedder.embed_batch(texts: list[str]) -> np.ndarray` — shape
  `(N, dim)`, dtype `float32`. An empty list returns shape `(0, dim)`.
- `EmbedderDimMismatch` — raised when the loaded model returns a vector
  whose dimensionality does not match the constructor-declared `dim`.
  This is a configuration error, surfaced at the first call rather than
  silently corrupting the embedding column.

**Invariants.**
- The cache is an in-process LRU keyed by `sha1(text)`, capped at
  `cache_size` entries (default 4096), evicted in insertion order on
  overflow. The cache is *per Embedder instance* — no module-level
  mutable state (modularization rule 5).
- The model is loaded at most once per instance, on the first call that
  needs it. Concurrent first calls are not made safe here; the wiring in
  `main.py` warms the model serially before stages start.
- `embed_batch([])` short-circuits to `(0, dim)` without touching the
  model.
- Output dtype is always `float32` — the matcher's cosine math runs in
  float32 and a stray float64 column would silently double memory.

**Side effects.** First call imports `sentence_transformers` and downloads
~80MB of model weights into the HuggingFace cache. After that, pure
compute.

**Failure modes.**
- `EmbedderDimMismatch` on the first call if the loaded model's
  dimensionality contradicts the declared `dim`.
- `ImportError` (from `sentence_transformers`) propagates if the package
  is not installed in the environment.

**Non-goals.** Persisting the cache across processes (revisit in P2 when
extraction-call volume grows). Async wrapping (callers run `embed` in a
thread executor — see seulgi `markets.py` for the pattern). Writing
embeddings to the DB; that is each stage's responsibility.
"""

from __future__ import annotations

import hashlib
import logging
from collections import OrderedDict
from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:  # heavy imports only for type-checkers, never at runtime
    from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)


class EmbedderDimMismatch(RuntimeError):
    """Raised when the loaded model's output dim disagrees with the
    constructor-declared `dim`."""


def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


class Embedder:
    def __init__(
        self,
        model_name: str,
        dim: int,
        *,
        cache_size: int = 4096,
    ) -> None:
        self._model_name = model_name
        self._dim = dim
        self._cache_size = cache_size
        self._model: "SentenceTransformer | None" = None
        # OrderedDict so move_to_end + popitem(last=False) implements LRU
        # without dragging in a third-party cache library.
        self._cache: "OrderedDict[str, np.ndarray]" = OrderedDict()

    @property
    def embedder_id(self) -> str:
        """Returns `{model_name}@{dim}` — stamped on every embedding row."""
        return f"{self._model_name}@{self._dim}"

    def _load(self) -> None:
        if self._model is not None:
            return
        # Local import: keeps `import app.embed` cheap (no torch at top level).
        from sentence_transformers import SentenceTransformer

        logger.info("loading embedding model %s", self._model_name)
        self._model = SentenceTransformer(self._model_name)

    def _cache_get(self, key: str) -> np.ndarray | None:
        vec = self._cache.get(key)
        if vec is None:
            return None
        self._cache.move_to_end(key)
        return vec

    def _cache_put(self, key: str, vec: np.ndarray) -> None:
        if key in self._cache:
            self._cache.move_to_end(key)
            self._cache[key] = vec
            return
        self._cache[key] = vec
        while len(self._cache) > self._cache_size:
            self._cache.popitem(last=False)

    def _check_dim(self, vec: np.ndarray) -> None:
        if vec.shape[-1] != self._dim:
            raise EmbedderDimMismatch(
                f"model {self._model_name!r} returned dim {vec.shape[-1]} "
                f"but Embedder was declared with dim={self._dim}"
            )

    def embed(self, text: str) -> np.ndarray:
        key = _sha1(text)
        cached = self._cache_get(key)
        if cached is not None:
            return cached
        self._load()
        assert self._model is not None
        raw = self._model.encode(
            text,
            normalize_embeddings=True,
            show_progress_bar=False,
        )
        vec = np.asarray(raw, dtype=np.float32).reshape(-1)
        self._check_dim(vec)
        self._cache_put(key, vec)
        return vec

    def embed_batch(self, texts: list[str]) -> np.ndarray:
        if not texts:
            return np.zeros((0, self._dim), dtype=np.float32)

        out = np.empty((len(texts), self._dim), dtype=np.float32)
        missing_idx: list[int] = []
        missing_texts: list[str] = []
        missing_keys: list[str] = []
        for i, t in enumerate(texts):
            key = _sha1(t)
            cached = self._cache_get(key)
            if cached is None:
                missing_idx.append(i)
                missing_texts.append(t)
                missing_keys.append(key)
            else:
                out[i] = cached

        if missing_texts:
            self._load()
            assert self._model is not None
            raw = self._model.encode(
                missing_texts,
                normalize_embeddings=True,
                show_progress_bar=False,
                batch_size=64,
            )
            arr = np.asarray(raw, dtype=np.float32)
            if arr.ndim == 1:
                # Single-row models occasionally return a flat vector.
                arr = arr.reshape(1, -1)
            if arr.shape[-1] != self._dim:
                raise EmbedderDimMismatch(
                    f"model {self._model_name!r} returned dim {arr.shape[-1]} "
                    f"but Embedder was declared with dim={self._dim}"
                )
            for slot, key, row in zip(missing_idx, missing_keys, arr):
                out[slot] = row
                self._cache_put(key, row)

        return out
