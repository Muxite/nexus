"""
Pluggable news→market matcher protocol + two implementations (A4E.2).

## Specification
**Purpose.** Augment `markets.py:find_matches` with non-cosine ranking
strategies. Each matcher contributes `extra_score ∈ [0, 1]` folded into
the final ranking via a per-matcher weight. OPT-IN per config — default
(cosine-only) behaviour is preserved.

**Public interface.**
  - `MatchScore` — dataclass each matcher emits per candidate.
  - `Matcher` — runtime-checkable Protocol with one `rerank()` method.
  - `CrossEncoderReranker` — fires only on cosine ∈ [band_low, band_high]
    (default [0.3, 0.55]). SCAFFOLDED zero-score fallback when the
    model can't load. Opt-in.
  - `LearnedMLPMatcher` — numpy MLP (768→32→16→1) on `(headline_emb,
    market_emb) → P(realized_pnl > 0)`. Trains on ≤ 200 closed trades
    sampled from the DB. Weights in-memory; refit on boot + every
    `refresh_days` days (caller's responsibility).

**Irene merge point.** A sibling session ships a `KnowledgeGraphMatcher`
as the fourth strategy. The Matcher Protocol is intentionally minimal so
irene plugs in here without changes.

**Invariants.** `extra_score ∈ [0, 1]`; one MatchScore per input candidate
in input order; on exception every score collapses to 0.0 (matcher
errors NEVER tank cosine).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional, Protocol, runtime_checkable

import numpy as np

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public dataclass + Protocol
# ---------------------------------------------------------------------------


@dataclass
class MatchScore:
    """One matcher's contribution to a single candidate's final rank.

    The caller folds `extra_score` (× `weight`) onto the candidate's
    cosine similarity to produce the final sort key.
    """
    cond_id: str
    cosine_sim: float                # original SBERT
    extra_score: float               # 0..1, this matcher's contribution
    method: str                      # "cross_encoder" | "learned_mlp" | "knowledge_graph"


@runtime_checkable
class Matcher(Protocol):
    """Pluggable matcher protocol. Implementations:
    `CrossEncoderReranker`, `LearnedMLPMatcher`,
    `irene.KnowledgeGraphMatcher` (future)."""

    async def rerank(
        self,
        headline: str,
        headline_emb: list[float],
        candidates: list,                       # list[MarketMatch]
    ) -> list[MatchScore]:
        ...


# ---------------------------------------------------------------------------
# CrossEncoderReranker
# ---------------------------------------------------------------------------


class CrossEncoderReranker:
    """Sentence-transformers cross-encoder fired on cosine ∈
    [band_low, band_high]. Out-of-band candidates pass through with
    extra_score=0. Scaffolded zero-score fallback when the model can't
    load (default until operator opts in)."""

    METHOD = "cross_encoder"

    def __init__(
        self,
        model_name: str = "cross-encoder/ms-marco-MiniLM-L-6-v2",
        band_low: float = 0.3,
        band_high: float = 0.55,
    ) -> None:
        self._model_name = model_name
        self._band_low = float(band_low)
        self._band_high = float(band_high)
        self._model: Optional[object] = None
        self._load_failed: bool = False  # latches True after a failed lazy-load

    def _maybe_load(self) -> None:
        """Lazy-load the cross-encoder; latch _load_failed on error."""
        if self._model is not None or self._load_failed:
            return
        try:
            from sentence_transformers import CrossEncoder  # type: ignore
            self._model = CrossEncoder(self._model_name)
            logger.info("CrossEncoderReranker: loaded %s", self._model_name)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "CrossEncoderReranker: %s unavailable (%s) — scaffolded "
                "zero-score mode. TODO: opt-in flips this on first use.",
                self._model_name, exc,
            )
            self._load_failed = True

    def rerank_sync(
        self,
        headline: str,
        headline_emb: list[float],
        candidates: list,
    ) -> list[MatchScore]:
        """Synchronous rerank — invoked from `markets.find_matches`.
        Identical math to `rerank()`; lifted out so the sync caller
        doesn't need an event loop."""
        if not candidates:
            return []
        self._maybe_load()

        scores: list[MatchScore] = []
        in_band_idx: list[int] = []
        pairs: list[tuple[str, str]] = []
        for i, m in enumerate(candidates):
            sim = float(getattr(m, "similarity", 0.0))
            if self._band_low <= sim < self._band_high and self._model is not None:
                in_band_idx.append(i)
                pairs.append((headline, getattr(m, "question", "")))

        ce_scores: dict[int, float] = {}
        if pairs and self._model is not None:
            try:
                raw = self._model.predict(pairs)  # type: ignore[attr-defined]
                # Sigmoid-squash logits to [0, 1]; idempotent on probs.
                arr = np.asarray(raw, dtype=np.float32).ravel()
                squashed = 1.0 / (1.0 + np.exp(-arr))
                for k, idx in enumerate(in_band_idx):
                    ce_scores[idx] = float(np.clip(squashed[k], 0.0, 1.0))
            except Exception as exc:  # noqa: BLE001
                logger.warning("CrossEncoderReranker.predict failed: %s", exc)

        for i, m in enumerate(candidates):
            scores.append(MatchScore(
                cond_id=getattr(m, "condition_id", ""),
                cosine_sim=float(getattr(m, "similarity", 0.0)),
                extra_score=ce_scores.get(i, 0.0),
                method=self.METHOD,
            ))
        return scores

    async def rerank(
        self,
        headline: str,
        headline_emb: list[float],
        candidates: list,
    ) -> list[MatchScore]:
        """Async Protocol entry point — delegates to `rerank_sync`."""
        return self.rerank_sync(headline, headline_emb, candidates)


# ---------------------------------------------------------------------------
# LearnedMLPMatcher — tiny numpy MLP, no torch dep
# ---------------------------------------------------------------------------


class LearnedMLPMatcher:
    """Numpy MLP (768→32→16→1) on `concat(headline_emb, market_emb)` →
    `P(realized_pnl > 0)`. Pure numpy (no torch dep). Weights in-memory;
    `fit()` runs on boot and on a refresh cadence the caller owns."""

    METHOD = "learned_mlp"
    EMBED_DIM = 384

    def __init__(self, db=None, refresh_days: int = 7, min_train_samples: int = 30) -> None:
        self._db = db
        self._refresh_days = int(refresh_days)
        self._min_train_samples = int(min_train_samples)
        self._fitted: bool = False
        # Layer 1: 768 → 32; Layer 2: 32 → 16; Output: 16 → 1
        rng = np.random.default_rng(seed=42)
        self._W1 = (rng.standard_normal((768, 32)) * 0.05).astype(np.float32)
        self._b1 = np.zeros(32, dtype=np.float32)
        self._W2 = (rng.standard_normal((32, 16)) * 0.05).astype(np.float32)
        self._b2 = np.zeros(16, dtype=np.float32)
        self._W3 = (rng.standard_normal((16, 1)) * 0.05).astype(np.float32)
        self._b3 = np.zeros(1, dtype=np.float32)

    async def _sample_training_data(self) -> tuple[np.ndarray, np.ndarray]:
        """Pull ≤ 200 closed trades' (headline_emb, market_emb, pnl>0)."""
        if self._db is None:
            return np.zeros((0, 768), dtype=np.float32), np.zeros(0, dtype=np.float32)
        try:
            async with self._db._conn.execute(
                """
                SELECT
                    t.realized_pnl AS pnl,
                    di.embedding_bytes AS hemb,
                    m.embedding AS memb
                FROM trades t
                JOIN decision_inputs di ON di.headline_id = t.headline_id
                JOIN markets m ON m.condition_id = t.condition_id
                WHERE t.realized_pnl IS NOT NULL
                  AND di.embedding_bytes IS NOT NULL
                  AND m.embedding IS NOT NULL
                ORDER BY t.closed_at DESC
                LIMIT 200
                """
            ) as cur:
                rows = await cur.fetchall()
        except Exception as exc:  # noqa: BLE001
            logger.debug("LearnedMLPMatcher: training-sample query failed: %s", exc)
            return np.zeros((0, 768), dtype=np.float32), np.zeros(0, dtype=np.float32)

        xs: list[np.ndarray] = []
        ys: list[float] = []
        for r in rows:
            try:
                h = np.frombuffer(r["hemb"], dtype=np.float32)
                k = np.frombuffer(r["memb"], dtype=np.float32)
            except Exception:  # noqa: BLE001
                continue
            if h.size != self.EMBED_DIM or k.size != self.EMBED_DIM:
                continue
            xs.append(np.concatenate([h, k]).astype(np.float32))
            ys.append(1.0 if float(r["pnl"]) > 0 else 0.0)
        if not xs:
            return np.zeros((0, 768), dtype=np.float32), np.zeros(0, dtype=np.float32)
        return np.stack(xs), np.array(ys, dtype=np.float32)

    @staticmethod
    def _relu(x: np.ndarray) -> np.ndarray:
        return np.maximum(x, 0.0)

    @staticmethod
    def _sigmoid(x: np.ndarray) -> np.ndarray:
        # Numerically stable sigmoid.
        return np.where(x >= 0, 1.0 / (1.0 + np.exp(-x)), np.exp(x) / (1.0 + np.exp(x)))

    def _forward(self, X: np.ndarray) -> tuple[np.ndarray, ...]:
        z1 = X @ self._W1 + self._b1
        a1 = self._relu(z1)
        z2 = a1 @ self._W2 + self._b2
        a2 = self._relu(z2)
        z3 = a2 @ self._W3 + self._b3
        out = self._sigmoid(z3).ravel()
        return out, a1, a2, z1, z2

    async def fit(self, X: Optional[np.ndarray] = None, y: Optional[np.ndarray] = None) -> bool:
        """Train the MLP. `X`/`y` override the DB-sampled set (test path).
        Returns True iff ≥ min_train_samples were available."""
        if X is None or y is None:
            X, y = await self._sample_training_data()
        if X.shape[0] < self._min_train_samples:
            logger.info(
                "LearnedMLPMatcher: only %d closed-trade samples (<%d) — "
                "refusing to fit; score() will return neutral 0.5.",
                X.shape[0], self._min_train_samples,
            )
            self._fitted = False
            return False

        # Vanilla SGD + BCE loss, 200 epochs (sub-second on 200×768 CPU).
        lr = 0.05
        n_epochs = 200
        y_col = y.reshape(-1, 1)
        for epoch in range(n_epochs):
            out, a1, a2, _, _ = self._forward(X)
            # dL/dz3 = pred - y  (BCE + sigmoid combined gradient)
            dz3 = (out.reshape(-1, 1) - y_col) / X.shape[0]
            dW3 = a2.T @ dz3
            db3 = dz3.sum(axis=0)
            da2 = dz3 @ self._W3.T
            dz2 = da2 * (a2 > 0)
            dW2 = a1.T @ dz2
            db2 = dz2.sum(axis=0)
            da1 = dz2 @ self._W2.T
            dz1 = da1 * (a1 > 0)
            dW1 = X.T @ dz1
            db1 = dz1.sum(axis=0)
            # Linear lr decay over the last quarter — stabilises convergence.
            lr_eff = lr * (1.0 - 0.5 * (epoch / n_epochs))
            self._W1 -= lr_eff * dW1
            self._b1 -= lr_eff * db1
            self._W2 -= lr_eff * dW2
            self._b2 -= lr_eff * db2
            self._W3 -= lr_eff * dW3
            self._b3 -= lr_eff * db3
        self._fitted = True
        logger.info("LearnedMLPMatcher: fitted on %d closed-trade samples", X.shape[0])
        return True

    def score(self, headline_emb, market_emb) -> float:
        """Score one (headline, market) pair → P(profitable) ∈ [0, 1]."""
        if not self._fitted:
            return 0.5
        h = np.asarray(headline_emb, dtype=np.float32).ravel()
        m = np.asarray(market_emb, dtype=np.float32).ravel()
        if h.size != self.EMBED_DIM or m.size != self.EMBED_DIM:
            return 0.5
        X = np.concatenate([h, m]).reshape(1, -1)
        out, *_ = self._forward(X)
        return float(np.clip(out[0], 0.0, 1.0))

    def rerank_sync(
        self,
        headline: str,
        headline_emb: list[float],
        candidates: list,
    ) -> list[MatchScore]:
        """Synchronous rerank — invoked from `markets.find_matches`.
        Cold-boot / unfit → neutral 0.5. Reads `market_embedding` off
        the MarketMatch when the caller attaches it; otherwise neutral
        (preserves cosine behaviour)."""
        h = np.asarray(headline_emb, dtype=np.float32).ravel()
        scores: list[MatchScore] = []
        for m in candidates:
            memb = getattr(m, "market_embedding", None)
            extra = self.score(h, memb) if memb is not None else 0.5
            scores.append(MatchScore(
                cond_id=getattr(m, "condition_id", ""),
                cosine_sim=float(getattr(m, "similarity", 0.0)),
                extra_score=float(np.clip(extra, 0.0, 1.0)),
                method=self.METHOD,
            ))
        return scores

    async def rerank(
        self,
        headline: str,
        headline_emb: list[float],
        candidates: list,
    ) -> list[MatchScore]:
        """Async Protocol entry point — delegates to `rerank_sync`."""
        return self.rerank_sync(headline, headline_emb, candidates)
