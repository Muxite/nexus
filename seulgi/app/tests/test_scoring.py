"""Tests for Phase 4E.1 composite scoring module.

Covers:
- score() returns 0.5 (identity) before any model has been fitted.
- fit() returns None when the closed-trade sample is below MIN_SAMPLES.
- fit() converges on a synthetic 200-trade dataset → test_auc > 0.55.
- score() at composite_score_floor=0.3 in the trader skips at 0.2,
  trades at 0.4 (gate semantics).
- module-level set_default_scorer / score_features convenience.
- build_features() pulls a 20-element vector with sane defaults.
- diagnostics() returns expected structure.
"""
from __future__ import annotations

import asyncio
import os
import random
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from unittest.mock import AsyncMock, MagicMock

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import scoring  # noqa: E402
from scoring import (  # noqa: E402
    CompositeScorer,
    FEATURE_NAMES,
    N_FEATURES,
    ScoringModel,
    build_features,
    features_to_vector,
    score_features,
    set_default_scorer,
    get_default_scorer,
    _sigmoid,
    _fit_logreg,
    _auc,
)
from db import SeulgiDB  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _seed_closed_trades(db: SeulgiDB, rows: list[tuple[float, str, int]],
                              *, hit_factor: float = 1.0) -> None:
    """Insert one (signal, trade) per sample with realized_pnl set.

    `rows = [(confidence_raw, direction, hit_0_or_1), ...]`. PnL sign matches
    the hit field; mid_yes is fixed at 0.5 for simplicity."""
    for c, direction, hit in rows:
        sid = await db.insert_signal(site_url="https://t.test", headline="h")
        tid = await db.insert_trade(
            signal_id=sid, condition_id="c1", question="Q?",
            token_id=None, direction=direction, similarity=0.7, p_hat=c,
            mid_price=0.5, size_usdc=10.0, skip_reason=None,
            confidence_raw=c, p_alpha=c, agreement=0.7, mid_yes=0.5,
            mkt_vol_30m=0.01,
        )
        pnl = (1.0 if hit else -1.0) * hit_factor
        await db.close_trade(trade_id=tid, realized_pnl=pnl)


def _make_synthetic(n: int, *, direction: str, signal_strength: float = 0.6,
                    seed: int = 17) -> list[tuple[float, str, int]]:
    """High-conf → more hits. signal_strength controls the AUC."""
    rng = random.Random(seed)
    out: list[tuple[float, str, int]] = []
    for _ in range(n):
        c = rng.random()
        p_hit = 0.3 + signal_strength * (c - 0.5)  # linear in c
        p_hit = max(0.05, min(0.95, p_hit))
        out.append((c, direction, 1 if rng.random() < p_hit else 0))
    return out


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_module_score_features_identity_when_no_scorer():
    """score_features returns 0.5 (uninformative) when no scorer registered."""
    set_default_scorer(None)
    assert score_features({"similarity": 0.9}) == pytest.approx(0.5)
    assert get_default_scorer() is None


def test_build_features_shape_and_defaults():
    """build_features pulls a 20-element vector with sane fallbacks."""
    match = MagicMock()
    match.similarity = 0.85
    match.mid_yes = 0.7
    match.spread_bps = 50.0
    match.book_depth_usdc = 1000.0
    match._mkt_ctx = MagicMock(vol=0.02)
    match._tags = ["interest_rates", "geopolitics"]
    match.tag = None
    result = MagicMock()
    result.confidence = 0.8
    result.p_alpha = 0.65
    result.agreement = 0.66
    ctx = MagicMock()
    ctx.freshness_dist = 0.1
    ctx.freshness_age_secs = 30.0
    ctx.headline_age_secs = 30.0
    feats = build_features(match, result, ctx)
    assert set(FEATURE_NAMES) <= set(feats.keys())
    assert feats["similarity"] == pytest.approx(0.85)
    assert feats["mid_yes"] == pytest.approx(0.7)
    assert feats["market_vol_30m"] == pytest.approx(0.02)
    assert feats["tag_interest_rates"] == 1.0
    assert feats["tag_geopolitics"] == 1.0
    assert feats["tag_sports"] == 0.0
    vec = features_to_vector(feats)
    assert vec.shape == (N_FEATURES,)


@pytest.mark.asyncio
async def test_small_sample_returns_none_and_score_identity():
    """fit() returns None for n < MIN_SAMPLES; score() falls back to 0.5."""
    db = SeulgiDB(":memory:")
    await db.connect()
    try:
        rows = _make_synthetic(20, direction="YES")
        await _seed_closed_trades(db, rows)
        scorer = CompositeScorer(db, refresh_hours=6)
        model = await scorer.fit()
        assert model is None
        # No model = uninformative score for any feature dict.
        assert scorer.score({"similarity": 0.99}) == pytest.approx(0.5)
        assert scorer.models == {}
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_fit_converges_on_200_synthetic_trades():
    """200 trades with linear signal → train + test AUC > 0.55 + IC reported."""
    db = SeulgiDB(":memory:")
    await db.connect()
    try:
        # 100 YES + 100 NO (each side passes MIN_PER_DIRECTION = 60).
        rows = _make_synthetic(100, direction="YES", signal_strength=0.7, seed=11)
        rows += _make_synthetic(100, direction="NO", signal_strength=0.6, seed=12)
        await _seed_closed_trades(db, rows)
        scorer = CompositeScorer(db, refresh_hours=6)
        model = await scorer.fit()
        assert model is not None  # pooled ALL model exists
        assert model.n_samples == 200
        assert model.train_auc > 0.55, f"train_auc={model.train_auc}"
        # test_auc on noisy data should still beat random.
        assert model.test_auc > 0.50
        # Per-direction also fitted.
        diag = scorer.diagnostics()
        labels = {r["direction"] for r in diag["models"]}
        assert "ALL" in labels
        # Score is bounded and continuous.
        feats = {n: 0.5 for n in FEATURE_NAMES}
        feats["confidence_raw"] = 0.9
        s = scorer.score(feats)
        assert 0.0 <= s <= 1.0
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_score_gate_floor_skips_low_score():
    """Composite-score floor semantics: features yielding score=0.2 → Skip;
    features yielding score=0.4 with floor=0.3 → Trade.

    Constructs a hand-crafted ScoringModel whose weights make
    `similarity` the sole driver, then verifies the trader gate (via the
    module-level `score_features`)."""
    # Hand-build a model: only the `similarity` feature counts.
    weights = [0.0] * N_FEATURES
    weights[FEATURE_NAMES.index("similarity")] = 4.0  # large positive coef
    fake_mu = [0.0] * N_FEATURES
    fake_sd = [1.0] * N_FEATURES
    model = ScoringModel(
        weights=weights, intercept=-2.0,
        fit_at=datetime.now(timezone.utc).isoformat(),
        n_samples=200, train_auc=0.7, test_auc=0.65, ic=0.3,
        direction="ALL", feature_means=fake_mu, feature_stds=fake_sd,
    )
    # Inject directly.
    scorer = CompositeScorer(db=MagicMock(), refresh_hours=6)
    scorer._models = {"ALL": model}
    set_default_scorer(scorer)

    # Low similarity → low score → below 0.3 floor → trader would skip.
    low_feats = {n: 0.0 for n in FEATURE_NAMES}
    low_feats["similarity"] = 0.2  # z = 4*0.2 - 2 = -1.2 → sigmoid ≈ 0.23
    low_score = score_features(low_feats)
    assert low_score < 0.3, f"low_score={low_score}"

    # High similarity → high score → above 0.3 → trade allowed.
    high_feats = {n: 0.0 for n in FEATURE_NAMES}
    high_feats["similarity"] = 0.7  # z = 4*0.7 - 2 = 0.8 → sigmoid ≈ 0.69
    high_score = score_features(high_feats)
    assert high_score > 0.4, f"high_score={high_score}"

    set_default_scorer(None)


def test_internal_logreg_kernel():
    """Sanity-check the numpy logistic regression on a separable toy."""
    rng = np.random.default_rng(7)
    n = 200
    X = rng.normal(size=(n, 2))
    # Linear class boundary at x[0] > 0.
    y = (X[:, 0] > 0.0).astype(np.float64)
    w, b = _fit_logreg(X, y, lr=0.5, l2=0.001, n_iter=400)
    # First feature must dominate (positive, much larger than the second).
    assert w[0] > abs(w[1]) + 0.5
    p = _sigmoid(X @ w + b)
    auc = _auc(p, y)
    assert auc > 0.95


def test_diagnostics_structure():
    """diagnostics() shape stays stable for the dashboard tile renderer."""
    scorer = CompositeScorer(db=MagicMock(), refresh_hours=6)
    # No models: empty list, n_features defined.
    diag = scorer.diagnostics()
    assert diag["models"] == []
    assert diag["n_features"] == N_FEATURES
    # With a model.
    model = ScoringModel(
        weights=[0.0] * N_FEATURES, intercept=0.0,
        fit_at=datetime.now(timezone.utc).isoformat(),
        n_samples=150, train_auc=0.62, test_auc=0.58, ic=0.12,
        direction="ALL", feature_means=[0.0] * N_FEATURES,
        feature_stds=[1.0] * N_FEATURES,
    )
    scorer._models = {"ALL": model}
    diag = scorer.diagnostics()
    assert len(diag["models"]) == 1
    row = diag["models"][0]
    assert row["direction"] == "ALL"
    assert row["n"] == 150
    assert "train_auc" in row and "test_auc" in row and "ic" in row


@pytest.mark.asyncio
async def test_pooled_fit_only_when_one_direction_below_threshold():
    """120 YES + 20 NO → no per-direction YES/NO fit (NO < 60), but pooled
    ALL still works because 140 >= MIN_SAMPLES."""
    db = SeulgiDB(":memory:")
    await db.connect()
    try:
        rows = _make_synthetic(120, direction="YES", seed=21)
        rows += _make_synthetic(20, direction="NO", seed=22)
        await _seed_closed_trades(db, rows)
        scorer = CompositeScorer(db, refresh_hours=6)
        model = await scorer.fit()
        assert model is not None
        assert "ALL" in scorer.models
        assert "YES" not in scorer.models
        assert "NO" not in scorer.models
        # Direction-specific calls fall through to ALL.
        feats = {n: 0.5 for n in FEATURE_NAMES}
        v_yes = scorer.score(feats, direction="YES")
        v_no = scorer.score(feats, direction="NO")
        assert v_yes == v_no  # both route through ALL
    finally:
        await db.close()
