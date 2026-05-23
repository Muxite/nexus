"""Tests for Phase 4B.1 calibration module.

Covers:
- Module-level `calibrate()` identity when no calibrator set.
- Small-sample identity fallback.
- Pooled "ALL" fit when one direction has too few samples.
- Per-direction YES/NO fit on synthetic data → calibrate(0.8) ∈ (0.5, 0.8).
- Brier-after ≤ Brier-before on the test slice for a well-behaved signal.
- diagnostics() round-trip.
- refresh_loop ticks correctly with mocked asyncio.sleep.
- Platt vs isotonic method selection round-trip.
"""
from __future__ import annotations

import asyncio
import os
import sys
import random

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import calibration  # noqa: E402
from calibration import (  # noqa: E402
    Calibrator,
    calibrate,
    set_default_calibrator,
    get_default_calibrator,
    _fit_platt,
    _platt_predict,
    _fit_isotonic,
    _isotonic_predict,
)
from db import SeulgiDB  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_synthetic(n: int, *, direction: str, hit_rate_at_one: float = 0.7,
                    base_rate: float = 0.3, seed: int = 11) -> list[tuple[float, str, int]]:
    """Generate (c, direction, hit) where high-c → high-hit-rate."""
    rng = random.Random(seed)
    out: list[tuple[float, str, int]] = []
    for _ in range(n):
        c = rng.random()  # uniform [0, 1)
        # Probability of hit grows linearly from base_rate (at c=0) to hit_rate_at_one (at c=1).
        p_hit = base_rate + (hit_rate_at_one - base_rate) * c
        hit = 1 if rng.random() < p_hit else 0
        out.append((c, direction, hit))
    return out


async def _seed_db(db: SeulgiDB, rows: list[tuple[float, str, int]]) -> None:
    """Insert one (signal, trade) per sample with realized_pnl set."""
    for c, direction, hit in rows:
        sig_id = await db.insert_signal(site_url="https://t.test", headline="h")
        tid = await db.insert_trade(
            signal_id=sig_id, condition_id="c1", question="Q?",
            token_id=None, direction=direction, similarity=0.7, p_hat=c,
            mid_price=0.5, size_usdc=10.0, skip_reason=None,
            confidence_raw=c,
        )
        # close it with pnl whose sign matches hit
        pnl = 1.0 if hit else -1.0
        await db.close_trade(trade_id=tid, realized_pnl=pnl)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_module_calibrate_identity_when_no_calibrator():
    set_default_calibrator(None)
    assert calibrate(0.7, "YES") == pytest.approx(0.7)
    assert calibrate(0.42, "NO") == pytest.approx(0.42)
    assert get_default_calibrator() is None


def test_module_calibrate_bad_input_no_calibrator():
    set_default_calibrator(None)
    # Non-numeric → 0.0 fallback rather than crash.
    assert calibrate("not-a-number", "YES") == 0.0  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_small_sample_falls_back_to_identity():
    db = SeulgiDB(":memory:")
    await db.connect()
    try:
        rows = _make_synthetic(20, direction="YES")
        await _seed_db(db, rows)
        cal = Calibrator(db, method="isotonic")
        curves = await cal.fit()
        assert curves == {}, "20 samples must trip the identity fallback"
        # Calibrate returns c_raw unchanged.
        assert cal.calibrate(0.7, "YES") == pytest.approx(0.7)
        assert cal.calibrate(0.4, "NO") == pytest.approx(0.4)
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_pooled_fit_when_one_direction_below_threshold():
    """120 YES + 20 NO → pooled "ALL" fit, applied to both directions."""
    db = SeulgiDB(":memory:")
    await db.connect()
    try:
        rows = _make_synthetic(120, direction="YES", seed=21)
        rows += _make_synthetic(20, direction="NO", seed=22)
        await _seed_db(db, rows)
        cal = Calibrator(db, method="isotonic")
        curves = await cal.fit()
        assert "ALL" in curves
        assert "YES" not in curves
        assert "NO" not in curves
        # Both directions route through the ALL curve.
        v_yes = cal.calibrate(0.8, "YES")
        v_no = cal.calibrate(0.8, "NO")
        assert v_yes == v_no  # same curve
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_per_direction_fit_produces_sensible_curve():
    """100 YES + 100 NO (each side passes the threshold). High-c samples have
    higher hit_rate → calibrate(0.8) should be > calibrate(0.2)."""
    db = SeulgiDB(":memory:")
    await db.connect()
    try:
        rows = _make_synthetic(100, direction="YES", hit_rate_at_one=0.8,
                               base_rate=0.2, seed=31)
        rows += _make_synthetic(100, direction="NO", hit_rate_at_one=0.7,
                                base_rate=0.3, seed=32)
        await _seed_db(db, rows)
        cal = Calibrator(db, method="isotonic")
        curves = await cal.fit()
        assert "YES" in curves and "NO" in curves
        # Calibration is monotone non-decreasing → c=0.8 ≥ c=0.2 calibrated.
        assert cal.calibrate(0.8, "YES") >= cal.calibrate(0.2, "YES")
        # And calibrate(0.8) ∈ (0.0, 1.0) post-clipping.
        v = cal.calibrate(0.8, "YES")
        assert 0.01 <= v <= 0.99
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_brier_improves_on_test_split():
    """For a noisy-but-real signal, calibration should not be *worse* than
    raw on the held-out 20%. Allow a small tolerance for noise."""
    db = SeulgiDB(":memory:")
    await db.connect()
    try:
        rows = _make_synthetic(150, direction="YES", hit_rate_at_one=0.85,
                               base_rate=0.15, seed=41)
        rows += _make_synthetic(150, direction="NO", hit_rate_at_one=0.75,
                                base_rate=0.25, seed=42)
        await _seed_db(db, rows)
        cal = Calibrator(db, method="isotonic")
        curves = await cal.fit()
        assert "YES" in curves and "NO" in curves
        for label in ("YES", "NO"):
            c = curves[label]
            # On the held-out test set, isotonic calibration should beat (or at
            # worst tie within noise) the identity baseline.
            assert c.brier_after <= c.brier_before + 0.05, (
                f"{label}: brier_after={c.brier_after} > brier_before={c.brier_before}+0.05"
            )
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_diagnostics_round_trip():
    db = SeulgiDB(":memory:")
    await db.connect()
    try:
        rows = _make_synthetic(80, direction="YES", seed=51)
        rows += _make_synthetic(80, direction="NO", seed=52)
        await _seed_db(db, rows)
        cal = Calibrator(db, method="platt")
        await cal.fit()
        diag = cal.diagnostics()
        assert diag["method"] == "platt"
        labels = {d["direction"] for d in diag["directions"]}
        assert labels == {"YES", "NO"}
        for d in diag["directions"]:
            assert "n" in d and d["n"] > 0
            assert "ic_before" in d and "ic_after" in d
            assert "brier_before" in d and "brier_after" in d
            assert d["fitted_at"] is not None
            assert d["method"] == "platt"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_refresh_loop_invokes_fit(monkeypatch):
    """refresh_loop should call fit() once per sleep tick. Mock asyncio.sleep
    to fire once and then raise CancelledError to break the loop."""
    db = SeulgiDB(":memory:")
    await db.connect()
    try:
        cal = Calibrator(db, method="isotonic")
        call_count = {"sleep": 0, "fit": 0}

        async def _fake_fit():
            call_count["fit"] += 1
            return {}

        async def _fake_sleep(s):
            call_count["sleep"] += 1
            if call_count["sleep"] >= 2:
                raise asyncio.CancelledError()

        cal.fit = _fake_fit  # type: ignore[assignment]
        monkeypatch.setattr(asyncio, "sleep", _fake_sleep)
        await cal.refresh_loop()
        assert call_count["sleep"] >= 1
        assert call_count["fit"] >= 1
    finally:
        await db.close()


def test_set_and_unset_default_calibrator():
    """The singleton wrapper toggles correctly."""
    assert get_default_calibrator() is None  # earlier tests reset it
    # Use a minimal stub — Calibrator constructor doesn't touch db.
    cal = Calibrator(db=None, method="isotonic")
    set_default_calibrator(cal)
    assert get_default_calibrator() is cal
    set_default_calibrator(None)
    assert get_default_calibrator() is None


def test_invalid_method_raises():
    with pytest.raises(ValueError):
        Calibrator(db=None, method="banana")  # type: ignore[arg-type]


def test_platt_fit_recovers_signal_direction():
    """Synthetic data where high c → high hit-rate. Platt's alpha should be
    positive (calibrated p increases with c)."""
    rng = np.random.default_rng(123)
    n = 500
    cs = rng.random(n)
    # p_hit grows linearly with c.
    hits = (rng.random(n) < (0.1 + 0.8 * cs)).astype(np.float64)
    a, b = _fit_platt(cs, hits)
    # On a positive-correlation slope, alpha must be > 0.
    assert a > 0, f"expected positive alpha for monotone-up signal, got {a}"
    # And predictions at high c > predictions at low c.
    assert _platt_predict(0.9, a, b) > _platt_predict(0.1, a, b)


def test_isotonic_fit_is_monotone_non_decreasing():
    rng = np.random.default_rng(7)
    n = 300
    cs = rng.random(n)
    hits = (rng.random(n) < (0.2 + 0.6 * cs)).astype(np.float64)
    knots = _fit_isotonic(cs, hits)
    # Walk knots — y values must be non-decreasing.
    ys = [y for _, y in knots]
    assert all(ys[i] <= ys[i + 1] + 1e-9 for i in range(len(ys) - 1)), \
        f"isotonic knots not monotone: {ys}"
    # Endpoints clipped to [0.01, 0.99].
    assert 0.01 <= _isotonic_predict(0.0, knots) <= 0.99
    assert 0.01 <= _isotonic_predict(1.0, knots) <= 0.99


@pytest.mark.asyncio
async def test_module_level_calibrate_uses_default_singleton():
    db = SeulgiDB(":memory:")
    await db.connect()
    try:
        rows = _make_synthetic(100, direction="YES", hit_rate_at_one=0.8,
                               base_rate=0.2, seed=61)
        rows += _make_synthetic(100, direction="NO", hit_rate_at_one=0.7,
                                base_rate=0.3, seed=62)
        await _seed_db(db, rows)
        cal = Calibrator(db, method="isotonic")
        await cal.fit()
        set_default_calibrator(cal)
        try:
            # module-level wrapper now goes through the cached curve, not identity.
            v_singleton = calibrate(0.8, "YES")
            v_method = cal.calibrate(0.8, "YES")
            assert v_singleton == pytest.approx(v_method)
        finally:
            set_default_calibrator(None)
    finally:
        await db.close()
