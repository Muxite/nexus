"""Per-direction (YES/NO) confidence calibration — Phase 4B.1.

## Specification

    cal = Calibrator(db, method="isotonic")
    await cal.fit()                       # best-effort; identity on failure
    set_default_calibrator(cal)
    asyncio.create_task(cal.refresh_loop())
    p_true = calibrate(c_raw, direction)  # identity if no curve fitted

Sample-size policy: per-direction n ≥ 50 → fit YES + NO independently.
One side < 50 but total ≥ 100 → pooled "ALL" fit. Total < 100 → identity.
CV: 80/20 train/test (deterministic seed). Brier + IC on test slice.
Stdlib + numpy only. Algorithms ported from A3.2 prototype.
"""
from __future__ import annotations
import asyncio
import logging
import math
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal, Optional, Union

import numpy as np

logger = logging.getLogger(__name__)

@dataclass
class CalibrationCurve:
    method: Literal["platt", "isotonic", "identity"]
    direction: Literal["YES", "NO", "ALL"]
    n_samples: int
    brier_before: float
    brier_after: float
    ic_before: float
    ic_after: float
    alpha: Optional[float] = None
    beta: Optional[float] = None
    knots: Optional[list[tuple[float, float]]] = None
    fitted_at: Optional[str] = None

def _fit_platt(cs: np.ndarray, ys: np.ndarray, *, max_iter: int = 200,
               tol: float = 1e-7) -> tuple[float, float]:
    """Newton-Raphson on logistic NLL with Platt's Bayesian target smoothing."""
    n_pos = int(ys.sum()); n_neg = len(ys) - n_pos
    if n_pos == 0 or n_neg == 0 or len(ys) == 0: return 0.0, 0.0
    t = np.where(ys > 0.5, (n_pos + 1.0) / (n_pos + 2.0), 1.0 / (n_neg + 2.0))
    a, b = 0.0, 0.0
    for _ in range(max_iter):
        z = a * cs + b
        p = np.where(z >= 0, 1.0 / (1.0 + np.exp(-z)), np.exp(z) / (1.0 + np.exp(z)))
        diff = p - t
        g_a, g_b = float(np.dot(diff, cs)), float(diff.sum())
        w = p * (1.0 - p)
        H_aa, H_ab, H_bb = float(np.dot(w, cs * cs)), float(np.dot(w, cs)), float(w.sum())
        det = H_aa * H_bb - H_ab * H_ab
        if abs(det) < 1e-12: d_a, d_b = 0.5 * g_a, 0.5 * g_b
        else:
            d_a = (H_bb * g_a - H_ab * g_b) / det
            d_b = (-H_ab * g_a + H_aa * g_b) / det
        a -= d_a; b -= d_b
        if max(abs(d_a), abs(d_b)) < tol: break
    return a, b

def _platt_predict(c: float, alpha: float, beta: float) -> float:
    z = alpha * c + beta
    p = 1.0 / (1.0 + math.exp(-z)) if z >= 0 else math.exp(z) / (1.0 + math.exp(z))
    return max(0.01, min(0.99, p))

def _fit_isotonic(cs: np.ndarray, ys: np.ndarray) -> list[tuple[float, float]]:
    """Pool-Adjacent-Violators → list of (x, y) knots."""
    if len(cs) == 0: return []
    order = np.argsort(cs)
    xs, vs = cs[order].astype(float), ys[order].astype(float)
    sy: list[float] = []; sw: list[float] = []; slo: list[float] = []; shi: list[float] = []
    i, n = 0, len(xs)
    while i < n:
        j, s_y, s_w = i, 0.0, 0.0
        while j < n and xs[j] == xs[i]:
            s_y += vs[j]; s_w += 1.0; j += 1
        cy, cw, clo, chi = s_y / s_w, s_w, xs[i], xs[i]
        while sy and sy[-1] > cy:
            py, pw, plo = sy.pop(), sw.pop(), slo.pop(); shi.pop()
            cy = (py * pw + cy * cw) / (pw + cw); cw = pw + cw; clo = plo
        sy.append(cy); sw.append(cw); slo.append(clo); shi.append(chi)
        i = j
    knots: list[tuple[float, float]] = []
    for lo, hi, yv in zip(slo, shi, sy):
        if knots and lo == knots[-1][0]:
            if hi != lo: knots.append((float(hi), float(yv)))
        else:
            knots.append((float(lo), float(yv)))
            if hi != lo: knots.append((float(hi), float(yv)))
    return knots

def _isotonic_predict(c: float, knots: list[tuple[float, float]]) -> float:
    if not knots: return max(0.01, min(0.99, c))
    if c <= knots[0][0]: return max(0.01, min(0.99, knots[0][1]))
    if c >= knots[-1][0]: return max(0.01, min(0.99, knots[-1][1]))
    lo, hi = 0, len(knots) - 1
    while hi - lo > 1:
        mid = (lo + hi) // 2
        if knots[mid][0] <= c: lo = mid
        else: hi = mid
    x0, y0 = knots[lo]; x1, y1 = knots[hi]
    v = y1 if x1 == x0 else y0 + (y1 - y0) * (c - x0) / (x1 - x0)
    return max(0.01, min(0.99, v))

def _brier(cs: np.ndarray, ys: np.ndarray, predict) -> float:
    if len(cs) == 0: return float("nan")
    d = np.array([predict(float(c)) - float(y) for c, y in zip(cs, ys)])
    return float(np.mean(d * d))

def _pearson(cs: np.ndarray, ys: np.ndarray) -> float:
    if len(cs) < 2 or cs.std() == 0 or ys.std() == 0: return 0.0
    return float(np.corrcoef(cs, ys)[0, 1])

def _norm_direction(direction: Union[str, object, None]) -> Optional[str]:
    if direction is None: return None
    v = direction.value if hasattr(direction, "value") else direction
    if not isinstance(v, str): return None
    v = v.upper()
    return v if v in ("YES", "NO") else None

class Calibrator:
    """Per-direction Platt + isotonic calibration, refreshed every 4h
    from closed trades in `trades`. Identity fallback for small samples."""

    MIN_SAMPLES_PER_DIRECTION = 50
    MIN_SAMPLES_POOLED = 100
    REFRESH_TTL_SECONDS = 4 * 3600
    _TEST_FRAC = 0.2
    _RNG_SEED = 1729

    def __init__(self, db, method: Literal["platt", "isotonic"] = "isotonic"):
        if method not in ("platt", "isotonic"): raise ValueError(f"unknown method: {method!r}")
        self._db = db; self._method = method
        self._curves: dict[str, CalibrationCurve] = {}

    @property
    def method(self) -> str: return self._method

    @property
    def curves(self) -> dict[str, CalibrationCurve]: return dict(self._curves)

    async def _load_samples(self) -> list[tuple[float, str, int]]:
        conn = getattr(self._db, "_conn", None)
        if conn is None: return []
        sql = ("SELECT confidence_raw, direction, realized_pnl FROM trades "
               "WHERE realized_pnl IS NOT NULL AND confidence_raw IS NOT NULL "
               "AND skip_reason IS NULL AND direction IN ('YES','NO')")
        rows: list[tuple[float, str, int]] = []
        async with conn.execute(sql) as cur:
            async for r in cur:
                try: c, d, pnl = r["confidence_raw"], r["direction"], r["realized_pnl"]
                except (TypeError, IndexError, KeyError): c, d, pnl = r[0], r[1], r[2]
                rows.append((float(c), str(d), 1 if float(pnl) > 0 else 0))
        return rows

    def _fit_one(self, samples: list[tuple[float, int]], label: str) -> CalibrationCurve:
        rng = random.Random(self._RNG_SEED); shuf = list(samples); rng.shuffle(shuf)
        n = len(shuf); n_test = max(1, int(round(n * self._TEST_FRAC)))
        cs_tr = np.array([c for c, _ in shuf[n_test:]], dtype=np.float64)
        ys_tr = np.array([y for _, y in shuf[n_test:]], dtype=np.float64)
        cs_te = np.array([c for c, _ in shuf[:n_test]], dtype=np.float64)
        ys_te = np.array([y for _, y in shuf[:n_test]], dtype=np.float64)
        bb = _brier(cs_te, ys_te, lambda c: max(0.01, min(0.99, c)))
        ib = _pearson(cs_te, ys_te); ts = datetime.now(timezone.utc).isoformat()
        if self._method == "platt":
            a, b = _fit_platt(cs_tr, ys_tr)
            pred = lambda c, _a=a, _b=b: _platt_predict(c, _a, _b)  # noqa: E731
            ba = _brier(cs_te, ys_te, pred)
            ia = _pearson(np.array([pred(float(c)) for c in cs_te]), ys_te)
            return CalibrationCurve("platt", label, n, bb, ba, ib, ia, alpha=a, beta=b, fitted_at=ts)  # type: ignore[arg-type]
        knots = _fit_isotonic(cs_tr, ys_tr)
        pred = lambda c, _k=knots: _isotonic_predict(c, _k)  # noqa: E731
        ba = _brier(cs_te, ys_te, pred)
        ia = _pearson(np.array([pred(float(c)) for c in cs_te]), ys_te)
        return CalibrationCurve("isotonic", label, n, bb, ba, ib, ia, knots=knots, fitted_at=ts)  # type: ignore[arg-type]

    async def fit(self) -> dict[str, CalibrationCurve]:
        try: rows = await self._load_samples()
        except Exception as exc:  # noqa: BLE001
            logger.warning("[calibration] sample load failed: %s", exc)
            self._curves = {}; return {}
        yes = [(c, h) for c, d, h in rows if d == "YES"]
        no = [(c, h) for c, d, h in rows if d == "NO"]
        n_yes, n_no = len(yes), len(no)
        new: dict[str, CalibrationCurve] = {}
        try:
            if n_yes >= self.MIN_SAMPLES_PER_DIRECTION and n_no >= self.MIN_SAMPLES_PER_DIRECTION:
                new["YES"] = self._fit_one(yes, "YES"); new["NO"] = self._fit_one(no, "NO")
                logger.info("[calibration] per-direction fit: YES=%d NO=%d", n_yes, n_no)
            elif n_yes + n_no >= self.MIN_SAMPLES_POOLED:
                new["ALL"] = self._fit_one(yes + no, "ALL")
                logger.info("[calibration] pooled fit: ALL n=%d (YES=%d NO=%d)", n_yes + n_no, n_yes, n_no)
            else:
                logger.info("[calibration] identity: n=%d < %d", n_yes + n_no, self.MIN_SAMPLES_POOLED)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[calibration] fit failed: %s", exc); new = {}
        self._curves = new; return new

    def calibrate(self, c_raw: float, direction: str) -> float:
        try: c = float(c_raw)
        except (TypeError, ValueError): return 0.0
        if not self._curves: return c
        d = _norm_direction(direction)
        curve = (self._curves.get(d) if d else None) or self._curves.get("ALL")
        if curve is None: return c
        if curve.method == "platt" and curve.alpha is not None and curve.beta is not None:
            return _platt_predict(c, curve.alpha, curve.beta)
        if curve.method == "isotonic" and curve.knots is not None:
            return _isotonic_predict(c, curve.knots)
        return c

    async def refresh_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.REFRESH_TTL_SECONDS)
                await self.fit()
            except asyncio.CancelledError: break
            except Exception as exc:  # noqa: BLE001
                logger.warning("[calibration] refresh error: %s", exc)

    def diagnostics(self) -> dict:
        rows = [{"direction": c.direction, "method": c.method, "n": c.n_samples,
                 "ic_before": c.ic_before, "ic_after": c.ic_after,
                 "brier_before": c.brier_before, "brier_after": c.brier_after,
                 "fitted_at": c.fitted_at}
                for lbl in ("YES", "NO", "ALL") if (c := self._curves.get(lbl)) is not None]
        return {"method": self._method, "directions": rows}

_DEFAULT_CALIBRATOR: Optional[Calibrator] = None

def set_default_calibrator(c: Optional[Calibrator]) -> None:
    global _DEFAULT_CALIBRATOR
    _DEFAULT_CALIBRATOR = c

def get_default_calibrator() -> Optional[Calibrator]:
    return _DEFAULT_CALIBRATOR

def calibrate(c_raw: float, direction: Union[str, object, None] = None) -> float:
    """Module-level convenience: identity if no calibrator set."""
    if _DEFAULT_CALIBRATOR is None:
        try: return float(c_raw)
        except (TypeError, ValueError): return 0.0
    return _DEFAULT_CALIBRATOR.calibrate(c_raw, _norm_direction(direction) or "")
