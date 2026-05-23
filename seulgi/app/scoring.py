"""Composite signal scoring — Phase 4E.1.

## Specification
    scorer = CompositeScorer(db, refresh_hours=6)
    await scorer.fit()                     # None on small sample
    set_default_scorer(scorer); asyncio.create_task(scorer.refresh_loop())
    p = scorer.score(build_features(...))  # 0.5 identity if not fit

Logistic regression on (features, hit:=pnl>0) over closed trades. Replaces
hand-tuned skip rules with one learned P(profitable_trade) scalar. Trader
gate fires only when `cfg.composite_score_floor > 0`; default 0.0 = no-op.

Needs >= MIN_SAMPLES (=100). Per-direction YES/NO fit when each side >= 60;
pooled ALL always attempted. 80/20 split. Train + test AUC + IC (Pearson
r(score, pnl)) reported. Stdlib + numpy only; batch GD + L2.
"""
from __future__ import annotations
import asyncio, logging, math, random
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
import numpy as np

logger = logging.getLogger(__name__)

FEATURE_NAMES: list[str] = [
    "similarity", "confidence_raw", "p_alpha",
    "freshness_dist", "freshness_age_secs", "headline_age_secs",
    "mid_yes", "market_vol_30m",
    "hour_utc", "agreement_fraction",
    "spread_bps", "book_depth_usdc",
    "tag_interest_rates", "tag_geopolitics", "tag_elections",
    "tag_sports", "tag_weather", "tag_tech", "tag_crypto", "tag_earnings",
]
N_FEATURES = len(FEATURE_NAMES)
_TAG_FEATURE_NAMES = ["interest_rates", "geopolitics", "elections", "sports",
                     "weather", "tech", "crypto", "earnings"]


@dataclass
class ScoringModel:
    """Fitted logistic regression — single direction or pooled."""
    weights: list[float]
    intercept: float
    fit_at: str
    n_samples: int
    train_auc: float
    test_auc: float
    ic: float
    direction: str = "ALL"
    feature_means: list[float] = field(default_factory=list)
    feature_stds: list[float] = field(default_factory=list)


def build_features(match, classify_result, ctx) -> dict:
    """Pull the 20-feature vector out of the live trader objects. Best-effort:
    any missing attr maps to a sensible zero/default."""
    def _f(obj, attr: str, default=0.0):
        try:
            v = getattr(obj, attr, default)
            return float(default) if v is None else float(v)
        except (TypeError, ValueError):
            return float(default)
    mkt_ctx = getattr(match, "_mkt_ctx", None)
    tag = getattr(match, "tag", None) or ""
    tags_list = list(getattr(match, "_tags", None) or [])
    if tag and tag not in tags_list: tags_list.append(tag)
    feats: dict[str, float] = {
        "similarity": _f(match, "similarity"),
        "confidence_raw": _f(classify_result, "confidence"),
        "p_alpha": _f(classify_result, "p_alpha"),
        "freshness_dist": _f(ctx, "freshness_dist"),
        "freshness_age_secs": _f(ctx, "freshness_age_secs"),
        "headline_age_secs": _f(ctx, "headline_age_secs"),
        "mid_yes": _f(match, "mid_yes", default=0.5),
        "market_vol_30m": _f(mkt_ctx, "vol") if mkt_ctx is not None else 0.0,
        "hour_utc": float(datetime.now(timezone.utc).hour),
        "agreement_fraction": _f(classify_result, "agreement"),
        "spread_bps": _f(match, "spread_bps"),
        "book_depth_usdc": _f(match, "book_depth_usdc"),
    }
    for t in _TAG_FEATURE_NAMES:
        feats[f"tag_{t}"] = 1.0 if t in tags_list else 0.0
    return feats


def features_to_vector(feats: dict) -> np.ndarray:
    """Flatten a feature dict to the canonical FEATURE_NAMES order."""
    return np.array([float(feats.get(n, 0.0) or 0.0) for n in FEATURE_NAMES],
                    dtype=np.float64)


def _sigmoid(z: np.ndarray) -> np.ndarray:
    """Numerically stable elementwise sigmoid."""
    out = np.empty_like(z, dtype=np.float64); pos = z >= 0
    out[pos] = 1.0 / (1.0 + np.exp(-z[pos]))
    ez = np.exp(z[~pos]); out[~pos] = ez / (1.0 + ez)
    return out


def _fit_logreg(X: np.ndarray, y: np.ndarray, *, lr: float = 0.1,
                l2: float = 0.01, n_iter: int = 600) -> tuple[np.ndarray, float]:
    """Vanilla batch GD on logistic NLL with L2. Returns (weights, intercept)."""
    n, d = X.shape
    w = np.zeros(d, dtype=np.float64); b = 0.0
    for _ in range(n_iter):
        diff = _sigmoid(X @ w + b) - y
        w -= lr * ((X.T @ diff) / n + l2 * w)
        b -= lr * float(diff.mean())
    return w, b


def _auc(scores: np.ndarray, y: np.ndarray) -> float:
    """ROC-AUC via Mann-Whitney U. 0.5 when one class is empty."""
    pos = scores[y > 0.5]; neg = scores[y <= 0.5]
    if len(pos) == 0 or len(neg) == 0: return 0.5
    s = np.concatenate([pos, neg])
    order = np.argsort(s, kind="mergesort")
    ranks = np.empty_like(s); ranks[order] = np.arange(1, len(s) + 1)
    n_p, n_n = len(pos), len(neg)
    u = ranks[:n_p].sum() - n_p * (n_p + 1) / 2
    return float(u / (n_p * n_n))


def _pearson(a: np.ndarray, b: np.ndarray) -> float:
    if len(a) < 2 or a.std() == 0 or b.std() == 0: return 0.0
    return float(np.corrcoef(a, b)[0, 1])


class CompositeScorer:
    """Logistic regression on (features, hit:=pnl>0) over closed trades.

    Refreshes every `refresh_hours`. Identity fallback (score=0.5) until
    enough samples accumulate."""

    MIN_SAMPLES = 100
    MIN_PER_DIRECTION = 60
    _TEST_FRAC = 0.2
    _RNG_SEED = 4711

    def __init__(self, db, refresh_hours: int = 6):
        self._db = db
        self._refresh_secs = max(60, int(refresh_hours * 3600))
        self._models: dict[str, ScoringModel] = {}

    @property
    def models(self) -> dict[str, ScoringModel]:
        return dict(self._models)

    async def _load_samples(self) -> list[tuple[dict, str, float]]:
        """Return [(features_dict, direction, realized_pnl), ...] from closed trades."""
        conn = getattr(self._db, "_conn", None)
        if conn is None:
            return []
        sql = ("SELECT t.similarity, t.confidence_raw, t.p_alpha, t.agreement, "
               "t.mid_yes, t.mkt_vol_30m, t.direction, t.realized_pnl, "
               "t.decided_at, t.freshness_label, s.nearest_age_secs, s.headline "
               "FROM trades t LEFT JOIN signals s ON s.id = t.signal_id "
               "WHERE t.realized_pnl IS NOT NULL AND t.skip_reason IS NULL "
               "AND t.direction IN ('YES','NO')")
        try: from tagger import tag_headline
        except Exception: tag_headline = None  # noqa: BLE001
        _FD = {"FRESH": 0.0, "EVOLVING": 0.5, "STALE": 1.0}
        out: list[tuple[dict, str, float]] = []
        async with conn.execute(sql) as cur:
            async for r in cur:
                d = dict(r) if hasattr(r, "keys") else {
                    "similarity": r[0], "confidence_raw": r[1], "p_alpha": r[2],
                    "agreement": r[3], "mid_yes": r[4], "mkt_vol_30m": r[5],
                    "direction": r[6], "realized_pnl": r[7], "decided_at": r[8],
                    "freshness_label": r[9], "nearest_age_secs": r[10], "headline": r[11]}
                try: hour = datetime.fromisoformat(d["decided_at"]).hour if d.get("decided_at") else 0
                except Exception: hour = 0  # noqa: BLE001
                tags: list[str] = []
                if tag_headline is not None and d.get("headline"):
                    try: tags = tag_headline(str(d["headline"])).tags
                    except Exception: tags = []  # noqa: BLE001
                feats: dict[str, float] = {
                    "similarity": float(d.get("similarity") or 0.0),
                    "confidence_raw": float(d.get("confidence_raw") or 0.0),
                    "p_alpha": float(d.get("p_alpha") or 0.0),
                    "freshness_dist": _FD.get(str(d.get("freshness_label") or "").upper(), 0.5),
                    "freshness_age_secs": float(d.get("nearest_age_secs") or 0.0),
                    "headline_age_secs": float(d.get("nearest_age_secs") or 0.0),
                    "mid_yes": float(d["mid_yes"] if d.get("mid_yes") is not None else 0.5),
                    "market_vol_30m": float(d.get("mkt_vol_30m") or 0.0),
                    "hour_utc": float(hour),
                    "agreement_fraction": float(d.get("agreement") or 0.0),
                    "spread_bps": 0.0, "book_depth_usdc": 0.0,
                }
                for t in _TAG_FEATURE_NAMES:
                    feats[f"tag_{t}"] = 1.0 if t in tags else 0.0
                out.append((feats, str(d["direction"]), float(d["realized_pnl"])))
        return out

    def _fit_one(self, samples: list[tuple[dict, float]], label: str) -> Optional[ScoringModel]:
        if len(samples) < 20: return None
        rng = random.Random(self._RNG_SEED + (0 if label == "ALL" else hash(label) & 0xFF))
        shuf = list(samples); rng.shuffle(shuf)
        Xs = np.stack([features_to_vector(f) for f, _ in shuf]).astype(np.float64)
        ys = np.array([1.0 if pnl > 0 else 0.0 for _, pnl in shuf], dtype=np.float64)
        pnls = np.array([pnl for _, pnl in shuf], dtype=np.float64)
        n = len(shuf); n_test = max(1, int(round(n * self._TEST_FRAC)))
        X_te, X_tr = Xs[:n_test], Xs[n_test:]
        y_te, y_tr = ys[:n_test], ys[n_test:]; pnl_te = pnls[:n_test]
        mu = X_tr.mean(axis=0); sd = np.where(X_tr.std(axis=0) < 1e-9, 1.0, X_tr.std(axis=0))
        Xn_tr = (X_tr - mu) / sd; Xn_te = (X_te - mu) / sd
        w, b = _fit_logreg(Xn_tr, y_tr)
        train_auc = _auc(_sigmoid(Xn_tr @ w + b), y_tr)
        test_scores = _sigmoid(Xn_te @ w + b)
        test_auc = _auc(test_scores, y_te)
        ic = _pearson(test_scores, pnl_te)
        return ScoringModel(
            weights=w.tolist(), intercept=float(b),
            fit_at=datetime.now(timezone.utc).isoformat(),
            n_samples=n, train_auc=train_auc, test_auc=test_auc, ic=ic,
            direction=label, feature_means=mu.tolist(), feature_stds=sd.tolist(),
        )

    async def fit(self) -> Optional[ScoringModel]:
        """Fit per-direction + pooled models. Returns the pooled ALL model
        (or None if the closed-trade sample is too small)."""
        try: rows = await self._load_samples()
        except Exception as exc:  # noqa: BLE001
            logger.warning("[scoring] sample load failed: %s", exc)
            self._models = {}; return None
        if len(rows) < self.MIN_SAMPLES:
            logger.info("[scoring] identity: n=%d < %d", len(rows), self.MIN_SAMPLES)
            self._models = {}; return None
        yes = [(f, p) for f, d, p in rows if d == "YES"]
        no = [(f, p) for f, d, p in rows if d == "NO"]
        new: dict[str, ScoringModel] = {}
        try:
            if len(yes) >= self.MIN_PER_DIRECTION and len(no) >= self.MIN_PER_DIRECTION:
                my, mn = self._fit_one(yes, "YES"), self._fit_one(no, "NO")
                if my: new["YES"] = my
                if mn: new["NO"] = mn
                logger.info("[scoring] per-direction fit: YES=%d NO=%d", len(yes), len(no))
            pooled = self._fit_one([(f, p) for f, _, p in rows], "ALL")
            if pooled is not None:
                new["ALL"] = pooled
                logger.info("[scoring] pooled n=%d train_auc=%.3f test_auc=%.3f ic=%+.3f",
                            len(rows), pooled.train_auc, pooled.test_auc, pooled.ic)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[scoring] fit failed: %s", exc)
            self._models = {}; return None
        self._models = new
        return new.get("ALL")

    def score(self, features: dict, direction: Optional[str] = None) -> float:
        """Apply the cached model. 0.5 identity fallback when no model fit."""
        if not self._models: return 0.5
        model = self._models.get(direction) if direction in ("YES", "NO") else None
        if model is None:
            model = self._models.get("ALL") or next(iter(self._models.values()), None)
        if model is None: return 0.5
        x = features_to_vector(features)
        if model.feature_means and model.feature_stds:
            mu = np.array(model.feature_means, dtype=np.float64)
            sd = np.array(model.feature_stds, dtype=np.float64)
            if len(mu) == len(x): x = (x - mu) / sd
        z = float(np.dot(np.array(model.weights, dtype=np.float64), x) + model.intercept)
        if z >= 0: p = 1.0 / (1.0 + math.exp(-z))
        else: ez = math.exp(z); p = ez / (1.0 + ez)
        return max(0.0, min(1.0, p))

    async def refresh_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._refresh_secs); await self.fit()
            except asyncio.CancelledError: break
            except Exception as exc:  # noqa: BLE001
                logger.warning("[scoring] refresh error: %s", exc)

    def diagnostics(self) -> dict:
        """For the dashboard tile."""
        rows = [{"direction": m.direction, "n": m.n_samples,
                 "train_auc": m.train_auc, "test_auc": m.test_auc,
                 "ic": m.ic, "fitted_at": m.fit_at}
                for lbl in ("YES", "NO", "ALL")
                if (m := self._models.get(lbl)) is not None]
        return {"models": rows, "n_features": N_FEATURES}


_DEFAULT_SCORER: Optional[CompositeScorer] = None


def set_default_scorer(s: Optional[CompositeScorer]) -> None:
    global _DEFAULT_SCORER
    _DEFAULT_SCORER = s


def get_default_scorer() -> Optional[CompositeScorer]:
    return _DEFAULT_SCORER


def score_features(features: dict, direction: Optional[str] = None) -> float:
    """Module-level convenience: 0.5 if no scorer set."""
    if _DEFAULT_SCORER is None: return 0.5
    return _DEFAULT_SCORER.score(features, direction=direction)
