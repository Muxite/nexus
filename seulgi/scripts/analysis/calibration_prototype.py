#!/usr/bin/env python3
"""calibration_prototype.py — A3.2 offline calibration sandbox.

Reads (confidence_raw, realized_pnl) pairs from closed trades and fits
two calibrators:

  * Platt scaling: 1 / (1 + exp(alpha · c + beta)) — hand-rolled stdlib
    logistic regression (BFGS via numpy gradient descent + line search).
  * Isotonic regression: pool-adjacent-violators (PAV), stdlib + numpy.

For each method we report:

  - fitted parameters (Platt: alpha/beta; isotonic: breakpoints)
  - bucketed table: c_raw bucket / n / hit_rate / c_platt / c_isotonic
  - Brier score before vs after calibration
  - per-direction split (YES vs NO); pooled fallback if either side < 25

This is **prototype only** — the production calibration module is
planned for Phase 4B.1 (`seulgi/app/calibration.py`). The output JSON
under `--out` is consumed by `scripts/replay/backtest.py --calibration
{platt,isotonic}` to verify rank-stability of variants under
calibration.

Usage:
    python3 -m scripts.analysis.calibration_prototype --since 2026-05-18
    python3 -m scripts.analysis.calibration_prototype --since 30d \\
        --out /tmp/a3-2/calibration.json
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Reuse the standard analysis helpers (open_db, parse_since).
_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
from _common import open_db, parse_since  # noqa: E402

import numpy as np  # numpy is required (used by db_health_audit etc.)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


@dataclass
class _Sample:
    confidence_raw: float
    realized_pnl: float
    direction: str  # "YES" or "NO"

    @property
    def hit(self) -> int:
        return 1 if self.realized_pnl > 0 else 0


def _load_samples(conn: sqlite3.Connection, since: Optional[str],
                  until: Optional[str]) -> list[_Sample]:
    sql = """
        SELECT confidence_raw, realized_pnl, direction
          FROM trades
         WHERE realized_pnl IS NOT NULL
           AND confidence_raw IS NOT NULL
           AND skip_reason IS NULL
           AND direction IN ('YES','NO')
    """
    params: list = []
    if since:
        sql += " AND decided_at >= ?"
        params.append(since)
    if until:
        sql += " AND decided_at < ?"
        params.append(until)
    sql += " ORDER BY decided_at ASC"
    return [
        _Sample(
            confidence_raw=float(r["confidence_raw"]),
            realized_pnl=float(r["realized_pnl"]),
            direction=r["direction"],
        )
        for r in conn.execute(sql, params).fetchall()
    ]


# ---------------------------------------------------------------------------
# Platt scaling (hand-rolled logistic regression)
# ---------------------------------------------------------------------------


@dataclass
class PlattFit:
    """Calibrated probability = sigmoid(alpha · c + beta).

    Standard Platt convention: lower alpha (or shifted beta) "shrinks"
    over-confident scores toward base rate. Stored unbounded; eval clips
    output to [0.01, 0.99] to avoid log(0)."""
    alpha: float
    beta: float
    n_samples: int
    n_hits: int
    converged: bool
    iters: int

    def predict(self, c: float) -> float:
        z = self.alpha * c + self.beta
        # Numerically safe sigmoid.
        if z >= 0:
            ez = math.exp(-z)
            p = 1.0 / (1.0 + ez)
        else:
            ez = math.exp(z)
            p = ez / (1.0 + ez)
        return max(0.01, min(0.99, p))


def _fit_platt(samples: list[_Sample], *,
               max_iter: int = 500, lr: float = 0.5,
               tol: float = 1e-7) -> PlattFit:
    """Hand-rolled batch gradient descent with Hessian step.

    Minimises negative log-likelihood:
       L = -sum y log p + (1-y) log (1-p), p = sigmoid(alpha c + beta)

    Per-step uses Newton-Raphson on (alpha, beta) — cheap and converges
    in ~10 iters on this tiny 500-row dataset. Falls back to pure GD
    when Hessian is singular.
    """
    if not samples:
        return PlattFit(alpha=0.0, beta=0.0, n_samples=0, n_hits=0,
                        converged=False, iters=0)
    c = np.array([s.confidence_raw for s in samples], dtype=np.float64)
    y = np.array([s.hit for s in samples], dtype=np.float64)
    # Platt's Bayesian smoothing of targets — avoids overconfident
    # extrapolation when one side of the curve has very few samples.
    n_pos = int(y.sum())
    n_neg = len(y) - n_pos
    t_pos = (n_pos + 1.0) / (n_pos + 2.0)
    t_neg = 1.0 / (n_neg + 2.0)
    t = np.where(y > 0.5, t_pos, t_neg)
    alpha, beta = 0.0, 0.0
    converged = False
    iters = 0
    for it in range(max_iter):
        iters = it + 1
        z = alpha * c + beta
        # Stable sigmoid.
        p = np.where(z >= 0,
                     1.0 / (1.0 + np.exp(-z)),
                     np.exp(z) / (1.0 + np.exp(z)))
        # Gradient of NLL w.r.t. (alpha, beta).
        diff = p - t
        g_alpha = float(np.dot(diff, c))
        g_beta = float(diff.sum())
        # Hessian (2x2).
        w = p * (1.0 - p)
        H_aa = float(np.dot(w, c * c))
        H_ab = float(np.dot(w, c))
        H_bb = float(w.sum())
        det = H_aa * H_bb - H_ab * H_ab
        if abs(det) < 1e-12:
            # Singular — fall back to scaled-GD step.
            d_alpha = lr * g_alpha
            d_beta = lr * g_beta
        else:
            # Newton step = H^-1 g
            inv00 = H_bb / det
            inv11 = H_aa / det
            inv01 = -H_ab / det
            d_alpha = inv00 * g_alpha + inv01 * g_beta
            d_beta = inv01 * g_alpha + inv11 * g_beta
        new_alpha = alpha - d_alpha
        new_beta = beta - d_beta
        if max(abs(d_alpha), abs(d_beta)) < tol:
            alpha, beta = new_alpha, new_beta
            converged = True
            break
        alpha, beta = new_alpha, new_beta
    return PlattFit(alpha=alpha, beta=beta, n_samples=len(samples),
                    n_hits=n_pos, converged=converged, iters=iters)


# ---------------------------------------------------------------------------
# Isotonic regression (Pool Adjacent Violators)
# ---------------------------------------------------------------------------


@dataclass
class IsotonicFit:
    """Piecewise-constant monotone non-decreasing calibrator.

    Stored as parallel arrays of breakpoints `x_knots` (sorted unique
    inputs) and `y_knots` (calibrated probability at that input). Eval
    is linear-interpolation between adjacent knots, clipped to [0.01,
    0.99] for downstream safety (avoid log(0))."""
    x_knots: list[float]
    y_knots: list[float]
    n_samples: int
    n_hits: int

    def predict(self, c: float) -> float:
        if not self.x_knots:
            return max(0.01, min(0.99, c))
        if c <= self.x_knots[0]:
            return max(0.01, min(0.99, self.y_knots[0]))
        if c >= self.x_knots[-1]:
            return max(0.01, min(0.99, self.y_knots[-1]))
        # Binary search.
        lo, hi = 0, len(self.x_knots) - 1
        while hi - lo > 1:
            mid = (lo + hi) // 2
            if self.x_knots[mid] <= c:
                lo = mid
            else:
                hi = mid
        x0, x1 = self.x_knots[lo], self.x_knots[hi]
        y0, y1 = self.y_knots[lo], self.y_knots[hi]
        if x1 == x0:
            v = y1
        else:
            v = y0 + (y1 - y0) * (c - x0) / (x1 - x0)
        return max(0.01, min(0.99, v))


def _fit_isotonic(samples: list[_Sample]) -> IsotonicFit:
    """Pool Adjacent Violators algorithm — O(n) after sort."""
    if not samples:
        return IsotonicFit(x_knots=[], y_knots=[], n_samples=0, n_hits=0)
    # Sort by input.
    sorted_s = sorted(samples, key=lambda s: s.confidence_raw)
    x = [s.confidence_raw for s in sorted_s]
    y = [float(s.hit) for s in sorted_s]
    # Aggregate ties (same x) into one weighted node — keeps PAV stable.
    merged_x: list[float] = []
    merged_y: list[float] = []
    merged_w: list[float] = []
    i = 0
    while i < len(x):
        j = i
        s_y = 0.0
        s_w = 0.0
        while j < len(x) and x[j] == x[i]:
            s_y += y[j]
            s_w += 1.0
            j += 1
        merged_x.append(x[i])
        merged_y.append(s_y / s_w)
        merged_w.append(s_w)
        i = j
    # PAV: walk left-to-right; whenever previous mean > current, pool.
    stack_y: list[float] = []
    stack_w: list[float] = []
    stack_x_lo: list[float] = []
    stack_x_hi: list[float] = []
    for xi, yi, wi in zip(merged_x, merged_y, merged_w):
        cur_y = yi
        cur_w = wi
        cur_lo = xi
        cur_hi = xi
        while stack_y and stack_y[-1] > cur_y:
            prev_y = stack_y.pop()
            prev_w = stack_w.pop()
            prev_lo = stack_x_lo.pop()
            prev_hi = stack_x_hi.pop()
            cur_y = (prev_y * prev_w + cur_y * cur_w) / (prev_w + cur_w)
            cur_w = prev_w + cur_w
            cur_lo = prev_lo
            # prev_hi < cur_hi already
        stack_y.append(cur_y)
        stack_w.append(cur_w)
        stack_x_lo.append(cur_lo)
        stack_x_hi.append(cur_hi)
    # Expand to per-knot output: emit (lo, y) and (hi, y) so adjacent
    # plateaus are honoured exactly.
    x_knots: list[float] = []
    y_knots: list[float] = []
    for lo, hi, yv in zip(stack_x_lo, stack_x_hi, stack_y):
        if x_knots and lo == x_knots[-1]:
            # Avoid duplicate x — extend with hi if different.
            if hi != lo:
                x_knots.append(hi)
                y_knots.append(yv)
        else:
            x_knots.append(lo)
            y_knots.append(yv)
            if hi != lo:
                x_knots.append(hi)
                y_knots.append(yv)
    n_hits = sum(s.hit for s in samples)
    return IsotonicFit(x_knots=x_knots, y_knots=y_knots,
                       n_samples=len(samples), n_hits=n_hits)


# ---------------------------------------------------------------------------
# Brier score
# ---------------------------------------------------------------------------


def _brier(samples: list[_Sample], predict) -> float:
    if not samples:
        return float("nan")
    s = 0.0
    for sm in samples:
        p = predict(sm.confidence_raw)
        s += (p - sm.hit) ** 2
    return s / len(samples)


def _brier_identity(samples: list[_Sample]) -> float:
    return _brier(samples, lambda c: max(0.01, min(0.99, c)))


# ---------------------------------------------------------------------------
# Buckets / table
# ---------------------------------------------------------------------------


def _bucket_table(samples: list[_Sample], platt: PlattFit,
                  isot: IsotonicFit, *, bucket_width: float = 0.1
                  ) -> list[dict]:
    """Aggregate samples into [0.0,0.1), [0.1,0.2), ... [0.9, 1.0] buckets."""
    n_buckets = int(round(1.0 / bucket_width))
    buckets: list[dict] = []
    for b in range(n_buckets):
        lo = b * bucket_width
        hi = lo + bucket_width
        if b == n_buckets - 1:
            in_b = [s for s in samples if lo <= s.confidence_raw <= hi]
        else:
            in_b = [s for s in samples if lo <= s.confidence_raw < hi]
        n = len(in_b)
        if n == 0:
            continue
        hits = sum(s.hit for s in in_b)
        mid_c = (lo + hi) / 2.0
        buckets.append({
            "bucket": f"[{lo:.1f},{hi:.1f})" if b < n_buckets - 1
                      else f"[{lo:.1f},{hi:.1f}]",
            "lo": round(lo, 4),
            "hi": round(hi, 4),
            "n": n,
            "hit_rate": round(hits / n, 4),
            "c_platt_at_mid": round(platt.predict(mid_c), 4),
            "c_isotonic_at_mid": round(isot.predict(mid_c), 4),
            "avg_c_raw": round(sum(s.confidence_raw for s in in_b) / n, 4),
            "avg_c_platt": round(
                sum(platt.predict(s.confidence_raw) for s in in_b) / n, 4),
            "avg_c_isotonic": round(
                sum(isot.predict(s.confidence_raw) for s in in_b) / n, 4),
        })
    return buckets


# ---------------------------------------------------------------------------
# Per-direction orchestrator
# ---------------------------------------------------------------------------


@dataclass
class DirectionFit:
    direction: str
    n: int
    n_hits: int
    platt: dict
    isotonic: dict
    brier_raw: float
    brier_platt: float
    brier_isotonic: float
    buckets: list[dict] = field(default_factory=list)


def _fit_direction(samples: list[_Sample], dir_label: str) -> DirectionFit:
    pf = _fit_platt(samples)
    isf = _fit_isotonic(samples)
    table = _bucket_table(samples, pf, isf)
    return DirectionFit(
        direction=dir_label,
        n=len(samples),
        n_hits=sum(s.hit for s in samples),
        platt={"alpha": round(pf.alpha, 6), "beta": round(pf.beta, 6),
               "converged": pf.converged, "iters": pf.iters},
        isotonic={"x_knots": [round(x, 6) for x in isf.x_knots],
                  "y_knots": [round(y, 6) for y in isf.y_knots]},
        brier_raw=round(_brier_identity(samples), 6),
        brier_platt=round(_brier(samples, pf.predict), 6),
        brier_isotonic=round(_brier(samples, isf.predict), 6),
        buckets=table,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--db", default=None,
                   help="Path to seulgi.db (default: data/seulgi.db).")
    p.add_argument("--since", default="30d",
                   help="ISO date or duration (e.g. 30d, 7d). Default last 30d.")
    p.add_argument("--until", default=None,
                   help="ISO date; default = now. Trades with decided_at < this.")
    p.add_argument("--min-per-direction", type=int, default=25,
                   help="Below this, pool YES+NO into a single fit.")
    p.add_argument("--out", default=None,
                   help="Optional path to write JSON output (consumed by backtest).")
    p.add_argument("--format", choices=("text", "json"), default="text")
    return p.parse_args(argv)


def _format_text(report: dict) -> str:
    L: list[str] = []
    L.append(f"# Calibration prototype (A3.2)")
    L.append(f"window: {report['window']['since']} → "
             f"{report['window']['until'] or 'now'}")
    L.append(f"samples: {report['total_n']} "
             f"(YES={report['n_yes']}, NO={report['n_no']})")
    L.append(f"strategy: {report['strategy']}")
    L.append("")
    for d in report["fits"]:
        L.append(f"## direction = {d['direction']}  (n={d['n']}, "
                 f"hits={d['n_hits']}, hit_rate={d['n_hits']/d['n']:.3f})")
        L.append(f"  Platt:    alpha={d['platt']['alpha']:+.4f}  "
                 f"beta={d['platt']['beta']:+.4f}  "
                 f"converged={d['platt']['converged']} "
                 f"({d['platt']['iters']} iters)")
        L.append(f"  Isotonic: {len(d['isotonic']['x_knots'])} knots: "
                 f"{list(zip(d['isotonic']['x_knots'], d['isotonic']['y_knots']))}")
        L.append(f"  Brier:    raw={d['brier_raw']:.4f}  "
                 f"platt={d['brier_platt']:.4f}  "
                 f"isotonic={d['brier_isotonic']:.4f}  "
                 f"(Δplatt={d['brier_platt']-d['brier_raw']:+.4f}, "
                 f"Δisotonic={d['brier_isotonic']-d['brier_raw']:+.4f})")
        L.append("")
        L.append("  | bucket       |   n | hit_rate | avg_c_raw | "
                 "avg_c_platt | avg_c_isotonic |")
        L.append("  |--------------|----:|---------:|----------:|"
                 "------------:|---------------:|")
        for b in d["buckets"]:
            L.append(f"  | {b['bucket']:<12} | {b['n']:>3} | "
                     f"{b['hit_rate']:>8.3f} | {b['avg_c_raw']:>9.3f} | "
                     f"{b['avg_c_platt']:>11.3f} | "
                     f"{b['avg_c_isotonic']:>14.3f} |")
        L.append("")
    return "\n".join(L)


def main(argv: Optional[list] = None) -> int:
    args = _parse_args(argv)
    db_path = Path(args.db) if args.db else None
    conn = open_db(db_path)
    since_iso = parse_since(args.since) if args.since else None
    until_iso = parse_since(args.until) if args.until else None
    samples = _load_samples(conn, since_iso, until_iso)
    yes_samples = [s for s in samples if s.direction == "YES"]
    no_samples = [s for s in samples if s.direction == "NO"]

    fits: list[DirectionFit] = []
    if (len(yes_samples) >= args.min_per_direction
            and len(no_samples) >= args.min_per_direction):
        strategy = "per_direction"
        fits.append(_fit_direction(yes_samples, "YES"))
        fits.append(_fit_direction(no_samples, "NO"))
    else:
        strategy = ("pooled (one side < {} samples)"
                    .format(args.min_per_direction))
        fits.append(_fit_direction(samples, "ALL"))

    report = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "window": {"since": since_iso, "until": until_iso},
        "total_n": len(samples),
        "n_yes": len(yes_samples),
        "n_no": len(no_samples),
        "min_per_direction": args.min_per_direction,
        "strategy": strategy,
        "fits": [asdict(f) for f in fits],
    }

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(report, indent=2, default=str))
        print(f"wrote {out_path}", file=sys.stderr)

    if args.format == "json":
        print(json.dumps(report, indent=2, default=str))
    else:
        print(_format_text(report))
    return 0


# ---------------------------------------------------------------------------
# Loader for downstream consumers (backtest.py).
# ---------------------------------------------------------------------------


def load_calibrator(path: str, method: str):
    """Return a callable `f(confidence_raw, direction) -> calibrated` from
    a JSON report previously written by `--out`. `method` ∈
    {"platt","isotonic","none"}. `direction` may be "YES"/"NO"/"ALL"."""
    if method == "none":
        return lambda c, _direction=None: c
    with open(path) as f:
        report = json.load(f)
    fits_by_dir: dict[str, dict] = {f["direction"]: f for f in report["fits"]}

    def _calibrate(c: float, direction: Optional[str] = None) -> float:
        # Per-direction fit if available, else fall back to ALL.
        f = fits_by_dir.get(direction or "")
        if f is None:
            f = fits_by_dir.get("ALL")
            if f is None:
                # Use first available fit (shouldn't happen).
                f = next(iter(fits_by_dir.values()))
        if method == "platt":
            a = f["platt"]["alpha"]
            b = f["platt"]["beta"]
            z = a * c + b
            if z >= 0:
                p = 1.0 / (1.0 + math.exp(-z))
            else:
                p = math.exp(z) / (1.0 + math.exp(z))
            return max(0.01, min(0.99, p))
        elif method == "isotonic":
            xs = f["isotonic"]["x_knots"]
            ys = f["isotonic"]["y_knots"]
            if not xs:
                return max(0.01, min(0.99, c))
            if c <= xs[0]:
                return max(0.01, min(0.99, ys[0]))
            if c >= xs[-1]:
                return max(0.01, min(0.99, ys[-1]))
            lo, hi = 0, len(xs) - 1
            while hi - lo > 1:
                mid = (lo + hi) // 2
                if xs[mid] <= c:
                    lo = mid
                else:
                    hi = mid
            x0, x1 = xs[lo], xs[hi]
            y0, y1 = ys[lo], ys[hi]
            if x1 == x0:
                v = y1
            else:
                v = y0 + (y1 - y0) * (c - x0) / (x1 - x0)
            return max(0.01, min(0.99, v))
        raise ValueError(f"unknown method: {method}")

    return _calibrate


if __name__ == "__main__":
    raise SystemExit(main())
