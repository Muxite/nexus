#!/usr/bin/env python3
"""composite_scoring_report — Phase 4E.1 readout.

Fits the live composite-scoring model (`app/scoring.py`) on closed trades
in the seulgi DB and emits a markdown report covering:

  - per-direction + pooled AUC + IC + n_samples
  - top-5 features by absolute coefficient
  - full ranked coefficient table
  - calibration curve: decile of P(profitable_trade) → realized hit rate

Read-only. Run offline; the live process is not consulted.

Usage:
    python3 -m scripts.analysis.composite_scoring_report
    python3 -m scripts.analysis.composite_scoring_report --out /tmp/report.md
    SEULGI_DB=/path/to/seulgi.db python3 -m \
        scripts.analysis.composite_scoring_report
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "app"))

from db import SeulgiDB  # noqa: E402
from scoring import CompositeScorer, FEATURE_NAMES, features_to_vector  # noqa: E402

import numpy as np  # noqa: E402


def _default_db_path() -> Path:
    env = os.getenv("SEULGI_DB")
    if env:
        return Path(env)
    live = REPO / "data" / "seulgi.db"
    if live.exists():
        return live
    return live  # fall through; SeulgiDB will raise a friendly error


async def _run(db_path: Path, out: Path | None) -> str:
    db = SeulgiDB(str(db_path))
    await db.connect()
    lines: list[str] = []
    try:
        scorer = CompositeScorer(db, refresh_hours=6)
        rows = await scorer._load_samples()
        model = await scorer.fit()
        lines.append("# Composite Scoring Report — Phase 4E.1")
        lines.append("")
        lines.append(f"  db        : `{db_path}`")
        lines.append(f"  n_closed  : **{len(rows)}**")
        lines.append("")
        if model is None:
            lines.append("> No model fitted — closed-trade sample too small.")
            text = "\n".join(lines)
            if out is not None:
                out.write_text(text)
            return text

        # ---- Per-model summary --------------------------------------
        lines.append("## Model summary")
        lines.append("")
        lines.append("| direction |   n | train_AUC | test_AUC |       IC | fitted_at           |")
        lines.append("|-----------|----:|----------:|---------:|---------:|---------------------|")
        for label in ("YES", "NO", "ALL"):
            m = scorer.models.get(label)
            if m is None:
                continue
            ts = (m.fit_at or "")[:19]
            lines.append(
                f"| {m.direction:<9} | {m.n_samples:>3} | "
                f"{m.train_auc:>9.3f} | {m.test_auc:>8.3f} | "
                f"{m.ic:+8.3f} | {ts:<19} |"
            )
        lines.append("")

        # ---- Coefficient ranking (pooled ALL model) -----------------
        lines.append("## Coefficients (pooled ALL, standardized features)")
        lines.append("")
        weights = list(model.weights)
        ranked = sorted(zip(FEATURE_NAMES, weights), key=lambda kv: -abs(kv[1]))
        lines.append("Top-5 by |weight|:")
        lines.append("")
        for i, (name, w) in enumerate(ranked[:5], 1):
            lines.append(f"  {i}. `{name}` : {w:+.4f}")
        lines.append("")
        lines.append("Full ranking:")
        lines.append("")
        lines.append("| rank | feature                 | coef     |")
        lines.append("|-----:|-------------------------|---------:|")
        for i, (name, w) in enumerate(ranked, 1):
            lines.append(f"| {i:>4} | `{name:<23}` | {w:+8.4f} |")
        lines.append("")
        lines.append(f"  intercept : {model.intercept:+.4f}")
        lines.append("")

        # ---- Calibration curve (decile binning) ---------------------
        lines.append("## Calibration curve — decile of score → realized hit rate")
        lines.append("")
        if rows:
            X = np.stack([features_to_vector(f) for f, _, _ in rows]).astype(np.float64)
            ys = np.array([1.0 if pnl > 0 else 0.0 for _, _, pnl in rows])
            mu = np.array(model.feature_means, dtype=np.float64)
            sd = np.array(model.feature_stds, dtype=np.float64)
            Xn = (X - mu) / sd
            z = Xn @ np.array(model.weights, dtype=np.float64) + model.intercept
            p = 1.0 / (1.0 + np.exp(-z))
            order = np.argsort(p)
            p_sorted = p[order]
            y_sorted = ys[order]
            n = len(p_sorted)
            n_bins = 10
            lines.append("| decile | score_lo | score_hi |   n |  hit_rate |")
            lines.append("|-------:|---------:|---------:|----:|----------:|")
            for k in range(n_bins):
                lo, hi = k * n // n_bins, (k + 1) * n // n_bins
                if hi <= lo:
                    continue
                s = p_sorted[lo:hi]; y = y_sorted[lo:hi]
                lines.append(
                    f"| {k+1:>6} | {float(s.min()):>8.3f} | "
                    f"{float(s.max()):>8.3f} | {len(s):>3} | "
                    f"{float(y.mean()):>9.3f} |"
                )
            lines.append("")

        # ---- Sanity diagnostics -------------------------------------
        diag = scorer.diagnostics()
        lines.append("## Raw diagnostics() output")
        lines.append("")
        lines.append("```json")
        import json
        lines.append(json.dumps(diag, indent=2, default=str))
        lines.append("```")
        lines.append("")

        text = "\n".join(lines)
        if out is not None:
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(text)
        return text
    finally:
        await db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=_default_db_path(),
                        help="SQLite path (default: data/seulgi.db or $SEULGI_DB)")
    parser.add_argument("--out", type=Path, default=None,
                        help="Write report to this markdown file (default: stdout)")
    args = parser.parse_args()
    text = asyncio.run(_run(args.db, args.out))
    if args.out is None:
        print(text)
    else:
        print(f"[ok] report written: {args.out}")


if __name__ == "__main__":
    main()
