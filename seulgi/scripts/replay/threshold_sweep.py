#!/usr/bin/env python3
"""threshold_sweep.py — A4B.4 driver that sweeps the
(use_true_ev_formula, calibration_method, min_ev_per_dollar, min_confidence)
grid by invoking ``scripts.replay.backtest`` and summarising
n_trades / sum_pnl / per-engine PnL into a single JSON.

Each grid point writes its own ``runs/<run_id>/`` sub-tree under the
chosen ``--out-dir``; the aggregated index lives at
``<out_dir>/sweep_index.json``.

Usage (typical):

    python3 -m scripts.replay.threshold_sweep \\
        --since 2026-05-18 --until 2026-05-23 \\
        --base-config config.yaml \\
        --calibration-curve /tmp/a4b4/calibration.json \\
        --out-dir /tmp/a4b4-sweep \\
        --grid coarse

Only the global ``trading.use_true_ev_formula``, ``trading.min_ev_per_dollar``
and ``ai.min_confidence`` knobs are varied — per-engine overrides in the
base config are preserved.  Engine-level ``min_confidence`` overrides are
*not* perturbed because A3.2 confirmed each engine's gate is hand-tuned;
the sweep targets the global floor that scout/super_scout inherit.
"""
from __future__ import annotations

import argparse
import itertools
import json
import logging
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger("threshold_sweep")

_HERE = os.path.dirname(os.path.abspath(__file__))
_SEULGI = os.path.normpath(os.path.join(_HERE, "..", ".."))


def _write_config(base_yaml: str, out_path: str, *,
                  use_true_ev: bool, min_ev: float, min_conf: float) -> None:
    """Re-emit a config.yaml with the three knobs patched at the
    top-level ``trading:`` + ``ai:`` blocks.

    We do *line-oriented* edits rather than full YAML parsing because
    ``config.yaml`` carries dozens of pages of important comments we do
    not want to round-trip through a YAML library.
    """
    with open(base_yaml) as f:
        txt = f.read()
    # Top-level trading: min_edge: 0.04 anchor — only insert once.
    anchor = "  min_edge: 0.04              # minimum edge (p_hat − mid) required to execute"
    if anchor in txt:
        injected = (
            anchor + "\n"
            f"  use_true_ev_formula: {'true' if use_true_ev else 'false'}\n"
            f"  min_ev_per_dollar: {min_ev:.4f}"
        )
        txt = txt.replace(anchor, injected, 1)
    else:
        # Fallback — append a fresh block (unlikely path; warns).
        logger.warning("[sweep] min_edge anchor not found; appending knobs")
        txt += (
            f"\n# A4B.4 sweep injection\n"
            f"trading:\n"
            f"  use_true_ev_formula: {'true' if use_true_ev else 'false'}\n"
            f"  min_ev_per_dollar: {min_ev:.4f}\n"
        )
    # ai.min_confidence — replace existing value.
    import re
    txt = re.sub(
        r"^(\s*min_confidence:\s*)0\.35(\s*#.*below this.*)",
        rf"\g<1>{min_conf:.4f}\g<2>",
        txt, count=1, flags=re.MULTILINE,
    )
    with open(out_path, "w") as f:
        f.write(txt)


def _run_one(point: dict, args, sweep_dir: Path) -> dict:
    tag = (f"ev{int(point['use_true_ev'])}_cal{point['calibration']}_"
           f"me{point['min_ev']:.2f}_mc{point['min_conf']:.2f}")
    point_dir = sweep_dir / "points" / tag
    point_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = point_dir / "config.yaml"
    _write_config(args.base_config, str(cfg_path),
                  use_true_ev=point["use_true_ev"],
                  min_ev=point["min_ev"], min_conf=point["min_conf"])

    cmd = [sys.executable, "-m", "scripts.replay.backtest",
           "--since", args.since, "--until", args.until,
           "--variant-config", str(cfg_path),
           "--out-dir", str(point_dir),
           "--log-level", "WARNING"]
    if point["calibration"] != "identity":
        # A4B.4 uses production-matching semantics: only the EV path sees the
        # calibrated probability; result.confidence (gate + edge) stays raw.
        cmd += ["--prod-calibration", point["calibration"],
                "--calibration-curve", args.calibration_curve]

    started = time.time()
    try:
        r = subprocess.run(cmd, cwd=_SEULGI, check=True,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           timeout=600)
    except subprocess.CalledProcessError as exc:
        logger.error("[sweep:%s] FAILED rc=%d stderr=%s",
                     tag, exc.returncode, exc.stderr.decode("utf-8", "replace")[-500:])
        return {**point, "error": exc.stderr.decode("utf-8", "replace")[-500:]}
    secs = time.time() - started

    # Locate the manifest the backtest just wrote (runs/<run_id>/).
    runs_dir = point_dir / "runs"
    if not runs_dir.exists():
        return {**point, "error": "no_runs_dir"}
    mfs = sorted(runs_dir.glob("*/manifest.json"))
    if not mfs:
        return {**point, "error": "no_manifest"}
    manifest = json.loads(mfs[-1].read_text())
    # Per-engine roll-up.
    per_eng = manifest.get("total_pnl_per_engine", {})
    sum_pnl = sum(v.get("sum_pnl", 0.0) for v in per_eng.values())
    # Sort engines by trade volume desc — used for rank stability.
    ranked = sorted(per_eng.items(),
                    key=lambda kv: kv[1].get("n_trades_total", 0),
                    reverse=True)
    rank_order = [k for k, _ in ranked if ranked[0][1].get("n_trades_total", 0) > 0]
    # Per-engine PnL rank (by sum_pnl desc) — for Kendall's tau.
    pnl_rank = [k for k, _ in sorted(per_eng.items(),
                                     key=lambda kv: -kv[1].get("sum_pnl", 0.0))]
    # Win-rate proxy: count engines with positive sum_pnl.
    n_positive_engines = sum(1 for v in per_eng.values() if v.get("sum_pnl", 0.0) > 0)
    return {
        **point,
        "tag": tag,
        "manifest_path": str(mfs[-1]),
        "n_trades": manifest.get("n_trades_simulated", 0),
        "n_decisions": manifest.get("n_decisions_walked", 0),
        "sum_pnl": sum_pnl,
        "n_positive_engines": n_positive_engines,
        "engine_count": len(per_eng),
        "rank_order_by_volume": rank_order,
        "rank_order_by_pnl": pnl_rank,
        "per_engine_pnl": {k: v.get("sum_pnl", 0.0) for k, v in per_eng.items()},
        "per_engine_trades": {k: v.get("n_trades_total", 0) for k, v in per_eng.items()},
        "skipped_by_reason": manifest.get("n_skipped", {}).get("by_reason", {}),
        "run_seconds": secs,
    }


def _coarse_grid() -> list[dict]:
    """3×3×2×3 = 54 points with three big knobs + calibration off/on/iso."""
    pts: list[dict] = []
    for use_true_ev in (True, False):
        for cal in ("identity", "isotonic"):
            for me in (0.00, 0.04, 0.08):
                for mc in (0.20, 0.35, 0.50):
                    pts.append({
                        "use_true_ev": use_true_ev,
                        "calibration": cal,
                        "min_ev": me, "min_conf": mc,
                    })
    return pts


def _refinement_grid(centre_ev: float, centre_conf: float) -> list[dict]:
    """5×5 around the chosen coarse winner, isotonic + true-EV."""
    pts: list[dict] = []
    ev_grid = sorted({round(max(0.0, centre_ev + d), 4)
                      for d in (-0.04, -0.02, 0.0, 0.02, 0.04)})
    conf_grid = sorted({round(max(0.0, min(0.95, centre_conf + d)), 4)
                        for d in (-0.10, -0.05, 0.0, 0.05, 0.10)})
    for me in ev_grid:
        for mc in conf_grid:
            pts.append({
                "use_true_ev": True,
                "calibration": "isotonic",
                "min_ev": me, "min_conf": mc,
            })
    return pts


def _platt_check(centre_ev: float, centre_conf: float) -> list[dict]:
    """Verify the chosen point with Platt for parity."""
    return [{
        "use_true_ev": True,
        "calibration": "platt",
        "min_ev": centre_ev,
        "min_conf": centre_conf,
    }]


def _baseline_legacy() -> list[dict]:
    """Pre-bundle reference point (legacy gate, no calibration)."""
    return [{
        "use_true_ev": False,
        "calibration": "identity",
        "min_ev": 0.0,
        "min_conf": 0.35,
    }]


def main(argv: Optional[list] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--since", required=True)
    ap.add_argument("--until", required=True)
    ap.add_argument("--base-config", required=True,
                    help="Base config.yaml (the production one).")
    ap.add_argument("--calibration-curve", required=True,
                    help="Path to calibration.json from calibration_prototype.")
    ap.add_argument("--out-dir", required=True,
                    help="Output root; per-point runs land in <out_dir>/points/.")
    ap.add_argument("--grid", default="coarse",
                    choices=("coarse", "refine", "platt", "baseline"),
                    help="Which grid to run.")
    ap.add_argument("--centre-ev", type=float, default=0.0,
                    help="For --grid refine/platt: centre of the refinement grid.")
    ap.add_argument("--centre-conf", type=float, default=0.35,
                    help="For --grid refine/platt: centre of the refinement grid.")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    sweep_dir = Path(args.out_dir)
    sweep_dir.mkdir(parents=True, exist_ok=True)

    if args.grid == "coarse":
        points = _coarse_grid()
    elif args.grid == "refine":
        points = _refinement_grid(args.centre_ev, args.centre_conf)
    elif args.grid == "platt":
        points = _platt_check(args.centre_ev, args.centre_conf)
    elif args.grid == "baseline":
        points = _baseline_legacy()
    else:
        raise SystemExit(f"unknown grid {args.grid}")
    logger.info("[sweep] grid=%s n_points=%d", args.grid, len(points))

    results: list[dict] = []
    index_path = sweep_dir / f"sweep_index_{args.grid}.json"
    for i, point in enumerate(points, 1):
        logger.info("[sweep] %d/%d %s", i, len(points), point)
        res = _run_one(point, args, sweep_dir)
        results.append(res)
        index_path.write_text(json.dumps(results, indent=2, default=str))
    logger.info("[sweep] done; %d/%d succeeded; index=%s",
                sum(1 for r in results if "error" not in r),
                len(results), index_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
