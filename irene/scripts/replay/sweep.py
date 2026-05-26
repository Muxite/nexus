"""Tier 2 — cartesian-product sweep over `predict.py` config overrides.

## Specification

**Purpose.** Run `scripts.replay.predict.run_inproc` once per point in
the cartesian product of `--sweep section.key=v1,v2,...` flags, write
one CSV row per (snapshot × config) combination. Direct artifact for
the parameter-sensitivity research questions:

- **i-F1** cosine floor — `--sweep trade.min_similarity=0.30,0.32,0.35,0.40,0.45`
- **i-F2** edge floor   — `--sweep trade.min_edge=0.02,0.03,0.04,0.06,0.08`
- **i-F3** Kelly       — `--sweep trade.kelly_fraction=0.10,0.15,0.25,0.50,1.0`
- **i-E1** funnel stability across cycles — pass multiple `--snapshot` dirs.

**Public interface.**
- CLI:
    python -m scripts.replay.sweep \\
      --snapshot data/snapshots/cycle_0029_cycle7-expand/ \\
      [--snapshot ...] \\
      --sweep trade.min_edge=0.02,0.04,0.06,0.08 \\
      --sweep trade.kelly_fraction=0.10,0.25,0.50 \\
      --out data/reports/sweep_min_edge_kelly.csv
- `run_inproc(snapshot_dirs, sweeps) -> list[dict]` — programmatic.

**CSV columns** (one row per snapshot × config point):
  snapshot, n_signals, n_skipped,
  <every overridden knob, one column each>,
  bucket_a_n, bucket_a_orig_pnl, bucket_a_cf_pnl,
  bucket_b_n, bucket_b_displaced_pnl,
  bucket_c_n, bucket_c_projected_pnl, bucket_c_notional,
  bucket_d_n, bucket_d_notional,
  no_change_reject, decision_changes,
  net_pnl_delta, projected_frac.

**Invariants.**
- Smoke test: a `--sweep` with a single value equal to the default
  should match the zero-override run from `predict.py`. PR-D's
  self-consistency invariant carries forward.

**Non-goals.**
- Statistical significance scoring.
- Plotting.
- Live-DB reads.
"""

from __future__ import annotations

import argparse
import csv
import itertools
import json
import sys
from pathlib import Path
from typing import Any, Iterable, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.replay.predict import _coerce, run_inproc as predict_run  # noqa: E402


def _parse_sweep_flag(raw: str) -> tuple[str, list[Any]]:
    """`trade.min_edge=0.02,0.04,0.06` → ("trade.min_edge", [0.02, 0.04, 0.06])."""
    if "=" not in raw:
        raise ValueError(f"--sweep: missing '=' in {raw!r}")
    key, values_str = raw.split("=", 1)
    values = [_coerce(v.strip()) for v in values_str.split(",") if v.strip()]
    if not values:
        raise ValueError(f"--sweep: no values for {key!r}")
    return key.strip(), values


def _cartesian(sweeps: dict[str, list[Any]]) -> Iterable[dict[str, Any]]:
    """Cartesian product. Yields one dict per combination, e.g.
    {'trade.min_edge': 0.02, 'trade.kelly_fraction': 0.10}."""
    if not sweeps:
        yield {}
        return
    keys = list(sweeps.keys())
    values_lists = [sweeps[k] for k in keys]
    for combo in itertools.product(*values_lists):
        yield dict(zip(keys, combo))


def _report_to_row(snapshot: str, overrides: dict, rep) -> dict:
    """Flatten a ReplayReport into a single CSV row."""
    row: dict[str, Any] = {
        "snapshot": snapshot,
        "n_signals": rep.n_signals,
        "n_skipped": rep.n_skipped,
    }
    # Overridden knobs surface as their own columns.
    for k, v in sorted(overrides.items()):
        row[k] = v
    row.update({
        "bucket_a_n": rep.bucket_a.n,
        "bucket_a_orig_pnl": round(rep.bucket_a.original_pnl, 4),
        "bucket_a_cf_pnl": round(rep.bucket_a.counterfactual_pnl, 4),
        "bucket_b_n": rep.bucket_b.n,
        "bucket_b_displaced_pnl": round(rep.bucket_b.original_pnl, 4),
        "bucket_b_notional": round(rep.bucket_b.implied_notional, 4),
        "bucket_c_n": rep.bucket_c.n,
        "bucket_c_projected_pnl": round(rep.bucket_c.counterfactual_pnl, 4),
        "bucket_c_notional": round(rep.bucket_c.implied_notional, 4),
        "bucket_d_n": rep.bucket_d.n,
        "bucket_d_notional": round(rep.bucket_d.implied_notional, 4),
        "no_change_reject": rep.bucket_no_change_reject,
        "decision_changes": rep.counterfactual_decision_changes,
        "net_pnl_delta": round(rep.net_pnl_delta, 4),
        "projected_frac": round(rep.projected_frac, 4),
    })
    return row


def run_inproc(
    snapshot_dirs: list[str],
    sweeps: dict[str, list[Any]],
) -> list[dict]:
    """Run the cartesian product. Returns a list of row dicts (one per
    snapshot × override combination)."""
    out: list[dict] = []
    points = list(_cartesian(sweeps))
    for snap in snapshot_dirs:
        for overrides in points:
            rep = predict_run(snap, overrides)
            out.append(_report_to_row(snap, overrides, rep))
    return out


def _write_csv(rows: list[dict], out_path: Path) -> None:
    if not rows:
        return
    # Stable column order: union of all keys, sorted with knowns first.
    base_cols = [
        "snapshot", "n_signals", "n_skipped",
    ]
    extra_cols = sorted(
        k for k in rows[0].keys()
        if k not in base_cols and k not in {
            "bucket_a_n", "bucket_a_orig_pnl", "bucket_a_cf_pnl",
            "bucket_b_n", "bucket_b_displaced_pnl", "bucket_b_notional",
            "bucket_c_n", "bucket_c_projected_pnl", "bucket_c_notional",
            "bucket_d_n", "bucket_d_notional",
            "no_change_reject", "decision_changes",
            "net_pnl_delta", "projected_frac",
        }
    )
    tail_cols = [
        "bucket_a_n", "bucket_a_orig_pnl", "bucket_a_cf_pnl",
        "bucket_b_n", "bucket_b_displaced_pnl", "bucket_b_notional",
        "bucket_c_n", "bucket_c_projected_pnl", "bucket_c_notional",
        "bucket_d_n", "bucket_d_notional",
        "no_change_reject", "decision_changes",
        "net_pnl_delta", "projected_frac",
    ]
    cols = base_cols + extra_cols + tail_cols
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in cols})


def _print_summary(rows: list[dict]) -> None:
    print(f"sweep: {len(rows)} rows")
    if not rows:
        return
    # Find the best (most positive net_pnl_delta) and the worst.
    best = max(rows, key=lambda r: r["net_pnl_delta"])
    worst = min(rows, key=lambda r: r["net_pnl_delta"])
    print(f"  best  net_pnl_delta = {best['net_pnl_delta']:+.4f}  "
          f"projected_frac={best['projected_frac']}  ({best['snapshot']})")
    print(f"  worst net_pnl_delta = {worst['net_pnl_delta']:+.4f}  "
          f"projected_frac={worst['projected_frac']}  ({worst['snapshot']})")
    print()
    # Show overridden knobs for both.
    overridden_keys = [
        k for k in best.keys()
        if k not in {
            "snapshot", "n_signals", "n_skipped",
            "bucket_a_n", "bucket_a_orig_pnl", "bucket_a_cf_pnl",
            "bucket_b_n", "bucket_b_displaced_pnl", "bucket_b_notional",
            "bucket_c_n", "bucket_c_projected_pnl", "bucket_c_notional",
            "bucket_d_n", "bucket_d_notional",
            "no_change_reject", "decision_changes",
            "net_pnl_delta", "projected_frac",
        }
    ]
    if overridden_keys:
        print(f"  best knobs:  {', '.join(f'{k}={best.get(k)}' for k in overridden_keys)}")
        print(f"  worst knobs: {', '.join(f'{k}={worst.get(k)}' for k in overridden_keys)}")


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--snapshot", action="append", required=True,
        help="repeatable: snapshot directory",
    )
    p.add_argument(
        "--sweep", action="append", default=[],
        help="repeatable: section.key=v1,v2,v3",
    )
    p.add_argument("--out", help="CSV output path (recommended)")
    args = p.parse_args(argv)

    sweeps: dict[str, list[Any]] = {}
    for raw in args.sweep:
        key, values = _parse_sweep_flag(raw)
        sweeps[key] = values

    try:
        rows = run_inproc(args.snapshot, sweeps)
    except (FileNotFoundError, ValueError) as exc:
        print(f"sweep: ERROR {exc}", file=sys.stderr)
        return 2

    _print_summary(rows)
    if args.out:
        _write_csv(rows, Path(args.out))
        print(f"\ncsv: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
