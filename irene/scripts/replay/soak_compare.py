"""PR-G — soak-vs-replay reproducibility check.

## Specification

**Purpose.** The user-required validation deliverable: prove that
running `predict.py` over the same time window as a recorded live soak
reproduces the soak's headline numbers within tolerance. If the replay
diverges, the cascade extraction (PR-A) is broken — or a code change
landed between the soak and the replay run.

Reads a snapshot manifest's wallet stats + per-table row counts +
per-decision_path PnL, runs `predict.run_inproc` with **zero
overrides** against the same snapshot, asserts:

  • counterfactual decision count per signal matches snapshot's
    `decisions.outcome` / `reason` exactly
  • net realised PnL on bucket (a) matches snapshot's wallet
    `realized_pnl` within `--tolerance` (default ±$0.01,
    matching `accuracy.py`)
  • bucket (b) is empty (no decisions should flip with zero
    overrides)
  • bucket (c) and (d) are empty (no new fires)

A non-zero `decision_changes` count means CODE DRIFT — either the
cascade has changed since the snapshot was written, or PR-A's
extraction is mis-mapping a gate. The output identifies WHICH gate
changed.

**Public interface.**
- CLI:
    python -m scripts.replay.soak_compare \\
      --snapshot data/snapshots/cycle_NNNN/ \\
      [--tolerance 0.01]
- `run_inproc(snapshot_dir, tolerance) -> SoakCompareResult` —
  programmatic.

**Exit codes.**
- 0 — every reproducibility check passed.
- 1 — at least one check failed; the report identifies which.
- 2 — couldn't read the snapshot.

**Non-goals.**
- Sweeps / counterfactual exploration — that's `predict.py` / `sweep.py`.
- Re-running LLM. The replay reuses snapshot predictions.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.replay._snapshot_loader import SnapshotLoader  # noqa: E402
from scripts.replay.predict import (  # noqa: E402
    ReplayReport,
    run_inproc as predict_run,
)


@dataclass
class SoakCompareResult:
    snapshot: str
    tolerance: float
    rep: ReplayReport
    # Reproducibility checks (each True = pass).
    decisions_match: bool
    bucket_b_empty: bool
    bucket_c_empty: bool
    bucket_d_empty: bool
    pnl_within_tolerance: bool
    realised_pnl_drift: float
    # Per-gate drift table — gates whose new vs old reason counts differ.
    funnel_drift: dict[str, int] = field(default_factory=dict)
    # n_skipped is informational — signals whose features can't be
    # rebuilt (e.g. pre-PR-B snapshot without `mid` in gate_history)
    # — does NOT count as a reproducibility failure.
    n_skipped: int = 0
    skip_reasons: dict[str, int] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return (
            self.decisions_match
            and self.bucket_b_empty
            and self.bucket_c_empty
            and self.bucket_d_empty
            and self.pnl_within_tolerance
        )

    def to_dict(self) -> dict:
        return {
            "snapshot": self.snapshot,
            "tolerance": self.tolerance,
            "passed": self.passed,
            "checks": {
                "decisions_match": self.decisions_match,
                "bucket_b_empty": self.bucket_b_empty,
                "bucket_c_empty": self.bucket_c_empty,
                "bucket_d_empty": self.bucket_d_empty,
                "pnl_within_tolerance": self.pnl_within_tolerance,
            },
            "realised_pnl_drift": round(self.realised_pnl_drift, 4),
            "funnel_drift": self.funnel_drift,
            "n_skipped": self.n_skipped,
            "skip_reasons": self.skip_reasons,
            "rep_summary": {
                "n_signals": self.rep.n_signals,
                "decision_changes": self.rep.counterfactual_decision_changes,
                "bucket_a_n": self.rep.bucket_a.n,
                "bucket_a_orig_pnl": round(self.rep.bucket_a.original_pnl, 4),
                "bucket_a_cf_pnl": round(self.rep.bucket_a.counterfactual_pnl, 4),
                "bucket_b_n": self.rep.bucket_b.n,
                "bucket_c_n": self.rep.bucket_c.n,
                "bucket_d_n": self.rep.bucket_d.n,
            },
        }


def run_inproc(snapshot_dir: str, tolerance: float = 0.01) -> SoakCompareResult:
    loader = SnapshotLoader(snapshot_dir)
    rep = predict_run(snapshot_dir)  # zero overrides

    snapshot_realised = float(loader.manifest.get("wallet", {}).get("realized_pnl", 0.0))

    decisions_match = rep.counterfactual_decision_changes == 0
    bucket_b_empty = rep.bucket_b.n == 0
    bucket_c_empty = rep.bucket_c.n == 0
    bucket_d_empty = rep.bucket_d.n == 0

    # PnL comparison: bucket-a counterfactual PnL should equal the
    # snapshot's recorded realised_pnl within tolerance (since with zero
    # overrides the replay should reproduce the same trades + closures).
    realised_drift = rep.bucket_a.counterfactual_pnl - snapshot_realised
    pnl_within_tolerance = abs(realised_drift) <= tolerance

    funnel_drift = {k: v for k, v in rep.funnel_deltas.items() if v != 0}

    return SoakCompareResult(
        snapshot=str(snapshot_dir),
        tolerance=tolerance,
        rep=rep,
        decisions_match=decisions_match,
        bucket_b_empty=bucket_b_empty,
        bucket_c_empty=bucket_c_empty,
        bucket_d_empty=bucket_d_empty,
        pnl_within_tolerance=pnl_within_tolerance,
        realised_pnl_drift=realised_drift,
        funnel_drift=funnel_drift,
        n_skipped=rep.n_skipped,
        skip_reasons=dict(rep.skip_reasons),
    )


def _print_report(r: SoakCompareResult) -> None:
    print(f"soak_compare: {r.snapshot}")
    print(f"  tolerance: ${r.tolerance:.4f}")
    print()
    pass_str = lambda ok: "PASS" if ok else "FAIL"
    print(f"  [ {pass_str(r.decisions_match)} ] decisions_match   "
          f"(decision_changes = {r.rep.counterfactual_decision_changes})")
    print(f"  [ {pass_str(r.bucket_b_empty)} ] bucket_b_empty    "
          f"(n = {r.rep.bucket_b.n}, displaced = ${r.rep.bucket_b.original_pnl:.4f})")
    print(f"  [ {pass_str(r.bucket_c_empty)} ] bucket_c_empty    "
          f"(n = {r.rep.bucket_c.n})")
    print(f"  [ {pass_str(r.bucket_d_empty)} ] bucket_d_empty    "
          f"(n = {r.rep.bucket_d.n})")
    print(f"  [ {pass_str(r.pnl_within_tolerance)} ] pnl_within_tolerance  "
          f"(drift = ${r.realised_pnl_drift:+.4f})")
    print()
    if r.n_skipped > 0:
        print(f"  informational: {r.n_skipped} signals skipped (not a reproducibility failure)")
        for k, v in sorted(r.skip_reasons.items()):
            print(f"    skip[{k}] = {v}")
        print()
    if r.funnel_drift:
        print("  funnel drift (new − original):")
        for k, v in sorted(r.funnel_drift.items(), key=lambda kv: -abs(kv[1])):
            print(f"    {k:30s} {v:+d}")
        print()
    print(f"  RESULT: {'PASS' if r.passed else 'FAIL'}")
    if not r.passed:
        print()
        print("  diagnosis hints:")
        if not r.decisions_match:
            print("    - decisions_match=FAIL means the cascade flipped at least")
            print("      one signal between snapshot-write and now. Check the funnel")
            print("      drift table above; the gate whose count moved is the culprit.")
            print("    - common cause: trade.py was edited between snapshot write")
            print("      and this run; OR a config default changed (cycle-16 raised")
            print("      default_fee_rate 0.05→0.10, which retroactively rejects")
            print("      old-cycle trades on net-PnL math).")
        if not r.pnl_within_tolerance:
            print(f"    - PnL drift ${r.realised_pnl_drift:+.4f} > tolerance "
                  f"${r.tolerance:.4f}. Likely fee_rate change since snapshot;")
            print("      compare per-market `fee_rate` (snapshot vs current).")


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--snapshot", required=True, help="snapshot directory")
    p.add_argument(
        "--tolerance", type=float, default=0.01,
        help="USD tolerance per total PnL (default 0.01, matches accuracy.py)",
    )
    p.add_argument("--out", help="optional JSON output path")
    args = p.parse_args(argv)

    try:
        result = run_inproc(args.snapshot, tolerance=args.tolerance)
    except (FileNotFoundError, ValueError) as exc:
        print(f"soak_compare: ERROR {exc}", file=sys.stderr)
        return 2

    _print_report(result)
    if args.out:
        Path(args.out).write_text(json.dumps(result.to_dict(), indent=2))
        print(f"\nreport JSON: {args.out}")
    return 0 if result.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
