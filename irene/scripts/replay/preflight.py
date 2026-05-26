"""Backtest preflight gate.

Runs `scripts.replay.accuracy` against the live soak DB to assert that every
closed paper trade's stored `realized_pnl_usdc` reconstructs within
`--tolerance` of the closer formula. Wired into `irene-soak.service` as
`ExecStartPre=-…/preflight` so each soak tick logs whether the captured
state remains replay-able. Non-blocking on failure by design — replay drift
and soak health are independent signals.

CLI: `python -m scripts.replay.preflight [--tolerance 0.01] [--db PATH]`

Exit codes:
- 0 — every closed trade within tolerance (or no closures yet).
- 1 — at least one stored pnl drifts beyond tolerance.
- 2 — bad input (DB missing, `:memory:`, etc.).
"""
from __future__ import annotations

import argparse
import sys
from typing import Optional

from scripts.replay.accuracy import TOLERANCE_DEFAULT, run_inproc

DEFAULT_DB = "/home/muk/projects/jennie/irene/data/irene.db"


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Backtest preflight gate (wraps replay.accuracy).")
    parser.add_argument("--db", default=DEFAULT_DB, help=f"Path to soak DB (default: {DEFAULT_DB}).")
    parser.add_argument("--tolerance", type=float, default=TOLERANCE_DEFAULT,
                        help="USD tolerance per trade (default 0.01).")
    args = parser.parse_args(argv)

    try:
        result = run_inproc(args.db, tolerance=args.tolerance)
    except (FileNotFoundError, ValueError) as exc:
        print(f"PREFLIGHT ERROR: {exc}", flush=True)
        return 2

    if result.passed:
        print(
            f"PREFLIGHT OK: {result.within_tolerance}/{result.total_closed} "
            f"trades within ±{result.tolerance}",
            flush=True,
        )
        return 0

    print(
        f"PREFLIGHT DRIFT: {result.within_tolerance}/{result.total_closed} "
        f"within ±{result.tolerance}; max drift ${result.max_drift:.4f}",
        flush=True,
    )
    worst = sorted(result.mismatches, key=lambda m: -m.diff)[:5]
    for m in worst:
        print(
            f"  trade {m.trade_id}: stored=${m.stored_pnl:.4f} "
            f"recon=${m.reconstructed_pnl:.4f} diff=${m.diff:.4f} "
            f"({m.direction} size=${m.size_usdc:.2f} entry={m.entry_mid:.4f} exit={m.exit_mid:.4f})",
            flush=True,
        )
    return 1


if __name__ == "__main__":
    sys.exit(main())
