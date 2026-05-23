"""
## Specification

**Purpose.** Read every `(trades JOIN trade_closures)` row from a DB and
reconstruct each `realized_pnl_usdc` from `(direction, size_usdc,
entry_mid, exit_mid)` using the same formulas the closer uses. Fail (exit
1) if any stored pnl drifts more than `--tolerance` from the reconstruction.

**Why this exists.** The P1 ship-gate accuracy invariant
(`master-plan.md` §"Phase 1 — Detailed scope") is "every closed paper
trade's `pnl_usdc` reconstructed within ±1¢". This script IS that gate —
running it against a soak DB tells us whether the captured numbers
suffice to replay the bot's history.

**Public interface.**
- CLI: `python -m scripts.replay.accuracy --db <path> [--tolerance 0.01]`
- `run_inproc(db_path, tolerance=0.01)` — programmatic entry point used by
  tests + the soak runner; returns a `ReplayResult` dataclass.

**Invariants.**
- `:memory:` is rejected — replay only runs against a real on-disk DB
  (the soak writer's DB).
- Formula identical to `app/stages/closer.py`'s. If that file changes,
  this file changes in the same PR.

**Exit codes.**
- 0 — every row within tolerance.
- 1 — at least one row drifted; the worst offenders are printed.

**Non-goals.** Re-deciding trades (no LLM calls, no model swap). Replay
accuracy answers "did we store enough state to reconstruct PnL?"; the
*counterfactual* replay (re-run the gate cascade with different config)
lives in `scripts/replay/predict.py` (P2+).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


TOLERANCE_DEFAULT = 0.01  # 1 cent


# ---------------------------------------------------------------------------
# Formulas (mirror app/stages/closer.py exactly)
# ---------------------------------------------------------------------------


def reconstruct_pnl(
    *,
    direction: str,
    size_usdc: float,
    entry_mid: float,
    exit_mid: float,
) -> float:
    """Mirror of `app/stages/closer.py`'s PnL formula. P1 invariant."""
    if direction == "YES":
        if entry_mid <= 0.0:
            return 0.0
        return size_usdc * (exit_mid - entry_mid) / entry_mid
    if direction == "NO":
        if entry_mid >= 1.0:
            return 0.0
        return size_usdc * (entry_mid - exit_mid) / (1.0 - entry_mid)
    raise ValueError(f"unknown direction: {direction!r}")


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------


@dataclass
class _MismatchRow:
    trade_id: int
    direction: str
    size_usdc: float
    entry_mid: float
    exit_mid: float
    stored_pnl: float
    reconstructed_pnl: float

    @property
    def diff(self) -> float:
        return abs(self.stored_pnl - self.reconstructed_pnl)


@dataclass
class ReplayResult:
    total_closed: int = 0
    within_tolerance: int = 0
    mismatches: list[_MismatchRow] = field(default_factory=list)
    tolerance: float = TOLERANCE_DEFAULT

    @property
    def passed(self) -> bool:
        return not self.mismatches

    @property
    def max_drift(self) -> float:
        return max((m.diff for m in self.mismatches), default=0.0)


# ---------------------------------------------------------------------------
# Implementation
# ---------------------------------------------------------------------------


def _query_closed_trades(db_path: str) -> list[sqlite3.Row]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT t.id AS trade_id,
                   t.direction       AS direction,
                   t.size_usdc       AS size_usdc,
                   t.mid_price       AS entry_mid,
                   c.exit_mid_price  AS exit_mid,
                   c.realized_pnl_usdc AS stored_pnl
            FROM trades t
            JOIN trade_closures c ON c.trade_id = t.id
            ORDER BY t.id
            """
        ).fetchall()
    finally:
        conn.close()
    return rows


def run_inproc(db_path: str, tolerance: float = TOLERANCE_DEFAULT) -> ReplayResult:
    if db_path == ":memory:":
        raise ValueError(":memory: cannot be replayed — accuracy.py reads from on-disk SQLite.")
    if not Path(db_path).exists():
        raise FileNotFoundError(f"DB not found: {db_path}")

    rows = _query_closed_trades(db_path)
    result = ReplayResult(tolerance=tolerance)
    for row in rows:
        result.total_closed += 1
        reconstructed = reconstruct_pnl(
            direction=row["direction"],
            size_usdc=row["size_usdc"],
            entry_mid=row["entry_mid"],
            exit_mid=row["exit_mid"],
        )
        diff = abs(reconstructed - row["stored_pnl"])
        if diff <= tolerance:
            result.within_tolerance += 1
        else:
            result.mismatches.append(
                _MismatchRow(
                    trade_id=int(row["trade_id"]),
                    direction=str(row["direction"]),
                    size_usdc=float(row["size_usdc"]),
                    entry_mid=float(row["entry_mid"]),
                    exit_mid=float(row["exit_mid"]),
                    stored_pnl=float(row["stored_pnl"]),
                    reconstructed_pnl=float(reconstructed),
                )
            )
    return result


def _print_report(result: ReplayResult) -> None:
    print(f"replay accuracy: {result.within_tolerance}/{result.total_closed} within ±{result.tolerance}")
    if result.mismatches:
        print(f"  max drift: ${result.max_drift:.4f}")
        print("  worst rows (up to 10):")
        for m in sorted(result.mismatches, key=lambda r: -r.diff)[:10]:
            print(
                f"    trade {m.trade_id}: stored=${m.stored_pnl:.4f}  "
                f"recon=${m.reconstructed_pnl:.4f}  diff=${m.diff:.4f}  "
                f"({m.direction} size=${m.size_usdc:.2f} entry={m.entry_mid:.4f} exit={m.exit_mid:.4f})"
            )


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Replay accuracy gate for irene paper trades.")
    parser.add_argument("--db", required=True, help="Path to a soak DB (file, not :memory:).")
    parser.add_argument("--tolerance", type=float, default=TOLERANCE_DEFAULT, help="USD tolerance per trade (default 0.01).")
    args = parser.parse_args(argv)
    try:
        result = run_inproc(args.db, tolerance=args.tolerance)
    except (FileNotFoundError, ValueError) as exc:
        print(f"replay accuracy: ERROR {exc}", flush=True)
        return 2
    _print_report(result)
    return 0 if result.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
