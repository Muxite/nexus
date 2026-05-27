"""Cycle 25-I — unit test for the wallet-harness audit script.

Seeds a fresh SQLite DB with hand-crafted trades + closures whose math is
known to be consistent OR known to drift, then runs the audit script and
checks the exit code + report content.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

# The audit script lives under scripts/cycle25; make sure it's importable.
ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.cycle25 import audit_wallet_harness  # noqa: E402


def _seed_minimal_schema(con: sqlite3.Connection) -> None:
    """Create just enough of the trades/trade_closures schema to drive
    the audit. Mirrors the real schema fields the audit reads."""
    con.executescript(
        """
        CREATE TABLE trades (
            id INTEGER PRIMARY KEY,
            market_id TEXT,
            direction TEXT,
            mid_price REAL,
            size_usdc REAL,
            decided_at TEXT,
            decision_path TEXT,
            tracer_id TEXT,
            is_tracer INTEGER DEFAULT 0
        );
        CREATE TABLE trade_closures (
            id INTEGER PRIMARY KEY,
            trade_id INTEGER,
            closed_at TEXT,
            close_reason TEXT,
            exit_mid_price REAL,
            realized_pnl_usdc REAL,
            entry_fee_usdc REAL,
            exit_fee_usdc REAL
        );
        """
    )


def _gross_pnl(direction: str, entry: float, exit_: float, size: float) -> float:
    if direction == "YES":
        return size * (exit_ - entry) / entry
    return size * (entry - exit_) / (1.0 - entry)


def _seed_consistent_trades(con: sqlite3.Connection) -> None:
    """A YES winner and a NO loser, both with realistic fees. Realized
    PnL stored as `gross - entry_fee - exit_fee` (matches closer.py).
    """
    # YES @ 0.40, exit 0.60, size $100, fees $0.50 + $0.45.
    g1 = _gross_pnl("YES", 0.40, 0.60, 100.0)  # +$50
    fees1_e, fees1_x = 0.50, 0.45
    # NO @ 0.30, exit 0.40, size $80, fees $0.30 + $0.25.
    g2 = _gross_pnl("NO", 0.30, 0.40, 80.0)  # NO loses when mid rises
    fees2_e, fees2_x = 0.30, 0.25
    con.executescript(
        f"""
        INSERT INTO trades (id, market_id, direction, mid_price, size_usdc,
                            decided_at, decision_path, tracer_id, is_tracer)
        VALUES (1, 'm1', 'YES', 0.40, 100.0, '2026-05-26T10:00', 'mvp', 'cycle25-test-1', 1);
        INSERT INTO trades (id, market_id, direction, mid_price, size_usdc,
                            decided_at, decision_path, tracer_id, is_tracer)
        VALUES (2, 'm2', 'NO',  0.30,  80.0, '2026-05-26T10:01', 'mvp', 'cycle25-test-2', 1);
        INSERT INTO trade_closures (trade_id, closed_at, close_reason, exit_mid_price,
                                     realized_pnl_usdc, entry_fee_usdc, exit_fee_usdc)
        VALUES (1, '2026-05-26T10:10', 'take_profit', 0.60,
                {g1 - fees1_e - fees1_x}, {fees1_e}, {fees1_x});
        INSERT INTO trade_closures (trade_id, closed_at, close_reason, exit_mid_price,
                                     realized_pnl_usdc, entry_fee_usdc, exit_fee_usdc)
        VALUES (2, '2026-05-26T10:11', 'stop_loss', 0.40,
                {g2 - fees2_e - fees2_x}, {fees2_e}, {fees2_x});
        """
    )


def _seed_drifted_closure(con: sqlite3.Connection) -> None:
    """Single trade whose stored realized PnL disagrees with the recomputed
    gross-minus-fees by $5 — the per-closure check should flag it.
    """
    g = _gross_pnl("YES", 0.50, 0.55, 100.0)  # +$10
    con.executescript(
        f"""
        INSERT INTO trades (id, market_id, direction, mid_price, size_usdc,
                            decided_at, decision_path, tracer_id, is_tracer)
        VALUES (10, 'm10', 'YES', 0.50, 100.0, '2026-05-26T11:00', 'mvp', 'cycle25-drift', 1);
        INSERT INTO trade_closures (trade_id, closed_at, close_reason, exit_mid_price,
                                     realized_pnl_usdc, entry_fee_usdc, exit_fee_usdc)
        VALUES (10, '2026-05-26T11:10', 'take_profit', 0.55,
                {g - 0.5 - 0.5 - 5.00}, 0.5, 0.5);
        """
    )


def test_audit_passes_on_consistent_trades(tmp_path):
    db_path = tmp_path / "audit-pass.db"
    con = sqlite3.connect(str(db_path))
    _seed_minimal_schema(con)
    _seed_consistent_trades(con)
    con.commit()
    con.close()

    out = tmp_path / "report.md"
    rc = audit_wallet_harness.run_audit(
        str(db_path),
        initial_balance=1000.0,
        prefix="cycle25-test",
        tol_aggregate=0.01,
        tol_per_closure=0.005,
        output=out,
        print_to_stdout=False,
    )
    assert rc == 0
    report = out.read_text()
    assert "**OK**" in report and "Failures: **0**" in report
    assert "PASS" in report


def test_audit_flags_per_closure_drift(tmp_path):
    db_path = tmp_path / "audit-fail.db"
    con = sqlite3.connect(str(db_path))
    _seed_minimal_schema(con)
    _seed_drifted_closure(con)
    con.commit()
    con.close()

    out = tmp_path / "report.md"
    rc = audit_wallet_harness.run_audit(
        str(db_path),
        initial_balance=1000.0,
        prefix="cycle25-drift",
        tol_aggregate=0.01,
        tol_per_closure=0.005,
        output=out,
        print_to_stdout=False,
    )
    assert rc == 1
    report = out.read_text()
    assert "FAIL" in report
    # The synthetic drift was $5; the failure table should mention the
    # trade and the delta.
    assert "Failures: **1**" in report


def test_audit_prefix_isolates_runs(tmp_path):
    db_path = tmp_path / "audit-prefix.db"
    con = sqlite3.connect(str(db_path))
    _seed_minimal_schema(con)
    _seed_consistent_trades(con)  # prefix cycle25-test
    _seed_drifted_closure(con)    # prefix cycle25-drift
    con.commit()
    con.close()

    # Asking only for the clean prefix should pass.
    rc_clean = audit_wallet_harness.run_audit(
        str(db_path),
        initial_balance=1000.0,
        prefix="cycle25-test",
        tol_aggregate=0.01,
        tol_per_closure=0.005,
        output=None,
        print_to_stdout=False,
    )
    assert rc_clean == 0

    # Asking only for the drift prefix should fail.
    rc_dirty = audit_wallet_harness.run_audit(
        str(db_path),
        initial_balance=1000.0,
        prefix="cycle25-drift",
        tol_aggregate=0.01,
        tol_per_closure=0.005,
        output=None,
        print_to_stdout=False,
    )
    assert rc_dirty == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
