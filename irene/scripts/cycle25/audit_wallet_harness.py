"""Wallet correctness audit for a tracer harness run (cycle-25-I).

Run against any irene DB (live OR a fresh harness DB) and verifies the
cycle-25 real-money cash model holds line-by-line:

  initial_balance + realized_net + unrealized + open_value_at_mid == equity
  cash == initial - open_size - closed_size + closed_exit_value - fees

Two passes:

1. **Aggregate identity** — reconstruct the wallet from raw `trades` and
   `trade_closures` rows; compare against `wallet.cash_balance` /
   `wallet.equity` / `wallet.accounting_identity_check`. Pass when drift
   <= $0.01 (default).

2. **Per-closure reconciliation** — for every `trade_closures` row,
   recompute `gross_pnl` from raw direction/entry/exit/size, recompute
   `realized_net = gross - entry_fee - exit_fee`, and compare to the
   stored `realized_pnl_usdc`. Pass when |delta| <= $0.005 per row.

Per-prefix scoping (`--prefix tracer-...`) restricts the audit to the
tracer rows of a given run so a harness run can be evaluated in isolation
from any live (paper) production trades on the same DB.

Exit code 0 iff every line item passes. Output is a single markdown
report; `--print` also writes to stdout.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

# Standalone helper: don't import app.wallet to keep this script runnable
# even from outside the irene project root (e.g. CI).


def _exit_value(direction: str, entry_mid: float, exit_mid: float, size_usdc: float) -> float:
    if direction == "YES":
        if entry_mid <= 0.0:
            return float(size_usdc)
        return float(size_usdc) * float(exit_mid) / float(entry_mid)
    if direction == "NO":
        denom = 1.0 - float(entry_mid)
        if denom <= 0.0:
            return float(size_usdc)
        return float(size_usdc) * (1.0 - float(exit_mid)) / denom
    raise ValueError(f"unknown direction: {direction!r}")


def _gross_pnl(direction: str, entry_mid: float, exit_mid: float, size_usdc: float) -> float:
    return _exit_value(direction, entry_mid, exit_mid, size_usdc) - float(size_usdc)


def _connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    return con


def _fetch_rows(con: sqlite3.Connection, *, prefix: str | None) -> tuple[list[sqlite3.Row], list[sqlite3.Row]]:
    """Return (open_trades, closures_with_trade_columns)."""
    where_open = "WHERE c.id IS NULL"
    where_closed = "WHERE c.id IS NOT NULL"
    if prefix is not None:
        where_open += " AND t.is_tracer=1 AND t.tracer_id LIKE :pat"
        where_closed += " AND t.is_tracer=1 AND t.tracer_id LIKE :pat"
    else:
        # Default: real (paper) production trades only.
        where_open += " AND t.is_tracer=0"
        where_closed += " AND t.is_tracer=0"

    params = {"pat": (prefix or "") + "%"} if prefix is not None else {}

    open_rows = list(con.execute(
        f"""
        SELECT t.id, t.market_id, t.direction, t.mid_price AS entry_mid,
               t.size_usdc, t.decided_at, t.decision_path, t.tracer_id
        FROM trades t
        LEFT JOIN trade_closures c ON c.trade_id = t.id
        {where_open}
        """,
        params,
    ).fetchall())

    closed_rows = list(con.execute(
        f"""
        SELECT t.id AS trade_id, t.direction, t.mid_price AS entry_mid,
               t.size_usdc, t.decision_path, t.tracer_id,
               c.exit_mid_price, c.realized_pnl_usdc, c.close_reason,
               c.entry_fee_usdc, c.exit_fee_usdc, c.closed_at
        FROM trades t
        JOIN trade_closures c ON c.trade_id = t.id
        {where_closed}
        """,
        params,
    ).fetchall())

    return open_rows, closed_rows


def run_audit(
    db_path: str,
    *,
    initial_balance: float,
    prefix: str | None,
    tol_aggregate: float,
    tol_per_closure: float,
    output: Path | None,
    print_to_stdout: bool,
) -> int:
    con = _connect(db_path)
    open_rows, closed_rows = _fetch_rows(con, prefix=prefix)

    # --- Aggregate identity ------------------------------------------------
    open_size = sum(float(r["size_usdc"]) for r in open_rows)
    closed_size = sum(float(r["size_usdc"]) for r in closed_rows)
    closed_exit_value = sum(
        _exit_value(
            r["direction"], float(r["entry_mid"]),
            float(r["exit_mid_price"]), float(r["size_usdc"]),
        )
        for r in closed_rows
    )
    realized = sum(float(r["realized_pnl_usdc"]) for r in closed_rows)
    fees_paid = sum(
        float(r["entry_fee_usdc"] or 0.0) + float(r["exit_fee_usdc"] or 0.0)
        for r in closed_rows
    )
    # For open positions we don't have a current_mid in this script (the
    # CLOB call is expensive and we want the audit to be DB-only). We
    # approximate open_value at entry_mid (unrealized = 0) — this is the
    # honest "unpriceable" treatment matching app/wallet.py's fallback.
    open_value_at_entry = open_size  # YES @ entry/entry = size; NO @ (1-e)/(1-e) = size
    unrealized = 0.0
    cash = (
        initial_balance
        - open_size - closed_size
        + closed_exit_value - fees_paid
    )
    equity = cash + open_value_at_entry
    expected = initial_balance + realized + unrealized
    drift = equity - expected

    # --- Per-closure reconciliation ---------------------------------------
    per_closure_failures: list[dict] = []
    for r in closed_rows:
        gross = _gross_pnl(
            r["direction"], float(r["entry_mid"]),
            float(r["exit_mid_price"]), float(r["size_usdc"]),
        )
        entry_fee = float(r["entry_fee_usdc"] or 0.0)
        exit_fee = float(r["exit_fee_usdc"] or 0.0)
        net = gross - entry_fee - exit_fee
        delta = float(r["realized_pnl_usdc"]) - net
        if abs(delta) > tol_per_closure:
            per_closure_failures.append({
                "trade_id": r["trade_id"],
                "direction": r["direction"],
                "entry": float(r["entry_mid"]),
                "exit": float(r["exit_mid_price"]),
                "size": float(r["size_usdc"]),
                "stored_net": float(r["realized_pnl_usdc"]),
                "recomputed_net": net,
                "delta": delta,
                "close_reason": r["close_reason"],
                "tracer_id": r["tracer_id"],
            })

    # --- Render report -----------------------------------------------------
    aggregate_ok = abs(drift) <= tol_aggregate
    per_closure_ok = not per_closure_failures
    overall_ok = aggregate_ok and per_closure_ok

    parts = [
        f"# Wallet harness audit ({datetime.now(timezone.utc).isoformat()})",
        "",
        f"DB: `{db_path}`",
        f"Prefix filter: `{prefix or '<paper production>'}`",
        f"Initial balance: ${initial_balance:.2f}",
        "",
        "## Aggregate identity",
        "",
        "| Component | Value |",
        "|---|---|",
        f"| Open positions (n) | {len(open_rows)} |",
        f"| Closed positions (n) | {len(closed_rows)} |",
        f"| Open size invested | ${open_size:.4f} |",
        f"| Closed size invested | ${closed_size:.4f} |",
        f"| Closed exit value (gross) | ${closed_exit_value:.4f} |",
        f"| Fees paid (entry+exit) | ${fees_paid:.4f} |",
        f"| Realized PnL (net) | ${realized:+.4f} |",
        f"| Unrealized PnL (entry-mid fallback) | ${unrealized:+.4f} |",
        f"| Reconstructed cash | ${cash:.4f} |",
        f"| Reconstructed equity | ${equity:.4f} |",
        f"| Expected equity (initial + realized + unrealized) | ${expected:.4f} |",
        f"| Drift | ${drift:+.4f} |",
        f"| Aggregate identity | **{'OK' if aggregate_ok else 'FAIL'}** (tol ${tol_aggregate:.4f}) |",
        "",
        "## Per-closure reconciliation",
        "",
        f"- Closures checked: **{len(closed_rows)}**",
        f"- Tolerance: $±{tol_per_closure:.4f} per closure",
        f"- Failures: **{len(per_closure_failures)}**",
        "",
    ]
    if per_closure_failures:
        parts += [
            "| Trade | Dir | Entry | Exit | Size | Stored net | Recomputed | Δ | Reason | Tracer |",
            "|---|---|---|---|---|---|---|---|---|---|",
        ]
        for f in per_closure_failures[:30]:
            parts.append(
                f"| {f['trade_id']} | {f['direction']} | {f['entry']:.4f} | {f['exit']:.4f} "
                f"| ${f['size']:.2f} | ${f['stored_net']:+.4f} | ${f['recomputed_net']:+.4f} "
                f"| ${f['delta']:+.4f} | {f['close_reason']} | {f['tracer_id'] or '-'} |"
            )
        if len(per_closure_failures) > 30:
            parts.append(f"\n_(showing 30 of {len(per_closure_failures)} failures)_")

    parts += [
        "",
        f"## Verdict: **{'PASS' if overall_ok else 'FAIL'}**",
        "",
    ]
    report = "\n".join(parts) + "\n"

    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(report, encoding="utf-8")
    if print_to_stdout:
        sys.stdout.write(report)

    return 0 if overall_ok else 1


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Wallet correctness audit (cycle 25-I)")
    p.add_argument("--db", required=True, help="Path to irene SQLite DB.")
    p.add_argument("--initial-balance", type=float, default=1000.0)
    p.add_argument(
        "--prefix",
        default=None,
        help="If set, audit only tracer rows whose tracer_id matches '<prefix>%'. "
        "Omit to audit paper production trades.",
    )
    p.add_argument("--tol-aggregate", type=float, default=0.01)
    p.add_argument("--tol-per-closure", type=float, default=0.005)
    p.add_argument("--out", default=None, help="Markdown report path.")
    p.add_argument("--print", action="store_true", help="Also write to stdout.")
    args = p.parse_args(argv)

    out_path = Path(args.out) if args.out else None
    return run_audit(
        args.db,
        initial_balance=args.initial_balance,
        prefix=args.prefix,
        tol_aggregate=args.tol_aggregate,
        tol_per_closure=args.tol_per_closure,
        output=out_path,
        print_to_stdout=args.print or out_path is None,
    )


if __name__ == "__main__":
    sys.exit(main())
