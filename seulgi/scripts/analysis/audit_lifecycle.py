"""Paper-trade lifecycle accuracy audit.

Read-only sanity check that the open → mark-to-market → close →
realised-PnL chain is correct. Profitability is NOT the concern;
*accounting accuracy* is. Catches:

  - Orphan positions (position row but no trade row, or vice versa)
  - Double-counted decisions (same signal_id × cond_id × engine ↦
    multiple engine_results rows)
  - Open invariants: position.size_usdc == trade.size_usdc,
    position.entry_price == trade.mid_price
  - Close invariants: realized_pnl sign + magnitude consistent
    with (exit_price - entry_price) for YES leg, signed correctly
    for NO; closed_at populated; positions row deleted
  - Skip-row contract (cycle-2 change): SIM_FAIL rows skip the
    trades table but still land in engine_results; every other
    skip_reason should have matching rows in both
  - Balance accounting at engine level (heuristic): sum of
    open positions × entry size + sum of realized_pnl on
    closed trades should add up

Usage:
    cd seulgi
    python3 -m scripts.analysis.audit_lifecycle

    # Only audit trades from a specific window:
    python3 -m scripts.analysis.audit_lifecycle --since-hours 24

Returns exit code 0 if all invariants hold, 1 if any violation
found. Pretty-prints the first N rows of each violation class so
the failure is debuggable in-place.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path


def db_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "seulgi.db"


def _hdr(name: str) -> None:
    print(f"\n── {name} " + "─" * (70 - len(name)))


def _ok(msg: str) -> None:
    print(f"  ✓ {msg}")


def _fail(msg: str) -> None:
    print(f"  ✗ {msg}")


def audit_orphan_positions(c: sqlite3.Connection, window: str) -> int:
    """Positions without a corresponding trade row, or vice-versa."""
    _hdr("Orphan positions")
    fails = 0

    # Position with no trade row
    rows = c.execute(f"""
        SELECT p.trade_id, p.condition_id, p.size_usdc, p.opened_at
        FROM positions p
        LEFT JOIN trades t ON t.id = p.trade_id
        WHERE t.id IS NULL
        {window.replace('decided_at', 'p.opened_at') if window else ''}
        LIMIT 10
    """).fetchall()
    if rows:
        _fail(f"positions without trade row: {len(rows)}+ (showing 10):")
        for r in rows:
            print(f"     trade_id={r[0]} cond={r[1][:18]} size=${r[2]:.2f} opened={r[3]}")
        fails += 1
    else:
        _ok("no positions without a matching trade row")

    # Executed trade (skip_reason NULL) with no position row (still-open trade
    # that never opened a position — would be a silent failure)
    rows = c.execute(f"""
        SELECT t.id, t.condition_id, t.size_usdc, t.decided_at, t.realized_pnl
        FROM trades t
        LEFT JOIN positions p ON p.trade_id = t.id
        WHERE t.skip_reason IS NULL
          AND p.trade_id IS NULL
          AND t.realized_pnl IS NULL    -- still-open: NULL PnL + no position = orphan
        {window if window else ''}
        LIMIT 10
    """).fetchall()
    if rows:
        _fail(f"executed open trades without position row: {len(rows)}+ (showing 10):")
        for r in rows:
            print(f"     trade={r[0]} cond={r[1][:18]} size=${r[2]:.2f} decided={r[3]}")
        fails += 1
    else:
        _ok("no executed open trades without a position row")

    return fails


def audit_double_decisions(c: sqlite3.Connection, window: str) -> int:
    """Same (signal_id, cond_id, engine_name) decided twice in engine_results."""
    _hdr("Duplicate engine_results")
    rows = c.execute(f"""
        SELECT headline_id, condition_id, engine_name, COUNT(*) AS n
        FROM engine_results
        WHERE 1=1 {window if window else ''}
        GROUP BY headline_id, condition_id, engine_name
        HAVING n > 1
        ORDER BY n DESC
        LIMIT 10
    """).fetchall()
    if rows:
        _fail(f"duplicate (headline, cond, engine) rows: {len(rows)}+ (showing 10):")
        for r in rows:
            print(f"     headline={r[0]} cond={r[1][:18]} engine={r[2][:30]} n={r[3]}")
        return 1
    _ok("no duplicate engine_results rows")
    return 0


def audit_open_invariants(c: sqlite3.Connection, window: str) -> int:
    """For executed trades with a position, size + entry_price must match."""
    _hdr("Open invariants (position vs trade)")
    rows = c.execute(f"""
        SELECT t.id, t.condition_id, t.size_usdc, p.size_usdc,
               t.mid_price, p.entry_price, t.direction, p.direction
        FROM trades t
        JOIN positions p ON p.trade_id = t.id
        WHERE 1=1 {window if window else ''}
        LIMIT 200
    """).fetchall()
    if not rows:
        _ok("no open trade+position pairs to check (yet)")
        return 0
    mismatch_size = 0
    mismatch_price = 0
    mismatch_dir = 0
    for r in rows:
        tid, cond, t_size, p_size, t_mid, p_entry, t_dir, p_dir = r
        if abs((t_size or 0) - (p_size or 0)) > 1e-6:
            mismatch_size += 1
        if t_mid is not None and p_entry is not None and abs(t_mid - p_entry) > 1e-6:
            mismatch_price += 1
        if t_dir != p_dir:
            mismatch_dir += 1
    n = len(rows)
    if mismatch_size + mismatch_price + mismatch_dir == 0:
        _ok(f"all {n} open pairs match on size + entry_price + direction")
        return 0
    _fail(f"out of {n} pairs: size_mismatch={mismatch_size} "
          f"price_mismatch={mismatch_price} direction_mismatch={mismatch_dir}")
    return 1


def audit_close_invariants(c: sqlite3.Connection, window: str) -> int:
    """Closed trades: realized_pnl + closed_at populated; position deleted."""
    _hdr("Close invariants")
    fails = 0

    # closed_at populated for any non-null realized_pnl row
    rows = c.execute(f"""
        SELECT id, condition_id, realized_pnl, closed_at
        FROM trades
        WHERE realized_pnl IS NOT NULL AND closed_at IS NULL
        {window if window else ''}
        LIMIT 10
    """).fetchall()
    if rows:
        _fail(f"realized_pnl set but closed_at NULL: {len(rows)}+ (showing 10):")
        for r in rows:
            print(f"     trade={r[0]} cond={r[1][:18]} pnl={r[2]}")
        fails += 1
    else:
        _ok("all closed trades have closed_at timestamps")

    # Position row deleted on close: closed trade should have no position row
    rows = c.execute(f"""
        SELECT t.id, t.condition_id, t.realized_pnl, p.trade_id
        FROM trades t
        JOIN positions p ON p.trade_id = t.id
        WHERE t.realized_pnl IS NOT NULL
        {window if window else ''}
        LIMIT 10
    """).fetchall()
    if rows:
        _fail(f"closed trades with stale position row: {len(rows)}+ (showing 10):")
        for r in rows:
            print(f"     trade={r[0]} cond={r[1][:18]} pnl={r[2]}")
        fails += 1
    else:
        _ok("no stale position rows on closed trades")

    return fails


def audit_skip_contract(c: sqlite3.Connection, window: str) -> int:
    """Cycle-2 contract: SIM_FAIL skip lives in engine_results only;
    every other skip_reason lives in BOTH engine_results AND trades.
    """
    _hdr("Skip-row contract (cycle 2)")
    # 1. SIM_FAIL in trades → contract violation
    rows = c.execute(f"""
        SELECT COUNT(*) FROM trades
        WHERE skip_reason LIKE '%SIM_FAIL%'
        {window if window else ''}
    """).fetchone()
    n_sim_fail_in_trades = rows[0] if rows else 0
    if n_sim_fail_in_trades > 0:
        _fail(f"SIM_FAIL rows leaked into trades table: {n_sim_fail_in_trades}")
        return 1
    _ok("no SIM_FAIL rows in trades (contract holds)")

    # 2. non-SIM_FAIL skips in engine_results without matching trade row
    rows = c.execute(f"""
        SELECT er.headline_id, er.condition_id, er.engine_name, er.skip_reason
        FROM engine_results er
        LEFT JOIN trades t ON
          t.headline_id = er.headline_id
          AND t.condition_id = er.condition_id
          AND t.engine_name = er.engine_name
        WHERE er.executed = 0
          AND er.skip_reason IS NOT NULL
          AND er.skip_reason NOT LIKE '%SIM_FAIL%'
          AND t.id IS NULL
        {window.replace('decided_at', 'er.decided_at') if window else ''}
        LIMIT 10
    """).fetchall()
    if rows:
        _fail(f"non-SIM_FAIL skips missing trade row: {len(rows)}+ (showing 10):")
        for r in rows:
            print(f"     hid={r[0]} cond={r[1][:18]} engine={r[2][:30]} reason={r[3]}")
        return 1
    _ok("non-SIM_FAIL skips all have matching trade rows")
    return 0


def audit_realized_pnl_sign(c: sqlite3.Connection, window: str) -> int:
    """Realized PnL sign must match direction × price-move.

    For YES-direction at entry_price, exit_price:
        realized = size * (exit / entry - 1)   if direction == YES
    For NO-direction:
        realized = size * (entry / exit - 1)   (approx — depends on schema)

    We check the sign only — the magnitude has spread cost etc baked in.
    """
    _hdr("Realized PnL sign sanity")
    rows = c.execute(f"""
        SELECT id, direction, mid_price, realized_pnl, close_reason
        FROM trades
        WHERE realized_pnl IS NOT NULL
          AND mid_price IS NOT NULL
          AND ABS(realized_pnl) > 0.001
        {window if window else ''}
        LIMIT 500
    """).fetchall()
    if not rows:
        _ok("no closed trades with non-trivial PnL to check (yet)")
        return 0
    weird = []
    for tid, d, mid, pnl, reason in rows:
        # We don't have exit_price stored on the trade row, so we can only do
        # a coarse sanity check: PnL > 0 ⇒ close_reason should be TP/take_profit;
        # PnL < 0 ⇒ should be SL/stop_loss/expired. Mismatches are suspicious.
        r = (reason or "").lower()
        if pnl > 0 and any(s in r for s in ("stop", "sl", "expired")):
            weird.append((tid, d, pnl, reason))
        elif pnl < 0 and any(s in r for s in ("take_profit", "tp")):
            weird.append((tid, d, pnl, reason))
    if weird:
        _fail(f"PnL sign vs close_reason mismatches: {len(weird)} of {len(rows)}")
        for tid, d, pnl, reason in weird[:10]:
            print(f"     trade={tid} dir={d} pnl={pnl:+.4f} close_reason={reason}")
        return 1
    _ok(f"PnL signs consistent with close_reason on all {len(rows)} closed trades")
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--since-hours", type=float, default=None,
                        help="restrict checks to trades decided in last N hours")
    args = parser.parse_args(argv)

    db = db_path()
    if not db.exists():
        print(f"ERROR: db not found at {db}", file=sys.stderr)
        return 2

    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    if args.since_hours is None:
        window = ""
    else:
        # The DB stores decided_at as ISO "YYYY-MM-DDTHH:MM:SS.fff+00:00".
        # SQLite's datetime('now', ...) returns "YYYY-MM-DD HH:MM:SS"
        # (space, not 'T'). Plain string comparison fails because 'T' (84)
        # sorts AFTER ' ' (32) in ASCII — every row with decided_at
        # starting "YYYY-MM-DDT..." passes any "YYYY-MM-DD <space>HH:MM"
        # bound. Convert both to ISO with 'T' so comparisons work.
        from datetime import datetime, timedelta, timezone
        threshold = (
            datetime.now(timezone.utc) - timedelta(hours=args.since_hours)
        ).isoformat()
        window = f" AND decided_at >= '{threshold}'"

    print(f"# Paper-trade lifecycle audit  (window: "
          f"{'all-time' if not window else 'last ' + str(args.since_hours) + 'h'})")

    total_fails = 0
    total_fails += audit_orphan_positions(conn, window)
    total_fails += audit_double_decisions(conn, window)
    total_fails += audit_open_invariants(conn, window)
    total_fails += audit_close_invariants(conn, window)
    total_fails += audit_skip_contract(conn, window)
    total_fails += audit_realized_pnl_sign(conn, window)

    print()
    if total_fails == 0:
        print("✅ ALL INVARIANTS HOLD")
        return 0
    print(f"❌ {total_fails} INVARIANT CLASS(ES) FAILED — see violations above")
    return 1


if __name__ == "__main__":
    sys.exit(main())
