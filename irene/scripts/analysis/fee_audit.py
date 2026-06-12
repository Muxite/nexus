"""Cycle 27 fee-model audit (read-only).

Quantifies what the pre-cycle-27 wallet fee model invented, and validates
the cycle-27 replacement against the live CLOB:

  (a) distribution of stored `markets.fee_rate` (per-market CLOB
      `taker_base_fee`, the weekly-refreshed source of truth);
  (b) optional spot-diff of ~N markets against a fresh
      `GET /markets/<conditionId>` (`--probe N`);
  (c) recompute of every historical non-tracer closure's fees under the
      old model (parabolic p*(1-p), category table, default 0.10) vs the
      new model (min(p,1-p), per-market rate else 0.0), reporting mean
      fee/size for both;
  (d) recompute of NO-trade gross PnL under the position-space formula
      vs the stored realized numbers — quantifies the pre-cycle-27
      NO price-space bug (closer.py spec block).

Usage:
    python3 scripts/analysis/fee_audit.py [--db irene/data/irene.db] [--probe 50]

Read-only: opens the DB with mode=ro; never writes.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
from collections import Counter

# Pre-cycle-27 constants, reproduced here so the audit doesn't depend on
# the (now-deleted) category fee table in app/config.py.
_OLD_CATEGORY_RATES = {
    "crypto": 0.072,
    "culture": 0.05, "economics": 0.05, "weather": 0.05, "general": 0.05,
    "finance": 0.04, "politics": 0.04, "tech": 0.04, "mentions": 0.04,
    "sports": 0.03,
    "geopolitics": 0.00,
}
_OLD_DEFAULT_RATE = 0.10
_NEW_DEFAULT_RATE = 0.0


def _old_rate(category, market_fee_rate):
    if market_fee_rate is not None and 0.0 <= market_fee_rate <= 1.0:
        return market_fee_rate
    if category:
        key = str(category).strip().lower()
        if key in _OLD_CATEGORY_RATES:
            return _OLD_CATEGORY_RATES[key]
        for token, rate in _OLD_CATEGORY_RATES.items():
            if token in key:
                return rate
    return _OLD_DEFAULT_RATE


def _new_rate(market_fee_rate):
    if market_fee_rate is not None and 0.0 <= market_fee_rate <= 1.0:
        return market_fee_rate
    return _NEW_DEFAULT_RATE


def _old_fee(price, shares, rate):
    p = max(0.0, min(1.0, price))
    return shares * rate * p * (1.0 - p)


def _new_fee(price, shares, rate):
    p = max(0.0, min(1.0, price))
    return shares * rate * min(p, 1.0 - p)


def _position_prices(direction, entry_mid, exit_mid):
    """OLD model converted prices assuming YES-space mids; the data feed
    stored position-space (traded-token) mids. Reproduce the old
    (wrong) conversion for the old-model recompute; the new model uses
    the stored prices as-is."""
    if direction == "YES":
        return entry_mid, exit_mid
    return 1.0 - entry_mid, 1.0 - exit_mid


def section_a(conn) -> None:
    print("\n== (a) stored markets.fee_rate distribution ==")
    rows = conn.execute(
        "SELECT fee_rate, COUNT(*) FROM markets GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()
    total = sum(r[1] for r in rows)
    for rate, n in rows[:12]:
        label = "NULL (never fetched)" if rate is None else f"{rate:.4f}"
        print(f"  fee_rate={label:<22} n={n:>7} ({100.0 * n / total:.1f}%)")
    nonnull = conn.execute(
        "SELECT COUNT(*), AVG(fee_rate) FROM markets WHERE fee_rate IS NOT NULL"
    ).fetchone()
    print(f"  fetched rows: {nonnull[0]} (mean rate {nonnull[1] or 0:.4f})")
    print(
        "  NOTE on the cycle-15 '82% at 1000 bps' survey: taker_base_fee=1000"
        " bps is the schedule CEILING parameter, applied as"
        " rate*min(p,1-p) per share — NOT a flat 10% of notional. The old"
        " parabolic p*(1-p) reading plus the 0.10 default is what inflated"
        " mean fee/size to ~5.7% (retro 2026-06-12 §3)."
    )


def _has_column(conn, table: str, column: str) -> bool:
    return any(
        r[1] == column for r in conn.execute(f"PRAGMA table_info({table})")
    )


def section_c_d(conn) -> None:
    print("\n== (c) historical closures: old vs new fee model ==")
    # The cycle-27 fill columns land at container boot; a pre-migration
    # DB snapshot simply has no fills (every row uses the mid fallback).
    entry_fill_col = (
        "t.entry_fill_price" if _has_column(conn, "trades", "entry_fill_price")
        else "NULL"
    )
    exit_fill_col = (
        "c.exit_fill_price" if _has_column(conn, "trade_closures", "exit_fill_price")
        else "NULL"
    )
    rows = conn.execute(
        f"""
        SELECT t.direction, t.mid_price, t.size_usdc, {entry_fill_col},
               c.exit_mid_price, {exit_fill_col}, c.close_reason,
               c.entry_fee_usdc, c.exit_fee_usdc, c.realized_pnl_usdc,
               m.category, m.fee_rate
        FROM trade_closures c
        JOIN trades t ON t.id = c.trade_id
        LEFT JOIN markets m ON m.condition_id = t.market_id
        WHERE t.is_tracer = 0
        """
    ).fetchall()
    if not rows:
        print("  no closures found.")
        return
    n = 0
    old_fee_sum = new_fee_sum = size_sum = stored_fee_sum = 0.0
    no_stored_gross = no_fixed_gross = 0.0
    n_no = 0
    sign_flips = 0
    for r in rows:
        (direction, entry_mid, size, entry_fill, exit_mid, exit_fill,
         reason, e_fee, x_fee, pnl, category, mkt_rate) = r
        entry_mid = float(entry_mid); exit_mid = float(exit_mid); size = float(size)
        if size <= 0:
            continue
        n += 1
        size_sum += size
        stored_fee_sum += float(e_fee or 0.0) + float(x_fee or 0.0)
        # --- old model (reproduced) ---
        op_entry, op_exit = _position_prices(direction, entry_mid, exit_mid)
        rate_old = _old_rate(category, mkt_rate)
        if op_entry > 0:
            shares_old = size / op_entry
            f = _old_fee(op_entry, shares_old, rate_old)
            if reason not in ("market_resolved", "market_dead_unresolved"):
                f += _old_fee(op_exit, shares_old, rate_old)
            old_fee_sum += f
        # --- new model (position space, fills when present) ---
        entry_p = float(entry_fill) if entry_fill is not None else entry_mid
        exit_p = float(exit_fill) if exit_fill is not None else exit_mid
        rate_new = _new_rate(mkt_rate)
        if entry_p > 0:
            shares_new = size / entry_p
            f = _new_fee(entry_p, shares_new, rate_new)
            if reason not in ("market_resolved", "market_dead_unresolved"):
                f += _new_fee(exit_p, shares_new, rate_new)
            new_fee_sum += f
        # --- (d) NO gross PnL: stored convention vs position-space ---
        if direction == "NO":
            n_no += 1
            denom = 1.0 - entry_mid
            stored_gross = size * (entry_mid - exit_mid) / denom if denom > 0 else 0.0
            fixed_gross = size * (exit_mid - entry_mid) / entry_mid if entry_mid > 0 else 0.0
            no_stored_gross += stored_gross
            no_fixed_gross += fixed_gross
            if stored_gross * fixed_gross < 0:
                sign_flips += 1
    print(f"  closures: {n}  total size ${size_sum:,.0f}")
    print(f"  fees as charged (stored):  ${stored_fee_sum:,.2f}"
          f"  = {100.0 * stored_fee_sum / size_sum:.2f}% of size")
    print(f"  old model (reproduced):    ${old_fee_sum:,.2f}"
          f"  = {100.0 * old_fee_sum / size_sum:.2f}% of size")
    print(f"  new model (cycle 27):      ${new_fee_sum:,.2f}"
          f"  = {100.0 * new_fee_sum / size_sum:.2f}% of size")
    print("\n== (d) NO-trade gross PnL: stored YES-space formula vs position space ==")
    print(f"  NO closures: {n_no}  (gross sign flipped on {sign_flips})")
    print(f"  gross under stored formula:        ${no_stored_gross:,.2f}")
    print(f"  gross under position-space formula: ${no_fixed_gross:,.2f}")
    print("  (position space is what the data feed actually recorded — see"
          " closer.py spec block, verified 2026-06-12 against"
          " features_snapshot.edge_components.)")


async def section_b(conn, probe_n: int) -> None:
    print(f"\n== (b) live spot-diff of {probe_n} markets vs CLOB ==")
    import sys
    sys.path.insert(0, ".")
    from app.clob import CLOBClient

    rows = conn.execute(
        """
        SELECT condition_id, fee_rate FROM markets
        WHERE active = 1 AND fee_rate IS NOT NULL
        ORDER BY volume_usdc DESC LIMIT ?
        """,
        (probe_n,),
    ).fetchall()
    client = CLOBClient()
    same = diff = miss = 0
    try:
        for cid, stored in rows:
            live = await client.get_market_fee_rate(cid)
            if live is None:
                miss += 1
            elif abs(float(live) - float(stored)) < 1e-9:
                same += 1
            else:
                diff += 1
                print(f"  DIFF {cid}: stored={stored} live={live}")
    finally:
        await client.close()
    print(f"  match={same} diff={diff} fetch-miss={miss} / {len(rows)} probed")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default="data/irene.db")
    ap.add_argument("--probe", type=int, default=0,
                    help="spot-check N markets against the live CLOB (network)")
    args = ap.parse_args()

    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    try:
        section_a(conn)
        section_c_d(conn)
        if args.probe > 0:
            asyncio.run(section_b(conn, args.probe))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
