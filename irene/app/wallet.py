"""## Specification

**Purpose.** Pure accounting helpers for the paper-trading wallet. Every
number on the dashboard (cash / equity / realized + unrealized PnL) comes
from one of these functions. Stateless — derives everything from
`trades` + `trade_closures` rows + an `initial_balance` constant. No
new tables; the wallet is a view, not a writer.

**The accounting identity.** For any point in time:
```
equity == initial_balance + realized_pnl + unrealized_pnl
```
`assert_accounting_identity()` is the gate that ensures we never silently
break this. Dashboard panels call it on every tick; tests assert it on
seeded scenarios.

**Real-money cash model (cycle 25).** `cash_balance` deducts cumulative
fees (`entry_fee_usdc + exit_fee_usdc` per closure) so the wallet behaves
the way a live exchange would: fees actually leave the cash bucket on
close. `realized_pnl_usdc` is already net of fees (per `closer.py`), so
once cash is also net the identity holds exactly — no DRIFT line.

**The Polymarket position math** (kept in sync with `closer._compute_pnl`;
cycle 27 — all prices are **position space**, the traded token's own:
`trades.mid_price` and the dashboard's current-mid poll both read the
token the bet actually holds, for BOTH directions):
- A bet of `size_usdc` at `entry_mid` buys `shares = size_usdc / entry_mid`.
  Current value = `current_mid * shares = size_usdc * current_mid / entry_mid`.
  Unrealized PnL = `size_usdc * (current_mid - entry_mid) / entry_mid`.
- The pre-cycle-27 NO branch (`(1 - current) / (1 - entry)`) assumed
  YES-space mids the data feed never supplied — NO positions were
  marked with the wrong sign. See closer.py's spec block.

**Public interface.**
- `position_value(direction, entry_mid, current_mid, size_usdc) -> float`
- `unrealized_pnl(direction, entry_mid, current_mid, size_usdc) -> float`
- `cash_balance(initial_balance, open_size_invested, closed_exit_value_total) -> float`
- `equity(cash, open_positions_value_total) -> float`
- `accounting_identity_check(initial, realized, unrealized, equity, tol=0.01) -> bool`

**Non-goals.** Database I/O (callers pass in row sequences). Logging.
Async — these are sync pure functions called from async dashboard pages.
"""

from __future__ import annotations

from typing import Iterable, Optional


def position_value(
    direction: str,
    entry_mid: float,
    current_mid: float,
    size_usdc: float,
) -> float:
    """Return the *current* USDC value of an open position.

    Both prices are position space (the traded token's own — cycle 27),
    so the formula is direction-independent:
    `size_usdc * current_mid / entry_mid`. `direction` stays in the
    signature for call-site stability and to keep rejecting unknown
    values loudly.

    Degenerate denominator (entry_mid == 0) returns `size_usdc` — the
    position can't be valued, so we conservatively treat it as worth
    its cost basis.
    """
    if direction not in ("YES", "NO"):
        raise ValueError(f"unknown direction: {direction!r}")
    if entry_mid <= 0.0:
        return float(size_usdc)
    return float(size_usdc) * float(current_mid) / float(entry_mid)


def unrealized_pnl(
    direction: str,
    entry_mid: float,
    current_mid: float,
    size_usdc: float,
) -> float:
    """Unrealized PnL = current position value - cost basis."""
    return position_value(direction, entry_mid, current_mid, size_usdc) - float(size_usdc)


def exit_value(
    direction: str,
    entry_mid: float,
    exit_mid: float,
    size_usdc: float,
) -> float:
    """USDC returned to the wallet when a position closes.
    Equivalent to `position_value` evaluated at the exit mid.
    """
    return position_value(direction, entry_mid, exit_mid, size_usdc)


def cash_balance(
    initial_balance: float,
    open_size_invested: float,
    closed_exit_value_total: float,
    closed_size_invested: float,
    fees_paid_total: float = 0.0,
) -> float:
    """Cash is what's NOT currently locked in positions or paid out.

    Step through the lifecycle:
      - At init: cash = initial.
      - Trade opens (size=S): cash -= S (locked into position).
      - Trade closes (exit=V, fees=F): cash += V - F.

    So: cash = initial - sum(sizes opened) + sum(exit values) - sum(fees).
              = initial - (open_size + closed_size) + closed_exit_value - fees

    Cycle 25: `fees_paid_total` defaults to 0 to keep legacy callers
    working, but the dashboard passes `SUM(entry_fee + exit_fee)` from
    `trade_closures` so the wallet matches what a real exchange would
    show. With this subtraction, `equity = initial + realized_net +
    unrealized` holds exactly — no separate DRIFT bucket needed.
    """
    return (
        float(initial_balance)
        - float(open_size_invested)
        - float(closed_size_invested)
        + float(closed_exit_value_total)
        - float(fees_paid_total)
    )


def equity(cash: float, open_positions_value_total: float) -> float:
    """Total wallet equity = cash + mark-to-market value of open positions."""
    return float(cash) + float(open_positions_value_total)


def accounting_identity_check(
    initial_balance: float,
    realized_pnl: float,
    unrealized_pnl: float,
    equity_value: float,
    *,
    tol: float = 0.01,
) -> tuple[bool, float]:
    """Returns (ok, drift). `ok` is True when the identity holds within
    tolerance. `drift` is the signed mismatch (equity - expected) so the
    dashboard can show "how off are we" when it does break.
    """
    expected = float(initial_balance) + float(realized_pnl) + float(unrealized_pnl)
    drift = float(equity_value) - expected
    return abs(drift) <= float(tol), drift


# Convenience aggregators that take row sequences and produce the totals
# needed by the dashboard. Sequences may be dict-like or sqlite3.Row.

def sum_open_size_invested(open_trades: Iterable) -> float:
    return float(sum(float(t["size_usdc"]) for t in open_trades))


def sum_closed_size_invested(closed_trades: Iterable) -> float:
    return float(sum(float(t["size_usdc"]) for t in closed_trades))


def sum_closed_exit_value(closed_pairs: Iterable) -> float:
    """`closed_pairs` is an iterable of dicts containing
    direction/entry_mid/size_usdc/exit_mid_price (one per closed trade),
    plus optionally entry_fill_price/exit_fill_price (cycle 27 rows).

    Fill prices take precedence over mids when present — realized PnL is
    fills-based on cycle-27 closures, and the accounting identity only
    holds when the cash-side exit value is priced off the same legs.
    """
    total = 0.0
    for r in closed_pairs:
        entry = _row_get(r, "entry_fill_price")
        entry = float(entry) if entry is not None else float(r["entry_mid"])
        exit_p = _row_get(r, "exit_fill_price")
        exit_p = float(exit_p) if exit_p is not None else float(r["exit_mid_price"])
        total += exit_value(r["direction"], entry, exit_p, float(r["size_usdc"]))
    return total


def sum_realized_pnl(closures: Iterable) -> float:
    return float(sum(float(c["realized_pnl_usdc"]) for c in closures))


def _row_get(row, key: str):
    """Best-effort field lookup that works on both dicts and sqlite3.Row.

    dict.get(key) returns None on miss; sqlite3.Row only supports __getitem__
    and raises IndexError on miss. We wrap both so callers don't need to
    care about row provenance.
    """
    try:
        return row[key]
    except (KeyError, IndexError):
        return None


def sum_fees_paid(closures: Iterable) -> float:
    """Cumulative Polymarket taker fees deducted across all closures.

    Each closure row carries `entry_fee_usdc` + `exit_fee_usdc` (cycle 7).
    Missing or NULL columns are treated as 0 so we don't crash on legacy
    rows that pre-date the columns. The dashboard subtracts this total
    from cash so the wallet matches a real exchange's books.
    """
    total = 0.0
    for c in closures:
        entry = _row_get(c, "entry_fee_usdc")
        exit_ = _row_get(c, "exit_fee_usdc")
        total += float(entry or 0.0) + float(exit_ or 0.0)
    return total


def _open_row_entry_basis(row) -> float:
    """Cost basis of an open position: the entry fill (cycle 27 rows)
    or the legacy entry mid. Keeps unrealized PnL consistent with the
    fills-based realized PnL the closer will eventually write."""
    fill = _row_get(row, "entry_fill_price")
    return float(fill) if fill is not None else float(row["entry_mid"])


def sum_unrealized_pnl(open_with_mid: Iterable) -> float:
    """Each row needs direction/entry_mid/current_mid/size_usdc (plus
    optionally entry_fill_price). Rows where `current_mid` is None
    (CLOB miss) fall back to the entry basis so unrealized PnL is 0
    for that position rather than skipping it."""
    total = 0.0
    for r in open_with_mid:
        cur: Optional[float] = r["current_mid"]
        entry = _open_row_entry_basis(r)
        if cur is None:
            cur = entry
        total += unrealized_pnl(r["direction"], entry, float(cur), float(r["size_usdc"]))
    return total


def sum_open_positions_value(open_with_mid: Iterable) -> float:
    total = 0.0
    for r in open_with_mid:
        cur: Optional[float] = r["current_mid"]
        entry = _open_row_entry_basis(r)
        if cur is None:
            cur = entry
        total += position_value(r["direction"], entry, float(cur), float(r["size_usdc"]))
    return total
