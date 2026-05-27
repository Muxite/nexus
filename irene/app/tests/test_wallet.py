"""Wallet accounting tests — verify the identity holds across
representative scenarios: open YES, open NO, closed winner, closed loser,
mixed open + closed, CLOB-miss fallback."""

from __future__ import annotations

import pytest

from app import wallet


# ---------------------------------------------------------------------------
# Pure formula
# ---------------------------------------------------------------------------


def test_position_value_yes_winner():
    # $10 YES @ 0.40 → 25 shares. Mid moves to 0.60 → value = 25 * 0.60 = $15.
    v = wallet.position_value("YES", entry_mid=0.40, current_mid=0.60, size_usdc=10.0)
    assert v == pytest.approx(15.0)


def test_position_value_yes_loser():
    v = wallet.position_value("YES", entry_mid=0.40, current_mid=0.20, size_usdc=10.0)
    assert v == pytest.approx(5.0)


def test_position_value_no_winner():
    # $10 NO @ 0.40 (i.e. NO-shares cost 0.60 each → 16.667 NO-shares).
    # Mid drops to 0.20 → NO-shares worth 0.80 each. Value = 10 * (1-0.20)/(1-0.40)
    #                                                       = 10 * 0.80 / 0.60 = $13.333.
    v = wallet.position_value("NO", entry_mid=0.40, current_mid=0.20, size_usdc=10.0)
    assert v == pytest.approx(10.0 * 0.80 / 0.60, abs=1e-6)


def test_position_value_unknown_direction_raises():
    with pytest.raises(ValueError):
        wallet.position_value("MAYBE", 0.4, 0.5, 10.0)


def test_position_value_degenerate_returns_cost():
    # YES @ entry_mid=0 (can't compute) → returns size as fallback.
    assert wallet.position_value("YES", 0.0, 0.5, 10.0) == 10.0
    assert wallet.position_value("NO", 1.0, 0.5, 10.0) == 10.0


def test_unrealized_pnl_is_value_minus_cost():
    pv = wallet.position_value("YES", 0.4, 0.6, 10.0)
    upnl = wallet.unrealized_pnl("YES", 0.4, 0.6, 10.0)
    assert upnl == pytest.approx(pv - 10.0)


# ---------------------------------------------------------------------------
# Accounting identity end-to-end
# ---------------------------------------------------------------------------


def _identity_holds(initial, realized, unrealized, equity):
    ok, drift = wallet.accounting_identity_check(initial, realized, unrealized, equity)
    assert ok, f"accounting identity broken; drift={drift}"
    return drift


def test_identity_holds_empty_wallet():
    cash = wallet.cash_balance(initial_balance=1000.0, open_size_invested=0.0,
                               closed_exit_value_total=0.0, closed_size_invested=0.0)
    eq = wallet.equity(cash, 0.0)
    _identity_holds(1000.0, 0.0, 0.0, eq)
    assert eq == pytest.approx(1000.0)


def test_identity_holds_one_open_yes_winner():
    # $10 YES @ 0.40, current 0.60 → unrealized = +$5, cash = $990, equity = $1005.
    open_trades = [{"size_usdc": 10.0}]
    open_with_mid = [
        {"direction": "YES", "entry_mid": 0.40, "current_mid": 0.60, "size_usdc": 10.0}
    ]
    open_size = wallet.sum_open_size_invested(open_trades)
    cash = wallet.cash_balance(1000.0, open_size, 0.0, 0.0)
    upnl = wallet.sum_unrealized_pnl(open_with_mid)
    open_val = wallet.sum_open_positions_value(open_with_mid)
    eq = wallet.equity(cash, open_val)
    assert cash == pytest.approx(990.0)
    assert upnl == pytest.approx(5.0)
    assert eq == pytest.approx(1005.0)
    _identity_holds(1000.0, 0.0, upnl, eq)


def test_identity_holds_one_closed_no_loser():
    # $10 NO @ 0.40, exit 0.50 → exit value = 10 * (1-0.50)/(1-0.40) = $8.333
    # → realized = -$1.667, cash = 990 + 8.333 = $998.333, equity = $998.333.
    # No fees: identity holds with fees_paid_total=0.
    closed_trades = [{"size_usdc": 10.0}]
    closed_pairs = [
        {"direction": "NO", "entry_mid": 0.40, "exit_mid_price": 0.50, "size_usdc": 10.0}
    ]
    closures = [{"realized_pnl_usdc": 10.0 * (0.40 - 0.50) / (1.0 - 0.40),
                 "entry_fee_usdc": 0.0, "exit_fee_usdc": 0.0}]
    closed_size = wallet.sum_closed_size_invested(closed_trades)
    closed_exit_value = wallet.sum_closed_exit_value(closed_pairs)
    realized = wallet.sum_realized_pnl(closures)
    fees = wallet.sum_fees_paid(closures)
    cash = wallet.cash_balance(1000.0, 0.0, closed_exit_value, closed_size,
                               fees_paid_total=fees)
    eq = wallet.equity(cash, 0.0)
    assert cash == pytest.approx(1000.0 - 10.0 + 10.0 * 0.50 / 0.60, abs=1e-4)
    assert realized == pytest.approx(-10.0 * 0.10 / 0.60, abs=1e-6)
    _identity_holds(1000.0, realized, 0.0, eq)


def test_identity_holds_mixed_open_and_closed():
    # Open: $20 YES @ 0.50, current 0.55 → upnl = 20*(0.55-0.50)/0.50 = $2
    # Closed: $15 NO @ 0.30, exit 0.20  → realized_gross = 15*(0.30-0.20)/(1-0.30) = $2.143
    # Fees on the closure: entry $0.05 + exit $0.04 = $0.09 → realized_net = $2.053
    open_trades = [{"size_usdc": 20.0}]
    open_with_mid = [
        {"direction": "YES", "entry_mid": 0.50, "current_mid": 0.55, "size_usdc": 20.0}
    ]
    closed_trades = [{"size_usdc": 15.0}]
    closed_pairs = [
        {"direction": "NO", "entry_mid": 0.30, "exit_mid_price": 0.20, "size_usdc": 15.0}
    ]
    realized_gross = 15.0 * 0.10 / 0.70
    closures = [{
        "realized_pnl_usdc": realized_gross - 0.09,
        "entry_fee_usdc": 0.05,
        "exit_fee_usdc": 0.04,
    }]

    cash = wallet.cash_balance(
        initial_balance=1000.0,
        open_size_invested=wallet.sum_open_size_invested(open_trades),
        closed_exit_value_total=wallet.sum_closed_exit_value(closed_pairs),
        closed_size_invested=wallet.sum_closed_size_invested(closed_trades),
        fees_paid_total=wallet.sum_fees_paid(closures),
    )
    open_val = wallet.sum_open_positions_value(open_with_mid)
    upnl = wallet.sum_unrealized_pnl(open_with_mid)
    realized = wallet.sum_realized_pnl(closures)
    eq = wallet.equity(cash, open_val)

    drift = _identity_holds(1000.0, realized, upnl, eq)
    assert abs(drift) < 0.001
    # Fees aggregator covers both columns.
    assert wallet.sum_fees_paid(closures) == pytest.approx(0.09)


def test_clob_miss_falls_back_to_entry_mid_no_unrealized():
    # When current_mid is None, the helper treats it as entry_mid → upnl = 0.
    open_with_mid = [
        {"direction": "YES", "entry_mid": 0.40, "current_mid": None, "size_usdc": 10.0},
        {"direction": "NO",  "entry_mid": 0.65, "current_mid": None, "size_usdc": 5.0},
    ]
    assert wallet.sum_unrealized_pnl(open_with_mid) == pytest.approx(0.0)
    # Position value falls back to size_usdc when current_mid == entry_mid... almost.
    # For YES: size * entry/entry = size = 10
    # For NO: size * (1-entry)/(1-entry) = size = 5
    assert wallet.sum_open_positions_value(open_with_mid) == pytest.approx(15.0)


def test_accounting_identity_tolerates_small_float_drift():
    # equity = 1005.0001, expected = 1005.0 → drift 0.0001 should be OK.
    ok, drift = wallet.accounting_identity_check(1000.0, 0.0, 5.0, 1005.0001, tol=0.01)
    assert ok
    assert drift == pytest.approx(0.0001, abs=1e-9)


def test_accounting_identity_catches_real_drift():
    ok, drift = wallet.accounting_identity_check(1000.0, 0.0, 5.0, 1010.0, tol=0.01)
    assert not ok
    assert drift == pytest.approx(5.0)


# ---------------------------------------------------------------------------
# Cycle 25 — real-money fees model
# ---------------------------------------------------------------------------


def test_sum_fees_paid_handles_missing_columns():
    # Legacy rows without the fee columns should not crash the aggregator.
    closures = [
        {"realized_pnl_usdc": 1.0},  # no fee columns at all
        {"realized_pnl_usdc": 2.0, "entry_fee_usdc": None, "exit_fee_usdc": None},
        {"realized_pnl_usdc": 3.0, "entry_fee_usdc": 0.10, "exit_fee_usdc": 0.05},
    ]
    assert wallet.sum_fees_paid(closures) == pytest.approx(0.15)


def test_cash_real_money_fees_deducted():
    # $100 YES @ 0.50, exit 0.60 → gross_pnl = $20, exit_value = $120.
    # Fees: $0.50 + $0.60 = $1.10.
    # Cash should be: 1000 - 100 + 120 - 1.10 = $1018.90.
    closed_trades = [{"size_usdc": 100.0}]
    closed_pairs = [{
        "direction": "YES", "entry_mid": 0.50,
        "exit_mid_price": 0.60, "size_usdc": 100.0,
    }]
    closures = [{
        "realized_pnl_usdc": 20.0 - 1.10,  # gross − fees
        "entry_fee_usdc": 0.50,
        "exit_fee_usdc": 0.60,
    }]
    cash = wallet.cash_balance(
        initial_balance=1000.0,
        open_size_invested=0.0,
        closed_exit_value_total=wallet.sum_closed_exit_value(closed_pairs),
        closed_size_invested=wallet.sum_closed_size_invested(closed_trades),
        fees_paid_total=wallet.sum_fees_paid(closures),
    )
    realized = wallet.sum_realized_pnl(closures)
    eq = wallet.equity(cash, 0.0)
    assert cash == pytest.approx(1018.90, abs=1e-6)
    assert realized == pytest.approx(18.90, abs=1e-6)
    # Identity is EXACT now (no fee bucket sitting in cash).
    ok, drift = wallet.accounting_identity_check(1000.0, realized, 0.0, eq, tol=1e-6)
    assert ok, f"identity should be exact under real-money cash; drift={drift}"


def test_cycle35_audit_replayed_identity_is_exact_after_cycle25():
    # The cycle-35 audit found:
    #   initial = $1000, realized_net = -$79.88, fees_paid = $47.22,
    #   open_positions valued at $112.64 (entry mid), unrealized = 0,
    #   pre-cycle-25 cash (gross) = $967.34 → drift = +$47.22.
    # Under cycle-25 real-money cash, cash drops by exactly $47.22 to
    # $920.12 and the identity is exact.
    closures = [
        {"realized_pnl_usdc": -79.88,
         "entry_fee_usdc": 27.22, "exit_fee_usdc": 20.00},  # totals add to $47.22
    ]
    # 1 open position carrying $112.64 invested at entry mid (unpriceable
    # in the audit; the wallet treats current_mid=None as entry_mid so
    # unrealized=0 and position_value=size).
    open_with_mid = [{
        "direction": "NO", "entry_mid": 0.455,
        "current_mid": None, "size_usdc": 112.64,
    }]
    closed_size = 2880.79  # invested across the 20 closures
    closed_exit_value = 2880.79 - 79.88 + 47.22  # gross exit = invested + gross_pnl
    cash = wallet.cash_balance(
        initial_balance=1000.0,
        open_size_invested=112.64,
        closed_exit_value_total=closed_exit_value,
        closed_size_invested=closed_size,
        fees_paid_total=wallet.sum_fees_paid(closures),
    )
    upnl = wallet.sum_unrealized_pnl(open_with_mid)
    open_val = wallet.sum_open_positions_value(open_with_mid)
    realized = wallet.sum_realized_pnl(closures)
    eq = wallet.equity(cash, open_val)
    # Open value = size (unpriceable fallback). Equity = cash + open_val.
    # Pre-cycle-25: cash $854.70 (gross), equity $967.34, expected $920.12, drift +$47.22.
    # Post-cycle-25: cash $854.70 - fees $47.22 = $807.48, equity $920.12, identity exact.
    assert open_val == pytest.approx(112.64, abs=1e-6)
    assert upnl == pytest.approx(0.0, abs=1e-6)
    assert cash == pytest.approx(807.48, abs=0.02)
    assert eq == pytest.approx(920.12, abs=0.02)
    ok, drift = wallet.accounting_identity_check(1000.0, realized, upnl, eq, tol=0.02)
    assert ok, f"cycle-35 replayed identity should hold; drift={drift}"
