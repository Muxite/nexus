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
    closed_trades = [{"size_usdc": 10.0}]
    closed_pairs = [
        {"direction": "NO", "entry_mid": 0.40, "exit_mid_price": 0.50, "size_usdc": 10.0}
    ]
    closures = [{"realized_pnl_usdc": 10.0 * (0.40 - 0.50) / (1.0 - 0.40)}]
    closed_size = wallet.sum_closed_size_invested(closed_trades)
    closed_exit_value = wallet.sum_closed_exit_value(closed_pairs)
    realized = wallet.sum_realized_pnl(closures)
    cash = wallet.cash_balance(1000.0, 0.0, closed_exit_value, closed_size)
    eq = wallet.equity(cash, 0.0)
    assert cash == pytest.approx(1000.0 - 10.0 + 10.0 * 0.50 / 0.60, abs=1e-4)
    assert realized == pytest.approx(-10.0 * 0.10 / 0.60, abs=1e-6)
    _identity_holds(1000.0, realized, 0.0, eq)


def test_identity_holds_mixed_open_and_closed():
    # Open: $20 YES @ 0.50, current 0.55 → upnl = 20*(0.55-0.50)/0.50 = $2
    # Closed: $15 NO @ 0.30, exit 0.20  → realized = 15*(0.30-0.20)/(1-0.30) = $2.143
    open_trades = [{"size_usdc": 20.0}]
    open_with_mid = [
        {"direction": "YES", "entry_mid": 0.50, "current_mid": 0.55, "size_usdc": 20.0}
    ]
    closed_trades = [{"size_usdc": 15.0}]
    closed_pairs = [
        {"direction": "NO", "entry_mid": 0.30, "exit_mid_price": 0.20, "size_usdc": 15.0}
    ]
    closures = [{"realized_pnl_usdc": 15.0 * 0.10 / 0.70}]

    cash = wallet.cash_balance(
        initial_balance=1000.0,
        open_size_invested=wallet.sum_open_size_invested(open_trades),
        closed_exit_value_total=wallet.sum_closed_exit_value(closed_pairs),
        closed_size_invested=wallet.sum_closed_size_invested(closed_trades),
    )
    open_val = wallet.sum_open_positions_value(open_with_mid)
    upnl = wallet.sum_unrealized_pnl(open_with_mid)
    realized = wallet.sum_realized_pnl(closures)
    eq = wallet.equity(cash, open_val)

    drift = _identity_holds(1000.0, realized, upnl, eq)
    assert abs(drift) < 0.001


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
