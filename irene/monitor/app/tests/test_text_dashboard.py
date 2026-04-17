"""Unit tests for text_dashboard.format_dashboard_text."""

from __future__ import annotations

from datetime import datetime, timezone

from text_dashboard import format_dashboard_text


def test_format_dashboard_text_contains_sections():
    stats = {
        "active_markets": 3,
        "open_positions": 1,
        "signals_total": 50,
        "signals_mapped_total": 10,
        "signals_ingested_24h": 12,
        "mapped_signals_total": 9,
        "mapped_signals_24h": 8,
        "mapped_signals_dispatched": 7,
        "trades_closed_24h": 2,
        "trades_open_executed": 1,
        "trades_closed_total": 5,
        "trades_executed_total": 6,
        "trades_skipped_total": 4,
        "realized_pnl_total": 12.5,
        "realized_pnl_24h": 3.0,
        "unrealized_pnl_total": 1.25,
        "open_exposure_usdc": 40.0,
        "closed_wins": 3,
        "closed_losses": 2,
        "closed_breakeven": 0,
        "first_signal_ingested_at": None,
        "first_trade_decided_at": None,
        "last_trade_closed_at": None,
        "generated_at": datetime(2026, 4, 2, 12, 0, 0, tzinfo=timezone.utc),
    }
    last_seen = {
        "scanner": "2026-04-02T11:59:00+00:00",
        "feeder": None,
        "trader": "2026-04-02T11:58:00+00:00",
    }
    text = format_dashboard_text(
        stats,
        last_seen,
        starting_balance_usdc=100.0,
        mode="paper",
    )
    assert "IRENE" in text
    assert "paper" in text
    assert "Starting balance" in text
    assert "Realized" in text
    assert "3" in text
    assert "2026-04-02T11:59:00" in text
    assert text.count("—") >= 1
