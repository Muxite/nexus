"""Unit tests for ExitPolicy — every close branch + min_hold gating."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from ...config import CoreConfig
from ...exits import ExitPolicy


def _iso_ago(secs: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=secs)).isoformat()


def test_win_threshold_closes():
    p = ExitPolicy(CoreConfig())
    d = p.decide(entry_price=0.5, current_price=0.96, high_water_price=0.96,
                 opened_at_iso=_iso_ago(120))
    assert d.close and d.close_reason == "WIN"


def test_loss_threshold_closes():
    p = ExitPolicy(CoreConfig())
    d = p.decide(entry_price=0.5, current_price=0.04, high_water_price=0.5,
                 opened_at_iso=_iso_ago(120))
    assert d.close and d.close_reason == "LOSS"


def test_take_profit_after_min_hold():
    cfg = CoreConfig()
    p = ExitPolicy(cfg)
    d = p.decide(entry_price=0.50, current_price=0.50 + cfg.take_profit + 0.001,
                 high_water_price=None, opened_at_iso=_iso_ago(cfg.min_hold_seconds + 1))
    assert d.close and d.close_reason == "TP"


def test_take_profit_blocked_before_min_hold():
    cfg = CoreConfig()
    p = ExitPolicy(cfg)
    d = p.decide(entry_price=0.50, current_price=0.50 + cfg.take_profit + 0.001,
                 high_water_price=None, opened_at_iso=_iso_ago(0))
    assert not d.close


def test_stop_loss_after_min_hold():
    cfg = CoreConfig()
    p = ExitPolicy(cfg)
    d = p.decide(entry_price=0.50, current_price=0.50 - cfg.stop_loss - 0.001,
                 high_water_price=None, opened_at_iso=_iso_ago(cfg.min_hold_seconds + 1))
    assert d.close and d.close_reason == "SL"


def test_trailing_stop_engages_after_hwm():
    cfg = CoreConfig()
    p = ExitPolicy(cfg)
    hwm = 0.50 + cfg.trailing_stop + 0.001
    cur = hwm - cfg.trailing_stop - 0.001
    d = p.decide(entry_price=0.50, current_price=cur, high_water_price=hwm,
                 opened_at_iso=_iso_ago(cfg.min_hold_seconds + 1))
    assert d.close and d.close_reason == "TRAIL"


def test_timeout_after_position_timeout():
    cfg = CoreConfig()
    p = ExitPolicy(cfg)
    d = p.decide(entry_price=0.50, current_price=0.50,
                 high_water_price=None,
                 opened_at_iso=_iso_ago(cfg.position_timeout_hours * 3600 + 1))
    assert d.close and d.close_reason == "TIMEOUT"


def test_no_close_when_quiet():
    cfg = CoreConfig()
    p = ExitPolicy(cfg)
    d = p.decide(entry_price=0.50, current_price=0.501,
                 high_water_price=None, opened_at_iso=_iso_ago(120))
    assert not d.close
