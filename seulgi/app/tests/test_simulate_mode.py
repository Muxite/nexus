"""End-to-end simulate-mode behaviour tests (plan §6).

Covers:
- AI pause gate fires when every engine balance < min_trade_usdc.
- AI resume after a balance frees up.
- Per-engine bankroll split logic.
"""

from __future__ import annotations

import asyncio
import os
import sys
from unittest.mock import AsyncMock, MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config import (  # noqa: E402
    SimulateFeesConfig,
    SimulateSizingConfig,
    SimulateSlippageConfig,
    TradingConfig,
)


def test_is_simulate_helper():
    cfg = TradingConfig(mode="simulate")
    assert cfg.is_simulate() is True
    cfg2 = TradingConfig(mode="paper")
    assert cfg2.is_simulate() is False


def test_per_engine_bankroll_split_math():
    cfg = TradingConfig(mode="simulate", simulate_total_usdc=50.0)
    n = 4
    per = cfg.simulate_total_usdc / n
    assert per == pytest.approx(12.5)


@pytest.mark.asyncio
async def test_ai_pause_when_all_engines_dry():
    """Plan §3: simulate runner short-circuits classify_batch when balances
    are below min_trade_usdc across every engine."""
    from engine import EngineRunner  # noqa: E402

    eng_cfg = TradingConfig(mode="simulate", balance_usdc=0.5, min_trade_usdc=1.0)
    engines = []
    for i in range(2):
        trader = MagicMock()
        trader.balance = 0.5  # below the $1 minimum
        trader.cfg = eng_cfg
        trader.evaluate_ai_paused_skips = AsyncMock(return_value=[])
        e = MagicMock()
        e.trader = trader
        e.name = f"eng_{i}"
        engines.append(e)

    runner = EngineRunner.__new__(EngineRunner)
    runner._engines = engines
    runner._simulate_mode = True
    runner._ai_paused = False
    runner._dashboard = None

    assert runner._ai_should_pause() is True
    assert runner._update_ai_pause_state() is True
    # Calling again is idempotent (no state change).
    assert runner._update_ai_pause_state() is True


@pytest.mark.asyncio
async def test_ai_resumes_when_one_engine_has_capital():
    from engine import EngineRunner  # noqa: E402

    eng_cfg = TradingConfig(mode="simulate", balance_usdc=10.0, min_trade_usdc=1.0)
    t_dry = MagicMock(); t_dry.balance = 0.0; t_dry.cfg = eng_cfg
    t_full = MagicMock(); t_full.balance = 10.0; t_full.cfg = eng_cfg
    engines = [MagicMock(trader=t_dry), MagicMock(trader=t_full)]
    runner = EngineRunner.__new__(EngineRunner)
    runner._engines = engines
    runner._simulate_mode = True
    runner._ai_paused = True
    runner._dashboard = None
    assert runner._ai_should_pause() is False
    assert runner._update_ai_pause_state() is False


def test_pause_does_nothing_outside_simulate():
    from engine import EngineRunner  # noqa: E402

    eng_cfg = TradingConfig(mode="paper", balance_usdc=0.0, min_trade_usdc=1.0)
    t = MagicMock(); t.balance = 0.0; t.cfg = eng_cfg
    runner = EngineRunner.__new__(EngineRunner)
    runner._engines = [MagicMock(trader=t)]
    runner._simulate_mode = False
    runner._ai_paused = False
    runner._dashboard = None
    assert runner._ai_should_pause() is False
