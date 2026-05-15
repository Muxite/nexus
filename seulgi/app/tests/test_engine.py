"""
Tests for seulgi/app/engine.py

Covers:
- build_engines: no engines config → single default engine
- build_engines: multiple engine configs with overrides
- EngineRunner.process: fan-out to all engines, parallel execution
- EngineRunner.monitor_all_positions: concurrent for all engines
- _merge_trading_cfg: overrides applied correctly
"""

import asyncio
import sys
import os
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ai import Direction, ClassifyResult
from config import TradingConfig, EngineConfig, AIConfig
from engine import EngineRunner, Engine, build_engines, _merge_trading_cfg
from markets import Market, MarketIndex, MarketMatch
from trader import PaperTrader, TradeDecision
from watcher import RawHeadline


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_headline():
    return RawHeadline(
        site_url="https://apnews.com",
        headline="Fed slashes rates by 50bps in emergency meeting today",
        article_url="https://apnews.com/fed",
        detected_at=datetime.now(timezone.utc),
    )


def _make_trader_mock(name="eng", balance=1000.0):
    trader = MagicMock(spec=PaperTrader)
    trader.balance = balance
    decision = TradeDecision(
        condition_id="cid1",
        question="Will X?",
        direction=Direction.YES,
        confidence=0.85,
        similarity=0.80,
        p_hat=0.534,
        mid_price=0.45,
        edge=0.084,
        size_usdc=50.0,
        executed=True,
        skip_reason=None,
    )
    # Runner now calls evaluate(signal_id, headline, survivors,
    # classify_results, prompt_meta_json, pre_ai_skips=...). Keep `.process`
    # as a no-op so tests asserting "was not called" still pass for the
    # legacy entry point.
    trader.evaluate = AsyncMock(return_value=[decision])
    trader.process = AsyncMock(return_value=[decision])
    trader.monitor_positions = AsyncMock()
    return trader


def _make_engine(name="eng", balance=1000.0) -> Engine:
    return Engine(name=name, trader=_make_trader_mock(name, balance))


def _make_runner(engines=None, n_matches=1):
    db = MagicMock()
    db.record_engine_decisions = AsyncMock()
    db.bump_market_heat = AsyncMock()
    db.insert_signal = AsyncMock(return_value=1)
    db.log_usage = AsyncMock()
    embedder = MagicMock()
    vec = np.ones(384, dtype=np.float32)
    vec /= np.linalg.norm(vec)
    embedder.embed.return_value = vec

    cfg = MagicMock()
    cfg.markets.similarity_threshold = 0.0
    cfg.markets.top_k = 5
    cfg.markets.refresh_hours = 4.0

    index = MarketIndex(db=MagicMock(), embedder=embedder, cfg=cfg)
    index._markets = [Market(f"cid{i}", f"Q{i}?", f"ty{i}", f"tn{i}") for i in range(n_matches)]
    index._matrix = np.tile(vec, (n_matches, 1))

    if engines is None:
        engines = [_make_engine("default")]

    # The refactored runner does the AI call itself, so it needs a
    # classifier. Return one ClassifyResult per match — same shape every
    # variant receives.
    classifier = MagicMock()
    classifier.classify_batch = AsyncMock(return_value=(
        [
            ClassifyResult(Direction.YES, 0.85, "bullish")
            for _ in range(n_matches)
        ],
        {"model": "test", "n_samples": 1},
    ))

    runner = EngineRunner(
        engines=engines, index=index, embedder=embedder, db=db,
        classifier=classifier, rule_engine=None,
    )
    return runner


# ---------------------------------------------------------------------------
# build_engines
# ---------------------------------------------------------------------------


class TestBuildEngines:
    def test_no_engines_config_creates_default(self):
        cfg = MagicMock()
        cfg.engines = []
        cfg.trading = TradingConfig()
        cfg.ai = AIConfig()
        db = MagicMock()
        classifier = MagicMock()

        engines = build_engines(cfg, db, classifier)
        assert len(engines) == 1
        assert engines[0].name == "default"

    def test_multiple_engines_created(self):
        cfg = MagicMock()
        cfg.engines = [
            EngineConfig(name="aggressive", min_edge=0.03),
            EngineConfig(name="conservative", min_edge=0.08),
        ]
        cfg.trading = TradingConfig()
        cfg.ai = AIConfig()
        db = MagicMock()
        classifier = MagicMock()

        engines = build_engines(cfg, db, classifier)
        assert len(engines) == 2
        assert [e.name for e in engines] == ["aggressive", "conservative"]

    def test_engine_overrides_applied(self):
        base = TradingConfig(min_edge=0.04, max_position_usdc=100.0)
        override = EngineConfig(name="big", max_position_usdc=200.0)
        merged = _merge_trading_cfg(base, override)
        assert merged.max_position_usdc == 200.0
        assert merged.min_edge == 0.04  # inherited

    def test_none_override_inherits_base(self):
        base = TradingConfig(min_edge=0.07, edge_increment=0.15)
        override = EngineConfig(name="x")  # all None
        merged = _merge_trading_cfg(base, override)
        assert merged.min_edge == 0.07
        assert merged.edge_increment == 0.15


# ---------------------------------------------------------------------------
# EngineRunner.process
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=False)
def _mock_prefetch_prices():
    """Patches engine._fetch_mid_price to a constant so the prefetch in
    EngineRunner.process() returns prices instead of None (which would
    drop every match as 'no_price')."""
    with patch("engine._fetch_mid_price", AsyncMock(return_value=0.5)):
        yield


class TestEngineRunnerProcess:
    @pytest.fixture(autouse=True)
    def _setup(self, _mock_prefetch_prices):
        # Activate the prefetch-price patch for every test in this class
        pass

    @pytest.mark.asyncio
    async def test_no_matches_skips_all_engines(self):
        """If no market matches, engines are never called."""
        runner = _make_runner(n_matches=0)
        eng = _make_engine()
        runner._engines = [eng]
        runner._index._markets = []
        runner._index._matrix = None

        await runner.process(_make_headline())
        eng.trader.evaluate.assert_not_called()

    @pytest.mark.asyncio
    async def test_all_engines_receive_same_matches(self):
        """Each engine's evaluate() should be called with the same survivors."""
        e1 = _make_engine("e1")
        e2 = _make_engine("e2")
        e3 = _make_engine("e3")
        runner = _make_runner(engines=[e1, e2, e3], n_matches=2)

        await runner.process(_make_headline())

        for eng in [e1, e2, e3]:
            eng.trader.evaluate.assert_called_once()
            args = eng.trader.evaluate.call_args
            # evaluate(signal_id, headline, survivors, classify_results, ...)
            survivors = args[0][2]
            assert len(survivors) == 2

    @pytest.mark.asyncio
    async def test_engines_run_concurrently(self):
        """Fan-out should be parallel, not sequential."""

        async def _slow_evaluate(*_a, **_kw):
            await asyncio.sleep(0.02)
            return []

        engines = []
        for i in range(4):
            eng = _make_engine(f"eng{i}")
            eng.trader.evaluate = _slow_evaluate
            engines.append(eng)

        runner = _make_runner(engines=engines, n_matches=1)

        import time
        t0 = time.monotonic()
        await runner.process(_make_headline())
        elapsed = time.monotonic() - t0

        # If parallel: ~0.02s. If sequential: ~0.08s.
        assert elapsed < 0.06, f"Engine fan-out appears sequential ({elapsed:.3f}s)"

    @pytest.mark.asyncio
    async def test_engine_error_doesnt_stop_others(self):
        """An exception in one engine should not abort the others."""
        good_engine = _make_engine("good")
        bad_engine = _make_engine("bad")
        bad_engine.trader.evaluate = AsyncMock(side_effect=RuntimeError("boom"))

        runner = _make_runner(engines=[good_engine, bad_engine], n_matches=1)
        # Should not raise
        await runner.process(_make_headline())

        good_engine.trader.evaluate.assert_called_once()

    @pytest.mark.asyncio
    async def test_decisions_recorded_to_db(self):
        """record_engine_decisions must be called once per engine."""
        e1 = _make_engine("e1")
        e2 = _make_engine("e2")
        runner = _make_runner(engines=[e1, e2], n_matches=1)

        await runner.process(_make_headline())

        assert runner._db.record_engine_decisions.await_count == 2
        # First call should be for e1 or e2 with the engine name
        call_args = [
            runner._db.record_engine_decisions.call_args_list[i][0][0]
            for i in range(2)
        ]
        assert set(call_args) == {"e1", "e2"}

    @pytest.mark.asyncio
    async def test_embed_error_aborts_gracefully(self):
        """If embedding fails, no engines should be called."""
        runner = _make_runner(n_matches=1)
        runner._embedder.embed.side_effect = RuntimeError("CUDA OOM")
        eng = _make_engine()
        runner._engines = [eng]

        await runner.process(_make_headline())
        eng.trader.evaluate.assert_not_called()


# ---------------------------------------------------------------------------
# EngineRunner.monitor_all_positions
# ---------------------------------------------------------------------------


class TestMonitorAllPositions:
    @pytest.mark.asyncio
    async def test_all_engines_monitored(self):
        engines = [_make_engine(f"eng{i}") for i in range(3)]
        runner = _make_runner(engines=engines)
        await runner.monitor_all_positions()
        for eng in engines:
            eng.trader.monitor_positions.assert_called_once()

    @pytest.mark.asyncio
    async def test_monitors_run_concurrently(self):
        async def _slow_monitor():
            await asyncio.sleep(0.02)

        engines = [_make_engine(f"eng{i}") for i in range(4)]
        for eng in engines:
            eng.trader.monitor_positions = AsyncMock(side_effect=_slow_monitor)

        runner = _make_runner(engines=engines)

        import time
        t0 = time.monotonic()
        await runner.monitor_all_positions()
        elapsed = time.monotonic() - t0

        assert elapsed < 0.06, f"Monitors appear sequential ({elapsed:.3f}s)"
