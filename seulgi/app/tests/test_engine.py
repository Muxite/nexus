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
            await asyncio.sleep(0.1)
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

        # 4 engines × 0.1s. Parallel ≈ 0.1s + ~0.3s test setup ≈ 0.4s.
        # Sequential ≈ 0.4s + 0.3s setup ≈ 0.7s. 0.6s threshold catches a
        # regression while leaving headroom for host jitter.
        assert elapsed < 0.6, f"Engine fan-out appears sequential ({elapsed:.3f}s)"

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

    @pytest.mark.asyncio
    async def test_scout_bypasses_scoped_pre_ai_rule(self):
        """A pre_ai rule with engines_exclude=('scout',) must skip scout."""
        import re
        from pathlib import Path
        from rules import RuleEngine, Rule

        rules = RuleEngine(rules_dir=Path("/nonexistent-rules-dir"))
        rules._rules = [
            Rule(
                name="kill_all",
                stage="pre_ai",
                market_type_rx=re.compile(".*"),
                engines_exclude=("scout",),
            ),
        ]

        scout = _make_engine("scout")
        scout.trader._parent_engine = "scout"
        runner = _make_runner(engines=[scout], n_matches=2)
        runner._rules = rules

        await runner.process(_make_headline())

        scout.trader.evaluate.assert_called_once()
        args = scout.trader.evaluate.call_args
        survivors_passed = args[0][2]      # positional: (signal_id, headline, survivors, ...)
        pre_ai_skips = args.kwargs.get("pre_ai_skips") or []
        # Rule excludes scout at the scoped stage → all 2 survivors pass through
        assert len(survivors_passed) == 2
        assert len(pre_ai_skips) == 0

    @pytest.mark.asyncio
    async def test_production_engine_still_applies_scoped_pre_ai_rule(self):
        """The same scoped rule must still fire for engines not in engines_exclude."""
        import re
        from pathlib import Path
        from rules import RuleEngine, Rule

        rules = RuleEngine(rules_dir=Path("/nonexistent-rules-dir"))
        rules._rules = [
            Rule(
                name="kill_all",
                stage="pre_ai",
                market_type_rx=re.compile(".*"),
                engines_exclude=("scout",),
            ),
        ]

        prod = _make_engine("balanced")
        prod.trader._parent_engine = "balanced"
        runner = _make_runner(engines=[prod], n_matches=2)
        runner._rules = rules

        await runner.process(_make_headline())

        prod.trader.evaluate.assert_called_once()
        args = prod.trader.evaluate.call_args
        survivors_passed = args[0][2]
        pre_ai_skips = args.kwargs.get("pre_ai_skips") or []
        # "balanced" is not in engines_exclude → the rule kills every survivor
        assert len(survivors_passed) == 0
        assert len(pre_ai_skips) == 2
        # Each skip carries the rule's skip_reason so funnel attribution works
        for _, reason in pre_ai_skips:
            assert reason == "RULE_kill_all_SKIP"

    @pytest.mark.asyncio
    async def test_process_records_engine_decisions_per_engine(self):
        """process() must call record_engine_decisions once per active engine,
        regardless of whether the gather is wrapped in db.batch().

        Phase B (2026-05-21) wrapped the gather in db.batch() for one-fsync
        coalescing, but the wrap was reverted 2026-05-21 07:10 UTC because
        production headlined a stuck-batch hang (task #24). The test now
        verifies the essential behaviour: every engine gets its own
        record_engine_decisions call after the gather completes.
        """
        e1 = _make_engine("e1")
        e2 = _make_engine("e2")
        runner = _make_runner(engines=[e1, e2], n_matches=1)

        await runner.process(_make_headline())

        assert runner._db.record_engine_decisions.await_count == 2


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
            await asyncio.sleep(0.1)

        engines = [_make_engine(f"eng{i}") for i in range(4)]
        for eng in engines:
            eng.trader.monitor_positions = AsyncMock(side_effect=_slow_monitor)

        runner = _make_runner(engines=engines)

        import time
        t0 = time.monotonic()
        await runner.monitor_all_positions()
        elapsed = time.monotonic() - t0

        # See test_engines_run_concurrently for tolerance reasoning.
        assert elapsed < 0.6, f"Monitors appear sequential ({elapsed:.3f}s)"


# ---------------------------------------------------------------------------
# Phase 4A.1: stage-0a capital floor short-circuit
# ---------------------------------------------------------------------------


def _make_drained_trader(name="drained", balance=0.5, min_trade=50.0):
    """A trader mock with ONLY engines drained — capital check should fail.

    Explicitly sets `infinite_balance=False` (the default MagicMock attr is
    truthy and would short-circuit _engines_have_capital's no-op path).
    """
    trader = MagicMock(spec=PaperTrader)
    trader.balance = float(balance)
    trader.infinite_balance = False
    trader.cfg = MagicMock()
    trader.cfg.min_trade_usdc = float(min_trade)
    trader.cfg.infinite_balance = False
    trader.cfg.mode = "paper"
    # Spec-bound attrs.
    trader.evaluate = AsyncMock(return_value=[])
    trader.process = AsyncMock(return_value=[])
    trader.monitor_positions = AsyncMock()
    trader.set_balance_change_callback = MagicMock()
    return trader


class TestStage0aCapitalFloor:
    """Phase 4A.1: capital_floor short-circuit fires BEFORE embed / freshness /
    match / CLOB / DB writes when every engine is drained.

    Tests set `SEULGI_DISABLE_HOUR_GATE=1` because the hour-gate runs first
    and otherwise eats the test on UTC 23/00/01.
    """

    def test_engines_have_capital_returns_false_when_all_drained(self, monkeypatch):
        monkeypatch.delenv("SEULGI_BYPASS_LLM", raising=False)
        e1 = Engine(name="e1", trader=_make_drained_trader("e1", balance=0.5))
        e2 = Engine(name="e2", trader=_make_drained_trader("e2", balance=0.0))
        runner = _make_runner(engines=[e1, e2], n_matches=1)
        assert runner._engines_have_capital() is False

    def test_engines_have_capital_returns_true_when_one_has_capital(self, monkeypatch):
        monkeypatch.delenv("SEULGI_BYPASS_LLM", raising=False)
        e_rich = Engine(name="rich", trader=_make_drained_trader("rich", balance=500.0))
        e_poor = Engine(name="poor", trader=_make_drained_trader("poor", balance=0.0))
        runner = _make_runner(engines=[e_rich, e_poor], n_matches=1)
        assert runner._engines_have_capital() is True

    def test_engines_have_capital_noop_in_bypass(self, monkeypatch):
        """SEULGI_BYPASS_LLM=1 ⇒ always True (no-op, even when drained)."""
        monkeypatch.setenv("SEULGI_BYPASS_LLM", "1")
        e1 = Engine(name="e1", trader=_make_drained_trader("e1", balance=0.0))
        runner = _make_runner(engines=[e1], n_matches=1)
        assert runner._engines_have_capital() is True

    def test_engines_have_capital_noop_in_infinite_balance(self, monkeypatch):
        """infinite_balance=True on any engine ⇒ always True (no-op)."""
        monkeypatch.delenv("SEULGI_BYPASS_LLM", raising=False)
        t = _make_drained_trader("inf", balance=0.0)
        t.infinite_balance = True
        e1 = Engine(name="inf", trader=t)
        runner = _make_runner(engines=[e1], n_matches=1)
        assert runner._engines_have_capital() is True

    @pytest.mark.asyncio
    async def test_drained_short_circuits_before_embed(self, monkeypatch):
        """All engines drained → process() returns before embed/match/DB.

        The strongest assertion possible: embed must NOT be called, no
        signal must be inserted, evaluate must NOT be called.
        """
        monkeypatch.setenv("SEULGI_DISABLE_HOUR_GATE", "1")
        monkeypatch.delenv("SEULGI_BYPASS_LLM", raising=False)
        e1 = Engine(name="e1", trader=_make_drained_trader("e1", balance=0.0))
        e2 = Engine(name="e2", trader=_make_drained_trader("e2", balance=0.0))
        runner = _make_runner(engines=[e1, e2], n_matches=2)

        await runner.process(_make_headline())

        # Zero downstream cost: embed, signal insert, evaluate untouched.
        runner._embedder.embed.assert_not_called()
        runner._db.insert_signal.assert_not_called()
        runner._db.bump_market_heat.assert_not_called()
        e1.trader.evaluate.assert_not_called()
        e2.trader.evaluate.assert_not_called()
        # Counter ticked.
        assert runner._capital_floor_skips == 1

    @pytest.mark.asyncio
    async def test_one_engine_with_capital_lets_pipeline_proceed(self, monkeypatch):
        """At least one engine has free cash → check passes; embed runs."""
        monkeypatch.setenv("SEULGI_DISABLE_HOUR_GATE", "1")
        monkeypatch.delenv("SEULGI_BYPASS_LLM", raising=False)
        e_rich = Engine(name="rich", trader=_make_drained_trader("rich", balance=500.0))
        # rich needs to behave like a real engine — return a decision when
        # evaluate is called. Same shape as _make_trader_mock.
        decision = TradeDecision(
            condition_id="cid0", question="Q0?", direction=Direction.YES,
            confidence=0.85, similarity=0.80, p_hat=0.534, mid_price=0.45,
            edge=0.084, size_usdc=50.0, executed=True, skip_reason=None,
        )
        e_rich.trader.evaluate = AsyncMock(return_value=[decision])
        e_poor = Engine(name="poor", trader=_make_drained_trader("poor", balance=0.0))
        e_poor.trader.evaluate = AsyncMock(return_value=[])
        runner = _make_runner(engines=[e_rich, e_poor], n_matches=1)

        # Same prefetch patch as TestEngineRunnerProcess: a real CLOB fetch
        # would return None and drop every match as 'no_price' before
        # insert_signal runs.
        with patch("engine._fetch_mid_price", AsyncMock(return_value=0.5)):
            await runner.process(_make_headline())

        # Pipeline progressed past stage 0a.
        runner._embedder.embed.assert_called()
        runner._db.insert_signal.assert_called()
        assert runner._capital_floor_skips == 0

    def test_balance_change_callback_invalidates_cache(self, monkeypatch):
        """A balance mutation must clear the cache so the next call sees fresh state."""
        monkeypatch.delenv("SEULGI_BYPASS_LLM", raising=False)
        t = _make_drained_trader("e", balance=500.0)
        e = Engine(name="e", trader=t)
        runner = _make_runner(engines=[e], n_matches=1)
        # Prime the cache (True).
        assert runner._engines_have_capital() is True
        assert runner._capital_cache is not None
        # Fire the invalidator (this is what PaperTrader._notify_balance_change does).
        runner._invalidate_capital_cache()
        assert runner._capital_cache is None
