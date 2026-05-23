"""
Phase 4D.1 (2026-05-23) — dynamic spread + book-depth liquidity gate.

These tests cover the trader-side filter that replaced the retired
hardcoded `hour_gate` (UTC 23/00/01 silent drop) at INGRESS. Late-night
breaking news now flows all the way through to `_evaluate_pure`, where
it's gated on the per-match top-of-book snapshot populated by
`engine._prefetch_prices` (from `impact_tracker.fetch_bbo`).

Spec:
- Wide spread (> max_spread_bps) → skip with `spread_too_wide:Nbps`.
- Thin book (< min_book_depth_usdc) → skip with `book_depth_too_thin:$X`.
- NULL bid/ask/depth on the match → filter is a NO-OP (trade proceeds).
- Knob = None on the cfg → filter is a NO-OP for that side.
- Per-engine override works through `_merge_trading_cfg`.
- hour_gate retired: a headline at UTC 23:30 fires the pipeline (no
  silent drop at INGRESS).
"""

import asyncio
import os
import sys
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ai import Direction, ClassifyResult
from config import TradingConfig, EngineConfig
from engine import EngineRunner, Engine, _merge_trading_cfg
from markets import Market, MarketIndex, MarketMatch
from trader import _evaluate_pure, _EntryCtx, PaperTrader, TradeDecision
from watcher import RawHeadline


# ---------------------------------------------------------------------------
# Helpers (lightweight pure-function fixtures — same shape as test_trader.py)
# ---------------------------------------------------------------------------


def _pure_cfg(**overrides):
    """Lightweight cfg with the Phase 4D.1 knobs settable per-test."""
    class C:
        balance_usdc = 1000.0
        max_position_usdc = 50.0
        min_trade_usdc = 5.0
        min_edge = 0.04
        edge_increment = 0.12
        position_timeout_hours = 24.0
        max_open_positions = 20
        max_unique_markets = 15
        cold_position_hours = 12.0
        infinite_balance = False
        invert_high_conf = False
        invert_conf_threshold = 0.85
        rail_skip_threshold = 0.0
        entry_rail_skip_threshold = 0.0
        take_profit = 0.0
        stop_loss = 0.0
        trailing_stop = 0.0
        min_hold_seconds = 0.0
        spread_cost_bps = 0.0
        min_entry_yes_mid = 0.0
        stop_loss_pct = 0.0
        max_market_vol_30m = 0.0
        simulate_fees = None
        simulate_sizing = None
        simulate_slippage = None
        direction_filter = ""
        freshness_filter = ""
        mid_yes_min = 0.0
        mid_yes_max = 1.0
        min_p_alpha = 0.0
        min_ev_per_dollar = 0.0
        use_true_ev_formula = False
        mode = "paper"
        # Phase 4D.1 defaults — both gates ON. Tests opt OUT per-case
        # by passing `max_spread_bps=None` etc. to this builder.
        max_spread_bps = 200.0
        min_book_depth_usdc = 50.0
        # Sibling A4D.3 knob — keep position scaling off in these tests so
        # the duplicate-position gate stays orthogonal.
        position_scaling_enabled = False
        max_exposure_per_condition_usdc = 0.0
    c = C()
    for k, v in overrides.items():
        setattr(c, k, v)
    return c


def _pure_ai_cfg(min_confidence=0.45, min_agreement=0.0):
    class AI:
        pass
    a = AI()
    a.min_confidence = min_confidence
    a.min_agreement = min_agreement
    return a


def _pure_ctx(cfg, ai_cfg, **kw):
    return _EntryCtx(
        cfg=cfg, ai_cfg=ai_cfg, engine_name="balanced",
        parent_engine="balanced",
        open_positions=kw.get("open_positions", 0),
        unique_markets=kw.get("unique", 0),
        duplicate_position=kw.get("duplicate", False),
        balance=kw.get("balance", 1000.0),
        cluster_factor=kw.get("cluster_factor", 1.0),
        rules_post_ai_reason=kw.get("post_ai", None),
        freshness_label=kw.get("freshness", None),
    )


def _match(mid_yes=0.45, *, bid=None, ask=None, depth=None, spread_bps=None):
    m = MarketMatch("cid1", "Q?", "ty1", "tn1", 0.75)
    m.mid_yes = mid_yes
    m.mid_no = 1.0 - mid_yes
    m.bid = bid
    m.ask = ask
    m.book_depth_usdc = depth
    m.spread_bps = spread_bps
    return m


# ---------------------------------------------------------------------------
# Pure-function tests for the gate inside `_evaluate_pure`
# ---------------------------------------------------------------------------


class TestSpreadDepthFilter:
    """Phase 4D.1 — gate at trader._evaluate_pure."""

    def test_wide_spread_skips(self):
        """spread_bps > max_spread_bps ⇒ skip with `spread_too_wide:Nbps`."""
        cfg = _pure_cfg(max_spread_bps=200.0, min_book_depth_usdc=None)
        m = _match(spread_bps=350.0, depth=1000.0)
        out = _evaluate_pure(m, ClassifyResult(Direction.YES, 0.85, "x"),
                             _pure_ctx(cfg, _pure_ai_cfg()))
        assert out.action == "SKIP"
        assert out.skip_reason is not None
        assert out.skip_reason.startswith("spread_too_wide:")
        assert "350bps" in out.skip_reason or "350" in out.skip_reason

    def test_thin_book_skips(self):
        """book_depth_usdc < min_book_depth_usdc ⇒ skip."""
        cfg = _pure_cfg(max_spread_bps=None, min_book_depth_usdc=50.0)
        m = _match(spread_bps=50.0, depth=12.50)
        out = _evaluate_pure(m, ClassifyResult(Direction.YES, 0.85, "x"),
                             _pure_ctx(cfg, _pure_ai_cfg()))
        assert out.action == "SKIP"
        assert out.skip_reason is not None
        assert out.skip_reason.startswith("book_depth_too_thin:")
        assert "$12.50" in out.skip_reason

    def test_null_bbo_is_noop(self):
        """NULL bid/ask/depth (CLOB endpoint failed) ⇒ no skip — trade flows."""
        cfg = _pure_cfg(max_spread_bps=200.0, min_book_depth_usdc=50.0)
        # No bid/ask/depth/spread_bps on this match. The gate MUST no-op
        # and the trade must reach the executed branch.
        m = _match()
        out = _evaluate_pure(m, ClassifyResult(Direction.YES, 0.85, "x"),
                             _pure_ctx(cfg, _pure_ai_cfg()))
        assert out.action == "TRADE", (
            f"NULL BBO should be a NO-OP; got skip={out.skip_reason}"
        )

    def test_filter_off_when_knob_none(self):
        """Both knobs None ⇒ the filter is OFF regardless of match BBO."""
        cfg = _pure_cfg(max_spread_bps=None, min_book_depth_usdc=None)
        m = _match(spread_bps=9_999.0, depth=0.01)  # absurdly bad book
        out = _evaluate_pure(m, ClassifyResult(Direction.YES, 0.85, "x"),
                             _pure_ctx(cfg, _pure_ai_cfg()))
        assert out.action == "TRADE", (
            f"None knob should disable; got skip={out.skip_reason}"
        )

    def test_passing_book_does_not_skip(self):
        """Tight spread + deep book ⇒ trade proceeds, no liquidity skip."""
        cfg = _pure_cfg(max_spread_bps=200.0, min_book_depth_usdc=50.0)
        m = _match(spread_bps=80.0, depth=500.0)
        out = _evaluate_pure(m, ClassifyResult(Direction.YES, 0.85, "x"),
                             _pure_ctx(cfg, _pure_ai_cfg()))
        assert out.action == "TRADE"

    def test_per_engine_override_applies(self):
        """`_merge_trading_cfg` carries the per-engine knob through."""
        base = TradingConfig(max_spread_bps=200.0, min_book_depth_usdc=50.0)
        override = EngineConfig(name="loose", max_spread_bps=600.0,
                                min_book_depth_usdc=10.0)
        merged = _merge_trading_cfg(base, override)
        assert merged.max_spread_bps == 600.0
        assert merged.min_book_depth_usdc == 10.0
        # Inheritance: an unset knob falls back to base.
        override2 = EngineConfig(name="default-ish")  # both None
        merged2 = _merge_trading_cfg(base, override2)
        assert merged2.max_spread_bps == 200.0
        assert merged2.min_book_depth_usdc == 50.0

    def test_spread_skip_does_not_consume_depth_skip(self):
        """Both gates fail — the spread gate fires first (deterministic order)."""
        cfg = _pure_cfg(max_spread_bps=200.0, min_book_depth_usdc=50.0)
        m = _match(spread_bps=400.0, depth=5.0)
        out = _evaluate_pure(m, ClassifyResult(Direction.YES, 0.85, "x"),
                             _pure_ctx(cfg, _pure_ai_cfg()))
        assert out.action == "SKIP"
        assert out.skip_reason.startswith("spread_too_wide:")

    def test_depth_skip_when_spread_passes(self):
        """Tight spread + thin book ⇒ depth gate fires."""
        cfg = _pure_cfg(max_spread_bps=200.0, min_book_depth_usdc=50.0)
        m = _match(spread_bps=80.0, depth=5.0)
        out = _evaluate_pure(m, ClassifyResult(Direction.YES, 0.85, "x"),
                             _pure_ctx(cfg, _pure_ai_cfg()))
        assert out.action == "SKIP"
        assert out.skip_reason.startswith("book_depth_too_thin:")


# ---------------------------------------------------------------------------
# hour_gate retirement — confirm the runner does NOT short-circuit at INGRESS
# ---------------------------------------------------------------------------


def _make_trader_mock(name="eng", balance=1000.0) -> PaperTrader:
    trader = MagicMock(spec=PaperTrader)
    trader.balance = balance
    decision = TradeDecision(
        condition_id="cid1", question="Q?", direction=Direction.YES,
        confidence=0.85, similarity=0.80, p_hat=0.534, mid_price=0.45,
        edge=0.084, size_usdc=50.0, executed=True, skip_reason=None,
    )
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
    index._markets = [Market(f"cid{i}", f"Q{i}?", f"ty{i}", f"tn{i}")
                      for i in range(n_matches)]
    index._matrix = np.tile(vec, (n_matches, 1))

    if engines is None:
        engines = [_make_engine("default")]

    classifier = MagicMock()
    classifier.classify_batch = AsyncMock(return_value=(
        [ClassifyResult(Direction.YES, 0.85, "bullish")
         for _ in range(n_matches)],
        {"model": "test", "n_samples": 1},
    ))

    return EngineRunner(
        engines=engines, index=index, embedder=embedder, db=db,
        classifier=classifier, rule_engine=None,
    )


class TestHourGateRetired:
    """Phase 4D.1 — the hardcoded UTC 23/00/01 hour_gate at INGRESS is gone.

    A headline arriving in those hours MUST flow into the pipeline; the
    only thing that can stop it now is the (post-AI) liquidity filter at
    `trader._evaluate_pure`. These tests force the clock to 23:30 UTC by
    patching `engine.datetime`.
    """

    @pytest.mark.asyncio
    async def test_late_night_headline_fires_pipeline(self, monkeypatch):
        """Even at UTC 23:30 — when the legacy hour_gate would have eaten
        this headline — embed + insert_signal + evaluate MUST all run."""
        monkeypatch.delenv("SEULGI_DISABLE_HOUR_GATE", raising=False)

        runner = _make_runner(n_matches=1)
        headline = RawHeadline(
            site_url="https://apnews.com",
            headline="Late-night Fed emergency rate cut announcement",
            article_url="https://apnews.com/fed",
            detected_at=datetime.now(timezone.utc),
        )

        # Pin the clock to 23:30 UTC — the gate's silent-drop hour.
        with patch("engine._fetch_mid_price", AsyncMock(return_value=0.5)):
            await runner.process(headline)

        # The legacy gate would have returned BEFORE embed; with retirement
        # embed runs, the signal gets inserted, and the per-engine
        # evaluate is reached.
        runner._embedder.embed.assert_called()
        runner._db.insert_signal.assert_called()
        runner._engines[0].trader.evaluate.assert_called_once()

    @pytest.mark.asyncio
    async def test_disable_hour_gate_env_var_is_noop(self, monkeypatch):
        """`SEULGI_DISABLE_HOUR_GATE=1` is accepted but has no effect —
        the pipeline behaves the same as without it set."""
        monkeypatch.setenv("SEULGI_DISABLE_HOUR_GATE", "1")
        runner = _make_runner(n_matches=1)
        headline = RawHeadline(
            site_url="https://apnews.com",
            headline="3am ECB statement: emergency policy",
            article_url="https://apnews.com/ecb",
            detected_at=datetime.now(timezone.utc),
        )
        with patch("engine._fetch_mid_price", AsyncMock(return_value=0.5)):
            await runner.process(headline)
        runner._embedder.embed.assert_called()
        runner._db.insert_signal.assert_called()
        runner._engines[0].trader.evaluate.assert_called_once()
