"""Tests for app.counterfactual + db migration + trader wiring (A4E.5).

The sampler is deterministic — same (filter, headline, condition, engine)
quadruple always maps to the same bypass decision. That property is what
lets the Phase-5 replay harness reproduce cf trades byte-for-byte.
"""
from __future__ import annotations

import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ai import ClassifyResult, Direction
from counterfactual import (
    TOP_5_FILTERS,
    CounterfactualConfig,
    CounterfactualSampler,
    load_from_yaml,
)
from db import SeulgiDB
from markets import MarketMatch


# ---------------------------------------------------------------------------
# CounterfactualSampler unit tests
# ---------------------------------------------------------------------------


class TestSamplerRate:
    def test_default_off_returns_false_even_at_full_rate(self):
        """Disabled by default — bypass NEVER fires regardless of rate."""
        cfg = CounterfactualConfig(enabled=False, sample_rate_per_filter=1.0)
        s = CounterfactualSampler(cfg)
        for i in range(500):
            assert (
                s.should_bypass("min_confidence", f"h{i}", "c1", "eng") is False
            )

    def test_sample_rate_one_percent_hits_roughly_one_percent(self):
        """At rate=0.01 over 5000 distinct headlines, fraction of True
        should be in [0.005, 0.02] (wide enough to dodge flakiness)."""
        cfg = CounterfactualConfig(
            enabled=True, sample_rate_per_filter=0.01,
            max_cf_per_headline=1_000_000,
        )
        s = CounterfactualSampler(cfg)
        n_total = 5000
        n_bypass = sum(
            1 for i in range(n_total)
            if s.should_bypass("min_confidence", f"h{i}", "c1", "eng")
        )
        frac = n_bypass / n_total
        assert 0.005 <= frac <= 0.02, (
            f"expected ~1% bypass at rate=0.01; got {frac:.3%} "
            f"({n_bypass}/{n_total})"
        )

    def test_full_rate_bypasses_almost_everything(self):
        """At rate=1.0 every input gets bypassed (per-headline cap aside)."""
        cfg = CounterfactualConfig(
            enabled=True, sample_rate_per_filter=1.0,
            max_cf_per_headline=1_000_000,
        )
        s = CounterfactualSampler(cfg)
        for i in range(200):
            assert s.should_bypass("min_confidence", f"h{i}", "c1", "eng") is True

    def test_zero_rate_bypasses_nothing(self):
        cfg = CounterfactualConfig(
            enabled=True, sample_rate_per_filter=0.0,
        )
        s = CounterfactualSampler(cfg)
        for i in range(200):
            assert s.should_bypass("min_confidence", f"h{i}", "c1", "eng") is False


class TestSamplerDeterminism:
    def test_same_inputs_same_answer(self):
        """Repeated calls with the same quadruple must agree."""
        cfg = CounterfactualConfig(
            enabled=True, sample_rate_per_filter=0.5,
            max_cf_per_headline=1_000_000,
        )
        s1 = CounterfactualSampler(cfg)
        s2 = CounterfactualSampler(cfg)
        # Sample 50 quadruples and verify both samplers agree on each.
        for i in range(50):
            args = ("freshness_filter", f"hid_{i}", f"cid_{i}", "engineA")
            assert s1.should_bypass(*args) == s2.should_bypass(*args)

    def test_filter_name_changes_decision_independently(self):
        """Hash is filter-scoped — different filters on the same headline
        get independent random decisions."""
        cfg = CounterfactualConfig(
            enabled=True, sample_rate_per_filter=0.5,
            max_cf_per_headline=1_000_000,
        )
        s = CounterfactualSampler(cfg)
        # Search for one headline whose two filters disagree. With rate=0.5
        # and 100 headlines, the chance both filters agree on all 100 is
        # 0.5^100 ≈ 0 — but use a fresh sampler each iteration so the
        # max_cf_per_headline cap doesn't interfere.
        for i in range(100):
            sa = CounterfactualSampler(cfg)
            sb = CounterfactualSampler(cfg)
            a = sa.should_bypass("min_confidence", f"hid_{i}", "c", "eng")
            b = sb.should_bypass("freshness_filter", f"hid_{i}", "c", "eng")
            if a != b:
                return
        pytest.fail(
            "min_confidence and freshness_filter agreed on every headline — "
            "hash is not filter-scoped"
        )


class TestSamplerCap:
    def test_max_cf_per_headline_caps_bypasses(self):
        """At rate=1.0 only `max_cf_per_headline` bypasses fire per headline."""
        cfg = CounterfactualConfig(
            enabled=True, sample_rate_per_filter=1.0,
            max_cf_per_headline=3,
        )
        s = CounterfactualSampler(cfg)
        # Spam the same headline across 20 conditions. Only 3 should pass.
        n_bypass = sum(
            1 for i in range(20)
            if s.should_bypass("min_confidence", "h_one", f"cond_{i}", "eng")
        )
        assert n_bypass == 3, (
            f"expected exactly 3 bypasses (cap=3), got {n_bypass}"
        )

    def test_reset_headline_counter_releases_cap(self):
        cfg = CounterfactualConfig(
            enabled=True, sample_rate_per_filter=1.0,
            max_cf_per_headline=2,
        )
        s = CounterfactualSampler(cfg)
        # Hit the cap.
        for i in range(2):
            assert s.should_bypass("min_confidence", "h", f"c{i}", "e") is True
        assert s.should_bypass("min_confidence", "h", "c_extra", "e") is False
        # Reset → cap is fresh.
        s.reset_headline_counter("h")
        assert s.should_bypass("min_confidence", "h", "c0", "e") is True


class TestSamplerFilterScope:
    def test_filter_not_in_enabled_set_never_bypasses(self):
        """Filters outside `enabled_filters` are pass-through (no bypass)."""
        cfg = CounterfactualConfig(
            enabled=True, sample_rate_per_filter=1.0,
            enabled_filters={"min_confidence"},
        )
        s = CounterfactualSampler(cfg)
        # Enabled filter does bypass.
        assert s.should_bypass("min_confidence", "h", "c", "e") is True
        # Disabled filter never bypasses, even at rate=1.0.
        for i in range(20):
            assert s.should_bypass("freshness_filter", f"h{i}", "c", "e") is False

    def test_default_enabled_filters_is_top_5(self):
        cfg = CounterfactualConfig(enabled=True)
        s = CounterfactualSampler(cfg)
        assert s.enabled_filters == TOP_5_FILTERS

    def test_missing_identifier_disables_bypass(self):
        """No headline_id / no condition_id / no engine → no bypass."""
        cfg = CounterfactualConfig(
            enabled=True, sample_rate_per_filter=1.0,
        )
        s = CounterfactualSampler(cfg)
        assert s.should_bypass("min_confidence", None, "c", "e") is False
        assert s.should_bypass("min_confidence", "h", None, "e") is False
        assert s.should_bypass("min_confidence", "h", "c", None) is False


class TestLoadFromYaml:
    def test_empty_yields_defaults(self):
        cfg = load_from_yaml(None)
        assert cfg.enabled is False
        assert cfg.sample_rate_per_filter == 0.01
        assert cfg.max_cf_per_headline == 3
        assert cfg.enabled_filters is None

    def test_explicit_values_load_through(self):
        cfg = load_from_yaml({
            "enabled": True,
            "sample_rate_per_filter": 0.05,
            "max_cf_per_headline": 7,
            "enabled_filters": ["min_confidence", "min_edge"],
        })
        assert cfg.enabled is True
        assert cfg.sample_rate_per_filter == 0.05
        assert cfg.max_cf_per_headline == 7
        assert cfg.enabled_filters == {"min_confidence", "min_edge"}


# ---------------------------------------------------------------------------
# DB migration
# ---------------------------------------------------------------------------


class TestMigration:
    @pytest.mark.asyncio
    async def test_cf_column_added_to_trades_and_engine_results(self):
        db = SeulgiDB(":memory:")
        await db.connect()
        try:
            async with db._conn.execute("PRAGMA table_info(trades)") as cur:
                trade_cols = {row[1] for row in await cur.fetchall()}
            async with db._conn.execute(
                "PRAGMA table_info(engine_results)"
            ) as cur:
                er_cols = {row[1] for row in await cur.fetchall()}
            assert "cf_bypassed_filter" in trade_cols
            assert "cf_bypassed_filter" in er_cols
        finally:
            await db.close()

    @pytest.mark.asyncio
    async def test_migration_is_idempotent(self):
        """Re-running `_migrate` on a connected DB must not fail."""
        db = SeulgiDB(":memory:")
        await db.connect()
        try:
            # Second migrate pass should silently skip duplicate-column errors.
            await db._migrate()
            await db._migrate()
            async with db._conn.execute("PRAGMA table_info(trades)") as cur:
                cols = [row[1] for row in await cur.fetchall()]
            # Column appears exactly once.
            assert cols.count("cf_bypassed_filter") == 1
        finally:
            await db.close()

    @pytest.mark.asyncio
    async def test_insert_trade_defaults_cf_column_to_null(self):
        """Existing callers that don't pass cf_bypassed_filter keep working."""
        db = SeulgiDB(":memory:")
        await db.connect()
        try:
            sid = await db.insert_signal(
                site_url="https://a.test", headline="h",
                headline_id="hid",
            )
            tid = await db.insert_trade(
                signal_id=sid, condition_id="c1", question="q?",
                token_id="t1", direction="YES", similarity=0.7,
                p_hat=0.55, mid_price=0.5, size_usdc=10.0,
                skip_reason=None, headline_id="hid",
            )
            async with db._conn.execute(
                "SELECT cf_bypassed_filter FROM trades WHERE id = ?", (tid,)
            ) as cur:
                row = await cur.fetchone()
            assert row[0] is None

            tid2 = await db.insert_trade(
                signal_id=sid, condition_id="c1", question="q?",
                token_id="t1", direction="YES", similarity=0.7,
                p_hat=0.55, mid_price=0.5, size_usdc=10.0,
                skip_reason=None, headline_id="hid",
                cf_bypassed_filter="min_confidence",
            )
            async with db._conn.execute(
                "SELECT cf_bypassed_filter FROM trades WHERE id = ?", (tid2,)
            ) as cur:
                row = await cur.fetchone()
            assert row[0] == "min_confidence"
        finally:
            await db.close()


# ---------------------------------------------------------------------------
# Trader wiring — `_evaluate_pure` consults cf_sampler before skip
# ---------------------------------------------------------------------------


def _make_cfg_trader():
    cfg = MagicMock()
    cfg.balance_usdc = 1000.0
    cfg.max_position_usdc = 50.0
    cfg.min_trade_usdc = 5.0
    cfg.min_edge = 0.04
    cfg.edge_increment = 0.12
    cfg.position_timeout_hours = 24.0
    cfg.max_open_positions = 20
    cfg.max_unique_markets = 15
    cfg.cold_position_hours = 12.0
    cfg.infinite_balance = False
    cfg.invert_high_conf = False
    cfg.invert_conf_threshold = 0.85
    cfg.rail_skip_threshold = 0.0
    cfg.entry_rail_skip_threshold = 0.0
    cfg.take_profit = 0.0
    cfg.stop_loss = 0.0
    cfg.trailing_stop = 0.0
    cfg.min_hold_seconds = 0.0
    cfg.spread_cost_bps = 0.0
    cfg.min_entry_yes_mid = 0.0
    cfg.stop_loss_pct = 0.0
    cfg.max_market_vol_30m = 0.0
    cfg.simulate_fees = None
    cfg.simulate_sizing = None
    cfg.simulate_slippage = None
    cfg.direction_filter = ""
    cfg.freshness_filter = ""
    cfg.mid_yes_min = 0.0
    cfg.mid_yes_max = 1.0
    cfg.use_true_ev_formula = False
    cfg.min_ev_per_dollar = 0.0
    # A4D.3: tests in this file probe the legacy binary `duplicate_position`
    # skip + cf-bypass interaction. Position scaling is OFF here.
    cfg.position_scaling_enabled = False
    cfg.max_exposure_per_condition_usdc = 0.0
    cfg.simulate_min_trade_usdc = 1.0
    return cfg


def _make_ai_cfg():
    ai = MagicMock()
    ai.min_confidence = 0.45
    ai.min_agreement = 0.0
    return ai


class TestTraderBypass:
    """End-to-end: cf-sampler attached → duplicate-position trade goes through.

    `duplicate_position` is the cleanest filter to wire-test because it
    fires AFTER min_confidence/edge/mid_band checks, so a well-behaved
    AI result that would normally land lands here too — the only thing
    we need to mock is `has_open_position=True`.
    """

    @pytest.mark.asyncio
    async def test_duplicate_position_bypass_lets_trade_through(self):
        from trader import PaperTrader  # noqa: F401

        db = MagicMock()
        db.insert_signal = AsyncMock(return_value=1)
        db.insert_trade = AsyncMock(return_value=10)
        db.open_position = AsyncMock()
        db.count_open_positions = AsyncMock(return_value=0)
        # Force the duplicate_position skip path.
        db.has_open_position = AsyncMock(return_value=True)
        db.count_unique_open_markets = AsyncMock(return_value=0)

        cfg = _make_cfg_trader()
        ai_cfg = _make_ai_cfg()

        cf_cfg = CounterfactualConfig(
            enabled=True, sample_rate_per_filter=1.0,
            max_cf_per_headline=10,
            enabled_filters={"duplicate_position"},
        )
        sampler = CounterfactualSampler(cf_cfg)

        trader = PaperTrader(
            db=db, classifier=MagicMock(), cfg=cfg, ai_cfg=ai_cfg,
            cf_sampler=sampler,
        )
        trader._balance = 1000.0
        # Confidence 0.85 → edge 0.084 > min_edge 0.04 → would normally
        # trade; the duplicate_position guard is what blocks.
        trader._classifier.classify_batch = AsyncMock(
            return_value=[ClassifyResult(Direction.YES, 0.85, "strong")]
        )
        session = MagicMock()
        session.closed = False
        trader._session = session

        from watcher import RawHeadline
        from datetime import datetime, timezone
        headline = RawHeadline(
            site_url="https://test", headline="h",
            article_url="https://test/x",
            detected_at=datetime.now(timezone.utc),
        )
        object.__setattr__(headline, "headline_id", "hid_xyz")
        match = MarketMatch(
            condition_id="cid1", question="Q?",
            token_yes="ty1", token_no="tn1", similarity=0.75,
            mid_yes=0.45, mid_no=0.55,
        )

        with patch("trader._fetch_mid_price", AsyncMock(return_value=0.45)):
            decisions = await trader.process(headline, [match])

        assert len(decisions) == 1
        d = decisions[0]
        assert d.executed, (
            f"expected executed; got skip_reason={d.skip_reason!r}"
        )
        assert d.cf_bypassed_filter == "duplicate_position"
        assert db.insert_trade.called
        kwargs = db.insert_trade.call_args.kwargs
        assert kwargs["cf_bypassed_filter"] == "duplicate_position"

    @pytest.mark.asyncio
    async def test_no_sampler_attached_keeps_legacy_skip_behaviour(self):
        """When `cf_sampler=None`, the trader hard-skips on duplicate_position."""
        from trader import PaperTrader

        db = MagicMock()
        db.insert_signal = AsyncMock(return_value=1)
        db.insert_trade = AsyncMock(return_value=10)
        db.open_position = AsyncMock()
        db.count_open_positions = AsyncMock(return_value=0)
        db.has_open_position = AsyncMock(return_value=True)
        db.count_unique_open_markets = AsyncMock(return_value=0)

        cfg = _make_cfg_trader()
        ai_cfg = _make_ai_cfg()
        trader = PaperTrader(
            db=db, classifier=MagicMock(), cfg=cfg, ai_cfg=ai_cfg,
        )
        trader._balance = 1000.0
        trader._classifier.classify_batch = AsyncMock(
            return_value=[ClassifyResult(Direction.YES, 0.85, "strong")]
        )
        session = MagicMock()
        session.closed = False
        trader._session = session

        from watcher import RawHeadline
        from datetime import datetime, timezone
        headline = RawHeadline(
            site_url="https://test", headline="h",
            article_url="https://test/x",
            detected_at=datetime.now(timezone.utc),
        )
        object.__setattr__(headline, "headline_id", "hid")
        match = MarketMatch(
            condition_id="cid1", question="Q?",
            token_yes="ty1", token_no="tn1", similarity=0.75,
            mid_yes=0.45, mid_no=0.55,
        )
        with patch("trader._fetch_mid_price", AsyncMock(return_value=0.45)):
            decisions = await trader.process(headline, [match])
        d = decisions[0]
        assert not d.executed
        assert d.skip_reason == "duplicate_position"
        assert d.cf_bypassed_filter is None
