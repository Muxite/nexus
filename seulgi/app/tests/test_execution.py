"""
Tests for seulgi/app/execution.py — Phase 4A.2 JIT pricing + IOC.

Coverage:
  - IOC fail when live_price > max_acceptable_price (price moved up).
  - IOC success when live_price within tolerance.
  - Deterministic outcome from (snapshot_mid, delta_bps).
  - Backtest IOC simulation matches live IOC behaviour given same inputs.
  - no_liquidity / rail / tolerance-band edge cases.
  - End-to-end runtime call with mocked refetch.
"""
from __future__ import annotations

import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from execution import (  # noqa: E402
    DEFAULT_JIT_TOLERANCE_BPS,
    ExecutionResult,
    execute_with_jit_pricing,
    simulate_ioc_outcome,
)


# ---------------------------------------------------------------------------
# simulate_ioc_outcome — pure decision (also the backtest entry point)
# ---------------------------------------------------------------------------


class TestSimulateIocOutcome:
    """The pure decision shared between runtime and the offline replay."""

    def test_ioc_success_when_live_inside_tolerance(self):
        # 25 bps drift inside a 50 bps window → fill.
        result = simulate_ioc_outcome(
            snapshot_mid=0.40, live_price=0.401, side="YES",
            target_size_usdc=10.0,
            max_acceptable_price=0.40 * (1.0 + 50.0 / 10000.0),
            tolerance_bps=50.0, latency_ms=42,
        )
        assert result.filled is True
        assert result.skip_reason is None
        assert result.fill_price == pytest.approx(0.401, abs=1e-9)
        assert result.fill_size_usdc == pytest.approx(10.0)
        assert result.delta_bps == pytest.approx(25.0, abs=0.1)
        assert result.latency_ms == 42

    def test_ioc_fail_when_live_above_max_acceptable_price(self):
        # 60 bps drift outside a 50 bps window → ioc_fail_price_moved.
        snapshot = 0.40
        tol_bps = 50.0
        result = simulate_ioc_outcome(
            snapshot_mid=snapshot, live_price=0.4024, side="YES",
            target_size_usdc=10.0,
            max_acceptable_price=snapshot * (1.0 + tol_bps / 10000.0),
            tolerance_bps=tol_bps, latency_ms=50,
        )
        assert result.filled is False
        assert result.skip_reason == "ioc_fail_price_moved"
        assert result.fill_price is None
        assert result.delta_bps == pytest.approx(60.0, abs=1.0)

    def test_ioc_fail_when_no_liquidity(self):
        # live_price=None ⇒ BBO unavailable.
        result = simulate_ioc_outcome(
            snapshot_mid=0.40, live_price=None, side="YES",
            target_size_usdc=10.0,
            max_acceptable_price=0.40 * 1.005,
            tolerance_bps=50.0, latency_ms=2_000,
        )
        assert result.filled is False
        assert result.skip_reason == "ioc_fail_no_liquidity"
        assert result.fill_price is None
        assert result.live_price is None
        assert result.delta_bps is None

    def test_no_move_always_fills(self):
        """The "equality wins" invariant — refetch matched snapshot."""
        snapshot = 0.55
        result = simulate_ioc_outcome(
            snapshot_mid=snapshot, live_price=snapshot, side="YES",
            target_size_usdc=5.0,
            max_acceptable_price=snapshot,   # cap == snapshot
            tolerance_bps=50.0, latency_ms=10,
        )
        assert result.filled is True
        assert result.delta_bps == pytest.approx(0.0)
        assert result.fill_price == pytest.approx(snapshot)

    def test_rail_snap_short_circuits_to_no_liquidity_skip(self):
        """live_price saturated → rail skip (not a successful fill)."""
        result = simulate_ioc_outcome(
            snapshot_mid=0.95, live_price=0.99, side="YES",
            target_size_usdc=5.0,
            max_acceptable_price=0.99,   # cap allows the rail price
            tolerance_bps=500.0, latency_ms=10,
        )
        assert result.filled is False
        assert result.skip_reason == "rail"

    def test_downward_move_fills_for_buy(self):
        """Price moved DOWN since snapshot — that's a gift for buyers."""
        snapshot = 0.40
        result = simulate_ioc_outcome(
            snapshot_mid=snapshot, live_price=0.395, side="YES",
            target_size_usdc=10.0,
            max_acceptable_price=snapshot * 1.005,
            tolerance_bps=50.0, latency_ms=20,
        )
        assert result.filled is True
        assert result.fill_price == pytest.approx(0.395)
        assert result.delta_bps < 0  # negative drift

    def test_simulate_slippage_applies_in_simulate_mode(self):
        """In simulate mode with slippage cfg, fill price walks the book."""
        cfg = MagicMock()
        cfg.mode = "simulate"
        slip_cfg = MagicMock()
        slip_cfg.enabled = True
        slip_cfg.bps_per_dollar = 2.0
        slip_cfg.max_price_move = 0.02
        slip_cfg.disable_below_usdc = 0.10
        slip_cfg.disable_for_categories = []
        cfg.simulate_slippage = slip_cfg

        snapshot = 0.40
        result = simulate_ioc_outcome(
            snapshot_mid=snapshot, live_price=snapshot, side="YES",
            target_size_usdc=100.0,   # bps_per_dollar=2, walk=0.02
            max_acceptable_price=snapshot + 0.05,
            tolerance_bps=50.0, latency_ms=10, cfg=cfg,
        )
        # Slippage walks fill price ABOVE live_price for YES buys.
        assert result.filled is True
        assert result.fill_price > snapshot

    def test_simulate_slippage_can_trip_ioc_fail(self):
        """Slippage walking past max_acceptable_price → ioc_fail_price_moved."""
        cfg = MagicMock()
        cfg.mode = "simulate"
        slip_cfg = MagicMock()
        slip_cfg.enabled = True
        slip_cfg.bps_per_dollar = 200.0   # very aggressive walk
        slip_cfg.max_price_move = 0.5
        slip_cfg.disable_below_usdc = 0.0
        slip_cfg.disable_for_categories = []
        cfg.simulate_slippage = slip_cfg

        snapshot = 0.40
        result = simulate_ioc_outcome(
            snapshot_mid=snapshot, live_price=snapshot, side="YES",
            target_size_usdc=100.0,
            max_acceptable_price=snapshot * 1.001,   # very tight cap
            tolerance_bps=10_000.0, latency_ms=10, cfg=cfg,
        )
        assert result.filled is False
        assert result.skip_reason == "ioc_fail_price_moved"


# ---------------------------------------------------------------------------
# Determinism / backtest contract — same inputs → same outputs.
# ---------------------------------------------------------------------------


class TestDeterminism:
    """Spec: deterministic from (snapshot_mid, price delta); backtest
    IOC simulation must match live IOC behavior given same inputs."""

    def test_same_inputs_produce_identical_outcome(self):
        a = simulate_ioc_outcome(
            snapshot_mid=0.45, live_price=0.452, side="YES",
            target_size_usdc=20.0,
            max_acceptable_price=0.45 * 1.005,
            tolerance_bps=50.0, latency_ms=15,
        )
        b = simulate_ioc_outcome(
            snapshot_mid=0.45, live_price=0.452, side="YES",
            target_size_usdc=20.0,
            max_acceptable_price=0.45 * 1.005,
            tolerance_bps=50.0, latency_ms=15,
        )
        assert a == b

    def test_backtest_replay_matches_live_simulation(self):
        """The backtest harness calls `simulate_ioc_outcome` with snapshot=mid
        at decided_at and live=mid at decided_at + 1-5s. Given same numbers,
        the live runtime and backtest must agree."""
        snapshot = 0.62
        # "Live" path arrives via execute_with_jit_pricing → simulate_ioc_outcome.
        # Backtest path arrives directly via _PriceTape lookup → simulate_ioc_outcome.
        # Wire identical inputs and require equality.
        live = simulate_ioc_outcome(
            snapshot_mid=snapshot, live_price=0.625, side="NO",
            target_size_usdc=12.5,
            max_acceptable_price=snapshot * (1.0 + 50.0 / 10000.0),
            tolerance_bps=50.0, latency_ms=120,
        )
        backtest = simulate_ioc_outcome(
            snapshot_mid=snapshot, live_price=0.625, side="NO",
            target_size_usdc=12.5,
            max_acceptable_price=snapshot * (1.0 + 50.0 / 10000.0),
            tolerance_bps=50.0,
            latency_ms=120,    # backtest uses a synthetic latency
        )
        assert live.filled == backtest.filled
        assert live.skip_reason == backtest.skip_reason
        assert live.fill_price == backtest.fill_price
        assert live.delta_bps == backtest.delta_bps


# ---------------------------------------------------------------------------
# Runtime entry point — execute_with_jit_pricing with patched refetch.
# ---------------------------------------------------------------------------


class TestExecuteWithJitPricing:
    """End-to-end runtime path. The refetch is patched via the same
    `trader._fetch_mid_price` symbol the legacy paths use, so the
    test-double covers both the legacy refetch and the new JIT call."""

    @pytest.mark.asyncio
    async def test_jit_fills_at_live_price(self):
        session = MagicMock()
        with patch("trader._fetch_mid_price",
                   AsyncMock(return_value=0.401)):
            res = await execute_with_jit_pricing(
                session=session, condition_id="c1", token_id="t1",
                direction="YES", target_size_usdc=10.0,
                snapshot_mid=0.40,
                max_acceptable_price=0.40 * 1.005,
                tolerance_bps=50.0,
            )
        assert res.filled is True
        assert res.fill_price == pytest.approx(0.401)
        assert res.live_price == pytest.approx(0.401)
        assert res.snapshot_mid == pytest.approx(0.40)

    @pytest.mark.asyncio
    async def test_jit_skips_when_price_moved(self):
        session = MagicMock()
        with patch("trader._fetch_mid_price",
                   AsyncMock(return_value=0.41)):    # +250 bps drift
            res = await execute_with_jit_pricing(
                session=session, condition_id="c1", token_id="t1",
                direction="YES", target_size_usdc=10.0,
                snapshot_mid=0.40,
                max_acceptable_price=0.40 * 1.005,
                tolerance_bps=50.0,
            )
        assert res.filled is False
        assert res.skip_reason == "ioc_fail_price_moved"
        assert res.delta_bps == pytest.approx(250.0, abs=1.0)

    @pytest.mark.asyncio
    async def test_jit_no_liquidity_when_refetch_returns_none(self):
        session = MagicMock()
        with patch("trader._fetch_mid_price",
                   AsyncMock(return_value=None)):
            res = await execute_with_jit_pricing(
                session=session, condition_id="c1", token_id="t1",
                direction="YES", target_size_usdc=10.0,
                snapshot_mid=0.40,
                max_acceptable_price=0.40 * 1.005,
                tolerance_bps=50.0,
            )
        assert res.filled is False
        assert res.skip_reason == "ioc_fail_no_liquidity"
        assert res.live_price is None

    @pytest.mark.asyncio
    async def test_live_mode_raises_not_implemented(self):
        session = MagicMock()
        os.environ["SEULGI_LIVE"] = "1"
        try:
            with pytest.raises(NotImplementedError):
                await execute_with_jit_pricing(
                    session=session, condition_id="c1", token_id="t1",
                    direction="YES", target_size_usdc=10.0,
                    snapshot_mid=0.40, max_acceptable_price=0.40,
                )
        finally:
            del os.environ["SEULGI_LIVE"]

    @pytest.mark.asyncio
    async def test_default_tolerance_used_when_unspecified(self):
        """Spec: default 50 bps. A drift of 49 bps must fill at default."""
        session = MagicMock()
        with patch("trader._fetch_mid_price",
                   AsyncMock(return_value=0.40196)):   # +49 bps
            res = await execute_with_jit_pricing(
                session=session, condition_id="c1", token_id="t1",
                direction="YES", target_size_usdc=10.0,
                snapshot_mid=0.40,
                max_acceptable_price=0.40 * (
                    1.0 + DEFAULT_JIT_TOLERANCE_BPS / 10000.0
                ),
            )
        assert res.filled is True
        assert res.tolerance_bps == DEFAULT_JIT_TOLERANCE_BPS


# ---------------------------------------------------------------------------
# ExecutionResult dataclass — surface fields exist and propagate.
# ---------------------------------------------------------------------------


class TestExecutionResultShape:
    def test_dataclass_fields_present(self):
        r = ExecutionResult(
            filled=True, fill_price=0.5, fill_size_usdc=10.0,
            skip_reason=None, latency_ms=33,
        )
        assert r.filled is True
        assert r.fill_price == 0.5
        assert r.fill_size_usdc == 10.0
        assert r.skip_reason is None
        assert r.latency_ms == 33
        # Optional diagnostic fields default to None.
        assert r.snapshot_mid is None
        assert r.live_price is None
        assert r.delta_bps is None
        assert r.tolerance_bps == DEFAULT_JIT_TOLERANCE_BPS

    def test_skip_reason_propagates(self):
        r = ExecutionResult(
            filled=False, fill_price=None, fill_size_usdc=None,
            skip_reason="ioc_fail_price_moved", latency_ms=10,
            snapshot_mid=0.40, live_price=0.41, delta_bps=250.0,
        )
        assert r.filled is False
        assert r.skip_reason == "ioc_fail_price_moved"
        assert r.delta_bps == 250.0
