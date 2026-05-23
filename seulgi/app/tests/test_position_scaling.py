"""
Phase 4D.3 (2026-05-23) — position scaling: delta-adds replace the binary
`duplicate_position` lockout.

Old: any open size on `(condition_id, engine)` → hard-skip every
subsequent reinforcing headline with `Skip("duplicate_position")`.

New: a reinforcing headline is sized as a TARGET total exposure (the
Kelly stake) on the condition; if the engine already holds < target,
the trade fires for the DELTA. If the engine holds ≥ target, the trade
skips with a finer-grained reason. A per-condition exposure cap
truncates the delta or blocks it outright.

These tests cover:
- empty exposure → full kelly_size fires;
- existing exposure < kelly_size → delta-add fires;
- existing exposure ≥ kelly_size → "exposure_already_at_or_above_optimal";
- delta < min_trade floor → "delta_below_min_trade";
- `max_exposure_per_condition_usdc` cap enforced (both pre-add block + delta truncate);
- weighted-mean SL/TP across 2 legs;
- legacy binary lockout still available via `position_scaling_enabled=False`;
- `OpenPositionsState` exposure / multi-leg accounting (list-per-key).
"""

import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ai import ClassifyResult, Direction
from fastpath import OpenPositionsState
from markets import MarketMatch
from trader import _EntryCtx, _close_pure, _evaluate_pure


# ---------------------------------------------------------------------------
# Helpers — same shape as test_trader._pure_cfg / _pure_ctx but with the
# A4D.3 knobs default-ON.
# ---------------------------------------------------------------------------


def _pure_cfg(**overrides):
    class C:
        balance_usdc = 1000.0
        max_position_usdc = 100.0
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
        simulate_min_trade_usdc = 5.0
        # A4D.3 knobs — scaling default-ON in this file.
        position_scaling_enabled = True
        max_exposure_per_condition_usdc = 100.0

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


def _ctx(cfg, *, current_exposure=0.0, duplicate=False, **kw):
    return _EntryCtx(
        cfg=cfg, ai_cfg=_pure_ai_cfg(), engine_name="balanced",
        parent_engine="balanced",
        open_positions=kw.get("open_positions", 0),
        unique_markets=kw.get("unique", 0),
        duplicate_position=duplicate,
        balance=kw.get("balance", 1000.0),
        cluster_factor=kw.get("cluster_factor", 1.0),
        rules_post_ai_reason=None, freshness_label=None,
        current_exposure=current_exposure,
    )


def _match(condition_id="cid1", mid_yes=0.45):
    m = MarketMatch(condition_id, "Will Fed cut?", "ty1", "tn1", 0.75)
    m.mid_yes = mid_yes
    m.mid_no = None
    return m


def _result_strong_yes() -> ClassifyResult:
    return ClassifyResult(Direction.YES, 0.85, "strong bullish")


# ---------------------------------------------------------------------------
# Delta-add behaviour
# ---------------------------------------------------------------------------


class TestDeltaAdds:

    def test_empty_exposure_full_kelly_fires(self):
        """No prior exposure → kelly_size is the full trade."""
        cfg = _pure_cfg(max_position_usdc=80.0)
        out = _evaluate_pure(
            _match(), _result_strong_yes(),
            _ctx(cfg, duplicate=False, current_exposure=0.0),
        )
        assert out.action == "TRADE"
        # paper-mode sizing: min(max_position, balance) = 80.
        assert out.size_usdc == pytest.approx(80.0)
        assert out.skip_reason is None

    def test_existing_50_kelly_80_delta_30_fires(self):
        """current_exposure=50, kelly target=80 → delta-add at 30."""
        cfg = _pure_cfg(max_position_usdc=80.0)
        out = _evaluate_pure(
            _match(), _result_strong_yes(),
            _ctx(cfg, duplicate=True, current_exposure=50.0),
        )
        assert out.action == "TRADE", (
            f"expected delta-add to fire; got {out.skip_reason!r}"
        )
        assert out.size_usdc == pytest.approx(30.0)
        # The position_scale step should appear in the trace with the delta.
        ps_step = next(s for s in out.trader_trace
                       if s.get("step") == "position_scale")
        assert ps_step["pass"] is True
        assert ps_step["current_exposure"] == pytest.approx(50.0)
        assert ps_step["delta_size"] == pytest.approx(30.0)

    def test_existing_80_kelly_50_skips_above_optimal(self):
        """current_exposure=80, kelly target=50 → skip exposure_already_at_or_above_optimal."""
        cfg = _pure_cfg(max_position_usdc=50.0)
        out = _evaluate_pure(
            _match(), _result_strong_yes(),
            _ctx(cfg, duplicate=True, current_exposure=80.0),
        )
        assert out.action == "SKIP"
        assert out.skip_reason == (
            "duplicate_position:exposure_already_at_or_above_optimal"
        )

    def test_existing_98_kelly_100_delta_2_below_min_trade_skips(self):
        """current_exposure=98, kelly target=100, delta=2 < min_trade=5 → skip."""
        cfg = _pure_cfg(
            max_position_usdc=100.0, simulate_min_trade_usdc=5.0,
        )
        out = _evaluate_pure(
            _match(), _result_strong_yes(),
            _ctx(cfg, duplicate=True, current_exposure=98.0),
        )
        assert out.action == "SKIP"
        assert out.skip_reason == "duplicate_position:delta_below_min_trade"

    def test_delta_floor_uses_simulate_min_trade_usdc(self):
        """The min-trade floor for the delta path is `simulate_min_trade_usdc`."""
        # delta would be 3.0 — above paper `min_trade_usdc=2.0` floor but
        # below the `simulate_min_trade_usdc=5.0` floor used by the
        # position-scale gate.
        cfg = _pure_cfg(
            max_position_usdc=80.0, min_trade_usdc=2.0,
            simulate_min_trade_usdc=5.0,
        )
        out = _evaluate_pure(
            _match(), _result_strong_yes(),
            _ctx(cfg, duplicate=True, current_exposure=77.0),
        )
        assert out.action == "SKIP"
        assert out.skip_reason == "duplicate_position:delta_below_min_trade"


# ---------------------------------------------------------------------------
# `max_exposure_per_condition_usdc` cap
# ---------------------------------------------------------------------------


class TestExposureCap:

    def test_at_cap_blocks(self):
        """current_exposure already at the cap → skip at_max_exposure_cap."""
        cfg = _pure_cfg(
            max_position_usdc=100.0, max_exposure_per_condition_usdc=100.0,
        )
        out = _evaluate_pure(
            _match(), _result_strong_yes(),
            _ctx(cfg, duplicate=True, current_exposure=100.0),
        )
        assert out.action == "SKIP"
        assert out.skip_reason == "duplicate_position:at_max_exposure_cap"

    def test_cap_truncates_delta(self):
        """kelly_size > cap room — delta is truncated so total stays under cap.

        current_exposure=80, kelly_size=120, cap=100 →
        room=20 → delta truncated to 20.
        """
        cfg = _pure_cfg(
            max_position_usdc=120.0, max_exposure_per_condition_usdc=100.0,
        )
        out = _evaluate_pure(
            _match(), _result_strong_yes(),
            _ctx(cfg, duplicate=True, current_exposure=80.0),
        )
        assert out.action == "TRADE"
        assert out.size_usdc == pytest.approx(20.0)


# ---------------------------------------------------------------------------
# Backwards-compat: legacy binary lockout via `position_scaling_enabled=False`
# ---------------------------------------------------------------------------


class TestLegacyLockout:

    def test_position_scaling_off_keeps_binary_lockout(self):
        cfg = _pure_cfg(position_scaling_enabled=False)
        out = _evaluate_pure(
            _match(), _result_strong_yes(),
            _ctx(cfg, duplicate=True, current_exposure=50.0),
        )
        assert out.action == "SKIP"
        assert out.skip_reason == "duplicate_position"


# ---------------------------------------------------------------------------
# Weighted-mean SL/TP across multiple legs (close path)
# ---------------------------------------------------------------------------


class TestWeightedClose:
    """Per-leg PnL math stays per-leg, but the TP/SL gate uses the
    share-weighted entry across legs so a basket-as-a-whole signal drives
    the close decision."""

    def _leg(self, *, entry_price, shares, opened_minutes_ago=15.0):
        opened = (datetime.now(timezone.utc)
                  - timedelta(minutes=opened_minutes_ago))
        return {
            "trade_id": 0, "condition_id": "c1", "token_id": "t1",
            "direction": "YES",
            "size_usdc": entry_price * shares,
            "entry_price": entry_price, "shares": shares,
            "opened_at": opened.isoformat(),
            "last_price": entry_price, "p_hat": 0.55,
            "market_type": None, "high_water_price": None,
        }

    def test_weighted_entry_keeps_basket_above_sl(self):
        """Leg A: 100 shares @ 0.40. Leg B: 100 shares @ 0.50.
        weighted = 0.45. cfg.stop_loss = 0.06. Current 0.41.
        Leg A's per-leg basis (0.40 - 0.41 = +0.01) wouldn't trip SL anyway,
        but Leg B's per-leg basis (0.50 - 0.41 = -0.09) WOULD trip its
        own SL at -0.06. With the weighted entry the gate sees
        (0.41 - 0.45 = -0.04) which stays above -0.06 → HOLD.
        """
        cfg = _pure_cfg(stop_loss=0.06, take_profit=0.0, trailing_stop=0.0)
        leg_a = self._leg(entry_price=0.40, shares=100)
        leg_b = self._leg(entry_price=0.50, shares=100)
        # share-weighted entry across the basket.
        total_shares = leg_a["shares"] + leg_b["shares"]
        weighted = (
            leg_a["entry_price"] * leg_a["shares"]
            + leg_b["entry_price"] * leg_b["shares"]
        ) / total_shares
        assert weighted == pytest.approx(0.45)
        # Leg B alone WOULD trip its own SL.
        legacy = _close_pure(
            leg_b, current_price=0.41, cfg=cfg, now=datetime.now(timezone.utc),
        )
        assert legacy.action == "CLOSE"
        assert legacy.close_reason == "SL"
        # Basket-weighted: still inside the SL band.
        out = _close_pure(
            leg_b, current_price=0.41, cfg=cfg,
            now=datetime.now(timezone.utc), weighted_entry_price=weighted,
        )
        assert out.action == "HOLD", (
            f"expected basket-weighted close to HOLD; got "
            f"action={out.action!r} reason={out.close_reason!r}"
        )

    def test_weighted_entry_fires_basket_sl(self):
        """If price moves far enough below the weighted entry, the gate
        fires on the basket. PnL math still uses the leg's per-leg entry
        so realized_pnl is computed against the LEG's own basis."""
        cfg = _pure_cfg(stop_loss=0.06, take_profit=0.0, trailing_stop=0.0)
        leg_a = self._leg(entry_price=0.40, shares=100)
        leg_b = self._leg(entry_price=0.50, shares=100)
        weighted = 0.45
        # Current 0.38 → basket = 0.38 - 0.45 = -0.07 → trip SL.
        out = _close_pure(
            leg_a, current_price=0.38, cfg=cfg,
            now=datetime.now(timezone.utc), weighted_entry_price=weighted,
        )
        assert out.action == "CLOSE"
        assert out.close_reason == "SL"
        # Per-leg PnL is still computed against leg_a's own 0.40 entry.
        # leg_a: 100 shares × (0.38 - 0.40) = -2.0
        assert out.unrealized_pnl == pytest.approx(-2.0)


# ---------------------------------------------------------------------------
# OpenPositionsState — multi-leg accounting (lists per key)
# ---------------------------------------------------------------------------


class TestOpenPositionsStateMultiLeg:

    @pytest.mark.asyncio
    async def test_exposure_sums_size_usdc_across_legs(self):
        s = OpenPositionsState()
        await s.add("cid_x", "scout", {
            "trade_id": 1, "size_usdc": 30.0, "shares": 60.0,
            "entry_price": 0.5, "direction": "YES",
        })
        await s.add("cid_x", "scout", {
            "trade_id": 2, "size_usdc": 50.0, "shares": 100.0,
            "entry_price": 0.5, "direction": "YES",
        })
        assert await s.exposure("cid_x", "scout") == pytest.approx(80.0)
        assert await s.exposure("cid_x", "other") == 0.0

    @pytest.mark.asyncio
    async def test_count_treats_multi_leg_as_one_open_position(self):
        """`count()` is per-`(cond_id, engine)` (the cap unit), NOT per-leg.
        Multiple legs on the same key count once."""
        s = OpenPositionsState()
        await s.add("cid_x", "scout", {"trade_id": 1, "size_usdc": 30.0})
        await s.add("cid_x", "scout", {"trade_id": 2, "size_usdc": 50.0})
        await s.add("cid_y", "scout", {"trade_id": 3, "size_usdc": 10.0})
        assert await s.count("scout") == 2
        assert await s.unique_markets("scout") == 2

    @pytest.mark.asyncio
    async def test_remove_by_trade_id_drops_single_leg(self):
        s = OpenPositionsState()
        await s.add("cid_x", "scout", {"trade_id": 1, "size_usdc": 30.0})
        await s.add("cid_x", "scout", {"trade_id": 2, "size_usdc": 50.0})
        assert await s.remove_by_trade_id("cid_x", "scout", 1) is True
        # Bucket still has the second leg.
        assert await s.has("cid_x", "scout") is True
        assert await s.exposure("cid_x", "scout") == pytest.approx(50.0)
        # Idempotent — same trade_id again returns False.
        assert await s.remove_by_trade_id("cid_x", "scout", 1) is False
        # Drop the last leg → key cleared.
        assert await s.remove_by_trade_id("cid_x", "scout", 2) is True
        assert await s.has("cid_x", "scout") is False

    @pytest.mark.asyncio
    async def test_positions_for_returns_deep_copies(self):
        s = OpenPositionsState()
        await s.add("cid_x", "scout", {"trade_id": 1, "size_usdc": 30.0})
        legs = await s.positions_for("cid_x", "scout")
        assert len(legs) == 1
        legs[0]["size_usdc"] = 9999
        # Internal state not mutated.
        assert await s.exposure("cid_x", "scout") == pytest.approx(30.0)
