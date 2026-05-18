"""Variant catalog — all surviving (post-2026-05-17 retirement) legacy
strategies expressed as CoreConfig instances so they share the new
friction-aware PaperTrader, guard cascade, and observability bus.

This is the "system merge" (Option B): instead of running the legacy
mid-price PaperTrader for variants and the new friction-aware one for
just `aggressive_core`, we run ALL variants through the new pipeline.
Comparison becomes apples-to-apples and historical YAML drift becomes
impossible because configs live in code.

Each VariantSpec maps 1:1 to a row in the trades table via
engine_name = "<engine>/<variant>". Analysis tooling already keys off
this name so the existing leaderboard / report scripts keep working.

Capital/frequency/safety/friction constants are pinned to the
CoreConfig defaults ($50 bankroll, $5 max position, etc.) so every
variant competes on a level playing field. Historical max_position
overrides ($150/$100) are intentionally dropped — they were only
meaningful in the old infinite-balance regime.
"""
from __future__ import annotations

from dataclasses import replace
from typing import Iterable

from .config import CoreConfig


def _base() -> CoreConfig:
    """Neutral defaults — vol cap, rail filter, invert, SL% all OFF.
    Each variant turns on whatever it needs via replace(...)."""
    return CoreConfig(
        max_market_vol_30m=0.0,      # disabled unless variant opts in
        rail_skip_threshold=0.0,     # disabled
        entry_rail_skip_threshold=0.02,  # legacy default — keeps hard rail on
        stop_loss_pct=0.0,           # disabled
        min_entry_yes_mid=0.0,       # disabled
        invert_high_conf=False,
    )


# ─── Aggressive family (min_conf=0.25, micro TP/SL) ────────────────────────
_AGGR = _base()  # all aggressive variants share these baselines
_AGGR = replace(_AGGR,
    min_edge=0.005, edge_increment=0.20,
    min_confidence=0.25, min_hold_seconds=60.0,
    take_profit=0.005, stop_loss=0.015, trailing_stop=0.003,
)

# ─── Balanced family (min_conf=0.40, wide TP/SL) ───────────────────────────
_BAL = _base()
_BAL = replace(_BAL,
    min_edge=0.03, edge_increment=0.12,
    min_confidence=0.40, min_hold_seconds=300.0,
    take_profit=0.020, stop_loss=0.030, trailing_stop=0.010,
)

# ─── Inverted-highconf family (balanced + flip on conf >= 0.75) ────────────
_INV = replace(_BAL,
    invert_high_conf=True, invert_conf_threshold=0.75,
)


VARIANT_CONFIGS: list[CoreConfig] = [
    # ── Aggressive (5 surviving) ────────────────────────────────────────────
    replace(_AGGR,
        engine_name="aggressive/conf25_tp005_sl015",
        source_variant="aggressive/conf25_tp005_sl015"),
    replace(_AGGR,
        engine_name="aggressive/conf25_tp010_sl020",
        source_variant="aggressive/conf25_tp010_sl020",
        take_profit=0.010, stop_loss=0.020, trailing_stop=0.005),
    replace(_AGGR,
        engine_name="aggressive/conf25_tp005_sl015_norail",
        source_variant="aggressive/conf25_tp005_sl015_norail",
        rail_skip_threshold=0.25),
    replace(_AGGR,
        engine_name="aggressive/conf25_tp005_sl015_slpct10",
        source_variant="aggressive/conf25_tp005_sl015_slpct10",
        stop_loss_pct=0.10),
    replace(_AGGR,
        engine_name="aggressive/conf25_tp005_sl015_lowvol",
        source_variant="aggressive/conf25_tp005_sl015_lowvol",
        max_market_vol_30m=0.02),

    # ── Balanced (6 surviving) ──────────────────────────────────────────────
    replace(_BAL,
        engine_name="balanced/conf40_tp010_sl020",
        source_variant="balanced/conf40_tp010_sl020",
        take_profit=0.010, stop_loss=0.020, trailing_stop=0.005),
    replace(_BAL,
        engine_name="balanced/conf40_tp030_sl050",
        source_variant="balanced/conf40_tp030_sl050",
        take_profit=0.030, stop_loss=0.050, trailing_stop=0.015),
    replace(_BAL,
        engine_name="balanced/conf40_tp020_sl030_inv65",
        source_variant="balanced/conf40_tp020_sl030_inv65",
        invert_high_conf=True, invert_conf_threshold=0.65),
    replace(_BAL,
        engine_name="balanced/conf40_tp020_sl030_norail",
        source_variant="balanced/conf40_tp020_sl030_norail",
        rail_skip_threshold=0.25),
    replace(_BAL,
        engine_name="balanced/conf40_tp020_sl030_slpct10",
        source_variant="balanced/conf40_tp020_sl030_slpct10",
        stop_loss_pct=0.10),
    replace(_BAL,
        engine_name="balanced/conf40_tp020_sl030_lowvol",
        source_variant="balanced/conf40_tp020_sl030_lowvol",
        max_market_vol_30m=0.02),

    # ── Inverted-highconf (6 surviving) ─────────────────────────────────────
    replace(_INV,
        engine_name="inverted_highconf/conf40_tp020_sl030_inv65",
        source_variant="inverted_highconf/conf40_tp020_sl030_inv65",
        invert_conf_threshold=0.65),
    replace(_INV,
        engine_name="inverted_highconf/conf40_tp010_sl020_inv75",
        source_variant="inverted_highconf/conf40_tp010_sl020_inv75",
        take_profit=0.010, stop_loss=0.020, trailing_stop=0.005),
    replace(_INV,
        engine_name="inverted_highconf/conf40_tp030_sl050_inv75",
        source_variant="inverted_highconf/conf40_tp030_sl050_inv75",
        take_profit=0.030, stop_loss=0.050, trailing_stop=0.015),
    replace(_INV,
        engine_name="inverted_highconf/conf40_tp020_sl030_inv75_hi75",
        source_variant="inverted_highconf/conf40_tp020_sl030_inv75_hi75",
        min_entry_yes_mid=0.75),
    replace(_INV,
        engine_name="inverted_highconf/conf40_tp020_sl030_inv75_hi65",
        source_variant="inverted_highconf/conf40_tp020_sl030_inv75_hi65",
        min_entry_yes_mid=0.65),
    replace(_INV,
        engine_name="inverted_highconf/conf40_tp020_sl030_inv75_lowvol",
        source_variant="inverted_highconf/conf40_tp020_sl030_inv75_lowvol",
        max_market_vol_30m=0.02),
]


def all_variants() -> Iterable[CoreConfig]:
    return iter(VARIANT_CONFIGS)
