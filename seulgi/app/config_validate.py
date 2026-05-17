"""Fail-fast config validation (§D.1).

Replaces the legacy silent-coercion path (config.py:503-526) with hard
range checks. Called from main.py right after load_config(); raises
SystemExit(2) with the full error list on any violation. Partial boot
is never acceptable in real-money mode.
"""
from __future__ import annotations

import sys
from typing import Iterable


def _check(name: str, value, *, ge=None, le=None, gt=None, lt=None,
           type=None) -> list[str]:
    errs: list[str] = []
    if type is not None and not isinstance(value, type):
        errs.append(f"{name}: expected {type.__name__}, got {value!r}")
        return errs
    if gt is not None and not (value > gt):
        errs.append(f"{name}: must be > {gt}, got {value}")
    if ge is not None and not (value >= ge):
        errs.append(f"{name}: must be >= {ge}, got {value}")
    if lt is not None and not (value < lt):
        errs.append(f"{name}: must be < {lt}, got {value}")
    if le is not None and not (value <= le):
        errs.append(f"{name}: must be <= {le}, got {value}")
    return errs


def validate(cfg) -> list[str]:
    """Return a list of error strings. Empty list = OK."""
    errs: list[str] = []

    # ── Global trading ─────────────────────────────────────────────────────
    t = cfg.trading
    errs += _check("trading.balance_usdc", t.balance_usdc, gt=0)
    errs += _check("trading.max_position_usdc", t.max_position_usdc, gt=0)
    errs += _check("trading.min_trade_usdc", t.min_trade_usdc, gt=0)
    errs += _check("trading.min_edge", t.min_edge, ge=0, lt=1)
    errs += _check("trading.edge_increment", t.edge_increment, ge=0, le=2)
    errs += _check("trading.position_timeout_hours", t.position_timeout_hours, gt=0)
    errs += _check("trading.max_open_positions", t.max_open_positions, gt=0)
    errs += _check("trading.spread_cost_bps", t.spread_cost_bps, ge=0, le=1000)
    errs += _check("trading.take_profit", t.take_profit, ge=0, lt=1)
    errs += _check("trading.stop_loss", t.stop_loss, ge=0, lt=1)
    errs += _check("trading.trailing_stop", t.trailing_stop, ge=0, lt=1)
    errs += _check("trading.min_hold_seconds", t.min_hold_seconds, ge=0)
    if t.min_hold_seconds >= t.position_timeout_hours * 3600:
        errs.append(
            f"trading.min_hold_seconds ({t.min_hold_seconds}) must be < "
            f"position_timeout_hours*3600 ({t.position_timeout_hours*3600})"
        )
    if not t.infinite_balance and t.max_position_usdc > t.balance_usdc:
        errs.append(
            f"trading.max_position_usdc ({t.max_position_usdc}) cannot exceed "
            f"balance_usdc ({t.balance_usdc}) outside infinite_balance mode"
        )
    if t.mode not in ("paper", "live"):
        errs.append(f"trading.mode: expected 'paper'|'live', got {t.mode!r}")

    # ── AI ─────────────────────────────────────────────────────────────────
    a = cfg.ai
    errs += _check("ai.min_confidence", a.min_confidence, ge=0, le=1)
    errs += _check("ai.min_agreement", a.min_agreement, ge=0, le=1)
    if hasattr(a, "ensemble_samples"):
        errs += _check("ai.ensemble_samples", a.ensemble_samples, ge=1, le=20)

    # ── Engines: names unique across engine+variant ────────────────────────
    names: set[str] = set()
    for ec in (cfg.engines or []):
        if ec.name in names:
            errs.append(f"engines: duplicate name {ec.name!r}")
        names.add(ec.name)
        for v in (ec.variants or []):
            composite = f"{ec.name}/{v.name}"
            if composite in names:
                errs.append(f"engines: duplicate variant name {composite!r}")
            names.add(composite)

    # ── Carved aggressive_core (if importable) ─────────────────────────────
    try:
        try:
            from strategies.aggressive_core import CoreConfig
        except ImportError:
            from app.strategies.aggressive_core import CoreConfig  # type: ignore[no-redef]
        cc = CoreConfig()
        errs += _check("aggressive_core.bankroll_usdc", cc.bankroll_usdc, gt=0)
        errs += _check("aggressive_core.max_position_usdc", cc.max_position_usdc,
                       gt=0, le=cc.bankroll_usdc)
        errs += _check("aggressive_core.max_exposure_pct", cc.max_exposure_pct,
                       gt=0, le=1)
        errs += _check("aggressive_core.max_drawdown_pct", cc.max_drawdown_pct,
                       gt=0, le=1)
        errs += _check("aggressive_core.daily_ai_token_budget_usd",
                       cc.daily_ai_token_budget_usd, ge=0)
        if cc.mode not in ("paper", "live"):
            errs.append(f"aggressive_core.mode: expected 'paper'|'live', got {cc.mode!r}")
    except Exception as exc:  # noqa: BLE001
        errs.append(f"aggressive_core: failed to instantiate CoreConfig: {exc}")

    return errs


def validate_or_exit(cfg) -> None:
    errs = validate(cfg)
    if not errs:
        return
    print("CONFIG VALIDATION FAILED:", file=sys.stderr)
    for e in errs:
        print(f"  - {e}", file=sys.stderr)
    raise SystemExit(2)
