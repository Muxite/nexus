"""Seulgi paper trader (Phase 1 A1.2 rewrite — 2026-05-22).

Architecture: two pure module-level functions hold the business logic and
the `PaperTrader` class is a thin orchestrator that pulls live state
(DB / HTTP / balance) and delegates:

    _evaluate_pure(match, classify, ctx) -> EntryOutcome
    _close_pure(position, current_price, cfg, now) -> CloseOutcome

Public interface (engine.py call-signature compatible): __init__,
init_from_db, process, evaluate, evaluate_low_sim_skips,
evaluate_ai_paused_skips, evaluate_freshness_skips, monitor_positions,
close, properties (balance, total_staked, infinite_balance, cfg).

Bug fixes (xref: 2026-05-22-trader-{entry,exit}-bugs.md):
- Confidence vs p_alpha — `ClassifyResult.confidence` is silently `p_alpha`
  else `mean_p_winner`. Treated as direction-confidence for `min_confidence`;
  the AI's real +EV signal lives in `result.p_alpha`. New optional gate
  `cfg.min_p_alpha` (default 0.0) filters on +EV mass directly.
- True EV (4B.2) — `_evaluate_pure` computes `ev_per_dollar` via
  `sim.compute_ev_{yes,no}`. Legacy `edge` stays for the `min_edge` gate
  and `trades.signed_edge`; opt-in EV gate via `cfg.min_ev_per_dollar > 0`
  (threshold = min_ev + 2·default_rate). TODO(4B.1) calibrate p_alpha.
- Cycle-2 skip contract — `_record` drops skip writes to `trades`;
  `engine_results` (runner) holds them. DEV raises on violation.
- `is_scout` = `"scout" in name` (scout AND super_scout).
- Exit FIX-1/2A/2B — `_close_pure` compares NET MTM for TP/TRAIL, plumbs
  `market_type` into apply_slippage/compute_fee, returns `exit_price` for
  postmortem reconstruction.
- Single short-circuit fan-out (`_short_circuit`) covers pre-AI / relevance /
  freshness / low-sim / ai-paused.
- `ClassifyResult.bypass` is propagated to `trader_trace`.

Invariants: one `trades` row per (variant × match) per executed headline;
balance/position guards run under `self._lock`; skip decisions never hit
`trades`; in simulate mode `positions.size_usdc` is gross (entry_fee + gas
included) so `realized_pnl = proceeds − size_usdc`; close-reason vocab is
{WIN, LOSS, RESOLVE_WIN, RESOLVE_LOSS, TP, SL, TRAIL, TIMEOUT, TIMEOUT_BLEED,
COLD_CULL} (admin paths write outside); close priority is
WIN/LOSS > TRAIL > TP > SL > TIMEOUT > TIMEOUT_BLEED; HWM monotonic via SQL MAX.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal, Optional

import aiohttp

from ai import Direction, ClassifyResult, DirectionClassifier
from db import SeulgiDB
from markets import MarketMatch
from watcher import RawHeadline
import simulate as sim

# Defensive — analysis CLIs import trader.py without app.metrics.
try:
    import metrics as _metrics
except Exception:  # noqa: BLE001
    _metrics = None

logger = logging.getLogger(__name__)

_CLOB_PRICE_URL = "https://clob.polymarket.com/midpoint?token_id={token_id}"
_WIN_PRICE  = 0.95
_LOSS_PRICE = 0.05

@dataclass
class TradeDecision:
    """Public per-(variant × match) record returned by `evaluate()`.
    Call-signature compatible with the pre-rewrite struct."""
    condition_id: str
    question: str
    direction: Direction
    confidence: float
    similarity: float
    p_hat: float
    mid_price: Optional[float]
    edge: float
    size_usdc: float
    executed: bool
    skip_reason: Optional[str]
    confidence_raw: float = 0.0
    p_hat_if_raw: float = 0.0
    signed_edge: float = 0.0
    signed_edge_if_raw: float = 0.0
    agreement: float = 0.0
    p_alpha: float = 0.0
    risk_score: float = 0.0
    mid_yes: Optional[float] = None
    mid_no: Optional[float] = None
    inverted: bool = False

@dataclass
class EntryOutcome:
    """Structured return of `_evaluate_pure`."""
    action: Literal["TRADE", "SKIP"]
    direction: Optional[str] = None
    size_usdc: Optional[float] = None
    shares: Optional[float] = None
    entry_price: Optional[float] = None
    entry_fee_usdc: Optional[float] = None
    edge_value: Optional[float] = None
    ev_usdc: Optional[float] = None
    skip_reason: Optional[str] = None
    trader_trace: list = field(default_factory=list)
    token_id: Optional[str] = None
    mid_price: Optional[float] = None
    p_hat: float = 0.0
    inverted: bool = False
    direction_enum: Optional[Direction] = None

@dataclass
class CloseOutcome:
    """Structured return of `_close_pure`."""
    action: Literal["CLOSE", "HOLD"]
    close_reason: Optional[str] = None
    exit_price: Optional[float] = None
    exit_fee_usdc: Optional[float] = None
    realized_pnl: Optional[float] = None
    proceeds: Optional[float] = None
    unrealized_pnl: Optional[float] = None
    high_water_price: Optional[float] = None
    notes: dict = field(default_factory=dict)

@dataclass
class _EntryCtx:
    """Runtime context for `_evaluate_pure` — pulled live by the orchestrator."""
    cfg: object
    ai_cfg: object
    engine_name: str
    parent_engine: str
    open_positions: int
    unique_markets: int
    duplicate_position: bool
    balance: float
    cluster_factor: float = 1.0
    rules_post_ai_reason: Optional[str] = None
    freshness_label: Optional[str] = None

async def _fetch_mid_price(
    session: aiohttp.ClientSession, token_id: str
) -> Optional[float]:
    """Fetch the current mid-price for a Polymarket CLOB token."""
    url = _CLOB_PRICE_URL.format(token_id=token_id)
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            mid = data.get("mid")
            return float(mid) if mid is not None else None
    except Exception as exc:
        logger.debug("[trader] price fetch failed %s: %s", token_id[:12], exc)
        return None

def _compute_edge(confidence: float, cfg) -> float:
    """Legacy edge model: `edge = edge_increment × (2·conf − 1)`."""
    return cfg.edge_increment * max(0.0, 2.0 * confidence - 1.0)

def _compute_ev_per_dollar(direction: Direction, p_true: float,
                           mid_yes: Optional[float], mid_no: Optional[float]) -> float:
    """4B.2 EV per $1 of exposure on the directional token."""
    if direction == Direction.YES:
        return sim.compute_ev_yes(p_true, mid_yes if mid_yes is not None else 0.5)
    if direction == Direction.NO:
        p_ask = mid_no if mid_no is not None else (1.0 - (mid_yes or 0.5))
        return sim.compute_ev_no(p_true, p_ask)
    return 0.0

def _select_side(direction: Direction, match: MarketMatch
                 ) -> tuple[Optional[str], Optional[float]]:
    if direction == Direction.YES:
        return match.token_yes, match.mid_yes
    return match.token_no, match.mid_no

def _is_scout(engine_name: str) -> bool:
    """scout AND super_scout (fixes legacy startswith bug)."""
    return "scout" in engine_name

def _opt_float(cfg, name: str, default: float = 0.0) -> float:
    """Safe numeric extraction. Rejects MagicMock auto-attrs (whose
    `__float__` returns 1.0 by default) — only real int/float pass."""
    v = getattr(cfg, name, default)
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return default
    return float(v)

def _est_exit_fee(shares: float, current_price: float, market_type: Optional[str],
                  cfg) -> float:
    """Estimated exit fee for the TP/TRAIL net-MTM gate. Simulate: dynamic
    taker fee + gas. Paper: half round-trip `spread_cost_bps`."""
    simulate_mode = getattr(cfg, "mode", "paper") == "simulate"
    if simulate_mode and getattr(cfg, "simulate_fees", None) is not None:
        fee = sim.compute_fee(shares, current_price, market_type, cfg.simulate_fees).fee
        gas = getattr(cfg.simulate_fees, "gas_cost_per_trade_usdc", 0.0) or 0.0
        return fee + gas
    if not simulate_mode:
        return 0.5 * shares * current_price * (cfg.spread_cost_bps / 10000.0)
    return 0.0

def _size_kelly(direction: Direction, match: MarketMatch, mid_price: float,
                p_hat: float, balance: float, cfg) -> tuple[Optional[float],
                                                            dict, Optional[str]]:
    """Run fractional Kelly. Returns (stake, trace_step, skip_reason)."""
    if direction == Direction.YES:
        p_market = match.mid_yes if match.mid_yes is not None else mid_price
    else:
        p_market = match.mid_no if match.mid_no is not None else (1.0 - mid_price)
    k = sim.fractional_kelly(
        bankroll_usdc=balance, p_true=p_hat, p_market=p_market,
        max_position_usdc=cfg.max_position_usdc, cfg=cfg.simulate_sizing,
    )
    step = {"step": "kelly", "f_star": round(k.f_star, 4),
            "fractional": round(k.fractional, 4),
            "capped_pct": round(k.capped_pct, 4),
            "stake_usdc": round(k.stake_usdc, 4),
            "skip_reason": k.skip_reason, "bankroll": round(balance, 4),
            "p_market": round(p_market, 4), "p_true": round(p_hat, 4)}
    return k.stake_usdc, step, k.skip_reason

def _apply_fidelity(size: float, mid_price: float, direction: str,
                    match: MarketMatch, cfg
                    ) -> tuple[float, float, float, Optional[str], dict]:
    """Simulate-mode fidelity: slippage + dynamic fee + share quantization.

    Returns (effective_entry_price, shares_held, entry_fee, skip_reason, trace_step).
    `skip_reason` is non-None when the trade must abort (rail snap or zero
    shares after quantize)."""
    market_type = getattr(match, "market_type", None) or getattr(match, "_market_type", None)
    slip = sim.apply_slippage(
        mid=mid_price, notional_usdc=size, side=direction,
        market_type=market_type, cfg=cfg.simulate_slippage,
    )
    eff = slip.effective_price
    if sim.is_at_rail(eff):
        return eff, 0.0, 0.0, "rail_zero_liquidity", {
            "step": "rail_zero_liquidity", "pass": False,
            "effective_price": round(eff, 4),
        }
    gas = getattr(cfg.simulate_fees, "gas_cost_per_trade_usdc", 0.0) or 0.0
    fee_est = sim.compute_fee(size / eff, eff, market_type, cfg.simulate_fees)
    shares = sim.quantize_shares(size, eff, fee_est.fee + gas)
    fee_res = sim.compute_fee(shares, eff, market_type, cfg.simulate_fees)
    entry_fee = fee_res.fee + gas
    step = {"step": "fidelity", "mid": round(mid_price, 4),
            "slip_walk": round(slip.walk, 5), "entry_price": round(eff, 4),
            "shares": round(shares, 4), "entry_fee_usdc": round(entry_fee, 6),
            "gas_usdc": round(gas, 6), "fee_rate": fee_res.rate,
            "market_type": market_type or "(none)"}
    if shares <= 0:
        return eff, 0.0, entry_fee, "shares_zero_after_quantize", step
    return eff, shares, entry_fee, None, step

def _evaluate_pure(match: MarketMatch, result: ClassifyResult,
                   ctx: _EntryCtx) -> EntryOutcome:
    """The big pure decision function. No DB, no HTTP, no `self`.

    Returns an `EntryOutcome` with `action ∈ {"TRADE", "SKIP"}` and the
    full trader_trace step list. The orchestrator writes the result.
    """
    cfg = ctx.cfg
    ai_cfg = ctx.ai_cfg
    trace: list[dict] = []
    # Skip-helper state bag (carries partial-progress fields onto EntryOutcome).
    state = {
        "token_id": None, "mid_price": match.mid_yes, "p_hat": 0.0,
        "edge_value": None, "ev_usdc": None, "inverted": False,
    }

    def _skip(reason: str, step: Optional[dict] = None) -> EntryOutcome:
        if step is not None:
            trace.append(step)
        trace.append({"step": "skip", "reason": reason})
        return EntryOutcome(
            action="SKIP", skip_reason=reason, trader_trace=trace,
            direction_enum=result.direction,
            token_id=state["token_id"], mid_price=state["mid_price"],
            p_hat=state["p_hat"], edge_value=state["edge_value"],
            ev_usdc=state["ev_usdc"], inverted=state["inverted"],
        )

    trace.append({
        "step": "ai",
        "direction": result.direction.value,
        "confidence": round(result.confidence, 4),
        "p_alpha": round(getattr(result, "p_alpha", 0.0), 4),
        "agreement": round(getattr(result, "agreement", 0.0), 4),
        "votes": getattr(result, "votes", {}) or {},
        "bypass": bool(getattr(result, "bypass", False)),
    })

    if result.direction == Direction.NEUTRAL:
        return _skip("neutral")

    conf_step = {"step": "min_confidence",
                 "value": round(result.confidence, 4),
                 "threshold": ai_cfg.min_confidence}
    if result.confidence < ai_cfg.min_confidence:
        return _skip(f"low_confidence:{result.confidence:.2f}",
                     step={**conf_step, "pass": False})
    trace.append({**conf_step, "pass": True})

    min_agree = getattr(ai_cfg, "min_agreement", 0.66)
    agree = getattr(result, "agreement", 0.0) or 0.0
    agree_step = {"step": "agreement", "value": round(agree, 4),
                  "threshold": min_agree}
    if agree > 0.0 and agree < min_agree:
        return _skip(f"low_agreement:{agree:.2f}",
                     step={**agree_step, "pass": False})
    trace.append({**agree_step, "pass": True})

    # A1.2 optional p_alpha gate — defaults off (0.0).
    min_p_alpha = _opt_float(cfg, "min_p_alpha", 0.0)
    p_alpha = float(getattr(result, "p_alpha", 0.0) or 0.0)
    if min_p_alpha > 0.0 and p_alpha < min_p_alpha:
        return _skip(f"low_p_alpha:{p_alpha:.3f}",
                     step={"step": "min_p_alpha", "pass": False,
                           "value": round(p_alpha, 4),
                           "threshold": min_p_alpha})

    min_entry = cfg.min_entry_yes_mid
    if min_entry > 0 and match.mid_yes is not None and match.mid_yes < min_entry:
        return _skip(f"min_entry_yes_mid:{match.mid_yes:.3f}",
                     step={"step": "min_entry", "pass": False,
                           "mid_yes": round(match.mid_yes, 4),
                           "threshold": min_entry})
    dir_filter = getattr(cfg, "direction_filter", "") or ""
    if dir_filter and result.direction.value != dir_filter:
        return _skip(f"direction_filter:{dir_filter}_only",
                     step={"step": "direction_filter", "pass": False,
                           "got": result.direction.value, "want": dir_filter})
    fresh_filter = getattr(cfg, "freshness_filter", "") or ""
    if fresh_filter and (ctx.freshness_label or "") != fresh_filter:
        return _skip(f"freshness_filter:{fresh_filter}_only",
                     step={"step": "freshness_filter", "pass": False,
                           "got": ctx.freshness_label or "",
                           "want": fresh_filter})
    v_mid_min = getattr(cfg, "mid_yes_min", 0.0) or 0.0
    v_mid_max = getattr(cfg, "mid_yes_max", 1.0) or 1.0
    if match.mid_yes is not None and (
        match.mid_yes < v_mid_min or match.mid_yes > v_mid_max
    ):
        return _skip(f"variant_mid_band:{match.mid_yes:.3f}",
                     step={"step": "variant_mid_band", "pass": False,
                           "mid_yes": round(match.mid_yes, 4),
                           "band": [v_mid_min, v_mid_max]})
    max_vol = cfg.max_market_vol_30m
    mkt_ctx = getattr(match, "_mkt_ctx", None)
    if max_vol > 0 and mkt_ctx is not None and mkt_ctx.vol > max_vol:
        return _skip(f"market_too_noisy:{mkt_ctx.vol:.3f}",
                     step={"step": "mkt_vol", "pass": False,
                           "vol": round(mkt_ctx.vol, 4),
                           "threshold": max_vol})
    if ctx.rules_post_ai_reason is not None:
        return _skip(ctx.rules_post_ai_reason,
                     step={"step": "post_ai_rule", "pass": False,
                           "reason": ctx.rules_post_ai_reason})
    trace.append({"step": "post_ai_rule", "pass": True})

    if cfg.invert_high_conf:
        if result.confidence >= cfg.invert_conf_threshold:
            orig = match.mid_yes if result.direction == Direction.YES else match.mid_no
            if orig is not None and 0.05 < orig < 0.95:
                from dataclasses import replace as _dc_replace
                flipped = Direction.NO if result.direction == Direction.YES else Direction.YES
                trace.append({"step": "invert", "fired": True,
                              "from": result.direction.value, "to": flipped.value})
                result = _dc_replace(result, direction=flipped)
                state["inverted"] = True
            else:
                trace.append({"step": "invert", "fired": False, "reason": "rail_or_no_mid"})
        else:
            trace.append({"step": "invert", "fired": False, "reason": "below_threshold"})

    token_id, mid_price = _select_side(result.direction, match)
    state["token_id"] = token_id
    if not token_id:
        return _skip("no_token")
    if mid_price is None:
        return _skip("no_price")
    state["mid_price"] = mid_price

    hard_thr = cfg.entry_rail_skip_threshold
    if hard_thr > 0 and (mid_price <= hard_thr or mid_price >= 1.0 - hard_thr):
        return _skip(f"hard_rail:{mid_price:.3f}",
                     step={"step": "hard_rail", "pass": False,
                           "mid": round(mid_price, 4), "threshold": hard_thr})
    trace.append({"step": "hard_rail", "pass": True, "mid": round(mid_price, 4)})
    rail_thr = cfg.rail_skip_threshold
    if rail_thr > 0 and (mid_price <= rail_thr or mid_price >= 1.0 - rail_thr):
        return _skip(f"rail_price:{mid_price:.3f}",
                     step={"step": "rail_skip", "pass": False,
                           "mid": round(mid_price, 4), "threshold": rail_thr})
    if rail_thr > 0:
        trace.append({"step": "rail_skip", "pass": True, "mid": round(mid_price, 4)})

    edge = _compute_edge(result.confidence, cfg)
    p_hat = mid_price + edge
    state["p_hat"] = p_hat
    state["edge_value"] = edge
    # TODO(4B.1): replace `p_alpha` with calibrated probability once available.
    ev_per_dollar = _compute_ev_per_dollar(result.direction, p_alpha,
                                           match.mid_yes, match.mid_no)
    state["ev_usdc"] = ev_per_dollar

    edge_step = {"step": "edge",
                 "p_hat": round(p_hat, 4), "mid": round(mid_price, 4),
                 "edge": round(edge, 4), "min_edge": cfg.min_edge,
                 "ev_per_dollar": round(ev_per_dollar, 4)}
    if edge < cfg.min_edge:
        return _skip(f"edge_too_small:{edge:.4f}",
                     step={**edge_step, "pass": False})
    trace.append({**edge_step, "pass": True})

    # 4B.2 optional EV gate (threshold = min_ev + 2·default_rate, off by default).
    min_ev = _opt_float(cfg, "min_ev_per_dollar", 0.0)
    if min_ev > 0.0:
        sim_fees = getattr(cfg, "simulate_fees", None)
        default_rate = getattr(sim_fees, "default_rate", 0.0) if sim_fees is not None else 0.0
        ev_threshold = min_ev + 2.0 * default_rate
        ev_step = {"step": "ev_gate",
                   "ev_per_dollar": round(ev_per_dollar, 4),
                   "threshold": round(ev_threshold, 4)}
        if ev_per_dollar < ev_threshold:
            return _skip(f"ev_too_small:{ev_per_dollar:.4f}",
                         step={**ev_step, "pass": False})
        trace.append({**ev_step, "pass": True})

    if ctx.open_positions >= cfg.max_open_positions:
        return _skip(f"max_positions:{ctx.open_positions}",
                     step={"step": "max_positions", "pass": False,
                           "value": ctx.open_positions,
                           "threshold": cfg.max_open_positions})
    if ctx.duplicate_position:
        return _skip("duplicate_position",
                     step={"step": "duplicate_position", "pass": False})
    if ctx.unique_markets >= cfg.max_unique_markets:
        return _skip(f"max_unique_markets:{ctx.unique_markets}",
                     step={"step": "max_unique_markets", "pass": False,
                           "value": ctx.unique_markets,
                           "threshold": cfg.max_unique_markets})

    simulate_mode = getattr(cfg, "mode", "paper") == "simulate"
    sizing_cfg = getattr(cfg, "simulate_sizing", None)
    use_kelly = (
        simulate_mode and sizing_cfg is not None
        and getattr(sizing_cfg, "method", "") == "fractional_kelly"
        and not _is_scout(ctx.engine_name)
    )
    size: Optional[float] = None
    if use_kelly:
        # TODO(4B.1): swap p_hat for calibrated p_alpha.
        size, kelly_step, kelly_skip = _size_kelly(
            result.direction, match, mid_price, p_hat, ctx.balance, cfg,
        )
        trace.append(kelly_step)
        if kelly_skip is not None:
            return _skip(kelly_skip)
    elif cfg.infinite_balance:
        size = cfg.max_position_usdc
    else:
        size = min(cfg.max_position_usdc, ctx.balance)
        if size < cfg.min_trade_usdc:
            reason = "no_balance" if ctx.balance <= 0 else f"size_too_small:{size:.2f}"
            return _skip(reason, step={"step": "size", "pass": False,
                                       "size": round(size, 2),
                                       "balance": round(ctx.balance, 2)})

    if ctx.cluster_factor < 1.0:
        size *= ctx.cluster_factor
        trace.append({"step": "cluster_scale",
                      "factor": round(ctx.cluster_factor, 4),
                      "size_usdc": round(size, 4)})
        if size < cfg.min_trade_usdc:
            return _skip(f"cluster_size_too_small:{size:.2f}")

    entry_fee = 0.0
    shares_held: Optional[float] = None
    effective_entry_price = mid_price
    if simulate_mode:
        eff, shares_held, entry_fee, fidelity_skip, fidelity_step = _apply_fidelity(
            size, mid_price, result.direction.value, match, cfg,
        )
        effective_entry_price = eff
        if fidelity_skip is not None:
            return _skip(fidelity_skip, step=fidelity_step)
        trace.append(fidelity_step)

    trace.append({"step": "size", "pass": True, "size_usdc": round(size, 2)})
    trace.append({"step": "executed"})
    return EntryOutcome(
        action="TRADE", direction=result.direction.value,
        direction_enum=result.direction, size_usdc=size, shares=shares_held,
        entry_price=effective_entry_price if simulate_mode else mid_price,
        entry_fee_usdc=entry_fee if simulate_mode else None,
        edge_value=edge, ev_usdc=ev_per_dollar, trader_trace=trace,
        token_id=token_id, mid_price=mid_price, p_hat=p_hat,
        inverted=state["inverted"],
    )

def _exit_at_mid(pos: dict, current_price: float, shares: float, cfg
                 ) -> tuple[float, float, float, float]:
    """Walk the book on an active-market exit at `current_price`. Returns
    (exit_price, exit_fee, proceeds, realized_pnl). Shared by COLD_CULL
    and the early-exit branches of `_close_pure`."""
    simulate_mode = getattr(cfg, "mode", "paper") == "simulate"
    size_usdc = pos["size_usdc"]
    if simulate_mode:
        market_type = pos.get("market_type")
        slip = sim.apply_slippage(
            mid=current_price, notional_usdc=shares * current_price,
            side="NO" if pos["direction"] == "YES" else "YES",
            market_type=market_type, cfg=cfg.simulate_slippage,
        )
        exit_price = slip.effective_price
        fee_res = sim.compute_fee(shares, exit_price, market_type, cfg.simulate_fees)
        gas_per_leg = getattr(cfg.simulate_fees, "gas_cost_per_trade_usdc", 0.0) or 0.0
        exit_fee = fee_res.fee + gas_per_leg
        proceeds = shares * exit_price - exit_fee
        return exit_price, exit_fee, proceeds, proceeds - size_usdc
    gross = (current_price - pos["entry_price"]) * shares
    realized_pnl = gross - size_usdc * (cfg.spread_cost_bps / 10000.0)
    return current_price, 0.0, size_usdc + realized_pnl, realized_pnl

def _cold_cull_outcome(pos: dict, price: float, cfg) -> CloseOutcome:
    """COLD_CULL — close unconditionally at current mid."""
    persisted = pos.get("shares")
    shares = persisted if persisted else (pos["size_usdc"] / pos["entry_price"])
    exit_price, exit_fee, proceeds, realized_pnl = _exit_at_mid(pos, price, shares, cfg)
    simulate_mode = getattr(cfg, "mode", "paper") == "simulate"
    return CloseOutcome(
        action="CLOSE", close_reason="COLD_CULL", exit_price=exit_price,
        exit_fee_usdc=exit_fee if simulate_mode else None,
        realized_pnl=realized_pnl, proceeds=proceeds,
    )

def _close_pure(pos: dict, current_price: float, cfg, now: datetime) -> CloseOutcome:
    """Pure exit-decision. FIX-2A: TP/TRAIL compare NET MTM (after sunk
    entry_fee + est_exit_fee); SL stays gross. FIX-2B: returns `exit_price`
    so postmortems reconcile from `trades` alone. FIX-1: `market_type` from
    the position row plumbs into `apply_slippage`/`compute_fee`.
    """
    size_usdc = pos["size_usdc"]
    entry_price = pos["entry_price"]
    direction = pos["direction"]
    prev_hwm = pos.get("high_water_price")
    opened_at = datetime.fromisoformat(pos["opened_at"])
    if opened_at.tzinfo is None:
        opened_at = opened_at.replace(tzinfo=timezone.utc)
    age_secs = (now - opened_at).total_seconds()
    persisted = pos.get("shares")
    shares = persisted if persisted else (size_usdc / entry_price)
    unrealized = (current_price - entry_price) * shares
    local_hwm = max(current_price, prev_hwm if prev_hwm is not None else current_price)
    mtm = current_price - entry_price
    hwm_mtm = local_hwm - entry_price
    tp = cfg.take_profit
    sl = max(cfg.stop_loss, cfg.stop_loss_pct * entry_price)
    ts = cfg.trailing_stop
    min_hold = cfg.min_hold_seconds
    timeout_secs = cfg.position_timeout_hours * 3600
    min_hold_met = (min_hold <= 0) or (age_secs >= min_hold)
    # FIX-2A net MTM estimate.
    est_exit_fee = _est_exit_fee(shares, current_price, pos.get("market_type"), cfg)
    entry_fee_paid = max(0.0, size_usdc - shares * entry_price)
    mtm_net = mtm * shares - entry_fee_paid - est_exit_fee

    close_reason: Optional[str] = None
    if current_price >= _WIN_PRICE:
        close_reason = "WIN"
    elif current_price <= _LOSS_PRICE:
        close_reason = "LOSS"
    elif (min_hold_met and ts > 0
          and hwm_mtm >= ts and (hwm_mtm - mtm) >= ts and mtm_net >= 0.0):
        close_reason = "TRAIL"
    elif min_hold_met and tp > 0 and mtm_net >= tp * shares:
        close_reason = "TP"
    elif min_hold_met and sl > 0 and mtm <= -sl:
        close_reason = "SL"
    elif age_secs >= timeout_secs:
        close_reason = "TIMEOUT"
    elif min_hold_met and sl > 0 and age_secs >= 3600 and mtm <= -0.5 * sl:
        close_reason = "TIMEOUT_BLEED"
    if close_reason is None:
        return CloseOutcome(action="HOLD", unrealized_pnl=unrealized,
                            high_water_price=local_hwm)

    # Settlement WIN/LOSS pay $1/$0 no-fee; other closes walk the book.
    simulate_mode = getattr(cfg, "mode", "paper") == "simulate"
    if simulate_mode and close_reason in ("WIN", "LOSS"):
        win = close_reason == "WIN"
        close_reason = "RESOLVE_WIN" if win else "RESOLVE_LOSS"
        exit_price = 1.0 if win else 0.0
        proceeds = sim.settlement_payout(shares, win=win)
        realized_pnl = proceeds - size_usdc
        exit_fee = 0.0
    else:
        exit_price, exit_fee, proceeds, realized_pnl = _exit_at_mid(
            pos, current_price, shares, cfg,
        )

    return CloseOutcome(
        action="CLOSE", close_reason=close_reason,
        exit_price=exit_price,
        exit_fee_usdc=exit_fee if simulate_mode else None,
        realized_pnl=realized_pnl, proceeds=proceeds,
        unrealized_pnl=unrealized, high_water_price=local_hwm,
        notes={"age_secs": age_secs, "mtm_net": mtm_net},
    )

def _outcome_to_decision(outcome: EntryOutcome, match: MarketMatch,
                         result: ClassifyResult) -> TradeDecision:
    """Adapt `EntryOutcome` -> legacy `TradeDecision`."""
    e = outcome.edge_value or 0.0
    mid = outcome.mid_price
    c = result.confidence
    return TradeDecision(
        condition_id=match.condition_id, question=match.question,
        direction=outcome.direction_enum or result.direction,
        confidence=c, similarity=match.similarity,
        p_hat=outcome.p_hat, mid_price=mid, edge=e,
        size_usdc=outcome.size_usdc or 0.0,
        executed=outcome.action == "TRADE", skip_reason=outcome.skip_reason,
        confidence_raw=c, p_hat_if_raw=c, signed_edge=e,
        signed_edge_if_raw=(c - mid) if mid is not None else 0.0,
        agreement=getattr(result, "agreement", 0.0),
        p_alpha=getattr(result, "p_alpha", 0.0),
        risk_score=getattr(result, "risk_score", 0.0),
        mid_yes=match.mid_yes, mid_no=match.mid_no,
        inverted=outcome.inverted,
    )


class PaperTrader:
    """Orchestrator. Owns DB / HTTP / balance / lock; delegates decisions
    to the pure `_evaluate_pure` and `_close_pure` functions above."""

    def __init__(
        self,
        db: SeulgiDB,
        classifier: DirectionClassifier,
        cfg,
        ai_cfg,
        engine_name: str = "default",
        rule_engine=None,
        variant_name: Optional[str] = None,
        parent_engine: Optional[str] = None,
    ) -> None:
        self._db = db
        self._classifier = classifier
        self._cfg = cfg
        self._ai_cfg = ai_cfg
        self._engine_name = engine_name
        self._variant_name = variant_name or engine_name
        self._parent_engine = parent_engine or engine_name
        self._rules = rule_engine
        self._balance: float = 0.0 if cfg.infinite_balance else cfg.balance_usdc
        self._total_staked: float = 0.0
        self._last_tokens_used: int = 0
        self._lock = asyncio.Lock()
        self._session: Optional[aiohttp.ClientSession] = None

    async def init_from_db(self) -> None:
        """Restore balance/staked from DB (cycle-scoped under infinite mode)."""
        if self._cfg.infinite_balance:
            cycle = await self._db.get_current_cycle()
            since_ts = cycle["started_at"] if cycle else None
            gain, staked = await self._db.get_engine_gain_and_staked(
                self._engine_name, since_ts=since_ts,
            )
            self._balance, self._total_staked = gain, staked
            logger.info(
                "[trader] [%s] infinite restored (cycle=%s): gain=%+.2f staked=%.2f",
                self._engine_name, cycle["name"] if cycle else "<none>",
                self._balance, self._total_staked,
            )
            return
        committed = await self._db.get_committed_usdc(engine_name=self._engine_name)
        if committed > 0:
            self._balance = self._cfg.balance_usdc - committed
            logger.info(
                "[trader] [%s] restored: balance=%.2f (%.2f committed in %d)",
                self._engine_name, self._balance, committed,
                await self._db.count_open_positions(engine_name=self._engine_name),
            )
        else:
            self._balance = self._cfg.balance_usdc

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    async def process(
        self, headline: RawHeadline, matches: list[MarketMatch]
    ) -> list[TradeDecision]:
        """Single-engine entry path used by tests. EngineRunner skips this
        and calls `evaluate()` directly with shared `classify_batch` output."""
        if not matches:
            self._last_tokens_used = 0
            return []
        signal_id = await self._db.insert_signal(
            site_url=headline.site_url, headline=headline.headline,
            headline_id=getattr(headline, "headline_id", None),
        )
        survivors, pre_skips = self._gate_pre_ai(headline, matches)
        classify_results, prompt_meta = await self._classify(headline, survivors)
        self._last_tokens_used = sum(r.tokens_used for r in classify_results)
        prompt_meta_json = (
            json.dumps(prompt_meta, separators=(",", ":")) if prompt_meta else None
        )
        return await self.evaluate(
            signal_id, headline, survivors, classify_results,
            prompt_meta_json, pre_ai_skips=pre_skips,
        )

    def _gate_pre_ai(self, headline: RawHeadline, matches: list[MarketMatch]
                     ) -> tuple[list[MarketMatch], list[tuple[MarketMatch, str]]]:
        survivors: list[MarketMatch] = []
        pre_skips: list[tuple[MarketMatch, str]] = []
        if self._rules is None:
            return list(matches), pre_skips
        fresh_lbl = getattr(headline, "_freshness_label", None)
        for m in matches:
            reason = self._rules.gate_pre_ai(
                market_type=m.market_type, mid_price=m.mid_yes,
                freshness_label=fresh_lbl, question=m.question,
            )
            if reason is None:
                survivors.append(m)
            else:
                pre_skips.append((m, reason))
        return survivors, pre_skips

    async def _classify(self, headline: RawHeadline,
                        survivors: list[MarketMatch]
                        ) -> tuple[list[ClassifyResult], dict]:
        if not survivors:
            return [], {}
        source_host: Optional[str] = None
        site_url = getattr(headline, "site_url", None)
        if site_url:
            try:
                from urllib.parse import urlparse
                source_host = urlparse(site_url).netloc or None
            except Exception:  # noqa: BLE001
                source_host = None
        raw = await self._classifier.classify_batch(
            headline=headline.headline, matches=survivors,
            is_republish=getattr(headline, "_is_republish", False),
            similar_to=getattr(headline, "_similar_to", None),
            source=source_host,
        )
        if isinstance(raw, tuple) and len(raw) == 2:
            return raw
        return raw, {}

    async def evaluate(
        self,
        signal_id: int,
        headline: RawHeadline,
        survivors: list[MarketMatch],
        classify_results: list[ClassifyResult],
        prompt_meta_json: Optional[str],
        pre_ai_skips: Optional[list[tuple[MarketMatch, str]]] = None,
        relevance_skips: Optional[list] = None,
        cluster_factor: float = 1.0,
    ) -> list[TradeDecision]:
        decisions: list[TradeDecision] = []
        for match, reason in pre_ai_skips or []:
            decisions.append(await self._short_circuit(signal_id, match, reason))
        for match, verdict in relevance_skips or []:
            score = getattr(verdict, "score", "NO")
            decisions.append(await self._short_circuit(
                signal_id, match, f"not_relevant:{score}",
            ))
        if not survivors:
            return decisions
        session = self._get_session()
        for match, result in zip(survivors, classify_results):
            decisions.append(await self._evaluate_market(
                session, signal_id, headline, match, result,
                prompt_meta_json, cluster_factor=cluster_factor,
            ))
        return decisions

    # Short-circuit skip fan-outs (one NEUTRAL row per variant × match).
    async def evaluate_low_sim_skips(
        self, signal_id: int, headline: RawHeadline,
        matches: list[MarketMatch], reason: str, max_sim: float,
        prompt_meta_json: Optional[str] = None,
    ) -> list[TradeDecision]:
        return [await self._short_circuit(signal_id, m, reason)
                for m in matches]

    async def evaluate_ai_paused_skips(
        self, signal_id: int, headline: RawHeadline,
        matches: list[MarketMatch],
    ) -> list[TradeDecision]:
        return [await self._short_circuit(signal_id, m, "ai_paused_no_capital")
                for m in matches]

    async def evaluate_freshness_skips(
        self, signal_id: int, headline: RawHeadline,
        matches: list[MarketMatch], freshness_label: str,
        nearest_age_secs: Optional[float],
        prompt_meta_json: Optional[str] = None,
    ) -> list[TradeDecision]:
        age = int(nearest_age_secs) if nearest_age_secs is not None else 0
        reason = f"freshness:{freshness_label.lower()}:age={age}"
        return [await self._short_circuit(signal_id, m, reason) for m in matches]

    async def _short_circuit(
        self, signal_id: int, match: MarketMatch, skip_reason: str,
    ) -> TradeDecision:
        decision = TradeDecision(
            condition_id=match.condition_id, question=match.question,
            direction=Direction.NEUTRAL, confidence=0.0,
            similarity=match.similarity, p_hat=0.0,
            mid_price=match.mid_yes, edge=0.0, size_usdc=0.0,
            executed=False, skip_reason=skip_reason,
            mid_yes=match.mid_yes, mid_no=match.mid_no,
        )
        await self._record(signal_id, decision, match=match)
        return decision

    async def _persist_close(
        self, pos: dict, outcome: CloseOutcome, *, close_reason: str,
    ) -> None:
        """Single write path for both COLD_CULL and main-loop closes."""
        simulate_mode = getattr(self._cfg, "mode", "paper") == "simulate"
        async with self._lock:
            if self._cfg.infinite_balance:
                self._balance += outcome.realized_pnl
            elif simulate_mode:
                self._balance += outcome.proceeds
            else:
                self._balance += pos["size_usdc"] + outcome.realized_pnl
            await self._db.close_position(pos["trade_id"])
            await self._db.close_trade(
                pos["trade_id"], outcome.realized_pnl,
                close_reason=close_reason,
                exit_fee_usdc=outcome.exit_fee_usdc,
                exit_price=outcome.exit_price,
                entry_price_at_close=pos["entry_price"],
            )

    async def _evaluate_market(
        self, session: aiohttp.ClientSession, signal_id: int,
        headline: RawHeadline, match: MarketMatch, result: ClassifyResult,
        prompt_meta_json: Optional[str], cluster_factor: float = 1.0,
    ) -> TradeDecision:
        # Post-AI rule (runs out of band — rule engine is I/O-free).
        rules_post_ai_reason: Optional[str] = None
        if self._rules is not None and result.direction != Direction.NEUTRAL:
            rules_post_ai_reason = self._rules.gate_post_ai(
                market_type=match.market_type, mid_price=match.mid_yes,
                direction=result.direction.value, confidence=result.confidence,
                freshness_label=getattr(headline, "_freshness_label", None),
                question=match.question, engine_root=self._parent_engine,
            )
        # CLOB refetch when the prefetch missed the directional mid.
        if (result.direction != Direction.NEUTRAL
                and result.confidence >= self._ai_cfg.min_confidence):
            token_id, mid_price = _select_side(result.direction, match)
            if token_id and mid_price is None:
                refetched = await _fetch_mid_price(session, token_id)
                if refetched is not None:
                    if result.direction == Direction.YES:
                        match.mid_yes = refetched
                    else:
                        match.mid_no = refetched

        async with self._lock:
            ctx = _EntryCtx(
                cfg=self._cfg, ai_cfg=self._ai_cfg,
                engine_name=self._engine_name, parent_engine=self._parent_engine,
                open_positions=await self._db.count_open_positions(engine_name=self._engine_name),
                unique_markets=await self._db.count_unique_open_markets(engine_name=self._engine_name),
                duplicate_position=await self._db.has_open_position(
                    match.condition_id, engine_name=self._engine_name),
                balance=self._balance, cluster_factor=cluster_factor,
                rules_post_ai_reason=rules_post_ai_reason,
                freshness_label=getattr(headline, "_freshness_label", None),
            )
            outcome = _evaluate_pure(match, result, ctx)
            decision = _outcome_to_decision(outcome, match, result)
            if outcome.action == "SKIP":
                # Cycle-2: skip row is dropped (engine_results holds it).
                await self._record(signal_id, decision, token_id=outcome.token_id)
                return decision

            # TRADE — persist + open position. Roll back on open_position failure.
            if not self._cfg.infinite_balance:
                self._balance -= outcome.size_usdc
            self._total_staked += outcome.size_usdc
            mid = outcome.mid_price
            mc = getattr(match, "_mkt_ctx", None)
            trade_id = await self._db.insert_trade(
                signal_id=signal_id, condition_id=match.condition_id,
                question=match.question, token_id=outcome.token_id,
                direction=outcome.direction, similarity=match.similarity,
                p_hat=outcome.p_hat, mid_price=mid,
                size_usdc=outcome.size_usdc, skip_reason=None,
                headline_id=getattr(headline, "headline_id", None),
                confidence_raw=result.confidence, p_hat_if_raw=result.confidence,
                signed_edge=outcome.edge_value or 0.0,
                signed_edge_if_raw=(result.confidence - mid if mid is not None else 0.0),
                engine_name=self._engine_name,
                agreement=getattr(result, "agreement", 0.0),
                p_alpha=getattr(result, "p_alpha", 0.0),
                risk_score=getattr(result, "risk_score", 0.0),
                ai_reason=getattr(result, "reason", None),
                ai_raw_response=getattr(result, "raw_response", None) or None,
                ai_prompt_meta=prompt_meta_json,
                trader_trace=json.dumps(outcome.trader_trace, separators=(",", ":")),
                mid_yes=match.mid_yes, mid_no=match.mid_no,
                inverted=1 if outcome.inverted else 0,
                variant_name=self._variant_name,
                freshness_label=getattr(headline, "_freshness_label", None),
                relevance_score=getattr(match, "_relevance_score", None),
                relevance_reason=getattr(match, "_relevance_reason", None),
                mkt_vol_30m=getattr(mc, "vol", None), mkt_mean_30m=getattr(mc, "mean", None),
                mkt_min_30m=getattr(mc, "lo", None), mkt_max_30m=getattr(mc, "hi", None),
                mkt_drift_30m=getattr(mc, "drift", None),
                mkt_n_samples=getattr(mc, "n_samples", None),
            )
            simulate_mode = getattr(self._cfg, "mode", "paper") == "simulate"
            try:
                await self._db.open_position(
                    trade_id=trade_id, condition_id=match.condition_id,
                    question=match.question, token_id=outcome.token_id,
                    direction=outcome.direction, size_usdc=outcome.size_usdc,
                    entry_price=outcome.entry_price, p_hat=outcome.p_hat,
                    engine_name=self._engine_name, shares=outcome.shares,
                    entry_fee_usdc=(outcome.entry_fee_usdc if simulate_mode else None),
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "[trader] open_position failed trade_id=%s engine=%s "
                    "cond=%s: %r", trade_id, self._engine_name,
                    match.condition_id[:12], exc,
                )
                if not self._cfg.infinite_balance:
                    self._balance += outcome.size_usdc
                self._total_staked -= outcome.size_usdc

        logger.info(
            "[trader] PAPER %s %s  $%.2f @ %.3f  edge=%.4f  conf=%.0f%%  %s",
            outcome.direction, match.condition_id[:12],
            outcome.size_usdc, outcome.mid_price or 0.0,
            outcome.edge_value or 0.0, result.confidence * 100,
            match.question[:70],
        )
        return decision

    async def _record(
        self, signal_id: int, decision: TradeDecision,
        token_id: Optional[str] = None, *, match=None, **_legacy_kwargs,
    ) -> None:
        """Cycle-2 skip-row guard. Skip decisions never land in `trades`;
        the runner persists them to `engine_results`. DEV raises;
        PROD silently drops and bumps `db.skip_to_trades_redirected`."""
        if decision.skip_reason is None:
            return
        if _metrics is not None:
            try:
                _metrics.incr("db.skip_to_trades_redirected",
                              tags={"reason": (decision.skip_reason or "")[:40]})
            except Exception:  # noqa: BLE001
                pass
        if os.environ.get("SEULGI_DEV") == "1":
            raise ValueError(
                f"cycle-2 contract violation: skip row to trades "
                f"(skip_reason={decision.skip_reason!r}, "
                f"engine={self._engine_name!r}, "
                f"cond={decision.condition_id[:12]!r})."
            )

    async def monitor_positions(self) -> None:
        """Exit-path tick. Pass 1: COLD_CULL stale-losing rows. Pass 2:
        TP/SL/TRAIL/TIMEOUT/WIN/LOSS via `_close_pure` on all open positions.
        Prices for pass 2 are fetched concurrently via `asyncio.gather`."""
        session = self._get_session()
        now = datetime.now(timezone.utc)

        cold = await self._db.get_cold_losing_positions(
            self._cfg.cold_position_hours, engine_name=self._engine_name
        )
        for pos in cold:
            price = await _fetch_mid_price(session, pos["token_id"])
            if price is None:
                continue
            outcome = _cold_cull_outcome(pos, price, self._cfg)
            await self._persist_close(pos, outcome, close_reason="COLD_CULL")
            logger.info(
                "[trader] COLD_CULL %s %s  exit=%.3f  pnl=%.2f",
                pos["direction"], pos["condition_id"][:12], price,
                outcome.realized_pnl,
            )

        positions = await self._db.get_open_positions(engine_name=self._engine_name)
        if not positions:
            return

        prices: list[Optional[float]] = await asyncio.gather(
            *[_fetch_mid_price(session, pos["token_id"]) for pos in positions]
        )

        for pos, current_price in zip(positions, prices):
            if current_price is None:
                continue
            outcome = _close_pure(pos, current_price, self._cfg, now)
            await self._db.update_position_price(
                pos["trade_id"], current_price,
                outcome.unrealized_pnl,
                high_water_price=outcome.high_water_price,
            )
            if outcome.action == "HOLD":
                continue
            await self._persist_close(pos, outcome,
                                      close_reason=outcome.close_reason)
            sign = "+" if outcome.realized_pnl >= 0 else ""
            logger.info(
                "[trader] CLOSE %s %s @ %.3f  %s  PnL=%s%.2f  balance=%.2f",
                pos["direction"], pos["condition_id"][:12], current_price,
                outcome.close_reason, sign, outcome.realized_pnl, self._balance,
            )

    @property
    def balance(self) -> float:
        return self._balance

    @property
    def total_staked(self) -> float:
        return self._total_staked

    @property
    def infinite_balance(self) -> bool:
        return self._cfg.infinite_balance

    @property
    def cfg(self):
        return self._cfg
