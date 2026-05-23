"""
Just-In-Time pricing + IOC/FOK execution layer (Phase 4A.2 — 2026-05-23).

Closes the alpha-leak between the LLM snapshot mid-price (taken BEFORE
the 1-3s Stage-2 + Stage-3 LLM calls) and the price actually executed.
The trader still computes EV on the snapshot view; this module
refetches the top-of-book once more, then either fires an IOC limit at
the live price or bails if the market moved against us > tolerance.

Public: ExecutionResult dataclass, execute_with_jit_pricing() (runtime
entry), simulate_ioc_outcome() (pure decision; shared with backtest).

Invariants
    - Pure-ish: NO DB writes. trader.py persists the ExecutionResult.
    - `max_acceptable_price` is a hard cap; equality wins (no-move fills).
    - LIVE mode (SEULGI_LIVE=1) raises NotImplementedError; 4A.2 ships
      paper-mode simulation only.

Backtest: simulate_ioc_outcome is shared with scripts/replay/backtest.py.
Same (snapshot_mid, live_price, tolerance_bps) → same ExecutionResult.

Telemetry: emits `jit.ioc_fill` / `jit.ioc_fail` counters and a
`jit.delta_bps` gauge via app/metrics.py (best-effort; no-op without it).
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Optional

import aiohttp

try:
    import metrics as _metrics
except Exception:  # noqa: BLE001 — analysis CLIs import without metrics.
    _metrics = None

try:
    import simulate as sim
except Exception:  # noqa: BLE001
    sim = None  # type: ignore

logger = logging.getLogger(__name__)

# 50 bps = 0.50% relative drift window. Lives on TradingConfig.jit_tolerance_bps.
DEFAULT_JIT_TOLERANCE_BPS: float = 50.0


@dataclass
class ExecutionResult:
    """Outcome of one JIT pricing + IOC attempt."""
    filled: bool
    fill_price: Optional[float]
    fill_size_usdc: Optional[float]
    skip_reason: Optional[str]
    latency_ms: int
    snapshot_mid: Optional[float] = None
    live_price: Optional[float] = None
    delta_bps: Optional[float] = None
    tolerance_bps: float = DEFAULT_JIT_TOLERANCE_BPS


def _signed_delta_bps(snapshot_mid: float, live_price: float) -> float:
    """Signed bps live-snapshot, relative to snapshot. Positive ⇒ live UP."""
    if snapshot_mid <= 0.0:
        return 0.0
    return (live_price - snapshot_mid) / snapshot_mid * 10_000.0


def _emit(name: str, value: Optional[float] = None,
          tags: Optional[dict] = None) -> None:
    """Best-effort metric emit; never raises into the trader hot path."""
    if _metrics is None:
        return
    try:
        if value is None:
            _metrics.incr(name, tags=tags)
        else:
            _metrics.observe(name, value, tags=tags)
    except Exception:  # noqa: BLE001
        pass


def simulate_ioc_outcome(
    snapshot_mid: float,
    live_price: Optional[float],
    side: str,
    target_size_usdc: float,
    max_acceptable_price: float,
    tolerance_bps: float,
    latency_ms: int,
    cfg=None,
    market_type: Optional[str] = None,
) -> ExecutionResult:
    """Deterministic IOC outcome from one snapshot/live pair. Shared by
    paper-mode runtime AND offline backtest replay. `live_price=None` →
    ioc_fail_no_liquidity. `tolerance_bps` is the drift window relative
    to snapshot_mid. `cfg` (TradingConfig) enables simulate-mode slippage."""
    if live_price is None:
        return ExecutionResult(
            filled=False, fill_price=None, fill_size_usdc=None,
            skip_reason="ioc_fail_no_liquidity", latency_ms=latency_ms,
            snapshot_mid=snapshot_mid, live_price=None, delta_bps=None,
            tolerance_bps=tolerance_bps,
        )

    delta_bps = _signed_delta_bps(snapshot_mid, live_price)

    # Hard cap — never fill above the trader's max_acceptable_price.
    if live_price > max_acceptable_price + 1e-9:
        return ExecutionResult(
            filled=False, fill_price=None, fill_size_usdc=None,
            skip_reason="ioc_fail_price_moved", latency_ms=latency_ms,
            snapshot_mid=snapshot_mid, live_price=live_price,
            delta_bps=delta_bps, tolerance_bps=tolerance_bps,
        )

    # Belt-and-braces: if the caller passed a looser cap than the
    # tolerance alone allows, still enforce the bps band on upward drift.
    if delta_bps > tolerance_bps + 1e-9:
        return ExecutionResult(
            filled=False, fill_price=None, fill_size_usdc=None,
            skip_reason="ioc_fail_price_moved", latency_ms=latency_ms,
            snapshot_mid=snapshot_mid, live_price=live_price,
            delta_bps=delta_bps, tolerance_bps=tolerance_bps,
        )

    # Rail-snap: live saturated. Treat as no_liquidity.
    if sim is not None and sim.is_at_rail(live_price):
        return ExecutionResult(
            filled=False, fill_price=None, fill_size_usdc=None,
            skip_reason="rail", latency_ms=latency_ms,
            snapshot_mid=snapshot_mid, live_price=live_price,
            delta_bps=delta_bps, tolerance_bps=tolerance_bps,
        )

    # SUCCESS — fill at live price. In simulate mode, apply_slippage
    # walks the book so micro-cap notional still pays the bps drag.
    fill_price = live_price
    if (sim is not None and cfg is not None
            and getattr(cfg, "mode", "paper") == "simulate"
            and getattr(cfg, "simulate_slippage", None) is not None):
        slip = sim.apply_slippage(
            mid=live_price, notional_usdc=target_size_usdc, side=side,
            market_type=market_type, cfg=cfg.simulate_slippage,
        )
        if slip.effective_price > max_acceptable_price + 1e-9:
            return ExecutionResult(
                filled=False, fill_price=None, fill_size_usdc=None,
                skip_reason="ioc_fail_price_moved", latency_ms=latency_ms,
                snapshot_mid=snapshot_mid, live_price=live_price,
                delta_bps=delta_bps, tolerance_bps=tolerance_bps,
            )
        fill_price = slip.effective_price

    return ExecutionResult(
        filled=True, fill_price=fill_price, fill_size_usdc=target_size_usdc,
        skip_reason=None, latency_ms=latency_ms,
        snapshot_mid=snapshot_mid, live_price=live_price,
        delta_bps=delta_bps, tolerance_bps=tolerance_bps,
    )


async def _refetch_live_price(
    session: aiohttp.ClientSession, token_id: str,
) -> tuple[Optional[float], int]:
    """Refetch top-of-book mid. Returns (mid, latency_ms).

    Uses `trader._fetch_mid_price` today — it shares the canonical
    /midpoint endpoint with the rest of the bot, and existing tests
    patch this symbol directly. When A4A.3 lands real bid/ask parsing
    in `impact_tracker._fetch_bbo`, swap the helper here.
    """
    t0 = time.monotonic()
    mid: Optional[float] = None
    try:
        from trader import _fetch_mid_price
        mid = await _fetch_mid_price(session, token_id)
    except Exception as exc:  # noqa: BLE001
        logger.debug("[exec] live-price refetch failed %s: %s",
                     (token_id or "?")[:12], exc)
        mid = None
    return mid, int((time.monotonic() - t0) * 1000)


async def execute_with_jit_pricing(
    session: aiohttp.ClientSession,
    condition_id: str,
    token_id: str,
    direction: str,
    target_size_usdc: float,
    snapshot_mid: float,
    max_acceptable_price: float,
    *,
    tolerance_bps: float = DEFAULT_JIT_TOLERANCE_BPS,
    cfg=None,
    market_type: Optional[str] = None,
    headline_id: Optional[str] = None,
) -> ExecutionResult:
    """JIT-refetch + IOC decision for one trade about to fire. Refetches
    live top-of-book, computes snapshot→live delta, then delegates to
    `simulate_ioc_outcome`. LIVE mode raises NotImplementedError."""
    if os.environ.get("SEULGI_LIVE") == "1":
        raise NotImplementedError(
            "TODO(4A.live): SEULGI_LIVE IOC post-to-CLOB not implemented; "
            "4A.2 ships paper-mode simulation only.",
        )

    live_price, latency_ms = await _refetch_live_price(session, token_id)
    result = simulate_ioc_outcome(
        snapshot_mid=snapshot_mid, live_price=live_price,
        side=direction, target_size_usdc=target_size_usdc,
        max_acceptable_price=max_acceptable_price,
        tolerance_bps=tolerance_bps, latency_ms=latency_ms,
        cfg=cfg, market_type=market_type,
    )

    hid_short = (headline_id or "?")[:12]
    snap_str = f"{snapshot_mid:.4f}" if snapshot_mid is not None else "?"
    live_str = f"{result.live_price:.4f}" if result.live_price is not None else "?"
    delta_str = (f"{result.delta_bps / 100.0:+.2f}%"
                 if result.delta_bps is not None else "?")
    outcome_str = (
        f"FILLED @ {result.fill_price:.4f}"
        if result.filled and result.fill_price is not None
        else f"SKIP {result.skip_reason}"
    )
    logger.info(
        "[EXEC] [hid=%s] JIT refetch took %dms, snapshot=%s live=%s "
        "delta=%s -> %s",
        hid_short, latency_ms, snap_str, live_str, delta_str, outcome_str,
    )

    if result.delta_bps is not None:
        _emit("jit.delta_bps", value=result.delta_bps,
              tags={"side": direction})
    _emit("jit.refetch_latency_ms", value=float(latency_ms))
    if result.filled:
        _emit("jit.ioc_fill", tags={"side": direction})
    else:
        _emit("jit.ioc_fail",
              tags={"side": direction, "reason": result.skip_reason or "?"})

    return result
