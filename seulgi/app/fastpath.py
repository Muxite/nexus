"""Trade-critical fast-path (Phase 4C.1 — 2026-05-23).

Three steps. Nothing else on the hot path.
    1. EV calc against the LIVE BBO.
    2. Exchange API call (paper/simulate today, real CLOB later).
    3. In-memory state update (OpenPositionsState).

DB writes for `trades`, `engine_results`, and skip rows ENQUEUE to a
`TelemetryQueue`; sibling A4C.2 (`telemetry.py`) ships the concrete
`AsyncTelemetryQueue` worker that drains the queue and dispatches to
`db.insert_trade` / `db.record_engine_decisions`.

Row schema is shared with A4C.2 — we import `TradeWriteRow`,
`EngineDecisionsRow`, `TelemetryQueue`, and the priority constants
directly from `telemetry.py`. This avoids drift between writer + reader.

Stays ≤ 250 lines on purpose: the fast path is small; slop accumulates
elsewhere.
"""

from __future__ import annotations

import asyncio
import copy
import logging
from dataclasses import dataclass, field
from typing import Any, Optional

from telemetry import (
    EngineDecisionsRow,
    PRIORITY_SKIP,
    PRIORITY_TRADE,
    TelemetryQueue,
    TradeWriteRow,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Execution outcome — what FastPath returns to the caller.
# ---------------------------------------------------------------------------


@dataclass
class ExecutionOutcome:
    executed: bool
    condition_id: str
    engine_name: str
    direction: Optional[str] = None
    size_usdc: float = 0.0
    shares: Optional[float] = None
    entry_price: Optional[float] = None
    ev_per_dollar: Optional[float] = None
    skip_reason: Optional[str] = None
    notes: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# OpenPositionsState — single source of truth on the hot path. Replaces
# the `db.has_open_position` query. Monitor cycle (off-path) reconciles
# with the DB.
# ---------------------------------------------------------------------------


class OpenPositionsState:
    """`(condition_id, engine_name) -> list[position dict]`.

    Hydrated from DB once at boot; mutated in-process thereafter.

    A4D.3 (2026-05-23): each key now maps to a LIST of position dicts so
    position scaling (multiple delta-adds against the same condition × engine)
    can coexist. Pre-A4D.3 callers treated this as a single position dict;
    `has()` / `count()` / `unique_markets()` semantics preserved by counting
    keys with at least one position.
    """

    def __init__(self) -> None:
        self._positions: dict[tuple[str, str], list[dict]] = {}
        self._lock = asyncio.Lock()

    async def add(self, condition_id: str, engine_name: str, pos: dict) -> None:
        """Append a position to the (cond_id, engine) bucket. Multiple
        adds on the same key STACK — each represents a separate trade row."""
        async with self._lock:
            self._positions.setdefault(
                (condition_id, engine_name), []
            ).append(dict(pos))

    async def remove(self, condition_id: str, engine_name: str) -> None:
        """Remove ALL positions on this key. Legacy single-position callers
        keep this behaviour; multi-position closers should use
        `remove_by_trade_id` to drop one leg at a time."""
        async with self._lock:
            self._positions.pop((condition_id, engine_name), None)

    async def remove_by_trade_id(
        self, condition_id: str, engine_name: str, trade_id: Any,
    ) -> bool:
        """A4D.3: drop a single leg by `trade_id`. Returns True iff a row
        was removed. Empties the bucket entirely when it was the last leg."""
        async with self._lock:
            key = (condition_id, engine_name)
            bucket = self._positions.get(key)
            if not bucket:
                return False
            for i, p in enumerate(bucket):
                if p.get("trade_id") == trade_id:
                    bucket.pop(i)
                    if not bucket:
                        del self._positions[key]
                    return True
            return False

    async def has(self, condition_id: str, engine_name: str) -> bool:
        async with self._lock:
            return bool(self._positions.get((condition_id, engine_name)))

    async def exposure(self, condition_id: str, engine_name: str) -> float:
        """A4D.3: sum of `size_usdc` across all positions on this key."""
        async with self._lock:
            bucket = self._positions.get((condition_id, engine_name)) or []
            return sum(float(p.get("size_usdc", 0.0) or 0.0) for p in bucket)

    async def positions_for(
        self, condition_id: str, engine_name: str,
    ) -> list[dict]:
        """A4D.3: deep-copy of every position on this key. Callers iterate
        without holding the lock."""
        async with self._lock:
            bucket = self._positions.get((condition_id, engine_name)) or []
            return [dict(p) for p in bucket]

    async def count(self, engine_name: Optional[str] = None) -> int:
        """Number of (cond_id, engine) keys with at least one position.
        NOT the leg count — open-position cap is per-condition, not per-leg.
        """
        async with self._lock:
            if engine_name is None:
                return sum(1 for v in self._positions.values() if v)
            return sum(1 for (_, e), v in self._positions.items()
                       if e == engine_name and v)

    async def unique_markets(self, engine_name: Optional[str] = None) -> int:
        async with self._lock:
            if engine_name is None:
                return len({c for (c, _), v in self._positions.items() if v})
            return len({c for (c, e), v in self._positions.items()
                        if e == engine_name and v})

    def snapshot(self) -> dict[tuple[str, str], list[dict]]:
        """Deep copy for the dashboard. Single torn read tolerated."""
        return copy.deepcopy(self._positions)


# ---------------------------------------------------------------------------
# FastPath — the hot-path orchestrator.
# ---------------------------------------------------------------------------


def _build_trade_kwargs(decision: Any, engine_name: str) -> dict:
    """Project a TradeDecision-like object onto `db.insert_trade(**kwargs)`
    shape. A4C.2's worker invokes `db.insert_trade(**row.kwargs)` so the
    schema lives here, on the producer side."""
    d = decision
    mid = getattr(d, "mid_price", None)
    direction = getattr(d, "direction_value", None) or (
        d.direction.value if hasattr(getattr(d, "direction", None), "value")
        else getattr(d, "direction", None)
    )
    return {
        "signal_id": getattr(d, "signal_id", None),
        "condition_id": d.condition_id,
        "question": getattr(d, "question", ""),
        "token_id": getattr(d, "token_id", None),
        "direction": direction,
        "similarity": getattr(d, "similarity", 0.0),
        "p_hat": getattr(d, "p_hat", 0.0),
        "mid_price": mid,
        "size_usdc": float(getattr(d, "size_usdc", 0.0) or 0.0),
        "skip_reason": None,
        "engine_name": engine_name,
        "agreement": getattr(d, "agreement", 0.0),
        "p_alpha": getattr(d, "p_alpha", 0.0),
        "risk_score": getattr(d, "risk_score", 0.0),
        "mid_yes": getattr(d, "mid_yes", None),
        "mid_no": getattr(d, "mid_no", None),
        "inverted": 1 if getattr(d, "inverted", False) else 0,
    }


class FastPath:
    """Owns the trade-critical sequence + `OpenPositionsState`. No DB
    writes on the hot path — telemetry goes through the queue."""

    def __init__(self, state: Optional[OpenPositionsState] = None) -> None:
        self.state = state if state is not None else OpenPositionsState()
        self._hydrated = False

    async def hydrate(self, db: Any) -> int:
        """Boot-time hydration. After this, in-memory state is the
        source of truth for duplicate-position checks. Returns row count."""
        rows = await db.get_open_positions()
        for row in rows:
            cond = row["condition_id"]
            eng = row.get("engine_name") or "default"
            await self.state.add(cond, eng, row)
        self._hydrated = True
        logger.info("[fastpath] hydrated %d open positions from DB", len(rows))
        return len(rows)

    async def execute_trade(
        self,
        decision: Any,
        telemetry: TelemetryQueue,
        *,
        engine_name: str,
        # TODO(A4A.2 wiring): swap pre-computed EV for live-BBO recompute
        # via `execution.execute_with_jit_pricing` once trader is wired
        # to call us with the recomputed value.
        live_ev_per_dollar: Optional[float] = None,
    ) -> ExecutionOutcome:
        """Critical-path sequence — three steps. No DB calls."""
        # 1. EV against live BBO (fallback to decision's value).
        ev = live_ev_per_dollar
        if ev is None:
            ev = float(getattr(decision, "ev_per_dollar", 0.0) or 0.0)

        # 2. Exchange API. Paper/simulate fills happen upstream today;
        # this is the seam where the real CLOB call will land.
        entry_price = getattr(decision, "entry_price", None) or getattr(
            decision, "mid_price", None
        )
        size_usdc = float(getattr(decision, "size_usdc", 0.0) or 0.0)
        shares = getattr(decision, "shares", None)
        direction_val = getattr(decision, "direction_value", None)
        if direction_val is None:
            d = getattr(decision, "direction", None)
            direction_val = d.value if hasattr(d, "value") else d

        # 3. In-memory state update.
        # A4D.3 — carry `trade_id` when the decision exposes it (FastPath
        # callers wire it on after the telemetry writer has minted one) so
        # multi-leg buckets can be drained one leg at a time on close via
        # `remove_by_trade_id`.
        pos_row = {
            "condition_id": decision.condition_id,
            "engine_name": engine_name,
            "direction": direction_val,
            "size_usdc": size_usdc,
            "entry_price": entry_price,
            "shares": shares,
            "trade_id": getattr(decision, "trade_id", None),
        }
        await self.state.add(decision.condition_id, engine_name, pos_row)

        # Off-path telemetry enqueue. Build the insert_trade kwargs on
        # the producer side so A4C.2's worker can just splat them.
        kwargs = _build_trade_kwargs(decision, engine_name)
        await telemetry.put(TradeWriteRow(
            kwargs=kwargs, priority=PRIORITY_TRADE,
        ))
        return ExecutionOutcome(
            executed=True, condition_id=decision.condition_id,
            engine_name=engine_name, direction=direction_val,
            size_usdc=size_usdc, shares=shares,
            entry_price=entry_price, ev_per_dollar=ev,
        )

    async def record_skip(
        self, skip_reason: str, ctx: dict, telemetry: TelemetryQueue,
        decision: Any = None,
    ) -> ExecutionOutcome:
        """Skip-side off-path enqueue. NO synchronous DB write. Routes
        through `EngineDecisionsRow` at PRIORITY_SKIP — drops first
        under backpressure (per A4C.2)."""
        engine_name = ctx.get("engine_name", "default")
        condition_id = ctx.get("condition_id", "")
        # We carry the decision through unchanged when the caller has it;
        # otherwise the worker can lift fields from ctx.
        decisions = [decision] if decision is not None else []
        await telemetry.put(EngineDecisionsRow(
            engine_name=engine_name, decisions=decisions,
            headline_id=ctx.get("headline_id"),
            priority=PRIORITY_SKIP,
        ))
        return ExecutionOutcome(
            executed=False, condition_id=condition_id,
            engine_name=engine_name, skip_reason=skip_reason,
        )

    def snapshot_state(self) -> dict[tuple[str, str], list[dict]]:
        """Dashboard / health-check accessor. Deep copy; no locking.

        A4D.3: returns lists per key (multiple legs supported).
        """
        return self.state.snapshot()
