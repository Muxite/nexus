"""Tests for seulgi/app/fastpath.py — Phase 4C.1.

Covers:
- `FastPath.execute_trade` enqueues, never writes DB.
- `OpenPositionsState` updates atomically; duplicate check works
  against in-memory state.
- Boot-time hydration from `positions` table.
- `snapshot_state()` returns a deep copy.
- `record_skip` enqueues without DB.
- `_build_trade_kwargs` round-trips the schema A4C.2's worker expects.

A4C.2's `AsyncTelemetryQueue` is the writer side; we stub it with a
recording double here — tests verify FastPath calls `.put(...)` with
the correct row types.
"""

from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass
from typing import Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fastpath import (
    ExecutionOutcome,
    FastPath,
    OpenPositionsState,
    _build_trade_kwargs,
)
from telemetry import (
    EngineDecisionsRow,
    PRIORITY_SKIP,
    PRIORITY_TRADE,
    TradeWriteRow,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _RecordingQueue:
    """In-memory double for A4C.2's AsyncTelemetryQueue. We only need
    `.put(row)` on the FastPath path."""

    def __init__(self) -> None:
        self.rows: list = []

    async def put(self, row) -> None:
        self.rows.append(row)


@dataclass
class _FakeDir:
    value: str


@dataclass
class _FakeDecision:
    """Minimal stand-in for trader.TradeDecision."""

    condition_id: str = "cid_x"
    question: str = "Will X?"
    direction: _FakeDir = None  # type: ignore
    similarity: float = 0.7
    p_hat: float = 0.5
    mid_price: Optional[float] = 0.40
    size_usdc: float = 10.0
    entry_price: Optional[float] = 0.42
    shares: Optional[float] = 24.0
    ev_per_dollar: float = 0.07
    signal_id: int = 7
    token_id: Optional[str] = "ty1"
    inverted: bool = False
    mid_yes: Optional[float] = 0.40
    mid_no: Optional[float] = 0.60
    agreement: float = 0.8
    p_alpha: float = 0.62
    risk_score: float = 0.1

    def __post_init__(self) -> None:
        if self.direction is None:
            self.direction = _FakeDir("YES")


def _mock_db_with_positions(rows: list[dict]) -> MagicMock:
    db = MagicMock()
    db.get_open_positions = AsyncMock(return_value=rows)
    return db


# ---------------------------------------------------------------------------
# OpenPositionsState
# ---------------------------------------------------------------------------


class TestOpenPositionsState:
    @pytest.mark.asyncio
    async def test_add_and_has(self):
        s = OpenPositionsState()
        await s.add("cid_a", "scout", {"size_usdc": 10})
        assert await s.has("cid_a", "scout") is True
        assert await s.has("cid_a", "other") is False
        assert await s.has("cid_b", "scout") is False

    @pytest.mark.asyncio
    async def test_remove(self):
        s = OpenPositionsState()
        await s.add("cid_a", "scout", {"size_usdc": 10})
        await s.remove("cid_a", "scout")
        assert await s.has("cid_a", "scout") is False
        # Idempotent.
        await s.remove("cid_a", "scout")

    @pytest.mark.asyncio
    async def test_count_and_unique_markets(self):
        s = OpenPositionsState()
        await s.add("cid_a", "scout", {})
        await s.add("cid_a", "main", {})
        await s.add("cid_b", "scout", {})
        assert await s.count() == 3
        assert await s.count("scout") == 2
        assert await s.unique_markets() == 2
        assert await s.unique_markets("scout") == 2

    @pytest.mark.asyncio
    async def test_concurrent_adds_atomic(self):
        """Two concurrent add() calls must both land — the lock
        prevents the dict-mutation lost-update class of bug."""
        s = OpenPositionsState()

        async def _add(i: int) -> None:
            await s.add(f"cid_{i}", "scout", {"i": i})

        await asyncio.gather(*[_add(i) for i in range(50)])
        assert await s.count() == 50

    @pytest.mark.asyncio
    async def test_snapshot_is_deep_copy(self):
        s = OpenPositionsState()
        await s.add("cid_a", "scout", {"size_usdc": 10, "nested": {"x": 1}})
        snap = s.snapshot()
        # Mutating the snapshot must not bleed into state.
        snap[("cid_a", "scout")]["size_usdc"] = 9999
        snap[("cid_a", "scout")]["nested"]["x"] = 9999
        snap[("cid_z", "main")] = {"size_usdc": 1}
        snap2 = s.snapshot()
        assert snap2[("cid_a", "scout")]["size_usdc"] == 10
        assert snap2[("cid_a", "scout")]["nested"]["x"] == 1
        assert ("cid_z", "main") not in snap2


# ---------------------------------------------------------------------------
# FastPath.execute_trade — hot-path slim
# ---------------------------------------------------------------------------


class TestExecuteTrade:
    @pytest.mark.asyncio
    async def test_execute_trade_does_not_touch_db(self):
        """The critical path is forbidden from synchronous DB writes."""
        fp = FastPath()
        q = _RecordingQueue()
        db = MagicMock()
        db.insert_trade = AsyncMock()
        db.open_position = AsyncMock()
        db.record_engine_decisions = AsyncMock()
        outcome = await fp.execute_trade(
            _FakeDecision(), q, engine_name="scout",
        )
        assert outcome.executed is True
        assert outcome.condition_id == "cid_x"
        assert outcome.size_usdc == 10.0
        assert outcome.ev_per_dollar == pytest.approx(0.07)
        db.insert_trade.assert_not_called()
        db.open_position.assert_not_called()
        db.record_engine_decisions.assert_not_called()

    @pytest.mark.asyncio
    async def test_execute_trade_enqueues_trade_row(self):
        fp = FastPath()
        q = _RecordingQueue()
        await fp.execute_trade(_FakeDecision(), q, engine_name="main")
        assert len(q.rows) == 1
        row = q.rows[0]
        assert isinstance(row, TradeWriteRow)
        assert row.priority == PRIORITY_TRADE
        assert row.kwargs["condition_id"] == "cid_x"
        assert row.kwargs["engine_name"] == "main"
        assert row.kwargs["size_usdc"] == 10.0
        assert row.kwargs["direction"] == "YES"
        assert row.kwargs["skip_reason"] is None

    @pytest.mark.asyncio
    async def test_execute_trade_updates_in_memory_state(self):
        fp = FastPath()
        q = _RecordingQueue()
        assert await fp.state.has("cid_x", "scout") is False
        await fp.execute_trade(_FakeDecision(), q, engine_name="scout")
        # Duplicate-position check against in-memory state.
        assert await fp.state.has("cid_x", "scout") is True
        assert await fp.state.has("cid_x", "main") is False

    @pytest.mark.asyncio
    async def test_execute_trade_live_ev_overrides_decision(self):
        """A4A.2 hook: when JIT recompute supplies a fresh EV, the
        outcome reports it (and the decision's stale value is ignored)."""
        fp = FastPath()
        q = _RecordingQueue()
        outcome = await fp.execute_trade(
            _FakeDecision(ev_per_dollar=0.01), q,
            engine_name="scout", live_ev_per_dollar=0.09,
        )
        assert outcome.ev_per_dollar == pytest.approx(0.09)


# ---------------------------------------------------------------------------
# FastPath.record_skip
# ---------------------------------------------------------------------------


class TestRecordSkip:
    @pytest.mark.asyncio
    async def test_record_skip_enqueues_no_db(self):
        fp = FastPath()
        q = _RecordingQueue()
        outcome = await fp.record_skip(
            "low_confidence:0.30",
            {"engine_name": "scout", "condition_id": "cid_q",
             "question": "Will X?"},
            q,
        )
        assert outcome.executed is False
        assert outcome.skip_reason == "low_confidence:0.30"
        assert len(q.rows) == 1
        row = q.rows[0]
        assert isinstance(row, EngineDecisionsRow)
        assert row.priority == PRIORITY_SKIP  # drops first under pressure.
        assert row.engine_name == "scout"

    @pytest.mark.asyncio
    async def test_record_skip_does_not_mutate_state(self):
        fp = FastPath()
        q = _RecordingQueue()
        await fp.record_skip(
            "neutral",
            {"engine_name": "scout", "condition_id": "cid_q"},
            q,
        )
        assert await fp.state.count() == 0


# ---------------------------------------------------------------------------
# Boot-time hydration
# ---------------------------------------------------------------------------


class TestHydrate:
    @pytest.mark.asyncio
    async def test_hydrate_loads_three_positions(self):
        rows = [
            {"condition_id": "cid_1", "engine_name": "scout",
             "size_usdc": 5.0, "entry_price": 0.4},
            {"condition_id": "cid_2", "engine_name": "main",
             "size_usdc": 8.0, "entry_price": 0.55},
            {"condition_id": "cid_3", "engine_name": "scout",
             "size_usdc": 3.0, "entry_price": 0.6},
        ]
        db = _mock_db_with_positions(rows)
        fp = FastPath()
        n = await fp.hydrate(db)
        assert n == 3
        assert await fp.state.count() == 3
        assert await fp.state.has("cid_1", "scout")
        assert await fp.state.has("cid_2", "main")
        assert await fp.state.has("cid_3", "scout")
        # Boot reads `positions` exactly once.
        db.get_open_positions.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_hydrate_handles_missing_engine_name(self):
        """Legacy rows with NULL engine_name → default."""
        rows = [{"condition_id": "cid_legacy", "engine_name": None,
                 "size_usdc": 1.0, "entry_price": 0.5}]
        db = _mock_db_with_positions(rows)
        fp = FastPath()
        await fp.hydrate(db)
        assert await fp.state.has("cid_legacy", "default")

    @pytest.mark.asyncio
    async def test_hydrate_empty_is_ok(self):
        db = _mock_db_with_positions([])
        fp = FastPath()
        n = await fp.hydrate(db)
        assert n == 0
        assert await fp.state.count() == 0


# ---------------------------------------------------------------------------
# snapshot_state
# ---------------------------------------------------------------------------


class TestSnapshotState:
    @pytest.mark.asyncio
    async def test_snapshot_returns_deep_copy(self):
        fp = FastPath()
        q = _RecordingQueue()
        await fp.execute_trade(_FakeDecision(), q, engine_name="scout")
        snap = fp.snapshot_state()
        assert ("cid_x", "scout") in snap
        # Mutation isolation.
        snap[("cid_x", "scout")]["size_usdc"] = -1
        snap2 = fp.snapshot_state()
        assert snap2[("cid_x", "scout")]["size_usdc"] == 10.0


# ---------------------------------------------------------------------------
# Schema contract with A4C.2 — `_build_trade_kwargs` shape must match
# `db.insert_trade` signature.
# ---------------------------------------------------------------------------


class TestSchemaContract:
    def test_build_trade_kwargs_has_required_fields(self):
        d = _FakeDecision()
        kw = _build_trade_kwargs(d, "scout")
        # These are the fields A4C.2's worker splats into db.insert_trade.
        required = {
            "signal_id", "condition_id", "question", "token_id",
            "direction", "similarity", "p_hat", "mid_price",
            "size_usdc", "skip_reason", "engine_name",
        }
        assert required.issubset(set(kw.keys()))
        assert kw["engine_name"] == "scout"
        assert kw["direction"] == "YES"
        assert kw["skip_reason"] is None
        assert kw["size_usdc"] == 10.0
