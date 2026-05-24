"""CloserStage tests.

Strategy:
- Real in-memory DB (`DB.ephemeral()`); pre-populate `markets`, `trades`,
  and sometimes `trade_closures` with raw SQL.
- Fake CLOB client: an object exposing `async def get_mid(token_id)` that
  the test controls per-token.
- Drive a single iteration of the close loop via the public
  `run_close_cycle()` method — no asyncio.sleep, no real poll task.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

import pytest

# DDL registration side effects -------------------------------------------
import app.main  # noqa: F401  — cycles DDL
import app.stages.closer  # noqa: F401
import app.stages.markets  # noqa: F401
import app.stages.trade  # noqa: F401
from app.cycle import cycle_scope
from app.db import DB
from app.deps import StageDeps
from app.stages.closer import CloserStage, _compute_pnl


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeBus:
    """Closer doesn't publish, but StageDeps requires a bus reference."""

    async def publish(self, topic: str, payload: dict) -> None:
        pass

    def subscribe(self, topic: str, handler) -> None:  # noqa: ARG002
        pass

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


@dataclass
class _FakeCLOB:
    """`get_mid` returns whatever the test stuffs into `mids`, keyed by token.
    A token missing from the dict resolves to `None` — the same shape the
    real CLOBClient produces on every recoverable failure mode."""

    mids: dict = field(default_factory=dict)
    calls: list = field(default_factory=list)

    async def get_mid(self, token_id: str) -> Optional[float]:
        self.calls.append(token_id)
        return self.mids.get(token_id)


@dataclass
class _CloserCfg:
    poll_seconds: int = 60
    timeout_hours: float = 24.0
    skip_on_missing_mid: bool = True
    # Cycle 2 rules — disabled by default in tests.
    take_profit_pct: float = 0.0
    stop_loss_pct: float = 0.0
    # Cycle 3 rules — disabled by default in tests (set per-test where needed).
    stagnation_minutes: int = 0
    stagnation_drift: float = 0.02


@dataclass
class _Config:
    closer: _CloserCfg = field(default_factory=_CloserCfg)


# ---------------------------------------------------------------------------
# Fixtures + helpers
# ---------------------------------------------------------------------------


@pytest.fixture
async def db():
    d = DB.ephemeral()
    await d.connect()
    try:
        yield d
    finally:
        await d.close()


async def _seed_market(
    db: DB,
    *,
    condition_id: str = "m1",
    token_yes: str = "tY",
    token_no: str = "tN",
) -> None:
    async with db.batch() as cur:
        await cur.execute(
            """
            INSERT INTO markets
                (condition_id, slug, question, description, category, end_date,
                 volume_usdc, token_yes, token_no, embedding, embedder_id,
                 embedding_dim, embedding_updated_at, active, closed_at,
                 first_seen_at, updated_at)
            VALUES (?, 'slug', 'Q?', 'desc', 'cat', '2026-12-31', 1000.0,
                    ?, ?, NULL, NULL, NULL, NULL, 1, NULL,
                    '2026-05-23T00:00:00+00:00', '2026-05-23T00:00:00+00:00')
            """,
            (condition_id, token_yes, token_no),
        )


async def _seed_trade(
    db: DB,
    *,
    trade_id: int,
    signal_id: int,
    market_id: str = "m1",
    direction: str = "YES",
    mid_price: float = 0.4,
    size_usdc: float = 100.0,
    decided_at: Optional[datetime] = None,
    is_tracer: int = 0,
    tracer_id: Optional[str] = None,
    target_mid: Optional[float] = None,
) -> None:
    if decided_at is None:
        decided_at = datetime.now(timezone.utc)
    async with db.batch() as cur:
        # decisions row first (trades.decision_id has a REFERENCES, though
        # SQLite only enforces it when foreign_keys pragma is ON — which
        # `db.py` sets. So we must seed a parent decision row).
        await cur.execute(
            """
            INSERT INTO decisions
                (signal_id, outcome, reason, gate_history, prediction_id,
                 cycle_id, is_tracer, tracer_id, created_at)
            VALUES (?, 'execute', 'ok', '[]', 1, 1, ?, ?,
                    '2026-05-23T00:00:00+00:00')
            """,
            (signal_id, is_tracer, tracer_id),
        )
        decision_id = cur.lastrowid
        await cur.execute(
            """
            INSERT INTO trades
                (id, signal_id, decision_id, prediction_id, market_id, direction,
                 confidence, similarity, freshness, p_hat, mid_price, edge,
                 kelly_fraction, size_usdc, decision_path, features_snapshot,
                 cycle_id, is_tracer, tracer_id, decided_at, target_mid)
            VALUES (?, ?, ?, 1, ?, ?, 0.7, 0.8, 'BREAKING', 0.7, ?, 0.1, 0.1,
                    ?, 'mvp', '{}', 1, ?, ?, ?, ?)
            """,
            (
                trade_id, signal_id, decision_id, market_id, direction,
                mid_price, size_usdc, is_tracer, tracer_id,
                decided_at.isoformat(), target_mid,
            ),
        )


def _make_stage(db: DB, clob: _FakeCLOB, cfg: Optional[_Config] = None) -> CloserStage:
    deps = StageDeps(db=db, bus=_FakeBus(), config=cfg or _Config(), clob=clob)
    return CloserStage(deps)


async def _closures(db: DB) -> list[dict]:
    cur = await db.conn.execute(
        "SELECT trade_id, close_reason, exit_mid_price, realized_pnl_usdc, "
        "cycle_id, is_tracer, tracer_id FROM trade_closures ORDER BY id"
    )
    rows = await cur.fetchall()
    await cur.close()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_owned_table_exists_after_connect():
    d = DB.ephemeral()
    await d.connect()
    try:
        cur = await d.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='trade_closures'"
        )
        row = await cur.fetchone()
        await cur.close()
        assert row is not None
    finally:
        await d.close()


async def test_handle_raises_not_implemented(db):
    stage = _make_stage(db, _FakeCLOB())
    with pytest.raises(NotImplementedError):
        await stage.handle({})


async def test_init_requires_clob(db):
    with pytest.raises(RuntimeError, match="clob"):
        CloserStage(StageDeps(db=db, bus=_FakeBus(), config=_Config(), clob=None))


async def test_timeout_fires_when_trade_is_old_enough(db):
    await _seed_market(db)
    decided = datetime.now(timezone.utc) - timedelta(hours=25)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=decided,
                      direction="YES", mid_price=0.4, size_usdc=100.0)
    clob = _FakeCLOB(mids={"tY": 0.5})

    stage = _make_stage(db, clob)
    with cycle_scope(7):
        written = await stage.run_close_cycle()

    assert written == 1
    rows = await _closures(db)
    assert len(rows) == 1
    assert rows[0]["trade_id"] == 1
    assert rows[0]["close_reason"] == "timeout"
    assert rows[0]["cycle_id"] == 7
    assert rows[0]["exit_mid_price"] == pytest.approx(0.5)


async def test_trade_not_yet_timed_out_stays_open(db):
    await _seed_market(db)
    decided = datetime.now(timezone.utc) - timedelta(hours=1)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=decided)
    clob = _FakeCLOB(mids={"tY": 0.5})

    stage = _make_stage(db, clob)
    with cycle_scope(1):
        written = await stage.run_close_cycle()

    assert written == 0
    assert await _closures(db) == []
    # Cycle 2: CLOB is queried to evaluate take_profit/stop_loss even
    # when no rule fires. The token IS queried; we just don't close.
    assert clob.calls == ["tY"]


async def test_take_profit_fires_when_gain_exceeds_pct(db):
    # YES bought at 0.4, current 0.7 → pnl_frac = (0.7-0.4)/0.4 = 0.75 (75%).
    # take_profit_pct=0.50 should fire. timeout_hours stays at 24h, decided 1h ago.
    await _seed_market(db)
    decided = datetime.now(timezone.utc) - timedelta(hours=1)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=decided,
                      direction="YES", mid_price=0.4, size_usdc=10.0)
    clob = _FakeCLOB(mids={"tY": 0.7})
    cfg = _Config(closer=_CloserCfg(take_profit_pct=0.50, stop_loss_pct=0.30))
    stage = _make_stage(db, clob, cfg=cfg)
    with cycle_scope(1):
        n = await stage.run_close_cycle()
    assert n == 1
    rows = await _closures(db)
    assert rows[0]["close_reason"] == "target"
    assert rows[0]["exit_mid_price"] == pytest.approx(0.7)
    assert rows[0]["realized_pnl_usdc"] == pytest.approx(7.5)  # 10 * 0.75


async def test_stop_loss_fires_when_loss_exceeds_pct(db):
    # YES bought at 0.5, current 0.2 → pnl_frac = (0.2-0.5)/0.5 = -0.60.
    # stop_loss_pct=0.30 should fire.
    await _seed_market(db)
    decided = datetime.now(timezone.utc) - timedelta(hours=1)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=decided,
                      direction="YES", mid_price=0.5, size_usdc=10.0)
    clob = _FakeCLOB(mids={"tY": 0.2})
    cfg = _Config(closer=_CloserCfg(take_profit_pct=0.50, stop_loss_pct=0.30))
    stage = _make_stage(db, clob, cfg=cfg)
    with cycle_scope(1):
        n = await stage.run_close_cycle()
    assert n == 1
    rows = await _closures(db)
    assert rows[0]["close_reason"] == "stop"


async def test_neither_rule_fires_when_pnl_within_band(db):
    # YES at 0.5, current 0.55 → pnl_frac = +10%; neither rule triggers.
    # Trade decided 1h ago so timeout (24h) also doesn't trigger.
    await _seed_market(db)
    decided = datetime.now(timezone.utc) - timedelta(hours=1)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=decided,
                      direction="YES", mid_price=0.5, size_usdc=10.0)
    clob = _FakeCLOB(mids={"tY": 0.55})
    cfg = _Config(closer=_CloserCfg(take_profit_pct=0.50, stop_loss_pct=0.30))
    stage = _make_stage(db, clob, cfg=cfg)
    with cycle_scope(1):
        n = await stage.run_close_cycle()
    assert n == 0
    assert await _closures(db) == []


async def test_edge_decay_fires_when_yes_mid_reaches_target(db):
    # YES bought at 0.4 with target_mid=0.65. Current 0.66 → edge_decay.
    await _seed_market(db)
    decided = datetime.now(timezone.utc) - timedelta(minutes=10)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=decided,
                      direction="YES", mid_price=0.4, size_usdc=10.0,
                      target_mid=0.65)
    clob = _FakeCLOB(mids={"tY": 0.66})
    cfg = _Config(closer=_CloserCfg(take_profit_pct=0.50, stop_loss_pct=0.30))
    stage = _make_stage(db, clob, cfg=cfg)
    with cycle_scope(1):
        n = await stage.run_close_cycle()
    assert n == 1
    rows = await _closures(db)
    assert rows[0]["close_reason"] == "edge_decay"


async def test_edge_decay_fires_when_no_mid_reaches_target(db):
    # NO bought at 0.6 (i.e. we bet against YES). target_mid=0.40 (price falls).
    # Current 0.38 → edge_decay.
    await _seed_market(db)
    decided = datetime.now(timezone.utc) - timedelta(minutes=10)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=decided,
                      direction="NO", mid_price=0.6, size_usdc=10.0,
                      target_mid=0.40)
    clob = _FakeCLOB(mids={"tN": 0.38})
    cfg = _Config(closer=_CloserCfg(take_profit_pct=0.50, stop_loss_pct=0.30))
    stage = _make_stage(db, clob, cfg=cfg)
    with cycle_scope(1):
        n = await stage.run_close_cycle()
    assert n == 1
    rows = await _closures(db)
    assert rows[0]["close_reason"] == "edge_decay"


async def test_edge_decay_skipped_when_target_mid_is_null(db):
    # Legacy trade without target_mid — fall through to other rules.
    await _seed_market(db)
    decided = datetime.now(timezone.utc) - timedelta(minutes=10)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=decided,
                      direction="YES", mid_price=0.4, size_usdc=10.0,
                      target_mid=None)
    clob = _FakeCLOB(mids={"tY": 0.66})   # would otherwise trip edge_decay
    cfg = _Config(closer=_CloserCfg(take_profit_pct=0.50, stop_loss_pct=0.30))
    stage = _make_stage(db, clob, cfg=cfg)
    with cycle_scope(1):
        n = await stage.run_close_cycle()
    rows = await _closures(db)
    # Should fire take_profit (gain ~65%), NOT edge_decay.
    assert rows[0]["close_reason"] == "target"


async def test_stagnation_fires_after_minutes_without_movement(db):
    # Price hasn't moved (entry 0.50, current 0.51) and trade is 45 min old.
    await _seed_market(db)
    decided = datetime.now(timezone.utc) - timedelta(minutes=45)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=decided,
                      direction="YES", mid_price=0.50, size_usdc=10.0,
                      target_mid=0.70)
    clob = _FakeCLOB(mids={"tY": 0.51})
    cfg = _Config(closer=_CloserCfg(stagnation_minutes=30, stagnation_drift=0.02))
    stage = _make_stage(db, clob, cfg=cfg)
    with cycle_scope(1):
        n = await stage.run_close_cycle()
    rows = await _closures(db)
    assert rows[0]["close_reason"] == "stagnation"


async def test_stagnation_skipped_when_price_did_move(db):
    # 45 min old, but price moved 0.50 → 0.55 (> 0.02 drift).
    await _seed_market(db)
    decided = datetime.now(timezone.utc) - timedelta(minutes=45)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=decided,
                      direction="YES", mid_price=0.50, size_usdc=10.0,
                      target_mid=0.70)
    clob = _FakeCLOB(mids={"tY": 0.55})
    cfg = _Config(closer=_CloserCfg(stagnation_minutes=30, stagnation_drift=0.02))
    stage = _make_stage(db, clob, cfg=cfg)
    with cycle_scope(1):
        n = await stage.run_close_cycle()
    assert n == 0


async def test_pnl_yes_formula(db):
    await _seed_market(db)
    decided = datetime.now(timezone.utc) - timedelta(hours=48)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=decided,
                      direction="YES", mid_price=0.4, size_usdc=100.0)
    clob = _FakeCLOB(mids={"tY": 0.6})

    stage = _make_stage(db, clob)
    with cycle_scope(1):
        await stage.run_close_cycle()

    rows = await _closures(db)
    # 100 * (0.6 - 0.4) / 0.4 = 50.0
    assert rows[0]["realized_pnl_usdc"] == pytest.approx(50.0)
    # Sanity-check the pure helper matches.
    assert _compute_pnl("YES", 0.4, 0.6, 100.0) == pytest.approx(50.0)


async def test_pnl_no_formula(db):
    # NO direction: entry mid is the price of YES at decision time (0.4),
    # so we bought NO at 1 - 0.4 = 0.6. Exit YES mid 0.6 → exit NO 0.4 →
    # we lost. Formula form (with YES-mids as inputs):
    #   100 * (0.4 - 0.6) / (1 - 0.4) = -33.333…
    await _seed_market(db)
    decided = datetime.now(timezone.utc) - timedelta(hours=48)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=decided,
                      direction="NO", mid_price=0.4, size_usdc=100.0)
    clob = _FakeCLOB(mids={"tN": 0.6})  # NO direction reads token_no

    stage = _make_stage(db, clob)
    with cycle_scope(1):
        await stage.run_close_cycle()

    rows = await _closures(db)
    assert rows[0]["realized_pnl_usdc"] == pytest.approx(-33.3333333, rel=1e-4)
    assert _compute_pnl("NO", 0.4, 0.6, 100.0) == pytest.approx(-33.3333333, rel=1e-4)


async def test_idempotent_double_run(db):
    await _seed_market(db)
    decided = datetime.now(timezone.utc) - timedelta(hours=25)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=decided)
    clob = _FakeCLOB(mids={"tY": 0.5})

    stage = _make_stage(db, clob)
    with cycle_scope(1):
        first = await stage.run_close_cycle()
        second = await stage.run_close_cycle()

    assert first == 1
    assert second == 0  # Second pass sees no open trades.
    rows = await _closures(db)
    assert len(rows) == 1


async def test_skip_on_missing_mid_holds_position(db):
    await _seed_market(db)
    decided = datetime.now(timezone.utc) - timedelta(hours=25)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=decided)
    clob = _FakeCLOB(mids={})  # token has no mid

    stage = _make_stage(db, clob, cfg=_Config(closer=_CloserCfg(skip_on_missing_mid=True)))
    with cycle_scope(1):
        written = await stage.run_close_cycle()

    assert written == 0
    assert await _closures(db) == []
    # And the next poll could still see the trade as open.
    with cycle_scope(1):
        written2 = await stage.run_close_cycle()
    assert written2 == 0


async def test_missing_mid_falls_back_to_entry_when_skip_is_false(db):
    await _seed_market(db)
    decided = datetime.now(timezone.utc) - timedelta(hours=25)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=decided,
                      direction="YES", mid_price=0.4, size_usdc=100.0)
    clob = _FakeCLOB(mids={})

    stage = _make_stage(db, clob, cfg=_Config(closer=_CloserCfg(skip_on_missing_mid=False)))
    with cycle_scope(1):
        written = await stage.run_close_cycle()

    assert written == 1
    rows = await _closures(db)
    assert rows[0]["exit_mid_price"] == pytest.approx(0.4)
    assert rows[0]["realized_pnl_usdc"] == pytest.approx(0.0)


async def test_tracer_flag_carried_to_closure(db):
    await _seed_market(db)
    decided = datetime.now(timezone.utc) - timedelta(hours=25)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=decided,
                      is_tracer=1, tracer_id="tracer-abc")
    clob = _FakeCLOB(mids={"tY": 0.5})

    stage = _make_stage(db, clob)
    with cycle_scope(1):
        await stage.run_close_cycle()

    rows = await _closures(db)
    assert rows[0]["is_tracer"] == 1
    assert rows[0]["tracer_id"] == "tracer-abc"
