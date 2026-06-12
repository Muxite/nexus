"""CloserStage tests.

Strategy:
- Real in-memory DB (`DB.ephemeral()`); pre-populate `markets`, `trades`,
  and sometimes `trade_closures` with raw SQL.
- Fake CLOB client: an object exposing `async def get_top_of_book(token_id)`
  that the test controls per-token (cycle 27 — the closer is book-based).
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
from app.stages.closer import CloserStage, _compute_pnl, write_trade_closure


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
    """`get_top_of_book` returns whatever the test stuffs into `mids`,
    keyed by token: a `(bid, ask)` tuple, or a bare float that becomes a
    zero-spread book (`bid == ask == mid`) so mid-level expectations stay
    exact. A token missing from the dict resolves to `None` — the same
    shape the real CLOBClient produces on every recoverable failure mode
    (network error, empty side, crossed book)."""

    mids: dict = field(default_factory=dict)
    calls: list = field(default_factory=list)

    async def get_top_of_book(
        self, token_id: str
    ) -> Optional[tuple[float, float]]:
        self.calls.append(token_id)
        v = self.mids.get(token_id)
        if v is None:
            return None
        if isinstance(v, tuple):
            return (float(v[0]), float(v[1]))
        return (float(v), float(v))


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
class _TradeCfg:
    # Cycle 27: the fee path is `markets.fee_rate` else `default_fee_rate`
    # (the category table is gone). 0.05 here is deliberately non-zero so
    # tests prove the default actually applies; production default is 0.0.
    default_fee_rate: float = 0.05
    gas_cost_usdc: float = 0.0


@dataclass
class _Config:
    closer: _CloserCfg = field(default_factory=_CloserCfg)
    trade: _TradeCfg = field(default_factory=_TradeCfg)


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
    entry_best_bid: Optional[float] = None,
    entry_best_ask: Optional[float] = None,
    entry_fill_price: Optional[float] = None,
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
                 cycle_id, is_tracer, tracer_id, decided_at, target_mid,
                 entry_best_bid, entry_best_ask, entry_fill_price)
            VALUES (?, ?, ?, 1, ?, ?, 0.7, 0.8, 'BREAKING', 0.7, ?, 0.1, 0.1,
                    ?, 'mvp', '{}', 1, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                trade_id, signal_id, decision_id, market_id, direction,
                mid_price, size_usdc, is_tracer, tracer_id,
                decided_at.isoformat(), target_mid,
                entry_best_bid, entry_best_ask, entry_fill_price,
            ),
        )


def _make_stage(db: DB, clob: _FakeCLOB, cfg: Optional[_Config] = None) -> CloserStage:
    deps = StageDeps(db=db, bus=_FakeBus(), config=cfg or _Config(), clob=clob)
    return CloserStage(deps)


async def _closures(db: DB) -> list[dict]:
    cur = await db.conn.execute(
        "SELECT trade_id, close_reason, exit_mid_price, realized_pnl_usdc, "
        "cycle_id, is_tracer, tracer_id, "
        "entry_fee_usdc, exit_fee_usdc, "
        "exit_best_bid, exit_best_ask, exit_fill_price "
        "FROM trade_closures ORDER BY id"
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
    # Cycle 27: net of fees (min(p,1-p) model). gross=7.5; shares=25;
    # entry 25*0.05*min(0.4,0.6)=0.50, exit 25*0.05*min(0.7,0.3)=0.375;
    # net = 7.5 - 0.875 = 6.625. Zero-spread fake book → exit fill = mid.
    assert rows[0]["realized_pnl_usdc"] == pytest.approx(6.625, rel=1e-3)
    assert rows[0]["exit_fill_price"] == pytest.approx(0.7)


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


async def test_edge_decay_fires_when_no_mid_reaches_target_legacy_row(db):
    # Legacy NO row (entry_fill_price IS NULL): target_mid was stored in
    # YES space (p_hat + fee). mid_price / the closer's poll are the NO
    # token's own price (position space). The closer mirrors the legacy
    # target (1 - 0.40 = 0.60 in NO space) and fires when the NO token
    # RISES to it: entry 0.55, current 0.62 ≥ 0.60 → edge_decay.
    await _seed_market(db)
    decided = datetime.now(timezone.utc) - timedelta(minutes=10)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=decided,
                      direction="NO", mid_price=0.55, size_usdc=10.0,
                      target_mid=0.40)
    clob = _FakeCLOB(mids={"tN": 0.62})
    cfg = _Config(closer=_CloserCfg(take_profit_pct=0.50, stop_loss_pct=0.30))
    stage = _make_stage(db, clob, cfg=cfg)
    with cycle_scope(1):
        n = await stage.run_close_cycle()
    assert n == 1
    rows = await _closures(db)
    assert rows[0]["close_reason"] == "edge_decay"


async def test_edge_decay_no_position_space_target_on_fill_rows(db):
    # Cycle 27 NO row (entry_fill_price set): target_mid is already
    # position space — the NO token's own target. No mirroring: current
    # 0.62 ≥ target 0.60 → edge_decay.
    await _seed_market(db)
    decided = datetime.now(timezone.utc) - timedelta(minutes=10)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=decided,
                      direction="NO", mid_price=0.55, size_usdc=10.0,
                      target_mid=0.60,
                      entry_best_bid=0.54, entry_best_ask=0.56,
                      entry_fill_price=0.56)
    clob = _FakeCLOB(mids={"tN": 0.62})
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
    # Cycle 27: realized_pnl_usdc is NET of Polymarket taker fees
    # (min(p, 1-p) model, default fee_rate=0.05 from the test cfg).
    # YES @ entry=0.4, exit=0.6, size=$100:
    #   shares = 100 / 0.4 = 250
    #   entry_fee = 250 * 0.05 * min(0.4, 0.6) = 5.00
    #   exit_fee  = 250 * 0.05 * min(0.6, 0.4) = 5.00
    #   gross_pnl = 100 * (0.6 - 0.4) / 0.4 = 50.0
    #   net_pnl   = 50.0 - 10.0 = 40.0
    await _seed_market(db)
    decided = datetime.now(timezone.utc) - timedelta(hours=48)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=decided,
                      direction="YES", mid_price=0.4, size_usdc=100.0)
    clob = _FakeCLOB(mids={"tY": 0.6})

    stage = _make_stage(db, clob)
    with cycle_scope(1):
        await stage.run_close_cycle()

    rows = await _closures(db)
    assert rows[0]["realized_pnl_usdc"] == pytest.approx(40.0, rel=1e-4)
    assert rows[0]["entry_fee_usdc"] == pytest.approx(5.0, rel=1e-4)
    assert rows[0]["exit_fee_usdc"] == pytest.approx(5.0, rel=1e-4)
    assert _compute_pnl("YES", 0.4, 0.6, 100.0) == pytest.approx(50.0)


async def test_pnl_no_formula_position_space(db):
    # Cycle 27: all prices are the traded token's own (position space).
    # A NO trade whose NO token rises 0.4 → 0.6 WINS:
    #   shares = 100 / 0.4 = 250
    #   entry_fee = 250 * 0.05 * min(0.4, 0.6) = 5.0
    #   exit_fee  = 250 * 0.05 * min(0.6, 0.4) = 5.0
    #   gross_pnl = 100 * (0.6 - 0.4) / 0.4 = +50.0
    #   net_pnl   = 50.0 - 10.0 = 40.0
    # (The pre-cycle-27 NO branch returned -33.33 here — it assumed
    # YES-space mids the pipeline never stored; see closer.py spec.)
    await _seed_market(db)
    decided = datetime.now(timezone.utc) - timedelta(hours=48)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=decided,
                      direction="NO", mid_price=0.4, size_usdc=100.0)
    clob = _FakeCLOB(mids={"tN": 0.6})  # NO direction reads token_no

    stage = _make_stage(db, clob)
    with cycle_scope(1):
        await stage.run_close_cycle()

    rows = await _closures(db)
    assert rows[0]["realized_pnl_usdc"] == pytest.approx(40.0, rel=1e-3)
    assert rows[0]["entry_fee_usdc"] == pytest.approx(5.0, rel=1e-4)
    assert rows[0]["exit_fee_usdc"] == pytest.approx(5.0, rel=1e-4)
    assert _compute_pnl("NO", 0.4, 0.6, 100.0) == pytest.approx(50.0)


async def test_pnl_yes_with_zero_per_market_fee_rate(db):
    """A market with a fetched fee_rate of 0 (Polymarket's modal taker
    fee) pays nothing: net PnL == gross PnL. fee_rate=0.0 is a VALID
    value, not a missing one — it must not fall back to the default."""
    async with db.batch() as cur:
        await cur.execute(
            """
            INSERT INTO markets
                (condition_id, slug, question, description, category, end_date,
                 volume_usdc, token_yes, token_no, embedding, embedder_id,
                 embedding_dim, embedding_updated_at, active, closed_at,
                 first_seen_at, updated_at, fee_rate)
            VALUES ('m1', 'slug', 'Q?', 'desc', 'geopolitics', '2026-12-31',
                    1000.0, 'tY', 'tN', NULL, NULL, NULL, NULL, 1, NULL,
                    '2026-05-23T00:00:00+00:00', '2026-05-23T00:00:00+00:00',
                    0.0)
            """,
        )
    decided = datetime.now(timezone.utc) - timedelta(hours=48)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=decided,
                      direction="YES", mid_price=0.4, size_usdc=100.0)
    clob = _FakeCLOB(mids={"tY": 0.6})

    stage = _make_stage(db, clob)
    with cycle_scope(1):
        await stage.run_close_cycle()

    rows = await _closures(db)
    # gross 50, fee_rate=0 → net 50 too
    assert rows[0]["realized_pnl_usdc"] == pytest.approx(50.0)
    assert rows[0]["entry_fee_usdc"] == pytest.approx(0.0)
    assert rows[0]["exit_fee_usdc"] == pytest.approx(0.0)


async def test_pnl_null_fee_rate_falls_back_to_default(db):
    """Cycle 27: a NULL `markets.fee_rate` (fetch never succeeded) falls
    back to `cfg.trade.default_fee_rate` — the category column is
    ignored entirely (the category→rate table is gone). The default
    `_seed_market` row has category='cat' and no fee_rate."""
    await _seed_market(db)
    decided = datetime.now(timezone.utc) - timedelta(hours=48)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=decided,
                      direction="YES", mid_price=0.4, size_usdc=100.0)
    clob = _FakeCLOB(mids={"tY": 0.6})

    stage = _make_stage(db, clob)
    with cycle_scope(1):
        await stage.run_close_cycle()

    rows = await _closures(db)
    # shares=250; per-leg fee = 250 * 0.05 * min(p, 1-p) = 5.0 both legs
    # net = 50 - 10 = 40
    assert rows[0]["realized_pnl_usdc"] == pytest.approx(40.0, rel=1e-3)


async def test_per_market_fee_rate_used_when_present(db):
    """Cycle 10/27: when `markets.fee_rate` is populated (per-market
    fetch from `clob.get_market_fee_rate(conditionId)`), the closure
    uses that value instead of `default_fee_rate` (0.05 in the test
    cfg)."""
    async with db.batch() as cur:
        await cur.execute(
            """
            INSERT INTO markets
                (condition_id, slug, question, description, category, end_date,
                 volume_usdc, token_yes, token_no, embedding, embedder_id,
                 embedding_dim, embedding_updated_at, active, closed_at,
                 first_seen_at, updated_at, fee_rate)
            VALUES ('m1', 'slug', 'Q?', 'desc', 'crypto', '2026-12-31',
                    1000.0, 'tY', 'tN', NULL, NULL, NULL, NULL, 1, NULL,
                    '2026-05-23T00:00:00+00:00', '2026-05-23T00:00:00+00:00',
                    0.02)
            """,
        )
    decided = datetime.now(timezone.utc) - timedelta(hours=48)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=decided,
                      direction="YES", mid_price=0.4, size_usdc=100.0)
    clob = _FakeCLOB(mids={"tY": 0.6})

    stage = _make_stage(db, clob)
    with cycle_scope(1):
        await stage.run_close_cycle()

    rows = await _closures(db)
    # shares=100/0.4=250; per-leg fee at fee_rate=0.02:
    #   entry: 250 * 0.02 * min(0.4, 0.6) = 2.00
    #   exit:  250 * 0.02 * min(0.6, 0.4) = 2.00
    # gross = 100 * (0.6 - 0.4) / 0.4 = 50.0
    # net   = 50.0 - 4.00 = 46.00
    # (If the 0.05 default had fired instead, net would be 40.0 —
    #  way outside the 1e-3 tolerance.)
    assert rows[0]["entry_fee_usdc"] == pytest.approx(2.00, rel=1e-3)
    assert rows[0]["exit_fee_usdc"] == pytest.approx(2.00, rel=1e-3)
    assert rows[0]["realized_pnl_usdc"] == pytest.approx(46.00, rel=1e-3)


async def test_market_resolved_charges_no_exit_fee(db):
    """Polymarket charges no fee at resolution — winning shares redeem at
    $1 / losing at $0. write_trade_closure must zero the exit leg when
    close_reason='market_resolved' but still charge the entry leg.
    Mirrors the MarketsStage auto-close path (markets.py:_close_orphaned_trades_for).
    """
    await _seed_market(db)
    decided = datetime.now(timezone.utc) - timedelta(hours=48)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=decided,
                      direction="YES", mid_price=0.4, size_usdc=100.0)

    trade = {
        "id": 1,
        "direction": "YES",
        "mid_price": 0.4,
        "size_usdc": 100.0,
        "is_tracer": 0,
        "tracer_id": None,
    }
    cfg = _Config()
    with cycle_scope(1):
        ok = await write_trade_closure(
            db,
            trade=trade,
            exit_mid=1.0,  # YES resolved true → shares redeem at $1
            close_reason="market_resolved",
            trade_cfg=cfg.trade,
        )
    assert ok

    rows = await _closures(db)
    # shares = 100/0.4 = 250; entry_fee = 250 * 0.05 * min(0.4, 0.6) = 5.0
    # exit_fee = 0.0 (resolution exception)
    # gross  = 100 * (1.0 - 0.4) / 0.4 = 150.0
    # net    = 150.0 - 5.0 = 145.0
    assert rows[0]["close_reason"] == "market_resolved"
    assert rows[0]["entry_fee_usdc"] == pytest.approx(5.0, rel=1e-4)
    assert rows[0]["exit_fee_usdc"] == pytest.approx(0.0)
    assert rows[0]["realized_pnl_usdc"] == pytest.approx(145.0, rel=1e-4)


async def test_non_market_resolved_still_charges_exit_fee(db):
    """Sanity check: every other close_reason still charges the exit-leg
    taker fee (the resolution exception is narrow)."""
    await _seed_market(db)
    decided = datetime.now(timezone.utc) - timedelta(hours=48)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=decided,
                      direction="YES", mid_price=0.4, size_usdc=100.0)

    trade = {
        "id": 1,
        "direction": "YES",
        "mid_price": 0.4,
        "size_usdc": 100.0,
        "is_tracer": 0,
        "tracer_id": None,
    }
    cfg = _Config()
    with cycle_scope(1):
        await write_trade_closure(
            db,
            trade=trade,
            exit_mid=0.6,
            close_reason="timeout",
            trade_cfg=cfg.trade,
        )

    rows = await _closures(db)
    # entry_fee = exit_fee = 250 * 0.05 * min(p, 1-p) = 5.0 both legs
    assert rows[0]["entry_fee_usdc"] == pytest.approx(5.0, rel=1e-4)
    assert rows[0]["exit_fee_usdc"] == pytest.approx(5.0, rel=1e-4)


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
    clob = _FakeCLOB(mids={})  # token has no book (empty/missing)

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
    # Cycle 27: exit==entry price → gross=0, but fees still paid both
    # legs. shares=250; per-leg fee=250*0.05*min(0.4,0.6)=5.0; net=-10.0.
    assert rows[0]["realized_pnl_usdc"] == pytest.approx(-10.0, rel=1e-3)
    # No book → no exit fill recorded.
    assert rows[0]["exit_fill_price"] is None


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


async def test_spread_aware_fills_round_trip(db):
    """Cycle 27 tracer: a fill-priced trade closes at the bid, persists
    the exit book, and the residual between mid-to-mid drift and gross
    PnL is exactly the spread paid on both legs.

    Entry book (0.38, 0.42): mid 0.40, fill at ask 0.42.
    Exit book  (0.58, 0.62): mid 0.60, fill at bid 0.58.
      shares     = 100 / 0.42 = 238.095
      gross      = 238.095 * (0.58 - 0.42) = 38.095
      mid drift  = (100 / 0.40) * (0.60 - 0.40) = 50.0   (what the old
                   wallet would have printed)
      residual   = 50.0 - 38.095 = 11.905 — the spread, now visible.
    Fees at default 0.05: entry 238.095*0.05*min(0.42,0.58)=5.0,
    exit 238.095*0.05*min(0.58,0.42)=5.0; net = 38.095 - 10 = 28.095.
    """
    await _seed_market(db)
    decided = datetime.now(timezone.utc) - timedelta(hours=48)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=decided,
                      direction="YES", mid_price=0.40, size_usdc=100.0,
                      entry_best_bid=0.38, entry_best_ask=0.42,
                      entry_fill_price=0.42)
    clob = _FakeCLOB(mids={"tY": (0.58, 0.62)})

    stage = _make_stage(db, clob)
    with cycle_scope(1):
        n = await stage.run_close_cycle()

    assert n == 1
    rows = await _closures(db)
    assert rows[0]["close_reason"] == "timeout"
    assert rows[0]["exit_best_bid"] == pytest.approx(0.58)
    assert rows[0]["exit_best_ask"] == pytest.approx(0.62)
    assert rows[0]["exit_fill_price"] == pytest.approx(0.58)
    assert rows[0]["exit_mid_price"] == pytest.approx(0.60)
    shares = 100.0 / 0.42
    gross = shares * (0.58 - 0.42)
    net = gross - 2 * (shares * 0.05 * 0.42)
    assert rows[0]["realized_pnl_usdc"] == pytest.approx(net, rel=1e-6)
    # The spread paid is now a visible residual: mid drift > gross.
    mid_drift = (100.0 / 0.40) * (0.60 - 0.40)
    assert mid_drift - gross == pytest.approx(11.9047619, rel=1e-6)


async def test_resolution_redeems_at_position_payout_for_fill_rows(db):
    """A cycle-27 NO trade that wins at resolution redeems the NO token
    at $1 — entry priced at the fill, no exit fee, no exit book."""
    await _seed_market(db)
    decided = datetime.now(timezone.utc) - timedelta(hours=1)
    await _seed_trade(db, trade_id=1, signal_id=1, decided_at=decided,
                      direction="NO", mid_price=0.55, size_usdc=100.0,
                      entry_best_bid=0.54, entry_best_ask=0.56,
                      entry_fill_price=0.56)
    trade = {
        "id": 1,
        "direction": "NO",
        "mid_price": 0.55,
        "entry_fill_price": 0.56,
        "size_usdc": 100.0,
        "is_tracer": 0,
        "tracer_id": None,
    }
    cfg = _Config()
    with cycle_scope(1):
        ok = await write_trade_closure(
            db,
            trade=trade,
            exit_mid=1.0,  # NO payout (position space) — market resolved NO
            close_reason="market_resolved",
            trade_cfg=cfg.trade,
        )
    assert ok
    rows = await _closures(db)
    shares = 100.0 / 0.56
    gross = shares * (1.0 - 0.56)
    entry_fee = shares * 0.05 * min(0.56, 1 - 0.56)
    assert rows[0]["realized_pnl_usdc"] == pytest.approx(gross - entry_fee, rel=1e-6)
    assert rows[0]["exit_fee_usdc"] == pytest.approx(0.0)
    assert rows[0]["exit_fill_price"] is None
