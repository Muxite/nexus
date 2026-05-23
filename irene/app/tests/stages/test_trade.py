"""TradeStage tests — gate cascade branches, idempotency, tracer threading.

Strategy:
- Real in-memory DB; pre-populate articles + markets so prompt composition
  works.
- Fake AIClient duck-typed against `ai.direction()`; returns a configurable
  `DirectionResult` (imported from app.ai).
- FakeBus collects publishes.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Optional

import pytest

# DDL registration side effects -------------------------------------------
import app.ai  # noqa: F401
import app.main  # noqa: F401 — cycles DDL
import app.stages.dedupe  # noqa: F401
import app.stages.ingest  # noqa: F401
import app.stages.markets  # noqa: F401
import app.stages.match  # noqa: F401
import app.stages.trade  # noqa: F401
from app.ai import DirectionResult
from app.cycle import cycle_scope
from app.db import DB
from app.deps import StageDeps
from app.stages.trade import TradeStage
from app.topics import TOPIC_TRADE_DECIDED


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeBus:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict]] = []

    async def publish(self, topic: str, payload: dict) -> None:
        self.published.append((topic, payload))

    def subscribe(self, topic: str, handler) -> None:  # noqa: ARG002
        pass

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


@dataclass
class _FakeAI:
    """Duck-typed AIClient stub. `next_result` is what `direction()` returns
    on the next call. `calls` records the inputs so tests can assert no-call
    on early-rejecting gates."""

    next_result: DirectionResult = field(
        default_factory=lambda: DirectionResult(
            direction="YES", confidence=0.7, prediction_id=42,
            cached=False, raw_response='{"direction":"YES","confidence":0.7}',
        )
    )
    calls: list[dict] = field(default_factory=list)

    async def direction(
        self,
        *,
        signal_id: int,
        prompt: str,
        is_tracer: bool = False,
        tracer_id: Optional[str] = None,
    ) -> DirectionResult:
        self.calls.append(
            {"signal_id": signal_id, "prompt": prompt,
             "is_tracer": is_tracer, "tracer_id": tracer_id}
        )
        return self.next_result


# Trade.py reads cfg.trade.*; a minimal duck-typed config is enough.
@dataclass
class _TradeCfg:
    balance_usdc: float = 1000.0
    min_similarity: float = 0.35
    min_confidence: float = 0.55
    min_edge: float = 0.04
    fee_buffer: float = 0.02
    kelly_fraction: float = 0.25
    max_position_fraction: float = 0.20
    min_position_usdc: float = 1.0
    default_mid_price: float = 0.5
    accept_freshness: list = field(default_factory=lambda: ["BREAKING", "EVOLVING"])


@dataclass
class _Config:
    trade: _TradeCfg = field(default_factory=_TradeCfg)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def db_seeded():
    """In-memory DB with one signal-shaped article + market + signal already
    written. Tests address them by id=1 / condition_id='m1' / signal_id=1."""
    db = DB.ephemeral()
    await db.connect()
    async with db.batch() as cur:
        await cur.execute(
            """
            INSERT INTO articles
                (id, source, source_detail, url, title, summary, published_at,
                 ingested_at, cycle_id, is_tracer, tracer_id)
            VALUES (1, 'rss','test','https://x/1','Breaking news headline',
                    'Some context summary.', '2026-05-23T00:00:00+00:00',
                    '2026-05-23T00:00:00+00:00', 1, 0, NULL)
            """,
        )
        await cur.execute(
            """
            INSERT INTO markets
                (condition_id, slug, question, description, category, end_date,
                 volume_usdc, token_yes, token_no, embedding, embedder_id,
                 embedding_dim, embedding_updated_at, active, closed_at,
                 first_seen_at, updated_at)
            VALUES ('m1', 'slug-1', 'Will X happen by end of year?',
                    'Resolution details.', 'politics', '2026-12-31', 1000.0,
                    'ty', 'tn', NULL, NULL, NULL, NULL, 1, NULL,
                    '2026-05-23T00:00:00+00:00', '2026-05-23T00:00:00+00:00')
            """,
        )
        await cur.execute(
            """
            INSERT INTO signals
                (id, article_id, market_id, similarity, cluster_id, freshness,
                 embedder_id, cycle_id, is_tracer, tracer_id, created_at)
            VALUES (1, 1, 'm1', 0.80, NULL, 'BREAKING', 'fake@4', 1, 0, NULL,
                    '2026-05-23T00:00:00+00:00')
            """,
        )
    try:
        yield db
    finally:
        await db.close()


def _make_stage(db, bus, ai, cfg: Optional[_Config] = None, *, clob=None) -> TradeStage:
    deps = StageDeps(
        db=db,
        bus=bus,
        config=cfg or _Config(),
        ai=ai,
        clob=clob,
    )
    return TradeStage(deps)


class _FakeCLOB:
    """Async CLOB stand-in. Returns the price keyed by token_id, else None."""

    def __init__(self, prices: dict[str, float]) -> None:
        self.prices = dict(prices)
        self.calls: list[str] = []

    async def get_mid(self, token_id: str):
        self.calls.append(token_id)
        return self.prices.get(token_id)


def _payload(
    *,
    signal_id: int = 1,
    article_id: int = 1,
    market_id: str = "m1",
    similarity: float = 0.80,
    freshness: str = "BREAKING",
    is_tracer: bool = False,
    tracer_id: Optional[str] = None,
) -> dict:
    return {
        "signal_id": signal_id,
        "article_id": article_id,
        "market_id": market_id,
        "similarity": similarity,
        "freshness": freshness,
        "is_tracer": is_tracer,
        "tracer_id": tracer_id,
    }


async def _single_decision(db) -> dict:
    cur = await db.conn.execute(
        "SELECT signal_id, outcome, reason, gate_history, prediction_id FROM decisions"
    )
    rows = await cur.fetchall()
    await cur.close()
    assert len(rows) == 1, f"expected exactly 1 decision row, got {len(rows)}"
    r = rows[0]
    return {
        "signal_id": r["signal_id"],
        "outcome": r["outcome"],
        "reason": r["reason"],
        "gate_history": json.loads(r["gate_history"]),
        "prediction_id": r["prediction_id"],
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_owned_tables_exist_after_connect():
    db = DB.ephemeral()
    await db.connect()
    try:
        for name in ("decisions", "trades", "trade_features"):
            cur = await db.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
            )
            row = await cur.fetchone()
            await cur.close()
            assert row is not None, f"table {name} not created"
    finally:
        await db.close()


async def test_init_requires_ai(db_seeded):
    bus = _FakeBus()
    with pytest.raises(RuntimeError, match="ai"):
        TradeStage(StageDeps(db=db_seeded, bus=bus, config=_Config(), ai=None))


async def test_happy_path_writes_trade_decision_and_features(db_seeded):
    bus = _FakeBus()
    ai = _FakeAI()  # default YES@0.7, prediction_id=42
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload())

    d = await _single_decision(db_seeded)
    assert d["outcome"] == "execute"
    assert d["reason"] == "ok"
    assert d["prediction_id"] == 42

    cur = await db_seeded.conn.execute(
        "SELECT signal_id, decision_id, direction, confidence, p_hat, mid_price, "
        "edge, size_usdc, decision_path, features_snapshot FROM trades"
    )
    rows = await cur.fetchall()
    await cur.close()
    assert len(rows) == 1
    t = rows[0]
    assert t["signal_id"] == 1
    assert t["direction"] == "YES"
    assert t["decision_path"] == "mvp"
    snap = json.loads(t["features_snapshot"])
    assert snap["direction"] == "YES" and snap["confidence"] == 0.7
    assert "gate_history" in snap and len(snap["gate_history"]) == 5

    cur = await db_seeded.conn.execute(
        "SELECT feature_name FROM trade_features ORDER BY feature_name"
    )
    names = sorted(r["feature_name"] for r in await cur.fetchall())
    await cur.close()
    assert set(names) >= {
        "similarity", "freshness", "confidence", "p_hat", "mid",
        "edge", "kelly_fraction", "size_usdc",
    }

    assert len(bus.published) == 1
    topic, p = bus.published[0]
    assert topic == TOPIC_TRADE_DECIDED
    assert p["outcome"] == "execute"
    assert p["reason"] == "ok"
    assert p["trade_id"] is not None


async def test_similarity_floor_rejects_without_ai_call(db_seeded):
    bus = _FakeBus()
    ai = _FakeAI()
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload(similarity=0.10))

    d = await _single_decision(db_seeded)
    assert d["outcome"] == "reject"
    assert d["reason"] == "similarity_floor"
    assert d["prediction_id"] is None
    assert ai.calls == []
    assert bus.published[0][1]["outcome"] == "reject"
    assert bus.published[0][1]["reason"] == "similarity_floor"


async def test_freshness_stale_rejects_without_ai_call(db_seeded):
    bus = _FakeBus()
    ai = _FakeAI()
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload(freshness="STALE_RETURN"))

    d = await _single_decision(db_seeded)
    assert d["outcome"] == "reject"
    assert d["reason"] == "freshness_stale"
    assert ai.calls == []
    assert bus.published[0][1]["reason"] == "freshness_stale"


async def test_direction_neutral_rejects(db_seeded):
    bus = _FakeBus()
    ai = _FakeAI(next_result=DirectionResult(
        direction="NEUTRAL", confidence=0.0, prediction_id=7,
        cached=False, raw_response="{}",
    ))
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload())

    d = await _single_decision(db_seeded)
    assert d["outcome"] == "reject"
    assert d["reason"] == "direction_neutral"
    assert d["prediction_id"] == 7
    assert len(ai.calls) == 1
    assert bus.published[0][1]["reason"] == "direction_neutral"


async def test_direction_lowconf_rejects(db_seeded):
    bus = _FakeBus()
    ai = _FakeAI(next_result=DirectionResult(
        direction="YES", confidence=0.20, prediction_id=8,
        cached=False, raw_response="{}",
    ))
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload())

    d = await _single_decision(db_seeded)
    assert d["outcome"] == "reject"
    assert d["reason"] == "direction_lowconf"
    assert d["prediction_id"] == 8
    assert bus.published[0][1]["reason"] == "direction_lowconf"


async def test_edge_below_min_rejects(db_seeded):
    # YES@0.55, mid=0.5, fee_buffer=0.02 -> edge = 0.55-0.5-0.02 = 0.03 < 0.04
    bus = _FakeBus()
    ai = _FakeAI(next_result=DirectionResult(
        direction="YES", confidence=0.55, prediction_id=9,
        cached=False, raw_response="{}",
    ))
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload())

    d = await _single_decision(db_seeded)
    assert d["outcome"] == "reject"
    assert d["reason"] == "edge_below_min"
    assert d["prediction_id"] == 9
    assert bus.published[0][1]["reason"] == "edge_below_min"


async def test_size_below_min_rejects(db_seeded):
    # Shrink balance so kelly*balance < min_position_usdc.
    cfg = _Config(trade=_TradeCfg(balance_usdc=0.5, min_position_usdc=1.0))
    bus = _FakeBus()
    ai = _FakeAI(next_result=DirectionResult(
        direction="YES", confidence=0.7, prediction_id=10,
        cached=False, raw_response="{}",
    ))
    stage = _make_stage(db_seeded, bus, ai, cfg=cfg)
    with cycle_scope(1):
        await stage.handle(_payload())

    d = await _single_decision(db_seeded)
    assert d["outcome"] == "reject"
    assert d["reason"] == "size_below_min"
    assert bus.published[0][1]["reason"] == "size_below_min"


async def test_idempotent_on_replay(db_seeded):
    bus = _FakeBus()
    ai = _FakeAI()
    stage = _make_stage(db_seeded, bus, ai)
    payload = _payload()
    with cycle_scope(1):
        await stage.handle(payload)
        await stage.handle(payload)

    cur = await db_seeded.conn.execute("SELECT COUNT(*) FROM decisions WHERE signal_id=1")
    assert (await cur.fetchone())[0] == 1
    await cur.close()
    cur = await db_seeded.conn.execute("SELECT COUNT(*) FROM trades WHERE signal_id=1")
    assert (await cur.fetchone())[0] == 1
    await cur.close()
    assert len(bus.published) == 1


async def test_tracer_threading_into_owned_tables(db_seeded):
    bus = _FakeBus()
    ai = _FakeAI()
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload(is_tracer=True, tracer_id="t-1"))

    cur = await db_seeded.conn.execute(
        "SELECT is_tracer, tracer_id FROM decisions WHERE signal_id=1"
    )
    row = await cur.fetchone()
    await cur.close()
    assert row["is_tracer"] == 1 and row["tracer_id"] == "t-1"

    cur = await db_seeded.conn.execute(
        "SELECT id, is_tracer, tracer_id FROM trades WHERE signal_id=1"
    )
    trow = await cur.fetchone()
    await cur.close()
    assert trow["is_tracer"] == 1 and trow["tracer_id"] == "t-1"

    # trade_features rows don't carry tracer cols directly per schema —
    # confirm one row landed for this trade as a sanity check that the
    # fan-out happened on the tracer path.
    cur = await db_seeded.conn.execute(
        "SELECT COUNT(*) FROM trade_features WHERE trade_id=?", (trow["id"],)
    )
    assert (await cur.fetchone())[0] >= 8
    await cur.close()

    pub = bus.published[0][1]
    assert pub["is_tracer"] is True
    assert pub["tracer_id"] == "t-1"


# ---------------------------------------------------------------------------
# CLOB integration — entry mid resolution
# ---------------------------------------------------------------------------


async def test_clob_supplies_real_mid_when_set(db_seeded):
    """When deps.clob is wired AND returns a usable mid, the trade row's
    `mid_price` reflects the CLOB number rather than the default stub."""
    bus = _FakeBus()
    ai = _FakeAI()  # default YES@0.7
    clob = _FakeCLOB(prices={"ty": 0.42, "tn": 0.58})
    stage = _make_stage(db_seeded, bus, ai, clob=clob)
    with cycle_scope(1):
        await stage.handle(_payload())
    cur = await db_seeded.conn.execute(
        "SELECT mid_price FROM trades WHERE signal_id=1"
    )
    row = await cur.fetchone()
    await cur.close()
    assert row is not None
    assert row["mid_price"] == pytest.approx(0.42)
    assert clob.calls == ["ty"], "YES direction should query the token_yes side"


async def test_clob_missing_token_falls_back_to_default(db_seeded):
    bus = _FakeBus()
    ai = _FakeAI()
    # Empty CLOB → get_mid returns None → fall back to default_mid_price (0.5).
    clob = _FakeCLOB(prices={})
    stage = _make_stage(db_seeded, bus, ai, clob=clob)
    with cycle_scope(1):
        await stage.handle(_payload())
    cur = await db_seeded.conn.execute(
        "SELECT mid_price FROM trades WHERE signal_id=1"
    )
    row = await cur.fetchone()
    await cur.close()
    # Default mid 0.5 + YES@0.7 → edge 0.2 - 0.02 = 0.18 → executes
    # (under default config which has min_edge=0.04).
    assert row is not None
    assert row["mid_price"] == pytest.approx(0.5)
