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
    taker_fee_buffer: float = 0.01
    kelly_fraction: float = 0.25
    max_position_fraction: float = 0.20
    min_position_usdc: float = 1.0
    default_mid_price: float = 0.5
    min_mid: float = 0.02
    max_mid: float = 0.98
    wide_spread_threshold: float = 0.10
    # Cycle 7: market-news weight gate knobs. Match TradeSection defaults
    # in app/config.py. Tests override only what's relevant per case.
    min_market_volume_usdc: float = 10_000.0
    far_future_days: int = 180
    min_days_to_expiry: int = 1
    extreme_mid_low: float = 0.15
    extreme_mid_high: float = 0.85
    require_breaking_for_extreme_mid: bool = True
    require_breaking_for_far_future: bool = True
    reject_singleton_on_extreme_mid: bool = True
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
                    'Resolution details.', 'politics', '2026-12-31', 50000.0,
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
    """Async CLOB stand-in. Returns the price keyed by token_id, else None.

    Optionally also supports `get_top_of_book` for the wide-spread gate:
    pass `books={token_id: (best_bid, best_ask)}`. A token missing from
    `books` resolves to `None` (no book → gate is skipped).
    """

    def __init__(
        self,
        prices: dict[str, float],
        books: Optional[dict[str, tuple[float, float]]] = None,
    ) -> None:
        self.prices = dict(prices)
        self.books = dict(books or {})
        self.calls: list[str] = []
        self.book_calls: list[str] = []

    async def get_mid(self, token_id: str):
        self.calls.append(token_id)
        return self.prices.get(token_id)

    async def get_top_of_book(self, token_id: str):
        self.book_calls.append(token_id)
        return self.books.get(token_id)


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
    # 7 gates: similarity_floor, freshness, entity_overlap, direction,
    # edge, size, cash
    assert "gate_history" in snap and len(snap["gate_history"]) == 7

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


async def test_target_mid_computed_correctly_for_yes_and_no(db_seeded):
    """Regression for the cycle-3 sign bug. target_mid is always in YES-mid
    terms (Polymarket's primary price). p_hat is the model's implied
    P(YES). fee_buffer is the cushion to lock profit before fees.

      - YES bet → sell when YES rises to p_hat - fee_buffer
      - NO  bet → sell when YES falls to p_hat + fee_buffer

    The original v6 code had `target = (1 - p_hat) + fee_buffer` for NO,
    which double-flipped the probability and made edge_decay fire on
    every NO trade's first poll. This test locks the correct formula.
    """
    # Default _Config has fee_buffer=0.02, min_confidence=0.55.
    # _FakeAI default = YES @ confidence 0.7 → p_hat=0.7 → target=0.68.
    bus = _FakeBus()
    ai = _FakeAI()  # YES @ 0.7
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload(signal_id=1))

    cur = await db_seeded.conn.execute(
        "SELECT direction, p_hat, target_mid FROM trades WHERE signal_id=1"
    )
    row = await cur.fetchone()
    await cur.close()
    assert row is not None
    assert row["direction"] == "YES"
    assert row["p_hat"] == pytest.approx(0.7)
    assert row["target_mid"] == pytest.approx(0.68)   # 0.70 - 0.02 fee buffer

    # Now fire a NO trade on a different (article, market). p_hat for NO = 1 - conf = 0.3.
    # Correct target = p_hat + fee_buffer = 0.32.
    # WRONG target would be (1 - p_hat) + fee = 0.72.
    async with db_seeded.batch() as cur:
        await cur.execute(
            """
            INSERT INTO markets
                (condition_id, slug, question, description, category, end_date,
                 volume_usdc, token_yes, token_no, embedding, embedder_id,
                 embedding_dim, embedding_updated_at, active, closed_at,
                 first_seen_at, updated_at)
            VALUES ('m2', 'slug-2', 'Will Y happen?', 'd2', 'p', '2026-12-31',
                    50000.0, 'tY2', 'tN2', NULL, NULL, NULL, NULL, 1, NULL,
                    '2026-05-23T00:00:00+00:00', '2026-05-23T00:00:00+00:00')
            """
        )
        await cur.execute(
            """
            INSERT INTO signals
                (id, article_id, market_id, similarity, cluster_id, freshness,
                 embedder_id, cycle_id, is_tracer, tracer_id, created_at)
            VALUES (2, 1, 'm2', 0.80, NULL, 'BREAKING', 'fake@4', 1, 0, NULL,
                    '2026-05-23T00:00:00+00:00')
            """
        )
    bus2 = _FakeBus()
    ai2 = _FakeAI(next_result=DirectionResult(
        direction="NO", confidence=0.7, prediction_id=99,
        cached=False, raw_response="N",
    ))
    stage2 = _make_stage(db_seeded, bus2, ai2)
    with cycle_scope(1):
        await stage2.handle(_payload(signal_id=2, market_id="m2"))
    cur = await db_seeded.conn.execute(
        "SELECT direction, p_hat, target_mid FROM trades WHERE signal_id=2"
    )
    row = await cur.fetchone()
    await cur.close()
    assert row is not None
    assert row["direction"] == "NO"
    assert row["p_hat"] == pytest.approx(0.3)         # 1 - 0.7
    assert row["target_mid"] == pytest.approx(0.32)   # 0.30 + 0.02 fee buffer
    # If the bug were back, target_mid would be 0.72 (= 0.70 + 0.02).
    assert row["target_mid"] != pytest.approx(0.72)


async def _seed_entities_for_article(db, article_id: int, entity_names: list[str]) -> None:
    """Helper: write entities + facts + entity_resolutions so the
    stage-0 entity_overlap gate can read them."""
    async with db.batch() as cur:
        await cur.execute(
            """
            INSERT INTO extraction_runs
                (id, article_id, model, prompt_version, ontology_version, status,
                 raw_response, latency_ms, cycle_id, is_tracer, tracer_id, created_at)
            VALUES (1, ?, 'fake', 'v1', 'v1', 'ok',
                    '{}', 10, 1, 0, NULL, '2026-05-23T00:00:00+00:00')
            """,
            (article_id,),
        )
        for i, name in enumerate(entity_names, start=1):
            await cur.execute(
                """
                INSERT INTO entities
                    (id, canonical_name, entity_type, first_seen_at, last_seen_at)
                VALUES (?, ?, 'PERSON', '2026-05-23T00:00:00+00:00',
                        '2026-05-23T00:00:00+00:00')
                """,
                (i, name),
            )
            await cur.execute(
                """
                INSERT INTO facts
                    (id, article_id, extraction_run_id, subj_entity_id, relation,
                     confidence, observed_at, evidence_span, cycle_id,
                     is_tracer, tracer_id)
                VALUES (?, ?, 1, ?, 'decides', 1.0,
                        '2026-05-23T00:00:00+00:00', 'x', 1, 0, NULL)
                """,
                (i, article_id, i),
            )
            await cur.execute(
                """
                INSERT INTO entity_resolutions
                    (entity_id, resolved_to_id, resolution_kind, confidence,
                     resolver_version, created_at)
                VALUES (?, ?, 'self', 1.0, 'v1', '2026-05-23T00:00:00+00:00')
                """,
                (i, i),
            )


async def test_entity_overlap_gate_rejects_when_no_match(db_seeded):
    # Article has entity "Volodymyr Zelenskyy" but market.question is "Will X happen by end of year?" — no overlap.
    await _seed_entities_for_article(db_seeded, 1, ["Volodymyr Zelenskyy"])
    bus = _FakeBus()
    ai = _FakeAI()
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload())
    d = await _single_decision(db_seeded)
    assert d["outcome"] == "reject"
    assert d["reason"] == "entity_mismatch"
    assert ai.calls == []   # LLM never called — stage-0 gate saved a call


async def test_entity_overlap_gate_permits_on_match(db_seeded):
    # Article entity "Volodymyr Zelenskyy" appears in the market question.
    async with db_seeded.batch() as cur:
        await cur.execute(
            "UPDATE markets SET question='Will Volodymyr Zelenskyy resign by 2026?' WHERE condition_id='m1'"
        )
    await _seed_entities_for_article(db_seeded, 1, ["Volodymyr Zelenskyy"])
    bus = _FakeBus()
    ai = _FakeAI()  # default YES → execute
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload())
    d = await _single_decision(db_seeded)
    assert d["outcome"] == "execute"
    assert ai.calls != []   # LLM was called


async def test_cash_clips_when_cash_below_requested_size(db_seeded):
    # Pre-seed a $995 open trade. Only $5 cash left.
    # New trade requests ~$99 → cycle-3 cash gate CLIPS to $5, still
    # executes because $5 > min_position_usdc ($1).
    async with db_seeded.batch() as cur:
        await cur.execute(
            """
            INSERT INTO predictions
                (id, signal_id, llm_call_id, model, prompt_version, direction,
                 confidence, cached, raw_response, label, cycle_id, is_tracer,
                 tracer_id, created_at)
            VALUES (99, NULL, NULL, 'fake', 'v1', 'YES', 0.9, 0, 'Y', 'Y', 1, 0,
                    NULL, '2026-05-23T00:00:00+00:00')
            """,
        )
        await cur.execute(
            """
            INSERT INTO decisions
                (id, signal_id, outcome, reason, gate_history, prediction_id,
                 cycle_id, is_tracer, tracer_id, created_at)
            VALUES (99, NULL, 'execute', 'ok', '[]', 99, 1, 0, NULL,
                    '2026-05-23T00:00:00+00:00')
            """,
        )
        await cur.execute(
            """
            INSERT INTO trades
                (id, signal_id, decision_id, prediction_id, market_id, direction,
                 confidence, similarity, freshness, p_hat, mid_price, edge,
                 kelly_fraction, size_usdc, decision_path, features_snapshot,
                 cycle_id, is_tracer, tracer_id, decided_at)
            VALUES (99, NULL, 99, 99, 'm1', 'YES', 0.9, 0.8, 'BREAKING', 0.9,
                    0.4, 0.1, 0.1, 995.0, 'mvp', '{}', 1, 0, NULL,
                    '2026-05-23T00:00:00+00:00')
            """,
        )
    bus = _FakeBus()
    ai = _FakeAI()  # YES@0.7 → would size ~$99 by default config
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload())
    # The pre-seeded decision (id=99) is already in the table; look up
    # the new one by signal_id=1 (the seeded fresh signal).
    cur = await db_seeded.conn.execute(
        "SELECT outcome, reason FROM decisions WHERE signal_id=1"
    )
    row = await cur.fetchone()
    await cur.close()
    assert row is not None
    assert row["outcome"] == "execute"   # clipped, not rejected
    assert row["reason"] == "ok"
    # Verify the executed trade is sized at the clip (~$5), not the requested $99.
    cur = await db_seeded.conn.execute(
        "SELECT size_usdc FROM trades WHERE signal_id=1"
    )
    trow = await cur.fetchone()
    await cur.close()
    assert 4.0 <= trow["size_usdc"] <= 5.5  # clip is $5 ± float jitter


async def test_insufficient_cash_rejects_when_overdrawn(db_seeded):
    # cash_available <= 0 still rejects (never overdraw the wallet).
    async with db_seeded.batch() as cur:
        await cur.execute(
            """
            INSERT INTO predictions
                (id, signal_id, llm_call_id, model, prompt_version, direction,
                 confidence, cached, raw_response, label, cycle_id, is_tracer,
                 tracer_id, created_at)
            VALUES (99, NULL, NULL, 'fake', 'v1', 'YES', 0.9, 0, 'Y', 'Y', 1, 0,
                    NULL, '2026-05-23T00:00:00+00:00')
            """,
        )
        await cur.execute(
            """
            INSERT INTO decisions
                (id, signal_id, outcome, reason, gate_history, prediction_id,
                 cycle_id, is_tracer, tracer_id, created_at)
            VALUES (99, NULL, 'execute', 'ok', '[]', 99, 1, 0, NULL,
                    '2026-05-23T00:00:00+00:00')
            """,
        )
        await cur.execute(
            """
            INSERT INTO trades
                (id, signal_id, decision_id, prediction_id, market_id, direction,
                 confidence, similarity, freshness, p_hat, mid_price, edge,
                 kelly_fraction, size_usdc, decision_path, features_snapshot,
                 cycle_id, is_tracer, tracer_id, decided_at)
            VALUES (99, NULL, 99, 99, 'm1', 'YES', 0.9, 0.8, 'BREAKING', 0.9,
                    0.4, 0.1, 0.1, 1100.0, 'mvp', '{}', 1, 0, NULL,
                    '2026-05-23T00:00:00+00:00')
            """,
        )
    bus = _FakeBus()
    ai = _FakeAI()
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload())
    cur = await db_seeded.conn.execute(
        "SELECT outcome, reason FROM decisions WHERE signal_id=1"
    )
    row = await cur.fetchone()
    await cur.close()
    assert row is not None
    assert row["outcome"] == "reject"
    assert row["reason"] == "insufficient_cash"


async def test_entity_overlap_gate_permits_on_extraction_race(db_seeded):
    # No entities seeded — extraction hasn't completed for this article.
    # Gate should be permissive (don't reject), let the LLM call proceed.
    bus = _FakeBus()
    ai = _FakeAI()  # default YES → execute
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload())
    d = await _single_decision(db_seeded)
    assert d["outcome"] == "execute"   # gate did NOT reject; LLM ran
    assert ai.calls != []


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


async def test_mid_degenerate_rejects_yes_at_low_mid(db_seeded):
    # CLOB returns mid=0.011 for the YES token — market essentially resolved
    # NO. Trader should reject before edge calc, no trade row written.
    clob = _FakeCLOB({"ty": 0.011, "tn": 0.989})
    bus = _FakeBus()
    ai = _FakeAI(next_result=DirectionResult(
        direction="YES", confidence=0.7, prediction_id=20,
        cached=False, raw_response="{}",
    ))
    stage = _make_stage(db_seeded, bus, ai, clob=clob)
    with cycle_scope(1):
        await stage.handle(_payload())

    d = await _single_decision(db_seeded)
    assert d["outcome"] == "reject"
    assert d["reason"] == "mid_degenerate"
    assert bus.published[0][1]["reason"] == "mid_degenerate"
    cur = await db_seeded.conn.execute("SELECT COUNT(*) FROM trades")
    assert (await cur.fetchone())[0] == 0
    await cur.close()


async def test_mid_degenerate_rejects_no_at_high_mid(db_seeded):
    # CLOB returns mid=0.995 for the NO token — market essentially resolved
    # NO already (NO bet at 0.995 buys NO at almost-full price). Reject.
    clob = _FakeCLOB({"ty": 0.005, "tn": 0.995})
    bus = _FakeBus()
    ai = _FakeAI(next_result=DirectionResult(
        direction="NO", confidence=0.7, prediction_id=21,
        cached=False, raw_response="{}",
    ))
    stage = _make_stage(db_seeded, bus, ai, clob=clob)
    with cycle_scope(1):
        await stage.handle(_payload())

    d = await _single_decision(db_seeded)
    assert d["outcome"] == "reject"
    assert d["reason"] == "mid_degenerate"
    cur = await db_seeded.conn.execute("SELECT COUNT(*) FROM trades")
    assert (await cur.fetchone())[0] == 0
    await cur.close()


async def test_wide_spread_rejects_when_book_exceeds_threshold(db_seeded):
    """Polymarket's /midpoint returns the last traded price (not the true
    mid) when spread > $0.10. We don't trust mid-derived edge on wide
    markets — reject at entry before the edge math fires."""
    # Mid is fine (0.50), but book shows a 0.20 spread → reject.
    clob = _FakeCLOB(
        prices={"ty": 0.50, "tn": 0.50},
        books={"ty": (0.40, 0.60)},  # spread = 0.20 > 0.10 default
    )
    bus = _FakeBus()
    ai = _FakeAI(next_result=DirectionResult(
        direction="YES", confidence=0.7, prediction_id=22,
        cached=False, raw_response="{}",
    ))
    stage = _make_stage(db_seeded, bus, ai, clob=clob)
    with cycle_scope(1):
        await stage.handle(_payload())

    d = await _single_decision(db_seeded)
    assert d["outcome"] == "reject"
    assert d["reason"] == "wide_spread"
    assert bus.published[0][1]["reason"] == "wide_spread"
    cur = await db_seeded.conn.execute("SELECT COUNT(*) FROM trades")
    assert (await cur.fetchone())[0] == 0
    await cur.close()


async def test_wide_spread_gate_skipped_when_book_unavailable(db_seeded):
    """If the book read fails (None), the wide-spread gate is skipped
    rather than blocking the trade — graceful degrade. The
    degenerate-mid + market-news gates still defend the entry."""
    # No `books` arg → get_top_of_book returns None for every token.
    clob = _FakeCLOB(prices={"ty": 0.50, "tn": 0.50})
    bus = _FakeBus()
    ai = _FakeAI(next_result=DirectionResult(
        direction="YES", confidence=0.9, prediction_id=23,
        cached=False, raw_response="{}",
    ))
    stage = _make_stage(db_seeded, bus, ai, clob=clob)
    with cycle_scope(1):
        await stage.handle(_payload())

    d = await _single_decision(db_seeded)
    # Whatever the downstream outcome, it must NOT be wide_spread.
    assert d["reason"] != "wide_spread"


async def test_narrow_spread_passes_gate(db_seeded):
    """A book with spread <= threshold doesn't trigger the wide-spread
    gate."""
    clob = _FakeCLOB(
        prices={"ty": 0.50, "tn": 0.50},
        books={"ty": (0.49, 0.51)},  # spread = 0.02 << 0.10
    )
    bus = _FakeBus()
    ai = _FakeAI(next_result=DirectionResult(
        direction="YES", confidence=0.9, prediction_id=24,
        cached=False, raw_response="{}",
    ))
    stage = _make_stage(db_seeded, bus, ai, clob=clob)
    with cycle_scope(1):
        await stage.handle(_payload())

    d = await _single_decision(db_seeded)
    assert d["reason"] != "wide_spread"


async def test_edge_uses_executable_side_when_book_available(db_seeded):
    """Cycle 10: when the live book is available, the edge gate uses
    `executable_side - p_hat - taker_fee_buffer - half_spread`, not the
    legacy lumped `fee_buffer`. With p_hat=0.9 and a tight book of
    (0.49, 0.51), the executable YES side is best_ask=0.51, half_spread
    is 0.01, taker_fee_buffer is 0.01, so
        edge = 0.9 - 0.51 - 0.01 - 0.01 = 0.37
    — well above min_edge=0.04. Trade should execute and the
    `edge_components` block should be plumbed into features_snapshot
    with the new fields and `fallback=False`."""
    clob = _FakeCLOB(
        prices={"ty": 0.51, "tn": 0.49},
        books={"ty": (0.49, 0.51)},
    )
    bus = _FakeBus()
    ai = _FakeAI(next_result=DirectionResult(
        direction="YES", confidence=0.9, prediction_id=101,
        cached=False, raw_response="{}",
    ))
    stage = _make_stage(db_seeded, bus, ai, clob=clob)
    with cycle_scope(1):
        await stage.handle(_payload())

    d = await _single_decision(db_seeded)
    assert d["outcome"] == "execute", d
    cur = await db_seeded.conn.execute(
        "SELECT edge, features_snapshot FROM trades WHERE prediction_id=101"
    )
    row = await cur.fetchone()
    await cur.close()
    assert row is not None
    assert row["edge"] == pytest.approx(0.9 - 0.51 - 0.01 - 0.01, abs=1e-9)
    snap = json.loads(row["features_snapshot"])
    ec = snap["edge_components"]
    assert ec["fallback"] is False
    assert ec["executable_price"] == pytest.approx(0.51, abs=1e-9)
    assert ec["spread"] == pytest.approx(0.02, abs=1e-9)
    assert ec["half_spread"] == pytest.approx(0.01, abs=1e-9)
    assert ec["taker_fee_buffer"] == pytest.approx(0.01, abs=1e-9)


async def test_edge_falls_back_to_mid_when_book_unavailable(db_seeded):
    """Cycle 10: with no live book, the edge gate falls back to the
    legacy `p_hat - mid - fee_buffer` formula. `features_snapshot.
    edge_components.fallback` must be True so we can audit how often
    the fallback path fires. With p_hat=0.7, mid=0.5, fee_buffer=0.02,
    edge = 0.18 > min_edge."""
    clob = _FakeCLOB(prices={"ty": 0.50, "tn": 0.50})  # no `books`
    bus = _FakeBus()
    ai = _FakeAI(next_result=DirectionResult(
        direction="YES", confidence=0.7, prediction_id=102,
        cached=False, raw_response="{}",
    ))
    stage = _make_stage(db_seeded, bus, ai, clob=clob)
    with cycle_scope(1):
        await stage.handle(_payload())

    d = await _single_decision(db_seeded)
    assert d["outcome"] == "execute", d
    cur = await db_seeded.conn.execute(
        "SELECT edge, features_snapshot FROM trades WHERE prediction_id=102"
    )
    row = await cur.fetchone()
    await cur.close()
    assert row is not None
    # Legacy formula: 0.7 - 0.5 - 0.02 = 0.18.
    assert row["edge"] == pytest.approx(0.18, abs=1e-9)
    snap = json.loads(row["features_snapshot"])
    ec = snap["edge_components"]
    assert ec["fallback"] is True
    assert ec["fee_buffer"] == pytest.approx(0.02, abs=1e-9)
    assert "executable_price" not in ec


async def test_edge_rejects_when_executable_side_eats_budget(db_seeded):
    """Cycle 10: a narrow-enough book to pass `wide_spread` (0.02 <
    0.10) can still kill the edge once we account for the executable
    side and explicit costs. Book (0.85, 0.87), confidence 0.86 → YES
    edge = 0.86 - 0.87 - 0.01 - 0.01 = -0.03 < min_edge=0.04 → reject
    with `edge_below_min`. The legacy formula would have given
    0.86 - 0.86 - 0.02 = -0.02 (also reject, but only because mid==p_hat
    coincidentally) — the new formula rejects cleanly on the executable
    side eating the budget regardless of where mid sits."""
    # Mid here is the YES mid surrogate — set to 0.86 so degenerate-mid
    # (max_mid=0.98) and market-news extreme-mid (0.85 boundary) don't
    # fire instead. We need a non-BREAKING-friendly setup: ensure
    # freshness=BREAKING so extreme-mid sub-rule is bypassed.
    clob = _FakeCLOB(
        prices={"ty": 0.86, "tn": 0.14},
        books={"ty": (0.85, 0.87)},
    )
    bus = _FakeBus()
    ai = _FakeAI(next_result=DirectionResult(
        direction="YES", confidence=0.86, prediction_id=103,
        cached=False, raw_response="{}",
    ))
    # Disable singleton-on-extreme-mid so we land on the edge gate, not
    # on market_news_imbalance (mid=0.86 is past the 0.85 threshold).
    cfg = _Config(trade=_TradeCfg(
        reject_singleton_on_extreme_mid=False,
        require_breaking_for_extreme_mid=False,
    ))
    stage = _make_stage(db_seeded, bus, ai, cfg=cfg, clob=clob)
    with cycle_scope(1):
        await stage.handle(_payload(freshness="BREAKING"))

    d = await _single_decision(db_seeded)
    assert d["outcome"] == "reject", d
    assert d["reason"] == "edge_below_min"


async def test_market_news_imbalance_thin_market_rejects(db_seeded):
    # Cycle 7: market with volume_usdc < min_market_volume_usdc rejects.
    # Override m1's volume to $5k (under the $10k default floor).
    async with db_seeded.batch() as cur:
        await cur.execute("UPDATE markets SET volume_usdc=5000.0 WHERE condition_id='m1'")
    bus = _FakeBus()
    ai = _FakeAI(next_result=DirectionResult(
        direction="YES", confidence=0.7, prediction_id=51,
        cached=False, raw_response="{}",
    ))
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload())
    d = await _single_decision(db_seeded)
    assert d["outcome"] == "reject"
    assert d["reason"] == "market_news_imbalance"


async def test_market_news_imbalance_far_future_evolving_rejects(db_seeded):
    # Cycle 7: end_date > 180 days AND freshness != BREAKING rejects.
    # Fixture has end_date='2026-12-31'; treat the test "now" as far enough
    # back that 2026-12-31 is > 180 days away. Force end_date to 2030 so
    # the test is robust to time-of-clock drift.
    async with db_seeded.batch() as cur:
        await cur.execute("UPDATE markets SET end_date='2030-12-31T00:00:00Z' WHERE condition_id='m1'")
    bus = _FakeBus()
    ai = _FakeAI(next_result=DirectionResult(
        direction="YES", confidence=0.7, prediction_id=52,
        cached=False, raw_response="{}",
    ))
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload(freshness="EVOLVING"))
    d = await _single_decision(db_seeded)
    assert d["outcome"] == "reject"
    assert d["reason"] == "market_news_imbalance"


async def test_market_news_imbalance_far_future_breaking_allows(db_seeded):
    # Same far-future market BUT freshness=BREAKING bypasses the rule.
    async with db_seeded.batch() as cur:
        await cur.execute("UPDATE markets SET end_date='2030-12-31T00:00:00Z' WHERE condition_id='m1'")
    bus = _FakeBus()
    ai = _FakeAI(next_result=DirectionResult(
        direction="YES", confidence=0.7, prediction_id=53,
        cached=False, raw_response="{}",
    ))
    # Use a CLOB that returns a non-extreme mid so we make it past gate.
    clob = _FakeCLOB({"ty": 0.5, "tn": 0.5})
    stage = _make_stage(db_seeded, bus, ai, clob=clob)
    with cycle_scope(1):
        await stage.handle(_payload(freshness="BREAKING"))
    d = await _single_decision(db_seeded)
    # Passes market_news_imbalance; should reach edge gate.
    assert d["reason"] != "market_news_imbalance"


async def test_market_news_imbalance_extreme_mid_evolving_rejects(db_seeded):
    # Mid 0.95 + EVOLVING → reject. Bring end_date close so far_future doesn't fire.
    async with db_seeded.batch() as cur:
        await cur.execute("UPDATE markets SET end_date='2026-06-15T00:00:00Z' WHERE condition_id='m1'")
    clob = _FakeCLOB({"ty": 0.95, "tn": 0.05})
    bus = _FakeBus()
    ai = _FakeAI(next_result=DirectionResult(
        direction="YES", confidence=0.7, prediction_id=54,
        cached=False, raw_response="{}",
    ))
    stage = _make_stage(db_seeded, bus, ai, clob=clob)
    with cycle_scope(1):
        await stage.handle(_payload(freshness="EVOLVING"))
    d = await _single_decision(db_seeded)
    # min_mid/max_mid = 0.02/0.98 so degenerate-mid doesn't fire at 0.95;
    # market_news_imbalance:extreme_mid_no_breaking should fire instead.
    assert d["reason"] == "market_news_imbalance"


async def test_market_news_imbalance_near_expiry_rejects(db_seeded):
    # end_date in the past (or < 1 day) → reject regardless of freshness.
    async with db_seeded.batch() as cur:
        await cur.execute("UPDATE markets SET end_date='2020-01-01T00:00:00Z' WHERE condition_id='m1'")
    bus = _FakeBus()
    ai = _FakeAI(next_result=DirectionResult(
        direction="YES", confidence=0.7, prediction_id=55,
        cached=False, raw_response="{}",
    ))
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload(freshness="BREAKING"))
    d = await _single_decision(db_seeded)
    assert d["outcome"] == "reject"
    assert d["reason"] == "market_news_imbalance"


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


async def test_size_gate_rejects_below_market_min_when_floor_binds(db_seeded):
    """Cycle 13 (plans/order-execution-spreads.md proposal #4).

    Polymarket exposes a per-market `minimum_order_size` in SHARES.
    The trade-stage size gate converts to USDC via the executable
    price for the bet's side:
        min_usdc_on_this_market = min_shares × executable_price
    With defaults (balance=1000, kelly_fraction=0.25,
    max_position_fraction=0.20) and YES@0.7 vs mid=0.5: size=100 USDC.

    Seed the market row with `min_order_size=300` shares. At the
    default mid 0.5 the per-market floor = 300×0.5 = 150 USDC,
    which exceeds the computed size (100). Gate must reject with
    `reason="below_market_min"` — NOT the legacy `size_below_min`
    — so funnel monitoring distinguishes "platform rejected" from
    "trader's own config bound"."""
    # Stamp the per-market floor on the seeded market row.
    async with db_seeded.batch() as cur:
        await cur.execute(
            "UPDATE markets SET min_order_size = 300 WHERE condition_id='m1'"
        )
    cfg = _Config(trade=_TradeCfg(
        balance_usdc=1000.0,
        kelly_fraction=0.25,
        max_position_fraction=0.20,
        min_position_usdc=1.0,  # tiny — the market floor binds
    ))
    bus = _FakeBus()
    ai = _FakeAI(next_result=DirectionResult(
        direction="YES", confidence=0.7, prediction_id=11,
        cached=False, raw_response="{}",
    ))
    stage = _make_stage(db_seeded, bus, ai, cfg=cfg)
    with cycle_scope(1):
        await stage.handle(_payload())

    d = await _single_decision(db_seeded)
    assert d["outcome"] == "reject"
    assert d["reason"] == "below_market_min"
    assert bus.published[0][1]["reason"] == "below_market_min"
    # Sanity check the gate_history records the market-derived
    # threshold (150.0 USDC at mid=0.5), not the global config floor.
    size_entries = [g for g in d["gate_history"] if g["gate"] == "size"]
    assert len(size_entries) == 1
    assert size_entries[0]["outcome"] == "reject"
    assert size_entries[0]["threshold"] == pytest.approx(150.0)


async def test_size_gate_config_floor_still_binds_when_market_floor_lower(db_seeded):
    """Cycle 13: when the per-market `min_order_size × price` is BELOW
    the global `cfg.trade.min_position_usdc`, the global config floor
    remains the binding threshold. Rejection reason stays `size_below_min`
    (legacy) so the funnel-monitoring contract for the global gate is
    preserved.

    Seed the market with a tiny `min_order_size=1` share. At mid=0.5
    that's 0.5 USDC — well below the (deliberately large) config
    floor of 50 USDC. With balance=1.0 the computed size is 0.1 USDC,
    so the config floor bites, not the market one."""
    async with db_seeded.batch() as cur:
        await cur.execute(
            "UPDATE markets SET min_order_size = 1 WHERE condition_id='m1'"
        )
    cfg = _Config(trade=_TradeCfg(
        balance_usdc=1.0,  # tiny so size < both floors
        min_position_usdc=50.0,  # deliberately large
    ))
    bus = _FakeBus()
    ai = _FakeAI(next_result=DirectionResult(
        direction="YES", confidence=0.7, prediction_id=12,
        cached=False, raw_response="{}",
    ))
    stage = _make_stage(db_seeded, bus, ai, cfg=cfg)
    with cycle_scope(1):
        await stage.handle(_payload())

    d = await _single_decision(db_seeded)
    assert d["outcome"] == "reject"
    assert d["reason"] == "size_below_min"
    # gate_history threshold is the config floor (50.0), confirming
    # the global config is what bound.
    size_entries = [g for g in d["gate_history"] if g["gate"] == "size"]
    assert len(size_entries) == 1
    assert size_entries[0]["threshold"] == pytest.approx(50.0)


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


# ---------------------------------------------------------------------------
# Cycle 14: per-signal latency-chain instrumentation
# ---------------------------------------------------------------------------


async def test_trade_row_has_decision_latency_columns(db_seeded):
    """Schema check: the cycle-14 ALTER TABLEs added the two latency
    columns to `trades`. PRAGMA table_info reflects them with the
    expected types and NULL-allowed (legacy rows stay NULL).
    """
    cur = await db_seeded.conn.execute("PRAGMA table_info(trades)")
    rows = await cur.fetchall()
    await cur.close()
    cols = {r["name"]: r["type"] for r in rows}
    assert "decision_latency_ms" in cols, (
        f"decision_latency_ms missing; columns are: {sorted(cols)}"
    )
    assert "decision_latency_breakdown" in cols, (
        f"decision_latency_breakdown missing; columns are: {sorted(cols)}"
    )
    # Types per the DDL — REAL for ms, TEXT for the JSON blob.
    assert cols["decision_latency_ms"].upper() == "REAL"
    assert cols["decision_latency_breakdown"].upper() == "TEXT"


async def test_executed_trade_records_latency_ms(db_seeded):
    """Drive a successful trade and confirm the cycle-14 instrumentation
    populates both columns. Range/shape checks only — never assert exact
    ms values because CI clocks vary.
    """
    bus = _FakeBus()
    ai = _FakeAI()  # default happy path
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload())

    cur = await db_seeded.conn.execute(
        "SELECT decision_latency_ms, decision_latency_breakdown "
        "FROM trades WHERE signal_id=1"
    )
    row = await cur.fetchone()
    await cur.close()
    assert row is not None

    # decision_latency_ms — positive, sane upper bound (sanity, not strict).
    latency_ms = row["decision_latency_ms"]
    assert latency_ms is not None, "decision_latency_ms not populated"
    assert isinstance(latency_ms, float)
    assert latency_ms > 0.0, f"expected positive latency, got {latency_ms}"
    assert latency_ms < 60_000.0, f"latency suspiciously large: {latency_ms}"

    # decision_latency_breakdown — JSON-encoded dict keyed by step name.
    raw = row["decision_latency_breakdown"]
    assert raw is not None, "decision_latency_breakdown not populated"
    breakdown = json.loads(raw)
    assert isinstance(breakdown, dict)
    expected_keys = {
        "fetch_article",
        "fetch_market",
        "ai_direction",
        "resolve_mid",
        "resolve_book",
        "imbalance_check",
        "db_insert",
    }
    missing = expected_keys - set(breakdown)
    assert not missing, f"breakdown missing keys: {missing}; got {breakdown}"
    # Every per-step ms is a non-negative float.
    for step, ms in breakdown.items():
        assert isinstance(ms, (int, float)), f"{step} value not numeric: {ms!r}"
        assert ms >= 0.0, f"{step} negative: {ms}"


# ---------------------------------------------------------------------------
# Cycle 16: reject-path latency instrumentation
# ---------------------------------------------------------------------------


async def test_rejected_decision_has_latency_columns(db_seeded):
    """Schema check: the cycle-16 ALTER TABLEs added the two latency
    columns to `decisions`. PRAGMA reflects them with the expected types
    (REAL for ms, TEXT for the JSON blob), NULL-allowed (legacy +
    execute rows stay NULL).
    """
    cur = await db_seeded.conn.execute("PRAGMA table_info(decisions)")
    rows = await cur.fetchall()
    await cur.close()
    cols = {r["name"]: r["type"] for r in rows}
    assert "decision_latency_ms" in cols, (
        f"decision_latency_ms missing on decisions; columns are: {sorted(cols)}"
    )
    assert "decision_latency_breakdown" in cols, (
        f"decision_latency_breakdown missing on decisions; columns are: "
        f"{sorted(cols)}"
    )
    assert cols["decision_latency_ms"].upper() == "REAL"
    assert cols["decision_latency_breakdown"].upper() == "TEXT"


async def test_similarity_reject_records_latency_with_partial_breakdown(db_seeded):
    """A `similarity_floor` reject is gate 1 — it fires BEFORE any
    fetch_article / fetch_market / ai_direction step runs. The
    breakdown JSON should therefore be present (not NULL) but empty
    (or strictly a subset of the execute-path keys; in particular it
    must NOT contain `ai_direction`).
    """
    bus = _FakeBus()
    ai = _FakeAI()
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload(similarity=0.10))

    cur = await db_seeded.conn.execute(
        "SELECT outcome, reason, decision_latency_ms, decision_latency_breakdown "
        "FROM decisions WHERE signal_id=1"
    )
    row = await cur.fetchone()
    await cur.close()
    assert row is not None
    assert row["outcome"] == "reject"
    assert row["reason"] == "similarity_floor"

    latency_ms = row["decision_latency_ms"]
    assert latency_ms is not None, "decision_latency_ms not populated on reject"
    assert isinstance(latency_ms, float)
    assert latency_ms > 0.0, f"expected positive latency, got {latency_ms}"
    assert latency_ms < 60_000.0, f"latency suspiciously large: {latency_ms}"

    raw = row["decision_latency_breakdown"]
    assert raw is not None, "decision_latency_breakdown not populated on reject"
    breakdown = json.loads(raw)
    assert isinstance(breakdown, dict)
    # Gate 1 fires before any instrumented step runs.
    assert "ai_direction" not in breakdown, (
        f"ai_direction must NOT be in similarity_floor breakdown; got {breakdown}"
    )
    assert "fetch_article" not in breakdown, (
        f"fetch_article must NOT be in similarity_floor breakdown; got {breakdown}"
    )
    assert "resolve_mid" not in breakdown
    assert "resolve_book" not in breakdown


async def test_edge_reject_records_latency_with_more_steps(db_seeded):
    """An `edge_below_min` reject is gate 4 — fetch_article, fetch_market,
    ai_direction, resolve_mid, resolve_book, imbalance_check all run
    before it fires. The breakdown should contain those keys but NOT
    `db_insert` (only execute paths reach the trade-row INSERT).
    """
    # Configure: high min_edge so the edge gate rejects.
    cfg = _Config()
    cfg.trade.min_edge = 0.99
    bus = _FakeBus()
    ai = _FakeAI()  # YES @ 0.7 → p_hat=0.7, mid=0.5 → edge=0.18 < 0.99
    stage = _make_stage(db_seeded, bus, ai, cfg=cfg)
    with cycle_scope(1):
        await stage.handle(_payload())

    cur = await db_seeded.conn.execute(
        "SELECT outcome, reason, decision_latency_ms, decision_latency_breakdown "
        "FROM decisions WHERE signal_id=1"
    )
    row = await cur.fetchone()
    await cur.close()
    assert row is not None
    assert row["outcome"] == "reject"
    assert row["reason"] == "edge_below_min"

    latency_ms = row["decision_latency_ms"]
    assert latency_ms is not None and latency_ms > 0.0

    raw = row["decision_latency_breakdown"]
    assert raw is not None
    breakdown = json.loads(raw)
    assert isinstance(breakdown, dict)
    # Gate 4 (edge) runs AFTER ai_direction + the two book/mid resolves.
    assert "ai_direction" in breakdown, (
        f"ai_direction expected in edge_below_min breakdown; got {breakdown}"
    )
    assert "resolve_mid" in breakdown
    assert "resolve_book" in breakdown
    # Reject paths NEVER reach the trade-row INSERT — so db_insert must
    # NOT be in the dict. (db_insert is appended by _record_execute only.)
    assert "db_insert" not in breakdown, (
        f"db_insert must NOT be in reject breakdown; got {breakdown}"
    )
    # All recorded values are non-negative floats.
    for step, ms in breakdown.items():
        assert isinstance(ms, (int, float))
        assert ms >= 0.0


async def test_execute_path_still_records_latency(db_seeded):
    """Regression guard for cycle 14: making `_record_reject` carry the
    same instrumentation must not break the execute path's existing
    latency columns on `trades`. We re-assert the canonical key set on a
    happy-path trade — duplicating part of
    `test_executed_trade_records_latency_ms` but isolating the
    regression-guard intent.
    """
    bus = _FakeBus()
    ai = _FakeAI()
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload())

    cur = await db_seeded.conn.execute(
        "SELECT decision_latency_ms, decision_latency_breakdown "
        "FROM trades WHERE signal_id=1"
    )
    row = await cur.fetchone()
    await cur.close()
    assert row is not None

    latency_ms = row["decision_latency_ms"]
    assert latency_ms is not None and latency_ms > 0.0

    raw = row["decision_latency_breakdown"]
    assert raw is not None
    breakdown = json.loads(raw)
    assert isinstance(breakdown, dict)
    # Full execute-path keys present.
    assert {"fetch_article", "fetch_market", "ai_direction",
            "resolve_mid", "resolve_book", "imbalance_check",
            "db_insert"} <= set(breakdown)
    # And the matching `decisions` row for the same execute path
    # leaves the latency columns NULL — the canonical numbers live on
    # `trades` for executes; double-writing would be redundant.
    cur = await db_seeded.conn.execute(
        "SELECT outcome, decision_latency_ms, decision_latency_breakdown "
        "FROM decisions WHERE signal_id=1"
    )
    drow = await cur.fetchone()
    await cur.close()
    assert drow is not None
    assert drow["outcome"] == "execute"
    assert drow["decision_latency_ms"] is None
    assert drow["decision_latency_breakdown"] is None


# ---------------------------------------------------------------------------
# Cycle 24 (handoff H): compose_direction_prompt — fuller article body,
# expanded market context, softer NEUTRAL guidance. The downstream parser
# (`ai._parse_response`) still expects a JSON-only reply, so the "Output
# JSON ONLY." tail must stay intact.
# ---------------------------------------------------------------------------


from app.stages.trade import (  # noqa: E402 — module-level import OK; collated with H tests
    MAX_ARTICLE_CHARS_IN_PROMPT,
    compose_direction_prompt,
)


def _h_article(*, title="T", summary="", body=""):
    """Minimal article dict for prompt-builder unit tests. Empty strings
    collapse to "" (the builder's own empty-default handling kicks in)."""
    return {"title": title, "summary": summary, "body": body}


def _h_market(**overrides):
    """Minimal market dict. Defaults populate every field the cycle-24
    prompt surfaces so tests can override one knob at a time."""
    base = {
        "question": "Will X happen?",
        "description": "Resolution details.",
        "end_date": "2026-12-31T23:59:59Z",
        "volume_usdc": 12345.0,
        "category": "politics",
    }
    base.update(overrides)
    return base


def test_h_prompt_prefers_body_over_summary_when_body_longer():
    """body must win over summary when meaningfully longer (>=1.5×, >=200 chars)."""
    body = "Full article body. " * 30  # ~570 chars
    summary = "Short lede sentence."     # ~20 chars
    prompt = compose_direction_prompt(
        _h_article(title="Headline", summary=summary, body=body),
        _h_market(),
    )
    assert body[:100] in prompt
    # The short summary should NOT appear verbatim — body replaced it.
    assert summary not in prompt


def test_h_prompt_falls_back_to_summary_when_body_absent():
    """Empty body → summary wins (the cycle-23 behaviour for legacy rows)."""
    summary = "The Senate passed the bill with 67 votes."
    prompt = compose_direction_prompt(
        _h_article(title="H", summary=summary, body=""),
        _h_market(),
    )
    assert summary in prompt


def test_h_prompt_falls_back_to_summary_when_body_too_short():
    """A body that's <1.5× the summary length is NOT preferred — protects
    against trafilatura returning a 1-sentence excerpt that happens to be
    a few characters longer than the RSS lede."""
    summary = "A clean one-sentence RSS lede that gives real context here."
    body = "A slightly longer one-sentence excerpt that gives real context."
    prompt = compose_direction_prompt(
        _h_article(title="H", summary=summary, body=body),
        _h_market(),
    )
    assert summary in prompt
    assert body not in prompt


def test_h_prompt_truncates_long_article_at_cap():
    """A 12 KB body must be capped at MAX_ARTICLE_CHARS_IN_PROMPT chars
    with a truncation marker so the LLM knows the input is incomplete."""
    body = "x" * 12_000
    prompt = compose_direction_prompt(
        _h_article(title="H", summary="", body=body),
        _h_market(),
    )
    # The "x" run must not exceed the cap inside the prompt.
    longest_x_run = max((len(s) for s in prompt.split("\n")), default=0)
    assert longest_x_run <= MAX_ARTICLE_CHARS_IN_PROMPT + 100, (
        f"prompt has a {longest_x_run}-char x-run; cap is "
        f"{MAX_ARTICLE_CHARS_IN_PROMPT}"
    )
    assert "truncated" in prompt


def test_h_prompt_surfaces_market_end_date_volume_category():
    """end_date, volume (formatted), and category must all appear in the
    prompt body so the LLM can reason about timing + depth + topic."""
    prompt = compose_direction_prompt(
        _h_article(title="H", summary="s"),
        _h_market(
            end_date="2026-06-15T00:00:00Z",
            volume_usdc=987_654.0,
            category="geopolitics",
        ),
    )
    assert "2026-06-15T00:00:00Z" in prompt
    # Comma-grouped formatting per the builder.
    assert "987,654" in prompt
    assert "geopolitics" in prompt


def test_h_prompt_handles_missing_market_fields_gracefully():
    """Missing optional market fields must NOT raise — placeholders only."""
    prompt = compose_direction_prompt(
        _h_article(title="H", summary="s"),
        {"question": "Q?", "description": "D"},
    )
    assert "(unknown)" in prompt          # end_date placeholder
    assert "(uncategorized)" in prompt    # category placeholder
    assert "?" in prompt                  # volume placeholder


def test_h_prompt_softens_neutral_guidance():
    """The cycle-24 wording must drop the hard "Default to … whenever
    you're unsure" line and explicitly invite a low-confidence
    directional lean. The downstream gate cascade still filters on
    `min_confidence`, so a more permissive prompt is safe."""
    prompt = compose_direction_prompt(
        _h_article(title="H", summary="s"),
        _h_market(),
    )
    assert "Default to" not in prompt
    # Anchor on the new permissive phrasing.
    assert "low confidence" in prompt
    assert "directional lean" in prompt


def test_h_prompt_preserves_json_only_tail():
    """The downstream parser (`ai._parse_response`) keys on a JSON-only
    response; if the tail line ever drifts, the parser regresses to
    NEUTRAL on every call."""
    prompt = compose_direction_prompt(
        _h_article(title="H", summary="s"),
        _h_market(),
    )
    assert prompt.rstrip().endswith("Output JSON ONLY.")
