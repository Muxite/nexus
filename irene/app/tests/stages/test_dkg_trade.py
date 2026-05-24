"""DKGTradeStage tests — score gate, idempotency, direction reuse, JSON
roundtrip. Mirrors the shape of test_trade.py.

Setup: ephemeral DB pre-populated with markets + an article + an
extraction-run + a fact pinned to an `event_id`, plus a `market_links`
row above the score floor. The stage's `handle()` reads the link, walks
back to the article, calls the (faked) AIClient, and writes
`decisions` / `trades` / `trade_features` / `dkg_trade_links`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Optional

import pytest

# DDL registration side effects — order matters because eventize.py
# ALTERs `facts` to add `event_id`, so extract.py must register first.
import app.ai  # noqa: F401
import app.main  # noqa: F401 — cycles DDL
import app.stages.dedupe  # noqa: F401
import app.stages.extract  # noqa: F401
import app.stages.eventize  # noqa: F401  (must follow extract)
import app.stages.ingest  # noqa: F401
import app.stages.link  # noqa: F401
import app.stages.markets  # noqa: F401
import app.stages.match  # noqa: F401
import app.stages.trade  # noqa: F401
import app.stages.dkg_trade  # noqa: F401  (owns dkg_trade_links)
from app.ai import DirectionResult
from app.cycle import cycle_scope
from app.db import DB
from app.deps import StageDeps
from app.stages.dkg_trade import DKGTradeStage
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
    next_result: DirectionResult = field(
        default_factory=lambda: DirectionResult(
            direction="YES", confidence=0.9, prediction_id=101,
            cached=False, raw_response='{"direction":"YES","confidence":0.9}',
        )
    )
    calls: list[dict] = field(default_factory=list)

    async def direction(
        self,
        *,
        signal_id: Optional[int],
        prompt: str,
        is_tracer: bool = False,
        tracer_id: Optional[str] = None,
    ) -> DirectionResult:
        self.calls.append(
            {"signal_id": signal_id, "prompt": prompt,
             "is_tracer": is_tracer, "tracer_id": tracer_id}
        )
        return self.next_result


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
class _DKGTradeCfg:
    enabled: bool = True
    score_floor: float = 0.8
    inherit_trade_gates: bool = True


@dataclass
class _Config:
    trade: _TradeCfg = field(default_factory=_TradeCfg)
    dkg_trade: _DKGTradeCfg = field(default_factory=_DKGTradeCfg)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def db_seeded():
    """Ephemeral DB with one event, one fact, one article, one market,
    and ONE `market_links` row at final_score=0.85.

    Layout:
      articles.id = 1, summary, title.
      markets.condition_id = 'cond-1'.
      entities.id = 1.
      extraction_runs.id = 1 → article 1.
      facts.id = 1 → article 1 → event_id 1.
      events.id = 1.
      market_links: event_id=1, market_condition_id='cond-1',
                    final_score=0.85.
    """
    db = DB.ephemeral()
    await db.connect()
    async with db.batch() as cur:
        await cur.execute(
            """
            INSERT INTO articles
                (id, source, source_detail, url, title, summary, published_at,
                 ingested_at, cycle_id, is_tracer, tracer_id)
            VALUES (1, 'rss','test','https://x/1','Trump strikes Iran',
                    'A breaking news article body.',
                    '2026-05-23T00:00:00+00:00',
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
            VALUES ('cond-1', 'slug-1', 'Will Trump strike Iran?',
                    'Resolution details.', 'politics', '2026-12-31', 1000.0,
                    'ty', 'tn', NULL, NULL, NULL, NULL, 1, NULL,
                    '2026-05-23T00:00:00+00:00', '2026-05-23T00:00:00+00:00')
            """,
        )
        await cur.execute(
            """
            INSERT INTO entities (entity_type, canonical_name,
                                  first_seen_at, last_seen_at)
            VALUES ('PERSON', 'Trump',
                    '2026-05-23T00:00:00+00:00', '2026-05-23T00:00:00+00:00')
            """,
        )
        await cur.execute(
            """
            INSERT INTO extraction_runs
                (id, article_id, model, prompt_version, ontology_version,
                 status, fact_count, entity_count, dropped_fact_count,
                 cycle_id, created_at)
            VALUES (1, 1, 'm', 'v3', 'v1', 'ok', 1, 1, 0, 1,
                    '2026-05-23T00:00:00+00:00')
            """,
        )
        await cur.execute(
            """
            INSERT INTO events
                (id, event_type, status, canonical_summary, support_count,
                 first_seen_at, last_seen_at, cycle_id, eventize_version)
            VALUES (1, 'REGULATORY_DECISION', 'emerging',
                    'Trump decides Iran strike', 1,
                    '2026-05-23T00:00:00+00:00',
                    '2026-05-23T00:00:00+00:00', 1, 'v1')
            """,
        )
        await cur.execute(
            """
            INSERT INTO facts
                (extraction_run_id, article_id, subj_entity_id, relation,
                 confidence, observed_at, evidence_span, cycle_id, event_id)
            VALUES (1, 1, 1, 'decides', 1.0,
                    '2026-05-23T00:00:00+00:00', 'span', 1, 1)
            """,
        )
        await cur.execute(
            """
            INSERT INTO market_links
                (event_id, market_condition_id, link_class,
                 directness_score, novelty_score, deadline_fit_score,
                 resolution_fit_score, final_score, causal_path,
                 linker_version, cycle_id, created_at)
            VALUES (1, 'cond-1', 'direct', 0.9, 1.0, 0.7, 0.9, 0.85,
                    NULL, 'v1', 1, '2026-05-23T00:00:00+00:00')
            """,
        )
    try:
        yield db
    finally:
        await db.close()


def _make_stage(db, bus, ai, cfg: Optional[_Config] = None, *, clob=None) -> DKGTradeStage:
    deps = StageDeps(
        db=db,
        bus=bus,
        config=cfg or _Config(),
        ai=ai,
        clob=clob,
    )
    return DKGTradeStage(deps)


def _payload(
    *,
    event_id: int = 1,
    links_written: int = 1,
    top_final_score: float = 0.85,
    is_tracer: bool = False,
    tracer_id: Optional[str] = None,
) -> dict:
    return {
        "event_id": event_id,
        "links_written": links_written,
        "top_final_score": top_final_score,
        "is_tracer": is_tracer,
        "tracer_id": tracer_id,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_owned_table_exists_after_connect():
    db = DB.ephemeral()
    await db.connect()
    try:
        cur = await db.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='dkg_trade_links'"
        )
        row = await cur.fetchone()
        await cur.close()
        assert row is not None
    finally:
        await db.close()


async def test_init_requires_ai(db_seeded):
    bus = _FakeBus()
    with pytest.raises(RuntimeError, match="ai"):
        DKGTradeStage(StageDeps(db=db_seeded, bus=bus, config=_Config(), ai=None))


async def test_happy_path_writes_dkg_trade(db_seeded):
    bus = _FakeBus()
    ai = _FakeAI()  # YES@0.9
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload())

    # decisions row
    cur = await db_seeded.conn.execute(
        "SELECT signal_id, outcome, reason, prediction_id FROM decisions"
    )
    drows = await cur.fetchall()
    await cur.close()
    assert len(drows) == 1
    assert drows[0]["signal_id"] is None  # DKG path leaves signal_id NULL
    assert drows[0]["outcome"] == "execute"
    assert drows[0]["reason"] == "ok"
    assert drows[0]["prediction_id"] == 101

    # trades row
    cur = await db_seeded.conn.execute(
        "SELECT signal_id, market_id, direction, decision_path, "
        "       similarity, features_snapshot "
        "FROM trades"
    )
    trows = await cur.fetchall()
    await cur.close()
    assert len(trows) == 1
    t = trows[0]
    assert t["signal_id"] is None
    assert t["market_id"] == "cond-1"
    assert t["direction"] == "YES"
    assert t["decision_path"] == "dkg"
    # similarity column is repurposed to carry the final_score for DKG rows
    assert t["similarity"] == pytest.approx(0.85)
    snap = json.loads(t["features_snapshot"])
    assert snap["decision_path"] == "dkg"
    assert snap["event_id"] == 1
    assert snap["final_score"] == pytest.approx(0.85)

    # dkg_trade_links row updated with trade_id
    cur = await db_seeded.conn.execute(
        "SELECT event_id, market_condition_id, decision_path, "
        "       trade_id, decision_id, reject_reason "
        "FROM dkg_trade_links"
    )
    lrows = await cur.fetchall()
    await cur.close()
    assert len(lrows) == 1
    assert lrows[0]["event_id"] == 1
    assert lrows[0]["market_condition_id"] == "cond-1"
    assert lrows[0]["decision_path"] == "dkg"
    assert lrows[0]["trade_id"] is not None
    assert lrows[0]["decision_id"] is not None
    assert lrows[0]["reject_reason"] is None

    # bus publish
    assert len(bus.published) == 1
    topic, p = bus.published[0]
    assert topic == TOPIC_TRADE_DECIDED
    assert p["decision_path"] == "dkg"
    assert p["outcome"] == "execute"


async def test_score_floor_rejects_low_score_link(db_seeded):
    """A link below cfg.dkg_trade.score_floor is invisible to the stage —
    no decision, no trade, no AI call."""
    # Replace the seeded high-score link with a low-score one.
    async with db_seeded.batch() as cur:
        await cur.execute("DELETE FROM market_links")
        await cur.execute(
            """
            INSERT INTO market_links
                (event_id, market_condition_id, link_class,
                 directness_score, novelty_score, deadline_fit_score,
                 resolution_fit_score, final_score, causal_path,
                 linker_version, cycle_id, created_at)
            VALUES (1, 'cond-1', 'direct', 0.3, 0.5, 0.5, 0.5, 0.50,
                    NULL, 'v1', 1, '2026-05-23T00:00:00+00:00')
            """,
        )

    bus = _FakeBus()
    ai = _FakeAI()
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload(top_final_score=0.5))

    cur = await db_seeded.conn.execute("SELECT COUNT(*) FROM decisions")
    assert (await cur.fetchone())[0] == 0
    await cur.close()
    cur = await db_seeded.conn.execute("SELECT COUNT(*) FROM trades")
    assert (await cur.fetchone())[0] == 0
    await cur.close()
    cur = await db_seeded.conn.execute("SELECT COUNT(*) FROM dkg_trade_links")
    assert (await cur.fetchone())[0] == 0
    await cur.close()
    assert ai.calls == []
    assert bus.published == []


async def test_idempotent_on_replay(db_seeded):
    """Re-handling the same payload doesn't create a second trade."""
    bus = _FakeBus()
    ai = _FakeAI()
    stage = _make_stage(db_seeded, bus, ai)
    payload = _payload()
    with cycle_scope(1):
        await stage.handle(payload)
        await stage.handle(payload)

    cur = await db_seeded.conn.execute("SELECT COUNT(*) FROM trades")
    n_trades = (await cur.fetchone())[0]
    await cur.close()
    cur = await db_seeded.conn.execute("SELECT COUNT(*) FROM decisions")
    n_dec = (await cur.fetchone())[0]
    await cur.close()
    cur = await db_seeded.conn.execute("SELECT COUNT(*) FROM dkg_trade_links")
    n_links = (await cur.fetchone())[0]
    await cur.close()

    assert n_trades == 1
    assert n_dec == 1
    assert n_links == 1
    # AI was called exactly once — the second handle short-circuited at
    # the dkg_trade_links idempotency claim.
    assert len(ai.calls) == 1
    assert len(bus.published) == 1


async def test_direction_reuse_calls_ai_with_article_prompt(db_seeded):
    """The prompt sent to ai.direction() carries the article title +
    summary so the LLM cache stays shared with the MVP path."""
    bus = _FakeBus()
    ai = _FakeAI()
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload())

    assert len(ai.calls) == 1
    call = ai.calls[0]
    # DKG path passes signal_id=None.
    assert call["signal_id"] is None
    # Prompt contains the article title + market question.
    assert "Trump strikes Iran" in call["prompt"]
    assert "Will Trump strike Iran?" in call["prompt"]


async def test_direction_neutral_writes_reject_decision(db_seeded):
    """When the LLM returns NEUTRAL the DKG path writes a reject
    decision and no trade — same shape as MVP."""
    bus = _FakeBus()
    ai = _FakeAI(next_result=DirectionResult(
        direction="NEUTRAL", confidence=0.0, prediction_id=222,
        cached=False, raw_response="{}",
    ))
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload())

    cur = await db_seeded.conn.execute(
        "SELECT outcome, reason, prediction_id FROM decisions"
    )
    rows = await cur.fetchall()
    await cur.close()
    assert len(rows) == 1
    assert rows[0]["outcome"] == "reject"
    assert rows[0]["reason"] == "direction_neutral"
    assert rows[0]["prediction_id"] == 222

    cur = await db_seeded.conn.execute("SELECT COUNT(*) FROM trades")
    assert (await cur.fetchone())[0] == 0
    await cur.close()

    # dkg_trade_links row exists with reject_reason populated, trade_id NULL.
    cur = await db_seeded.conn.execute(
        "SELECT trade_id, reject_reason FROM dkg_trade_links"
    )
    lrow = await cur.fetchone()
    await cur.close()
    assert lrow["trade_id"] is None
    assert lrow["reject_reason"] == "direction_neutral"

    assert len(bus.published) == 1
    assert bus.published[0][1]["reason"] == "direction_neutral"
    assert bus.published[0][1]["outcome"] == "reject"


async def test_payload_json_roundtrip(db_seeded):
    """The payload the bus delivers is JSON-serialisable. This is the
    contract the InProcBus enforces — every published dict must
    survive json.dumps/json.loads."""
    bus = _FakeBus()
    ai = _FakeAI()
    stage = _make_stage(db_seeded, bus, ai)
    raw = _payload()
    delivered = json.loads(json.dumps(raw))
    assert delivered == raw
    with cycle_scope(1):
        await stage.handle(delivered)
    # Sanity: it actually fired off the roundtripped payload.
    cur = await db_seeded.conn.execute("SELECT COUNT(*) FROM trades")
    assert (await cur.fetchone())[0] == 1
    await cur.close()


async def test_disabled_short_circuits(db_seeded):
    """`cfg.dkg_trade.enabled=False` makes handle() a no-op — nothing
    is written and AI is not called."""
    cfg = _Config(dkg_trade=_DKGTradeCfg(enabled=False))
    bus = _FakeBus()
    ai = _FakeAI()
    stage = _make_stage(db_seeded, bus, ai, cfg=cfg)
    with cycle_scope(1):
        await stage.handle(_payload())

    cur = await db_seeded.conn.execute("SELECT COUNT(*) FROM dkg_trade_links")
    assert (await cur.fetchone())[0] == 0
    await cur.close()
    assert ai.calls == []
    assert bus.published == []


async def test_tracer_threading_into_owned_tables(db_seeded):
    bus = _FakeBus()
    ai = _FakeAI()
    stage = _make_stage(db_seeded, bus, ai)
    with cycle_scope(1):
        await stage.handle(_payload(is_tracer=True, tracer_id="dkg-tracer-1"))

    cur = await db_seeded.conn.execute(
        "SELECT is_tracer, tracer_id FROM dkg_trade_links"
    )
    row = await cur.fetchone()
    await cur.close()
    assert row["is_tracer"] == 1
    assert row["tracer_id"] == "dkg-tracer-1"

    cur = await db_seeded.conn.execute(
        "SELECT is_tracer, tracer_id FROM trades"
    )
    row = await cur.fetchone()
    await cur.close()
    assert row["is_tracer"] == 1
    assert row["tracer_id"] == "dkg-tracer-1"

    # AI receives the tracer flags too.
    assert ai.calls[0]["is_tracer"] is True
    assert ai.calls[0]["tracer_id"] == "dkg-tracer-1"
