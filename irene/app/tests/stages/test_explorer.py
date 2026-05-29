"""ExplorerStage tests — permissive DKG floor + LLM relatedness → row +
odds-capture publish.

Mirrors test_dkg_trade.py: seed the same article/event/fact/market/
market_links shape, drive a synthetic `TOPIC_MARKET_LINKS_UPDATED`
payload through `handle()`, then assert on the `explorations` table
+ published bus events.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Optional

import pytest

import app.ai  # noqa: F401 — DDL registration
import app.main  # noqa: F401 — cycles DDL
import app.stages.dedupe  # noqa: F401
import app.stages.explorer  # noqa: F401  (owns explorations)
import app.stages.extract  # noqa: F401
import app.stages.eventize  # noqa: F401  (must follow extract)
import app.stages.ingest  # noqa: F401
import app.stages.link  # noqa: F401
import app.stages.markets  # noqa: F401
import app.stages.match  # noqa: F401
import app.stages.odds_capture  # noqa: F401  (owns odds_observations)
from app.config import DKGGateConfig, LLMBatchGateConfig, MarketOpenGateConfig
from app.cycle import cycle_scope
from app.db import DB
from app.deps import StageDeps
from app.stages.explorer import ExplorerStage
from app.topics import TOPIC_EXPLORATION_CREATED


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
class _FakeGateAI:
    """v2 gate LLM. `classify_batch_for_gate` is the only AI method the
    explorer calls now — the old single-shot `relatedness()` was folded
    into the `llm_batch` gate (irene/app/gates/llm_batch.py). Returns a
    v2 `verdicts` JSON marking every (news, market) pair in the batch
    `related=<self.related>`, so tests drive pass/reject by flipping the
    flag. Records each call for assertions."""

    related: bool = True
    calls: list[dict] = field(default_factory=list)

    async def classify_batch_for_gate(
        self,
        *,
        system: str,
        user: str,
        model: str,
        max_tokens: int,
        prompt_version: str,
    ) -> str:
        self.calls.append({
            "model": model, "prompt_version": prompt_version, "user": user,
        })
        # The gate labels each pair [A], [B], … in the rendered prompt;
        # echo one verdict per label so this works at any batch size.
        labels = re.findall(r"\[([A-Z]+)\]", user)
        verdicts = [
            {
                "label": lbl,
                "related": self.related,
                "entities_overlap": ["Iran", "Trump"],
                "claim_horizon_hours": 4,
                "directional_lean": "bullish",
                "novelty": "breaking",
                "short_reason": "news resolves market",
            }
            for lbl in labels
        ]
        return json.dumps({"verdicts": verdicts})


class _FakeCLOB:
    """Returns a fixed top-of-book for any token. Tests that exercise
    the missing-book path set `book = None`."""

    def __init__(self, book: Optional[tuple[float, float]] = (0.40, 0.42)):
        self.book = book
        self.calls: list[str] = []

    async def get_top_of_book(
        self, token_id: str
    ) -> Optional[tuple[float, float]]:
        self.calls.append(token_id)
        return self.book


@dataclass
class _ExplorerCfg:
    score_floor: float = 0.5
    odds_poll_secs: int = 15
    capture_post_secs: int = 1200
    capture_pre_secs: int = 600
    max_active_explorations: int = 200
    # 0 → deterministic: rejected rows are never sampled for capture, so
    # the audit-row assertions below can't flake on the RNG.
    reject_capture_sample_rate: float = 0.0
    max_active_reject_samples: int = 40
    # Mirrors the production explorer.yaml pipeline: free liveness +
    # DKG-score gates, then the bundled LLM relatedness gate.
    gates: list = field(default_factory=lambda: [
        MarketOpenGateConfig(),
        DKGGateConfig(name="dkg", min_final_score=0.5),
        LLMBatchGateConfig(name="llm_batch"),
    ])


@dataclass
class _Config:
    explorer: _ExplorerCfg = field(default_factory=_ExplorerCfg)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def db_seeded():
    """Ephemeral DB with one article/event/fact/market/market_links row
    at final_score=0.85. Mirrors test_dkg_trade.db_seeded but with the
    cycle-24 ingest columns (tier, breaking_score, body) populated so
    the explorer's feature snapshot pulls real values."""
    db = DB.ephemeral()
    await db.connect()
    async with db.batch() as cur:
        body = (
            "Full article body about the strike — the President "
            "announced a targeted operation against Iranian assets, "
            "citing security concerns."
        )
        await cur.execute(
            """
            INSERT INTO articles
                (id, source, source_detail, url, title, summary, body,
                 published_at, ingested_at, cycle_id, is_tracer, tracer_id,
                 tier, breaking_score)
            VALUES (1, 'rss', 'reuters', 'https://x/1',
                    'Trump strikes Iran',
                    'Trump announced a targeted strike on Iran today.',
                    ?,
                    '2026-05-23T00:00:00+00:00',
                    '2026-05-23T00:00:00+00:00', 1, 0, NULL,
                    'mainstream', 0.78)
            """,
            (body,),
        )
        await cur.execute(
            """
            INSERT INTO markets
                (condition_id, slug, question, description, category, end_date,
                 volume_usdc, token_yes, token_no, embedding, embedder_id,
                 embedding_dim, embedding_updated_at, active, closed_at,
                 first_seen_at, updated_at)
            VALUES ('cond-1', 'slug-1', 'Will Trump strike Iran?',
                    'Resolution details.', 'politics', '2026-12-31T00:00:00+00:00',
                    50000.0,
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
        # A signals row so the explorer can pull article ↔ market cosine
        # + freshness. DKG-only paths wouldn't have one; tested separately.
        await cur.execute(
            """
            INSERT INTO signals
                (article_id, market_id, similarity, freshness, embedder_id,
                 cycle_id, is_tracer, tracer_id, created_at)
            VALUES (1, 'cond-1', 0.62, 'BREAKING', 'mini', 1, 0, NULL,
                    '2026-05-23T00:00:00+00:00')
            """,
        )
    try:
        yield db
    finally:
        await db.close()


def _make_stage(
    db, bus, ai, cfg: Optional[_Config] = None, *, clob=None
) -> ExplorerStage:
    deps = StageDeps(
        db=db,
        bus=bus,
        config=cfg or _Config(),
        ai=ai,
        clob=clob if clob is not None else _FakeCLOB(),
    )
    return ExplorerStage(deps)


def _payload(*, event_id: int = 1, is_tracer: bool = False,
             tracer_id: Optional[str] = None) -> dict:
    return {
        "event_id": event_id,
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
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='explorations'"
        )
        row = await cur.fetchone()
        await cur.close()
        assert row is not None
    finally:
        await db.close()


async def test_init_requires_ai(db_seeded):
    bus = _FakeBus()
    with pytest.raises(RuntimeError, match="ai"):
        ExplorerStage(StageDeps(
            db=db_seeded, bus=bus, config=_Config(), ai=None,
            clob=_FakeCLOB(),
        ))


async def test_happy_path_writes_exploration_and_publishes(db_seeded):
    bus = _FakeBus()
    ai = _FakeGateAI()  # related=True for the whole batch
    stage = _make_stage(db_seeded, bus, ai)

    with cycle_scope(1):
        await stage.handle(_payload())

    cur = await db_seeded.conn.execute(
        "SELECT article_id, market_condition_id, dkg_final_score, "
        "       cosine_similarity, freshness, breaking_score, "
        "       article_tier, market_volume_24h, market_mid_t0, "
        "       market_spread_t0, hours_to_expiry, llm_related, "
        "       llm_confidence, llm_reason, llm_prompt_hash, gate_status, "
        "       capture_status, capture_end_at "
        "FROM explorations"
    )
    rows = await cur.fetchall()
    await cur.close()
    assert len(rows) == 1
    r = rows[0]
    assert r["article_id"] == 1
    assert r["market_condition_id"] == "cond-1"
    assert r["dkg_final_score"] == pytest.approx(0.85)
    assert r["cosine_similarity"] == pytest.approx(0.62)
    assert r["freshness"] == "BREAKING"
    assert r["breaking_score"] == pytest.approx(0.78)
    assert r["article_tier"] == "mainstream"
    assert r["market_volume_24h"] == pytest.approx(50000.0)
    assert r["market_mid_t0"] == pytest.approx(0.41)
    assert r["market_spread_t0"] == pytest.approx(0.02)
    assert r["hours_to_expiry"] > 0  # not yet expired
    assert r["llm_related"] == 1
    assert r["gate_status"] == "passed"
    # v2: the per-gate confidence/prompt-hash columns are retired — the
    # llm_batch gate's verdict + features live in `gate_history` JSON, and
    # `llm_reason` is now the gate-decision chain.
    assert r["llm_confidence"] is None
    assert r["llm_reason"] == "market_open:passed; dkg:passed; llm_batch:passed"
    assert r["llm_prompt_hash"] is None
    assert r["capture_status"] == "pending"
    assert r["capture_end_at"]  # ISO string populated

    # Published bus event.
    assert len(bus.published) == 1
    topic, payload = bus.published[0]
    assert topic == TOPIC_EXPLORATION_CREATED
    assert payload["article_id"] == 1
    assert payload["market_condition_id"] == "cond-1"
    assert payload["token_yes"] == "ty"
    assert payload["token_no"] == "tn"
    assert payload["t0_iso"]
    assert payload["capture_end_at"] == r["capture_end_at"]


async def test_score_below_floor_skips_silently(db_seeded):
    bus = _FakeBus()
    ai = _FakeGateAI()
    cfg = _Config(explorer=_ExplorerCfg(score_floor=0.9))  # link is 0.85
    stage = _make_stage(db_seeded, bus, ai, cfg)

    with cycle_scope(1):
        await stage.handle(_payload())

    # The SQL `score_floor` pre-filter in `_fetch_high_score_links` drops
    # the 0.85 link before any candidate is built, so no audit row, no
    # gate run, no LLM call.
    cur = await db_seeded.conn.execute("SELECT COUNT(*) FROM explorations")
    assert (await cur.fetchone())[0] == 0
    assert ai.calls == []  # no LLM call burned
    assert bus.published == []


async def test_related_false_writes_audit_row_no_capture(db_seeded):
    bus = _FakeBus()
    ai = _FakeGateAI(related=False)  # llm_batch gate rejects the pair
    stage = _make_stage(db_seeded, bus, ai)

    with cycle_scope(1):
        await stage.handle(_payload())

    # v2: a rejected candidate still writes an audit row (gate_status
    # 'rejected'); it just doesn't get promoted to odds capture or
    # published. The LLM gate IS called — we record the verdict.
    cur = await db_seeded.conn.execute(
        "SELECT gate_status, llm_related, rejected_by_gate FROM explorations"
    )
    rows = await cur.fetchall()
    await cur.close()
    assert len(rows) == 1
    assert rows[0]["gate_status"] == "rejected"
    assert rows[0]["llm_related"] == 0
    assert rows[0]["rejected_by_gate"] == "llm_batch"
    assert len(ai.calls) == 1
    assert bus.published == []


async def test_idempotent_on_repeat_handle(db_seeded):
    bus = _FakeBus()
    ai = _FakeGateAI()
    stage = _make_stage(db_seeded, bus, ai)

    with cycle_scope(1):
        await stage.handle(_payload())
        await stage.handle(_payload())

    cur = await db_seeded.conn.execute("SELECT COUNT(*) FROM explorations")
    assert (await cur.fetchone())[0] == 1
    # The second handle short-circuited via the pre-LLM `_exists()` check
    # — only one LLM call burned.
    assert len(ai.calls) == 1
    # Only one bus publish.
    assert len(bus.published) == 1


async def test_book_unavailable_still_writes_row(db_seeded):
    bus = _FakeBus()
    ai = _FakeGateAI()
    clob = _FakeCLOB(book=None)
    stage = _make_stage(db_seeded, bus, ai, clob=clob)

    with cycle_scope(1):
        await stage.handle(_payload())

    cur = await db_seeded.conn.execute(
        "SELECT market_mid_t0, market_spread_t0, capture_status "
        "FROM explorations"
    )
    row = await cur.fetchone()
    await cur.close()
    assert row is not None
    assert row["market_mid_t0"] is None
    assert row["market_spread_t0"] is None
    # The row is still written and the capture is still scheduled —
    # NULL data is honest, not a failure mode for the explorer.
    assert row["capture_status"] == "pending"


async def test_missing_article_or_market_skips(db_seeded):
    bus = _FakeBus()
    ai = _FakeGateAI()
    stage = _make_stage(db_seeded, bus, ai)

    # event_id 99 has no fact → no article → silent skip
    with cycle_scope(1):
        await stage.handle(_payload(event_id=99))

    cur = await db_seeded.conn.execute("SELECT COUNT(*) FROM explorations")
    assert (await cur.fetchone())[0] == 0
    assert ai.calls == []
    assert bus.published == []


async def test_tracer_flags_propagate(db_seeded):
    bus = _FakeBus()
    ai = _FakeGateAI()
    stage = _make_stage(db_seeded, bus, ai)

    with cycle_scope(1):
        await stage.handle(_payload(is_tracer=True, tracer_id="t-1"))

    cur = await db_seeded.conn.execute(
        "SELECT is_tracer, tracer_id FROM explorations"
    )
    row = await cur.fetchone()
    await cur.close()
    assert row["is_tracer"] == 1
    assert row["tracer_id"] == "t-1"
    # The tracer flags ride the payload through to the row and the
    # published event. (The v2 batch gate call carries no per-candidate
    # tracer metadata — that distinction was specific to the retired
    # single-shot relatedness() path.)
    assert bus.published[0][1]["is_tracer"] is True
    assert bus.published[0][1]["tracer_id"] == "t-1"


def test_wire_pipeline_production_wires_explorer_and_odds_capture():
    """Cycle 26 stage-1 soak fix #6: the two-container setup was
    collapsed into one. MODE_PRODUCTION must now wire ExplorerStage and
    OddsCaptureStage alongside the trade-tail stages so we get the
    labeled `explorations` + `odds_observations` dataset in the same
    writer process. Source-level assertion mirrors the pattern used by
    `test_wire_pipeline_subscribes_link_to_event_updated`."""
    from pathlib import Path
    src = (Path(__file__).resolve().parent.parent.parent.parent
           / "app" / "main.py").read_text()
    # Both subscriptions must be registered in MODE_PRODUCTION wiring.
    assert "bus.subscribe(TOPIC_MARKET_LINKS_UPDATED, explorer.handle)" in src
    assert "bus.subscribe(TOPIC_EXPLORATION_CREATED, odds_capture.handle)" in src
    # And both stages must be constructed in the production branch
    # (the production return tuple includes them).
    assert "explorer=explorer," in src
    assert "odds_capture=odds_capture," in src
