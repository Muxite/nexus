"""MatchStage tests — every branch of `handle()` against an in-memory DB.
Embedder + MarketIndex are stubbed; FakeBus collects publishes."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pytest

import app.ai  # noqa: F401 — register DDL
import app.main  # noqa: F401 — register cycles DDL
import app.stages.dedupe  # noqa: F401
import app.stages.ingest  # noqa: F401
import app.stages.markets  # noqa: F401
import app.stages.match  # noqa: F401 — register signals DDL
from app.cycle import cycle_scope
from app.db import DB
from app.deps import StageDeps
from app.stages.match import MatchStage
from app.topics import TOPIC_SIGNAL_CREATED


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
class _FakeEmbedder:
    dim: int = 4
    embedder_id_value: str = "fake-model@4"

    @property
    def embedder_id(self) -> str:
        return self.embedder_id_value

    def embed(self, text: str) -> np.ndarray:
        # Deterministic per-text vector so the test is reproducible.
        rng = np.random.default_rng(abs(hash(text)) % (2**32))
        v = rng.standard_normal(self.dim).astype(np.float32)
        v /= np.linalg.norm(v) or 1.0
        return v


@dataclass
class _FakeIndex:
    """Stand-in for MarketIndex — the test sets the candidates directly."""

    next_candidates: list[tuple[str, float]] = field(default_factory=list)
    n_calls: int = 0

    @property
    def size(self) -> int:
        return 0 if not self.next_candidates else 10

    def topk(self, query, k: int, floor: float):  # noqa: ARG002
        self.n_calls += 1
        return self.next_candidates[:k]


@dataclass
class _Config:
    class _MatchSection:
        similarity_floor: float = 0.35
        top_k: int = 5

    match: _MatchSection = field(default_factory=_MatchSection)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def db_with_article():
    db = DB.ephemeral()
    await db.connect()
    async with db.batch() as cur:
        await cur.execute(
            """
            INSERT INTO articles
                (source, source_detail, url, title, summary, published_at,
                 ingested_at, cycle_id, is_tracer, tracer_id)
            VALUES ('rss','test','https://x/1','Headline one',NULL,
                    '2026-05-23T00:00:00+00:00','2026-05-23T00:00:00+00:00',
                    1,0,NULL)
            """,
        )
    try:
        yield db
    finally:
        await db.close()


def _make_stage(db, bus, index, embedder=None) -> MatchStage:
    embedder = embedder or _FakeEmbedder()
    deps = StageDeps(
        db=db,
        bus=bus,
        config=_Config(),
        embedder=embedder,
        market_index=index,
    )
    return MatchStage(deps)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_signals_table_exists_after_connect():
    db = DB.ephemeral()
    await db.connect()
    try:
        cur = await db.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='signals'"
        )
        assert (await cur.fetchone()) is not None
        await cur.close()
    finally:
        await db.close()


async def test_init_requires_embedder_and_index(db_with_article):
    bus = _FakeBus()
    with pytest.raises(RuntimeError, match="embedder"):
        MatchStage(
            StageDeps(
                db=db_with_article,
                bus=bus,
                config=_Config(),
                embedder=None,
                market_index=_FakeIndex(),
            )
        )
    with pytest.raises(RuntimeError, match="market_index"):
        MatchStage(
            StageDeps(
                db=db_with_article,
                bus=bus,
                config=_Config(),
                embedder=_FakeEmbedder(),
                market_index=None,
            )
        )


async def test_handle_writes_signals_and_publishes(db_with_article):
    bus = _FakeBus()
    index = _FakeIndex(next_candidates=[("m1", 0.91), ("m2", 0.66)])
    stage = _make_stage(db_with_article, bus, index)
    with cycle_scope(1):
        await stage.handle(
            {"article_id": 1, "cluster_id": 7, "freshness": "BREAKING",
             "is_tracer": False, "tracer_id": None}
        )
    cur = await db_with_article.conn.execute(
        "SELECT market_id, similarity, freshness, cluster_id FROM signals ORDER BY id"
    )
    rows = await cur.fetchall()
    await cur.close()
    assert [(r["market_id"], r["similarity"]) for r in rows] == [("m1", 0.91), ("m2", 0.66)]
    assert all(r["freshness"] == "BREAKING" and r["cluster_id"] == 7 for r in rows)

    assert len(bus.published) == 2
    topics = [t for t, _ in bus.published]
    assert all(t == TOPIC_SIGNAL_CREATED for t in topics)
    payloads = [p for _, p in bus.published]
    assert payloads[0]["market_id"] == "m1"
    assert payloads[1]["market_id"] == "m2"
    assert all("similarity" in p and "freshness" in p for p in payloads)


async def test_handle_empty_topk_no_signals(db_with_article):
    bus = _FakeBus()
    stage = _make_stage(db_with_article, bus, _FakeIndex(next_candidates=[]))
    with cycle_scope(1):
        await stage.handle(
            {"article_id": 1, "cluster_id": None, "freshness": "BREAKING",
             "is_tracer": False, "tracer_id": None}
        )
    cur = await db_with_article.conn.execute("SELECT COUNT(*) FROM signals")
    assert (await cur.fetchone())[0] == 0
    await cur.close()
    assert bus.published == []


async def test_handle_article_missing_no_crash(db_with_article):
    bus = _FakeBus()
    stage = _make_stage(db_with_article, bus, _FakeIndex(next_candidates=[("m1", 0.9)]))
    with cycle_scope(1):
        await stage.handle({"article_id": 99999, "cluster_id": None, "freshness": "BREAKING"})
    cur = await db_with_article.conn.execute("SELECT COUNT(*) FROM signals")
    assert (await cur.fetchone())[0] == 0
    await cur.close()
    assert bus.published == []


async def test_handle_idempotent_on_duplicate(db_with_article):
    bus = _FakeBus()
    index = _FakeIndex(next_candidates=[("m1", 0.91)])
    stage = _make_stage(db_with_article, bus, index)
    payload = {"article_id": 1, "cluster_id": 1, "freshness": "BREAKING",
               "is_tracer": False, "tracer_id": None}
    with cycle_scope(1):
        await stage.handle(payload)
        await stage.handle(payload)
    cur = await db_with_article.conn.execute("SELECT COUNT(*) FROM signals")
    assert (await cur.fetchone())[0] == 1
    await cur.close()
    assert len(bus.published) == 1, "second handle must not republish a duplicate"


async def test_handle_respects_top_k_cap(db_with_article):
    bus = _FakeBus()
    # Index would return 8 candidates; config top_k=5 must cap.
    cands = [(f"m{i}", 0.9 - i * 0.05) for i in range(8)]
    index = _FakeIndex(next_candidates=cands)
    stage = _make_stage(db_with_article, bus, index)
    with cycle_scope(1):
        await stage.handle({"article_id": 1, "cluster_id": None, "freshness": "BREAKING"})
    cur = await db_with_article.conn.execute("SELECT COUNT(*) FROM signals")
    assert (await cur.fetchone())[0] == 5
    await cur.close()


async def test_handle_stamps_embedder_id_and_cycle(db_with_article):
    bus = _FakeBus()
    embedder = _FakeEmbedder(embedder_id_value="probe-model@4")
    stage = _make_stage(
        db_with_article,
        bus,
        _FakeIndex(next_candidates=[("m1", 0.8)]),
        embedder=embedder,
    )
    with cycle_scope(42):
        await stage.handle({"article_id": 1, "cluster_id": None, "freshness": "BREAKING"})
    cur = await db_with_article.conn.execute(
        "SELECT embedder_id, cycle_id FROM signals WHERE market_id='m1'"
    )
    row = await cur.fetchone()
    assert row["embedder_id"] == "probe-model@4"
    assert row["cycle_id"] == 42
    await cur.close()


async def test_handle_threads_tracer_flag(db_with_article):
    bus = _FakeBus()
    stage = _make_stage(db_with_article, bus, _FakeIndex(next_candidates=[("m1", 0.8)]))
    with cycle_scope(1):
        await stage.handle(
            {"article_id": 1, "cluster_id": None, "freshness": "BREAKING",
             "is_tracer": True, "tracer_id": "t-99"}
        )
    cur = await db_with_article.conn.execute(
        "SELECT is_tracer, tracer_id FROM signals WHERE market_id='m1'"
    )
    row = await cur.fetchone()
    assert row["is_tracer"] == 1
    assert row["tracer_id"] == "t-99"
    await cur.close()
    assert bus.published[0][1]["is_tracer"] is True
    assert bus.published[0][1]["tracer_id"] == "t-99"


async def test_handle_unset_cycle_raises(db_with_article):
    bus = _FakeBus()
    stage = _make_stage(db_with_article, bus, _FakeIndex(next_candidates=[("m1", 0.8)]))
    # No cycle_scope wrapping — must LookupError.
    with pytest.raises(LookupError):
        await stage.handle({"article_id": 1, "cluster_id": None, "freshness": "BREAKING"})


# ---------------------------------------------------------------------------
# decision_inputs capture (p1_contracts §7)
# ---------------------------------------------------------------------------


async def test_capture_row_written_alongside_signal(db_with_article):
    import json
    bus = _FakeBus()
    stage = _make_stage(db_with_article, bus, _FakeIndex(next_candidates=[("m-cap", 0.77)]))
    with cycle_scope(5):
        await stage.handle(
            {"article_id": 1, "cluster_id": 3, "freshness": "BREAKING",
             "is_tracer": True, "tracer_id": "trace-cap"}
        )
    cur = await db_with_article.conn.execute(
        "SELECT kind, ref_id, payload_json, snapshot_json, cycle_id, is_tracer, tracer_id "
        "FROM decision_inputs"
    )
    rows = await cur.fetchall()
    await cur.close()
    assert len(rows) == 1
    row = rows[0]
    assert row["kind"] == "signal"
    assert row["cycle_id"] == 5
    assert row["is_tracer"] == 1
    assert row["tracer_id"] == "trace-cap"
    payload = json.loads(row["payload_json"])
    assert payload["cluster_id"] == 3
    assert payload["freshness"] == "BREAKING"
    snapshot = json.loads(row["snapshot_json"])
    assert snapshot["market_id"] == "m-cap"
    assert snapshot["title"] == "Headline one"
    assert snapshot["similarity"] == 0.77
    assert snapshot["embedder_id"] == "fake-model@4"


async def test_capture_idempotent_on_duplicate_signal(db_with_article):
    bus = _FakeBus()
    stage = _make_stage(db_with_article, bus, _FakeIndex(next_candidates=[("m-dup", 0.8)]))
    payload = {"article_id": 1, "cluster_id": 1, "freshness": "BREAKING",
               "is_tracer": False, "tracer_id": None}
    with cycle_scope(1):
        await stage.handle(payload)
        await stage.handle(payload)  # second call's UNIQUE conflict short-circuits
    cur = await db_with_article.conn.execute(
        "SELECT COUNT(*) FROM decision_inputs WHERE kind='signal'"
    )
    assert (await cur.fetchone())[0] == 1
    await cur.close()


async def test_capture_rolls_back_if_signal_insert_does_not_fire(db_with_article):
    """When INSERT OR IGNORE collides, we should NOT write a capture row for
    the second collision attempt — they share a batch but the lastrowid is
    undefined and there's no signal to anchor the capture to."""
    bus = _FakeBus()
    stage = _make_stage(db_with_article, bus, _FakeIndex(next_candidates=[("m1", 0.8)]))
    payload = {"article_id": 1, "cluster_id": 1, "freshness": "BREAKING"}
    with cycle_scope(1):
        await stage.handle(payload)
    # Manually insert a colliding signals row to force the second
    # _insert_signal_with_capture call to short-circuit at the rowcount
    # gate.
    cur = await db_with_article.conn.execute(
        "SELECT COUNT(*) FROM decision_inputs WHERE kind='signal'"
    )
    before = (await cur.fetchone())[0]
    await cur.close()
    with cycle_scope(1):
        await stage.handle(payload)
    cur = await db_with_article.conn.execute(
        "SELECT COUNT(*) FROM decision_inputs WHERE kind='signal'"
    )
    after = (await cur.fetchone())[0]
    await cur.close()
    assert before == after == 1, "no extra capture rows on UNIQUE conflict"
