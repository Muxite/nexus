"""MarketsStage tests — DDL registration, the Gamma poll + upsert path,
disappearance → deactivate, embedding refresh + embedder_id stamping,
MarketIndex top-k, and the poller `handle()` contract.

Everything network-bound is mocked at the stage's helpers / the embedder.
No real httpx, no torch."""

from __future__ import annotations

import json
from typing import Awaitable, Callable, Optional

import numpy as np
import pytest

# Importing app.stages.markets registers the markets DDL with db.py.
import app.stages.markets  # noqa: F401
# main.py owns the cycles DDL; importing it keeps DB.connect() symmetrical with
# the rest of the suite even though MarketsStage itself never reads cycle_id.
import app.main  # noqa: F401
from app.db import DB
from app.deps import StageDeps
from app.stages.markets import (
    MarketIndex,
    MarketIndexEmbedderMismatch,
    MarketsStage,
    _parse_gamma_row,
)
from app.topics import TOPIC_MARKETS_REFRESHED


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeBus:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict]] = []

    async def publish(self, topic: str, payload: dict) -> None:
        self.published.append((topic, payload))

    def subscribe(self, topic: str, handler: Callable[[dict], Awaitable[None]]) -> None:
        pass

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


class _FakeEmbedder:
    """Deterministic mock embedder: encodes the first 4 chars of each text
    as a unique unit vector so order is preserved and rows are
    distinguishable in tests."""

    def __init__(self, dim: int = 8, embedder_id: str = "fake@8") -> None:
        self._dim = dim
        self._id = embedder_id
        self.encoded: list[str] = []

    @property
    def embedder_id(self) -> str:
        return self._id

    def embed(self, text: str) -> np.ndarray:
        return self._vec(text)

    def embed_batch(self, texts: list[str]) -> np.ndarray:
        self.encoded.extend(texts)
        if not texts:
            return np.zeros((0, self._dim), dtype=np.float32)
        return np.stack([self._vec(t) for t in texts]).astype(np.float32)

    def _vec(self, text: str) -> np.ndarray:
        v = np.zeros(self._dim, dtype=np.float32)
        # Hash the text into a slot; bump value so different texts → different vecs.
        slot = (sum(ord(c) for c in text) % self._dim)
        v[slot] = 1.0
        return v


def _config(dim: int = 8) -> object:
    class _Emb:
        # Mirrors the EmbedderSection surface MarketsStage reads.
        dim = 8

    class _Cfg:
        embedder = _Emb()

    _Emb.dim = dim
    return _Cfg()


def _gamma_row(condition_id: str, question: str, **overrides) -> dict:
    row = {
        "conditionId": condition_id,
        "question": question,
        "slug": condition_id.lower(),
        "description": f"desc for {condition_id}",
        "category": "Politics",
        "endDate": "2026-12-31T23:59:59Z",
        "volume": 1234.5,
        # Production Gamma returns clobTokenIds as a JSON-encoded *string*.
        "clobTokenIds": json.dumps([f"{condition_id}-yes", f"{condition_id}-no"]),
    }
    row.update(overrides)
    return row


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def db_ready():
    db = DB.ephemeral()
    await db.connect()
    try:
        yield db
    finally:
        await db.close()


@pytest.fixture
def bus() -> _FakeBus:
    return _FakeBus()


@pytest.fixture
def embedder() -> _FakeEmbedder:
    return _FakeEmbedder()


def _make_stage(
    db: DB,
    bus: _FakeBus,
    embedder: _FakeEmbedder,
    dim: int = 8,
) -> MarketsStage:
    deps = StageDeps(db=db, bus=bus, config=_config(dim=dim), embedder=embedder)
    return MarketsStage(deps)


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def test_parse_gamma_row_happy():
    row = _parse_gamma_row(_gamma_row("0xabc", "Will X happen?"))
    assert row is not None
    assert row["condition_id"] == "0xabc"
    assert row["question"] == "Will X happen?"
    assert row["token_yes"] == "0xabc-yes"
    assert row["token_no"] == "0xabc-no"
    assert row["volume_usdc"] == 1234.5
    assert row["category"] == "Politics"


def test_parse_gamma_row_rejects_missing_fields():
    assert _parse_gamma_row({"question": "X"}) is None       # no conditionId
    assert _parse_gamma_row({"conditionId": "0x1"}) is None  # no question


def test_parse_gamma_row_handles_native_clob_list():
    raw = _gamma_row("0xc", "Q")
    raw["clobTokenIds"] = ["a", "b"]  # not stringified
    row = _parse_gamma_row(raw)
    assert row["token_yes"] == "a"
    assert row["token_no"] == "b"


# ---------------------------------------------------------------------------
# DDL registration
# ---------------------------------------------------------------------------


async def test_markets_table_exists_after_connect(db_ready):
    cur = await db_ready.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='markets'"
    )
    assert (await cur.fetchone()) is not None

    cur = await db_ready.conn.execute("PRAGMA table_info(markets)")
    cols = {row["name"] for row in await cur.fetchall()}
    expected = {
        "condition_id", "slug", "question", "description", "category",
        "end_date", "volume_usdc", "token_yes", "token_no",
        "embedding", "embedder_id", "embedding_dim", "embedding_updated_at",
        "active", "closed_at", "first_seen_at", "updated_at",
    }
    assert expected.issubset(cols), f"missing cols: {expected - cols}"


async def test_markets_indexes_exist_after_connect(db_ready):
    cur = await db_ready.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='markets'"
    )
    names = {r["name"] for r in await cur.fetchall()}
    assert "idx_markets_active" in names
    assert "idx_markets_closed_at" in names


# ---------------------------------------------------------------------------
# Stage lifecycle
# ---------------------------------------------------------------------------


async def test_init_requires_embedder(db_ready, bus):
    deps = StageDeps(db=db_ready, bus=bus, config=_config(), embedder=None)
    with pytest.raises(RuntimeError, match="embedder"):
        MarketsStage(deps)


async def test_handle_raises_not_implemented(db_ready, bus, embedder):
    stage = _make_stage(db_ready, bus, embedder)
    with pytest.raises(NotImplementedError):
        await stage.handle({"any": "payload"})


# ---------------------------------------------------------------------------
# Gamma poll happy path + deactivate-on-disappear
# ---------------------------------------------------------------------------


async def test_poll_once_inserts_rows_and_publishes_topic(db_ready, bus, embedder, monkeypatch):
    stage = _make_stage(db_ready, bus, embedder)

    async def fake_fetch(self):
        return [_gamma_row("0x1", "Q1"), _gamma_row("0x2", "Q2")]

    monkeypatch.setattr(MarketsStage, "_fetch_all_gamma", fake_fetch)

    n = await stage.poll_once()
    assert n == 2

    cur = await db_ready.conn.execute(
        "SELECT condition_id, question, active, token_yes FROM markets ORDER BY condition_id"
    )
    rows = await cur.fetchall()
    assert [r["condition_id"] for r in rows] == ["0x1", "0x2"]
    assert all(r["active"] == 1 for r in rows)
    assert rows[0]["token_yes"] == "0x1-yes"

    assert len(bus.published) == 1
    topic, payload = bus.published[0]
    assert topic == TOPIC_MARKETS_REFRESHED
    assert payload["count"] == 2
    assert "refreshed_at" in payload


async def test_disappearing_market_is_deactivated(db_ready, bus, embedder, monkeypatch):
    stage = _make_stage(db_ready, bus, embedder)

    fetched = [_gamma_row("0xA", "QA"), _gamma_row("0xB", "QB")]

    async def fake_fetch(self):
        return list(fetched)

    monkeypatch.setattr(MarketsStage, "_fetch_all_gamma", fake_fetch)
    await stage.poll_once()

    # Now 0xB disappears from the open list.
    fetched.pop()  # drop 0xB
    await stage.poll_once()

    cur = await db_ready.conn.execute(
        "SELECT condition_id, active, closed_at FROM markets ORDER BY condition_id"
    )
    rows = await cur.fetchall()
    by_id = {r["condition_id"]: r for r in rows}
    assert by_id["0xA"]["active"] == 1
    assert by_id["0xA"]["closed_at"] is None
    assert by_id["0xB"]["active"] == 0
    assert by_id["0xB"]["closed_at"] is not None


async def test_network_error_keeps_loop_alive(db_ready, bus, embedder, monkeypatch, caplog):
    import httpx
    stage = _make_stage(db_ready, bus, embedder)

    async def fake_fetch(self):
        raise httpx.ConnectError("boom")

    monkeypatch.setattr(MarketsStage, "_fetch_all_gamma", fake_fetch)

    n = await stage.poll_once()
    assert n == 0
    # No bus publish on a failed cycle.
    assert bus.published == []
    # No rows inserted.
    cur = await db_ready.conn.execute("SELECT COUNT(*) FROM markets")
    assert (await cur.fetchone())[0] == 0


# ---------------------------------------------------------------------------
# Embedding refresh
# ---------------------------------------------------------------------------


async def test_refresh_embeddings_writes_embedder_id_and_blob(db_ready, bus, embedder, monkeypatch):
    stage = _make_stage(db_ready, bus, embedder)

    async def fake_fetch(self):
        return [_gamma_row("0x1", "Question one"), _gamma_row("0x2", "Question two")]

    monkeypatch.setattr(MarketsStage, "_fetch_all_gamma", fake_fetch)
    await stage.poll_once()

    n = await stage.refresh_embeddings_once()
    assert n == 2
    assert sorted(embedder.encoded) == ["Question one", "Question two"]

    cur = await db_ready.conn.execute(
        "SELECT condition_id, embedder_id, embedding_dim, embedding "
        "FROM markets ORDER BY condition_id"
    )
    rows = await cur.fetchall()
    assert all(r["embedder_id"] == "fake@8" for r in rows)
    assert all(r["embedding_dim"] == 8 for r in rows)
    assert all(len(r["embedding"]) == 8 * 4 for r in rows)  # 8 floats × 4 bytes


async def test_refresh_skips_rows_already_at_current_embedder_id(db_ready, bus, embedder, monkeypatch):
    stage = _make_stage(db_ready, bus, embedder)

    async def fake_fetch(self):
        return [_gamma_row("0x1", "Q1")]

    monkeypatch.setattr(MarketsStage, "_fetch_all_gamma", fake_fetch)
    await stage.poll_once()

    n_first = await stage.refresh_embeddings_once()
    assert n_first == 1
    embedder.encoded.clear()

    # Second pass: the row is already stamped with the current embedder_id,
    # so the refresh must short-circuit without re-embedding.
    n_second = await stage.refresh_embeddings_once()
    assert n_second == 0
    assert embedder.encoded == []


async def test_refresh_reembeds_rows_with_stale_embedder_id(db_ready, bus, embedder, monkeypatch):
    stage = _make_stage(db_ready, bus, embedder)

    async def fake_fetch(self):
        return [_gamma_row("0x1", "Q1")]

    monkeypatch.setattr(MarketsStage, "_fetch_all_gamma", fake_fetch)
    await stage.poll_once()
    await stage.refresh_embeddings_once()

    # Simulate a model rotation: swap in a fresh embedder with a new id.
    new_emb = _FakeEmbedder(dim=8, embedder_id="rotated@8")
    stage._embedder = new_emb

    n = await stage.refresh_embeddings_once()
    assert n == 1
    assert new_emb.encoded == ["Q1"]

    cur = await db_ready.conn.execute(
        "SELECT embedder_id FROM markets WHERE condition_id='0x1'"
    )
    row = await cur.fetchone()
    assert row["embedder_id"] == "rotated@8"


# ---------------------------------------------------------------------------
# MarketIndex
# ---------------------------------------------------------------------------


async def _populate(db: DB, items: list[tuple[str, np.ndarray, str]], active: int = 1) -> None:
    """Insert rows (condition_id, embedding, embedder_id) into the markets table
    bypassing the stage. Lets MarketIndex tests run without the full poll path."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    async with db.batch() as cur:
        for cid, vec, eid in items:
            await cur.execute(
                """
                INSERT INTO markets (condition_id, question, embedding, embedder_id,
                                     embedding_dim, embedding_updated_at,
                                     active, first_seen_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (cid, f"q-{cid}", vec.astype(np.float32).tobytes(), eid,
                 int(vec.shape[0]), now, active, now, now),
            )


async def test_market_index_topk_ordering(db_ready, embedder):
    # Three unit vectors aligned to distinct axes; query closest to axis 0.
    v0 = np.zeros(8, dtype=np.float32); v0[0] = 1.0
    v1 = np.zeros(8, dtype=np.float32); v1[1] = 1.0
    v2 = np.zeros(8, dtype=np.float32); v2[0] = 0.7; v2[1] = 0.3  # not unit; mid-similarity to v0
    await _populate(
        db_ready,
        [("c0", v0, "fake@8"), ("c1", v1, "fake@8"), ("c2", v2, "fake@8")],
    )
    idx = MarketIndex(embedder, dim=8)
    n = await idx.load(db_ready)
    assert n == 3

    results = idx.topk(v0, k=10, floor=0.0)
    assert [cid for cid, _ in results] == ["c0", "c2", "c1"]
    assert results[0][1] == pytest.approx(1.0)
    assert results[2][1] == pytest.approx(0.0)


async def test_market_index_floor_filters(db_ready, embedder):
    v0 = np.zeros(8, dtype=np.float32); v0[0] = 1.0
    v1 = np.zeros(8, dtype=np.float32); v1[1] = 1.0
    await _populate(db_ready, [("c0", v0, "fake@8"), ("c1", v1, "fake@8")])
    idx = MarketIndex(embedder, dim=8)
    await idx.load(db_ready)

    results = idx.topk(v0, k=10, floor=0.5)
    assert results == [("c0", pytest.approx(1.0))]


async def test_market_index_k_limit(db_ready, embedder):
    vecs = []
    for i in range(5):
        v = np.zeros(8, dtype=np.float32); v[0] = 1.0 - i * 0.1
        vecs.append((f"c{i}", v, "fake@8"))
    await _populate(db_ready, vecs)
    idx = MarketIndex(embedder, dim=8)
    await idx.load(db_ready)

    q = np.zeros(8, dtype=np.float32); q[0] = 1.0
    results = idx.topk(q, k=3, floor=0.0)
    assert len(results) == 3
    assert [cid for cid, _ in results] == ["c0", "c1", "c2"]


async def test_market_index_empty_returns_empty(db_ready, embedder):
    idx = MarketIndex(embedder, dim=8)
    n = await idx.load(db_ready)
    assert n == 0
    q = np.zeros(8, dtype=np.float32); q[0] = 1.0
    assert idx.topk(q, k=5, floor=0.0) == []


async def test_market_index_mixed_embedder_id_raises(db_ready, embedder):
    v0 = np.zeros(8, dtype=np.float32); v0[0] = 1.0
    v1 = np.zeros(8, dtype=np.float32); v1[1] = 1.0
    # Two rows tagged with different embedder_id values — the in-flight
    # rotation state. load() must refuse to mix them.
    await _populate(db_ready, [("c0", v0, "modelA@8"), ("c1", v1, "modelB@8")])
    idx = MarketIndex(embedder, dim=8)
    with pytest.raises(MarketIndexEmbedderMismatch):
        await idx.load(db_ready)


async def test_market_index_skips_inactive_rows(db_ready, embedder):
    v0 = np.zeros(8, dtype=np.float32); v0[0] = 1.0
    v1 = np.zeros(8, dtype=np.float32); v1[1] = 1.0
    await _populate(db_ready, [("c0", v0, "fake@8")], active=1)
    await _populate(db_ready, [("c1", v1, "fake@8")], active=0)
    idx = MarketIndex(embedder, dim=8)
    n = await idx.load(db_ready)
    assert n == 1
    results = idx.topk(v1, k=10, floor=0.0)
    assert [cid for cid, _ in results] == ["c0"]
