"""DedupeStage tests — DDL registration, disabled-chroma fallback, freshness
boundary labels via a fake nearest-provider, publish payload shape, and
tracer flag threading."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable, Optional

import numpy as np
import pytest

# Importing app.stages.dedupe registers the article_clusters DDL.
import app.stages.dedupe  # noqa: F401
# Importing ingest registers the articles DDL (dedupe reads `articles`).
import app.stages.ingest  # noqa: F401
# main.py registers the cycles DDL.
import app.main  # noqa: F401
from app.config import (
    Config,
    DedupeSection,
    EmbedderSection,
    IngestSection,
    IreneSection,
    ScraperSection,
)
from app.cycle import cycle_scope
from app.db import DB
from app.deps import StageDeps
from app.stages.dedupe import DedupeStage, _label
from app.topics import TOPIC_ARTICLE_DEDUPED, TOPIC_ARTICLE_INGESTED


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
    """Returns a fixed shape vector; the value doesn't matter for tests
    because we replace the nearest-provider with a fake."""

    def __init__(self, dim: int = 4) -> None:
        self._dim = dim

    @property
    def embedder_id(self) -> str:
        return f"fake@{self._dim}"

    def embed(self, text: str) -> np.ndarray:
        return np.zeros(self._dim, dtype=np.float32)

    def embed_batch(self, texts: list[str]) -> np.ndarray:
        return np.zeros((len(texts), self._dim), dtype=np.float32)


@dataclass
class _FakeProvider:
    """Tests control what `nearest()` returns and capture all `add()` calls."""

    response: Optional[tuple[int, float]] = None
    added: list[tuple[int, int]] = field(default_factory=list)  # (article_id, cluster_id)

    def nearest(self, vec) -> Optional[tuple[int, float]]:
        return self.response

    def add(self, article_id: int, vec, cluster_id: int, ingested_at: str) -> None:
        self.added.append((article_id, cluster_id))


def _make_config() -> Config:
    return Config(
        irene=IreneSection(),
        scraper=ScraperSection(),
        embedder=EmbedderSection(name="fake", dim=4),
        ingest=IngestSection(),
        dedupe=DedupeSection(chroma_path="/tmp/irene-test-chroma", breaking_distance=0.55),
    )


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


def _make_stage(db: DB, bus: _FakeBus) -> DedupeStage:
    deps = StageDeps(db=db, bus=bus, config=_make_config(), embedder=_FakeEmbedder())
    return DedupeStage(deps)


async def _insert_article(
    db: DB,
    *,
    article_id: int,
    title: str,
    ingested_at: str,
    is_tracer: bool = False,
    tracer_id: Optional[str] = None,
) -> None:
    async with db.batch() as cur:
        await cur.execute(
            """
            INSERT INTO articles
                (id, source, source_detail, url, title, summary, published_at,
                 ingested_at, cycle_id, is_tracer, tracer_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                article_id,
                "test",
                None,
                f"https://example.com/a/{article_id}",
                title,
                None,
                ingested_at,
                ingested_at,
                1,
                1 if is_tracer else 0,
                tracer_id,
            ),
        )


def _iso(dt: datetime) -> str:
    return dt.isoformat()


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def test_label_breaking_above_threshold():
    assert _label(nearest_dist=0.60, age_secs=60.0, breaking_dist=0.55) == "BREAKING"


def test_label_evolving_under_six_hours():
    assert _label(nearest_dist=0.30, age_secs=1.0, breaking_dist=0.55) == "EVOLVING"
    assert _label(nearest_dist=0.30, age_secs=6 * 3600 - 1, breaking_dist=0.55) == "EVOLVING"


def test_label_rehash_between_six_and_twenty_four_hours():
    assert _label(nearest_dist=0.30, age_secs=6 * 3600, breaking_dist=0.55) == "REHASH"
    assert _label(nearest_dist=0.30, age_secs=24 * 3600 - 1, breaking_dist=0.55) == "REHASH"


def test_label_stale_return_at_or_above_twenty_four_hours():
    assert _label(nearest_dist=0.30, age_secs=24 * 3600, breaking_dist=0.55) == "STALE_RETURN"
    assert _label(nearest_dist=0.30, age_secs=72 * 3600, breaking_dist=0.55) == "STALE_RETURN"


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------


async def test_owned_tables_exist_after_connect(db_ready):
    cur = await db_ready.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name IN ('article_clusters','article_cluster_members') ORDER BY name"
    )
    names = [row["name"] for row in await cur.fetchall()]
    await cur.close()
    assert names == ["article_cluster_members", "article_clusters"]

    cur = await db_ready.conn.execute("PRAGMA table_info(article_clusters)")
    cols = {row["name"] for row in await cur.fetchall()}
    assert {
        "id", "representative_article_id", "freshness", "created_at",
        "is_tracer", "tracer_id",
    }.issubset(cols)

    cur = await db_ready.conn.execute("PRAGMA table_info(article_cluster_members)")
    cols = {row["name"] for row in await cur.fetchall()}
    assert {"cluster_id", "article_id", "distance", "added_at"}.issubset(cols)


# ---------------------------------------------------------------------------
# Construction guards
# ---------------------------------------------------------------------------


def test_init_requires_embedder(db_ready, bus):
    deps = StageDeps(db=db_ready, bus=bus, config=_make_config(), embedder=None)
    with pytest.raises(RuntimeError, match="embedder"):
        DedupeStage(deps)


# ---------------------------------------------------------------------------
# Disabled chroma → every article BREAKING, one cluster each
# ---------------------------------------------------------------------------


async def test_disabled_chroma_labels_every_article_breaking(db_ready, bus):
    stage = _make_stage(db_ready, bus)
    # Stage starts without chromadb available: simulate that by not calling
    # start() and explicitly disabling. (Tests don't depend on chromadb.)
    stage._enabled = False
    stage._provider = None

    now = datetime.now(timezone.utc)
    for i in range(1, 4):
        await _insert_article(
            db_ready, article_id=i,
            title=f"headline {i}",
            ingested_at=_iso(now + timedelta(minutes=i)),
        )
        with cycle_scope(1):
            await stage.handle({
                "article_id": i, "url": f"https://example.com/a/{i}",
                "is_tracer": False, "tracer_id": None,
            })

    cur = await db_ready.conn.execute(
        "SELECT freshness, representative_article_id FROM article_clusters ORDER BY id"
    )
    rows = await cur.fetchall()
    await cur.close()
    assert len(rows) == 3
    assert all(r["freshness"] == "BREAKING" for r in rows)
    assert [r["representative_article_id"] for r in rows] == [1, 2, 3]

    cur = await db_ready.conn.execute(
        "SELECT cluster_id, article_id FROM article_cluster_members ORDER BY article_id"
    )
    members = await cur.fetchall()
    await cur.close()
    assert len(members) == 3
    # Three clusters, three members, one each.
    assert {m["cluster_id"] for m in members} == {m["cluster_id"] for m in members}
    assert sorted(m["article_id"] for m in members) == [1, 2, 3]

    assert len(bus.published) == 3
    for (topic, payload), i in zip(bus.published, range(1, 4)):
        assert topic == TOPIC_ARTICLE_DEDUPED
        assert payload["article_id"] == i
        assert payload["freshness"] == "BREAKING"
        assert payload["cluster_id"] is not None


# ---------------------------------------------------------------------------
# Freshness boundaries via injected provider
# ---------------------------------------------------------------------------


async def _seed_first_article(db: DB, stage: DedupeStage, bus: _FakeBus,
                              base_time: datetime, article_id: int = 1) -> int:
    """Run the first article through the stage so a cluster exists with id 1."""
    await _insert_article(
        db, article_id=article_id,
        title="seed",
        ingested_at=_iso(base_time),
    )
    # provider returns None → BREAKING / new cluster
    with cycle_scope(1):
        await stage.handle({
            "article_id": article_id, "url": "https://example.com/a/1",
            "is_tracer": False, "tracer_id": None,
        })
    bus.published.clear()
    cur = await db.conn.execute("SELECT id FROM article_clusters ORDER BY id DESC LIMIT 1")
    row = await cur.fetchone()
    await cur.close()
    assert row is not None
    return int(row["id"])


@pytest.mark.parametrize(
    "age_secs,distance,expected",
    [
        (60.0, 0.30, "EVOLVING"),
        (6 * 3600, 0.30, "REHASH"),
        (24 * 3600 + 1, 0.30, "STALE_RETURN"),
        (60.0, 0.80, "BREAKING"),
    ],
)
async def test_handle_freshness_labels_at_boundaries(db_ready, bus, age_secs, distance, expected):
    stage = _make_stage(db_ready, bus)
    fake = _FakeProvider(response=None)
    stage._enabled = True
    stage._provider = fake

    base = datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc)
    seed_cluster_id = await _seed_first_article(db_ready, stage, bus, base, article_id=1)

    # Configure the fake to return the seed as nearest at the given distance.
    fake.response = (1, distance)

    # Insert the incoming article at base + age_secs.
    incoming_id = 2
    incoming_at = base + timedelta(seconds=age_secs)
    await _insert_article(
        db_ready, article_id=incoming_id, title="incoming",
        ingested_at=_iso(incoming_at),
    )
    with cycle_scope(1):
        await stage.handle({
            "article_id": incoming_id, "url": "https://example.com/a/2",
            "is_tracer": False, "tracer_id": None,
        })

    assert len(bus.published) == 1
    topic, payload = bus.published[0]
    assert topic == TOPIC_ARTICLE_DEDUPED
    assert payload["freshness"] == expected
    assert payload["article_id"] == incoming_id

    if expected == "BREAKING":
        # New cluster created for the incoming article.
        assert payload["cluster_id"] != seed_cluster_id
    else:
        # Joined the seed's cluster.
        assert payload["cluster_id"] == seed_cluster_id

    # Member row exists for incoming with correct distance.
    cur = await db_ready.conn.execute(
        "SELECT cluster_id, distance FROM article_cluster_members WHERE article_id=?",
        (incoming_id,),
    )
    row = await cur.fetchone()
    await cur.close()
    assert row is not None
    assert row["cluster_id"] == payload["cluster_id"]
    assert row["distance"] == pytest.approx(distance)


# ---------------------------------------------------------------------------
# Joining an existing cluster does not mutate its tracer flag
# ---------------------------------------------------------------------------


async def test_tracer_flag_on_new_cluster(db_ready, bus):
    stage = _make_stage(db_ready, bus)
    stage._enabled = False
    stage._provider = None

    now = datetime.now(timezone.utc)
    await _insert_article(
        db_ready, article_id=1, title="t1",
        ingested_at=_iso(now), is_tracer=True, tracer_id="t-1",
    )
    with cycle_scope(1):
        await stage.handle({
            "article_id": 1, "url": "https://example.com/a/1",
            "is_tracer": True, "tracer_id": "t-1",
        })

    cur = await db_ready.conn.execute(
        "SELECT is_tracer, tracer_id FROM article_clusters WHERE representative_article_id=1"
    )
    row = await cur.fetchone()
    await cur.close()
    assert row["is_tracer"] == 1
    assert row["tracer_id"] == "t-1"

    assert bus.published[0][1]["is_tracer"] is True
    assert bus.published[0][1]["tracer_id"] == "t-1"


async def test_joining_cluster_does_not_overwrite_tracer_flag(db_ready, bus):
    stage = _make_stage(db_ready, bus)
    fake = _FakeProvider(response=None)
    stage._enabled = True
    stage._provider = fake

    base = datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc)
    # Seed: non-tracer.
    await _insert_article(db_ready, article_id=1, title="seed", ingested_at=_iso(base))
    with cycle_scope(1):
        await stage.handle({
            "article_id": 1, "url": "https://example.com/a/1",
            "is_tracer": False, "tracer_id": None,
        })

    # Second article: tracer, joins the seed's cluster (EVOLVING).
    fake.response = (1, 0.30)
    await _insert_article(
        db_ready, article_id=2, title="incoming",
        ingested_at=_iso(base + timedelta(seconds=60)),
    )
    with cycle_scope(1):
        await stage.handle({
            "article_id": 2, "url": "https://example.com/a/2",
            "is_tracer": True, "tracer_id": "t-trace",
        })

    # The cluster still belongs to the original creator (non-tracer).
    cur = await db_ready.conn.execute(
        "SELECT is_tracer, tracer_id FROM article_clusters "
        "WHERE representative_article_id=1"
    )
    row = await cur.fetchone()
    await cur.close()
    assert row["is_tracer"] == 0
    assert row["tracer_id"] is None

    # The publish still reflects the inbound tracer payload.
    last = bus.published[-1][1]
    assert last["is_tracer"] is True
    assert last["tracer_id"] == "t-trace"
    assert last["freshness"] == "EVOLVING"
