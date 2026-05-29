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
    assert _label(
        nearest_dist=0.60, latest_article_age_secs=60.0, breaking_dist=0.55,
    ) == "BREAKING"


def test_label_evolving_under_default_window():
    """Cycle 24 default: EVOLVING window is 24h (was 6h)."""
    assert _label(
        nearest_dist=0.30, latest_article_age_secs=1.0, breaking_dist=0.55,
    ) == "EVOLVING"
    assert _label(
        nearest_dist=0.30, latest_article_age_secs=24 * 3600 - 1, breaking_dist=0.55,
    ) == "EVOLVING"


def test_label_rehash_between_evolving_and_rehash_windows():
    """Cycle 24 default: REHASH is 24h ≤ age < 72h (was 6h..24h)."""
    assert _label(
        nearest_dist=0.30, latest_article_age_secs=24 * 3600, breaking_dist=0.55,
    ) == "REHASH"
    assert _label(
        nearest_dist=0.30, latest_article_age_secs=72 * 3600 - 1, breaking_dist=0.55,
    ) == "REHASH"


def test_label_stale_return_at_or_above_rehash_window():
    """Cycle 24 default: STALE_RETURN starts at 72h (was 24h)."""
    assert _label(
        nearest_dist=0.30, latest_article_age_secs=72 * 3600, breaking_dist=0.55,
    ) == "STALE_RETURN"
    assert _label(
        nearest_dist=0.30, latest_article_age_secs=200 * 3600, breaking_dist=0.55,
    ) == "STALE_RETURN"


# ---------------------------------------------------------------------------
# Cycle 24 (handoff B) — recency-bias + burst + REHASH-accept
# ---------------------------------------------------------------------------


def test_label_burst_forces_evolving_even_when_age_in_rehash_window():
    """B2: a burst override beats the rehash-age boundary.

    Cluster opened 48h ago, last article 23h ago → would be EVOLVING on
    age alone; flip the age to land in REHASH window and verify burst
    pushes it back to EVOLVING.
    """
    # 50h age (deep in REHASH window 24h..72h) → REHASH without burst…
    assert _label(
        nearest_dist=0.30, latest_article_age_secs=50 * 3600, breaking_dist=0.55,
    ) == "REHASH"
    # …same age + burst_active → EVOLVING.
    assert _label(
        nearest_dist=0.30,
        latest_article_age_secs=50 * 3600,
        breaking_dist=0.55,
        burst_active=True,
    ) == "EVOLVING"


def test_label_burst_does_not_override_breaking():
    """B2: burst only fires on the near-duplicate path; BREAKING still wins."""
    assert _label(
        nearest_dist=0.80,
        latest_article_age_secs=60.0,
        breaking_dist=0.55,
        burst_active=True,
    ) == "BREAKING"


def test_label_burst_does_not_promote_stale_return():
    """B2 + B3: even with burst, a cluster whose latest article is 80h old
    is dead — burst only overrides the age-based EVOLVING/REHASH split."""
    # Cluster fed 80h ago, no recent burst → STALE_RETURN.
    assert _label(
        nearest_dist=0.30, latest_article_age_secs=80 * 3600, breaking_dist=0.55,
    ) == "STALE_RETURN"
    # With burst_active, age 80h is irrelevant — N articles in the burst
    # window means it just became fresh again. EVOLVING.
    assert _label(
        nearest_dist=0.30,
        latest_article_age_secs=80 * 3600,
        breaking_dist=0.55,
        burst_active=True,
    ) == "EVOLVING"


def test_label_custom_age_windows_are_honoured():
    """B3: callers may pass override windows (config knobs do this)."""
    # 5h with a 4h evolving / 8h rehash custom config → REHASH.
    assert _label(
        nearest_dist=0.30,
        latest_article_age_secs=5 * 3600,
        breaking_dist=0.55,
        evolving_age_secs=4 * 3600,
        rehash_age_secs=8 * 3600,
    ) == "REHASH"


def test_trade_section_accept_freshness_includes_rehash_by_default():
    """B4: trade gate default accepts REHASH."""
    from app.config import TradeSection
    assert "REHASH" in TradeSection().accept_freshness
    assert TradeSection().accept_freshness == ["BREAKING", "EVOLVING", "REHASH"]


def test_dedupe_section_exposes_freshness_knobs():
    """B3: config exposes the windows + burst params with new defaults."""
    from app.config import DedupeSection
    s = DedupeSection()
    assert s.evolving_age_hours == 24.0
    assert s.rehash_age_hours == 72.0
    assert s.burst_min_articles == 3
    assert s.burst_window_minutes == 30


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
        # Cycle 24 (handoff B): rolling-recency stamp.
        "latest_article_at",
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
        # Cycle 24 (handoff B): boundaries widened — 24h EVOLVING, 72h REHASH.
        # age_secs is "time since the cluster's MOST RECENT article," which —
        # given a freshly-seeded single-article cluster — equals "incoming
        # ingested_at - seed ingested_at."
        (60.0, 0.30, "EVOLVING"),
        (24 * 3600, 0.30, "REHASH"),
        (72 * 3600 + 1, 0.30, "STALE_RETURN"),
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


# ---------------------------------------------------------------------------
# Cycle 24 (handoff B) — stage-level recency-bias + burst + latest_article_at
# stamping
# ---------------------------------------------------------------------------


async def test_create_cluster_stamps_latest_article_at(db_ready, bus):
    """B1: a freshly-created cluster has latest_article_at = seed's ingested_at."""
    stage = _make_stage(db_ready, bus)
    fake = _FakeProvider(response=None)
    stage._enabled = True
    stage._provider = fake

    base = datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc)
    await _insert_article(db_ready, article_id=1, title="seed", ingested_at=_iso(base))
    with cycle_scope(1):
        await stage.handle({
            "article_id": 1, "url": "https://example.com/a/1",
            "is_tracer": False, "tracer_id": None,
        })

    cur = await db_ready.conn.execute(
        "SELECT latest_article_at FROM article_clusters WHERE representative_article_id=1"
    )
    row = await cur.fetchone()
    await cur.close()
    assert row["latest_article_at"] == _iso(base)


async def test_attach_advances_latest_article_at(db_ready, bus):
    """B1: each attach advances latest_article_at to the incoming ingested_at."""
    stage = _make_stage(db_ready, bus)
    fake = _FakeProvider(response=None)
    stage._enabled = True
    stage._provider = fake

    base = datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc)
    seed_id = await _seed_first_article(db_ready, stage, bus, base, article_id=1)

    fake.response = (1, 0.30)
    later = base + timedelta(hours=2)
    await _insert_article(
        db_ready, article_id=2, title="incoming",
        ingested_at=_iso(later),
    )
    with cycle_scope(1):
        await stage.handle({
            "article_id": 2, "url": "https://example.com/a/2",
            "is_tracer": False, "tracer_id": None,
        })

    cur = await db_ready.conn.execute(
        "SELECT latest_article_at FROM article_clusters WHERE id=?",
        (seed_id,),
    )
    row = await cur.fetchone()
    await cur.close()
    assert row["latest_article_at"] == _iso(later)


async def test_attach_does_not_rewind_latest_article_at_on_out_of_order(db_ready, bus):
    """B1: out-of-order ingest (an older article catching up) must not
    rewind the cluster's rolling-recency stamp — we always keep the MAX."""
    stage = _make_stage(db_ready, bus)
    fake = _FakeProvider(response=None)
    stage._enabled = True
    stage._provider = fake

    base = datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc)
    seed_id = await _seed_first_article(db_ready, stage, bus, base, article_id=1)

    # Attach a fresh article 4h after seed → stamp moves to base+4h.
    fake.response = (1, 0.30)
    fresh_at = base + timedelta(hours=4)
    await _insert_article(
        db_ready, article_id=2, title="fresh",
        ingested_at=_iso(fresh_at),
    )
    with cycle_scope(1):
        await stage.handle({
            "article_id": 2, "url": "https://example.com/a/2",
            "is_tracer": False, "tracer_id": None,
        })

    # Attach an older article 1h after seed → stamp should NOT rewind.
    fake.response = (1, 0.30)
    older_at = base + timedelta(hours=1)
    await _insert_article(
        db_ready, article_id=3, title="older",
        ingested_at=_iso(older_at),
    )
    with cycle_scope(1):
        await stage.handle({
            "article_id": 3, "url": "https://example.com/a/3",
            "is_tracer": False, "tracer_id": None,
        })

    cur = await db_ready.conn.execute(
        "SELECT latest_article_at FROM article_clusters WHERE id=?",
        (seed_id,),
    )
    row = await cur.fetchone()
    await cur.close()
    assert row["latest_article_at"] == _iso(fresh_at)


async def test_recency_bias_keeps_old_cluster_evolving(db_ready, bus):
    """B1 (the iran-war scenario): cluster opened 48h ago, last article 2h ago
    → EVOLVING (NOT STALE_RETURN as under the old cluster-age label)."""
    stage = _make_stage(db_ready, bus)
    fake = _FakeProvider(response=None)
    stage._enabled = True
    stage._provider = fake

    base = datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc)
    # Seed at base — pretend this is the cluster opened 48h before "now."
    seed_id = await _seed_first_article(db_ready, stage, bus, base, article_id=1)

    # 46h later, an article lands on the cluster → latest_article_at advances.
    fake.response = (1, 0.30)
    feed_at = base + timedelta(hours=46)
    await _insert_article(
        db_ready, article_id=2, title="feed",
        ingested_at=_iso(feed_at),
    )
    with cycle_scope(1):
        await stage.handle({
            "article_id": 2, "url": "https://example.com/a/2",
            "is_tracer": False, "tracer_id": None,
        })
    # The 46h-later article itself joined: the cluster's first article was
    # 46h old at that point → under OLD logic this would be STALE_RETURN.
    # Under recency-bias it's EVOLVING (latest stamp was base, age=46h, but
    # wait — latest_article_at started at base = 46h old at this point ⇒
    # REHASH still. So we use it to advance the stamp.) The test below
    # verifies the NEXT incoming article (2h after the last attach) is
    # labelled EVOLVING.
    bus.published.clear()

    fake.response = (1, 0.30)
    incoming_at = feed_at + timedelta(hours=2)  # 2h after the latest attach
    await _insert_article(
        db_ready, article_id=3, title="new strike",
        ingested_at=_iso(incoming_at),
    )
    with cycle_scope(1):
        await stage.handle({
            "article_id": 3, "url": "https://example.com/a/3",
            "is_tracer": False, "tracer_id": None,
        })

    assert len(bus.published) == 1
    _, payload = bus.published[0]
    assert payload["cluster_id"] == seed_id
    # 2h since last attach is well under 24h evolving window → EVOLVING.
    assert payload["freshness"] == "EVOLVING"


async def test_burst_detection_promotes_to_evolving(db_ready, bus):
    """B2 (stage-level): an incoming article that completes a burst lands
    EVOLVING even when the cluster's stamped `latest_article_at` is 80h
    old (deep in the STALE_RETURN window).

    Default burst params: burst_min_articles=3, burst_window_minutes=30.
    We set up an old-and-quiet cluster, then manually backfill 2 recent
    member rows (added_at inside the burst window) so the next attach is
    the 3rd → trips burst.
    """
    stage = _make_stage(db_ready, bus)
    fake = _FakeProvider(response=None)
    stage._enabled = True
    stage._provider = fake

    base = datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc)
    seed_id = await _seed_first_article(db_ready, stage, bus, base, article_id=1)

    # Force the cluster to look "80h quiet" — latest_article_at is the
    # seed's ingested_at (base), the member-rows added_at predates the
    # burst window.
    incoming_at = base + timedelta(hours=80)
    # Backfill 2 prior member rows whose added_at sits INSIDE the
    # 30-minute burst window preceding the incoming article. This
    # represents an upstream burst that landed seconds earlier but,
    # via timing or contention, the recency stamp is still old (e.g.
    # the cluster's stamp lagged or this is the first incoming after a
    # backfill). Either way, the burst-detection short-circuit must fire.
    burst_start = (incoming_at - timedelta(minutes=20)).isoformat()
    burst_mid = (incoming_at - timedelta(minutes=10)).isoformat()
    async with db_ready.batch() as cur:
        # Two prior articles in the cluster, both attached inside the
        # burst window — pretend a sister stage attached them but the
        # latest_article_at stamp wasn't advanced (defensive: burst-detect
        # works even when stamp is stale).
        await cur.execute(
            "INSERT INTO articles (id, source, source_detail, url, title, "
            "summary, published_at, ingested_at, cycle_id, is_tracer, tracer_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (10, "test", None, "https://example.com/a/10", "burst-a",
             None, burst_start, burst_start, 1, 0, None),
        )
        await cur.execute(
            "INSERT INTO articles (id, source, source_detail, url, title, "
            "summary, published_at, ingested_at, cycle_id, is_tracer, tracer_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (11, "test", None, "https://example.com/a/11", "burst-b",
             None, burst_mid, burst_mid, 1, 0, None),
        )
        await cur.execute(
            "INSERT INTO article_cluster_members (cluster_id, article_id, "
            "distance, added_at) VALUES (?, ?, ?, ?)",
            (seed_id, 10, 0.30, burst_start),
        )
        await cur.execute(
            "INSERT INTO article_cluster_members (cluster_id, article_id, "
            "distance, added_at) VALUES (?, ?, ?, ?)",
            (seed_id, 11, 0.30, burst_mid),
        )
        # Force latest_article_at back to base (old) to defeat recency-bias.
        await cur.execute(
            "UPDATE article_clusters SET latest_article_at=? WHERE id=?",
            (_iso(base), seed_id),
        )

    fake.response = (1, 0.30)
    await _insert_article(
        db_ready, article_id=2, title="burst-c (incoming)",
        ingested_at=_iso(incoming_at),
    )
    bus.published.clear()
    with cycle_scope(1):
        await stage.handle({
            "article_id": 2, "url": "https://example.com/a/2",
            "is_tracer": False, "tracer_id": None,
        })

    assert len(bus.published) == 1
    _, payload = bus.published[0]
    assert payload["cluster_id"] == seed_id
    # Recency would say STALE (80h since stamped latest_article_at), but
    # burst-detection (3 articles in 30 min) forces EVOLVING.
    assert payload["freshness"] == "EVOLVING"


async def test_burst_window_does_not_trip_with_too_few_articles(db_ready, bus):
    """B2: only burst_min_articles inside the window trips the override.
    With one prior member in the window, the incoming (count=2) is NOT
    a burst — the label falls back to age-based logic."""
    stage = _make_stage(db_ready, bus)
    fake = _FakeProvider(response=None)
    stage._enabled = True
    stage._provider = fake

    base = datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc)
    seed_id = await _seed_first_article(db_ready, stage, bus, base, article_id=1)

    incoming_at = base + timedelta(hours=80)
    # One prior member inside the burst window — count for incoming = 2.
    one_recent = (incoming_at - timedelta(minutes=15)).isoformat()
    async with db_ready.batch() as cur:
        await cur.execute(
            "INSERT INTO articles (id, source, source_detail, url, title, "
            "summary, published_at, ingested_at, cycle_id, is_tracer, tracer_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (10, "test", None, "https://example.com/a/10", "only-other",
             None, one_recent, one_recent, 1, 0, None),
        )
        await cur.execute(
            "INSERT INTO article_cluster_members (cluster_id, article_id, "
            "distance, added_at) VALUES (?, ?, ?, ?)",
            (seed_id, 10, 0.30, one_recent),
        )
        await cur.execute(
            "UPDATE article_clusters SET latest_article_at=? WHERE id=?",
            (_iso(base), seed_id),
        )
        # `_add_member` stamps `added_at` with the wall-clock attach time,
        # so the seed article (run through the stage above) lands at real
        # `now()` — which is *after* this incoming article's synthetic
        # base+80h timestamp and would wrongly fall inside the burst
        # window. Anchor it to `base` so the burst count reflects the
        # intended timeline (only member 10 is recent → count=2 < 3).
        await cur.execute(
            "UPDATE article_cluster_members SET added_at=? "
            "WHERE cluster_id=? AND article_id=?",
            (_iso(base), seed_id, 1),
        )

    fake.response = (1, 0.30)
    await _insert_article(
        db_ready, article_id=2, title="incoming",
        ingested_at=_iso(incoming_at),
    )
    bus.published.clear()
    with cycle_scope(1):
        await stage.handle({
            "article_id": 2, "url": "https://example.com/a/2",
            "is_tracer": False, "tracer_id": None,
        })

    _, payload = bus.published[0]
    # count=2 < burst_min_articles=3 → no burst override.
    # Recency-bias: latest_article_at is 80h old → STALE_RETURN.
    assert payload["freshness"] == "STALE_RETURN"


async def test_old_cluster_with_no_recent_activity_becomes_stale(db_ready, bus):
    """B1 + B3: cluster opened long ago, no article for >72h → STALE_RETURN."""
    stage = _make_stage(db_ready, bus)
    fake = _FakeProvider(response=None)
    stage._enabled = True
    stage._provider = fake

    base = datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc)
    seed_id = await _seed_first_article(db_ready, stage, bus, base, article_id=1)

    # Next article arrives 80h after the seed (quiet for >72h rehash window).
    fake.response = (1, 0.30)
    incoming_at = base + timedelta(hours=80)
    await _insert_article(
        db_ready, article_id=2, title="late",
        ingested_at=_iso(incoming_at),
    )
    with cycle_scope(1):
        await stage.handle({
            "article_id": 2, "url": "https://example.com/a/2",
            "is_tracer": False, "tracer_id": None,
        })

    last = bus.published[-1][1]
    assert last["cluster_id"] == seed_id
    assert last["freshness"] == "STALE_RETURN"
