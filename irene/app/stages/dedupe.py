"""
## Specification

**Purpose.** Subscriber on `TOPIC_ARTICLE_INGESTED`. For each newly-ingested
article, embed the title, query the Chroma vector store for the nearest
prior headline, derive a freshness label (BREAKING / EVOLVING / REHASH /
STALE_RETURN) from distance + age, attach the article to an existing
cluster or open a new one, then publish `TOPIC_ARTICLE_DEDUPED` so
downstream stages (match.py) can react. The semantic dedup boundary is
the spot where this pipeline stops treating two restatements of the same
news as two independent signals.

**Public interface.**
- `DedupeStage`:
  - `DedupeStage(deps: StageDeps)` — pure assembly; raises if
    `deps.embedder is None` (the stage cannot run without an embedder).
  - `await stage.start()` — opens the Chroma collection. Lazy-imports
    chromadb; if the library is absent, the stage stays up with
    `_enabled=False` and labels every article BREAKING (no clustering).
    This mirrors the seulgi `news_memory` graceful-degradation pattern
    so tests can run without chromadb installed.
  - `await stage.stop()` — idempotent close.
  - `await stage.handle(payload)` — bus subscriber. Payload shape:
    `{article_id, url, is_tracer, tracer_id}`.

**Owned tables** (`docs/modularization.md` §4):
`article_clusters`, `article_cluster_members`. DDL registered with
`db.register_schema()` at import time.

**Freshness rules (cycle 24, handoff B — recency-bias + burst).**
The age driving REHASH/STALE_RETURN is **time since the cluster's
most-recent article was attached**, not cluster age. A cluster being
fed every few minutes = EVOLVING regardless of when it opened.
A burst (≥`burst_min_articles` in the last `burst_window_minutes`)
overrides the age check and forces EVOLVING.

  - nearest_dist > _BREAKING_DIST                          → BREAKING (new cluster)
  - burst (≥N articles in last T min)                      → EVOLVING (join nearest)
  - latest_article_age < evolving_age_hours (default 24h)  → EVOLVING (join nearest)
  - latest_article_age < rehash_age_hours   (default 72h)  → REHASH   (join nearest)
  - latest_article_age >= rehash_age_hours                 → STALE_RETURN (join nearest)

`_BREAKING_DIST` defaults to `config.dedupe.breaking_distance` (0.55)
— cosine distance, i.e. `1.0 - cosine_similarity`. The Chroma collection
is created with `metadata={"hnsw:space": "cosine"}`.

The `article_clusters.latest_article_at` column is stamped on every
`_attach` so the rolling-recency check is a single column read; the
burst check is a `SELECT COUNT(*)` over `article_cluster_members.added_at`
inside the burst window.

**Invariants.**
- `is_tracer` and `tracer_id` are stamped on every owned-table row from
  the inbound payload. A *new* cluster carries the inbound flag; *joining*
  an existing cluster does NOT mutate the cluster's flags (the original
  creator's flag wins — this preserves tracer-cohort attribution across
  re-ingest).
- One `article_cluster_members` row per article (article_id UNIQUE):
  duplicate ingests are no-ops.
- `cluster_id` on the published `article.deduped` is always an integer when
  `_enabled=True`. When chromadb is missing, the stage still creates a
  cluster row per article and publishes its id — the value is therefore
  *never* `None` in practice. The contract still permits `None` on the
  payload for forward-compatibility with future short-circuit modes.

**Side effects.**
- `chromadb.PersistentClient(path=config.dedupe.chroma_path)` is opened in
  `start()`. The collection `"irene_articles"` is created on demand.
- Writes `article_clusters` + `article_cluster_members`.
- Publishes `TOPIC_ARTICLE_DEDUPED`.

**Failure modes.**
- `RuntimeError` from `__init__` when `deps.embedder is None`.
- `ModuleNotFoundError` for chromadb at `start()` → logged at WARNING,
  `_enabled` set to False, every article labelled BREAKING in a new
  cluster (degraded but functional).
- DB write errors propagate; the bus middleware will roll back the batch.

**Non-goals.**
- Re-clustering past articles when thresholds change (replay only).
- Persisting embeddings to SQLite (they live in Chroma; SQLite stores
  cluster membership).
- Writing to `articles` (owned by ingest).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional, Protocol

from app import db as db_module
from app.deps import StageDeps
from app.topics import TOPIC_ARTICLE_DEDUPED

logger = logging.getLogger(__name__)


# Cosine-distance threshold above which the nearest neighbour is too far
# to be the "same news" — drives BREAKING. Configurable via
# `config.dedupe.breaking_distance`; default 0.55 is roughly the
# headline-rewrite vs. unrelated-news divide on MiniLM-L6-v2.
_BREAKING_DIST_DEFAULT = 0.55

# Cycle 24 (handoff B): fallback defaults if the config doesn't supply
# them. The real values come from `config.dedupe.evolving_age_hours` /
# `rehash_age_hours` and are passed into `_label` by the stage. These
# constants are kept for backwards-compatibility with old test imports
# and the pure-helper signature.
_EVOLVING_AGE_SECS = 24 * 3600   # was 6h pre-cycle-24
_STALE_AGE_SECS = 72 * 3600      # was 24h pre-cycle-24

_COLLECTION_NAME = "irene_articles"


_DEDUPE_DDL = [
    """
    CREATE TABLE IF NOT EXISTS article_clusters (
        id                          INTEGER PRIMARY KEY AUTOINCREMENT,
        representative_article_id   INTEGER NOT NULL,
        freshness                   TEXT NOT NULL CHECK(freshness IN
                                        ('BREAKING','EVOLVING','REHASH','STALE_RETURN')),
        created_at                  TEXT NOT NULL,
        is_tracer                   INTEGER NOT NULL DEFAULT 0,
        tracer_id                   TEXT,
        -- Cycle 24 (handoff B): timestamp of the most recently attached
        -- article on this cluster. Drives the recency-bias freshness
        -- label so a cluster being actively fed stays EVOLVING regardless
        -- of when it was created. Backfilled on existing DBs by
        -- `scripts/migrate/2026_05_26_cluster_latest.py`.
        latest_article_at           TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_article_clusters_freshness ON article_clusters(freshness)",
    "CREATE INDEX IF NOT EXISTS idx_article_clusters_latest_article_at "
    "ON article_clusters(latest_article_at)",
    """
    CREATE TABLE IF NOT EXISTS article_cluster_members (
        cluster_id  INTEGER NOT NULL REFERENCES article_clusters(id),
        article_id  INTEGER NOT NULL UNIQUE,
        distance    REAL,
        added_at    TEXT NOT NULL,
        PRIMARY KEY (cluster_id, article_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_article_cluster_members_cluster_id "
    "ON article_cluster_members(cluster_id)",
]
db_module.register_schema("app/stages/dedupe.py", _DEDUPE_DDL)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso(s: str | None) -> Optional[datetime]:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _label(
    nearest_dist: float,
    latest_article_age_secs: float,
    breaking_dist: float,
    *,
    burst_active: bool = False,
    evolving_age_secs: float = _EVOLVING_AGE_SECS,
    rehash_age_secs: float = _STALE_AGE_SECS,
) -> str:
    """Pure freshness-label decision — exposed for tests.

    Cycle 24 (handoff B): `latest_article_age_secs` is "time since the
    cluster's most recent article was attached," NOT cluster age. A
    burst (≥N articles in last T minutes) forces EVOLVING regardless of
    age — the caller computes the burst flag from
    `article_cluster_members.added_at` history and passes it in.
    """
    if nearest_dist > breaking_dist:
        return "BREAKING"
    if burst_active:
        return "EVOLVING"
    if latest_article_age_secs < evolving_age_secs:
        return "EVOLVING"
    if latest_article_age_secs < rehash_age_secs:
        return "REHASH"
    return "STALE_RETURN"


class _NearestProvider(Protocol):
    """Test-injectable nearest-neighbour query — returns the nearest match
    or `None` when the store is empty / disabled. The real implementation
    wraps Chroma; tests substitute a fake."""

    def nearest(self, vec) -> Optional[tuple[int, float]]:
        """Return `(article_id, distance)` of the nearest stored article."""
        ...

    def add(self, article_id: int, vec, cluster_id: int, ingested_at: str) -> None:
        """Store the article's embedding under the cluster's id."""
        ...


class _ChromaProvider:
    """Real nearest-neighbour provider — thin wrapper over a Chroma collection."""

    def __init__(self, collection) -> None:
        self._coll = collection

    def nearest(self, vec) -> Optional[tuple[int, float]]:
        try:
            count = self._coll.count()
        except Exception as exc:  # noqa: BLE001
            logger.debug("[dedupe] chroma count failed: %s", exc)
            return None
        if count == 0:
            return None
        try:
            qres = self._coll.query(
                query_embeddings=[vec.tolist() if hasattr(vec, "tolist") else list(vec)],
                n_results=1,
                include=["distances", "metadatas"],
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug("[dedupe] chroma query failed: %s", exc)
            return None
        ids = (qres.get("ids") or [[]])[0]
        dists = (qres.get("distances") or [[]])[0]
        if not ids or not dists:
            return None
        try:
            return int(ids[0]), float(dists[0])
        except (TypeError, ValueError):
            return None

    def add(self, article_id: int, vec, cluster_id: int, ingested_at: str) -> None:
        try:
            self._coll.add(
                ids=[str(article_id)],
                embeddings=[vec.tolist() if hasattr(vec, "tolist") else list(vec)],
                metadatas=[{"cluster_id": cluster_id, "ingested_at": ingested_at}],
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug("[dedupe] chroma add failed for article_id=%d: %s", article_id, exc)


class DedupeStage:
    def __init__(self, deps: StageDeps) -> None:
        if deps.embedder is None:
            raise RuntimeError(
                "DedupeStage requires deps.embedder — wire one in main.py."
            )
        self._db = deps.db
        self._bus = deps.bus
        self._embedder = deps.embedder
        cfg_dedupe = deps.config.dedupe
        self._chroma_path = cfg_dedupe.chroma_path
        self._breaking_dist = float(cfg_dedupe.breaking_distance)
        # Cycle 24 (handoff B): freshness-window + burst knobs from config.
        self._evolving_age_secs = float(cfg_dedupe.evolving_age_hours) * 3600.0
        self._rehash_age_secs = float(cfg_dedupe.rehash_age_hours) * 3600.0
        self._burst_min_articles = int(cfg_dedupe.burst_min_articles)
        self._burst_window_secs = float(cfg_dedupe.burst_window_minutes) * 60.0
        self._enabled: bool = False
        self._provider: Optional[_NearestProvider] = None

    async def start(self) -> None:
        if self._provider is not None:
            self._enabled = True
            return
        try:
            import chromadb  # type: ignore
        except ModuleNotFoundError:
            logger.warning(
                "[dedupe] chromadb not installed — clustering disabled, "
                "every article will be labelled BREAKING with its own cluster."
            )
            self._enabled = False
            self._provider = None
            return
        try:
            client = chromadb.PersistentClient(path=self._chroma_path)
            collection = client.get_or_create_collection(
                name=_COLLECTION_NAME,
                metadata={"hnsw:space": "cosine"},
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "[dedupe] chroma init failed (%s) — clustering disabled.", exc
            )
            self._enabled = False
            self._provider = None
            return
        self._provider = _ChromaProvider(collection)
        self._enabled = True
        logger.info("[dedupe] chroma ready at %s (collection=%s)",
                    self._chroma_path, _COLLECTION_NAME)

    async def stop(self) -> None:
        self._provider = None
        self._enabled = False

    async def handle(self, payload: dict) -> None:
        article_id = int(payload["article_id"])
        is_tracer = bool(payload.get("is_tracer", False))
        tracer_id: Optional[str] = payload.get("tracer_id")

        article = await self._fetch_article(article_id)
        if article is None:
            logger.warning("[dedupe] article_id=%d not found — skipping", article_id)
            return
        title: str = article["title"]
        ingested_at: str = article["ingested_at"]

        vec = self._embedder.embed(title)

        nearest = self._provider.nearest(vec) if (self._enabled and self._provider) else None
        cluster_id, freshness, distance = await self._resolve_cluster(
            article_id=article_id,
            ingested_at=ingested_at,
            nearest=nearest,
            is_tracer=is_tracer,
            tracer_id=tracer_id,
        )

        if self._enabled and self._provider is not None:
            self._provider.add(article_id, vec, cluster_id, ingested_at)

        await self._bus.publish(
            TOPIC_ARTICLE_DEDUPED,
            {
                "article_id": article_id,
                "cluster_id": cluster_id,
                "freshness": freshness,
                "is_tracer": is_tracer,
                "tracer_id": tracer_id,
            },
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _fetch_article(self, article_id: int) -> Optional[dict]:
        cur = await self._db.conn.execute(
            "SELECT title, ingested_at FROM articles WHERE id=?",
            (article_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return None
        return {"title": row["title"], "ingested_at": row["ingested_at"]}

    async def _resolve_cluster(
        self,
        *,
        article_id: int,
        ingested_at: str,
        nearest: Optional[tuple[int, float]],
        is_tracer: bool,
        tracer_id: Optional[str],
    ) -> tuple[int, str, Optional[float]]:
        if nearest is None:
            cluster_id = await self._create_cluster(
                representative_article_id=article_id,
                freshness="BREAKING",
                is_tracer=is_tracer,
                tracer_id=tracer_id,
                latest_article_at=ingested_at,
            )
            await self._add_member(cluster_id, article_id, distance=None)
            return cluster_id, "BREAKING", None

        nearest_article_id, nearest_dist = nearest
        existing = await self._lookup_cluster_for_article(nearest_article_id)
        if existing is None:
            cluster_id = await self._create_cluster(
                representative_article_id=article_id,
                freshness="BREAKING",
                is_tracer=is_tracer,
                tracer_id=tracer_id,
                latest_article_at=ingested_at,
            )
            await self._add_member(cluster_id, article_id, distance=None)
            return cluster_id, "BREAKING", None

        # Cycle 24 (handoff B): recency-bias + burst.
        latest_at = await self._fetch_latest_article_at(existing["id"])
        # Fall back to the rep's ingested_at if the cluster predates the
        # `latest_article_at` column (i.e. migration hasn't backfilled yet
        # or somehow returned NULL). Mirrors pre-cycle-24 behaviour minus
        # the recency-bias, so worst case we revert to the old label.
        if latest_at is None:
            latest_at = await self._fetch_ingested_at(existing["representative_article_id"])
        latest_age_secs = self._age_secs(latest_at, ingested_at)

        burst_count = await self._count_recent_members(
            existing["id"], ingested_at, self._burst_window_secs,
        )
        burst_active = burst_count >= self._burst_min_articles

        freshness = _label(
            nearest_dist,
            latest_age_secs,
            self._breaking_dist,
            burst_active=burst_active,
            evolving_age_secs=self._evolving_age_secs,
            rehash_age_secs=self._rehash_age_secs,
        )

        if freshness == "BREAKING":
            cluster_id = await self._create_cluster(
                representative_article_id=article_id,
                freshness="BREAKING",
                is_tracer=is_tracer,
                tracer_id=tracer_id,
                latest_article_at=ingested_at,
            )
            await self._add_member(cluster_id, article_id, distance=nearest_dist)
            return cluster_id, "BREAKING", nearest_dist

        await self._add_member(existing["id"], article_id, distance=nearest_dist)
        await self._stamp_latest_article_at(existing["id"], ingested_at)
        return existing["id"], freshness, nearest_dist

    async def _create_cluster(
        self,
        *,
        representative_article_id: int,
        freshness: str,
        is_tracer: bool,
        tracer_id: Optional[str],
        latest_article_at: Optional[str] = None,
    ) -> int:
        # Cycle 24 (handoff B): stamp latest_article_at at creation so the
        # first recency-bias read on the cluster is correct (= rep's
        # ingested_at).
        async with self._db.batch() as cur:
            await cur.execute(
                """
                INSERT INTO article_clusters
                    (representative_article_id, freshness, created_at,
                     is_tracer, tracer_id, latest_article_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    representative_article_id,
                    freshness,
                    _now_iso(),
                    1 if is_tracer else 0,
                    tracer_id,
                    latest_article_at,
                ),
            )
            new_id = cur.lastrowid
        if new_id is None:
            raise RuntimeError(
                f"article_clusters insert failed for rep={representative_article_id}"
            )
        return int(new_id)

    async def _add_member(
        self,
        cluster_id: int,
        article_id: int,
        *,
        distance: Optional[float],
    ) -> None:
        async with self._db.batch() as cur:
            await cur.execute(
                """
                INSERT OR IGNORE INTO article_cluster_members
                    (cluster_id, article_id, distance, added_at)
                VALUES (?, ?, ?, ?)
                """,
                (cluster_id, article_id, distance, _now_iso()),
            )

    async def _lookup_cluster_for_article(self, article_id: int) -> Optional[dict]:
        cur = await self._db.conn.execute(
            """
            SELECT c.id AS id, c.representative_article_id AS representative_article_id
            FROM article_cluster_members m
            JOIN article_clusters c ON c.id = m.cluster_id
            WHERE m.article_id = ?
            """,
            (article_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return None
        return {
            "id": int(row["id"]),
            "representative_article_id": int(row["representative_article_id"]),
        }

    async def _fetch_ingested_at(self, article_id: int) -> Optional[str]:
        cur = await self._db.conn.execute(
            "SELECT ingested_at FROM articles WHERE id=?",
            (article_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return None
        return row["ingested_at"]

    async def _fetch_latest_article_at(self, cluster_id: int) -> Optional[str]:
        """Cycle 24 (handoff B): rolling-recency timestamp on the cluster."""
        cur = await self._db.conn.execute(
            "SELECT latest_article_at FROM article_clusters WHERE id=?",
            (cluster_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return None
        return row["latest_article_at"]

    async def _count_recent_members(
        self,
        cluster_id: int,
        incoming_ingested_at: str,
        window_secs: float,
    ) -> int:
        """Cycle 24 (handoff B): burst detection.

        Count members attached in the `window_secs` seconds preceding the
        incoming article. Counts the incoming article too (incoming + N
        existing in the window ≥ burst_min_articles → burst). We compare
        against `article_cluster_members.added_at`, which is stamped at
        attach time.
        """
        inc_dt = _parse_iso(incoming_ingested_at)
        if inc_dt is None:
            return 0
        from datetime import timedelta
        window_start_iso = (inc_dt - timedelta(seconds=window_secs)).isoformat()
        cur = await self._db.conn.execute(
            """
            SELECT COUNT(*) AS c FROM article_cluster_members
            WHERE cluster_id = ? AND added_at >= ?
            """,
            (cluster_id, window_start_iso),
        )
        row = await cur.fetchone()
        await cur.close()
        existing = int(row["c"]) if row is not None else 0
        # The incoming article hasn't been inserted yet — count it
        # explicitly so the threshold check matches the user-facing
        # "N articles including this one" semantics.
        return existing + 1

    async def _stamp_latest_article_at(self, cluster_id: int, ingested_at: str) -> None:
        """Cycle 24 (handoff B): update the cluster's rolling-recency timestamp.

        Uses MAX(existing, incoming) so an out-of-order ingest (a stale
        feed catching up) never rewinds the recency stamp.
        """
        async with self._db.batch() as cur:
            await cur.execute(
                """
                UPDATE article_clusters
                SET latest_article_at = CASE
                    WHEN latest_article_at IS NULL OR latest_article_at < ?
                        THEN ?
                    ELSE latest_article_at
                END
                WHERE id = ?
                """,
                (ingested_at, ingested_at, cluster_id),
            )

    @staticmethod
    def _age_secs(rep_ingested_at: Optional[str], incoming_ingested_at: str) -> float:
        rep_dt = _parse_iso(rep_ingested_at)
        inc_dt = _parse_iso(incoming_ingested_at)
        if rep_dt is None or inc_dt is None:
            return 0.0
        return max(0.0, (inc_dt - rep_dt).total_seconds())
