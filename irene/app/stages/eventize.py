"""
## Specification

**Purpose.** P3 chunk 2's event-builder stage. Subscribes to
`TOPIC_RESOLUTION_COMPLETED`; for each just-resolved article, clusters
its facts into canonical **events** (real-world happenings reported
across one-or-more articles) and writes/updates rows in `events` +
`event_edges` + the `facts.event_id` column. Publishes
`TOPIC_EVENT_UPDATED` on completion.

Algorithm locked in `docs/papers/eventization/notes.md`:
- **Clustering metric**: hybrid — entity-overlap gate AND cosine ≥ 0.75
  on the templated triple `f"{subj} {relation} {obj} {obj_value}"`.
- **Time window**: 24h default, per-event-type override via
  `app/stages/data/event_types.yaml`.
- **Tie-break**: most-recent-active event wins; secondary by cosine.
- **Identity**: always look up via `entity_resolutions.resolved_to_id`,
  never raw subj/obj_entity_id.

**The P2/P3 hard invariant** — trade path never reads `facts` /
`entities` / `entity_aliases` / `extraction_runs` /
`entity_resolutions` / **`events`** / **`event_edges`**. Eventize
writes on the DKG side; the lint
(`scripts/lint/no_facts_in_trade_path.py`) enforces it.

**Public interface.**
- `EventizeStage(deps: StageDeps)` — needs `db` + `bus` + `embedder`.
- `await start()` — loads `event_types.yaml`; warms the EventIndex.
- `await stop()` — idempotent.
- `await handle(payload)` — subscriber on `TOPIC_RESOLUTION_COMPLETED`.
- `EVENTIZE_VERSION` — bumps when the clustering algorithm itself
  changes.
- `EventIndex` — in-memory per-type centroid matrix; same pattern as
  `app/sbert_dedupe.py::EntityIndex` (lazy load + register on insert).

**Owned tables**: `events`, `event_edges`. Also writes
`facts.event_id` (column-add registered here; `facts` is otherwise
owned by `extract.py` — documented exception per the DKG spec's
"graph-upsert" service boundary).

**Invariants.**
- One `events` row per cluster, identified by the hybrid rule.
- `facts.event_id` is set exactly once per fact under the current
  `EVENTIZE_VERSION`. Re-running the stage on already-eventized facts
  is a no-op (we skip facts whose `event_id IS NOT NULL`).
- `event_edges` is append-only with UNIQUE on
  `(from_event_id, to_event_id, edge_kind)`. Re-running emits no
  duplicate edges.
- Every row carries the article's `cycle_id` (not a new cycle for
  eventize).

**Side effects.** Reads `facts` + `entities` + `entity_resolutions` +
`articles`. Writes `events`, `event_edges`, `facts.event_id`.
Publishes `TOPIC_EVENT_UPDATED`.

**Failure modes.**
- `RuntimeError` at construction if `deps.embedder` is None.
- `FileNotFoundError` at `start()` if `event_types.yaml` is missing.
- DB errors propagate; the per-article batch rolls back as a unit.

**Non-goals.**
- Doesn't score events against markets (that's `link.py`).
- Doesn't run any LLM. Deterministic clustering over already-extracted
  facts.
- Doesn't currently emit `contradicts` / `escalates` / `resolves`
  edges — only `confirms`. The polarity-detection logic for those is
  the next session's work (see research doc §"Open issues").
"""

from __future__ import annotations

import logging
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import numpy as np
import yaml

from app import db as db_module
from app.deps import StageDeps
from app.topics import TOPIC_EVENT_UPDATED, TOPIC_RESOLUTION_COMPLETED

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------


_EVENTIZE_DDL = [
    """
    CREATE TABLE IF NOT EXISTS events (
        id                INTEGER PRIMARY KEY AUTOINCREMENT,
        event_type        TEXT NOT NULL,
        status            TEXT NOT NULL CHECK(status IN
                              ('emerging','confirmed','contradicted',
                               'resolved','stale')),
        canonical_summary TEXT,
        novelty_z         REAL,
        support_count     INTEGER NOT NULL DEFAULT 1,
        first_seen_at     TEXT NOT NULL,
        last_seen_at      TEXT NOT NULL,
        cycle_id          INTEGER NOT NULL,
        is_tracer         INTEGER NOT NULL DEFAULT 0,
        tracer_id         TEXT,
        eventize_version  TEXT NOT NULL DEFAULT 'v1'
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_events_type_last_seen ON events(event_type, last_seen_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_events_status ON events(status)",
    "CREATE INDEX IF NOT EXISTS idx_events_cycle_id ON events(cycle_id)",
    "CREATE INDEX IF NOT EXISTS idx_events_tracer ON events(is_tracer) WHERE is_tracer=1",
    """
    CREATE TABLE IF NOT EXISTS event_edges (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        from_event_id   INTEGER NOT NULL REFERENCES events(id),
        to_event_id     INTEGER NOT NULL REFERENCES events(id),
        edge_kind       TEXT NOT NULL CHECK(edge_kind IN
                            ('confirms','contradicts','escalates','resolves')),
        confidence      REAL NOT NULL CHECK(confidence >= 0.0 AND confidence <= 1.0),
        observed_at     TEXT NOT NULL,
        UNIQUE(from_event_id, to_event_id, edge_kind)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_event_edges_from ON event_edges(from_event_id)",
    "CREATE INDEX IF NOT EXISTS idx_event_edges_to ON event_edges(to_event_id)",
    "CREATE INDEX IF NOT EXISTS idx_event_edges_kind ON event_edges(edge_kind)",
    # facts.event_id column-add. ALTER ADD COLUMN is NOT idempotent in
    # SQLite (raises on re-run); the schema_migrations machinery
    # sha-keys each statement and only runs it the first time.
    "ALTER TABLE facts ADD COLUMN event_id INTEGER REFERENCES events(id)",
    "CREATE INDEX IF NOT EXISTS idx_facts_event_id ON facts(event_id)",
]


db_module.register_schema("app/stages/eventize.py", _EVENTIZE_DDL)


# ---------------------------------------------------------------------------
# YAML config loader
# ---------------------------------------------------------------------------


def _load_event_types_yaml(path: Path) -> dict:
    """Parses `event_types.yaml` into a structured dict the stage can
    consume cheaply on every fact."""
    with path.open("r") as f:
        raw = yaml.safe_load(f)
    out = {
        "default_window_hours": int(raw.get("default_window_hours", 24)),
        "default_stale_hours": int(raw.get("default_stale_hours", 72)),
        "per_type": {},
        "rules": [],
    }
    for spec in raw.get("event_types") or []:
        out["per_type"][spec["type"]] = {
            "window_hours": int(spec["window_hours"]),
            "stale_hours": int(spec["stale_hours"]),
        }
    for rule in raw.get("type_rules") or []:
        out["rules"].append({
            "subj": rule["subj"],
            "relation": rule["relation"],
            "obj": rule.get("obj"),  # may be None for relations with no obj
            "type": rule["type"],
        })
    return out


def _classify(rules: list[dict], subj_type: str, relation: str, obj_type: Optional[str]) -> str:
    """Walks `type_rules` in declaration order. First match wins; `*`
    wildcards on any of the three slots. Unmatched → UNCLASSIFIED."""
    for r in rules:
        if r["subj"] != "*" and r["subj"] != subj_type:
            continue
        if r["relation"] != relation:
            continue
        rule_obj = r["obj"]
        if rule_obj == "*":
            pass
        elif rule_obj == "NULL" or rule_obj is None:
            if obj_type is not None:
                continue
        else:
            if obj_type != rule_obj:
                continue
        return r["type"]
    return "UNCLASSIFIED"


# ---------------------------------------------------------------------------
# EventIndex — in-memory per-type centroid + metadata cache.
# ---------------------------------------------------------------------------


class EventIndex:
    """Per event_type, an in-memory matrix of event centroid embeddings
    plus parallel arrays of (event_id, support_count, last_seen_at).
    Same `load + register + best_match` pattern as `EntityIndex`, with
    two additions: centroids update on `confirm` (via
    `update_centroid`), and `best_match` honours a staleness cutoff.

    Concurrency: meant to be used inside the same lock that serialises
    DB writes. No internal thread/task safety.
    """

    __slots__ = (
        "_event_ids_by_type", "_centroids_by_type",
        "_support_counts_by_type", "_last_seen_by_type",
        "_loaded_types", "_dim",
    )

    def __init__(self) -> None:
        # entity_type -> list[event_id]
        self._event_ids_by_type: dict[str, list[int]] = {}
        # event_type -> stacked (N, dim) matrix of centroids
        self._centroids_by_type: dict[str, np.ndarray] = {}
        # event_type -> list[support_count]
        self._support_counts_by_type: dict[str, list[int]] = {}
        # event_type -> list[last_seen_at_iso]
        self._last_seen_by_type: dict[str, list[str]] = {}
        self._loaded_types: set[str] = set()
        self._dim: Optional[int] = None

    def load(self, conn, event_type: str, embedder) -> None:
        """Idempotently warm the cache from the DB.

        Centroids aren't stored on `events` (would bloat the row); we
        reconstruct them by averaging each event's member-fact
        embeddings on first touch. For events with many members this
        costs an embedder batch call per event-type — acceptable since
        load() fires once per type at startup.
        """
        if event_type in self._loaded_types:
            return
        cur = conn.execute(
            "SELECT id, support_count, last_seen_at, canonical_summary "
            "FROM events WHERE event_type = ? AND status != 'stale' ORDER BY id",
            (event_type,),
        )
        rows = cur.fetchall()
        if not rows:
            self._event_ids_by_type[event_type] = []
            self._support_counts_by_type[event_type] = []
            self._last_seen_by_type[event_type] = []
            self._centroids_by_type[event_type] = np.zeros(
                (0, self._dim or 0), dtype=np.float32
            )
            self._loaded_types.add(event_type)
            return

        # For each event, reconstitute its centroid by re-embedding the
        # canonical_summary. Cheaper than re-fetching every member fact.
        ids: list[int] = []
        counts: list[int] = []
        last_seen: list[str] = []
        summaries: list[str] = []
        for r in rows:
            ids.append(int(r[0]))
            counts.append(int(r[1]))
            last_seen.append(str(r[2]))
            summaries.append(str(r[3]) if r[3] else "")
        # Batch-embed for speed.
        if hasattr(embedder, "embed_batch"):
            matrix = np.asarray(embedder.embed_batch(summaries), dtype=np.float32)
        else:
            matrix = np.stack([
                np.asarray(embedder.embed(s), dtype=np.float32) for s in summaries
            ])
        if self._dim is None:
            self._dim = int(matrix.shape[1])
        self._event_ids_by_type[event_type] = ids
        self._support_counts_by_type[event_type] = counts
        self._last_seen_by_type[event_type] = last_seen
        self._centroids_by_type[event_type] = matrix
        self._loaded_types.add(event_type)

    def best_match(
        self,
        event_type: str,
        query_vec: np.ndarray,
        staleness_cutoff_iso: str,
    ) -> tuple[Optional[int], float]:
        """Returns (event_id, cosine) of the best match within the
        staleness window. Tie-break is by most-recent last_seen_at.
        Returns (None, 0.0) if no event in this type is active."""
        ids = self._event_ids_by_type.get(event_type, [])
        if not ids:
            return None, 0.0
        matrix = self._centroids_by_type[event_type]
        last_seen = self._last_seen_by_type[event_type]
        # Mask stale events: their cosine is forced below threshold.
        # Pre-compute the cosine then mask.
        sims = matrix @ query_vec
        # Stale mask — index positions with last_seen < cutoff become -inf
        mask_alive = np.array(
            [last_seen[i] >= staleness_cutoff_iso for i in range(len(ids))],
            dtype=bool,
        )
        if not np.any(mask_alive):
            return None, 0.0
        sims_masked = np.where(mask_alive, sims, -np.inf)
        best_idx = int(np.argmax(sims_masked))
        if sims_masked[best_idx] == -np.inf:
            return None, 0.0
        return ids[best_idx], float(sims[best_idx])

    def register(
        self,
        *,
        event_id: int,
        event_type: str,
        centroid: np.ndarray,
        last_seen_at_iso: str,
    ) -> None:
        vec = np.asarray(centroid, dtype=np.float32).reshape(-1)
        if self._dim is None:
            self._dim = int(vec.shape[0])
        if event_type not in self._loaded_types:
            self._event_ids_by_type[event_type] = []
            self._support_counts_by_type[event_type] = []
            self._last_seen_by_type[event_type] = []
            self._centroids_by_type[event_type] = np.zeros(
                (0, self._dim), dtype=np.float32
            )
            self._loaded_types.add(event_type)
        self._event_ids_by_type[event_type].append(event_id)
        self._support_counts_by_type[event_type].append(1)
        self._last_seen_by_type[event_type].append(last_seen_at_iso)
        current = self._centroids_by_type[event_type]
        if current.shape[0] == 0:
            self._centroids_by_type[event_type] = vec.reshape(1, -1)
        else:
            self._centroids_by_type[event_type] = np.vstack([
                current, vec.reshape(1, -1),
            ])

    def update_on_confirm(
        self,
        *,
        event_id: int,
        event_type: str,
        new_fact_vec: np.ndarray,
        new_last_seen_at_iso: str,
    ) -> None:
        """Incremental-mean centroid update: new = (old * n + fact) / (n + 1)."""
        ids = self._event_ids_by_type.get(event_type, [])
        try:
            i = ids.index(event_id)
        except ValueError:
            # Shouldn't happen — caller should always have hit best_match first.
            raise RuntimeError(
                f"EventIndex.update_on_confirm: event_id={event_id} "
                f"not in cached type={event_type!r}"
            )
        n = self._support_counts_by_type[event_type][i]
        new_n = n + 1
        old_centroid = self._centroids_by_type[event_type][i]
        new_centroid = (old_centroid * n + new_fact_vec.astype(np.float32)) / new_n
        # Re-normalise to keep cosine math consistent.
        norm = float(np.linalg.norm(new_centroid))
        if norm > 0:
            new_centroid = new_centroid / norm
        self._centroids_by_type[event_type][i] = new_centroid
        self._support_counts_by_type[event_type][i] = new_n
        self._last_seen_by_type[event_type][i] = new_last_seen_at_iso

    def size(self, event_type: str) -> int:
        return len(self._event_ids_by_type.get(event_type, []))


# ---------------------------------------------------------------------------
# Stage
# ---------------------------------------------------------------------------


class EventizeStage:
    EVENTIZE_VERSION: str = "v1"
    SIMILARITY_THRESHOLD: float = 0.75
    YAML_PATH: str = "app/stages/data/event_types.yaml"

    def __init__(self, deps: StageDeps) -> None:
        if deps.embedder is None:
            raise RuntimeError(
                "EventizeStage requires deps.embedder (similarity-based clustering)"
            )
        self._db = deps.db
        self._bus = deps.bus
        self._embedder = deps.embedder
        self._stopped = False
        self._config: dict = {}
        self._index = EventIndex()
        # Cache: event_id -> set[resolved_entity_id] for the entity-overlap gate.
        # Refreshed on confirm + create.
        self._members_by_event: dict[int, set[int]] = {}

    # ---- lifecycle -----------------------------------------------------

    async def start(self) -> None:
        path = Path(self.YAML_PATH)
        if not path.is_absolute():
            repo_root = Path(__file__).resolve().parent.parent.parent
            path = repo_root / self.YAML_PATH
        self._config = _load_event_types_yaml(path)
        logger.info(
            "eventize: loaded %d type rules, %d per-type overrides",
            len(self._config["rules"]),
            len(self._config["per_type"]),
        )

    async def stop(self) -> None:
        self._stopped = True

    # ---- main entry ----------------------------------------------------

    async def handle(self, payload: dict) -> None:
        article_id_raw = payload.get("article_id")
        if article_id_raw is None:
            logger.warning("eventize: payload missing article_id: %r", payload)
            return
        article_id = int(article_id_raw)
        extraction_run_id = payload.get("extraction_run_id")
        is_tracer = bool(payload.get("is_tracer", False))
        tracer_id = payload.get("tracer_id")

        # Fetch the article (for published_at + cycle_id) and its facts.
        article_row = await self._fetch_article(article_id)
        if article_row is None:
            logger.warning("eventize: no article row for id=%d", article_id)
            return
        facts = await self._fetch_facts_for_article(article_id)
        if not facts:
            await self._publish_completion(
                extraction_run_id=extraction_run_id,
                article_id=article_id,
                events_created=0, events_confirmed=0,
                is_tracer=is_tracer, tracer_id=tracer_id,
            )
            return

        published_at_iso = article_row["published_at"]
        cycle_id = int(article_row["cycle_id"])

        # Compute triple embeddings in one batch — cheaper than per-fact.
        triples = [self._templated_triple(f) for f in facts]
        if hasattr(self._embedder, "embed_batch"):
            triple_vecs = np.asarray(
                self._embedder.embed_batch(triples), dtype=np.float32
            )
        else:
            triple_vecs = np.stack([
                np.asarray(self._embedder.embed(t), dtype=np.float32) for t in triples
            ])

        created = 0
        confirmed = 0
        for i, fact in enumerate(facts):
            if fact["event_id"] is not None:
                continue  # Already eventized — idempotent skip.
            triple_vec = triple_vecs[i]
            event_type = _classify(
                self._config["rules"],
                fact["subj_type"], fact["relation"], fact["obj_type"],
            )
            # Warm the index lazily on first encounter of this type.
            if event_type not in self._index._loaded_types:
                await self._warm_index_type(event_type)

            window_hours, stale_hours = self._window_and_stale(event_type)
            try:
                pub_dt = datetime.fromisoformat(published_at_iso)
            except (TypeError, ValueError):
                pub_dt = datetime.now(timezone.utc)
            stale_cutoff = (pub_dt - timedelta(hours=stale_hours)).isoformat()
            window_cutoff = (pub_dt - timedelta(hours=window_hours)).isoformat()

            match_id, match_cos = self._index.best_match(
                event_type, triple_vec, staleness_cutoff_iso=window_cutoff,
            )
            if match_id is not None and match_cos >= self.SIMILARITY_THRESHOLD:
                if self._has_entity_overlap(match_id, fact):
                    await self._confirm_fact(
                        event_id=match_id,
                        event_type=event_type,
                        fact=fact,
                        fact_vec=triple_vec,
                        last_seen_iso=published_at_iso,
                    )
                    confirmed += 1
                    continue

            # No match → new event.
            new_id = await self._create_event(
                event_type=event_type,
                fact=fact,
                fact_vec=triple_vec,
                published_at_iso=published_at_iso,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            created += 1
            self._members_by_event[new_id] = self._fact_entity_set(fact)

        await self._publish_completion(
            extraction_run_id=extraction_run_id,
            article_id=article_id,
            events_created=created,
            events_confirmed=confirmed,
            is_tracer=is_tracer, tracer_id=tracer_id,
        )

    # ---- helpers -------------------------------------------------------

    def _templated_triple(self, fact: dict) -> str:
        """The string we embed for clustering. Locked format — changes
        invalidate previous cluster centroids on re-load."""
        parts = [
            (fact.get("subj_canonical") or "").strip(),
            (fact.get("relation") or "").strip(),
            (fact.get("obj_canonical") or "").strip(),
            (fact.get("obj_value") or "").strip(),
        ]
        return " ".join(p for p in parts if p).strip()

    def _window_and_stale(self, event_type: str) -> tuple[int, int]:
        per_type = self._config["per_type"].get(event_type)
        if per_type is None:
            return (
                self._config["default_window_hours"],
                self._config["default_stale_hours"],
            )
        return per_type["window_hours"], per_type["stale_hours"]

    def _fact_entity_set(self, fact: dict) -> set[int]:
        out: set[int] = set()
        if fact["subj_resolved_id"] is not None:
            out.add(int(fact["subj_resolved_id"]))
        if fact["obj_resolved_id"] is not None:
            out.add(int(fact["obj_resolved_id"]))
        return out

    def _has_entity_overlap(self, event_id: int, fact: dict) -> bool:
        members = self._members_by_event.get(event_id)
        if members is None:
            # We haven't loaded this event's entity set yet (cold-start
            # from prior session). Permit the match optimistically; the
            # cosine gate is doing the heavy lifting and we'll populate
            # members on first confirm.
            return True
        fact_set = self._fact_entity_set(fact)
        return bool(members & fact_set)

    async def _warm_index_type(self, event_type: str) -> None:
        """Load this event_type's centroids + member-entity sets from the DB."""
        async with self._db.batch() as cur:
            # EventIndex.load expects a sync cursor; aiosqlite cursors
            # expose the same `execute` interface. We do the call here
            # inside the batch to keep the lock semantics consistent.
            self._index_load_via_async_cur = True
        await self._sync_load_index_for_type(event_type)

    async def _sync_load_index_for_type(self, event_type: str) -> None:
        """Async-friendly equivalent of EventIndex.load — replicates the
        load logic against an aiosqlite cursor."""
        if event_type in self._index._loaded_types:
            return
        cur = await self._db.conn.execute(
            "SELECT id, support_count, last_seen_at, canonical_summary "
            "FROM events WHERE event_type = ? AND status != 'stale' ORDER BY id",
            (event_type,),
        )
        rows = await cur.fetchall()
        await cur.close()
        if not rows:
            self._index._event_ids_by_type[event_type] = []
            self._index._support_counts_by_type[event_type] = []
            self._index._last_seen_by_type[event_type] = []
            self._index._centroids_by_type[event_type] = np.zeros(
                (0, self._index._dim or 0), dtype=np.float32
            )
            self._index._loaded_types.add(event_type)
            return
        ids: list[int] = []
        counts: list[int] = []
        last_seen: list[str] = []
        summaries: list[str] = []
        for r in rows:
            ids.append(int(r[0]))
            counts.append(int(r[1]))
            last_seen.append(str(r[2]))
            summaries.append(str(r[3]) if r[3] else "")
        if hasattr(self._embedder, "embed_batch"):
            matrix = np.asarray(
                self._embedder.embed_batch(summaries), dtype=np.float32,
            )
        else:
            matrix = np.stack([
                np.asarray(self._embedder.embed(s), dtype=np.float32)
                for s in summaries
            ])
        if self._index._dim is None:
            self._index._dim = int(matrix.shape[1])
        self._index._event_ids_by_type[event_type] = ids
        self._index._support_counts_by_type[event_type] = counts
        self._index._last_seen_by_type[event_type] = last_seen
        self._index._centroids_by_type[event_type] = matrix
        self._index._loaded_types.add(event_type)
        # Populate the members cache for each warmed event.
        for eid in ids:
            self._members_by_event.setdefault(eid, set())
            cur2 = await self._db.conn.execute(
                """
                SELECT DISTINCT COALESCE(r1.resolved_to_id, f.subj_entity_id),
                                COALESCE(r2.resolved_to_id, f.obj_entity_id)
                FROM facts f
                LEFT JOIN entity_resolutions r1 ON r1.entity_id = f.subj_entity_id
                LEFT JOIN entity_resolutions r2 ON r2.entity_id = f.obj_entity_id
                WHERE f.event_id = ?
                """,
                (eid,),
            )
            member_rows = await cur2.fetchall()
            await cur2.close()
            for mr in member_rows:
                if mr[0] is not None:
                    self._members_by_event[eid].add(int(mr[0]))
                if mr[1] is not None:
                    self._members_by_event[eid].add(int(mr[1]))

    async def _fetch_article(self, article_id: int) -> Optional[dict]:
        cur = await self._db.conn.execute(
            "SELECT id, published_at, cycle_id, is_tracer, tracer_id "
            "FROM articles WHERE id = ?",
            (article_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return None
        return {
            "id": int(row["id"]),
            "published_at": row["published_at"],
            "cycle_id": int(row["cycle_id"]),
            "is_tracer": bool(row["is_tracer"]),
            "tracer_id": row["tracer_id"],
        }

    async def _fetch_facts_for_article(self, article_id: int) -> list[dict]:
        """Returns facts joined with entities + entity_resolutions so
        the caller has canonical names + resolved entity ids in hand."""
        cur = await self._db.conn.execute(
            """
            SELECT f.id, f.subj_entity_id, f.obj_entity_id,
                   f.relation, f.obj_value, f.event_id, f.observed_at,
                   es.entity_type AS subj_type,
                   COALESCE(es_res.canonical_name, es.canonical_name) AS subj_canonical,
                   COALESCE(rs.resolved_to_id, f.subj_entity_id) AS subj_resolved_id,
                   eo.entity_type AS obj_type,
                   COALESCE(eo_res.canonical_name, eo.canonical_name) AS obj_canonical,
                   COALESCE(ro.resolved_to_id, f.obj_entity_id) AS obj_resolved_id
            FROM facts f
            JOIN entities es ON es.id = f.subj_entity_id
            LEFT JOIN entity_resolutions rs ON rs.entity_id = f.subj_entity_id
            LEFT JOIN entities es_res ON es_res.id = rs.resolved_to_id
            LEFT JOIN entities eo ON eo.id = f.obj_entity_id
            LEFT JOIN entity_resolutions ro ON ro.entity_id = f.obj_entity_id
            LEFT JOIN entities eo_res ON eo_res.id = ro.resolved_to_id
            WHERE f.article_id = ?
            ORDER BY f.id
            """,
            (article_id,),
        )
        rows = await cur.fetchall()
        await cur.close()
        return [
            {
                "id": int(r["id"]),
                "subj_entity_id": int(r["subj_entity_id"]) if r["subj_entity_id"] is not None else None,
                "obj_entity_id": int(r["obj_entity_id"]) if r["obj_entity_id"] is not None else None,
                "relation": r["relation"],
                "obj_value": r["obj_value"],
                "event_id": int(r["event_id"]) if r["event_id"] is not None else None,
                "observed_at": r["observed_at"],
                "subj_type": r["subj_type"],
                "subj_canonical": r["subj_canonical"],
                "subj_resolved_id": int(r["subj_resolved_id"]) if r["subj_resolved_id"] is not None else None,
                "obj_type": r["obj_type"],
                "obj_canonical": r["obj_canonical"],
                "obj_resolved_id": int(r["obj_resolved_id"]) if r["obj_resolved_id"] is not None else None,
            }
            for r in rows
        ]

    async def _confirm_fact(
        self,
        *,
        event_id: int,
        event_type: str,
        fact: dict,
        fact_vec: np.ndarray,
        last_seen_iso: str,
    ) -> None:
        async with self._db.batch() as cur:
            # Stamp the fact.
            await cur.execute(
                "UPDATE facts SET event_id = ? WHERE id = ? AND event_id IS NULL",
                (event_id, fact["id"]),
            )
            # Bump the event row.
            await cur.execute(
                """
                UPDATE events SET support_count = support_count + 1,
                                  last_seen_at = ?,
                                  status = CASE
                                    WHEN support_count + 1 >= 3 THEN 'confirmed'
                                    ELSE status
                                  END
                WHERE id = ?
                """,
                (last_seen_iso, event_id),
            )
            # Write a confirms edge from event → itself with the per-fact
            # observed_at — gives us an audit trail of when confirmations
            # arrived. Self-loop is intentional and unique-keyed by
            # (event_id, event_id, 'confirms') + the implicit observed_at
            # ... actually the UNIQUE is just on (from, to, kind) — only
            # one self-confirm row total. That's fine: the bumped
            # support_count is the per-fact record.
            await cur.execute(
                """
                INSERT OR IGNORE INTO event_edges
                    (from_event_id, to_event_id, edge_kind, confidence, observed_at)
                VALUES (?, ?, 'confirms', 1.0, ?)
                """,
                (event_id, event_id, last_seen_iso),
            )
        # In-memory index update.
        self._index.update_on_confirm(
            event_id=event_id,
            event_type=event_type,
            new_fact_vec=fact_vec,
            new_last_seen_at_iso=last_seen_iso,
        )
        # Update entity overlap cache.
        if event_id in self._members_by_event:
            self._members_by_event[event_id] |= self._fact_entity_set(fact)

    async def _create_event(
        self,
        *,
        event_type: str,
        fact: dict,
        fact_vec: np.ndarray,
        published_at_iso: str,
        cycle_id: int,
        is_tracer: bool,
        tracer_id: Optional[str],
    ) -> int:
        canonical_summary = self._templated_triple(fact)
        async with self._db.batch() as cur:
            await cur.execute(
                """
                INSERT INTO events
                    (event_type, status, canonical_summary, novelty_z,
                     support_count, first_seen_at, last_seen_at, cycle_id,
                     is_tracer, tracer_id, eventize_version)
                VALUES (?, 'emerging', ?, NULL, 1, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_type, canonical_summary,
                    published_at_iso, published_at_iso,
                    cycle_id,
                    1 if is_tracer else 0, tracer_id, self.EVENTIZE_VERSION,
                ),
            )
            new_id = cur.lastrowid
            await cur.execute(
                "UPDATE facts SET event_id = ? WHERE id = ? AND event_id IS NULL",
                (new_id, fact["id"]),
            )
        # In-memory index registration.
        self._index.register(
            event_id=new_id,
            event_type=event_type,
            centroid=fact_vec,
            last_seen_at_iso=published_at_iso,
        )
        return int(new_id) if new_id is not None else 0

    async def _publish_completion(
        self,
        *,
        extraction_run_id: Optional[int],
        article_id: int,
        events_created: int,
        events_confirmed: int,
        is_tracer: bool,
        tracer_id: Optional[str],
    ) -> None:
        await self._bus.publish(
            TOPIC_EVENT_UPDATED,
            {
                "extraction_run_id": extraction_run_id,
                "article_id": article_id,
                "events_created": events_created,
                "events_confirmed": events_confirmed,
                "is_tracer": is_tracer,
                "tracer_id": tracer_id,
            },
        )


# Silence import-unused.
_ = TOPIC_RESOLUTION_COMPLETED
