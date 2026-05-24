"""
## Specification

**Purpose.** P3 chunk 2's event-builder stage. Subscribes to
`TOPIC_RESOLUTION_COMPLETED`; for each just-resolved article, clusters
its facts into canonical **events** (real-world happenings reported
across one-or-more articles) and writes/updates rows in `events` +
`event_edges` + the new `facts.event_id` column. Publishes
`TOPIC_EVENT_UPDATED` on completion.

**This is a CONTRACT-FIRST stub.** Per master-plan §"Phase 2+" P3, the
research session — `docs/papers/eventization/notes.md` — must answer
three questions before any clustering logic lands:
1. Fact-clustering similarity metric (Jaccard on entity sets vs cosine
   on the templated triple vs hybrid).
2. Time window for "same event" (probable: 24h, validated on real
   corpus).
3. Tie-break when a new fact is similar to multiple existing events
   (recommended: prefer most-recent, configurable).

Until that doc lands, `handle()` raises `NotImplementedError`. The
DDL, the topic, and the public surface ARE locked here so chunk 2's
implementation PR can be reviewed without touching the contract.

**The P2/P3 hard invariant (inherited).** The trade path NEVER reads
`facts` / `entities` / `entity_aliases` / `extraction_runs` /
`entity_resolutions` / `events` / `event_edges`. Eventize lives
strictly on the DKG side; the lint guards it (see
`scripts/lint/no_facts_in_trade_path.py`).

**Public interface.**
- `EventizeStage(deps: StageDeps)` — needs `db` + `bus` + `embedder`
  (the clustering similarity will rely on a sentence-transformer
  embedding of the canonical_summary; design-locked in the research
  session).
- `await start()` — loads any rule-table / threshold config from
  disk; lazily warms an in-memory event index (the same
  `MarketIndex`/`EntityIndex` pattern).
- `await stop()` — idempotent.
- `await handle(payload)` — subscriber on
  `TOPIC_RESOLUTION_COMPLETED`. Payload shape per
  `docs/p3_contracts.md` §3 (extraction_run_id, article_id,
  entities_resolved, merges, is_tracer, tracer_id).
- `EVENTIZE_VERSION` — class constant. Bumps when the clustering
  algorithm changes; lets a future re-pass re-bucket facts without
  losing the history.

**Owned tables** (`docs/modularization.md` §4): `events`,
`event_edges`. Also writes `facts.event_id` (column add — `facts` is
otherwise owned by `extract.py`; this is the documented exception per
the DKG spec's "graph-upsert" service boundary).

**Invariants (target — to be enforced when handle() ships).**
- One `events` row per canonical event, identified by the chosen
  clustering rule.
- `facts.event_id` is monotonic: once stamped, the resolver only
  re-stamps it to NULL via a versioned re-pass, never silently.
- `event_edges` is append-only with UNIQUE on
  `(from_event_id, to_event_id, edge_kind)`. Re-running the same
  resolution is a no-op.
- Every row carries `cycle_id` from the originating article (NOT a
  new cycle for the eventize pass — the cycle is the article's).

**Side effects.** Reads `facts` + `entities` + `entity_resolutions`.
Writes `events`, `event_edges`, `facts.event_id`. Publishes
`TOPIC_EVENT_UPDATED`.

**Failure modes.** TBD — to be enumerated when handle() ships.

**Non-goals.**
- Doesn't score events against markets (that's `link.py` in the next
  half of chunk 2).
- Doesn't run the LLM. Eventize is deterministic clustering over
  already-extracted facts.
- Doesn't mutate `facts` rows other than `event_id`.

**Open research questions (tracked in eventization/notes.md when written).**
- What similarity metric clusters (FDA, decides, Pfizer) and
  (FDA, denies, Pfizer) as the same event without over-merging
  unrelated FDA decisions?
- What time window admits "confirms" relationships (later facts that
  reinforce an earlier event) without admitting unrelated reports
  that share entities by coincidence?
- How does eventize behave when extraction lands out-of-order
  (e.g., facts about a later development arrive before the original
  event's facts)?
"""

from __future__ import annotations

import logging
from typing import Optional

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
    # facts.event_id is a column-add on extract.py's table. We add it
    # here (idempotent CREATE-then-ALTER pattern). Two-step so the
    # schema_migrations row tracks it independently.
    """
    CREATE TABLE IF NOT EXISTS _eventize_facts_column_marker (
        applied INTEGER PRIMARY KEY DEFAULT 1
    )
    """,
]


def _maybe_add_facts_event_id(conn) -> None:
    """ALTER facts ADD COLUMN event_id idempotently.

    Called from `register_schema`'s applier path. The marker table above
    only exists if this migration ran at least once; we still check the
    actual column to handle the case where the marker was lost.
    """
    cur = conn.execute("PRAGMA table_info(facts)")
    cols = [row[1] for row in cur.fetchall()]
    if "event_id" in cols:
        return
    conn.execute("ALTER TABLE facts ADD COLUMN event_id INTEGER REFERENCES events(id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_facts_event_id ON facts(event_id)")


db_module.register_schema("app/stages/eventize.py", _EVENTIZE_DDL)


# ---------------------------------------------------------------------------
# Stage shell
# ---------------------------------------------------------------------------


class EventizeStage:
    EVENTIZE_VERSION: str = "v1"

    def __init__(self, deps: StageDeps) -> None:
        if deps.embedder is None:
            # Eventize depends on the embedder for the clustering metric
            # (see notes.md when written). Fail loudly at construction.
            raise RuntimeError(
                "EventizeStage requires deps.embedder (similarity-based clustering)"
            )
        self._db = deps.db
        self._bus = deps.bus
        self._embedder = deps.embedder
        self._stopped = False

    async def start(self) -> None:
        # Belt-and-braces: also fire the ALTER on start in case the
        # registry-based applier order doesn't catch every situation
        # (e.g. ephemeral test DBs that bypass it).
        if self._db.conn is not None:
            cur = await self._db.conn.execute("PRAGMA table_info(facts)")
            cols = [row[1] async for row in cur]
            await cur.close()
            if "event_id" not in cols:
                await self._db.conn.execute(
                    "ALTER TABLE facts ADD COLUMN event_id INTEGER REFERENCES events(id)"
                )
                await self._db.conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_facts_event_id ON facts(event_id)"
                )
                await self._db.conn.commit()

    async def stop(self) -> None:
        self._stopped = True

    async def handle(self, payload: dict) -> None:
        # Subscriber stub. Until docs/papers/eventization/notes.md lands
        # the clustering rule, we don't write events — but we still
        # acknowledge the resolution so the chain is testable.
        _ = TOPIC_RESOLUTION_COMPLETED  # silence lint
        raise NotImplementedError(
            "EventizeStage.handle is awaiting the research-session "
            "deliverable docs/papers/eventization/notes.md before "
            "clustering logic ships (master-plan §'Phase 2+' P3)."
        )


# Silence "imported but unused" — TOPIC_EVENT_UPDATED is the
# publish-side constant the future handle() will use.
_ = TOPIC_EVENT_UPDATED
