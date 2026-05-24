"""
## Specification

**Purpose.** P3 chunk 2b's event→market linker. Subscribes to
`TOPIC_EVENT_UPDATED`; for each updated event, scores it against every
active market via four feature components — `directness`, `novelty`,
`deadline_fit`, `resolution_fit` — and writes the top-K candidates to
the `market_links` table. Publishes `TOPIC_MARKET_LINKS_UPDATED`.

Scoring formulas locked in `docs/papers/eventization/notes.md`
§"Problem 3":
- **directness_score**: `cosine(event_summary_emb, market_question_emb)`
  gated by entity-overlap bonus (1.0 if market.question mentions an
  entity from the event, 0.5 otherwise).
- **novelty_score**: `clamp(1 / (1 + log(support_count)), 0, 1)`.
- **deadline_fit_score**: `clamp(1 - days_to_deadline / 30, 0, 1)`.
- **resolution_fit_score**: per-event-type polarity lookup; falls back
  to 0.5 (neutral) on unknown types.
- **final_score**: arithmetic mean of the four components (equal
  weights initially; tunable later).

**The P2/P3 hard invariant** — trade path NEVER reads `facts` /
`entities` / `entity_aliases` / `extraction_runs` /
`entity_resolutions` / `events` / `event_edges` / **`market_links`**.
This stage writes the bridge between the DKG and the trade path; the
DKG path will read from `market_links` (the lint forbidden list will
NOT include `market_links` once chunk 3's dual-path trader needs it).

**Public interface.**
- `LinkerStage(deps: StageDeps)` — needs `db` + `bus` + `embedder`.
- `await start()` — loads all market embeddings into memory (~16 MB
  for 10k markets at 384-d).
- `await stop()` — idempotent.
- `await handle(payload)` — subscriber on `TOPIC_EVENT_UPDATED`.
- `LINKER_VERSION` — bumps when the scoring formula changes.

**Owned tables**: `market_links`.

**Invariants.**
- One `market_links` row per `(event_id, market_condition_id, link_class)`
  tuple under the current `LINKER_VERSION`.
- Top-K filter applied at write time (default K=10) — we keep only the
  highest-final-score links per event so the table doesn't bloat.
- Every row carries `cycle_id` from the event.

**Side effects.** Reads `events` + `markets`. Writes `market_links`.
Publishes `TOPIC_MARKET_LINKS_UPDATED`.

**Failure modes.**
- `RuntimeError` at construction if `deps.embedder` is None.
- `RuntimeError` at start() if no market embeddings exist (the
  in-memory matrix would be empty and every score 0).

**Non-goals.**
- Doesn't compute the trade decision. That's the dual-path trader
  (chunk 3), which reads `market_links.final_score`.
- Doesn't poll Polymarket. Market state is whatever's currently in
  the `markets` table at link-time.
- Doesn't refresh stale links when the embedder version changes
  (LINKER_VERSION bump signals a new pass).
"""

from __future__ import annotations

import json
import logging
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import numpy as np
import yaml

from app import db as db_module
from app.deps import StageDeps
from app.topics import TOPIC_EVENT_UPDATED, TOPIC_MARKET_LINKS_UPDATED

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------


_LINK_DDL = [
    """
    CREATE TABLE IF NOT EXISTS market_links (
        id                   INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id             INTEGER NOT NULL REFERENCES events(id),
        market_condition_id  TEXT NOT NULL REFERENCES markets(condition_id),
        link_class           TEXT NOT NULL CHECK(link_class IN
                                 ('direct','indirect','narrative')),
        directness_score     REAL NOT NULL,
        novelty_score        REAL NOT NULL,
        deadline_fit_score   REAL NOT NULL,
        resolution_fit_score REAL NOT NULL,
        final_score          REAL NOT NULL,
        causal_path          TEXT,
        linker_version       TEXT NOT NULL DEFAULT 'v1',
        cycle_id             INTEGER NOT NULL,
        created_at           TEXT NOT NULL,
        UNIQUE(event_id, market_condition_id, link_class, linker_version)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_market_links_event ON market_links(event_id)",
    "CREATE INDEX IF NOT EXISTS idx_market_links_market ON market_links(market_condition_id)",
    "CREATE INDEX IF NOT EXISTS idx_market_links_final_score ON market_links(final_score DESC)",
]

db_module.register_schema("app/stages/link.py", _LINK_DDL)


# ---------------------------------------------------------------------------
# Resolution-fit polarity table
# ---------------------------------------------------------------------------


# Per event_type, what does a polarity-matching event suggest about the
# market direction? Each value is the resolution_fit_score for a
# "matching" polarity. Unknown event_types get 0.5 (neutral).
_RESOLUTION_FIT_TABLE: dict[str, float] = {
    "REGULATORY_DECISION": 0.9,
    "ELECTION": 0.95,
    "RATE_CHANGE": 0.9,
    "MERGER": 0.85,
    "LAWSUIT": 0.75,
    "PRODUCT_LAUNCH": 0.7,
    "NOMINATION": 0.8,
    "RESIGNATION": 0.7,
    "ANNOUNCEMENT": 0.6,
    "EARNINGS": 0.75,
    "TRIAL": 0.85,
    "UNCLASSIFIED": 0.5,
}


# ---------------------------------------------------------------------------
# Scoring math (pure functions)
# ---------------------------------------------------------------------------


def directness_score(
    event_emb: np.ndarray,
    market_emb: np.ndarray,
    *,
    entity_overlap: bool,
) -> float:
    """Cosine on summary↔question, scaled by the entity-overlap bonus."""
    cos = float(np.clip(np.dot(event_emb, market_emb), 0.0, 1.0))
    bonus = 1.0 if entity_overlap else 0.5
    return cos * bonus


def novelty_score(support_count: int) -> float:
    """Decay: first confirmation = 1.0, decays with log(support_count).
    Cheap closed-form; doesn't need the per-type baseline at this stage."""
    if support_count <= 1:
        return 1.0
    return float(max(0.0, min(1.0, 1.0 / (1.0 + math.log(support_count)))))


def deadline_fit_score(event_last_seen_iso: str, market_end_iso: Optional[str]) -> float:
    """Closer-to-deadline = higher score. >30 days out = 0.
    Past-deadline events return 1.0 (already-resolved markets are
    filtered upstream of trade; here we just don't penalise)."""
    if not market_end_iso:
        return 0.5  # unknown deadline
    try:
        event_dt = datetime.fromisoformat(event_last_seen_iso)
        market_dt = datetime.fromisoformat(market_end_iso)
    except (TypeError, ValueError):
        return 0.5
    days = (market_dt - event_dt).total_seconds() / 86400.0
    if days < 0:
        return 1.0  # already past — upstream gate decides if we trade
    if days > 30:
        return 0.0
    return float(max(0.0, min(1.0, 1.0 - days / 30.0)))


def resolution_fit_score(event_type: str) -> float:
    """Per-type polarity score, defaulting to 0.5 (neutral) on unknown."""
    return _RESOLUTION_FIT_TABLE.get(event_type, 0.5)


def final_score(d: float, n: float, df: float, rf: float) -> float:
    return (d + n + df + rf) / 4.0


# ---------------------------------------------------------------------------
# Stage
# ---------------------------------------------------------------------------


class LinkerStage:
    LINKER_VERSION: str = "v1"
    TOP_K_LINKS: int = 10
    # Markets with cosine below this never become candidates — saves write
    # bloat for marginal pairs.
    CANDIDATE_FLOOR: float = 0.20

    def __init__(self, deps: StageDeps) -> None:
        if deps.embedder is None:
            raise RuntimeError("LinkerStage requires deps.embedder")
        self._db = deps.db
        self._bus = deps.bus
        self._embedder = deps.embedder
        self._stopped = False
        # In-memory market index.
        self._market_ids: list[str] = []
        self._market_questions: list[str] = []
        self._market_end_dates: list[Optional[str]] = []
        self._market_matrix: Optional[np.ndarray] = None

    async def start(self) -> None:
        """Load all market embeddings + metadata into memory once."""
        cur = await self._db.conn.execute(
            "SELECT condition_id, question, embedding, end_date "
            "FROM markets WHERE embedding IS NOT NULL AND active = 1"
        )
        rows = await cur.fetchall()
        await cur.close()
        if not rows:
            raise RuntimeError(
                "LinkerStage: no active markets with embeddings — refusing "
                "to start (every score would be zero)."
            )
        ids: list[str] = []
        questions: list[str] = []
        end_dates: list[Optional[str]] = []
        vecs: list[np.ndarray] = []
        for r in rows:
            ids.append(str(r["condition_id"]))
            questions.append(str(r["question"]))
            end_dates.append(r["end_date"])
            vecs.append(np.frombuffer(r["embedding"], dtype=np.float32))
        self._market_ids = ids
        self._market_questions = questions
        self._market_end_dates = end_dates
        self._market_matrix = np.stack(vecs).astype(np.float32, copy=False)
        logger.info(
            "linker: loaded %d active markets (%.1f MB)",
            len(ids), self._market_matrix.nbytes / 1024 / 1024,
        )

    async def stop(self) -> None:
        self._stopped = True

    async def handle(self, payload: dict) -> None:
        """For each event hinted in the payload, score it against all
        active markets and persist the top-K above the floor."""
        article_id = payload.get("article_id")
        if article_id is None:
            return
        events = await self._fetch_recent_events_for_article(int(article_id))
        if not events:
            return
        for ev in events:
            await self._score_and_persist(ev)

    async def _fetch_recent_events_for_article(self, article_id: int) -> list[dict]:
        """All events that received facts from this article. May be one
        or many (typical articles → 1-3 events)."""
        cur = await self._db.conn.execute(
            """
            SELECT DISTINCT e.id, e.event_type, e.canonical_summary,
                            e.support_count, e.last_seen_at, e.cycle_id
            FROM events e
            JOIN facts f ON f.event_id = e.id
            WHERE f.article_id = ?
            """,
            (article_id,),
        )
        rows = await cur.fetchall()
        await cur.close()
        return [
            {
                "id": int(r["id"]),
                "event_type": r["event_type"],
                "canonical_summary": r["canonical_summary"] or "",
                "support_count": int(r["support_count"]),
                "last_seen_at": r["last_seen_at"],
                "cycle_id": int(r["cycle_id"]),
            }
            for r in rows
        ]

    async def _score_event_against_markets(
        self, event: dict
    ) -> list[dict]:
        """Returns a list of scored candidates (one per market) sorted
        by final_score desc. Caller filters by TOP_K_LINKS."""
        if self._market_matrix is None:
            return []
        event_emb = np.asarray(
            self._embedder.embed(event["canonical_summary"]),
            dtype=np.float32,
        )
        # 1) Vectorised cosine against all markets.
        cosines = self._market_matrix @ event_emb  # shape (N,)
        # 2) Apply candidate floor — drop the long tail before per-row work.
        candidates_idx = np.where(cosines >= self.CANDIDATE_FLOOR)[0]
        if candidates_idx.size == 0:
            return []
        # 3) For each candidate, compute the 4 components.
        # Entity-overlap is approximated by a substring check on the
        # event summary words in the market question. Cheap but
        # surprisingly effective on real headlines.
        event_tokens = self._summary_tokens(event["canonical_summary"])
        nov = novelty_score(event["support_count"])
        rf = resolution_fit_score(event["event_type"])

        out: list[dict] = []
        for idx in candidates_idx:
            cos = float(cosines[idx])
            market_q = self._market_questions[idx]
            overlap = self._market_question_mentions(event_tokens, market_q)
            d = directness_score(
                event_emb, self._market_matrix[idx],
                entity_overlap=overlap,
            )
            df = deadline_fit_score(event["last_seen_at"], self._market_end_dates[idx])
            f = final_score(d, nov, df, rf)
            out.append({
                "market_condition_id": self._market_ids[idx],
                "directness_score": d,
                "novelty_score": nov,
                "deadline_fit_score": df,
                "resolution_fit_score": rf,
                "final_score": f,
                "raw_cosine": cos,
                "entity_overlap": overlap,
            })
        out.sort(key=lambda x: x["final_score"], reverse=True)
        return out[: self.TOP_K_LINKS]

    async def _score_and_persist(self, event: dict) -> None:
        scored = await self._score_event_against_markets(event)
        if not scored:
            return
        now = datetime.now(timezone.utc).isoformat()
        async with self._db.batch() as cur:
            for s in scored:
                # link_class is a placeholder until chunk 3 needs a real
                # taxonomy (direct = high-directness, narrative = low).
                link_class = "direct" if s["directness_score"] >= 0.5 else "narrative"
                await cur.execute(
                    """
                    INSERT OR IGNORE INTO market_links
                        (event_id, market_condition_id, link_class,
                         directness_score, novelty_score,
                         deadline_fit_score, resolution_fit_score,
                         final_score, causal_path, linker_version,
                         cycle_id, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event["id"], s["market_condition_id"], link_class,
                        s["directness_score"], s["novelty_score"],
                        s["deadline_fit_score"], s["resolution_fit_score"],
                        s["final_score"],
                        json.dumps({"raw_cosine": s["raw_cosine"],
                                    "entity_overlap": s["entity_overlap"]}),
                        self.LINKER_VERSION, event["cycle_id"], now,
                    ),
                )
        await self._bus.publish(
            TOPIC_MARKET_LINKS_UPDATED,
            {
                "event_id": event["id"],
                "links_written": len(scored),
                "top_final_score": scored[0]["final_score"] if scored else 0.0,
            },
        )

    @staticmethod
    def _summary_tokens(summary: str) -> set[str]:
        """Tokenise a canonical_summary into a set of lowercase non-stop
        words for substring-overlap with market.question. Stop words
        are the ontology relations themselves (they appear in every
        summary)."""
        stop = {
            "announces", "decides", "elects", "acquires", "launches",
            "accuses", "signs", "changes", "resigns", "nominates",
            "of", "the", "a", "an", "to", "in", "on", "for", "by",
            "and", "or", "with",
        }
        toks = {t.strip(".,'\"!?():;").lower() for t in summary.split()}
        return {t for t in toks if t and t not in stop and len(t) > 2}

    @staticmethod
    def _market_question_mentions(
        event_tokens: set[str], market_question: str
    ) -> bool:
        if not event_tokens:
            return False
        q = market_question.lower()
        for t in event_tokens:
            if t in q:
                return True
        return False


# Silence import-unused.
_ = TOPIC_EVENT_UPDATED
