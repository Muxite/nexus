"""
## Specification

**Purpose.** Noisy-exploration sister of `app/stages/dkg_trade.py`.
Subscribes to `TOPIC_MARKET_LINKS_UPDATED`; for every link with
`final_score >= cfg.explorer.score_floor` (permissive, default 0.5
vs production 0.8), builds a `Candidate` and runs it through the
configured gate pipeline (`cfg.explorer.gates`, see irene/app/gates/ —
default market_open → dkg_threshold → llm_batch). EVERY candidate
writes an `explorations` audit row tagged with its `gate_status` +
`gate_history`; only survivors (or a small reject sample) publish
`TOPIC_EXPLORATION_CREATED` so `odds_capture.py` starts the odds
time-series.

This stage does NOT trade. It exists to build a labeled dataset for
offline filter/hypothesis backtests. Treat the `explorations` log as
a research dataset, not a verdict on the strategy.

**Public interface.**
- `ExplorerStage(deps: StageDeps)` — requires `deps.ai`, `deps.clob`.
- `await start()` / `await stop()` — no-ops; the bus drives it.
- `await handle(payload: dict)` — bus subscriber.

**Owned tables.** `explorations`. DDL registered with
`db_module.register_schema()` at import time.

**Idempotency.** `UNIQUE(article_id, market_condition_id)` makes
re-fires safe — the pre-pipeline `_exists()` check short-circuits a
re-fire before any gate runs, so the LLM gate isn't re-called.

**Side effects.**
- Reads `market_links`, `events`, `facts`, `articles`, `markets`.
- The `llm_batch` gate calls `deps.ai.classify_batch_for_gate()`
  (charged to the OpenRouter budget); pure-Python gates are free.
- Calls `deps.clob.get_top_of_book()` for the t0 mid/spread snapshot
  (only for rows that will be captured).
- Writes `explorations`. Publishes `TOPIC_EXPLORATION_CREATED` for
  captured rows.

**Failure modes.**
- Score below floor (SQL pre-filter) / event missing / article missing
  / market missing → silent skip, no row written.
- Gate rejection (market dead / DKG below threshold / LLM unrelated) →
  an audit row IS written with `gate_status='rejected'` +
  `rejected_by_gate`, but no capture and no publish (unless picked by
  the reject-capture sample).
- Budget paused / LLM parse error → the `llm_batch` gate rejects the
  affected candidates (audit row, no capture).
- CLOB unavailable at t0 snapshot → `market_mid_t0` / `market_spread_t0`
  are NULL on the row but the row is still written.

**Non-goals.**
- Trading, dashboard, PnL — see `trade.py` / `dkg_trade.py`.
- Strict gates (similarity floor, freshness, expiry, edge, size) —
  the explorer is permissive on admission by design.
- Pre-news / post-news odds capture — that's `odds_capture.py`.
"""

from __future__ import annotations

import logging
import random
from datetime import datetime, timezone
from typing import Optional

from app import db as db_module
from app.cycle import current_cycle_id
from app.deps import StageDeps
from app.topics import TOPIC_EXPLORATION_CREATED, TOPIC_MARKET_LINKS_UPDATED

logger = logging.getLogger(__name__)


_EXPLORER_DDL = [
    """
    CREATE TABLE IF NOT EXISTS explorations (
        id                   INTEGER PRIMARY KEY AUTOINCREMENT,
        article_id           INTEGER NOT NULL,
        event_id             INTEGER,
        market_condition_id  TEXT NOT NULL,
        token_yes            TEXT,
        token_no             TEXT,
        t0_iso               TEXT NOT NULL,
        dkg_final_score      REAL,
        cosine_similarity    REAL,
        freshness            TEXT,
        breaking_score       REAL,
        article_tier         TEXT,
        market_volume_24h    REAL,
        market_spread_t0     REAL,
        market_mid_t0        REAL,
        hours_to_expiry      REAL,
        llm_related          INTEGER NOT NULL CHECK(llm_related IN (0, 1)),
        llm_confidence       REAL,
        llm_reason           TEXT,
        llm_prompt_hash      TEXT,
        capture_status       TEXT NOT NULL DEFAULT 'pending'
            CHECK(capture_status IN ('pending','active','complete','aborted')),
        capture_end_at       TEXT,
        cycle_id             INTEGER NOT NULL,
        is_tracer            INTEGER NOT NULL DEFAULT 0,
        tracer_id            TEXT,
        created_at           TEXT NOT NULL,
        UNIQUE(article_id, market_condition_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_explorations_cycle "
    "ON explorations(cycle_id)",
    "CREATE INDEX IF NOT EXISTS idx_explorations_capture_status "
    "ON explorations(capture_status)",
    "CREATE INDEX IF NOT EXISTS idx_explorations_t0 "
    "ON explorations(t0_iso)",
    "CREATE INDEX IF NOT EXISTS idx_explorations_market "
    "ON explorations(market_condition_id)",
    # Explorer-v2 gate pipeline: every candidate the explorer considers
    # writes a row. `gate_status='passed'` rows are the survivors that go
    # on to odds capture (existing behavior). `gate_status='rejected'`
    # rows are audit records — no odds, just the gate trace.
    # `gate_history` is a JSON list[dict] of {gate, outcome, value,
    # threshold} entries built by the pipeline.
    "ALTER TABLE explorations ADD COLUMN gate_status TEXT "
    "NOT NULL DEFAULT 'passed' "
    "CHECK(gate_status IN ('passed','rejected'))",
    "ALTER TABLE explorations ADD COLUMN rejected_by_gate TEXT",
    "ALTER TABLE explorations ADD COLUMN gate_history TEXT",
    "CREATE INDEX IF NOT EXISTS idx_explorations_gate_status "
    "ON explorations(gate_status)",
    # Explorer-v3 sandbox-feedback columns. All NULLABLE so legacy rows
    # don't need backfill.
    #   capture_end_reason:  why the per-exploration odds_capture loop
    #     exited (window_complete | evicted_by_cap | market_closed |
    #     capture_aborted | error). Sandbox uses this to filter out rows
    #     whose post-news window was cut short.
    #   cluster_id:          group key for "same news event across
    #     related markets" so backtest Sharpe can deduplicate. Derived
    #     from the article_id of the news that triggered the (article,
    #     market) row — articles already get a dedup pass upstream, so
    #     article_id IS the cluster across markets.
    #   capture_sample_reason: explains why a non-passed row got odds
    #     capture anyway. Currently 'rejected_sample' for the 5%
    #     reject-sampling path; NULL means "passed row" or "rejected,
    #     no capture".
    "ALTER TABLE explorations ADD COLUMN capture_end_reason TEXT",
    "ALTER TABLE explorations ADD COLUMN cluster_id TEXT",
    "ALTER TABLE explorations ADD COLUMN capture_sample_reason TEXT",
    "CREATE INDEX IF NOT EXISTS idx_explorations_cluster "
    "ON explorations(cluster_id)",
    # Cycle 29: long-horizon checkpoint bookkeeping (odds_capture's
    # low-frequency loop — see capture_schedule.CAPTURE_LONG_OFFSETS_SECS).
    #   long_capture_status: NULL = pending/not yet due; 'done' = the
    #     final (+7d) checkpoint landed; 'history_unavailable' = the
    #     final checkpoint's grace window passed and prices-history
    #     could not serve it (market delisted, t0 older than the
    #     history span at first deploy, ...). Partial checkpoint
    #     coverage is normal for backlog rows and visible per-row in
    #     odds_observations.
    "ALTER TABLE explorations ADD COLUMN long_capture_status  TEXT",
    "ALTER TABLE explorations ADD COLUMN long_capture_done_at TEXT",
    "CREATE INDEX IF NOT EXISTS idx_explorations_long_status "
    "ON explorations(long_capture_status)",
]
db_module.register_schema("app/stages/explorer.py", _EXPLORER_DDL)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now().isoformat()


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def _hours_until(end_date: Optional[str]) -> Optional[float]:
    dt = _parse_iso(end_date)
    if dt is None:
        return None
    delta = dt - _now()
    return delta.total_seconds() / 3600.0


class ExplorerStage:
    """DKG → pluggable gate pipeline → exploration row + odds capture.

    The gate pipeline is built once at construction from
    `cfg.explorer.gates`. Each (article, market) candidate gets a
    row in `explorations`; survivors get `gate_status='passed'` and
    odds_capture starts, rejections get `gate_status='rejected'` as
    audit-only.
    """

    def __init__(self, deps: StageDeps) -> None:
        if deps.ai is None:
            raise RuntimeError("ExplorerStage requires deps.ai")
        self._db = deps.db
        self._bus = deps.bus
        self._ai = deps.ai
        self._clob = deps.clob
        self._cfg = deps.config.explorer
        from app.gates import build_pipeline
        self._pipeline = build_pipeline(
            self._cfg.gates,
            llm_caller=self._ai.classify_batch_for_gate,
        )

    async def start(self) -> None:
        return

    async def stop(self) -> None:
        return

    async def handle(self, payload: dict) -> None:
        event_id = payload.get("event_id")
        if event_id is None:
            logger.warning("explorer: payload missing event_id: %r", payload)
            return
        links = await self._fetch_high_score_links(int(event_id))
        if not links:
            return
        cycle_id = current_cycle_id()
        is_tracer = bool(payload.get("is_tracer", False))
        tracer_id = payload.get("tracer_id")
        for link in links:
            await self._handle_link(
                event_id=int(event_id),
                link=link,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )

    # ------------------------------------------------------------------
    # Per-link pipeline
    # ------------------------------------------------------------------

    async def _handle_link(
        self,
        *,
        event_id: int,
        link: dict,
        cycle_id: int,
        is_tracer: bool,
        tracer_id: Optional[str],
    ) -> None:
        import json as _json
        from app.gates import Candidate

        market_condition_id = link["market_condition_id"]
        final_score = float(link["final_score"])

        article = await self._fetch_article_for_event(event_id)
        market = await self._fetch_market(market_condition_id)
        if article is None or market is None:
            return

        if await self._exists(article["id"], market_condition_id):
            return

        # Admission cap is on PASSED rows (the ones that consume CLOB
        # polling budget). Rejected rows are cheap audit-only unless they
        # get picked by the reject-sample path below.
        if await self._active_count() >= self._cfg.max_active_explorations:
            logger.debug(
                "explorer: at active cap (%d), skipping (article=%s, market=%s)",
                self._cfg.max_active_explorations,
                article["id"], market_condition_id,
            )
            return

        signal = await self._fetch_signal(article["id"], market_condition_id)
        cosine_similarity = signal.get("similarity") if signal else None
        # Pull cluster_id + cluster-freshness from the dedupe stage's
        # `article_clusters` table — populated upstream and gives us a
        # real BREAKING/EVOLVING/REHASH/STALE_RETURN signal regardless of
        # whether the legacy `signals` row was created. Falls back to the
        # signal-row freshness when the article isn't yet in a cluster.
        cluster = await self._fetch_cluster(article["id"])
        cluster_id = cluster.get("cluster_id") if cluster else None
        freshness = (
            (cluster.get("freshness") if cluster else None)
            or (signal.get("freshness") if signal else None)
        )

        candidate = Candidate(
            article_id=article["id"],
            market_condition_id=market_condition_id,
            final_score=final_score,
            cosine_similarity=cosine_similarity,
            freshness=freshness,
            breaking_score=article.get("breaking_score"),
            article_tier=article.get("tier"),
            market_volume_24h=market.get("volume_24h"),
            article_title=article.get("title") or "",
            article_body=article.get("body") or article.get("summary") or "",
            market_question=market.get("question") or "",
            market_description=market.get("description") or "",
            market_end_date=market.get("end_date"),
            market_active=market.get("active"),
        )

        history_map = await self._pipeline.run([candidate])
        key = f"{candidate.article_id}:{candidate.market_condition_id}"
        history = history_map[key]
        passed = self._pipeline.passed(history)
        rejected_by = None if passed else self._pipeline.rejected_by(history)
        gate_history_json = _json.dumps(
            [d.model_dump(mode="json") for d in history]
        )

        t0_iso = _now_iso()
        capture_end_at_iso = self._compute_capture_end_at(t0_iso)
        hours_to_expiry = _hours_until(market.get("end_date"))

        # Reject-sample path: a configurable fraction of rejected rows
        # still get odds capture so the sandbox can study "what if the
        # gate were looser?". Sampling decision happens here so that
        # `capture_sample_reason` and the captured t0 snapshot both
        # land on the same row.
        sample_reject = False
        if not passed and self._cfg.reject_capture_sample_rate > 0:
            under_cap = (
                await self._active_reject_sample_count()
                < self._cfg.max_active_reject_samples
            )
            if under_cap and random.random() < self._cfg.reject_capture_sample_rate:
                sample_reject = True

        capture_this_row = passed or sample_reject
        capture_sample_reason = "rejected_sample" if sample_reject else None

        if capture_this_row:
            mid_t0, spread_t0 = await self._snapshot_book(market.get("token_yes"))
        else:
            mid_t0, spread_t0 = None, None

        if passed:
            llm_reason = "; ".join(
                f"{d.gate_name}:{d.outcome.value}" for d in history
            )
        elif sample_reject:
            llm_reason = (
                f"rejected_by:{rejected_by}; sampled_for_capture"
                if rejected_by else "rejected; sampled_for_capture"
            )
        else:
            llm_reason = (
                f"rejected_by:{rejected_by}" if rejected_by else "rejected"
            )

        exploration_id = await self._insert_exploration(
            article_id=article["id"],
            event_id=event_id,
            market_condition_id=market_condition_id,
            token_yes=market.get("token_yes"),
            token_no=market.get("token_no"),
            t0_iso=t0_iso,
            dkg_final_score=final_score,
            cosine_similarity=cosine_similarity,
            freshness=freshness,
            breaking_score=article.get("breaking_score"),
            article_tier=article.get("tier"),
            market_volume_24h=market.get("volume_24h"),
            market_spread_t0=spread_t0,
            market_mid_t0=mid_t0,
            hours_to_expiry=hours_to_expiry,
            llm_related=passed,
            llm_confidence=None,
            llm_reason=llm_reason,
            llm_prompt_hash=None,
            capture_end_at=capture_end_at_iso,
            cycle_id=cycle_id,
            is_tracer=is_tracer,
            tracer_id=tracer_id,
            gate_status="passed" if passed else "rejected",
            rejected_by_gate=rejected_by,
            gate_history=gate_history_json,
            cluster_id=cluster_id,
            capture_sample_reason=capture_sample_reason,
            start_capture=capture_this_row,
        )
        if exploration_id is None:
            return
        if not capture_this_row:
            return  # audit row written; no odds capture

        await self._bus.publish(
            TOPIC_EXPLORATION_CREATED,
            {
                "exploration_id": exploration_id,
                "article_id": article["id"],
                "market_condition_id": market_condition_id,
                "token_yes": market.get("token_yes"),
                "token_no": market.get("token_no"),
                "t0_iso": t0_iso,
                "capture_end_at": capture_end_at_iso,
                "is_tracer": is_tracer,
                "tracer_id": tracer_id,
            },
        )

    def _compute_capture_end_at(self, t0_iso: str) -> str:
        dt = _parse_iso(t0_iso) or _now()
        end = datetime.fromtimestamp(
            dt.timestamp() + self._cfg.capture_post_secs, tz=timezone.utc
        )
        return end.isoformat()

    async def _snapshot_book(
        self, token_id: Optional[str]
    ) -> tuple[Optional[float], Optional[float]]:
        """Return `(mid, spread)` at t0 — best effort. NULL on any failure."""
        if self._clob is None or not token_id:
            return None, None
        try:
            book = await self._clob.get_top_of_book(str(token_id))
        except Exception:  # noqa: BLE001 — never raise out of handle()
            logger.warning("explorer: get_top_of_book(%s) raised", token_id)
            return None, None
        if book is None:
            return None, None
        bid, ask = book
        mid = (bid + ask) / 2.0
        spread = ask - bid
        return mid, spread

    # ------------------------------------------------------------------
    # Lookups
    # ------------------------------------------------------------------

    async def _fetch_high_score_links(self, event_id: int) -> list[dict]:
        # lint: allow-extraction-table-ref — explorer mirrors the DKG path's
        # `market_links` read by design.
        cur = await self._db.conn.execute(
            "SELECT market_condition_id, final_score, directness_score "
            "FROM market_links "
            "WHERE event_id = ? AND final_score >= ? "
            "ORDER BY final_score DESC",
            (event_id, float(self._cfg.score_floor)),
        )
        rows = await cur.fetchall()
        await cur.close()
        return [
            {
                "market_condition_id": str(r["market_condition_id"]),
                "final_score": float(r["final_score"]),
                # `directness_score` IS the event-summary ↔ market-question
                # cosine on the DKG side; we log it alongside the
                # article ↔ market signal cosine below for downstream
                # slicing.
                "directness_score": (
                    float(r["directness_score"])
                    if r["directness_score"] is not None else None
                ),
            }
            for r in rows
        ]

    async def _fetch_article_for_event(
        self, event_id: int
    ) -> Optional[dict]:
        """Walk facts → articles to the most-recent article behind this
        event. Returns title + body (preferring full body over summary)
        + tier + breaking_score. All ingest-added columns (`body`,
        `tier`, `breaking_score`) are guaranteed present by ingest.py
        DDL/migrate.
        """
        # lint: allow-extraction-table-ref — explorer walks DKG path tables.
        cur = await self._db.conn.execute(
            """
            SELECT a.id, a.title, a.summary, a.body,
                   a.tier, a.breaking_score
            FROM articles a
            JOIN facts f ON f.article_id = a.id
            WHERE f.event_id = ?
            ORDER BY a.published_at DESC, f.id DESC
            LIMIT 1
            """,
            (event_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return None
        return {
            "id": int(row["id"]),
            "title": row["title"],
            "summary": row["summary"],
            "body": row["body"],
            "tier": row["tier"],
            "breaking_score": (
                float(row["breaking_score"])
                if row["breaking_score"] is not None else None
            ),
        }

    async def _fetch_cluster(self, article_id: int) -> Optional[dict]:
        """Return `{cluster_id, freshness}` for the article's dedupe
        cluster, or None if the article hasn't been clustered yet. Used
        to populate the exploration row's `cluster_id` and `freshness`
        without depending on a `signals` row existing.
        """
        cur = await self._db.conn.execute(
            "SELECT cm.cluster_id, ac.freshness "
            "FROM article_cluster_members cm "
            "JOIN article_clusters ac ON ac.id = cm.cluster_id "
            "WHERE cm.article_id = ? LIMIT 1",
            (article_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return None
        return {
            "cluster_id": (
                str(row["cluster_id"])
                if row["cluster_id"] is not None else None
            ),
            "freshness": row["freshness"],
        }

    async def _fetch_signal(
        self, article_id: int, market_condition_id: str
    ) -> Optional[dict]:
        """Return the signals row for this (article, market) pair if
        present. Returns the most-recent by id when there are multiple
        (UNIQUE constraint normally prevents this, but defensive). NULL
        on DKG-only paths where no signal was created."""
        cur = await self._db.conn.execute(
            "SELECT similarity, freshness FROM signals "
            "WHERE article_id=? AND market_id=? "
            "ORDER BY id DESC LIMIT 1",
            (article_id, market_condition_id),
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return None
        return {
            "similarity": (
                float(row["similarity"]) if row["similarity"] is not None
                else None
            ),
            "freshness": row["freshness"],
        }

    async def _fetch_market(
        self, market_condition_id: str
    ) -> Optional[dict]:
        cur = await self._db.conn.execute(
            "SELECT question, description, token_yes, token_no, "
            "       active, closed_at, end_date, volume_usdc "
            "FROM markets WHERE condition_id=?",
            (market_condition_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return None
        return {
            "question": row["question"],
            "description": row["description"],
            "token_yes": row["token_yes"],
            "token_no": row["token_no"],
            "active": row["active"],
            "closed_at": row["closed_at"],
            "end_date": row["end_date"],
            "volume_24h": (
                float(row["volume_usdc"]) if row["volume_usdc"] is not None
                else None
            ),
        }

    async def _exists(
        self, article_id: int, market_condition_id: str
    ) -> bool:
        cur = await self._db.conn.execute(
            "SELECT 1 FROM explorations "
            "WHERE article_id=? AND market_condition_id=? LIMIT 1",
            (article_id, market_condition_id),
        )
        row = await cur.fetchone()
        await cur.close()
        return row is not None

    async def _active_count(self) -> int:
        cur = await self._db.conn.execute(
            "SELECT COUNT(*) FROM explorations WHERE capture_status = 'active'"
        )
        row = await cur.fetchone()
        await cur.close()
        return int(row[0]) if row else 0

    async def _active_reject_sample_count(self) -> int:
        """How many `capture_sample_reason='rejected_sample'` rows are
        currently in flight. Gated separately from passed-row capacity
        so a burst of rejects can't starve passed captures."""
        cur = await self._db.conn.execute(
            "SELECT COUNT(*) FROM explorations "
            "WHERE capture_status IN ('pending','active') "
            "AND capture_sample_reason = 'rejected_sample'"
        )
        row = await cur.fetchone()
        await cur.close()
        return int(row[0]) if row else 0

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    async def _insert_exploration(
        self,
        *,
        article_id: int,
        event_id: Optional[int],
        market_condition_id: str,
        token_yes: Optional[str],
        token_no: Optional[str],
        t0_iso: str,
        dkg_final_score: Optional[float],
        cosine_similarity: Optional[float],
        freshness: Optional[str],
        breaking_score: Optional[float],
        article_tier: Optional[str],
        market_volume_24h: Optional[float],
        market_spread_t0: Optional[float],
        market_mid_t0: Optional[float],
        hours_to_expiry: Optional[float],
        llm_related: bool,
        llm_confidence: Optional[float],
        llm_reason: Optional[str],
        llm_prompt_hash: Optional[str],
        capture_end_at: str,
        cycle_id: int,
        is_tracer: bool,
        tracer_id: Optional[str],
        gate_status: str = "passed",
        rejected_by_gate: Optional[str] = None,
        gate_history: Optional[str] = None,
        cluster_id: Optional[str] = None,
        capture_sample_reason: Optional[str] = None,
        start_capture: bool = True,
    ) -> Optional[int]:
        # `start_capture=True` rows (gate-passed OR reject-sampled) get
        # `capture_status='pending'` so odds_capture picks them up.
        # `start_capture=False` rejected rows are audit-only: status
        # 'complete' so odds_capture never picks them up.
        capture_status = "pending" if start_capture else "complete"
        async with self._db.batch() as cur:
            await cur.execute(
                """
                INSERT OR IGNORE INTO explorations
                    (article_id, event_id, market_condition_id,
                     token_yes, token_no, t0_iso,
                     dkg_final_score, cosine_similarity, freshness,
                     breaking_score, article_tier,
                     market_volume_24h, market_spread_t0, market_mid_t0,
                     hours_to_expiry,
                     llm_related, llm_confidence, llm_reason,
                     llm_prompt_hash,
                     capture_status, capture_end_at,
                     cycle_id, is_tracer, tracer_id, created_at,
                     gate_status, rejected_by_gate, gate_history,
                     cluster_id, capture_sample_reason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?)
                """,
                (
                    article_id, event_id, market_condition_id,
                    token_yes, token_no, t0_iso,
                    dkg_final_score, cosine_similarity, freshness,
                    breaking_score, article_tier,
                    market_volume_24h, market_spread_t0, market_mid_t0,
                    hours_to_expiry,
                    1 if llm_related else 0, llm_confidence, llm_reason,
                    llm_prompt_hash,
                    capture_status, capture_end_at,
                    cycle_id, 1 if is_tracer else 0, tracer_id, _now_iso(),
                    gate_status, rejected_by_gate, gate_history,
                    cluster_id, capture_sample_reason,
                ),
            )
            if cur.rowcount == 0:
                return None
            return cur.lastrowid
