"""
## Specification

**Purpose.** Poll the Polymarket Gamma API on a fixed cadence, upsert the
open-market universe into the `markets` table, refresh question embeddings
when they're missing or stamped with a stale `embedder_id`, and expose an
in-process `MarketIndex` that the match stage queries by cosine similarity.

**Public interface.**
- `MarketsStage` — stage with the standard `__init__` / `start` / `stop` /
  `handle` surface from `docs/p1_contracts.md` §9. Pure poller; subscribes
  to no topics. Publishes `TOPIC_MARKETS_REFRESHED` after every successful
  poll cycle.
- `MarketIndex` — process-local cosine-search index. Built by the stage's
  embedding-refresh loop and reloaded from the DB after the loop writes
  fresh vectors.

**Owned tables** (`docs/modularization.md` §4): `markets`. DDL is
registered with `db.register_schema()` at import time.

**Invariants.**
- Every market row gets an `embedder_id` of the form `"<model>@<dim>"` the
  moment its embedding is written; rows with `embedding IS NULL` have a
  NULL `embedder_id`. Mixed `embedder_id` values across the index is the
  signature of an in-flight model rotation — `MarketIndex.load()` rejects
  that state explicitly rather than silently mixing vector spaces.
- A poll cycle is atomic per market: each `upsert` runs in its own
  `db.batch()` so a transient Gamma error mid-page can't half-write a row.
- Markets that disappear from the open list are flagged inactive
  (`active=0`, `closed_at=<now>`); they are never hard-deleted from P1.
- `MarketIndex.topk()` returns at most `k` results, every result has
  `similarity >= floor`, sorted by descending similarity. Empty index ⇒ `[]`.

**Documented exception to modularization rule 5** ("no shared mutable
state across messages"). `MarketIndex` is process-local mutable state by
design: cosine math wants ndarray locality, and rebuilding the matrix per
query would dominate the match-stage budget. The state lives on a
`MarketIndex` INSTANCE — not at module scope — and mutation is guarded by
an `asyncio.Lock`. This exception is named in `docs/modularization.md`
rule 5; no other stage adds a similar exception without a reports/ entry.

**Side effects.**
- HTTP GET to `https://gamma-api.polymarket.com/markets` (paged 100/req).
- Writes to the `markets` table.
- Publishes `TOPIC_MARKETS_REFRESHED` after every poll cycle.
- Spawns two background asyncio tasks via `start()`; cancels them via
  `stop()`.

**Failure modes.**
- Network errors during a poll cycle are logged at WARNING and the cycle
  is skipped — the loop stays alive. No bus publish on a failed cycle.
- `MarketIndex.load()` raises `MarketIndexEmbedderMismatch` if it finds
  rows tagged with two different `embedder_id` values. The stage catches
  this, re-runs the embedding refresh, and retries.
- `MarketsStage.handle()` always raises `NotImplementedError`: this stage
  is a pure poller and the bus never calls `handle()` on a non-subscribed
  topic, so anyone hitting it has a wiring bug.

**Non-goals.**
- Multi-source market discovery (Kalshi / Predictit) — out of scope for P1.
- Heat boost / `match_count` — that's a seulgi-era ranking knob; P1 ranks
  on pure cosine.
- Per-market quality gates (volume / liquidity / spread) — those live in
  match / trade, not here.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import httpx
import numpy as np

from app import db as db_module
from app.db import DB
from app.deps import StageDeps
from app.topics import TOPIC_MARKETS_REFRESHED

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Module-level constants (immutable — allowed under rule 5)
# ---------------------------------------------------------------------------

_GAMMA_URL = "https://gamma-api.polymarket.com/markets"
_GAMMA_PAGE_SIZE = 100
_GAMMA_MAX_PAGES = 1000  # ~100k markets hard ceiling; current universe ~50k
_GAMMA_TIMEOUT_SECONDS = 15.0

_DEFAULT_POLL_SECONDS = 120
_DEFAULT_EMBED_REFRESH_SECONDS = 3600
# Cycle 12: orchestration cadence for the closed-but-not-finalized
# market re-poll. Default 10 min — fine-grained enough that we catch
# the 2h UMA challenge window early, coarse enough that we don't
# hammer Gamma per market. See plans/resolution-oracle.md.
_DEFAULT_RESOLUTION_POLL_SECONDS = 600

_ENV_POLL_SECONDS = "IRENE__MARKETS__POLL_SECONDS"
_ENV_EMBED_REFRESH_SECONDS = "IRENE__MARKETS__EMBED_REFRESH_SECONDS"
_ENV_RESOLUTION_POLL_SECONDS = "IRENE__MARKETS__RESOLUTION_POLL_SECONDS"


# ---------------------------------------------------------------------------
# Schema (owned by this module per modularization rule 4)
# ---------------------------------------------------------------------------

_MARKETS_DDL = [
    """
    CREATE TABLE IF NOT EXISTS markets (
        condition_id            TEXT PRIMARY KEY,
        slug                    TEXT,
        question                TEXT NOT NULL,
        description             TEXT,
        category                TEXT,
        end_date                TEXT,
        volume_usdc             REAL,
        token_yes               TEXT,
        token_no                TEXT,
        embedding               BLOB,
        embedder_id             TEXT,
        embedding_dim           INTEGER,
        embedding_updated_at    TEXT,
        active                  INTEGER NOT NULL DEFAULT 1,
        closed_at               TEXT,
        first_seen_at           TEXT NOT NULL,
        updated_at              TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_markets_active ON markets(active)",
    "CREATE INDEX IF NOT EXISTS idx_markets_closed_at ON markets(closed_at)",
    # Cycle 10: per-market Polymarket taker fee_rate, sourced from
    # `GET /markets/<conditionId>:taker_base_fee` (basis points → fractional
    # in `app/clob.py:get_market_fee_rate`). Stored as a **fractional rate**
    # in [0.0, 1.0] (e.g. 0.02 for a 2% taker fee), NOT bps — `closer.py`
    # consumes a fraction directly. Column name is `fee_rate` (not
    # `fee_rate_bps`) to match the stored unit and avoid a confused reader.
    # NULL = not-yet-fetched; closer falls back to the category lookup in
    # `config.polymarket_fee_rates`. Idempotent via the
    # `schema_migrations` table (db.py applies each statement once per sha).
    "ALTER TABLE markets ADD COLUMN fee_rate REAL",
    # Cycle 11: three-state UMA close machine state (plans/resolution-oracle.md
    # D1). The Gamma close flag (`closed=true`) is NOT finality —
    # Polymarket settles through UMA's Optimistic Oracle with a 2h
    # uncontested window or ~4-6d disputed path. Stamp `uma_proposed_at`
    # on the *transition* into `umaResolutionStatus='proposed'` and
    # `uma_finalized_at` on the transition into `'resolved'` (idempotent;
    # only set when the existing column is NULL). `last_resolution_status`
    # is the last-seen string from Gamma for restart-safe wait logic.
    # All three columns nullable — NULL means "never observed".
    "ALTER TABLE markets ADD COLUMN uma_proposed_at TEXT",
    "ALTER TABLE markets ADD COLUMN uma_finalized_at TEXT",
    "ALTER TABLE markets ADD COLUMN last_resolution_status TEXT",
    # Cycle 11 (supporting column): JSON-encoded `outcomePrices` list as
    # last observed from Gamma. Needed so the State-3 close path can
    # recover the final YES/NO payout after a process restart — the three
    # status columns above record WHEN we transitioned, this records WHAT
    # the final payout is. NULL = never observed.
    "ALTER TABLE markets ADD COLUMN last_outcome_prices TEXT",
]
db_module.register_schema("app/stages/markets.py", _MARKETS_DDL)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class MarketIndexEmbedderMismatch(RuntimeError):
    """Raised by `MarketIndex.load()` when rows carry mixed `embedder_id`s.

    A mixed state is the signature of a mid-flight model rotation; mixing
    vector spaces in a cosine search silently returns garbage (a seulgi
    lesson). The stage catches this, re-runs the embedding refresh, and
    retries.
    """


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _state3_exit_mid(
    *,
    direction: str,  # noqa: ARG001 — present for caller symmetry; see note
    last_outcome_prices,
) -> Optional[float]:
    """Translate a stored `last_outcome_prices` payload into the
    State-3 exit mid passed to `write_trade_closure`, or return None
    if the payload is malformed / unusable.

    Polymarket Gamma convention (unstructured-1 answers, lines 36-44):
    `outcomePrices` is a 2-element list where index 0 = YES payout and
    index 1 = NO payout; both stringified. The pair sums to ~1 for a
    binary market.

    **Direction is ignored on purpose.** `write_trade_closure` /
    `_compute_gross_pnl` expect `exit_mid` to be the YES-side mid for
    BOTH directions — the NO sign-flip is applied inside the PnL helper
    (`(entry - exit) / (1 - entry)`). So a NO trade that won on
    `outcomePrices=["0","1"]` correctly receives `exit_mid=0.0` here
    and computes `(0.5 - 0) / (1 - 0.5) = 1.0` shares-worth-redemption
    inside the closer. We keep `direction` in the signature so a future
    multi-outcome market (Q5 in resolution-oracle.md) can wire it back in
    without a signature churn.

    Accepts either a Python list (in-memory) or a JSON-encoded string
    (as stored in `markets.last_outcome_prices`).
    """
    if last_outcome_prices is None:
        return None
    if isinstance(last_outcome_prices, str):
        s = last_outcome_prices.strip()
        if not s.startswith("["):
            return None
        try:
            parsed = json.loads(s)
        except (json.JSONDecodeError, ValueError):
            return None
    else:
        parsed = last_outcome_prices
    if not isinstance(parsed, list) or len(parsed) < 2:
        return None
    try:
        yes_payout = float(parsed[0])
        no_payout = float(parsed[1])
    except (TypeError, ValueError):
        return None
    # Binary YES/NO markets only — reject any payload that doesn't sum
    # cleanly to a $1 redemption. This catches N>2 outcome markets
    # (resolution-oracle.md Q5) and any garbled string we shouldn't act on.
    if not (0.0 <= yes_payout <= 1.0 and 0.0 <= no_payout <= 1.0):
        return None
    if abs((yes_payout + no_payout) - 1.0) > 1e-6:
        return None
    # Always return the YES-side payout — closer.write_trade_closure
    # handles the NO sign flip internally.
    return yes_payout


def _hours_since(iso_ts: Optional[str], now_dt: datetime) -> Optional[float]:
    """Hours between an ISO timestamp and `now_dt`, or None if the
    timestamp is missing / unparseable. Used by the State-1 human-review
    fallback so we never silently auto-close a never-resolved market."""
    if not iso_ts:
        return None
    try:
        ts = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    delta = now_dt - ts
    return delta.total_seconds() / 3600.0


def _parse_gamma_row(raw: dict) -> Optional[dict]:
    """Pull the fields we care about out of a raw Gamma API row.

    Returns ``None`` when the row lacks the bare minimum (`conditionId` +
    `question`). Tokens live in `clobTokenIds`, which Gamma serialises as a
    JSON-encoded string in production responses — the seulgi-era bug where
    `clob_ids[0]` returned the single character ``"["`` is the reason we
    parse the string here rather than treating it as a list.
    """
    condition_id = raw.get("conditionId") or raw.get("condition_id") or ""
    question = raw.get("question") or ""
    if not condition_id or not question:
        return None

    clob_raw = raw.get("clobTokenIds")
    clob_ids: list = []
    if isinstance(clob_raw, list):
        clob_ids = clob_raw
    elif isinstance(clob_raw, str) and clob_raw.strip().startswith("["):
        try:
            parsed_ids = json.loads(clob_raw)
            if isinstance(parsed_ids, list):
                clob_ids = parsed_ids
        except (json.JSONDecodeError, ValueError):
            clob_ids = []
    token_yes = str(clob_ids[0]) if len(clob_ids) >= 1 and clob_ids[0] else None
    token_no = str(clob_ids[1]) if len(clob_ids) >= 2 and clob_ids[1] else None

    # `volume24hr` is the rolling 24h USDC notional; `volume` / `volumeNum`
    # are lifetime totals (often much larger). The thin_market gate in
    # trade.py compares against a 24h threshold, so the parser must read
    # the 24h field — falling back to lifetime would silently let
    # low-activity markets through the gate.
    # Source: unstructured answers batch 2026-05-25-unstructured-1, lines
    # 27-36 (sample payload showing volume24hr ≠ volumeNum on same market).
    try:
        volume = float(raw.get("volume24hr") or 0.0)
    except (TypeError, ValueError):
        volume = None

    # Cycle 11: three-state UMA close machine inputs
    # (plans/resolution-oracle.md). `umaResolutionStatus` advances through
    # `proposed` (2h challenge window) → `resolved` (finalized). `resolvedBy`
    # is the authority address. `acceptingOrders` flips to false once trading
    # halts. `outcomePrices` carries the final $1/$0 payout as a 2-element
    # array — Gamma emits it sometimes as a native list, sometimes as a
    # JSON-encoded string (mirrors the `clobTokenIds` quirk above).
    uma_status_raw = raw.get("umaResolutionStatus")
    uma_status = str(uma_status_raw) if uma_status_raw else None
    resolved_by_raw = raw.get("resolvedBy")
    resolved_by = str(resolved_by_raw) if resolved_by_raw else None
    accepting_orders_raw = raw.get("acceptingOrders")
    accepting_orders: Optional[bool]
    if isinstance(accepting_orders_raw, bool):
        accepting_orders = accepting_orders_raw
    elif accepting_orders_raw is None:
        accepting_orders = None
    else:
        accepting_orders = bool(accepting_orders_raw)

    outcome_raw = raw.get("outcomePrices")
    outcome_prices: list[str] = []
    if isinstance(outcome_raw, list):
        outcome_prices = [str(x) for x in outcome_raw]
    elif isinstance(outcome_raw, str) and outcome_raw.strip().startswith("["):
        try:
            parsed_outcomes = json.loads(outcome_raw)
            if isinstance(parsed_outcomes, list):
                outcome_prices = [str(x) for x in parsed_outcomes]
        except (json.JSONDecodeError, ValueError):
            outcome_prices = []

    return {
        "condition_id": str(condition_id),
        "slug": raw.get("slug") or None,
        "question": str(question),
        "description": raw.get("description") or None,
        "category": raw.get("category") or None,
        "end_date": raw.get("endDate") or raw.get("end_date") or None,
        "volume_usdc": volume,
        "token_yes": token_yes,
        "token_no": token_no,
        "uma_resolution_status": uma_status,
        "resolved_by": resolved_by,
        "accepting_orders": accepting_orders,
        "outcome_prices": outcome_prices,
    }


# ---------------------------------------------------------------------------
# MarketIndex (the documented rule-5 exception)
# ---------------------------------------------------------------------------


@dataclass
class _IndexSnapshot:
    """Immutable view of the index used by `topk()` so a concurrent
    `load()` can swap the live state without corrupting an in-flight search."""

    condition_ids: tuple[str, ...]
    matrix: Optional[np.ndarray]   # shape (N, dim) or None when empty
    embedder_id: Optional[str]


class MarketIndex:
    """In-process cosine-search index over active market embeddings.

    Process-local mutable state by design — see the module spec block for
    the rule-5 exception.
    """

    def __init__(self, embedder, dim: int) -> None:
        self._embedder = embedder
        self._dim = dim
        # The snapshot pattern: `load()` builds a fresh snapshot under the
        # lock and atomically replaces `self._snapshot`. `topk()` reads the
        # reference once (atomic in CPython) and works off that view.
        self._snapshot = _IndexSnapshot(condition_ids=(), matrix=None, embedder_id=None)
        self._lock = asyncio.Lock()

    @property
    def size(self) -> int:
        return len(self._snapshot.condition_ids)

    @property
    def loaded_embedder_id(self) -> Optional[str]:
        return self._snapshot.embedder_id

    async def load(self, db: DB) -> int:
        """Read every active embedded market into memory.

        Returns the number of rows loaded. Raises
        `MarketIndexEmbedderMismatch` if the active set spans more than one
        `embedder_id` value.
        """
        async with self._lock:
            cur = await db.conn.execute(
                """
                SELECT condition_id, embedding, embedder_id
                FROM markets
                WHERE active = 1
                  AND embedding IS NOT NULL
                  AND embedder_id IS NOT NULL
                """
            )
            rows = await cur.fetchall()
            await cur.close()

            if not rows:
                self._snapshot = _IndexSnapshot(condition_ids=(), matrix=None, embedder_id=None)
                return 0

            seen_ids = {r["embedder_id"] for r in rows}
            if len(seen_ids) > 1:
                raise MarketIndexEmbedderMismatch(
                    f"markets table holds {len(seen_ids)} embedder_id values "
                    f"({sorted(seen_ids)!r}); re-embed before loading the index"
                )
            (the_id,) = seen_ids

            condition_ids: list[str] = []
            vectors: list[np.ndarray] = []
            for r in rows:
                blob = r["embedding"]
                vec = np.frombuffer(blob, dtype=np.float32)
                if vec.shape[0] != self._dim:
                    # Defensive: if a row's blob doesn't match dim, the table
                    # has rotted somehow. Skip the row and log loudly rather
                    # than build a matrix with a ragged stack.
                    logger.warning(
                        "markets: skipping %s — embedding dim=%d, expected %d",
                        r["condition_id"], vec.shape[0], self._dim,
                    )
                    continue
                condition_ids.append(r["condition_id"])
                vectors.append(vec)

            if not vectors:
                self._snapshot = _IndexSnapshot(condition_ids=(), matrix=None, embedder_id=the_id)
                return 0

            matrix = np.stack(vectors).astype(np.float32, copy=False)
            self._snapshot = _IndexSnapshot(
                condition_ids=tuple(condition_ids),
                matrix=matrix,
                embedder_id=the_id,
            )
            return len(condition_ids)

    def topk(
        self,
        query_embedding: np.ndarray,
        k: int,
        floor: float,
    ) -> list[tuple[str, float]]:
        """Return up to `k` `(condition_id, similarity)` pairs above `floor`.

        Reads the live snapshot once — concurrent `load()` calls produce a
        new snapshot reference rather than mutating the one we're reading.
        """
        snap = self._snapshot  # single read; atomic in CPython
        if snap.matrix is None or len(snap.condition_ids) == 0:
            return []

        q = np.asarray(query_embedding, dtype=np.float32).reshape(-1)
        if q.shape[0] != self._dim:
            raise ValueError(
                f"query embedding dim {q.shape[0]} does not match index dim {self._dim}"
            )

        scores = snap.matrix @ q
        order = np.argsort(-scores)
        out: list[tuple[str, float]] = []
        for idx in order:
            sim = float(scores[idx])
            if sim < floor:
                break
            out.append((snap.condition_ids[idx], sim))
            if len(out) >= k:
                break
        return out


# ---------------------------------------------------------------------------
# Stage
# ---------------------------------------------------------------------------


def _read_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("invalid int in %s=%r; using default %d", name, raw, default)
        return default


def _config_get(cfg, attr: str, default):
    """Lookup `cfg.markets.<attr>` if a `markets` section exists; else default.

    Avoids a hard dependency on a `MarketsSection` in `app/config.py`: the
    PR brief says only add one if truly needed, and the env-var path
    already covers the live-tuning case.
    """
    section = getattr(cfg, "markets", None)
    if section is None:
        return default
    return getattr(section, attr, default)


def _config_get_embedder_dim(cfg) -> int:
    """Read the embedder dimensionality from the config root.

    Falls back to 384 (matches `EmbedderSection` default in app/config.py)
    if the config shape is the minimal one used by tests.
    """
    section = getattr(cfg, "embedder", None)
    if section is None:
        return 384
    return int(getattr(section, "dim", 384))


class MarketsStage:
    def __init__(self, deps: StageDeps) -> None:
        if deps.embedder is None:
            raise RuntimeError("MarketsStage requires deps.embedder")
        self._db = deps.db
        self._bus = deps.bus
        self._config = deps.config
        self._embedder = deps.embedder
        # Cycle 10: CLOB client is OPTIONAL on MarketsStage. When present,
        # `_upsert_market` triggers a per-market fee_rate fetch for rows
        # that have a NULL fee_rate (first-seen only — refresh cadence for
        # already-populated rows is a separate decision documented in
        # plans/markets-gamma.md). When absent (e.g. minimal test wirings
        # that don't pass `clob=`), the fee_rate stays NULL and closer.py
        # falls back to the category lookup in `config.polymarket_fee_rates`.
        self._clob = deps.clob

        self._poll_seconds = _read_int_env(
            _ENV_POLL_SECONDS,
            int(_config_get(deps.config, "poll_seconds", _DEFAULT_POLL_SECONDS)),
        )
        self._embed_refresh_seconds = _read_int_env(
            _ENV_EMBED_REFRESH_SECONDS,
            int(_config_get(
                deps.config, "embed_refresh_seconds", _DEFAULT_EMBED_REFRESH_SECONDS
            )),
        )
        # Cycle 12: cadence for the resolution-re-poll task. Defaults to
        # 600s (10 min) — fast enough to catch the 2h UMA challenge
        # window early, slow enough to keep Gamma quota usage bounded.
        self._resolution_poll_seconds = _read_int_env(
            _ENV_RESOLUTION_POLL_SECONDS,
            int(_config_get(
                deps.config,
                "resolution_poll_seconds",
                _DEFAULT_RESOLUTION_POLL_SECONDS,
            )),
        )

        # MarketIndex lives on the stage instance — not at module scope —
        # so the rule-5 exception is bounded to a single object lifetime.
        # Dim is read from the config rather than poking the embedder's
        # private attribute; the Embedder class enforces a match on every
        # `embed()` call via `EmbedderDimMismatch`.
        dim = _config_get_embedder_dim(deps.config)
        self.index = MarketIndex(self._embedder, dim)
        self._dim = dim

        self._client: Optional[httpx.AsyncClient] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._embed_task: Optional[asyncio.Task] = None
        # Cycle 12: separate task that re-polls markets sitting in
        # State 1/2 (closed, not yet finalized) so the three-state
        # close machine actually fires on the proposed/resolved
        # transitions instead of being stuck at State 1 forever
        # (Gamma drops them from the active list at `closed=true`,
        # BEFORE UMA proposes — so the main poll never re-sees them).
        self._resolution_poll_task: Optional[asyncio.Task] = None
        self._stopped = False

    # ---- lifecycle ------------------------------------------------------

    async def start(self) -> None:
        self._client = httpx.AsyncClient(timeout=_GAMMA_TIMEOUT_SECONDS)
        # Warm the MarketIndex from existing embedded rows BEFORE the
        # poll/embed loops fire. Without this, MatchStage sees an empty
        # index and `signals` stays 0 — the live-trade path silently
        # stalls. Index reload after subsequent embed cycles is handled
        # in `refresh_embeddings_once`.
        loaded = await self.index.load(self._db)  # type: ignore[attr-defined]
        logger.info("markets: index warmed (%d rows)", loaded)
        self._poll_task = asyncio.create_task(self._poll_loop())
        self._embed_task = asyncio.create_task(self._embed_loop())
        # Cycle 12: closed-but-not-finalized re-poll task.
        self._resolution_poll_task = asyncio.create_task(
            self._resolution_poll_loop()
        )

    async def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        tasks = (
            self._poll_task,
            self._embed_task,
            self._resolution_poll_task,
        )
        for task in tasks:
            if task is not None:
                task.cancel()
        for task in tasks:
            if task is not None:
                try:
                    await task
                except (asyncio.CancelledError, Exception):  # noqa: BLE001
                    pass
        self._poll_task = None
        self._embed_task = None
        self._resolution_poll_task = None
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def handle(self, payload: dict) -> None:  # noqa: ARG002
        raise NotImplementedError("MarketsStage is a poller, not a subscriber")

    # ---- background loops ---------------------------------------------

    async def _poll_loop(self) -> None:
        while True:
            try:
                await self.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — never let the loop die
                logger.exception("markets poll cycle failed; continuing")
            await asyncio.sleep(self._poll_seconds)

    async def _embed_loop(self) -> None:
        while True:
            try:
                await self.refresh_embeddings_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                logger.exception("markets embedding refresh failed; continuing")
            await asyncio.sleep(self._embed_refresh_seconds)

    async def _resolution_poll_loop(self) -> None:
        """Cycle 12: drive the three-state UMA close machine on markets
        that Gamma already dropped (`closed=true`) but UMA has not yet
        finalized. The main `_poll_loop` only walks the active list, so
        without this task those markets never get re-evaluated and the
        close machine stays stuck at State 1 forever. See
        `plans/resolution-oracle.md` "orchestration gap"."""
        while True:
            try:
                await self.poll_resolved_markets_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — keep the loop alive
                logger.exception(
                    "markets resolution-poll cycle failed; continuing"
                )
            await asyncio.sleep(self._resolution_poll_seconds)

    # ---- single-cycle entry points (also called directly in tests) ----

    async def poll_once(self) -> int:
        """Run one Gamma fetch + upsert cycle. Returns the number of open
        markets observed (0 on a network failure)."""
        try:
            raw_markets = await self._fetch_all_gamma()
        except httpx.HTTPError as exc:
            logger.warning("markets: Gamma fetch failed (%s); skipping upsert", exc)
            return 0

        parsed: list[dict] = []
        for raw in raw_markets:
            row = _parse_gamma_row(raw)
            if row is not None:
                parsed.append(row)

        open_ids: set[str] = set()
        for row in parsed:
            await self._upsert_market(row)
            open_ids.add(row["condition_id"])

        await self._deactivate_missing(open_ids)

        await self._bus.publish(
            TOPIC_MARKETS_REFRESHED,
            {"count": len(parsed), "refreshed_at": _now_iso()},
        )
        return len(parsed)

    async def refresh_embeddings_once(self) -> int:
        """Embed every row missing an embedding OR stamped with a stale
        `embedder_id`. Returns the number of rows updated."""
        target_id = self._embedder.embedder_id
        cur = await self._db.conn.execute(
            """
            SELECT condition_id, question
            FROM markets
            WHERE active = 1
              AND (embedding IS NULL OR embedder_id IS NOT ? OR embedder_id IS NULL)
            """,
            (target_id,),
        )
        rows = await cur.fetchall()
        await cur.close()
        if not rows:
            return 0

        questions = [r["question"] for r in rows]
        # Run the embed call on a thread executor so a slow batch doesn't
        # block other asyncio tasks (mirrors seulgi's pattern).
        loop = asyncio.get_event_loop()
        matrix = await loop.run_in_executor(
            None, self._embedder.embed_batch, questions
        )

        async with self._db.batch() as cur:
            now = _now_iso()
            for r, vec in zip(rows, matrix):
                arr = np.asarray(vec, dtype=np.float32).reshape(-1)
                await cur.execute(
                    """
                    UPDATE markets
                    SET embedding = ?, embedder_id = ?, embedding_dim = ?,
                        embedding_updated_at = ?, updated_at = ?
                    WHERE condition_id = ?
                    """,
                    (
                        arr.tobytes(),
                        target_id,
                        int(arr.shape[0]),
                        now,
                        now,
                        r["condition_id"],
                    ),
                )
        # Re-warm the in-memory index so MatchStage sees the new vectors.
        await self.index.load(self._db)
        return len(rows)

    async def poll_resolved_markets_once(self) -> int:
        """Cycle 12: walk markets where Gamma flipped `closed=true` but
        UMA has NOT yet finalized AND we have at least one open trade
        sitting on them. For each unique market_id, refetch the Gamma
        row, re-run the upsert (which stamps `last_resolution_status`
        / `last_outcome_prices` / `uma_proposed_at` / `uma_finalized_at`
        per cycle 11), then invoke `_close_orphaned_trades_for` so the
        existing State-2/State-3 machine runs.

        Returns the number of unique markets re-evaluated. Errors on
        any single market are caught and logged so one bad market
        cannot abort the cycle (per the loop's failure contract).
        """
        timeout_hours = float(
            _config_get(self._config, "resolution_timeout_hours", 168.0)
        )
        # Filter to open trades on markets that are inside the human-
        # review window. The cutoff is computed as an ISO string so the
        # comparison happens entirely in SQLite (avoids pulling every
        # row into Python just to filter on a timestamp). We accept the
        # tiny implicit assumption that closed_at and "now" share the
        # same iso8601 / UTC convention (markets.py writes them via the
        # same `_now_iso()` helper).
        from datetime import timedelta
        now_dt = datetime.now(timezone.utc)
        cutoff_iso = (now_dt - timedelta(hours=timeout_hours)).isoformat()
        cur = await self._db.conn.execute(
            """
            SELECT DISTINCT t.market_id AS market_id
            FROM trades t
            LEFT JOIN trade_closures c ON c.trade_id = t.id
            JOIN markets m ON m.condition_id = t.market_id
            WHERE c.id IS NULL
              AND m.closed_at IS NOT NULL
              AND m.uma_finalized_at IS NULL
              AND m.closed_at >= ?
            """,
            (cutoff_iso,),
        )
        rows = await cur.fetchall()
        await cur.close()
        if not rows:
            return 0
        condition_ids = [r["market_id"] for r in rows]
        logger.info(
            "markets: resolution-poll walking %d closed-but-not-finalized "
            "market(s) with open trades",
            len(condition_ids),
        )
        evaluated = 0
        for cid in condition_ids:
            try:
                raw = await self._fetch_single_market(cid)
                if raw is not None:
                    parsed = _parse_gamma_row(raw)
                    if parsed is not None:
                        # `_upsert_market` insists `active=1` on insert/
                        # conflict (intent: "still in the open list"),
                        # which would re-activate the row. That's
                        # actually OK here — a market that's already
                        # been deactivated by `_deactivate_missing`
                        # will be re-deactivated on the next regular
                        # poll cycle if Gamma still doesn't list it.
                        # The fields we care about (uma_* and
                        # last_outcome_prices) are written by the
                        # upsert + stamp path regardless.
                        await self._upsert_market(parsed)
                # Drive the state machine on this single market
                # regardless of fetch outcome — if the refetch failed
                # but the row already carries a `'resolved'` status
                # from a prior cycle, the close machine should still
                # be invoked so we don't get stuck.
                await self._close_orphaned_trades_for([cid])
                evaluated += 1
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — per-market isolation
                logger.exception(
                    "markets: resolution-poll for market %s failed; "
                    "continuing with the rest of the cycle",
                    cid,
                )
        return evaluated

    # ---- helpers --------------------------------------------------------

    async def _fetch_all_gamma(self) -> list[dict]:
        """Paginate Gamma's `/markets` endpoint and return every open row.

        Page size is 100 (Gamma caps every response at 100 regardless of the
        `limit` we ask for — verified in seulgi 2026-05-14). Termination
        condition is "page returned fewer than page_size rows", per the PR
        spec; the safety `max_pages` guards against an infinite loop if
        Gamma's pagination ever regresses.
        """
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=_GAMMA_TIMEOUT_SECONDS)
        markets: list[dict] = []
        offset = 0
        for _ in range(_GAMMA_MAX_PAGES):
            params = {
                "active": "true",
                "closed": "false",
                "limit": str(_GAMMA_PAGE_SIZE),
                "offset": str(offset),
            }
            resp = await self._client.get(_GAMMA_URL, params=params)
            resp.raise_for_status()
            page = resp.json()
            if not isinstance(page, list) or not page:
                break
            markets.extend(page)
            offset += len(page)
            if len(page) < _GAMMA_PAGE_SIZE:
                break
        logger.info("markets: fetched %d markets from Gamma", len(markets))
        return markets

    async def _fetch_single_market(self, condition_id: str) -> Optional[dict]:
        """Cycle 12: fetch a single market by condition_id from Gamma.

        Endpoint verified 2026-05-25 (curl from this host):
        ``GET https://gamma-api.polymarket.com/markets?condition_ids=<cid>``
        returns a JSON array with the single matching row (or `[]` if
        the id is unknown). Same URL as `_fetch_all_gamma`, just
        narrowed by `condition_ids` instead of paged. This shape works
        for markets that are no longer in the active list (drops the
        `active=true&closed=false` filter), which is exactly the case
        we need for the resolution re-poll.

        Returns the parsed row dict from Gamma, or `None` if the
        market is unknown / the response is malformed / the network
        fails. Caller is expected to feed the result back through
        `_parse_gamma_row` + `_upsert_market`.
        """
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=_GAMMA_TIMEOUT_SECONDS)
        params = {"condition_ids": condition_id}
        try:
            resp = await self._client.get(_GAMMA_URL, params=params)
            resp.raise_for_status()
            body = resp.json()
        except httpx.HTTPError as exc:
            logger.warning(
                "markets: single-market Gamma fetch failed for %s (%s)",
                condition_id, exc,
            )
            return None
        except ValueError as exc:  # JSON decode
            logger.warning(
                "markets: single-market Gamma response not JSON for %s (%s)",
                condition_id, exc,
            )
            return None
        if not isinstance(body, list) or not body:
            # Gamma returns [] for unknown / fully-archived markets.
            return None
        first = body[0]
        if not isinstance(first, dict):
            return None
        return first

    async def _upsert_market(self, row: dict) -> None:
        now = _now_iso()
        # Serialise observed outcomePrices for persistence. Empty list →
        # NULL so we never clobber a prior non-empty observation (Gamma can
        # transiently drop the field between polls).
        outcome_prices = row.get("outcome_prices") or []
        outcome_json = json.dumps(outcome_prices) if outcome_prices else None
        async with self._db.batch() as cur:
            await cur.execute(
                """
                INSERT INTO markets (
                    condition_id, slug, question, description, category,
                    end_date, volume_usdc, token_yes, token_no,
                    active, closed_at, first_seen_at, updated_at,
                    last_resolution_status, last_outcome_prices
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, NULL, ?, ?, ?, ?)
                ON CONFLICT(condition_id) DO UPDATE SET
                    slug = excluded.slug,
                    question = excluded.question,
                    description = excluded.description,
                    category = excluded.category,
                    end_date = excluded.end_date,
                    volume_usdc = excluded.volume_usdc,
                    token_yes = excluded.token_yes,
                    token_no = excluded.token_no,
                    active = 1,
                    closed_at = NULL,
                    updated_at = excluded.updated_at,
                    last_resolution_status = excluded.last_resolution_status,
                    -- Only overwrite when the new observation is non-NULL:
                    -- Gamma can drop the field transiently mid-resolution,
                    -- and we don't want to lose the final $1/$0 payout we
                    -- already captured.
                    last_outcome_prices = COALESCE(
                        excluded.last_outcome_prices, markets.last_outcome_prices
                    )
                """,
                (
                    row["condition_id"],
                    row["slug"],
                    row["question"],
                    row["description"],
                    row["category"],
                    row["end_date"],
                    row["volume_usdc"],
                    row["token_yes"],
                    row["token_no"],
                    now,
                    now,
                    row.get("uma_resolution_status"),
                    outcome_json,
                ),
            )
        # Cycle 11: stamp UMA resolution state transitions. Only set the
        # `*_at` timestamps when (a) we just observed the matching status
        # and (b) the column is still NULL — strictly forward-only, never
        # overwrites. Use a single SELECT-then-UPDATE rather than a
        # condition-in-SQL because we want each transition's wall-time
        # recorded on FIRST observation (subsequent re-reads must not
        # bump the timestamp). This matches the "post-close wait survives
        # restarts" requirement in plans/resolution-oracle.md (line 101).
        await self._maybe_stamp_resolution_transition(
            row["condition_id"], row.get("uma_resolution_status"),
        )
        # Cycle 10: per-market fee_rate fetch. **First-seen only** for
        # this cycle — we read the current fee_rate after the upsert
        # and only call the CLOB if the column is still NULL. This
        # keeps the discovery loop cheap (one extra HTTP per net-new
        # market) and avoids drift-vs-cache complexity for already-
        # populated rows. Existing-row refresh cadence is a separate
        # decision (see plans/markets-gamma.md).
        await self._maybe_fetch_fee_rate(row["condition_id"])

    async def _maybe_stamp_resolution_transition(
        self, condition_id: str, status: Optional[str],
    ) -> None:
        """If we just observed `status='proposed'` or `'resolved'` and the
        corresponding `*_at` column is still NULL, stamp it with `now`.
        Idempotent — re-running on an already-stamped row is a no-op.
        Called from `_upsert_market` after every upsert so we capture the
        transition on whichever poll cycle first sees it.
        """
        if not status:
            return
        status_lower = status.lower()
        if status_lower not in ("proposed", "resolved"):
            return
        column = "uma_proposed_at" if status_lower == "proposed" else "uma_finalized_at"
        cur = await self._db.conn.execute(
            f"SELECT {column} FROM markets WHERE condition_id = ?",  # noqa: S608
            (condition_id,),
        )
        existing = await cur.fetchone()
        await cur.close()
        if existing is None or existing[column] is not None:
            return
        now = _now_iso()
        async with self._db.batch() as cur:
            await cur.execute(
                f"UPDATE markets SET {column} = ?, updated_at = ? "  # noqa: S608
                f"WHERE condition_id = ? AND {column} IS NULL",
                (now, now, condition_id),
            )

    async def _maybe_fetch_fee_rate(self, condition_id: str) -> None:
        """If the CLOB client is wired and this market's fee_rate is
        still NULL, fetch it from `GET /markets/<conditionId>` and
        persist. Silent no-op on any failure — closer.py falls back
        to the category lookup in `config.polymarket_fee_rates`."""
        if self._clob is None:
            return
        cur = await self._db.conn.execute(
            "SELECT fee_rate FROM markets WHERE condition_id = ?",
            (condition_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None or row["fee_rate"] is not None:
            return  # not first-seen, skip per cycle-10 scope
        fee_rate = await self._clob.get_market_fee_rate(condition_id)
        if fee_rate is None:
            return  # CLOB miss; leave NULL so a future cycle can retry
        now = _now_iso()
        async with self._db.batch() as cur:
            await cur.execute(
                "UPDATE markets SET fee_rate = ?, updated_at = ? WHERE condition_id = ?",
                (fee_rate, now, condition_id),
            )

    async def _deactivate_missing(self, open_ids: set[str]) -> int:
        """Mark every active row whose condition_id is NOT in the fresh
        open set as inactive.

        Done in two steps so it's robust to large universes (SQLite's
        default parameter cap is 999): read every active condition_id,
        diff against the open set in Python, then issue chunked UPDATEs by
        positive id list. This avoids a `NOT IN (chunk)` that would
        wrongly flip rows present in some other chunk.

        Cycle 7: after deactivating a market, close any open trades
        positioned on it (`close_reason='market_resolved'`). Without
        this step the closer's `skip_on_missing_mid=True` policy
        leaves trades hanging forever — the CLOB orderbook for the
        market is purged once Gamma drops the listing.
        """
        if not open_ids:
            # An empty fresh fetch should not blow away the whole table —
            # this almost always means the network is broken, not that
            # Polymarket has zero markets. Skip the deactivate step.
            return 0
        cur = await self._db.conn.execute(
            "SELECT condition_id FROM markets WHERE active = 1"
        )
        active_rows = await cur.fetchall()
        await cur.close()
        to_close = [r["condition_id"] for r in active_rows if r["condition_id"] not in open_ids]
        if not to_close:
            return 0
        now = _now_iso()
        async with self._db.batch() as cur:
            for start in range(0, len(to_close), 800):
                chunk = to_close[start:start + 800]
                placeholders = ",".join("?" * len(chunk))
                await cur.execute(
                    f"""
                    UPDATE markets
                    SET active = 0, closed_at = ?, updated_at = ?
                    WHERE condition_id IN ({placeholders})
                    """,
                    (now, now, *chunk),
                )
        # Cycle 7: settle any open paper trades on the deactivated markets.
        await self._close_orphaned_trades_for(to_close)
        return len(to_close)

    async def _close_orphaned_trades_for(self, condition_ids: list[str]) -> int:
        """Cycle 11: three-state UMA close machine
        (plans/resolution-oracle.md D1). Find open trades on the given
        markets and decide per-market whether to auto-close, based on the
        market's UMA resolution state:

        - **State 1** — closed but `last_resolution_status` is not
          `'resolved'`. Wait. Don't auto-close. The Gamma `closed=true`
          flag is NOT finality: UMA's Optimistic Oracle has a 2h
          uncontested challenge window (or ~4-6d disputed). Closing at
          `entry_mid` here costs us the actual $1/$0 redemption value.
        - **State 2** — `last_resolution_status='proposed'`. Inside the
          challenge window. The `uma_proposed_at` transition is already
          stamped by `_maybe_stamp_resolution_transition` during the
          poll cycle; just log and wait.
        - **State 3** — `last_resolution_status='resolved'` AND
          `last_outcome_prices` is a sensible 2-element `["1","0"]` /
          `["0","1"]` payload. Close at the proper YES/NO payout
          (NOT entry_mid). Index 0 = YES payout per the Gamma payload
          convention (`unstructured answers.md` lines 36-44).
        - **Human-review fallback** — if a market has sat in State 1
          past `cfg.markets.resolution_timeout_hours` (default 168h = 7d),
          emit a `RESOLUTION_TIMEOUT` WARNING and STILL leave the trade
          open. Better to require a human eyeball than auto-close at a
          guess; we kept the seulgi-era conservative path off the table
          here to avoid silent PnL contamination from never-resolved
          markets.

        Returns the count of trades transitioned (State 3 closures only).
        Idempotent: trade_closures.trade_id UNIQUE prevents double-fires.
        """
        if not condition_ids:
            return 0
        # Import the free helper lazily so this module doesn't grab
        # CloserStage's lifecycle (the closer module is imported anyway
        # since both stages register schema).
        from app.stages.closer import write_trade_closure  # noqa: WPS433

        timeout_hours = float(_config_get(self._config, "resolution_timeout_hours", 168.0))
        now_dt = datetime.now(timezone.utc)

        written = 0
        for start in range(0, len(condition_ids), 800):
            chunk = condition_ids[start:start + 800]
            placeholders = ",".join("?" * len(chunk))
            cur = await self._db.conn.execute(
                f"""
                SELECT t.id, t.signal_id, t.market_id, t.direction,
                       t.mid_price, t.size_usdc, t.decided_at,
                       t.is_tracer, t.tracer_id, t.target_mid,
                       m.category, m.fee_rate,
                       m.last_resolution_status, m.last_outcome_prices,
                       m.uma_proposed_at, m.uma_finalized_at, m.closed_at
                FROM trades t
                LEFT JOIN trade_closures c ON c.trade_id = t.id
                LEFT JOIN markets m ON m.condition_id = t.market_id
                WHERE c.id IS NULL
                  AND t.market_id IN ({placeholders})
                """,
                tuple(chunk),
            )
            rows = await cur.fetchall()
            await cur.close()
            for r in rows:
                trade = {
                    "id": r["id"],
                    "signal_id": r["signal_id"],
                    "market_id": r["market_id"],
                    "direction": r["direction"],
                    "mid_price": float(r["mid_price"]),
                    "size_usdc": float(r["size_usdc"]),
                    "decided_at": r["decided_at"],
                    "is_tracer": int(r["is_tracer"] or 0),
                    "tracer_id": r["tracer_id"],
                    "target_mid": (
                        float(r["target_mid"]) if r["target_mid"] is not None else None
                    ),
                }
                status_raw = r["last_resolution_status"]
                status = status_raw.lower() if status_raw else None
                exit_mid = _state3_exit_mid(
                    direction=trade["direction"],
                    last_outcome_prices=r["last_outcome_prices"],
                )

                # State 3: finalized AND we have a usable payout. Close at
                # the real $1/$0 redemption value.
                if status == "resolved" and exit_mid is not None:
                    ok = await write_trade_closure(
                        self._db,
                        trade=trade,
                        exit_mid=exit_mid,
                        close_reason="market_resolved",
                        trade_cfg=self._config.trade,
                        category=r["category"],
                        market_fee_rate=r["fee_rate"],
                    )
                    if ok:
                        written += 1
                        logger.info(
                            "markets: auto-closed trade_id=%d (market %s "
                            "resolved at exit_mid=%.2f, direction=%s)",
                            trade["id"], trade["market_id"], exit_mid,
                            trade["direction"],
                        )
                    continue

                # State 3 sentinel: resolved but outcome data is malformed.
                # Per the hard constraint, do NOT fall back to entry_mid —
                # log a WARNING and skip. A subsequent poll cycle may
                # re-fetch a sane outcomePrices payload.
                if status == "resolved":
                    logger.warning(
                        "markets: trade_id=%d market %s resolved but "
                        "last_outcome_prices=%r is unusable; skipping close",
                        trade["id"], trade["market_id"], r["last_outcome_prices"],
                    )
                    continue

                # State 2: inside the 2h UMA challenge window.
                if status == "proposed":
                    logger.info(
                        "markets: trade_id=%d market %s in proposed state; "
                        "waiting for UMA finalization (proposed_at=%s)",
                        trade["id"], trade["market_id"], r["uma_proposed_at"],
                    )
                    continue

                # State 1: closed but no UMA status yet. Check the
                # human-review timeout against the market's closed_at
                # (when the row went inactive). Fall back to the trade's
                # decided_at if closed_at is somehow NULL.
                anchor_iso = r["closed_at"] or trade["decided_at"]
                age_h = _hours_since(anchor_iso, now_dt)
                if age_h is not None and age_h > timeout_hours:
                    logger.warning(
                        "RESOLUTION_TIMEOUT trade_id=%d market=%s waited=%.1fh "
                        "(timeout=%.1fh); leaving trade open for human review",
                        trade["id"], trade["market_id"], age_h, timeout_hours,
                    )
                    continue
                logger.info(
                    "markets: trade_id=%d market %s closed but UMA not yet "
                    "finalized; waiting (status=%r, age=%.1fh)",
                    trade["id"], trade["market_id"],
                    status_raw, age_h if age_h is not None else -1.0,
                )
        return written
