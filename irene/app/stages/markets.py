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
state across messages"). `MarketIndex` (now in `market_index.py`) is
process-local mutable state by design. See that module's docstring for
the full rationale; it is named in `docs/modularization.md` rule 5.

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
import random
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
# Cycle 26: events endpoint — bundles markets per Polymarket-event and
# bypasses the offset=10_000 cap that `/markets` enforces, because the
# events universe is ~3-4× smaller. Same pagination shape (limit/offset,
# 422-as-EOP guard).
_GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
_GAMMA_PAGE_SIZE = 100
_GAMMA_MAX_PAGES = 1000  # ~100k markets hard ceiling; current universe ~50k
_GAMMA_TIMEOUT_SECONDS = 15.0
# Cycle 26 stage-1 soak fix #6: send a real User-Agent on Gamma requests.
# Cloudflare's bot filter is more lenient on identified clients; a 403
# burst correlated with the dual-container peak load suggests we were
# pattern-matching as a scraper. This is polite hygiene — the structural
# fix (one container, halved load) is what actually solves the rate.
_GAMMA_HEADERS = {"User-Agent": "irene-markets-poller/1.0"}
# Cycle 13: Cloudflare-fronted Gamma occasionally returns 429. Honor
# `Retry-After` (seconds) when present; fall back to exponential backoff
# with jitter when not. Cap retries to prevent a poll task from getting
# stuck.
_GAMMA_429_MAX_RETRIES = 3
_GAMMA_429_INITIAL_BACKOFF_S = 1.0
_GAMMA_429_MAX_BACKOFF_S = 60.0

_DEFAULT_POLL_SECONDS = 120
_DEFAULT_EMBED_REFRESH_SECONDS = 3600
# Cycle 12: orchestration cadence for the closed-but-not-finalized
# market re-poll. Default 10 min — fine-grained enough that we catch
# the 2h UMA challenge window early, coarse enough that we don't
# hammer Gamma per market. See plans/resolution-oracle.md.
_DEFAULT_RESOLUTION_POLL_SECONDS = 600
# Cycle 15: refresh cadence for per-market `fee_rate` / `min_order_size`.
# Polymarket has historically shipped fee-schedule changes (the cycle-10
# survey caught one market at 1000 bps when most are 0); cycles 10/13 only
# fetched on first-seen, so any change after that point was invisible to
# us. Default 168h = 7 days: long enough to keep the CLOB HTTP budget
# bounded (one call per market per week), short enough that a real fee
# change is caught within a trading week.
_DEFAULT_MARKET_INFO_REFRESH_HOURS = 168.0

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
    # Cycle 13: per-market platform floor in **SHARES**, sourced from
    # `GET /markets/<conditionId>:minimum_order_size` (see `app/clob.py:
    # get_market_info`). Stored as REAL to match the `fee_rate` column
    # convention, even though the source is an integer. NOT in USDC —
    # the trade-stage size gate (`app/stages/trade.py`) is responsible
    # for the shares→USDC conversion via the executable price for the
    # bet's side. NULL = not-yet-fetched / fetch failed; the gate then
    # falls back to `cfg.trade.min_position_usdc` only (no per-market
    # floor enforced). See plans/order-execution-spreads.md proposal #4.
    "ALTER TABLE markets ADD COLUMN min_order_size REAL",
    # Cycle 15: UTC ISO timestamp of the LAST successful
    # `clob.get_market_info(condition_id)` call. NULL = never fetched.
    # Powers the per-market info refresh cadence (see
    # `_maybe_fetch_market_info` + `cfg.markets.market_info_refresh_hours`):
    # cycles 10/13 only fetched on first-seen (`fee_rate IS NULL`), which
    # meant a market whose Polymarket fee schedule changed mid-life would
    # keep the stale value forever. Idempotent via `schema_migrations`.
    # Legacy rows pre-cycle-15 have NULL `fee_rate_fetched_at`; they all
    # qualify for the next refresh — see the comment in
    # `_maybe_fetch_market_info` about the one-time burst.
    "ALTER TABLE markets ADD COLUMN fee_rate_fetched_at TEXT",
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
    # Cycle 26: Polymarket-event grouping discovered via the Gamma
    # `/events` endpoint. NULL when (a) the events endpoint is disabled,
    # (b) the dual-read pass surfaced this market only via `/markets`, or
    # (c) the market is event-less on Polymarket's side. `polymarket_*`
    # prefix because the `events` TABLE (owned by eventize.py) holds
    # article-cluster events — a name collision here would be a footgun.
    "ALTER TABLE markets ADD COLUMN polymarket_event_id TEXT",
    "ALTER TABLE markets ADD COLUMN polymarket_event_slug TEXT",
    # 0/1 flag stamped each poll: did this market appear under
    # `/events?active=true&closed=false` on the most recent cycle?
    # Used to log the events-vs-markets coverage gap during the
    # dual-read shadow window before promoting `/events` to sole source.
    "ALTER TABLE markets ADD COLUMN events_seen_in_last_poll INTEGER",
    "CREATE INDEX IF NOT EXISTS idx_markets_polymarket_event_id "
    "ON markets(polymarket_event_id)",
]
db_module.register_schema("app/stages/markets.py", _MARKETS_DDL)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


def _parse_gamma_row(
    raw: dict,
    *,
    event_meta: Optional[dict] = None,
) -> Optional[dict]:
    """Pull the fields we care about out of a raw Gamma API row.

    Returns ``None`` when the row lacks the bare minimum (`conditionId` +
    `question`). Tokens live in `clobTokenIds`, which Gamma serialises as a
    JSON-encoded string in production responses — the seulgi-era bug where
    `clob_ids[0]` returned the single character ``"["`` is the reason we
    parse the string here rather than treating it as a list.

    `event_meta` (cycle 26) is the parent event's `{"id": ..., "slug": ...}`
    when the row was harvested from `/events`. Stamped into
    `polymarket_event_id` / `polymarket_event_slug` on the returned dict;
    ``None`` when the row came from `/markets`.
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

    # Cycle 18: Gamma's `closed` flag + `closedTime` (precise platform
    # timestamp when the market entered closed state). Verified live
    # 2026-05-25 — `closedTime` is present on ~100% of recently-closed
    # markets and is more authoritative than our first-observation
    # `_now_iso()` stamp. When `closed=true` AND closedTime present,
    # `_upsert_market` will prefer it over `now`. See
    # `reports/2026-05-25-cycle17-resolved-schema-verification.md`.
    closed_raw = raw.get("closed")
    closed_flag = bool(closed_raw) if closed_raw is not None else False
    closed_time = raw.get("closedTime") or None
    if closed_time and not isinstance(closed_time, str):
        closed_time = str(closed_time)

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
        "closed": closed_flag,
        "closed_time": closed_time,
        "polymarket_event_id": (
            str(event_meta["id"]) if event_meta and event_meta.get("id") else None
        ),
        "polymarket_event_slug": (
            str(event_meta["slug"]) if event_meta and event_meta.get("slug") else None
        ),
    }


# MarketIndex / _IndexSnapshot / MarketIndexEmbedderMismatch live in
# `app/stages/market_index.py` as of the cycle-26 extraction. The names
# are re-imported into this module's namespace below so existing
# `from app.stages.markets import MarketIndex` call sites stay valid.
from app.stages.market_index import (  # noqa: E402
    MarketIndex,
    MarketIndexEmbedderMismatch,
)

__all__ = ["MarketIndex", "MarketIndexEmbedderMismatch", "MarketsStage"]


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
    # Cycle 23: per-process one-shot WARN flag for
    # `_maybe_fetch_market_info`'s clob-None silent-skip path. Class-level
    # so the flag survives across stage instances (tests construct many).
    _fee_warn_seen: bool = False

    def __init__(self, deps: StageDeps, *, read_only_replica: bool = False) -> None:
        if deps.embedder is None:
            raise RuntimeError("MarketsStage requires deps.embedder")
        self._db = deps.db
        self._bus = deps.bus
        self._config = deps.config
        self._embedder = deps.embedder
        # Cycle 26 stage-1 soak fix #5: when True, this instance is a
        # read-only replica — production owns the writes and we just
        # keep `self.index` warm by reloading from the DB. Set in
        # MODE_EXPLORER to remove the duplicate 69k-row upsert loop
        # that was the dominant WAL writer-lock contender.
        self._read_only_replica = bool(read_only_replica)
        # Cycle 10/15: CLOB client is OPTIONAL on MarketsStage. When
        # present, `_upsert_market` triggers a per-market fee_rate /
        # min_order_size fetch on first-seen (cycles 10/13) AND on a
        # configurable refresh cadence (cycle 15 — see
        # `_maybe_fetch_market_info` and
        # `cfg.markets.market_info_refresh_hours`, default 168h).
        # When absent (e.g. minimal test wirings that don't pass
        # `clob=`), `fee_rate` stays NULL and closer.py falls back to
        # the category lookup in `config.polymarket_fee_rates`.
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
        # Cycle 15: refresh window for per-market `fee_rate` /
        # `min_order_size`. See `_maybe_fetch_market_info` for the
        # fire-conditions. Cast through float because the config may carry
        # an int (`168`) or a float (`168.0`).
        self._market_info_refresh_hours = float(_config_get(
            deps.config,
            "market_info_refresh_hours",
            _DEFAULT_MARKET_INFO_REFRESH_HOURS,
        ))

        # MarketIndex lives on the stage instance — not at module scope —
        # so the rule-5 exception is bounded to a single object lifetime.
        # Dim is read from the config rather than poking the embedder's
        # private attribute; the Embedder class enforces a match on every
        # `embed()` call via `EmbedderDimMismatch`.
        dim = _config_get_embedder_dim(deps.config)
        self.index = MarketIndex(self._embedder, dim)
        self._dim = dim

        self._client: Optional[httpx.AsyncClient] = None
        # Cycle 26: optional WS pool. Owned by main.py; MarketsStage
        # drives the subscription set from each poll cycle's tradable
        # universe. None when `cfg.markets.enable_ws_feed=False`.
        self._ws_pool = getattr(deps, "ws_pool", None)
        self._poll_task: Optional[asyncio.Task] = None
        self._embed_task: Optional[asyncio.Task] = None
        # Cycle 12: separate task that re-polls markets sitting in
        # State 1/2 (closed, not yet finalized) so the three-state
        # close machine actually fires on the proposed/resolved
        # transitions instead of being stuck at State 1 forever
        # (Gamma drops them from the active list at `closed=true`,
        # BEFORE UMA proposes — so the main poll never re-sees them).
        self._resolution_poll_task: Optional[asyncio.Task] = None
        # Cycle 26 stage-1 soak fix #5: replica-mode refresh task; only
        # set when `read_only_replica=True`. Periodically reloads the
        # in-memory MarketIndex from the DB so explorer's downstream
        # stages (LinkerStage, MatchStage) see new markets within one
        # refresh cycle.
        self._index_refresh_task: Optional[asyncio.Task] = None
        self._stopped = False

    # ---- lifecycle ------------------------------------------------------

    async def start(self) -> None:
        self._client = httpx.AsyncClient(
            timeout=_GAMMA_TIMEOUT_SECONDS, headers=_GAMMA_HEADERS,
        )
        # Warm the MarketIndex from existing embedded rows BEFORE the
        # poll/embed loops fire. Without this, MatchStage sees an empty
        # index and `signals` stays 0 — the live-trade path silently
        # stalls. Index reload after subsequent embed cycles is handled
        # in `refresh_embeddings_once`.
        loaded = await self.index.load(self._db)  # type: ignore[attr-defined]
        logger.info(
            "markets: index warmed (%d rows)%s",
            loaded,
            " [replica]" if self._read_only_replica else "",
        )
        # Cycle 26 stage-1 soak fix #5: in replica mode, production owns
        # all market writes. Skip the boot resolution-rescan, the WS pool
        # (production drives the live feed), and all three writer loops.
        # We spin up a single read-only refresh loop instead.
        if self._read_only_replica:
            self._index_refresh_task = asyncio.create_task(
                self._index_refresh_loop()
            )
            return

        # Cycle 23: synchronous resolution-rescan on boot. Closes the gap
        # where a graceful restart during a market's post-close window
        # misses the UMA finalization stamp until the periodic loop
        # (every `_resolution_poll_seconds`, default 600s) next fires.
        # Pure additive — `poll_resolved_markets_once` is the same code
        # the periodic loop runs, just one early invocation. Wrapped so
        # a transient failure here never blocks boot.
        try:
            n = await self.poll_resolved_markets_once()
            logger.info("markets: startup resolution-rescan covered %d markets", n)
        except Exception:  # noqa: BLE001 — boot must succeed
            logger.exception(
                "markets: startup resolution-rescan failed; "
                "periodic loop will retry"
            )
        # Cycle 26: kick the WS pool BEFORE the poll loop fires. The
        # first `poll_once` then drives the initial subscription set
        # off the freshly-upserted active markets. No-op when the pool
        # is None (feature flag off).
        if self._ws_pool is not None:
            await self._ws_pool.start()
            logger.info("markets: ws pool started")

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
            self._index_refresh_task,
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
        self._index_refresh_task = None
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        # Cycle 26: tear the WS pool down AFTER the poll loops so a
        # final in-flight `_sync_ws_subscriptions` doesn't race the
        # pool stop. No-op when the pool is None.
        if self._ws_pool is not None:
            await self._ws_pool.stop()

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

    async def _index_refresh_loop(self) -> None:
        """Cycle 26 stage-1 soak fix #5: replica-mode loop.

        Production owns the markets writes; this replica just keeps the
        in-memory `MarketIndex` fresh by reloading from the DB every
        `replica_refresh_seconds` (default matches `poll_seconds` so
        downstream stages see new markets within one production cycle).

        `MarketIndex.load()` is a single indexed read against the
        markets table — it doesn't contend with writers."""
        interval = float(_config_get(
            self._config,
            "replica_refresh_seconds",
            float(self._poll_seconds),
        ))
        while True:
            try:
                await asyncio.sleep(interval)
                n = await self.index.load(self._db)
                logger.debug("markets[replica]: index refreshed (%d rows)", n)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — keep the loop alive
                logger.exception(
                    "markets[replica]: index refresh failed; will retry"
                )

    # ---- single-cycle entry points (also called directly in tests) ----

    async def poll_once(self) -> int:
        """Run one Gamma fetch + upsert cycle. Returns the number of open
        markets observed (0 on a network failure).

        Cycle 26: `_fetch_universe` is the single orchestrator that owns
        the markets-vs-events sourcing choice. `poll_once` is unchanged
        in shape: parse → upsert → deactivate → publish.
        """
        if self._read_only_replica:
            raise RuntimeError(
                "MarketsStage.poll_once: write op called in read-only replica mode"
            )
        try:
            parsed = await self._fetch_universe()
        except httpx.HTTPError as exc:
            logger.warning("markets: Gamma fetch failed (%s); skipping upsert", exc)
            return 0

        open_ids: set[str] = set()
        for row in parsed:
            await self._upsert_market(row)
            open_ids.add(row["condition_id"])

        await self._deactivate_missing(open_ids)

        # Cycle 26: drive the WS pool's subscription set off the latest
        # tradable universe. No-op when `cfg.markets.enable_ws_feed=False`
        # (deps.ws_pool is None). Wrapped so a transient pool error
        # never blocks the bus publish — the cache going stale is
        # already a safe degradation mode (REST fallback in CLOBClient).
        try:
            await self._sync_ws_subscriptions()
        except Exception:  # noqa: BLE001
            logger.exception(
                "markets: ws subscription sync failed; "
                "cache will degrade to REST on the affected tokens"
            )

        await self._bus.publish(
            TOPIC_MARKETS_REFRESHED,
            {"count": len(parsed), "refreshed_at": _now_iso()},
        )
        return len(parsed)

    async def _sync_ws_subscriptions(self) -> None:
        """Drive the `ClobWsPool` subscribed-set to match the current
        tradable universe (every market with `active=1`). Idempotent —
        the pool computes the sub/unsub delta internally."""
        if self._ws_pool is None:
            return
        cur = await self._db.conn.execute(
            "SELECT token_yes, token_no FROM markets WHERE active = 1"
        )
        rows = await cur.fetchall()
        await cur.close()
        desired: set[str] = set()
        for r in rows:
            if r["token_yes"]:
                desired.add(r["token_yes"])
            if r["token_no"]:
                desired.add(r["token_no"])
        current = self._ws_pool.subscribed_set()
        to_sub = desired - current
        to_unsub = current - desired
        if to_sub:
            await self._ws_pool.subscribe(to_sub)
        if to_unsub:
            await self._ws_pool.unsubscribe(to_unsub)
        if to_sub or to_unsub:
            status = self._ws_pool.status()
            logger.info(
                "markets: ws subscriptions synced (+%d -%d) — "
                "clients=%d total=%d",
                len(to_sub), len(to_unsub),
                status["clients"], status["total_subscriptions"],
            )

    async def _fetch_universe(self) -> list[dict]:
        """Cycle 26: orchestrate the markets discovery strategy.

        Three modes, controlled by `cfg.markets.use_events_endpoint` and
        `cfg.markets.events_endpoint_dual_read`:

        1. `use_events_endpoint=False` — legacy `/markets` sweep only.
        2. `use_events_endpoint=True, dual_read=False` — `/events` sweep
           only; safest only after dual-read telemetry has shown the
           events endpoint covers the universe.
        3. `use_events_endpoint=True, dual_read=True` — fetch BOTH,
           union on `condition_id` with the events row winning on
           conflict (so it carries the event metadata). Markets seen
           only via `/markets` get `events_seen_in_last_poll=0`. Logs
           a per-cycle coverage histogram. This is the default the
           rollout sits in until promotion.

        Tests monkeypatch this method or its two inputs in one place.
        """
        use_events = bool(_config_get(self._config, "use_events_endpoint", False))
        dual_read = bool(_config_get(self._config, "events_endpoint_dual_read", True))

        if not use_events:
            raw_markets = await self._fetch_all_gamma()
            parsed: list[dict] = []
            for raw in raw_markets:
                row = _parse_gamma_row(raw)
                if row is not None:
                    row["events_seen_in_last_poll"] = 0
                    parsed.append(row)
            return parsed

        events_rows = await self._fetch_all_gamma_events()
        from_events: dict[str, dict] = {}
        for event_meta, raw in events_rows:
            row = _parse_gamma_row(raw, event_meta=event_meta)
            if row is None:
                continue
            row["events_seen_in_last_poll"] = 1
            from_events[row["condition_id"]] = row

        if not dual_read:
            logger.info(
                "markets: cycle-26 single-source via /events — %d markets",
                len(from_events),
            )
            return list(from_events.values())

        raw_markets = await self._fetch_all_gamma()
        from_markets: dict[str, dict] = {}
        for raw in raw_markets:
            row = _parse_gamma_row(raw)
            if row is None:
                continue
            from_markets[row["condition_id"]] = row

        merged: dict[str, dict] = {}
        only_events = 0
        only_markets = 0
        both = 0
        for cid, row in from_events.items():
            merged[cid] = row
            if cid in from_markets:
                both += 1
            else:
                only_events += 1
        for cid, row in from_markets.items():
            if cid in merged:
                continue
            row["events_seen_in_last_poll"] = 0
            merged[cid] = row
            only_markets += 1
        logger.info(
            "markets: cycle-26 dual-read coverage events=%d markets=%d "
            "both=%d only_events=%d only_markets=%d",
            len(from_events), len(from_markets), both, only_events, only_markets,
        )
        return list(merged.values())

    async def refresh_embeddings_once(self) -> int:
        """Embed every row missing an embedding OR stamped with a stale
        `embedder_id`. Returns the number of rows updated."""
        if self._read_only_replica:
            raise RuntimeError(
                "MarketsStage.refresh_embeddings_once: write op called in "
                "read-only replica mode"
            )
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
        if self._read_only_replica:
            raise RuntimeError(
                "MarketsStage.poll_resolved_markets_once: write op called in "
                "read-only replica mode"
            )
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

    async def _gamma_get_with_backoff(
        self,
        params: dict,
        *,
        max_retries: int = _GAMMA_429_MAX_RETRIES,
        url: str = _GAMMA_URL,
    ) -> httpx.Response:
        """GET Gamma with Retry-After-aware backoff on 429s.

        Cycle 13: previously every fetch path called `raise_for_status()`
        directly, so a single Cloudflare 429 would crash the whole poll
        cycle. Now we honor `Retry-After` (seconds-form is what
        Polymarket sends) when present, fall back to exponential
        backoff with jitter when not, and cap retries at `max_retries`.
        On the final 429 we still raise so the caller's existing error
        path fires. Non-429 responses (including 4xx other than 429 and
        5xx) are returned unchanged for the caller to handle.
        """
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=_GAMMA_TIMEOUT_SECONDS, headers=_GAMMA_HEADERS,
            )
        delay = _GAMMA_429_INITIAL_BACKOFF_S
        resp = None
        for attempt in range(max_retries + 1):
            resp = await self._client.get(url, params=params)
            if resp.status_code != 429:
                return resp
            retry_after = resp.headers.get("Retry-After")
            try:
                sleep_s = float(retry_after) if retry_after else delay
            except ValueError:
                # HTTP-date form — uncommon from Polymarket; use the fallback
                sleep_s = delay
            if attempt == max_retries:
                # Out of retries — let the caller's raise_for_status fire.
                return resp
            sleep_s = sleep_s + random.uniform(0.0, 0.5)
            logger.warning(
                "markets: Gamma 429 attempt %d/%d, sleeping %.2fs "
                "(Retry-After=%s)",
                attempt + 1, max_retries + 1, sleep_s,
                retry_after if retry_after else "<none>",
            )
            await asyncio.sleep(sleep_s)
            delay = min(delay * 2, _GAMMA_429_MAX_BACKOFF_S)
        # Unreachable in practice but keeps the typechecker happy.
        return resp  # type: ignore[return-value]

    async def _fetch_all_gamma(self) -> list[dict]:
        """Paginate Gamma's `/markets` endpoint and return every open row.

        Page size is 100 (Gamma caps every response at 100 regardless of the
        `limit` we ask for — verified in seulgi 2026-05-14). Termination
        condition is "page returned fewer than page_size rows", per the PR
        spec; the safety `max_pages` guards against an infinite loop if
        Gamma's pagination ever regresses.

        Cycle 13: 429s now go through `_gamma_get_with_backoff` instead
        of crashing the poll cycle.

        Cycle 23: Gamma caps pagination at `offset=10000`; any larger
        offset returns 422 Unprocessable Entity. Pre-cycle-23 the 422
        propagated out of this method, `poll_once` caught the HTTPError
        and dropped EVERY market already fetched (typically 10,000+).
        Net effect: the markets table stopped updating on 2026-05-23,
        which masked itself as "fee_rate fetcher silently broken" in the
        cycle-22 audit. New behavior: treat a 422 with non-empty
        accumulated `markets` as "end-of-pagination" — preserve what we
        have, log INFO, return.
        """
        markets: list[dict] = []
        offset = 0
        for _ in range(_GAMMA_MAX_PAGES):
            params = {
                "active": "true",
                "closed": "false",
                "limit": str(_GAMMA_PAGE_SIZE),
                "offset": str(offset),
            }
            try:
                resp = await self._gamma_get_with_backoff(params)
                resp.raise_for_status()
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 422 and markets:
                    logger.info(
                        "markets: Gamma 422 at offset=%d — treating as "
                        "end-of-pagination, preserving %d already-fetched "
                        "markets (cycle-23 fix; Gamma caps at offset=10000)",
                        offset, len(markets),
                    )
                    break
                raise
            page = resp.json()
            if not isinstance(page, list) or not page:
                break
            markets.extend(page)
            offset += len(page)
            if len(page) < _GAMMA_PAGE_SIZE:
                break
        logger.info("markets: fetched %d markets from Gamma", len(markets))
        return markets

    async def _fetch_all_gamma_events(self) -> list[tuple[dict, dict]]:
        """Cycle 26: paginate Gamma's `/events?active=true&closed=false`
        and return every (event_meta, market_row) pair.

        Each event row carries a nested ``markets: [...]`` array of the
        same shape as a `/markets` row. We flatten it here so the caller
        can run the existing `_parse_gamma_row` over each market with
        the parent event meta attached.

        Pagination shape and 422-as-EOP guard mirror `_fetch_all_gamma`
        — Gamma's offset cap behavior is consistent across the two
        endpoints. Returns `[]` on a network error so the caller's
        existing union-with-`/markets` keeps the cycle alive.
        """
        out: list[tuple[dict, dict]] = []
        offset = 0
        for _ in range(_GAMMA_MAX_PAGES):
            params = {
                "active": "true",
                "closed": "false",
                "limit": str(_GAMMA_PAGE_SIZE),
                "offset": str(offset),
            }
            try:
                resp = await self._gamma_get_with_backoff(
                    params, url=_GAMMA_EVENTS_URL
                )
                resp.raise_for_status()
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 422 and out:
                    logger.info(
                        "markets: events 422 at offset=%d — treating as "
                        "end-of-pagination, preserving %d already-fetched "
                        "events-markets",
                        offset, len(out),
                    )
                    break
                raise
            page = resp.json()
            if not isinstance(page, list) or not page:
                break
            for event_row in page:
                if not isinstance(event_row, dict):
                    continue
                event_meta = {
                    "id": event_row.get("id"),
                    "slug": event_row.get("slug"),
                }
                nested = event_row.get("markets")
                if not isinstance(nested, list):
                    continue
                for m in nested:
                    if isinstance(m, dict):
                        out.append((event_meta, m))
            offset += len(page)
            if len(page) < _GAMMA_PAGE_SIZE:
                break
        logger.info(
            "markets: fetched %d markets nested under Gamma events", len(out)
        )
        return out

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
        params = {"condition_ids": condition_id}
        try:
            resp = await self._gamma_get_with_backoff(params)
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
        # Cycle 18: closed=true rows (which arrive via the cycle-12 single-
        # market re-poll for resolution finalization) MUST land as
        # `active=0`, not `active=1`. The previous unconditional `active=1`
        # caused intermittent active-flickering: cycle-12 refetch sets
        # active=1, next regular poll's _deactivate_missing fires it back
        # to 0 — and stamps `closed_at = now()`, overwriting any
        # authoritative `closedTime` we observed. Also: prefer the
        # platform `closedTime` over our first-observation `now`.
        closed_flag = bool(row.get("closed", False))
        closed_time = row.get("closed_time") or None
        active_value = 0 if closed_flag else 1
        # closed_at: NULL when active; closedTime when present; else now.
        closed_at_value: Optional[str]
        if closed_flag:
            closed_at_value = closed_time if closed_time else now
        else:
            closed_at_value = None
        # Cycle 26: events-endpoint provenance. `polymarket_event_*` only
        # overwrite NULL → non-NULL (preserves the parent event even when a
        # single-market resolution re-poll later writes via `/markets`,
        # which has no event metadata). `events_seen_in_last_poll` is per-
        # cycle telemetry; always reflect the latest observation.
        event_id = row.get("polymarket_event_id")
        event_slug = row.get("polymarket_event_slug")
        events_seen = int(row.get("events_seen_in_last_poll") or 0)
        async with self._db.batch() as cur:
            await cur.execute(
                """
                INSERT INTO markets (
                    condition_id, slug, question, description, category,
                    end_date, volume_usdc, token_yes, token_no,
                    active, closed_at, first_seen_at, updated_at,
                    last_resolution_status, last_outcome_prices,
                    polymarket_event_id, polymarket_event_slug,
                    events_seen_in_last_poll
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(condition_id) DO UPDATE SET
                    slug = excluded.slug,
                    question = excluded.question,
                    description = excluded.description,
                    category = excluded.category,
                    end_date = excluded.end_date,
                    volume_usdc = excluded.volume_usdc,
                    token_yes = excluded.token_yes,
                    token_no = excluded.token_no,
                    polymarket_event_id = COALESCE(
                        markets.polymarket_event_id, excluded.polymarket_event_id
                    ),
                    polymarket_event_slug = COALESCE(
                        markets.polymarket_event_slug, excluded.polymarket_event_slug
                    ),
                    events_seen_in_last_poll = excluded.events_seen_in_last_poll,
                    -- Cycle 24: markets do not re-open after closing on
                    -- Polymarket. If we already have a historical
                    -- `closed_at`, force `active=0` regardless of what
                    -- Gamma reports — this completes the active-flicker
                    -- fix that cycle-18 left half-done (cycle-18 only
                    -- preserved closed_at via COALESCE but still let
                    -- `active` flip back to 1 when Gamma transiently
                    -- re-emits the market with closed=false). Trade 40
                    -- (handoff A) is the worst-case witness: DKG bought
                    -- NO on a market with active=1 + closed_at past.
                    active = CASE
                        WHEN markets.closed_at IS NOT NULL THEN 0
                        ELSE excluded.active
                    END,
                    -- Cycle 18: COALESCE preserves an earlier authoritative
                    -- closedTime if Gamma later transiently drops it. Only
                    -- overwrite NULL → non-NULL; never non-NULL → different.
                    closed_at = COALESCE(markets.closed_at, excluded.closed_at),
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
                    active_value,
                    closed_at_value,
                    now,
                    now,
                    row.get("uma_resolution_status"),
                    outcome_json,
                    event_id,
                    event_slug,
                    events_seen,
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
        # Cycle 10/13/15: per-market fee_rate + min_order_size fetch.
        # Cycles 10/13 fired only on first-seen (`fee_rate IS NULL`);
        # cycle 15 extends this to ALSO refresh rows whose
        # `fee_rate_fetched_at` is older than
        # `cfg.markets.market_info_refresh_hours` (default 168h). See
        # `_maybe_fetch_market_info` for the exact fire-conditions and
        # the legacy-row backfill note.
        await self._maybe_fetch_market_info(row["condition_id"])

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

    async def _maybe_fetch_market_info(self, condition_id: str) -> None:
        """If the CLOB client is wired and this market is either
        first-seen (`fee_rate IS NULL`) OR overdue for a refresh
        (`fee_rate_fetched_at` older than
        `cfg.markets.market_info_refresh_hours`, default 168h), fetch
        `GET /markets/<conditionId>` and persist whichever fields the
        response yielded (`fee_rate` and/or `min_order_size`). Silent
        no-op on any failure: closer.py falls back to the category
        lookup in `config.polymarket_fee_rates` and the trade-stage
        size gate falls back to `cfg.trade.min_position_usdc` only.

        Cycle 15 — refresh cadence. Cycles 10/13 only fired on
        first-seen, so a market whose Polymarket fee schedule changed
        mid-life would stay stale forever. We now ALSO fire when the
        row's `fee_rate_fetched_at` is NULL or older than the configured
        window. On a successful CLOB hit we stamp `fee_rate_fetched_at
        = now`; on a CLOB miss (None / fully-empty payload) we leave
        the timestamp UNCHANGED so the next poll cycle retries — this
        matches the "fetch failure shouldn't burn the budget" discipline
        from the cycle-10 contract.

        **Backfill consideration.** Legacy rows pre-cycle-15 have NULL
        `fetched_at`; they all qualify for the next refresh. That's a
        one-time burst of CLOB calls (one per existing market) on the
        first post-deploy poll. Acceptable: `get_market_info` hits the
        CLOB (NOT Gamma), which has no Cloudflare-fronted 429 path
        (see `clob.get_market_info` — it just logs and returns None on
        any non-2xx), so the cycle-13 `_gamma_get_with_backoff` helper
        is not relevant here. The CLOB calls run sequentially per
        upsert inside the existing poll loop, so the burst is naturally
        rate-shaped by the loop's own pace.

        Cycle 13 carry-over: prefer the unified `get_market_info`
        helper (returns both fields from one HTTP). Fall back to the
        cycle-10 single-purpose method if a CLOB stub doesn't expose
        the new method — keeps legacy test stubs and any custom
        wirings working.
        """
        if self._clob is None:
            # Cycle 23: WARN-once per process. Pre-cycle-23 this path
            # silently no-op'd, contributing to the cycle-22 audit's
            # mis-diagnosis ("clob must be None at runtime"). Real cycle-22
            # bug was elsewhere (Gamma 422 pagination breaking _upsert_market
            # before it could reach this fetcher), but the defense remains:
            # any future regression that re-introduces self._clob is None
            # produces a single visible WARN log.
            if not type(self)._fee_warn_seen:
                type(self)._fee_warn_seen = True
                logger.warning(
                    "markets: _maybe_fetch_market_info observed self._clob is "
                    "None — fee_rate fetcher will silently skip. Investigate "
                    "wire_pipeline() / StageDeps.clob wiring.",
                )
            return
        cur = await self._db.conn.execute(
            "SELECT active, fee_rate, fee_rate_fetched_at "
            "FROM markets WHERE condition_id = ?",
            (condition_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return
        # Cycle 26 followup #2: skip inactive markets entirely. Cycle 26's
        # events endpoint pulls in tens of thousands of inactive event
        # aggregators / closed sub-markets that have no CLOB endpoint;
        # every one of them would cost a 5s ReadTimeout on first-seen.
        # Inactive markets are not tradable, so fee_rate is irrelevant.
        if not row["active"]:
            return
        # Cycle 26 followup: `first_seen` must ALSO require
        # fee_rate_fetched_at IS NULL — otherwise dead-endpoint markets
        # (fee_rate stays NULL forever) would re-qualify every poll
        # despite the new fetched_at stamping. `_is_market_info_stale`
        # carries the cadence after the first attempt.
        first_seen = row["fee_rate"] is None and row["fee_rate_fetched_at"] is None
        stale = self._is_market_info_stale(row["fee_rate_fetched_at"])
        if not (first_seen or stale):
            return
        fee_rate: Optional[float]
        min_order_size: Optional[float] = None
        if hasattr(self._clob, "get_market_info"):
            info = await self._clob.get_market_info(condition_id)
            fee_rate = info.get("fee_rate") if info else None
            min_order_size = info.get("min_order_size") if info else None
        else:
            fee_rate = await self._clob.get_market_fee_rate(condition_id)
        # Cycle 26 followup: stamp `fee_rate_fetched_at = now` on EVERY
        # attempt — success, miss (info is None), or partial. Pre-fix the
        # miss path left fetched_at NULL so the next poll re-queued the
        # same market 120s later; with cycle-26's events-endpoint adding
        # ~2700 markets without CLOB endpoints (sub-markets, event
        # aggregators), that meant a sustained ~10/min ReadTimeout rate
        # forever. Soft-cooldown via fetched_at means dead-endpoint
        # markets get retried only on the cycle-15 cadence
        # (default 168h) instead of every poll.
        now = _now_iso()
        # Build a partial UPDATE that only touches the columns we
        # actually have values for; preserves any already-populated
        # column on the row. `fee_rate_fetched_at` is stamped on EVERY
        # attempt (see comment above) — markets without CLOB endpoints
        # will retain `fee_rate IS NULL` but get `fee_rate_fetched_at`
        # advanced, so the cycle-15 cadence prevents re-querying them
        # every poll.
        set_clauses = []
        params: list = []
        if fee_rate is not None:
            set_clauses.append("fee_rate = ?")
            params.append(fee_rate)
        if min_order_size is not None:
            set_clauses.append("min_order_size = ?")
            params.append(min_order_size)
        set_clauses.append("fee_rate_fetched_at = ?")
        params.append(now)
        set_clauses.append("updated_at = ?")
        params.append(now)
        params.append(condition_id)
        async with self._db.batch() as cur:
            await cur.execute(
                f"UPDATE markets SET {', '.join(set_clauses)} "  # noqa: S608
                f"WHERE condition_id = ?",
                tuple(params),
            )

    def _is_market_info_stale(self, fetched_at_iso: Optional[str]) -> bool:
        """True when `fetched_at_iso` is missing or older than
        `cfg.markets.market_info_refresh_hours`. NULL fetched_at counts
        as stale so legacy pre-cycle-15 rows get refreshed on the next
        poll (see backfill note in `_maybe_fetch_market_info`).
        """
        if not fetched_at_iso:
            return True
        try:
            ts = datetime.fromisoformat(fetched_at_iso.replace("Z", "+00:00"))
        except (TypeError, ValueError):
            # Malformed timestamp — treat as stale and let the next
            # successful fetch overwrite it with a well-formed ISO.
            return True
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - ts).total_seconds() / 3600.0
        return age_hours >= self._market_info_refresh_hours

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
        # Cycle 25-A: dead-market force-close. State 1 (closed_at set, no
        # UMA finalization) used to wait forever then emit a
        # RESOLUTION_TIMEOUT warning. We now actively close at entry_mid
        # once a market has been closed long enough that UMA isn't going
        # to deliver a real payout. Default 24h: well past the 2h
        # uncontested challenge window, short enough that the bot
        # un-sticks within a day.
        force_close_hours = float(
            _config_get(self._config, "dead_market_force_close_hours", 24.0)
        )
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

                # State 1: closed but no UMA status yet. Check timeouts
                # against the market's closed_at (or the trade's
                # decided_at if closed_at is somehow NULL).
                anchor_iso = r["closed_at"] or trade["decided_at"]
                age_h = _hours_since(anchor_iso, now_dt)

                # State 4 (cycle 25-A): dead-market force-close. Once a
                # market has been closed past force_close_hours and UMA
                # still hasn't finalized, we stop waiting and close at
                # entry_mid (gross PnL ≈ 0; entry fee was already paid).
                # This unsticks the closer; without it the trade
                # reschedules every 60s forever (the cycle-35 zombie bug).
                if age_h is not None and age_h > force_close_hours:
                    ok = await write_trade_closure(
                        self._db,
                        trade=trade,
                        exit_mid=trade["mid_price"],  # entry mid → 0 gross PnL
                        close_reason="market_dead_unresolved",
                        trade_cfg=self._config.trade,
                        category=r["category"],
                        market_fee_rate=r["fee_rate"],
                    )
                    if ok:
                        written += 1
                        logger.warning(
                            "markets: force-closed trade_id=%d (market %s "
                            "closed %0.1fh ago with no UMA finalization; "
                            "exit_mid=entry_mid=%.4f, close_reason="
                            "market_dead_unresolved)",
                            trade["id"], trade["market_id"], age_h,
                            trade["mid_price"],
                        )
                    if age_h > timeout_hours:
                        # If we somehow missed the force-close window and
                        # the trade is BEYOND the human-review threshold,
                        # also emit the legacy WARNING so soak watchers
                        # still see the long-tail alert. (Should be rare
                        # once the force-close path is healthy.)
                        logger.warning(
                            "RESOLUTION_TIMEOUT trade_id=%d market=%s waited=%.1fh "
                            "(timeout=%.1fh); force-close attempted above",
                            trade["id"], trade["market_id"], age_h, timeout_hours,
                        )
                    continue
                logger.info(
                    "markets: trade_id=%d market %s closed but UMA not yet "
                    "finalized; waiting (status=%r, age=%.1fh, "
                    "force_close_at=%.1fh)",
                    trade["id"], trade["market_id"],
                    status_raw, age_h if age_h is not None else -1.0,
                    force_close_hours,
                )
        return written
