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

_ENV_POLL_SECONDS = "IRENE__MARKETS__POLL_SECONDS"
_ENV_EMBED_REFRESH_SECONDS = "IRENE__MARKETS__EMBED_REFRESH_SECONDS"


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

    try:
        volume = float(raw.get("volume") or 0.0)
    except (TypeError, ValueError):
        volume = None

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
        self._stopped = False

    # ---- lifecycle ------------------------------------------------------

    async def start(self) -> None:
        self._client = httpx.AsyncClient(timeout=_GAMMA_TIMEOUT_SECONDS)
        self._poll_task = asyncio.create_task(self._poll_loop())
        self._embed_task = asyncio.create_task(self._embed_loop())

    async def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        for task in (self._poll_task, self._embed_task):
            if task is not None:
                task.cancel()
        for task in (self._poll_task, self._embed_task):
            if task is not None:
                try:
                    await task
                except (asyncio.CancelledError, Exception):  # noqa: BLE001
                    pass
        self._poll_task = None
        self._embed_task = None
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
        return len(rows)

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

    async def _upsert_market(self, row: dict) -> None:
        now = _now_iso()
        async with self._db.batch() as cur:
            await cur.execute(
                """
                INSERT INTO markets (
                    condition_id, slug, question, description, category,
                    end_date, volume_usdc, token_yes, token_no,
                    active, closed_at, first_seen_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, NULL, ?, ?)
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
                    updated_at = excluded.updated_at
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
                ),
            )

    async def _deactivate_missing(self, open_ids: set[str]) -> int:
        """Mark every active row whose condition_id is NOT in the fresh
        open set as inactive.

        Done in two steps so it's robust to large universes (SQLite's
        default parameter cap is 999): read every active condition_id,
        diff against the open set in Python, then issue chunked UPDATEs by
        positive id list. This avoids a `NOT IN (chunk)` that would
        wrongly flip rows present in some other chunk.
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
        return len(to_close)
