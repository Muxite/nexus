"""
## Specification

**Purpose.** Continuous odds time-series capture for the noisy-explorer
container. For every `explorations` row published on
`TOPIC_EXPLORATION_CREATED`, this stage:

  1. Pre-news backfill (synchronous in `handle()`): calls
     `clob.get_price_history` once at t0 and persists samples that fall
     within `(t0 - capture_pre_secs, t0]` as `odds_observations` rows
     with `source='history'` and negative `offset_seconds`. Coarse but
     bounded — Polymarket's price-history fidelity is in the
     ~minutes range, so most explorations get 1-2 pre-news points.
     NULL pre-news data is honest and acceptable.

  2. Forward live capture: adds the exploration to an in-memory
     watchlist and polls `clob.get_top_of_book(token_yes)` every
     `cfg.explorer.odds_poll_secs` (default 15s) until
     `now >= capture_end_at`. Each tick writes one `odds_observations`
     row with `source='live'`, `offset_seconds = now - t0`. When the
     window elapses, the row is removed from the watchlist and the
     parent exploration is flipped to `capture_status='complete'`.

The watchlist is rehydrated from the DB at `start()` so explorer
restarts resume in-flight captures without losing data.

This stage is purely observational — it never trades, never gates,
never publishes back into the trade-decision path. It is the
data-collection arm of the explorer container.

**Public interface.**
- `OddsCaptureStage(deps: StageDeps)` — requires `deps.clob`.
- `await start()` — rehydrates active captures + launches the poll task.
- `await stop()` — cancels the task; leaves DB state intact for resume.
- `await handle(payload: dict)` — bus subscriber on
  `TOPIC_EXPLORATION_CREATED`.
- `await run_one_poll_cycle()` — single deterministic poll pass.
  Tests drive this directly instead of waiting for the background loop.

**Owned tables.** `odds_observations`. DDL registered with
`db_module.register_schema()` at import time.

**Sign convention for `offset_seconds`.**
  - `< 0` → pre-news (history backfill). The most-recent pre-news sample
    has the largest (least-negative) offset.
  - `0` → ~t0 (not separately recorded; t0 itself is the exploration row).
  - `> 0` → post-news (live poll).

**Invariants.**
- One pre-news backfill batch per exploration_id; never re-run.
- Multiple live samples per exploration_id, but each forward tick is
  uniquely indexed by `(exploration_id, sampled_at)` to prevent
  duplicate writes on a poll loop replay.
- `capture_status` on `explorations` transitions
  `pending → active → complete`; once `complete` the row is never
  re-touched by this stage.
- `cycle_id` is denormalised from the source exploration so analytics
  queries can filter `odds_observations` by cycle without a JOIN.

**Failure modes.**
- `clob.get_price_history` returns `None`: no pre-news rows are written;
  exploration still transitions to `active` and the live poll proceeds.
- `clob.get_top_of_book` returns `None` for a tick: an observation row
  is written with NULL `mid/bid/ask/spread` so the gap is visible (the
  market may have vanished mid-capture; downstream analysis can
  distinguish "no data" from "data not yet captured").
- An unhandled exception inside the poll body is logged and the loop
  continues — one bad row must not kill the poller.

**Non-goals.**
- Trade execution, PnL calculation, gating — pure observation only.
- Live odds for production markets — that's the impact tracker's
  discrete-bucket job (see `impact_tracker.py`). This stage is the
  explorer-specific continuous-tick equivalent.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from app import db as db_module
from app.deps import StageDeps
from app.topics import TOPIC_EXPLORATION_CREATED

logger = logging.getLogger(__name__)


_ODDS_DDL = [
    """
    CREATE TABLE IF NOT EXISTS odds_observations (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        exploration_id  INTEGER NOT NULL REFERENCES explorations(id),
        token_id        TEXT NOT NULL,
        source          TEXT NOT NULL CHECK(source IN ('history','live')),
        sampled_at      TEXT NOT NULL,
        offset_seconds  INTEGER NOT NULL,
        mid             REAL,
        best_bid        REAL,
        best_ask        REAL,
        spread          REAL,
        cycle_id        INTEGER
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_odds_obs_exploration "
    "ON odds_observations(exploration_id)",
    "CREATE INDEX IF NOT EXISTS idx_odds_obs_offset "
    "ON odds_observations(exploration_id, offset_seconds)",
    # Guard against duplicate live ticks on a poll-loop replay.
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_odds_obs_sample "
    "ON odds_observations(exploration_id, sampled_at, source)",
]
db_module.register_schema("app/stages/odds_capture.py", _ODDS_DDL)


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


class OddsCaptureStage:
    """Pre-news backfill + continuous post-news odds capture."""

    def __init__(self, deps: StageDeps) -> None:
        if deps.clob is None:
            raise RuntimeError("OddsCaptureStage requires deps.clob")
        self._db = deps.db
        self._clob = deps.clob
        self._cfg = deps.config.explorer
        # In-memory watchlist of active captures, keyed by exploration_id.
        # Each value is the cached exploration metadata needed per-tick.
        self._watch: dict[int, dict] = {}
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        await self._rehydrate()
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_forever())

    async def stop(self) -> None:
        self._stop_event.set()
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass

    async def handle(self, payload: dict) -> None:
        exploration_id = payload.get("exploration_id")
        if exploration_id is None:
            logger.warning(
                "odds_capture: payload missing exploration_id: %r", payload
            )
            return
        exploration_id = int(exploration_id)
        token_yes = payload.get("token_yes")
        t0_iso = payload.get("t0_iso")
        capture_end_at = payload.get("capture_end_at")
        if not token_yes or not t0_iso or not capture_end_at:
            logger.warning(
                "odds_capture: payload missing required field: %r", payload
            )
            return
        cycle_id = await self._read_cycle_id(exploration_id)

        await self._backfill_pre_news(
            exploration_id=exploration_id,
            token_id=str(token_yes),
            t0_iso=t0_iso,
            cycle_id=cycle_id,
        )
        await self._mark_active(exploration_id)
        self._watch[exploration_id] = {
            "token_id": str(token_yes),
            "t0_iso": t0_iso,
            "capture_end_at": capture_end_at,
            "cycle_id": cycle_id,
        }

    # ------------------------------------------------------------------
    # Pre-news backfill
    # ------------------------------------------------------------------

    async def _backfill_pre_news(
        self,
        *,
        exploration_id: int,
        token_id: str,
        t0_iso: str,
        cycle_id: Optional[int],
    ) -> None:
        history = None
        try:
            history = await self._clob.get_price_history(
                token_id,
                interval=str(self._cfg.capture_pre_interval),
                fidelity=int(self._cfg.capture_pre_fidelity),
            )
        except Exception:  # noqa: BLE001
            logger.warning(
                "odds_capture: get_price_history(%s) raised", token_id
            )
        if not history:
            return
        t0 = _parse_iso(t0_iso)
        if t0 is None:
            return
        t0_ts = int(t0.timestamp())
        window_start_ts = t0_ts - int(self._cfg.capture_pre_secs)
        rows: list[tuple] = []
        for ts, mid in history:
            try:
                ts_int = int(ts)
                mid_f = float(mid)
            except (TypeError, ValueError):
                continue
            if ts_int < window_start_ts or ts_int > t0_ts:
                continue
            offset = ts_int - t0_ts
            sampled_at = datetime.fromtimestamp(
                ts_int, tz=timezone.utc
            ).isoformat()
            rows.append((
                exploration_id, token_id, "history", sampled_at, offset,
                mid_f, None, None, None, cycle_id,
            ))
        if not rows:
            return
        async with self._db.batch() as cur:
            await cur.executemany(
                """
                INSERT OR IGNORE INTO odds_observations
                    (exploration_id, token_id, source, sampled_at,
                     offset_seconds, mid, best_bid, best_ask, spread,
                     cycle_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    # ------------------------------------------------------------------
    # Forward live capture
    # ------------------------------------------------------------------

    async def _run_forever(self) -> None:
        try:
            while not self._stop_event.is_set():
                try:
                    await self.run_one_poll_cycle()
                except Exception:  # noqa: BLE001
                    logger.exception("odds_capture: poll cycle failed")
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=float(self._cfg.odds_poll_secs),
                    )
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            pass

    async def run_one_poll_cycle(self) -> None:
        """Single poll pass over the watchlist. Each active capture
        gets one CLOB read; expired captures are graduated to
        `capture_status='complete'` and dropped from the watchlist.

        `capture_end_reason` is written when a capture exits so the
        sandbox can distinguish full-window rows from cut-short rows.
        """
        if not self._watch:
            return
        now = _now()
        now_iso = now.isoformat()
        completed: list[tuple[int, str]] = []
        for exploration_id, meta in list(self._watch.items()):
            end_at = _parse_iso(meta["capture_end_at"])
            if end_at is None or now >= end_at:
                # Window elapsed. The dense poll already covers every
                # interior checkpoint; the only gap is the *final* one
                # (the loop would otherwise graduate before any tick lands
                # at/after t0+capture_post_secs). Force one last snapshot
                # so the terminal checkpoint always has a usable book —
                # the sandbox's `offset >= horizon` exit lookup depends on
                # it. See irene/capture_schedule.py.
                t0 = _parse_iso(meta["t0_iso"])
                if t0 is not None:
                    offset = int((now - t0).total_seconds())
                    mid, bid, ask, spread = await self._read_book(
                        meta["token_id"]
                    )
                    await self._write_live_sample(
                        exploration_id=exploration_id,
                        token_id=meta["token_id"],
                        sampled_at=now_iso,
                        offset_seconds=offset,
                        mid=mid,
                        bid=bid,
                        ask=ask,
                        spread=spread,
                        cycle_id=meta.get("cycle_id"),
                    )
                completed.append((exploration_id, "window_complete"))
                continue
            t0 = _parse_iso(meta["t0_iso"])
            if t0 is None:
                completed.append((exploration_id, "capture_aborted"))
                continue
            offset = int((now - t0).total_seconds())
            mid, bid, ask, spread = await self._read_book(meta["token_id"])
            await self._write_live_sample(
                exploration_id=exploration_id,
                token_id=meta["token_id"],
                sampled_at=now_iso,
                offset_seconds=offset,
                mid=mid,
                bid=bid,
                ask=ask,
                spread=spread,
                cycle_id=meta.get("cycle_id"),
            )
        for exploration_id, reason in completed:
            await self._mark_complete(exploration_id, reason=reason)
            self._watch.pop(exploration_id, None)

    async def _read_book(
        self, token_id: str
    ) -> tuple[Optional[float], Optional[float],
               Optional[float], Optional[float]]:
        try:
            book = await self._clob.get_top_of_book(token_id)
        except Exception:  # noqa: BLE001
            logger.warning(
                "odds_capture: get_top_of_book(%s) raised", token_id
            )
            return None, None, None, None
        if book is None:
            return None, None, None, None
        bid, ask = book
        mid = (bid + ask) / 2.0
        spread = ask - bid
        return mid, bid, ask, spread

    async def _write_live_sample(
        self,
        *,
        exploration_id: int,
        token_id: str,
        sampled_at: str,
        offset_seconds: int,
        mid: Optional[float],
        bid: Optional[float],
        ask: Optional[float],
        spread: Optional[float],
        cycle_id: Optional[int],
    ) -> None:
        async with self._db.batch() as cur:
            await cur.execute(
                """
                INSERT OR IGNORE INTO odds_observations
                    (exploration_id, token_id, source, sampled_at,
                     offset_seconds, mid, best_bid, best_ask, spread,
                     cycle_id)
                VALUES (?, ?, 'live', ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    exploration_id, token_id, sampled_at, offset_seconds,
                    mid, bid, ask, spread, cycle_id,
                ),
            )

    # ------------------------------------------------------------------
    # Status transitions + rehydrate
    # ------------------------------------------------------------------

    async def _read_cycle_id(self, exploration_id: int) -> Optional[int]:
        cur = await self._db.conn.execute(
            "SELECT cycle_id FROM explorations WHERE id=?",
            (exploration_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return None
        return int(row["cycle_id"])

    async def _mark_active(self, exploration_id: int) -> None:
        async with self._db.batch() as cur:
            await cur.execute(
                "UPDATE explorations SET capture_status='active' "
                "WHERE id=? AND capture_status='pending'",
                (exploration_id,),
            )

    async def _mark_complete(
        self, exploration_id: int, *, reason: str = "window_complete",
    ) -> None:
        """Flip the exploration to `capture_status='complete'` and record
        why the capture loop exited. Reason values:
          - window_complete: t0+capture_post_secs reached (the happy path)
          - capture_aborted: bad metadata (missing t0 / end_at) — should
            be rare; if it isn't, something upstream is broken
          - evicted_by_cap: bounced from the watchlist because we ran out
            of capacity (currently never set; reserved for the future
            backpressure path)
          - market_closed:   not currently differentiated from
            window_complete (we'd need a CLOB signal); reserved
          - error:           reserved for unhandled-exception exits
        """
        async with self._db.batch() as cur:
            await cur.execute(
                "UPDATE explorations "
                "SET capture_status='complete', capture_end_reason=? "
                "WHERE id=? AND capture_status IN ('pending','active')",
                (reason, exploration_id),
            )

    async def _rehydrate(self) -> None:
        """Reload all in-flight captures from `explorations` whose
        `capture_status` is `pending` or `active` and whose
        `capture_end_at` is still in the future. Pending rows lost
        their pre-news backfill on the prior process exit; the live
        capture portion still runs to recover what we can.
        """
        cur = await self._db.conn.execute(
            "SELECT id, token_yes, t0_iso, capture_end_at, cycle_id "
            "FROM explorations "
            "WHERE capture_status IN ('pending','active')"
        )
        rows = await cur.fetchall()
        await cur.close()
        now = _now()
        for row in rows:
            end_at = _parse_iso(row["capture_end_at"])
            if end_at is None or now >= end_at:
                # Window already elapsed while we were down — graduate
                # to `complete` so we don't poll forever.
                await self._mark_complete(
                    int(row["id"]), reason="window_complete",
                )
                continue
            token = row["token_yes"]
            t0 = row["t0_iso"]
            if not token or not t0:
                continue
            self._watch[int(row["id"])] = {
                "token_id": str(token),
                "t0_iso": t0,
                "capture_end_at": row["capture_end_at"],
                "cycle_id": (
                    int(row["cycle_id"]) if row["cycle_id"] is not None
                    else None
                ),
            }
        if self._watch:
            logger.info(
                "odds_capture: rehydrated %d in-flight captures",
                len(self._watch),
            )
