"""
## Specification

**Purpose.** Diagnostic poller that verifies irene's *speed* thesis
(`docs/philosophy.md`): for every executed trade, anchor a row in
`price_impacts` at the trade's `decided_at`, then poll the CLOB mid at
+1m/+5m/+15m/+30m/+60m/+2h/+4h/+8h/+24h and record whether the price
moved the way we bet. The dataset answers the only question that
matters for an interpret-and-run bot: *did we get there before the
market repriced?*

This stage is purely diagnostic. It does NOT close trades, write PnL, or
gate anything. Replay reads `price_impacts` to bucket post-trade outcomes
into "edge realised", "no signal", "wrong direction".

**Cycle 11 — W6 pre-news drift.** Alongside the post-trade buckets we
also capture two pre-trade columns at handle()-time:
`pre_news_drift_15m` and `pre_news_drift_60m`. These measure the mid
move in the (pub - 15m, pub] and (pub - 60m, pub] windows from the
CLOB price history. Sign convention matches the post-trade buckets —
positive = market drifted in our direction before the article was
published (i.e. already priced in via rumours / X leaks / insider
action). Distinguishes "the market never read this article" from "the
market already knew" when post-trade buckets are flat.

**Cycle 11 — long-tail buckets.** Buckets at +2h/+4h/+8h/+24h were
added to measure retail-tail repricing horizons (see RAG slice in
`unstructured answers.md`). `cfg.impact.max_age_hours` default rose to
24.0 so the longest bucket has time to fire. Existing rows pre-cycle-11
keep the long-tail columns NULL — no backfill.

**Public interface.**
- `ImpactTrackerStage`:
  - `ImpactTrackerStage(deps: StageDeps)` — requires `deps.clob`.
  - `await start()` — launches the background poll task.
  - `await stop()` — cancels the task; idempotent.
  - `await handle(payload: dict)` — subscriber on `TOPIC_TRADE_DECIDED`.
    For payloads with `outcome='execute'`, inserts an anchor row.
  - `await run_one_poll_cycle()` — single deterministic poll pass.
    Tests drive this directly instead of waiting for the background loop.

**Owned tables** (`docs/modularization.md` §4): `price_impacts`. DDL is
registered with `db_module.register_schema()` at import time.

**Sign convention for `edge_realised_tXs`.** This is the column that
answers "did the price move the way we bet?":

  - YES direction: `edge_realised = price - entry_mid` (price went up
    after we bought YES ⇒ positive).
  - NO direction:  `edge_realised = entry_mid - price` (price went down
    after we bought NO  ⇒ positive).

A positive number means the speed thesis paid out for this trade. A
negative number means the market moved against us. `delta_tXs` is the
unsigned `price - entry_mid` and is always present alongside.

**Invariants.**
- One row per trade: `UNIQUE(trade_id)` makes `handle()` idempotent on
  re-fires.
- `t0_iso = trades.decided_at`, NOT this row's `created_at`. Lets replay
  join `(articles JOIN trades JOIN price_impacts)` to compute
  "how stale was the news when we acted" without a second timestamp source.
- `entry_mid` is a denormalised copy of `trades.mid_price`; saves the
  per-poll join.
- `cycle_id` is intentionally NOT carried — `price_impacts` is global
  diagnostic data tied to physical time (the +Nm buckets), not to the
  cycle partition the trade was decided in. Replay joins back through
  `trade_id` if it needs the cycle.
- `trades` and `markets` are read-only here.
- `is_tracer` / `tracer_id` are denormalised from the source trade row
  so tracer-flagged trades stay distinguishable in `price_impacts`
  without a join.

**Side effects.** Reads `trades` + `markets`. Calls
`deps.clob.get_mid(token_id)` for each due bucket. Writes
`price_impacts` inside `db.batch()`. Spawns a background asyncio task in
`start()`; cancels it in `stop()`.

**Failure modes.**
- CLOB returns `None` for a token: the bucket stays NULL; the next poll
  retries (until `cfg.impact.max_age_hours` elapses).
- `trades` / `markets` row missing on handle: WARNING + skip. (A trade
  payload arriving for a non-existent trade is a wiring bug; we don't
  crash the bus subscriber for it.)
- An unhandled exception inside the poll body is logged and the loop
  continues — a single bad row must not kill the poller.

**Non-goals.**
- Closing trades / writing PnL (that's `stages/closer.py`).
- `/prices-history` backfill, pre-move detection (a later chunk; mid
  polling at +1/5/15/30/60m is enough for the first cut).
- Gating, filtering, or any path that influences trade execution.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from app import db as db_module
from app.aioutil import cancel_and_join
from app.deps import StageDeps
from app.topics import TOPIC_TRADE_DECIDED

logger = logging.getLogger(__name__)


_IMPACT_DDL = [
    """
    CREATE TABLE IF NOT EXISTS price_impacts (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        trade_id        INTEGER NOT NULL UNIQUE REFERENCES trades(id),
        market_id       TEXT NOT NULL,
        token_id        TEXT NOT NULL,
        direction       TEXT NOT NULL CHECK(direction IN ('YES','NO')),
        t0_iso          TEXT NOT NULL,
        entry_mid       REAL NOT NULL,
        price_t60s      REAL,
        price_t300s     REAL,
        price_t900s     REAL,
        price_t1800s    REAL,
        price_t3600s    REAL,
        delta_t60s      REAL,
        delta_t300s     REAL,
        delta_t900s     REAL,
        delta_t1800s    REAL,
        delta_t3600s    REAL,
        edge_realised_t60s   REAL,
        edge_realised_t300s  REAL,
        edge_realised_t900s  REAL,
        edge_realised_t1800s REAL,
        edge_realised_t3600s REAL,
        is_tracer       INTEGER NOT NULL DEFAULT 0,
        tracer_id       TEXT,
        created_at      TEXT NOT NULL,
        last_polled_at  TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_price_impacts_trade_id ON price_impacts(trade_id)",
    "CREATE INDEX IF NOT EXISTS idx_price_impacts_market_id ON price_impacts(market_id)",
    "CREATE INDEX IF NOT EXISTS idx_price_impacts_last_polled ON price_impacts(last_polled_at)",
    # Cycle 11 (W6): pre-news drift — was the article already priced in
    # before publication? Positive ⇒ market moved IN our trade direction
    # in the pre-published window; negative ⇒ against us; NULL ⇒ history
    # didn't reach back that far. Computed at handle()-time from
    # `CLOBClient.get_price_history`. See plans/latency-repricing.md §W6.
    "ALTER TABLE price_impacts ADD COLUMN pre_news_drift_15m REAL",
    "ALTER TABLE price_impacts ADD COLUMN pre_news_drift_60m REAL",
    # Cycle 11 (impact-tracker extension): long-tail buckets for retail
    # repricing horizons (see RAG / latency-repricing.md). Existing rows
    # keep these NULL — no backfill — and the poller fills them only for
    # new trades that survive past the new max_age_hours window.
    "ALTER TABLE price_impacts ADD COLUMN price_t7200s  REAL",
    "ALTER TABLE price_impacts ADD COLUMN price_t14400s REAL",
    "ALTER TABLE price_impacts ADD COLUMN price_t28800s REAL",
    "ALTER TABLE price_impacts ADD COLUMN price_t86400s REAL",
    "ALTER TABLE price_impacts ADD COLUMN delta_t7200s  REAL",
    "ALTER TABLE price_impacts ADD COLUMN delta_t14400s REAL",
    "ALTER TABLE price_impacts ADD COLUMN delta_t28800s REAL",
    "ALTER TABLE price_impacts ADD COLUMN delta_t86400s REAL",
    "ALTER TABLE price_impacts ADD COLUMN edge_realised_t7200s  REAL",
    "ALTER TABLE price_impacts ADD COLUMN edge_realised_t14400s REAL",
    "ALTER TABLE price_impacts ADD COLUMN edge_realised_t28800s REAL",
    "ALTER TABLE price_impacts ADD COLUMN edge_realised_t86400s REAL",
]
db_module.register_schema("app/stages/impact_tracker.py", _IMPACT_DDL)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now().isoformat()


def _parse_iso(value: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def _mid_at_or_before(
    history: list[tuple[int, float]], target_ts: float
) -> Optional[float]:
    """Walk a CLOB price-history series (unix-ts, mid) and return the
    mid at the *latest* sample whose timestamp is ≤ `target_ts`. Returns
    `None` if every sample is strictly newer than `target_ts` (history
    doesn't reach back far enough — the pre-news drift signal is NULL
    for that window). `history` is assumed to be roughly time-ordered
    but we don't rely on it: a linear scan is cheap at the fidelity
    Polymarket returns (~hundreds of points)."""
    best_ts: Optional[int] = None
    best_mid: Optional[float] = None
    for ts, mid in history:
        if ts > target_ts:
            continue
        if best_ts is None or ts > best_ts:
            best_ts = ts
            best_mid = mid
    return best_mid


class ImpactTrackerStage:
    # Bucket offsets in seconds since `t0_iso`. The list order is the
    # column-fill order; never reorder without a schema migration since
    # the column names encode the offsets. Cycle 11 added the long-tail
    # buckets (7200..86400) to measure retail repricing horizons; raise
    # `cfg.impact.max_age_hours` to >= 24.0 to give the last bucket time
    # to fire.
    BUCKETS_SECONDS: tuple[int, ...] = (
        60, 300, 900, 1800, 3600,
        7200, 14400, 28800, 86400,
    )

    def __init__(self, deps: StageDeps) -> None:
        if deps.clob is None:
            raise RuntimeError("ImpactTrackerStage requires deps.clob")
        self._db = deps.db
        self._clob = deps.clob
        self._cfg = deps.config.impact
        self._task: Optional[asyncio.Task] = None
        self._stopped = False

    # ---- lifecycle -----------------------------------------------------

    async def start(self) -> None:
        self._task = asyncio.create_task(self._poll_loop())

    async def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        if self._task is not None:
            await cancel_and_join(self._task, log=logger)
            self._task = None

    async def handle(self, payload: dict) -> None:
        if payload.get("outcome") != "execute":
            return
        trade_id = payload.get("trade_id")
        if trade_id is None:
            return
        trade = await self._fetch_trade(int(trade_id))
        if trade is None:
            logger.warning(
                "impact: trade_id=%s referenced by payload but row missing",
                trade_id,
            )
            return
        tokens = await self._fetch_tokens(trade["market_id"])
        if tokens is None:
            logger.warning(
                "impact: market %s missing for trade_id=%d — skipping",
                trade["market_id"], trade["id"],
            )
            return
        direction = trade["direction"]
        token_id = tokens["token_yes"] if direction == "YES" else tokens["token_no"]
        if not token_id:
            logger.warning(
                "impact: trade_id=%d market %s has no %s token — skipping",
                trade["id"], trade["market_id"], direction,
            )
            return

        # Pre-news drift (W6): did the market already move before the
        # article was published? Fetch the article's `published_at` via
        # the signal join, pull the CLOB price history covering the
        # pre-published window, and compute mid(pub) - mid(pub - 15m)
        # / - 60m. NULL when history is missing or doesn't reach back
        # that far. Failures here NEVER block the anchor-row insert —
        # the W6 diagnostic is best-effort.
        pre_15m, pre_60m = await self._compute_pre_news_drift(
            trade_id=trade["id"],
            token_id=token_id,
            direction=direction,
        )

        async with self._db.batch() as cur:
            await cur.execute(
                """
                INSERT OR IGNORE INTO price_impacts
                    (trade_id, market_id, token_id, direction, t0_iso,
                     entry_mid, pre_news_drift_15m, pre_news_drift_60m,
                     is_tracer, tracer_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trade["id"],
                    trade["market_id"],
                    token_id,
                    direction,
                    trade["decided_at"],
                    float(trade["mid_price"]),
                    pre_15m,
                    pre_60m,
                    1 if trade["is_tracer"] else 0,
                    trade["tracer_id"],
                    _now_iso(),
                ),
            )

    # ---- background loop ----------------------------------------------

    async def _poll_loop(self) -> None:
        while True:
            try:
                await self.run_one_poll_cycle()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — never let the loop die
                logger.exception("impact poll cycle failed; continuing")
            await asyncio.sleep(self._cfg.poll_seconds)

    async def run_one_poll_cycle(self) -> int:
        """Run a single pass of the bucket-fill loop. Returns the number
        of (row × bucket) cells filled. Exposed so tests can drive it
        without sleeping."""
        rows = await self._fetch_unfilled_rows()
        if not rows:
            return 0
        now = _now()
        cutoff = timedelta(hours=float(self._cfg.max_age_hours))
        filled = 0
        for row in rows:
            t0 = _parse_iso(row["t0_iso"])
            if t0 is None:
                continue
            # max_age cutoff: stop polling rows whose t0 is past
            # `max_age_hours`. The t=3600s bucket is the last interesting
            # one; anything past that is noise on Polymarket's news cycle.
            if now - t0 > cutoff:
                continue
            filled += await self._fill_buckets(row, t0=t0, now=now)
        return filled

    # ---- internals -----------------------------------------------------

    async def _fill_buckets(self, row: dict, *, t0: datetime, now: datetime) -> int:
        age_s = (now - t0).total_seconds()
        token_id = row["token_id"]
        direction = row["direction"]
        entry_mid = float(row["entry_mid"])
        updates: list[tuple[str, float, float, float]] = []
        for bucket_s in self.BUCKETS_SECONDS:
            col = f"price_t{bucket_s}s"
            if row.get(col) is not None:
                continue
            if age_s < bucket_s:
                # Bucket not due yet. Walk continues — a later bucket may
                # *also* be unfilled but earlier ones (in seconds-order)
                # short-circuit so we never poll a future cell.
                break
            # Cycle 26: impact-tracker measures post-trade drift against an
            # authoritative source. The WebSocket-fed cache is fresh enough
            # for the closer's hot path, but here we explicitly want the
            # REST midpoint — otherwise we'd be measuring the bot's own
            # reflection of the live feed it's reading from.
            mid = await self._clob.get_mid(token_id, prefer_live=False)
            if mid is None:
                # CLOB transient failure: leave the cell NULL for the
                # next sweep. Don't bail on the whole row — later buckets
                # may have a different cached response.
                logger.info(
                    "impact: trade_id=%d bucket=%ds no CLOB mid; retry next poll",
                    row["id"], bucket_s,
                )
                continue
            mid_f = float(mid)
            delta = mid_f - entry_mid
            edge = delta if direction == "YES" else -delta
            updates.append((col, mid_f, delta, edge))

        if not updates:
            return 0

        # All bucket updates for one row land in a single batch so the
        # DB stays consistent if we crash mid-flush.
        set_clauses: list[str] = []
        params: list[object] = []
        for col, price, delta, edge in updates:
            bucket_s = col[len("price_t"):-1]  # "t60s" → "60"
            set_clauses.append(f"{col} = ?")
            params.append(price)
            set_clauses.append(f"delta_t{bucket_s}s = ?")
            params.append(delta)
            set_clauses.append(f"edge_realised_t{bucket_s}s = ?")
            params.append(edge)
        set_clauses.append("last_polled_at = ?")
        params.append(now.isoformat())
        params.append(row["id"])
        async with self._db.batch() as cur:
            await cur.execute(
                f"UPDATE price_impacts SET {', '.join(set_clauses)} WHERE id = ?",
                params,
            )
        return len(updates)

    async def _fetch_trade(self, trade_id: int) -> Optional[dict]:
        cur = await self._db.conn.execute(
            """
            SELECT id, market_id, direction, mid_price, decided_at,
                   is_tracer, tracer_id
            FROM trades
            WHERE id = ?
            """,
            (trade_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return None
        return {
            "id": row["id"],
            "market_id": row["market_id"],
            "direction": row["direction"],
            "mid_price": float(row["mid_price"]),
            "decided_at": row["decided_at"],
            "is_tracer": int(row["is_tracer"] or 0),
            "tracer_id": row["tracer_id"],
        }

    async def _fetch_tokens(self, market_id: str) -> Optional[dict]:
        cur = await self._db.conn.execute(
            "SELECT token_yes, token_no FROM markets WHERE condition_id = ?",
            (market_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return None
        return {"token_yes": row["token_yes"], "token_no": row["token_no"]}

    async def _fetch_article_published_at(self, trade_id: int) -> Optional[datetime]:
        """Return the article's `published_at` for a trade, via the
        signals join. DKG-path trades (signal_id NULL) and rows whose
        article has no `published_at` return None — pre-news drift is
        a best-effort diagnostic, not a hard requirement."""
        cur = await self._db.conn.execute(
            """
            SELECT a.published_at AS published_at
            FROM trades t
            JOIN signals s  ON s.id = t.signal_id
            JOIN articles a ON a.id = s.article_id
            WHERE t.id = ?
            """,
            (trade_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return None
        return _parse_iso(row["published_at"]) if row["published_at"] else None

    async def _compute_pre_news_drift(
        self,
        *,
        trade_id: int,
        token_id: str,
        direction: str,
    ) -> tuple[Optional[float], Optional[float]]:
        """Return `(pre_news_drift_15m, pre_news_drift_60m)`.

        Semantics: `mid(published_at) - mid(published_at - window)` then
        multiplied by `direction_sign` (+1 for YES, -1 for NO) so a
        positive value ALWAYS means the market moved in our trade
        direction during the pre-news window — i.e. the article was
        already (partially) priced in.

        Returns `(None, None)` on any failure (missing article,
        missing history, history doesn't reach back far enough). Never
        raises — pre-news drift is a diagnostic; it must not block the
        anchor-row insert.
        """
        try:
            published_at = await self._fetch_article_published_at(trade_id)
        except Exception:  # noqa: BLE001
            logger.exception(
                "impact: pre-news-drift article lookup failed for trade %d",
                trade_id,
            )
            return (None, None)
        if published_at is None:
            return (None, None)
        try:
            history = await self._clob.get_price_history(token_id)
        except Exception:  # noqa: BLE001
            logger.exception(
                "impact: pre-news-drift price-history fetch failed for trade %d",
                trade_id,
            )
            return (None, None)
        if not history:
            return (None, None)
        sign = 1.0 if direction == "YES" else -1.0
        pub_ts = published_at.timestamp()
        mid_at_pub = _mid_at_or_before(history, pub_ts)
        if mid_at_pub is None:
            return (None, None)
        mid_15m = _mid_at_or_before(history, pub_ts - 15 * 60)
        mid_60m = _mid_at_or_before(history, pub_ts - 60 * 60)
        drift_15m = sign * (mid_at_pub - mid_15m) if mid_15m is not None else None
        drift_60m = sign * (mid_at_pub - mid_60m) if mid_60m is not None else None
        return (drift_15m, drift_60m)

    async def _fetch_unfilled_rows(self) -> list[dict]:
        cur = await self._db.conn.execute(
            """
            SELECT id, trade_id, market_id, token_id, direction, t0_iso,
                   entry_mid,
                   price_t60s, price_t300s, price_t900s, price_t1800s,
                   price_t3600s, price_t7200s, price_t14400s, price_t28800s,
                   price_t86400s
            FROM price_impacts
            WHERE price_t60s    IS NULL
               OR price_t300s   IS NULL
               OR price_t900s   IS NULL
               OR price_t1800s  IS NULL
               OR price_t3600s  IS NULL
               OR price_t7200s  IS NULL
               OR price_t14400s IS NULL
               OR price_t28800s IS NULL
               OR price_t86400s IS NULL
            """
        )
        rows = await cur.fetchall()
        await cur.close()
        return [dict(r) for r in rows]


if __name__ == "__main__":
    # Per modularization rule 8: every stage runs standalone for dev poking.
    # No real CLOB available here; the stage simply raises on instantiation
    # without one, which is the contract.
    import sys

    print(
        "ImpactTrackerStage requires a CLOBClient; instantiate via app.main wiring.",
        file=sys.stderr,
    )
