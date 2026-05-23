"""
Price-impact tracker.

For every significant headline that matches a Polymarket market, record
the market token price at the moment of detection, then poll the price at
+1m, +5m, +15m, +30m, +60m. The resulting dataset lets us test the core
hypothesis:

    "Does critical news move Polymarket odds in a predictable direction
     within minutes, and can we profit by acting before the market adjusts?"

## Specification

Purpose
    Background diagnostic monitor that anchors a price-impact row to
    every *significant headline/market match* (NOT every trade), then
    polls CLOB midpoints at +1m/+5m/+15m/+30m/+60m to fill
    `price_impacts.price_t{bucket}` and derived `delta_t{bucket}`
    columns. Also backfills full minute-fidelity (-1h..+1h) windows
    into `price_timeseries` for offline analysis. Surfaces the realized
    direction-vs-prediction signal that the post-mortem and analysis
    scripts consume.

Public interface
    class PriceImpactTracker(db, dashboard_dir)
      - await start()       # spawns _poll_loop / _dashboard_loop /
                            #          _timeseries_backfill_loop
      - await record(...)   # called fire-and-forget by engine.py for
                            #          each survivor decision
      - await close()
    Module helpers exported to other modules:
      - _fetch_price_history_window (used by market_context.py)
      - classify_missed_alpha       (used by scripts/analysis, scripts/postmortem)
      - condition_id_short, MISSED_ALPHA_THRESH

Invariants
    - One row per (headline_id × condition_id × direction) match
      created in `record()`. Bucket columns are upserted in place by
      `update_price_impact_snapshot`; never duplicated.
    - Anchor is headline-detection time, NOT trade-open time. A row
      exists for matches that never executed a trade.
    - Significance gate: ai_confidence ≥ MIN_CONFIDENCE (0.50) AND
      similarity ≥ MIN_SIMILARITY (0.55). Sub-threshold matches are
      silently dropped.
    - `timeseries_backfilled_at` is monotone: a row is backfilled at
      most once even when CLOB returns empty.

Side effects
    - HTTP GET to clob.polymarket.com (/midpoint, /prices-history,
      /book — the last added in Phase 4A.3 for BBO + depth capture).
    - DB writes: INSERT price_impacts, UPDATE price_impacts (bucket
      cols + timeseries flag), batch INSERT price_timeseries.
    - DB reads: get_pending_price_impacts, get_impacts_needing_timeseries,
      get_price_impact_stats.
    - File write: `<dashboard_dir>/price_impact.txt` every 60s.

Failure modes
    - HTTP timeout / non-200 → price recorded as None; row stays
      "pending" until the next poll cycle.
    - History fetch empty → row marked backfilled to avoid retry loop.
    - Market closes before +60m → late buckets stay NULL; pending set
      drops the row once price_t60m is filled (or never on dead tokens).
    - aiohttp session loss → recreated on next `_get_session()`.

Non-goals
    - NOT trade execution (trader.py) or live position monitoring
      (trader.monitor_positions @ 60s via main._position_monitor_loop).
    - NOT spread/depth gating (Phase 4D.1 will consume the
      bid/ask/book_depth_usdc captured here). 4A.3 only populates
      the columns; it does NOT filter on them.
    - NOT on the critical path of `trader.evaluate`; all writes are
      fire-and-forget via `asyncio.create_task` from engine.py.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiohttp

from db import SeulgiDB
# A4A.3: BBO + book-depth capture lives in `bbo.py`. Re-exported here
# so existing call sites (`impact_tracker.fetch_bbo`, `BBOResult`) and
# tests that monkeypatch `impact_tracker._fetch_bbo` keep working.
# `_bbo_cache` / `_bbo_inflight` are re-exported so test setup can
# reset state via the impact_tracker namespace too.
from bbo import (  # noqa: F401 — public re-export surface
    BBOResult, fetch_bbo,
    _bbo_cache, _bbo_inflight,
)

logger = logging.getLogger(__name__)

_CLOB_PRICE_URL = "https://clob.polymarket.com/midpoint?token_id={token_id}"
_CLOB_HISTORY_URL = (
    "https://clob.polymarket.com/prices-history"
    "?market={token_id}&startTs={start_ts}&endTs={end_ts}&fidelity=1"
)

# Minimum thresholds to bother tracking a match
MIN_CONFIDENCE = 0.50
MIN_SIMILARITY  = 0.55

# Polling schedule: (bucket_name, seconds_after_detection)
_BUCKETS = [
    ("t1m",  60),
    ("t5m",  300),
    ("t15m", 900),
    ("t30m", 1800),
    ("t60m", 3600),
]

_POLL_INTERVAL = 30   # check pending rows every 30 s

# A2.2 (2026-05-22): trade-anchored continuous price tape. Distinct
# from `_BUCKETS` above (which is shared with the headline-anchored
# price_impacts row and must NOT grow — adding +4h would add a bogus
# +4h column to every legacy row). Each tuple is
# (t_minutes_since_entry, seconds_after_open) — minutes is the
# canonical column label written into position_price_snaps; seconds is
# used by the loop to decide "is this offset due yet?". Order matters:
# the loop fills shortest-first, so a late-starting position writes
# 5m before 15m, etc. Coalesced per-token fetch (see _coalesce_token)
# de-dupes when two positions share the same condition's token.
_POSITION_BUCKETS: list[tuple[int, int]] = [
    (5,   300),
    (15,  900),
    (30,  1800),
    (60,  3600),
    (240, 14400),
]
# Slowest checkpoint defines the loop's retention horizon: a position
# stops being "due" for new snaps `_MAX_OFFSET_S` after open.
_MAX_OFFSET_S = _POSITION_BUCKETS[-1][1]
# Tick cadence for `_position_snap_loop`. 60s is fine: the shortest
# offset (5m) is 300s, so a once-per-minute sweep can never miss a
# checkpoint by more than ~60s (well below the headline-impact 30s
# poll's noise floor). The poll is still cheap because per-token
# fetches are coalesced and capped by the semaphore below.
_POSITION_SNAP_POLL_INTERVAL = 60
# Bound concurrent CLOB calls when multiple positions become due in
# the same sweep (e.g. after a long write-lock stall or a startup
# reload of many open positions). 10 mirrors the master-plan §A2.2
# recommendation in the A0.11 extension plan.
_POSITION_SNAP_CONCURRENCY = 10


async def _fetch_price(session: aiohttp.ClientSession, token_id: str) -> Optional[float]:
    url = _CLOB_PRICE_URL.format(token_id=token_id)
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            mid = data.get("mid")
            return float(mid) if mid is not None else None
    except Exception as exc:
        logger.debug("[impact] price fetch failed %s: %s", token_id[:12], exc)
        return None


async def _fetch_bbo(
    session: aiohttp.ClientSession, token_id: str
) -> tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """Compatibility shim used by `_position_snap_loop` (and tests).

    Returns (mid, bid, ask, book_depth_usdc_top5) for the legacy
    4-tuple call sites. Delegates to `bbo.fetch_bbo` so the cache +
    book parsing path runs once per token across both surfaces.
    """
    r = await fetch_bbo(session, token_id)
    return (r.mid, r.bid, r.ask, r.book_depth_usdc_top5)


async def _fetch_pre_move(
    session: aiohttp.ClientSession,
    token_id: str,
    detected_unix: int,
) -> tuple[Optional[float], Optional[float]]:
    """
    Return (price_pre_60s, price_pre_300s): the CLOB midpoint observed roughly
    60s and 300s before headline detection. Used to distinguish "no signal"
    from "we were too slow." Returns (None, None) on error.

    Polymarket /prices-history has 1-minute fidelity, so price_pre_60s is the
    last sample at or before detected_unix - 60s.
    """
    url = _CLOB_HISTORY_URL.format(
        token_id=token_id,
        start_ts=detected_unix - 600,   # look back 10 min for a safety buffer
        end_ts=detected_unix,
    )
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            if resp.status != 200:
                return (None, None)
            data = await resp.json()
            history = data.get("history") or []
    except Exception as exc:
        logger.debug("[impact] history fetch failed %s: %s", token_id[:12], exc)
        return (None, None)

    def _at_or_before(target_ts: int) -> Optional[float]:
        best: Optional[float] = None
        for entry in history:
            t = entry.get("t")
            if t is None:
                continue
            if t <= target_ts:
                best = entry.get("p")
            else:
                break
        return float(best) if best is not None else None

    return (_at_or_before(detected_unix - 60), _at_or_before(detected_unix - 300))


async def _fetch_price_history_window(
    session: aiohttp.ClientSession,
    token_id: str,
    start_ts: int,
    end_ts: int,
) -> list[dict]:
    """
    Fetch the full price history between two unix timestamps (1m fidelity).
    Returns the raw [{t, p}, ...] list — empty on any error.
    """
    url = _CLOB_HISTORY_URL.format(
        token_id=token_id, start_ts=start_ts, end_ts=end_ts,
    )
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()
            return data.get("history") or []
    except Exception as exc:
        logger.debug("[impact] window fetch failed %s: %s", token_id[:12], exc)
        return []


class PriceImpactTracker:
    """
    Background service that:
    1. Records price_t0 for every significant headline/market match.
    2. Polls price at each time bucket and fills in the delta columns.
    3. Writes a human-readable summary to dashboard/price_impact.txt.
    """

    def __init__(
        self,
        db: SeulgiDB,
        dashboard_dir: str = "/data/dashboard",
    ) -> None:
        self._db = db
        self._dashboard = Path(dashboard_dir)
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        # In-memory pending set: {row_id: detected_at_unix}
        self._pending: dict[int, float] = {}

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        self._running = False
        if self._session and not self._session.closed:
            await self._session.close()

    # ------------------------------------------------------------------
    # Public: record a new impact row
    # ------------------------------------------------------------------

    async def record(
        self,
        headline_id: str,
        headline: str,
        site_url: str,
        condition_id: str,
        question: str,
        token_id: str,
        direction: str,
        ai_confidence: float,
        similarity: float,
        source: str = "news",
    ) -> None:
        """
        Fetch the current token price and create a new price_impacts row.

        Only records if confidence ≥ MIN_CONFIDENCE and similarity ≥ MIN_SIMILARITY.
        """
        if ai_confidence < MIN_CONFIDENCE or similarity < MIN_SIMILARITY:
            return

        session = self._get_session()
        detected_unix = int(time.time())
        price_t0, (price_pre_60s, price_pre_300s) = await asyncio.gather(
            _fetch_price(session, token_id),
            _fetch_pre_move(session, token_id, detected_unix),
        )
        detected_at = datetime.now(timezone.utc).isoformat()

        row_id = await self._db.insert_price_impact(
            headline_id=headline_id,
            headline=headline,
            site_url=site_url,
            condition_id=condition_id,
            question=question,
            token_id=token_id,
            direction=direction,
            ai_confidence=ai_confidence,
            similarity=similarity,
            price_t0=price_t0,
            detected_at=detected_at,
            source=source,
            price_pre_60s=price_pre_60s,
            price_pre_300s=price_pre_300s,
        )

        self._pending[row_id] = time.time()
        # Pre-move sign vs predicted direction: + = market already moved our way
        # (we were probably too slow), - = market moved the opposite way.
        pre_move = (
            (price_t0 - price_pre_60s)
            if (price_t0 is not None and price_pre_60s is not None) else None
        )
        logger.info(
            "[impact] [hid=%s] tracking %s  dir=%s  conf=%.2f  sim=%.2f  "
            "p0=%.3f  pre60s=%s  pre_move=%s  q=%s",
            headline_id, condition_id[:12], direction,
            ai_confidence, similarity,
            price_t0 or -1,
            "%.3f" % price_pre_60s if price_pre_60s is not None else "n/a",
            "%+.4f" % pre_move if pre_move is not None else "n/a",
            question[:60],
        )

    # ------------------------------------------------------------------
    # Background loop
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the background polling loop (call once, runs until close())."""
        self._running = True
        # Reload any pending rows from DB (survive restarts)
        pending_rows = await self._db.get_pending_price_impacts()
        for row in pending_rows:
            detected_dt = datetime.fromisoformat(row["detected_at"])
            if detected_dt.tzinfo is None:
                detected_dt = detected_dt.replace(tzinfo=timezone.utc)
            age = (datetime.now(timezone.utc) - detected_dt).total_seconds()
            if age < 3600:
                self._pending[row["id"]] = time.time() - age
        if self._pending:
            logger.info("[impact] resumed %d pending impact rows", len(self._pending))

        asyncio.create_task(self._poll_loop())
        asyncio.create_task(self._dashboard_loop())
        asyncio.create_task(self._timeseries_backfill_loop())
        # A2.2: trade-anchored continuous price tape. Sibling loop so
        # the headline-anchored `_poll_loop` doesn't get burdened with
        # the +4h horizon (its rows close at +60m). Crash recovery is
        # implicit: the open-positions table is the source of truth,
        # and INSERT OR IGNORE on (trade_id, t_minutes_since_entry)
        # means we never double-write a checkpoint on resume.
        asyncio.create_task(self._position_snap_loop())

    async def _poll_loop(self) -> None:
        """Periodically fill in time-bucket price snapshots."""
        while self._running:
            await asyncio.sleep(_POLL_INTERVAL)
            if not self._pending:
                continue

            # Load fresh rows from DB
            pending_rows = await self._db.get_pending_price_impacts()
            if not pending_rows:
                self._pending.clear()
                continue

            session = self._get_session()
            now = time.time()

            for row in pending_rows:
                row_id = row["id"]
                detected_dt = datetime.fromisoformat(row["detected_at"])
                if detected_dt.tzinfo is None:
                    detected_dt = detected_dt.replace(tzinfo=timezone.utc)
                age_s = (datetime.now(timezone.utc) - detected_dt).total_seconds()
                token_id = row["token_id"]
                if not token_id:
                    continue

                for bucket, offset_s in _BUCKETS:
                    col = f"price_{bucket}"
                    if row.get(col) is not None:
                        continue           # already filled
                    if age_s >= offset_s:  # time to fill this bucket
                        price = await _fetch_price(session, token_id)
                        if price is not None:
                            await self._db.update_price_impact_snapshot(row_id, bucket, price)
                            p0 = row.get("price_t0") or 0.0
                            delta = price - p0
                            logger.info(
                                "[impact] [hid=%s] %s bucket=%s  price=%.3f  "
                                "delta=%+.3f  q=%s",
                                row["headline_id"], condition_id_short(row["condition_id"]),
                                bucket, price, delta, row["question"][:50],
                            )

            # Remove rows whose 60m snapshot is filled
            done_ids = {r["id"] for r in pending_rows if r.get("price_t60m") is not None}
            for rid in done_ids:
                self._pending.pop(rid, None)

    # ------------------------------------------------------------------
    # A2.2: Trade-anchored continuous price tape (`position_price_snaps`)
    # ------------------------------------------------------------------

    async def _position_snap_loop(self) -> None:
        """Periodically write `position_price_snaps` for every open position.

        Tick cadence: `_POSITION_SNAP_POLL_INTERVAL` (60s). On each tick:
          1. Load all open positions from DB (source of truth — survives
             restarts; no in-memory pending set needed).
          2. For each position × bucket in `_POSITION_BUCKETS`, decide
             whether the offset is due (age >= offset_s).
          3. Coalesce per-token CLOB fetches so two positions sharing
             one condition_id's token only fetch once per sweep.
          4. INSERT OR IGNORE — the UNIQUE(trade_id,
             t_minutes_since_entry) constraint makes the write
             idempotent. On crash mid-sweep, the next tick picks up
             unfilled checkpoints automatically.

        Fetches are bounded by `_POSITION_SNAP_CONCURRENCY` so a sweep
        with many newly-due positions can't blow up the CLOB rate
        limit. The position_price_snaps rows survive position close —
        Phase-5 replay reads them by trade_id.
        """
        # Brief warm-up so the rest of `start()` can finish wiring.
        await asyncio.sleep(5)
        while self._running:
            try:
                await self._run_position_snap_sweep()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.debug("[impact] position-snap sweep error: %s", exc)
            await asyncio.sleep(_POSITION_SNAP_POLL_INTERVAL)

    async def _run_position_snap_sweep(self) -> None:
        """One pass of the position-snap loop.

        Split out so tests can drive a deterministic single sweep
        without spinning the outer `while self._running` loop. The
        loop body intentionally does no DB writes when no checkpoint
        is due — the only DB read per sweep is `get_open_positions()`.
        """
        positions = await self._db.get_open_positions()
        if not positions:
            return

        now = time.time()
        # (trade_id, token_id, side, t_minutes_since_entry, t_unix, age_s)
        due: list[tuple[int, str, str, int, int, float]] = []
        for pos in positions:
            token_id = pos.get("token_id")
            if not token_id:
                continue
            trade_id = pos.get("trade_id")
            if trade_id is None:
                continue
            opened_at = pos.get("opened_at")
            if not opened_at:
                continue
            try:
                opened_dt = datetime.fromisoformat(opened_at)
                if opened_dt.tzinfo is None:
                    opened_dt = opened_dt.replace(tzinfo=timezone.utc)
                opened_unix = opened_dt.timestamp()
            except Exception:  # noqa: BLE001
                continue
            age_s = now - opened_unix
            # Skip positions that are too old to have any new checkpoint
            # left to fill (cuts the per-sweep work as the open book ages).
            if age_s > _MAX_OFFSET_S + 60:
                continue
            # Side label matches the trade's direction so analysis joins
            # don't have to look up trades.direction separately.
            side = (pos.get("direction") or "").upper()
            for t_min, offset_s in _POSITION_BUCKETS:
                if age_s >= offset_s:
                    # `t_unix` is the moment the checkpoint comes due,
                    # NOT now() — keeps the column aligned across sweeps
                    # so a late write still reflects the intended offset.
                    snap_unix = int(opened_unix + offset_s)
                    due.append((trade_id, token_id, side, t_min, snap_unix, age_s))

        if not due:
            return

        # Coalesce per-token fetches: many positions can share the same
        # token (e.g. duplicate engines on the same market) — pay the
        # CLOB call once and share the price across all consumers.
        session = self._get_session()
        sem = asyncio.Semaphore(_POSITION_SNAP_CONCURRENCY)
        token_tasks: dict[str, asyncio.Task] = {}

        async def _fetch_one(tok: str):
            async with sem:
                return await _fetch_bbo(session, tok)

        for _, token_id, _, _, _, _ in due:
            if token_id not in token_tasks:
                token_tasks[token_id] = asyncio.create_task(_fetch_one(token_id))

        # Await fetches; each token's result is shared across due rows.
        results: dict[str, tuple] = {}
        for token_id, task in token_tasks.items():
            try:
                results[token_id] = await task
            except Exception as exc:  # noqa: BLE001
                logger.debug("[impact] bbo fetch failed %s: %s",
                             token_id[:12], exc)
                results[token_id] = (None, None, None, None)

        # Write rows. INSERT OR IGNORE keeps repeated checkpoints safe;
        # we still bump the writer counter on every call so observability
        # surfaces "loop ran" even on idempotent no-ops.
        for trade_id, token_id, side, t_min, snap_unix, _ in due:
            mid, bid, ask, depth = results.get(
                token_id, (None, None, None, None)
            )
            if mid is None:
                # No price → no row. The next sweep retries.
                continue
            try:
                await self._db.insert_position_price_snap(
                    trade_id=trade_id,
                    token_id=token_id,
                    side=side,
                    t_unix=snap_unix,
                    t_minutes_since_entry=t_min,
                    price=float(mid),
                    bid=bid,
                    ask=ask,
                    book_depth_usdc=depth,
                    source="monitor",
                )
            except Exception as exc:  # noqa: BLE001
                logger.debug(
                    "[impact] position snap write failed trade=%s t_min=%d: %s",
                    trade_id, t_min, exc,
                )

    async def _dashboard_loop(self) -> None:
        """Write price_impact.txt every 60 s."""
        while self._running:
            await asyncio.sleep(60)
            await self._write_dashboard()

    # ------------------------------------------------------------------
    # Full time-series backfill (-1h..+1h around each impact, 1m fidelity)
    # ------------------------------------------------------------------

    async def _timeseries_backfill_loop(self) -> None:
        """
        Every 5 minutes, find impact rows whose detection window has fully
        elapsed (≥ 65 minutes ago) and which have not yet been backfilled.
        For each, fetch the full -1h..+1h CLOB history in ONE call and store
        every minute sample in price_timeseries. Survives restarts because
        state lives in DB (timeseries_backfilled_at column).
        """
        # Small initial delay so the bot can warm up first.
        await asyncio.sleep(30)
        while self._running:
            try:
                rows = await self._db.get_impacts_needing_timeseries(
                    min_age_seconds=3900,  # 65 min
                    limit=50,
                )
                if rows:
                    session = self._get_session()
                    # Run all backfills concurrently — they hit different tokens.
                    await asyncio.gather(
                        *(self._backfill_one(session, r) for r in rows),
                        return_exceptions=True,
                    )
            except Exception as exc:
                logger.debug("[impact] backfill loop error: %s", exc)
            await asyncio.sleep(300)  # 5 min between sweeps

    async def _backfill_one(self, session: aiohttp.ClientSession, row: dict) -> None:
        detected_dt = datetime.fromisoformat(row["detected_at"])
        if detected_dt.tzinfo is None:
            detected_dt = detected_dt.replace(tzinfo=timezone.utc)
        detected_unix = int(detected_dt.timestamp())
        history = await _fetch_price_history_window(
            session, row["token_id"],
            start_ts=detected_unix - 3600,
            end_ts=detected_unix + 3600,
        )
        if not history:
            # Mark as backfilled anyway so we don't retry forever on dead tokens.
            await self._db.mark_timeseries_backfilled(row["id"])
            return
        batch = [
            (
                row["id"], row["headline_id"], row["condition_id"], row["token_id"],
                int(entry["t"]), float(entry["p"]), int(entry["t"]) - detected_unix,
            )
            for entry in history
            if entry.get("t") is not None and entry.get("p") is not None
        ]
        if batch:
            await self._db.insert_price_timeseries_batch(batch)
        await self._db.mark_timeseries_backfilled(row["id"])
        logger.info(
            "[impact] [hid=%s] backfilled %d samples (-1h..+1h) for %s",
            row["headline_id"], len(batch), condition_id_short(row["condition_id"]),
        )

    async def _write_dashboard(self) -> None:
        try:
            rows = await self._db.get_price_impact_stats(limit=100)
            pending = await self._db.get_pending_price_impacts()
            self._dashboard.mkdir(parents=True, exist_ok=True)
            path = self._dashboard / "price_impact.txt"
            path.write_text(_format_dashboard(rows, pending))
        except Exception as exc:
            logger.debug("[impact] dashboard write error: %s", exc)


# ------------------------------------------------------------------
# Formatting helpers
# ------------------------------------------------------------------

def condition_id_short(cid: str) -> str:
    return cid[:12] if cid else "?"


def _format_dashboard(completed: list[dict], pending: list[dict]) -> str:
    lines = [
        "──────────────────────────────────────────────────────────────────",
        f"  PRICE IMPACT TRACKER  ({len(completed)} completed, {len(pending)} pending)",
        "──────────────────────────────────────────────────────────────────",
        "",
    ]

    # Hypothesis summary from completed rows
    if completed:
        lines += _hypothesis_summary(completed)
        lines += _missed_alpha_summary(completed)
        lines += ["", "── TOP MOVES (sorted by |Δ5m|) ─────────────────────────────────────", ""]
        sorted_rows = sorted(
            completed,
            key=lambda r: abs(r.get("delta_t5m") or 0),
            reverse=True,
        )[:30]
        lines.append(
            f"  {'HEADLINE':<50} {'DIR':>4} {'CONF':>5} {'SIM':>5} "
            f"{'p0':>6} {'pre60':>7} {'Δ1m':>6} {'Δ5m':>6} {'Δ15m':>7} {'Δ60m':>7}"
        )
        lines.append("  " + "─" * 115)
        for r in sorted_rows:
            hl = (r.get("headline") or "")[:49]
            d = r.get("direction", "?")
            conf = r.get("ai_confidence") or 0.0
            sim  = r.get("similarity") or 0.0
            p0   = r.get("price_t0")
            dpre = r.get("delta_pre_60s")
            d1   = r.get("delta_t1m")
            d5   = r.get("delta_t5m")
            d15  = r.get("delta_t15m")
            d60  = r.get("delta_t60m")
            src  = r.get("source", "")[:6]

            def fmt(v):
                return f"{v:+.3f}" if v is not None else "  n/a"

            lines.append(
                f"  {hl:<50} {d:>4} {conf:>5.2f} {sim:>5.2f} "
                f"{(p0 or 0):>6.3f} {fmt(dpre):>7} {fmt(d1):>6} {fmt(d5):>6} "
                f"{fmt(d15):>7} {fmt(d60):>7}  [{src}]"
            )

    # Pending rows
    if pending:
        lines += ["", "── PENDING (awaiting price snapshots) ───────────────────────────────", ""]
        for r in pending[:20]:
            hl = (r.get("headline") or "")[:60]
            buckets_done = sum(
                1 for b in ("price_t1m", "price_t5m", "price_t15m", "price_t30m", "price_t60m")
                if r.get(b) is not None
            )
            lines.append(
                f"  [{r.get('source','news'):>10}] [{buckets_done}/5] {hl}"
            )

    lines.append("")
    return "\n".join(lines)


def _hypothesis_summary(rows: list[dict]) -> list[str]:
    """Aggregate stats: when direction=YES and delta>0, did we win?"""
    lines = ["── HYPOTHESIS TEST ──────────────────────────────────────────────────", ""]

    # For each direction, count hits (delta_t5m in predicted direction)
    hits = {"YES": 0, "NO": 0}
    total = {"YES": 0, "NO": 0}
    avg_delta = {"YES": [], "NO": []}

    for r in rows:
        d = r.get("direction", "")
        d5 = r.get("delta_t5m")
        if d5 is None or d not in ("YES", "NO"):
            continue
        total[d] += 1
        avg_delta[d].append(d5)
        # A "hit" = price moved in the predicted direction
        if (d == "YES" and d5 > 0) or (d == "NO" and d5 < 0):
            hits[d] += 1

    for d in ("YES", "NO"):
        n = total[d]
        if n == 0:
            lines.append(f"  {d}: no data")
            continue
        hit_rate = 100 * hits[d] / n
        mean_d = sum(avg_delta[d]) / n
        lines.append(
            f"  Direction={d}  n={n}  hit_rate={hit_rate:.0f}%  "
            f"mean_Δ5m={mean_d:+.4f}"
        )

    # Combined: avg |delta| at 5m for all significant matches
    all_d5 = [abs(r.get("delta_t5m") or 0) for r in rows if r.get("delta_t5m") is not None]
    if all_d5:
        lines.append(f"\n  Mean |Δ5m| = {sum(all_d5)/len(all_d5):.4f}  "
                     f"Max |Δ5m| = {max(all_d5):.4f}  n={len(all_d5)}")
        lines.append("  (>0.01 Δ5m suggests markets do react to news within 5 minutes)")

    # Per-source breakdown
    by_source: dict[str, dict] = {}
    for r in rows:
        d = r.get("direction", "")
        d5 = r.get("delta_t5m")
        src = r.get("source") or "news"
        if d5 is None or d not in ("YES", "NO"):
            continue
        s = by_source.setdefault(src, {"n": 0, "hits": 0, "abs_sum": 0.0})
        s["n"] += 1
        s["abs_sum"] += abs(d5)
        if (d == "YES" and d5 > 0) or (d == "NO" and d5 < 0):
            s["hits"] += 1
    if by_source:
        lines.append("")
        lines.append("  BY SOURCE:")
        for src, s in sorted(by_source.items(), key=lambda kv: -kv[1]["n"]):
            n = s["n"]
            rate = 100 * s["hits"] / n if n else 0
            avg_abs = s["abs_sum"] / n if n else 0.0
            lines.append(
                f"    {src:<14} n={n:>4}  hit_rate={rate:>3.0f}%  avg|Δ5m|={avg_abs:.4f}"
            )

    return lines


# Threshold used everywhere to call a price move "meaningful".
# 0.5 percentage points of probability; below this is dominated by spread noise.
MISSED_ALPHA_THRESH = 0.005


def classify_missed_alpha(direction: str, delta_pre_60s, delta_t5m) -> str | None:
    """
    Bucket a price_impacts row into one of:
      "too_slow"  -- pre-move already in our direction (latency-limited alpha)
      "captured"  -- no pre-move, post-move in our direction (real alpha caught)
      "no_signal" -- neither pre nor post moved meaningfully
      "wrong_dir" -- pre or post move against our predicted direction

    Returns None if the row lacks the data needed to classify (missing direction,
    or missing pre/post deltas). Single source of truth for analysis scripts.

    NOTE: these buckets are diagnostic — they MUST NOT be summed into PnL.
    """
    if direction not in ("YES", "NO"):
        return None
    if delta_pre_60s is None or delta_t5m is None:
        return None
    signed_pre = delta_pre_60s if direction == "YES" else -delta_pre_60s
    signed_post = delta_t5m if direction == "YES" else -delta_t5m
    if signed_pre >= MISSED_ALPHA_THRESH:
        return "too_slow"
    if signed_pre <= -MISSED_ALPHA_THRESH:
        return "wrong_dir"
    if abs(signed_post) < MISSED_ALPHA_THRESH:
        return "no_signal"
    if signed_post >= MISSED_ALPHA_THRESH:
        return "captured"
    return "wrong_dir"


def _missed_alpha_summary(rows: list[dict]) -> list[str]:
    """
    Distinguish two failure modes using the pre-detection price window:
      • too_slow   : market already moved in our predicted direction by
                     |delta_pre_60s| ≥ 0.005 before we touched it. The signal
                     was real; we just missed it. Recoverable via lower latency.
      • no_signal  : |delta_pre_60s| and |delta_t5m| both < 0.005. Headline did
                     not move the market on either side of our entry.
      • captured   : we entered before any meaningful pre-move AND post-move
                     went in our direction with |delta_t5m| ≥ 0.005.
                     This is real, attributable alpha.

    These counts MUST NOT be summed into PnL — they are diagnostic only.
    """
    THRESH = MISSED_ALPHA_THRESH
    lines = [
        "",
        "── PRE-MOVE DIAGNOSIS  (split-second post-mortem, NOT profit) ───────",
        "",
        "  Window: 60s before headline detection → 5m after.",
        "  Threshold for a 'real move': |Δ| ≥ 0.005 in our predicted direction.",
        "",
    ]

    too_slow = 0
    no_signal = 0
    captured = 0
    wrong_dir = 0    # pre-move was against us (price already moved opposite)
    measured = 0
    pre_moves_with_dir: list[float] = []  # signed by direction

    counts = {"too_slow": 0, "captured": 0, "no_signal": 0, "wrong_dir": 0}
    for r in rows:
        d = r.get("direction", "")
        dpre = r.get("delta_pre_60s")
        d5 = r.get("delta_t5m")
        bucket = classify_missed_alpha(d, dpre, d5)
        if bucket is None:
            continue
        measured += 1
        counts[bucket] += 1
        # Sign convention: positive = market moved in our predicted direction.
        signed_pre = dpre if d == "YES" else -dpre
        pre_moves_with_dir.append(signed_pre)
    too_slow = counts["too_slow"]
    captured = counts["captured"]
    no_signal = counts["no_signal"]
    wrong_dir = counts["wrong_dir"]

    if measured == 0:
        lines.append("  (no rows with pre-move data yet — wait one polling cycle)")
        return lines

    def pct(n: int) -> str:
        return f"{100 * n / measured:>4.1f}%"

    lines.append(f"  measured rows  : {measured}")
    lines.append(f"  too_slow       : {too_slow:>4}  ({pct(too_slow)})  pre-move already in our direction")
    lines.append(f"  captured       : {captured:>4}  ({pct(captured)})  no pre-move, post-move in our direction")
    lines.append(f"  no_signal      : {no_signal:>4}  ({pct(no_signal)})  market never moved")
    lines.append(f"  wrong_dir      : {wrong_dir:>4}  ({pct(wrong_dir)})  pre or post move against us")

    if pre_moves_with_dir:
        avg_signed = sum(pre_moves_with_dir) / len(pre_moves_with_dir)
        abs_vals = [abs(v) for v in pre_moves_with_dir]
        lines.append("")
        lines.append(
            f"  mean signed pre-move = {avg_signed:+.4f}   "
            f"mean |pre-move| = {sum(abs_vals)/len(abs_vals):.4f}   "
            f"max |pre-move| = {max(abs_vals):.4f}"
        )
        lines.append(
            "  Positive mean signed pre-move = AI direction agrees with pre-headline drift "
            "(latency-limited alpha)."
        )

    return lines
