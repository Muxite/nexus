"""
SEULGI metrics — async-safe counters and timers, flushed to SQLite.

Usage
─────
    from metrics import incr, timer, start_flush_task

    # in main.py after db.connect():
    asyncio.create_task(start_flush_task(db, interval_secs=60))

    # at any hot path:
    incr("ai.classify.call", n=1, tags={"model": "gemini-2.5-flash"})

    async with timer("trader.evaluate", tags={"engine": "scout"}):
        await trader.evaluate(...)

Storage
───────
Each (name, tags) keeps a rolling 1-minute reservoir in memory.
Every `interval_secs` a background task moves closed buckets into
the `metrics_minutely` table:

    bucket_ts   ISO minute boundary
    name        metric name
    tags        JSON-encoded tag dict
    count       observations
    sum         summed value (timers only)
    min, max    extremes
    p50,p95,p99 percentiles from reservoir sampling (default cap 100)

Read with `python3 seulgi/scripts/analysis/perf_timeline.py
--name X --since-hours N` or join `metrics_minutely` in SQL.

This is intentionally cheap. No external service, no Prometheus, no
StatsD. Just a thin local layer that gives us p99 latency over time.
"""
from __future__ import annotations

import asyncio
import bisect
import json
import logging
import random
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# ── Reservoir / bucket store ────────────────────────────────────────────────

# In-memory state: dict[bucket_minute][(name, tags_json)] = _Bucket
_buckets: "defaultdict[str, dict[tuple[str, str], _Bucket]]" = defaultdict(dict)
_lock = asyncio.Lock()

# Reservoir cap per bucket — 100 keeps memory bounded and gives reasonable
# p99 fidelity for the volumes we generate (~1k events/minute/site).
_RESERVOIR_CAP = 100


class _Bucket:
    __slots__ = ("count", "sum", "min", "max", "samples")

    def __init__(self) -> None:
        self.count: int = 0
        self.sum: float = 0.0
        self.min: float = float("inf")
        self.max: float = float("-inf")
        # Reservoir sample of values (for percentiles).
        self.samples: list[float] = []

    def observe(self, value: float) -> None:
        self.count += 1
        self.sum += value
        if value < self.min:
            self.min = value
        if value > self.max:
            self.max = value
        if len(self.samples) < _RESERVOIR_CAP:
            self.samples.append(value)
        else:
            # Algorithm R reservoir sampling.
            idx = random.randint(0, self.count - 1)
            if idx < _RESERVOIR_CAP:
                self.samples[idx] = value

    def percentile(self, p: float) -> float:
        """p in [0, 1]; e.g., 0.99 for p99. Returns 0 if no samples."""
        if not self.samples:
            return 0.0
        sorted_s = sorted(self.samples)
        k = int(p * (len(sorted_s) - 1))
        return sorted_s[k]


def _now_bucket() -> str:
    """ISO timestamp at the current minute boundary, UTC."""
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:00+00:00")


def _tags_to_key(tags: Optional[dict]) -> str:
    """Sorted JSON for stable dict-key. Empty string when no tags."""
    if not tags:
        return ""
    return json.dumps({k: tags[k] for k in sorted(tags)}, separators=(",", ":"))


# ── Public API ───────────────────────────────────────────────────────────────

def incr(name: str, n: int = 1, tags: Optional[dict] = None) -> None:
    """Counter — record N events. Cheap; no IO."""
    bucket = _now_bucket()
    tags_key = _tags_to_key(tags)
    key = (name, tags_key)
    bkt = _buckets[bucket].setdefault(key, _Bucket())
    bkt.count += n  # bypass observe(): counter has no value
    # No sum/min/max for counters (treat as count-only).


def observe(name: str, value: float, tags: Optional[dict] = None) -> None:
    """Record a single observation (e.g., a latency, a token count)."""
    bucket = _now_bucket()
    tags_key = _tags_to_key(tags)
    key = (name, tags_key)
    bkt = _buckets[bucket].setdefault(key, _Bucket())
    bkt.observe(value)


@asynccontextmanager
async def timer(name: str, tags: Optional[dict] = None):
    """Context manager: records wall-clock ms between enter and exit."""
    t0 = time.monotonic()
    try:
        yield
    finally:
        ms = (time.monotonic() - t0) * 1000.0
        observe(name, ms, tags=tags)


# ── Flush ────────────────────────────────────────────────────────────────────

async def _flush_one(db, bucket_ts: str, items: dict) -> int:
    """Write all (name, tags) entries for one closed bucket. Returns rows written."""
    n_written = 0
    for (name, tags_json), bkt in items.items():
        if bkt.count == 0:
            continue
        try:
            await db._conn.execute(
                """
                INSERT INTO metrics_minutely
                    (bucket_ts, name, tags, count, sum, min, max, p50, p95, p99)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(bucket_ts, name, tags) DO UPDATE SET
                    count = count + excluded.count,
                    sum   = sum   + excluded.sum,
                    min   = MIN(min, excluded.min),
                    max   = MAX(max, excluded.max),
                    p50   = excluded.p50,
                    p95   = excluded.p95,
                    p99   = excluded.p99
                """,
                (
                    bucket_ts, name, tags_json,
                    bkt.count, bkt.sum,
                    bkt.min if bkt.min != float("inf") else None,
                    bkt.max if bkt.max != float("-inf") else None,
                    bkt.percentile(0.50),
                    bkt.percentile(0.95),
                    bkt.percentile(0.99),
                ),
            )
            n_written += 1
        except Exception as exc:  # noqa: BLE001
            logger.warning("[metrics] insert failed for %s: %s", name, exc)
    # Phase C (2026-05-21): caller wraps the flush in db.batch() so
    # multiple bucket flushes commit as ONE transaction. Inside a
    # batch, _commit_if_unbatched is a no-op. Outside it commits
    # normally so direct callers still behave as before.
    await db._commit_if_unbatched()
    return n_written


async def start_flush_task(db, interval_secs: float = 60.0) -> None:
    """Background task: every interval_secs, persist closed buckets to DB.

    A bucket is "closed" if its timestamp is strictly less than the
    current minute (i.e., no more observations will land in it).
    """
    while True:
        try:
            await asyncio.sleep(interval_secs)
            current = _now_bucket()
            async with _lock:
                closed = {ts: items for ts, items in _buckets.items() if ts < current}
                for ts in closed:
                    del _buckets[ts]
            total = 0
            if closed:
                # One BEGIN/COMMIT for the whole flush tick — all bucket
                # rows commit together so the writer takes one fsync,
                # not one per bucket.
                async with db.batch():
                    for ts, items in closed.items():
                        total += await _flush_one(db, ts, items)
            if total:
                logger.info("[metrics] flushed %d metric rows from %d bucket(s)",
                            total, len(closed))
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.warning("[metrics] flush task error: %s", exc)


def snapshot_recent(window_minutes: int = 5) -> list[dict]:
    """In-memory snapshot of the last N minutes for the dashboard tile.

    Returns a list of {name, tags, count, sum, p50, p95, p99}.
    Does NOT hit the DB — pure in-memory aggregation.
    """
    out: dict[tuple[str, str], _Bucket] = {}
    now_bkt = _now_bucket()
    for ts, items in _buckets.items():
        if ts > now_bkt:
            continue
        for key, bkt in items.items():
            agg = out.setdefault(key, _Bucket())
            agg.count += bkt.count
            agg.sum += bkt.sum
            if bkt.min < agg.min:
                agg.min = bkt.min
            if bkt.max > agg.max:
                agg.max = bkt.max
            agg.samples.extend(bkt.samples[:_RESERVOIR_CAP])
            agg.samples = agg.samples[:_RESERVOIR_CAP]
    rows: list[dict] = []
    for (name, tags_json), bkt in out.items():
        rows.append({
            "name": name,
            "tags": tags_json,
            "count": bkt.count,
            "sum": bkt.sum,
            "min": bkt.min if bkt.min != float("inf") else 0.0,
            "max": bkt.max if bkt.max != float("-inf") else 0.0,
            "p50": bkt.percentile(0.50),
            "p95": bkt.percentile(0.95),
            "p99": bkt.percentile(0.99),
        })
    rows.sort(key=lambda r: (-r["count"], r["name"]))
    return rows
