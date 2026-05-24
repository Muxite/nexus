"""Heavy-query cache: refreshes five aggregate SELECTs every 5 min on a
read-only SeulgiDB clone, so the 30-s render tick never blocks on DB I/O.

Cycle-2 B-WRITES fix: queries run on `_db_ro` so SELECTs never serialise
with the writer thread.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)


async def heavy_cache_loop(dash) -> None:
    """Refresh dash._heavy_cache every dash._heavy_cache_interval seconds.

    First refresh fires immediately so real data lands within seconds of
    boot. Per-query failures isolated.
    """
    while True:
        try:
            await refresh_heavy_cache(dash)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.warning("[dash-cache] refresh loop error: %s", exc)
        try:
            await asyncio.sleep(dash._heavy_cache_interval)
        except asyncio.CancelledError:
            raise


async def refresh_heavy_cache(dash) -> None:
    """Run each heavy query and update its cache slot in-place.

    Phase 2: prefers `dash._db_ro` so SELECTs never serialise with the
    writer. Per-query try/except + 30s wait_for; 3 consecutive failures
    log at ERROR and tag the tile with "cache unavailable".
    """
    db = dash._db_ro or dash._db
    if db is None:
        return
    one_hour_ago = (
        datetime.now(timezone.utc) - timedelta(hours=1)
    ).isoformat()
    refreshers = [
        ("variant_stats",
         lambda: db.get_variant_stats(since_ts=one_hour_ago)),
        ("decision_funnel",
         lambda: db.get_decision_funnel(since_secs=3600)),
        ("confidence_buckets",
         lambda: db.get_confidence_buckets(since_secs=86400)),
        ("freshness_counts",
         lambda: db.get_freshness_counts(since_secs=3600)),
        ("freshness_buckets",
         lambda: db.get_freshness_buckets(since_secs=86400)),
    ]
    for key, make_coro in refreshers:
        try:
            dash._heavy_cache[key] = await asyncio.wait_for(
                make_coro(), timeout=30,
            )
            dash._heavy_cache_fail_streak[key] = 0
        except Exception as exc:  # noqa: BLE001
            streak = dash._heavy_cache_fail_streak.get(key, 0) + 1
            dash._heavy_cache_fail_streak[key] = streak
            level = logger.error if streak >= 3 else logger.warning
            level(
                "[dash-cache] %s refresh failed (streak=%d): %s",
                key, streak, exc,
            )
    try:
        dash._heavy_cache["scout_funnel"] = await asyncio.wait_for(
            compute_scout_funnel(dash, db_override=db), timeout=30,
        )
        dash._heavy_cache_fail_streak["scout_funnel"] = 0
    except Exception as exc:  # noqa: BLE001
        streak = dash._heavy_cache_fail_streak.get("scout_funnel", 0) + 1
        dash._heavy_cache_fail_streak["scout_funnel"] = streak
        level = logger.error if streak >= 3 else logger.warning
        level(
            "[dash-cache] scout_funnel refresh failed (streak=%d): %s",
            streak, exc,
        )
    dash._heavy_cache["_last_refresh_at"] = datetime.now(timezone.utc)


async def compute_scout_funnel(dash, db_override=None) -> Optional[dict]:
    """Fetch scout's trader_trace rows and tabulate the decision funnel.

    Cycle-scoped (falls back to 1h window when cycle start is unknown).
    The pure `compute_funnel` core lives in `app/scout_funnel_core.py`.
    Returns {stats, cutoff, since_hours} or None on DB failure.
    """
    db = db_override or dash._db
    if db is None:
        return None
    from scout_funnel_core import compute_funnel
    now = datetime.now(timezone.utc)
    if dash._cycle_started_at:
        cutoff = dash._cycle_started_at
        try:
            cutoff_dt = datetime.fromisoformat(
                cutoff.replace("Z", "+00:00")
            )
            since_hours = max((now - cutoff_dt).total_seconds() / 3600.0, 0.0)
        except Exception:  # noqa: BLE001
            since_hours = 0.0
    else:
        cutoff = (now - timedelta(hours=1)).isoformat()
        since_hours = 1.0
    try:
        rows = await db.get_scout_trader_traces(
            engine_prefix="scout", since_ts=cutoff,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("[dash-cache] scout_funnel query failed: %s", exc)
        return None
    stats = compute_funnel(rows)
    return {
        "stats": stats,
        "cutoff": cutoff,
        "since_hours": since_hours,
    }
