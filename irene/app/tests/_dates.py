"""Relative date helpers for market fixtures.

A fixed `end_date` literal silently changes meaning as wall-clock time passes,
because two gates read it against `datetime.now()`:

  * `_gate_market_state` rejects with `market_near_expiry` when the market is
    within `cfg.min_hours_to_expiry` (6h) of closing.
  * `market_news_imbalance` takes its far-future branch when the market is
    more than `cfg.far_future_days` (180) out and freshness != BREAKING.

The fixtures used to pin `'2026-12-31'`. Written in May 2026 that sat *beyond*
the 180-day threshold; by 2026-07-04 it had drifted inside it, and on
2026-12-30 it would have fallen inside the 6-hour buffer and started tripping
`market_near_expiry` in 71 tests across 7 modules. Nothing about those tests
changed — only the date did.

Compute fixture dates relative to now so the intended relationship to each
threshold holds at any wall-clock time.

Not named `test_*`, so pytest does not collect it.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

# Mirrors the production defaults these helpers are positioned against; see
# `app/config.py` (`far_future_days`, `min_hours_to_expiry`).
_FAR_FUTURE_DAYS = 180
_MIN_HOURS_TO_EXPIRY = 6.0


def iso_days_from_now(days: float) -> str:
    """ISO-8601 UTC timestamp `days` from now, `Z`-suffixed as Polymarket emits."""
    dt = datetime.now(timezone.utc) + timedelta(days=days)
    return dt.isoformat().replace("+00:00", "Z")


def open_end_date() -> str:
    """A market comfortably open: neither near-expiry nor far-future.

    90 days sits an order of magnitude past the 6h `min_hours_to_expiry`
    buffer and half the 180-day `far_future_days` threshold, so neither gate
    engages and neither boundary is near enough for clock drift to matter.
    """
    return iso_days_from_now(90)


def far_future_end_date() -> str:
    """A market far enough out to trip the far-future branch.

    365 days is comfortably past `far_future_days` (180). Replaces a pinned
    `'2030-12-31'`, which was the same bug with a longer fuse — it would have
    stopped being far-future in mid-2030.
    """
    return iso_days_from_now(365)
