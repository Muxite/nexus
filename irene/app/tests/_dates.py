"""Relative date helpers for market fixtures.

A fixed `end_date` literal silently changes meaning as wall-clock time passes,
because four production paths read it against `datetime.now()`:

  * `app/stages/_gates.py` `_gate_market_state` — rejects with
    `market_near_expiry` inside `cfg.min_hours_to_expiry` (6h) of close.
  * `app/stages/trade.py` `_days_to` — feeds the far-future branch of
    `market_news_imbalance` beyond `cfg.far_future_days` (180).
  * `app/stages/explorer.py` `_hours_until`.
  * `app/gates/market_open.py` `MarketOpenGate`.

The fixtures used to pin `'2026-12-31'`. Written in May 2026 that sat *beyond*
the 180-day threshold; by 2026-07-04 it had drifted inside it, and on
2026-12-30 it would have fallen inside the 6-hour buffer. Nothing about those
tests changed — only the date did. Measured blast radius at that point: 71
tests across 7 modules under `app/tests`, plus 9 more under
`scripts/replay/tests` (which `pytest.ini` does not collect by default).

Compute fixture dates relative to now so the intended relationship to each
threshold holds at any wall-clock time.

Format note: these helpers emit `Z`-suffixed UTC. The fixtures they replaced
used three different shapes — bare `'2026-12-31'` (naive, date-only),
`'...T00:00:00+00:00'`, and `'...T23:59:59Z'`. Normalizing to one shape drops
the only *integration-level* exercise of the naive branch, where
`_gate_market_state` (naive→UTC) and `_parse_end_date_string` (naive→ET)
disagree by 5 hours. That branch keeps unit coverage in
`app/tests/test_deadline_parsing.py`; do not rely on fixtures for it.

Not named `test_*`, so pytest does not collect this module.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

# Mirror the production defaults in `app/config.py` that these offsets are
# positioned against, so the offsets move if the thresholds ever do.
_FAR_FUTURE_DAYS = 180
_MIN_HOURS_TO_EXPIRY = 6.0

# Half the far-future threshold: an order of magnitude clear of the near-expiry
# buffer, and only half way to the far-future one, so neither boundary is close
# enough for clock drift to matter.
_OPEN_DAYS = _FAR_FUTURE_DAYS / 2
# Double it, so the far-future branch engages with the same margin.
_FAR_DAYS = _FAR_FUTURE_DAYS * 2

# Executable statement of the invariant both offsets exist to satisfy. If a
# threshold above is edited to something these no longer straddle, this fails
# at import rather than as a confusing gate assertion mid-suite.
assert _OPEN_DAYS * 24 > _MIN_HOURS_TO_EXPIRY, "open offset must clear near-expiry"
assert _OPEN_DAYS < _FAR_FUTURE_DAYS < _FAR_DAYS, "offsets must straddle far-future"


def iso_days_from_now(days: float) -> str:
    """ISO-8601 UTC timestamp `days` from now, `Z`-suffixed as Polymarket emits."""
    dt = datetime.now(timezone.utc) + timedelta(days=days)
    return dt.isoformat().replace("+00:00", "Z")


def open_end_date() -> str:
    """A market comfortably open: neither near-expiry nor far-future."""
    return iso_days_from_now(_OPEN_DAYS)


def far_future_end_date() -> str:
    """A market far enough out to trip the far-future branch.

    Replaces a pinned `'2030-12-31'` — the same bug with a longer fuse, since
    it would have stopped being far-future in mid-2030.
    """
    return iso_days_from_now(_FAR_DAYS)
