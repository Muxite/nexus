"""Single source of truth for the explorer's odds-capture schedule.

Both the live explorer (``app.config`` → ``app.stages.odds_capture``) and
the offline sandbox (``irene.sandbox.pnl`` / ``run_strategy``) read these
values, so the explorer's capture horizons and the sandbox's backtest
exit horizons can never silently drift.

The two entrypoints put different roots on ``sys.path`` (the app runs with
``irene/`` on the path; the sandbox with the repo root), so the import
statement differs by caller even though both resolve to *this one file*::

    app side:     from capture_schedule import CAPTURE_POST_OFFSETS_SECS
    sandbox side: from irene.capture_schedule import CAPTURE_POST_OFFSETS_SECS

To change the capture cadence, edit here (or override
``explorer.capture_*`` in config for the live explorer only).
"""

from __future__ import annotations

# How far before t0 pre-news history samples are kept (negative offsets).
CAPTURE_PRE_SECS: int = 86_400  # 24h baseline window

# Post-news checkpoints, in seconds after t0. The explorer's dense poll
# is guaranteed to land a usable book snapshot at/after each of these, and
# they are the canonical exit horizons the sandbox backtests at.
#   +5min, +30min, +1h
CAPTURE_POST_OFFSETS_SECS: tuple[int, ...] = (300, 1_800, 3_600)

# Capture window end = the last checkpoint. Derived so there is no second
# place to keep in sync.
CAPTURE_POST_SECS: int = max(CAPTURE_POST_OFFSETS_SECS)

# Cycle 29: sparse long-horizon checkpoints, +4h / +1d / +2d / +7d.
# These are NOT covered by the dense 15s live poll (which still ends at
# CAPTURE_POST_SECS) — a separate low-frequency loop backfills them from
# Polymarket's server-side `prices-history` (mid only, no book), so they
# cost ~4 rows per exploration instead of days of polling, work
# retroactively on the historical corpus, and survive restarts. The H8
# slow-catalyst lane (retro 2026-06-12 §8.1) backtests at these horizons.
CAPTURE_LONG_OFFSETS_SECS: tuple[int, ...] = (14_400, 86_400, 172_800, 604_800)
