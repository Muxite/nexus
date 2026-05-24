"""Single source of truth for which PNG plots the dashboard renders.

Importing this module forces matplotlib to load. Callers should wrap the
import in try/except so a missing matplotlib never crashes the dashboard.
"""

from __future__ import annotations

from . import (
    confidence_buckets,
    decision_funnel,
    equity_curve,
    freshness_buckets,
    scout_funnel,
    variant_leaderboard,
)

PLOTS = [
    variant_leaderboard,
    decision_funnel,
    confidence_buckets,
    freshness_buckets,
    equity_curve,
    scout_funnel,
]
