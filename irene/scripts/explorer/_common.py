"""Shared helpers for the explorer analysis scripts.

Read-only sqlite3 access + post-news drift computation. All scripts
import from here so the "what does drift mean" definition lives in
exactly one place.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Iterable, Iterator, Optional


def connect_readonly(db_path: str) -> sqlite3.Connection:
    """Open the irene DB read-only via the `file:?mode=ro` URI form so
    a misbehaving script can never corrupt the production data."""
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    return con


@dataclass(frozen=True)
class ExplorationRow:
    id: int
    article_id: int
    market_condition_id: str
    t0_iso: str
    dkg_final_score: Optional[float]
    cosine_similarity: Optional[float]
    freshness: Optional[str]
    breaking_score: Optional[float]
    article_tier: Optional[str]
    market_volume_24h: Optional[float]
    market_spread_t0: Optional[float]
    market_mid_t0: Optional[float]
    hours_to_expiry: Optional[float]
    llm_confidence: Optional[float]
    llm_reason: Optional[str]
    capture_status: str
    cycle_id: int


def iter_explorations(
    con: sqlite3.Connection,
    *,
    cycle: Optional[str] = None,
    only_complete: bool = False,
) -> Iterator[ExplorationRow]:
    """Yield `ExplorationRow` records, optionally filtered by cycle name
    + capture_status. The cycle filter joins through the `cycles` table.
    """
    where = ["llm_related = 1"]
    params: list = []
    if cycle is not None:
        where.append(
            "cycle_id = (SELECT id FROM cycles WHERE name = ?)"
        )
        params.append(cycle)
    if only_complete:
        where.append("capture_status = 'complete'")
    sql = (
        "SELECT id, article_id, market_condition_id, t0_iso, "
        "       dkg_final_score, cosine_similarity, freshness, "
        "       breaking_score, article_tier, market_volume_24h, "
        "       market_spread_t0, market_mid_t0, hours_to_expiry, "
        "       llm_confidence, llm_reason, capture_status, cycle_id "
        "FROM explorations "
        f"WHERE {' AND '.join(where)} ORDER BY t0_iso"
    )
    for row in con.execute(sql, params):
        yield ExplorationRow(
            id=int(row["id"]),
            article_id=int(row["article_id"]),
            market_condition_id=str(row["market_condition_id"]),
            t0_iso=str(row["t0_iso"]),
            dkg_final_score=_f(row["dkg_final_score"]),
            cosine_similarity=_f(row["cosine_similarity"]),
            freshness=row["freshness"],
            breaking_score=_f(row["breaking_score"]),
            article_tier=row["article_tier"],
            market_volume_24h=_f(row["market_volume_24h"]),
            market_spread_t0=_f(row["market_spread_t0"]),
            market_mid_t0=_f(row["market_mid_t0"]),
            hours_to_expiry=_f(row["hours_to_expiry"]),
            llm_confidence=_f(row["llm_confidence"]),
            llm_reason=row["llm_reason"],
            capture_status=str(row["capture_status"]),
            cycle_id=int(row["cycle_id"]),
        )


def _f(v) -> Optional[float]:
    return float(v) if v is not None else None


@dataclass(frozen=True)
class DriftMetrics:
    """Post-news drift for a single exploration.

    All values are NULL when there are no post-news live samples (the
    capture window had no usable book reads).
    """

    n_live: int                       # count of live samples
    mid_t_end: Optional[float]        # last live mid in the window
    drift_yes_end: Optional[float]    # mid_t_end - mid_t0 (YES drift)
    drift_abs_max: Optional[float]    # max |live_mid - mid_t0|
    drift_yes_min: Optional[float]    # min (live_mid - mid_t0) — worst against YES
    drift_yes_max: Optional[float]    # max (live_mid - mid_t0) — best for YES


def compute_drift(
    con: sqlite3.Connection, exploration_id: int, mid_t0: Optional[float],
) -> DriftMetrics:
    """Walk the live observations for one exploration and compute drift
    metrics relative to `mid_t0`. NULL `mid_t0` collapses every signed
    metric to None (we can't compute drift without an anchor)."""
    rows = list(con.execute(
        "SELECT mid FROM odds_observations "
        "WHERE exploration_id = ? AND source = 'live' AND mid IS NOT NULL "
        "ORDER BY offset_seconds ASC",
        (exploration_id,),
    ))
    if not rows:
        return DriftMetrics(0, None, None, None, None, None)
    mids = [float(r["mid"]) for r in rows]
    mid_end = mids[-1]
    if mid_t0 is None:
        return DriftMetrics(len(mids), mid_end, None, None, None, None)
    diffs = [m - mid_t0 for m in mids]
    return DriftMetrics(
        n_live=len(mids),
        mid_t_end=mid_end,
        drift_yes_end=mid_end - mid_t0,
        drift_abs_max=max(abs(d) for d in diffs),
        drift_yes_min=min(diffs),
        drift_yes_max=max(diffs),
    )


def bucket_label(value: Optional[float], edges: Iterable[float]) -> str:
    """Return a human-readable bucket label for `value` against an
    ordered list of edges. `None` → 'unknown'.

    Example: edges=[0.5, 0.7, 0.9] → '< 0.5', '0.5-0.7', '0.7-0.9', '>= 0.9'.
    """
    if value is None:
        return "unknown"
    edges = list(edges)
    if not edges:
        return str(value)
    if value < edges[0]:
        return f"< {edges[0]:g}"
    for i in range(len(edges) - 1):
        if edges[i] <= value < edges[i + 1]:
            return f"{edges[i]:g}-{edges[i+1]:g}"
    return f">= {edges[-1]:g}"


def mean(values: Iterable[float]) -> Optional[float]:
    vals = list(values)
    if not vals:
        return None
    return sum(vals) / len(vals)


def percentile(values: list[float], p: float) -> Optional[float]:
    if not values:
        return None
    vs = sorted(values)
    k = (len(vs) - 1) * p
    lo = int(k)
    hi = min(lo + 1, len(vs) - 1)
    if lo == hi:
        return vs[lo]
    return vs[lo] + (vs[hi] - vs[lo]) * (k - lo)
