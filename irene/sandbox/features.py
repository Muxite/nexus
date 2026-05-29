"""Per-trade time-series features extracted from odds_observations.

Pure functions, no matplotlib. The `TradeFeatures` dataclass is the
single source of truth for what we know about a row's price behavior
around news release; every visualization script consumes it.
"""

from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass, asdict
from typing import Optional

_BLOCKS = "▁▂▃▄▅▆▇█"


@dataclass(frozen=True)
class TradeFeatures:
    exploration_id: int
    direction: str                    # "YES"|"NO"|"" — copied from the trade if any
    mid_t0: Optional[float]           # mid at the latest pre-news tick (anchor)
    pre_n: int
    pre_mean: Optional[float]
    pre_slope_per_min: Optional[float]
    pre_stdev: Optional[float]
    pre_last_mid: Optional[float]
    jump_60s: Optional[float]         # first post-news mid >=60s minus pre_last_mid
    post_n: int
    post_mean: Optional[float]
    post_slope_per_min: Optional[float]
    post_stdev: Optional[float]
    post_drift_total: Optional[float] # last post-news mid minus mid_t0
    max_mid: Optional[float]
    min_mid: Optional[float]
    max_offset_s: Optional[int]
    min_offset_s: Optional[int]
    max_excursion_signed: Optional[float]
    mean_spread_pre: Optional[float]
    mean_spread_post: Optional[float]
    sparkline: str

    def to_dict(self) -> dict:
        return asdict(self)


def compute(
    con: sqlite3.Connection,
    exploration_id: int,
    *,
    direction: str = "",
    mid_t0_fallback: Optional[float] = None,
) -> TradeFeatures:
    """Compute all features for one exploration.

    direction: "YES" or "NO" — affects max_excursion_signed (positive when
    the move was in the direction we bet). Pass "" if not in a trade context.

    mid_t0_fallback: explorations.market_mid_t0 from the explorations row;
    used as the anchor when no pre-news ticks exist.
    """
    ticks = list(con.execute(
        "SELECT offset_seconds AS off, mid, best_bid, best_ask, source "
        "FROM odds_observations WHERE exploration_id = ? "
        "ORDER BY offset_seconds",
        (exploration_id,),
    ))

    pre = [r for r in ticks if r["off"] < 0 and r["mid"] is not None]
    post = [r for r in ticks if r["off"] >= 0 and r["mid"] is not None and r["source"] == "live"]

    pre_mids = [float(r["mid"]) for r in pre]
    pre_offsets = [int(r["off"]) for r in pre]
    pre_last_mid = pre_mids[-1] if pre_mids else None

    post_mids = [float(r["mid"]) for r in post]
    post_offsets = [int(r["off"]) for r in post]

    # Anchor: last pre-news mid; fall back to explorations.market_mid_t0,
    # then the first post-news mid.
    mid_t0 = pre_last_mid
    if mid_t0 is None:
        mid_t0 = mid_t0_fallback
    if mid_t0 is None and post_mids:
        mid_t0 = post_mids[0]

    pre_mean = _mean(pre_mids)
    pre_slope = _slope_per_min(pre_offsets, pre_mids)
    pre_stdev = _stdev(pre_mids)
    post_mean = _mean(post_mids)
    post_slope = _slope_per_min(post_offsets, post_mids)
    post_stdev = _stdev(post_mids)

    jump_60s: Optional[float] = None
    if pre_last_mid is not None:
        for r in post:
            if r["off"] >= 60:
                jump_60s = float(r["mid"]) - pre_last_mid
                break

    post_drift_total: Optional[float] = None
    if post_mids and mid_t0 is not None:
        post_drift_total = post_mids[-1] - mid_t0

    all_mids_with_off = [(int(r["off"]), float(r["mid"])) for r in ticks if r["mid"] is not None]
    max_mid = min_mid = None
    max_offset_s = min_offset_s = None
    if all_mids_with_off:
        max_offset_s, max_mid = max(all_mids_with_off, key=lambda p: p[1])
        min_offset_s, min_mid = min(all_mids_with_off, key=lambda p: p[1])

    max_excursion_signed: Optional[float] = None
    if mid_t0 is not None and post_mids:
        # Pick the post-news tick with the largest absolute deviation from mid_t0
        deltas = [float(r["mid"]) - mid_t0 for r in post]
        if deltas:
            extreme = max(deltas, key=abs)
            sign = 1.0 if direction == "YES" else (-1.0 if direction == "NO" else 1.0)
            max_excursion_signed = extreme * sign

    pre_spreads = [
        float(r["best_ask"]) - float(r["best_bid"])
        for r in pre
        if r["best_bid"] is not None and r["best_ask"] is not None
    ]
    post_spreads = [
        float(r["best_ask"]) - float(r["best_bid"])
        for r in post
        if r["best_bid"] is not None and r["best_ask"] is not None
    ]
    mean_spread_pre = _mean(pre_spreads) if pre_spreads else None
    mean_spread_post = _mean(post_spreads) if post_spreads else None

    all_mids = [float(r["mid"]) for r in ticks if r["mid"] is not None]
    spark = sparkline(all_mids, width=16)

    return TradeFeatures(
        exploration_id=exploration_id,
        direction=direction,
        mid_t0=mid_t0,
        pre_n=len(pre_mids),
        pre_mean=pre_mean,
        pre_slope_per_min=pre_slope,
        pre_stdev=pre_stdev,
        pre_last_mid=pre_last_mid,
        jump_60s=jump_60s,
        post_n=len(post_mids),
        post_mean=post_mean,
        post_slope_per_min=post_slope,
        post_stdev=post_stdev,
        post_drift_total=post_drift_total,
        max_mid=max_mid,
        min_mid=min_mid,
        max_offset_s=max_offset_s,
        min_offset_s=min_offset_s,
        max_excursion_signed=max_excursion_signed,
        mean_spread_pre=mean_spread_pre,
        mean_spread_post=mean_spread_post,
        sparkline=spark,
    )


def sparkline(values: list[float], *, width: int = 16) -> str:
    """Map a numeric series onto a fixed-width Unicode block bar.

    Returns "" for empty input. Bucketing: if len(values) > width we
    average into `width` chunks; if shorter we just map each value.
    """
    if not values:
        return ""
    if len(values) > width:
        # Average down
        chunk = len(values) / width
        buckets: list[float] = []
        for i in range(width):
            lo = int(round(i * chunk))
            hi = int(round((i + 1) * chunk))
            hi = max(hi, lo + 1)
            slice_ = values[lo:hi]
            if slice_:
                buckets.append(sum(slice_) / len(slice_))
        values = buckets
    lo, hi = min(values), max(values)
    if hi - lo < 1e-9:
        return _BLOCKS[3] * len(values)
    out = []
    for v in values:
        idx = int(round((v - lo) / (hi - lo) * (len(_BLOCKS) - 1)))
        out.append(_BLOCKS[max(0, min(len(_BLOCKS) - 1, idx))])
    return "".join(out)


def _mean(xs: list[float]) -> Optional[float]:
    if not xs:
        return None
    return sum(xs) / len(xs)


def _stdev(xs: list[float]) -> Optional[float]:
    if len(xs) < 2:
        return None
    mu = sum(xs) / len(xs)
    var = sum((x - mu) ** 2 for x in xs) / (len(xs) - 1)
    return math.sqrt(var)


def _slope_per_min(offsets: list[int], mids: list[float]) -> Optional[float]:
    """OLS slope of mid vs offset; returned in units of mid per minute.
    Returns None when n<2 or all offsets identical."""
    n = len(mids)
    if n < 2:
        return None
    xs = offsets
    ys = mids
    xbar = sum(xs) / n
    ybar = sum(ys) / n
    num = sum((x - xbar) * (y - ybar) for x, y in zip(xs, ys))
    den = sum((x - xbar) ** 2 for x in xs)
    if den < 1e-9:
        return None
    slope_per_sec = num / den
    return slope_per_sec * 60.0
