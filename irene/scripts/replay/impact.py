"""Tier 3 of the replay plan — impact analysis over `price_impacts`.

## Specification

**Purpose.** Pure read over `price_impacts` joined to `trades`. Answers
the research questions that need the *trajectory* of the mid after every
executed trade:

- **i-D2** (repricing half-life). Per-bucket median `edge_realised_tXs`
  + per-trade time-to-first-50%-move toward `p_hat`. Distribution by
  freshness, category, source, decision_path.
- **i-D3 partial** (counterfactual: would tighter min_confidence have
  made money). Compares `edge_realised_t3600s` between confidence
  buckets — if low-confidence trades happen to have *better* edge
  realised, the gate is mis-set.
- **W6** (pre-news drift). Correlation between
  `pre_news_drift_60m` and post-trade `edge_realised_t3600s` — does the
  market already know? See `impact_tracker.py` docstring.

This script does NO config sweeping and does NO LLM calls. It is a
read-only aggregation script that produces a report. The buckets it
reads from are populated by the LIVE impact_tracker background poll.

**Public interface.**
- CLI: `python -m scripts.replay.impact --snapshot <dir> [--out report.json]`
- `run_inproc(snapshot_dir) -> ImpactReport` — programmatic entry point.

**Invariants.**
- Snapshot directory only. No live-DB reads.
- Reports both `n` and the value for every aggregation — small-n
  caveats are the operator's job to read, not ours to suppress.

**Non-goals.**
- Plotting (the dashboard already plots per-trade impact curves at
  `data/dashboard/plots/missed_edge/`).
- Re-running the cascade — that's `predict.py` (Tier 1).
- LLM model comparison — that's a follow-up over `llm_probe_calls`.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# Allow `python -m scripts.replay.impact` from the irene/ directory.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.replay._snapshot_loader import SnapshotLoader  # noqa: E402


# The bucket suffixes price_impacts carries, in chronological order.
# Older rows pre-cycle-11 may have NULL on the long-tail (>1h) columns.
BUCKET_SECONDS = [60, 300, 900, 1800, 3600, 7200, 14400, 28800, 86400]
BUCKET_LABELS = ["1m", "5m", "15m", "30m", "1h", "2h", "4h", "8h", "24h"]


def _median(values: list[float]) -> Optional[float]:
    vs = [v for v in values if v is not None]
    if not vs:
        return None
    return statistics.median(vs)


def _percentile(values: list[float], p: float) -> Optional[float]:
    """Hand-rolled to avoid numpy dependency. p in [0,100]."""
    vs = sorted(v for v in values if v is not None)
    if not vs:
        return None
    k = (len(vs) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(vs) - 1)
    if f == c:
        return vs[f]
    return vs[f] + (vs[c] - vs[f]) * (k - f)


def _pearson(xs: list[float], ys: list[float]) -> Optional[float]:
    """Pearson correlation, nan-safe. Returns None when n<3 or zero variance."""
    pairs = [(x, y) for x, y in zip(xs, ys) if x is not None and y is not None]
    if len(pairs) < 3:
        return None
    xs2 = [x for x, _ in pairs]
    ys2 = [y for _, y in pairs]
    mx = sum(xs2) / len(xs2)
    my = sum(ys2) / len(ys2)
    num = sum((x - mx) * (y - my) for x, y in pairs)
    dx2 = sum((x - mx) ** 2 for x in xs2)
    dy2 = sum((y - my) ** 2 for y in ys2)
    if dx2 == 0 or dy2 == 0:
        return None
    return num / ((dx2 * dy2) ** 0.5)


@dataclass
class BucketStats:
    label: str
    n: int
    median_edge_realised: Optional[float]
    p25: Optional[float]
    p75: Optional[float]


@dataclass
class SliceReport:
    name: str
    n_trades: int
    buckets: list[BucketStats]


@dataclass
class TimeToFirstHalf:
    """Per-trade time (in seconds) for the mid to first move at least
    50% of the way from `entry_mid` toward `p_hat`. None when the trade
    never reaches the threshold within the observed 24h window."""

    trade_id: int
    direction: str
    entry_mid: float
    p_hat: Optional[float]
    target_move: Optional[float]
    first_half_seconds: Optional[int]


@dataclass
class ImpactReport:
    snapshot: str
    n_trades_with_impact: int
    n_trades_total: int
    overall: SliceReport
    by_freshness: list[SliceReport]
    by_decision_path: list[SliceReport]
    by_category: list[SliceReport]
    time_to_first_half_median_s: Optional[int]
    time_to_first_half_n: int
    pre_news_drift_correlation_t3600s_r: Optional[float]
    pre_news_drift_correlation_t3600s_n: int

    def to_dict(self) -> dict:
        def slice_to_dict(s: SliceReport) -> dict:
            return {
                "name": s.name,
                "n_trades": s.n_trades,
                "buckets": [
                    {
                        "label": b.label,
                        "n": b.n,
                        "median_edge_realised": b.median_edge_realised,
                        "p25": b.p25,
                        "p75": b.p75,
                    }
                    for b in s.buckets
                ],
            }

        return {
            "snapshot": self.snapshot,
            "n_trades_with_impact": self.n_trades_with_impact,
            "n_trades_total": self.n_trades_total,
            "overall": slice_to_dict(self.overall),
            "by_freshness": [slice_to_dict(s) for s in self.by_freshness],
            "by_decision_path": [slice_to_dict(s) for s in self.by_decision_path],
            "by_category": [slice_to_dict(s) for s in self.by_category],
            "time_to_first_half_median_s": self.time_to_first_half_median_s,
            "time_to_first_half_n": self.time_to_first_half_n,
            "pre_news_drift_correlation_t3600s_r": self.pre_news_drift_correlation_t3600s_r,
            "pre_news_drift_correlation_t3600s_n": self.pre_news_drift_correlation_t3600s_n,
        }


def _compute_slice(name: str, impacts: list[dict]) -> SliceReport:
    """Aggregate `edge_realised_tXs` across every bucket for a given
    slice of trades."""
    buckets: list[BucketStats] = []
    for sec, lbl in zip(BUCKET_SECONDS, BUCKET_LABELS):
        col = f"edge_realised_t{sec}s"
        values = [pi.get(col) for pi in impacts]
        buckets.append(
            BucketStats(
                label=lbl,
                n=sum(1 for v in values if v is not None),
                median_edge_realised=_median(values),
                p25=_percentile(values, 25.0),
                p75=_percentile(values, 75.0),
            )
        )
    return SliceReport(name=name, n_trades=len(impacts), buckets=buckets)


def _first_half_seconds(pi: dict, p_hat: Optional[float]) -> Optional[int]:
    """Walk the bucket trajectory chronologically; return the first
    bucket-second whose `edge_realised` is at least 50% of the full
    desired move toward p_hat. Returns None if no bucket clears or if
    p_hat is missing / degenerate."""
    if p_hat is None:
        return None
    entry_mid = pi.get("entry_mid")
    if entry_mid is None:
        return None
    full_target = abs(p_hat - entry_mid)
    if full_target <= 0:
        return None
    threshold = 0.5 * full_target
    for sec in BUCKET_SECONDS:
        edge = pi.get(f"edge_realised_t{sec}s")
        if edge is not None and edge >= threshold:
            return sec
    return None


def run_inproc(snapshot_dir: str) -> ImpactReport:
    loader = SnapshotLoader(snapshot_dir)

    fired = loader.fired_trades()
    with_impact = loader.trade_with_impact()
    impacts = [pi for _t, pi in with_impact]

    overall = _compute_slice("overall", impacts)

    # ---- by-freshness ----------------------------------------------------
    by_fresh: dict[str, list[dict]] = {}
    for t, pi in with_impact:
        key = (t.get("freshness") or "UNKNOWN")
        by_fresh.setdefault(key, []).append(pi)
    by_freshness = [
        _compute_slice(name, lst) for name, lst in sorted(by_fresh.items())
    ]

    # ---- by-decision_path ------------------------------------------------
    by_path: dict[str, list[dict]] = {}
    for t, pi in with_impact:
        key = (t.get("decision_path") or "mvp")
        by_path.setdefault(key, []).append(pi)
    by_decision_path = [
        _compute_slice(name, lst) for name, lst in sorted(by_path.items())
    ]

    # ---- by-category -----------------------------------------------------
    # Category lives in features_snapshot.market_at_trade (PR-B addition)
    # or on the live markets table (not in snapshot). Older trades
    # pre-PR-B will fall into UNKNOWN.
    by_cat: dict[str, list[dict]] = {}
    for t, pi in with_impact:
        fs = loader.features_snapshot(t) or {}
        mat = fs.get("market_at_trade") or {}
        key = (mat.get("category") or "UNKNOWN")
        by_cat.setdefault(key, []).append(pi)
    by_category = [
        _compute_slice(name, lst) for name, lst in sorted(by_cat.items())
    ]

    # ---- time-to-first-50%-move ------------------------------------------
    first_halfs: list[int] = []
    for t, pi in with_impact:
        fs = loader.features_snapshot(t) or {}
        p_hat = fs.get("p_hat")
        # For NO trades the realised edge moves DOWN; the impact_tracker
        # `edge_realised` column already normalises sign (positive = our
        # direction). Comparing against |p_hat - entry_mid| is correct
        # in either case because the absolute desired move is the same.
        fh = _first_half_seconds(pi, p_hat)
        if fh is not None:
            first_halfs.append(fh)

    # ---- pre_news_drift correlation --------------------------------------
    drift_60m: list[float] = []
    edge_1h: list[float] = []
    for t, pi in with_impact:
        d = pi.get("pre_news_drift_60m")
        e = pi.get("edge_realised_t3600s")
        if d is not None and e is not None:
            drift_60m.append(float(d))
            edge_1h.append(float(e))
    pre_news_r = _pearson(drift_60m, edge_1h)

    return ImpactReport(
        snapshot=str(snapshot_dir),
        n_trades_with_impact=len(with_impact),
        n_trades_total=len(fired),
        overall=overall,
        by_freshness=by_freshness,
        by_decision_path=by_decision_path,
        by_category=by_category,
        time_to_first_half_median_s=int(_median(first_halfs)) if first_halfs else None,
        time_to_first_half_n=len(first_halfs),
        pre_news_drift_correlation_t3600s_r=pre_news_r,
        pre_news_drift_correlation_t3600s_n=len(drift_60m),
    )


def _print_report(rep: ImpactReport) -> None:
    print(f"impact analysis: {rep.snapshot}")
    print(
        f"  trades with impact / fired total: "
        f"{rep.n_trades_with_impact} / {rep.n_trades_total}"
    )
    print()

    def print_slice(s: SliceReport) -> None:
        print(f"  [{s.name}] n_trades={s.n_trades}")
        if s.n_trades == 0:
            return
        print(f"  {'bucket':<8} {'n':>4} {'median':>10} {'p25':>10} {'p75':>10}")
        for b in s.buckets:
            med = f"{b.median_edge_realised:.4f}" if b.median_edge_realised is not None else "  -   "
            p25 = f"{b.p25:.4f}" if b.p25 is not None else "  -   "
            p75 = f"{b.p75:.4f}" if b.p75 is not None else "  -   "
            print(f"  {b.label:<8} {b.n:>4} {med:>10} {p25:>10} {p75:>10}")
        print()

    print_slice(rep.overall)

    print("by freshness:")
    for s in rep.by_freshness:
        print_slice(s)

    print("by decision_path:")
    for s in rep.by_decision_path:
        print_slice(s)

    print("by category:")
    for s in rep.by_category:
        print_slice(s)

    if rep.time_to_first_half_median_s is not None:
        print(
            f"time-to-first-50%-move (median): {rep.time_to_first_half_median_s}s "
            f"  n={rep.time_to_first_half_n}"
        )
    else:
        print(f"time-to-first-50%-move: NO trades reached threshold "
              f"(n={rep.time_to_first_half_n})")

    if rep.pre_news_drift_correlation_t3600s_r is not None:
        print(
            f"pre_news_drift_60m × edge_realised_t3600s  Pearson r = "
            f"{rep.pre_news_drift_correlation_t3600s_r:.3f}  "
            f"n={rep.pre_news_drift_correlation_t3600s_n}"
        )
    else:
        print("pre_news_drift correlation: insufficient data (need n≥3)")


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--snapshot", required=True, help="snapshot directory")
    p.add_argument("--out", help="optional JSON output path")
    args = p.parse_args(argv)

    try:
        rep = run_inproc(args.snapshot)
    except (FileNotFoundError, ValueError) as exc:
        print(f"impact: ERROR {exc}", file=sys.stderr)
        return 2

    _print_report(rep)
    if args.out:
        Path(args.out).write_text(json.dumps(rep.to_dict(), indent=2))
        print(f"\nreport JSON: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
