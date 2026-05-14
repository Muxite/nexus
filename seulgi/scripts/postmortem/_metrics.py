"""
Shared helpers for the recurrent post-mortem (deterministic SQL+numpy-free
metrics extraction). The orchestrator in `run_postmortem.py` wires these
into the per-run output directory; the AI recommender reads the resulting
markdown files.

Everything in this module is read-only against the seulgi DB. No imports
from the live trader/engine paths.
"""
from __future__ import annotations

import math
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional


REPO = Path(__file__).resolve().parents[2]
DB_PATH = REPO / "data" / "seulgi.db"
POSTMORTEM_ROOT = REPO / "data" / "postmortems"

# Reuse the bucket classifier from the live trader so the post-mortem and the
# live system stay in lock-step on what "captured / no_signal / wrong_dir /
# too_slow" means.
sys.path.insert(0, str(REPO / "app"))
from impact_tracker import classify_missed_alpha, MISSED_ALPHA_THRESH  # noqa: E402


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def open_db(path: Optional[Path] = None) -> sqlite3.Connection:
    """Open the seulgi DB read-only via URI mode (never touches the writer lock)."""
    p = path or DB_PATH
    conn = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r["name"] == column for r in rows)


def all_classified(conn: sqlite3.Connection, hours: Optional[float] = None) -> list[sqlite3.Row]:
    """
    Every price_impacts row with both pre-60s and post-5m deltas populated.
    `hours` clips to the recent window if set.
    """
    if hours is None:
        return conn.execute(
            "SELECT * FROM price_impacts "
            "WHERE delta_t5m IS NOT NULL AND delta_pre_60s IS NOT NULL"
        ).fetchall()
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    return conn.execute(
        "SELECT * FROM price_impacts "
        "WHERE delta_t5m IS NOT NULL AND delta_pre_60s IS NOT NULL "
        "  AND detected_at >= ?",
        (cutoff,),
    ).fetchall()


# ---------------------------------------------------------------------------
# Bucket stats
# ---------------------------------------------------------------------------

def bucket_counts(rows: Iterable[sqlite3.Row]) -> dict[str, int]:
    """Count rows by missed-alpha bucket."""
    c: Counter[str] = Counter()
    for r in rows:
        b = classify_missed_alpha(r["direction"], r["delta_pre_60s"], r["delta_t5m"])
        if b is not None:
            c[b] += 1
    return dict(c)


def bucket_counts_by_dim(
    rows: Iterable[sqlite3.Row], key_fn, *, label: str
) -> list[dict]:
    """
    Group rows by an arbitrary key (e.g. market_type, source, confidence bucket)
    and tally the four buckets per group. Returns a list of dicts ready to
    render as a markdown table.
    """
    grouped: dict[str, Counter[str]] = defaultdict(Counter)
    for r in rows:
        k = key_fn(r)
        if k is None:
            continue
        b = classify_missed_alpha(r["direction"], r["delta_pre_60s"], r["delta_t5m"])
        if b is None:
            continue
        grouped[k][b] += 1
    out = []
    for k, counts in grouped.items():
        n = sum(counts.values())
        out.append({
            label: k,
            "n": n,
            "captured":  counts.get("captured",  0),
            "no_signal": counts.get("no_signal", 0),
            "wrong_dir": counts.get("wrong_dir", 0),
            "too_slow":  counts.get("too_slow",  0),
            "captured_share":  counts.get("captured",  0) / n if n else 0.0,
            "no_signal_share": counts.get("no_signal", 0) / n if n else 0.0,
        })
    out.sort(key=lambda r: -r["n"])
    return out


# ---------------------------------------------------------------------------
# IC (Pearson) and bootstrap helpers
# ---------------------------------------------------------------------------

def pearson(xs: list[float], ys: list[float]) -> Optional[float]:
    """Standard Pearson r. Returns None on degenerate input."""
    n = len(xs)
    if n < 3 or n != len(ys):
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in ys)
    if sxx == 0 or syy == 0:
        return None
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    return sxy / math.sqrt(sxx * syy)


def ic_signed_vs_delta(rows: Iterable[sqlite3.Row]) -> Optional[float]:
    """Pearson r of (signed_confidence) against delta_t5m. Direction-aware."""
    xs, ys = [], []
    for r in rows:
        d = r["direction"]
        conf = r["ai_confidence"]
        d5 = r["delta_t5m"]
        if d not in ("YES", "NO") or conf is None or d5 is None:
            continue
        sign = 1 if d == "YES" else -1
        xs.append(sign * conf)
        ys.append(d5)
    return pearson(xs, ys)


# ---------------------------------------------------------------------------
# Aggregate metrics that go into metrics.json
# ---------------------------------------------------------------------------

def collect_metrics(conn: sqlite3.Connection) -> dict:
    """
    Compute the scalar summary written to `metrics.json`. Keep this dict shape
    stable — `delta_vs_previous.md` diffs against the prior run.
    """
    rows = all_classified(conn)
    buckets = bucket_counts(rows)
    total = sum(buckets.values()) or 1

    # Latency p50/p99 over engine_results
    lat_rows = conn.execute(
        "SELECT signal_latency_ms FROM engine_results "
        "WHERE signal_latency_ms IS NOT NULL "
        "ORDER BY signal_latency_ms ASC"
    ).fetchall()
    latencies = [r["signal_latency_ms"] for r in lat_rows]
    n_lat = len(latencies)

    def pct(p: float) -> Optional[float]:
        return latencies[int(p * n_lat)] if n_lat else None

    # AI flatness over last 1h
    cutoff_1h = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    ai_row = conn.execute(
        "SELECT SUM(CASE WHEN direction='NEUTRAL' THEN 1 ELSE 0 END) AS neu, "
        "       COUNT(*) AS tot "
        "FROM engine_results WHERE decided_at >= ?",
        (cutoff_1h,),
    ).fetchone()
    neutral_rate_1h = (ai_row["neu"] / ai_row["tot"]) if ai_row and ai_row["tot"] else None

    # Per-engine PnL — bridge through signals because closed trade rows can
    # have NULL headline_id (legacy data), but signal_id is always populated.
    engine_rows = conn.execute(
        """
        SELECT er.engine_name AS engine,
               COUNT(*) AS closed,
               SUM(t.realized_pnl) AS pnl
        FROM trades t
        JOIN signals s         ON s.id = t.signal_id
        JOIN engine_results er ON er.headline_id = s.headline_id
                              AND er.condition_id = t.condition_id
                              AND er.executed = 1
        WHERE t.realized_pnl IS NOT NULL
        GROUP BY er.engine_name
        """
    ).fetchall()
    per_engine = {
        r["engine"]: {"pnl": float(r["pnl"] or 0.0), "closed": int(r["closed"] or 0)}
        for r in engine_rows
    }

    # Top skip reasons
    skip_rows = conn.execute(
        "SELECT skip_reason, COUNT(*) AS n FROM engine_results "
        "WHERE executed=0 AND skip_reason IS NOT NULL "
        "GROUP BY skip_reason ORDER BY n DESC LIMIT 12"
    ).fetchall()

    # IC, optionally per market_type
    ic_global = ic_signed_vs_delta(rows)
    ic_by_type: dict[str, Optional[float]] = {}
    if has_column(conn, "markets", "market_type"):
        # Re-fetch with the join so we can group cleanly. Inline join is
        # cheaper than a Python-side map for 130k rows.
        joined = conn.execute(
            "SELECT pi.direction, pi.ai_confidence, pi.delta_t5m, pi.delta_pre_60s, "
            "       COALESCE(m.market_type, 'other') AS market_type "
            "FROM price_impacts pi "
            "LEFT JOIN markets m ON m.condition_id = pi.condition_id "
            "WHERE pi.delta_t5m IS NOT NULL AND pi.delta_pre_60s IS NOT NULL"
        ).fetchall()
        by_type: dict[str, list] = defaultdict(list)
        for r in joined:
            by_type[r["market_type"]].append(r)
        for t, sub in by_type.items():
            ic_by_type[t] = ic_signed_vs_delta(sub)

    # Close reasons (post-fix: from trades.close_reason; legacy → UNKNOWN)
    close_reasons: dict[str, int] = {}
    if has_column(conn, "trades", "close_reason"):
        cr_rows = conn.execute(
            "SELECT COALESCE(close_reason,'UNKNOWN') AS reason, COUNT(*) AS n "
            "FROM trades WHERE closed_at IS NOT NULL GROUP BY reason"
        ).fetchall()
        close_reasons = {r["reason"]: int(r["n"]) for r in cr_rows}

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "price_impacts_classified": total,
        "buckets": {
            k: {"n": v, "share": v / total} for k, v in buckets.items()
        },
        "ic_global": ic_global,
        "ic_by_market_type": ic_by_type,
        "latency_ms": {
            "n": n_lat,
            "p50": pct(0.50),
            "p90": pct(0.90),
            "p99": pct(0.99),
            "mean": (sum(latencies) / n_lat) if n_lat else None,
        },
        "ai_neutral_rate_1h": neutral_rate_1h,
        "ai_decisions_1h": int(ai_row["tot"]) if ai_row else 0,
        "per_engine_pnl": per_engine,
        "top_skip_reasons": [
            {"reason": r["skip_reason"], "n": int(r["n"])} for r in skip_rows
        ],
        "close_reasons": close_reasons,
    }


# ---------------------------------------------------------------------------
# Health canary
# ---------------------------------------------------------------------------

def dashboard_health(conn: sqlite3.Connection, metrics: dict) -> list[dict]:
    """
    One row per dashboard widget — does it have the inputs it needs to render
    something meaningful? "OK" / "BROKEN" / "EMPTY". The orchestrator turns
    this into a markdown table.
    """
    out: list[dict] = []

    # close_reasons.png
    n_closed = sum(metrics.get("close_reasons", {}).values())
    if not has_column(conn, "trades", "close_reason"):
        out.append({
            "widget": "close_reasons.png",
            "status": "BROKEN",
            "detail": "trades.close_reason column missing — restart bot to apply migration",
        })
    elif n_closed == 0:
        out.append({
            "widget": "close_reasons.png",
            "status": "EMPTY",
            "detail": "no closed trades yet (column exists, awaiting first close after restart)",
        })
    else:
        unknown = metrics["close_reasons"].get("UNKNOWN", 0)
        out.append({
            "widget": "close_reasons.png",
            "status": "OK",
            "detail": f"{n_closed} closed trades classified ({unknown} legacy UNKNOWN)",
        })

    # latency_hist.png
    lat = metrics.get("latency_ms", {})
    if not lat.get("n"):
        out.append({"widget": "latency_hist.png", "status": "EMPTY",
                    "detail": "no signal_latency_ms recorded"})
    else:
        out.append({
            "widget": "latency_hist.png",
            "status": "OK",
            "detail": f"n={lat['n']}  p50={lat['p50']:.0f}ms  p99={lat['p99']:.0f}ms",
        })

    # market_type slice
    if not has_column(conn, "markets", "market_type"):
        out.append({
            "widget": "market_type slice",
            "status": "BROKEN",
            "detail": "markets.market_type column missing — restart bot + run tag_market_types",
        })
    else:
        n_tagged = conn.execute(
            "SELECT COUNT(*) AS n FROM markets WHERE market_type IS NOT NULL"
        ).fetchone()["n"]
        n_total = conn.execute("SELECT COUNT(*) AS n FROM markets").fetchone()["n"]
        if n_tagged == 0:
            out.append({
                "widget": "market_type slice",
                "status": "EMPTY",
                "detail": f"column exists but {n_total} markets untagged — run tag_market_types",
            })
        else:
            out.append({
                "widget": "market_type slice",
                "status": "OK",
                "detail": f"{n_tagged}/{n_total} markets tagged",
            })

    # AI signal health
    rate = metrics.get("ai_neutral_rate_1h")
    n_dec = metrics.get("ai_decisions_1h", 0)
    if rate is None or n_dec == 0:
        out.append({"widget": "AI 1h NEUTRAL%", "status": "EMPTY",
                    "detail": "no decisions in last hour"})
    elif rate >= 0.85:
        out.append({"widget": "AI 1h NEUTRAL%", "status": "BROKEN",
                    "detail": f"signal starved: {rate*100:.1f}% NEUTRAL (n={n_dec})"})
    else:
        out.append({"widget": "AI 1h NEUTRAL%", "status": "OK",
                    "detail": f"{rate*100:.1f}% NEUTRAL (n={n_dec})"})

    # IC global
    ic = metrics.get("ic_global")
    if ic is None:
        out.append({"widget": "IC (global)", "status": "EMPTY",
                    "detail": "not enough rows for Pearson"})
    elif ic < 0:
        out.append({"widget": "IC (global)", "status": "BROKEN",
                    "detail": f"IC={ic:+.3f} (anti-informative)"})
    else:
        out.append({"widget": "IC (global)", "status": "OK",
                    "detail": f"IC={ic:+.3f}"})

    return out
