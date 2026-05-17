"""Shared helpers for analysis scripts.

Read-only access only. Every subcommand goes through `open_db()` (sqlite3
URI mode=ro) so the live process never blocks on us holding a write lock.

Conventions:
- "Closed trade" = `size_usdc > 0 AND skip_reason IS NULL AND realized_pnl IS NOT NULL`.
  Use `closed_trades_filter()` to get this clause verbatim.
- All timestamps in the DB are tz-aware ISO ("2026-05-16T00:13:22+00:00").
  `parse_since()` returns an ISO cutoff in the same format so `WHERE
  decided_at >= ?` is a direct string compare.
- `--db` flag → `open_db(path)`. `SEULGI_DB` env overrides the default.
"""
from __future__ import annotations

import os
import re
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

REPO = Path(__file__).resolve().parents[2]
DB_PATH = REPO / "data" / "seulgi.db"
ANALYSIS_DIR = REPO / "data" / "analysis"

# Make `from impact_tracker import ...` work for the existing analysis modules.
sys.path.insert(0, str(REPO / "app"))
try:
    from impact_tracker import classify_missed_alpha, MISSED_ALPHA_THRESH  # noqa: E402
except Exception:  # pragma: no cover — only used by feature_breakdown
    classify_missed_alpha = None  # type: ignore[assignment]
    MISSED_ALPHA_THRESH = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------


def _default_db() -> Path:
    env = os.getenv("SEULGI_DB")
    if env:
        return Path(env)
    if DB_PATH.exists():
        return DB_PATH
    # Fall back to the most recent snapshot if the live DB isn't reachable.
    snaps = REPO / "snapshots"
    if snaps.exists():
        candidates = sorted(
            (s / "seulgi.db" for s in snaps.iterdir() if (s / "seulgi.db").exists()),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if candidates:
            return candidates[0]
    return DB_PATH


def open_db(path: Optional[Path] = None) -> sqlite3.Connection:
    p = Path(path) if path else _default_db()
    conn = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def all_classified(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """All price_impacts rows with both delta_t5m and delta_pre_60s present.

    Kept for compatibility with the existing feature_breakdown.py script.
    """
    return conn.execute(
        """
        SELECT * FROM price_impacts
        WHERE delta_t5m IS NOT NULL AND delta_pre_60s IS NOT NULL
        """
    ).fetchall()


# ---------------------------------------------------------------------------
# Time parsing
# ---------------------------------------------------------------------------


_DURATION_RE = re.compile(r"^(\d+(?:\.\d+)?)([smhdw])$", re.IGNORECASE)
_UNIT_SECS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


def parse_since(spec: Optional[str]) -> Optional[str]:
    """Convert `24h`, `7d`, `30m`, or an ISO datetime to an ISO cutoff string.

    Returns None when spec is None / empty (caller treats as "no time
    filter"). Raises ValueError on unrecognised input — callers should
    bubble the error to argparse so the user sees it.
    """
    if not spec:
        return None
    m = _DURATION_RE.match(spec.strip())
    if m:
        n = float(m.group(1))
        unit = m.group(2).lower()
        delta = timedelta(seconds=n * _UNIT_SECS[unit])
        cutoff = datetime.now(timezone.utc) - delta
        return cutoff.isoformat()
    try:
        dt = datetime.fromisoformat(spec)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception as exc:
        raise ValueError(
            f"--since must be a duration (e.g. 24h, 7d) or ISO timestamp; got {spec!r}"
        ) from exc


# ---------------------------------------------------------------------------
# SQL boilerplate
# ---------------------------------------------------------------------------


def closed_trades_filter(prefix: str = "") -> str:
    """`WHERE`-clause body for closed (executed + resolved) trades.

    `prefix` adds a table alias like `t.` when joining.
    """
    p = prefix
    return (
        f"{p}size_usdc > 0 AND {p}skip_reason IS NULL "
        f"AND {p}realized_pnl IS NOT NULL"
    )


def executed_filter(prefix: str = "") -> str:
    """`WHERE`-clause body for executed (open OR closed) trades."""
    p = prefix
    return f"{p}size_usdc > 0 AND {p}skip_reason IS NULL"


def build_filters(args, prefix: str = "") -> tuple[str, list]:
    """Build the common `--since` + `--variant` WHERE fragment.

    Returns (sql_fragment, params). sql_fragment is already prefixed with
    "AND" so the caller can append it after their base WHERE clause.
    Empty string when no filters were supplied.
    """
    p = prefix
    frags: list[str] = []
    params: list = []
    since = parse_since(getattr(args, "since", None))
    if since:
        frags.append(f"{p}decided_at >= ?")
        params.append(since)
    variant = getattr(args, "variant", None)
    if variant:
        frags.append(
            f"COALESCE({p}variant_name, {p}engine_name, '') LIKE ?"
        )
        params.append(variant)
    if not frags:
        return "", []
    return " AND " + " AND ".join(frags), params


# ---------------------------------------------------------------------------
# Formatting helpers (kept here for backwards-compat; richer rendering
# lives in _format.py).
# ---------------------------------------------------------------------------


def compute_hit_pct(wins: int, losses: int) -> Optional[float]:
    closed = (wins or 0) + (losses or 0)
    if closed == 0:
        return None
    return wins / closed * 100.0


def signed_pct(n: int, total: int) -> str:
    if not total:
        return "  —  "
    return f"{n / total * 100.0:>5.1f}%"


# ---------------------------------------------------------------------------
# Argparse helpers — every subcommand reuses the same flag set
# ---------------------------------------------------------------------------


def add_common_args(parser) -> None:
    parser.add_argument("--db", type=Path, default=None,
                        help="path to seulgi.db (default: data/seulgi.db or "
                             "SEULGI_DB env var; falls back to latest snapshot)")
    parser.add_argument("--since", default=None,
                        help="time window (e.g. 24h, 7d, 30m, or ISO datetime)")
    parser.add_argument("--variant", default=None,
                        help="SQL LIKE pattern for variant_name "
                             "(e.g. 'aggressive%%', '%%norail%%')")
    parser.add_argument("--format", choices=("text", "md", "json"),
                        default="text", help="output format")
    parser.add_argument("--limit", type=int, default=None,
                        help="row cap (subcommand-specific default if unset)")


# ---------------------------------------------------------------------------
# Pipeline-decision pulls — shared by sample_failures + decision_card
# ---------------------------------------------------------------------------


def trade_outcome_for(conn, headline_id: Optional[str],
                      condition_id: Optional[str]) -> dict:
    """Aggregate realized PnL + close-reason mix across every variant
    that executed this (headline, market) pair.

    Returns {} (falsy) when there's no trade match — the caller renders
    "(not executed)" in that case.
    """
    if not headline_id or not condition_id:
        return {}
    rows = conn.execute(
        """
        SELECT realized_pnl, close_reason, size_usdc,
               COALESCE(variant_name, engine_name, 'default') AS variant,
               mkt_vol_30m, mkt_drift_30m, mid_yes, mid_no,
               confidence_raw, p_alpha
          FROM trades
         WHERE headline_id = ? AND condition_id = ?
           AND size_usdc > 0 AND skip_reason IS NULL
        """,
        (headline_id, condition_id),
    ).fetchall()
    if not rows:
        return {}
    n = len(rows)
    staked = sum(r["size_usdc"] or 0 for r in rows)
    realized = sum(r["realized_pnl"] or 0 for r in rows
                   if r["realized_pnl"] is not None)
    reasons: dict[str, int] = {}
    for r in rows:
        cr = r["close_reason"] or "open"
        reasons[cr] = reasons.get(cr, 0) + 1
    # Variants sorted by realized PnL desc; None (still open) at the end.
    variants = sorted(
        ({"name": r["variant"], "pnl": r["realized_pnl"]} for r in rows),
        key=lambda r: (r["pnl"] is None, -(r["pnl"] or 0)),
    )
    # First-row context fields (shared across all variants of one decision).
    sample = rows[0]
    return {
        "n": n, "staked": staked, "realized": realized,
        "close_reasons": reasons,
        "variants": [v["name"] for v in variants],
        "mkt_vol_30m": sample["mkt_vol_30m"],
        "mkt_drift_30m": sample["mkt_drift_30m"],
        "mid_yes": sample["mid_yes"],
        "mid_no": sample["mid_no"],
        "ai_conf": sample["confidence_raw"],
        "p_alpha": sample["p_alpha"],
    }


def fetch_pipeline_inputs(conn, trade_id: int) -> Optional[dict]:
    """One-shot pull of every artefact tied to a single trade row.

    Returns None when the trade_id doesn't exist. Otherwise the dict
    contains: `trade` (the row itself), `signal` (joined), `article`
    (headline_articles, may be None), `impact` (price_impacts, may be
    None), `prompt_meta` (parsed JSON, may be empty), `trader_trace`
    (parsed JSON list, may be empty).
    """
    import json as _json
    trade = conn.execute(
        "SELECT * FROM trades WHERE id = ?", (trade_id,)
    ).fetchone()
    if trade is None:
        return None
    signal = conn.execute(
        "SELECT * FROM signals WHERE id = ?", (trade["signal_id"],)
    ).fetchone()
    hid = trade["headline_id"]
    article = None
    impact = None
    if hid:
        article = conn.execute(
            "SELECT * FROM headline_articles WHERE headline_id = ? LIMIT 1",
            (hid,),
        ).fetchone()
        impact = conn.execute(
            """SELECT * FROM price_impacts
                WHERE headline_id = ? AND condition_id = ? LIMIT 1""",
            (hid, trade["condition_id"]),
        ).fetchone()
    try:
        prompt_meta = _json.loads(trade["ai_prompt_meta"] or "{}")
    except Exception:
        prompt_meta = {}
    try:
        trader_trace = _json.loads(trade["trader_trace"] or "[]")
    except Exception:
        trader_trace = []
    return {
        "trade": dict(trade),
        "signal": dict(signal) if signal else {},
        "article": dict(article) if article else None,
        "impact": dict(impact) if impact else None,
        "prompt_meta": prompt_meta,
        "trader_trace": trader_trace,
    }


__all__ = [
    "DB_PATH", "ANALYSIS_DIR", "REPO",
    "open_db", "all_classified", "parse_since",
    "closed_trades_filter", "executed_filter", "build_filters",
    "compute_hit_pct", "signed_pct",
    "add_common_args",
    "trade_outcome_for", "fetch_pipeline_inputs",
    "classify_missed_alpha", "MISSED_ALPHA_THRESH",
]
