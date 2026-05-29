"""Explorer firehose snapshot: throughput, capture state, end reasons,
reject-sample utilisation, cluster/freshness breakdowns, latest 15 rows.

Auto-refreshed by the dashboard run loop at `dash.interval_secs` (30s by
default). Mirrors the metrics exposed by
`irene/scripts/explorer/firehose_stats.py` but as a tailable file at
`/data/dashboard/explorations.txt`.

The page assumes the explorer-v3 columns added by
`app/stages/explorer.py` are present (capture_end_reason, cluster_id,
capture_sample_reason). These are nullable, so legacy rows just show
NULL in the breakdowns.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from ..db_helpers import scalar
from ..io import _now

FILENAME = "explorations.txt"

# Time windows for the throughput table. Tuple of (label, sqlite modifier).
_WINDOWS = [
    ("last 1m",  "-1 minute"),
    ("last 5m",  "-5 minutes"),
    ("last 1h",  "-1 hour"),
    ("last 24h", "-24 hours"),
]


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def _age(now: datetime, created_at: Optional[str]) -> str:
    dt = _parse_iso(created_at)
    if dt is None:
        return "?"
    secs = int((now - dt).total_seconds())
    if secs < 0:
        return "0s"
    if secs < 60:
        return f"{secs}s"
    if secs < 3600:
        return f"{secs // 60}m"
    if secs < 86400:
        return f"{secs // 3600}h"
    return f"{secs // 86400}d"


def _trunc(s: Optional[str], n: int) -> str:
    if not s:
        return ""
    s = " ".join(s.split())  # collapse newlines/runs of whitespace
    return s if len(s) <= n else s[: n - 1] + "…"


def _cfg_int(dash, key: str, default: int) -> int:
    cfg = getattr(dash, "cfg", None)
    explorer = getattr(cfg, "explorer", None) if cfg else None
    val = getattr(explorer, key, None) if explorer else None
    try:
        return int(val) if val is not None else default
    except (TypeError, ValueError):
        return default


def _cfg_num(dash, key: str, default: float) -> float:
    cfg = getattr(dash, "cfg", None)
    explorer = getattr(cfg, "explorer", None) if cfg else None
    val = getattr(explorer, key, None) if explorer else None
    try:
        return float(val) if val is not None else default
    except (TypeError, ValueError):
        return default


async def render(dash) -> str:
    cycle_id = dash.current_cycle_id
    cs = f" AND cycle_id = {int(cycle_id)}" if cycle_id is not None else ""
    now = _now()

    cap = _cfg_int(dash, "max_active_explorations", 200)
    reject_cap = _cfg_int(dash, "max_active_reject_samples", 0)
    poll_secs = _cfg_int(dash, "odds_poll_secs", 15)
    sample_rate = _cfg_num(dash, "reject_capture_sample_rate", 0.0)

    lines = [
        f"# irene explorations ({now.isoformat()})",
        f"  cycle_id={cycle_id}; throughput / capture state / latest 15 rows",
        "",
        "── THROUGHPUT ──",
        f"  {'WINDOW':<10} {'TOTAL':>8} {'PASSED':>8} {'REJECTED':>9} {'/MIN':>8}",
        "  " + "─" * 48,
    ]
    # Per-window counts. Cycle-scoped so closed cycles don't bleed in.
    for label, modifier in _WINDOWS:
        where_time = f"datetime(created_at) >= datetime('now','{modifier}')"
        total = await scalar(
            dash.db,
            f"SELECT COUNT(*) FROM explorations WHERE {where_time}{cs}",
        )
        passed = await scalar(
            dash.db,
            f"SELECT COUNT(*) FROM explorations WHERE {where_time}{cs} "
            "AND gate_status='passed'",
        )
        rejected = await scalar(
            dash.db,
            f"SELECT COUNT(*) FROM explorations WHERE {where_time}{cs} "
            "AND gate_status='rejected'",
        )
        # Convert window to minutes for /min rate.
        minutes = {"-1 minute": 1.0, "-5 minutes": 5.0,
                   "-1 hour": 60.0, "-24 hours": 1440.0}[modifier]
        per_min = total / minutes
        lines.append(
            f"  {label:<10} {total:>8d} {passed:>8d} "
            f"{rejected:>9d} {per_min:>8.2f}"
        )
    all_time = await scalar(
        dash.db, f"SELECT COUNT(*) FROM explorations WHERE 1=1{cs}")
    lines.append(f"  {'all-time':<10} {all_time:>8d}")

    # ── REJECTED BY GATE (last 24h) ────────────────────────────────────
    lines += [
        "",
        "── REJECTED BY GATE (last 24h) ──",
    ]
    cur = await dash.db.conn.execute(
        "SELECT rejected_by_gate, COUNT(*) AS n FROM explorations "
        "WHERE datetime(created_at) >= datetime('now','-24 hours') "
        f"AND gate_status='rejected'{cs} "
        "GROUP BY rejected_by_gate ORDER BY n DESC"
    )
    rows = await cur.fetchall()
    await cur.close()
    if not rows:
        lines.append("  (none)")
    else:
        for r in rows:
            name = r["rejected_by_gate"] or "(null)"
            lines.append(f"  {name:<16} {int(r['n']):>6d}")

    # ── CAPTURE STATE ──────────────────────────────────────────────────
    active = await scalar(
        dash.db,
        "SELECT COUNT(*) FROM explorations WHERE capture_status='active'",
    )
    pending = await scalar(
        dash.db,
        "SELECT COUNT(*) FROM explorations WHERE capture_status='pending'",
    )
    complete_24h = await scalar(
        dash.db,
        "SELECT COUNT(*) FROM explorations WHERE capture_status='complete' "
        "AND datetime(capture_end_at) >= datetime('now','-24 hours')",
    )
    cap_util = (active / cap * 100.0) if cap else 0.0
    # CLOB call estimate: sequential odds-capture poll = 1 call per active row
    # per `odds_poll_secs`. 400 req/s is Polymarket's ceiling.
    est_per_sec = (active / poll_secs) if poll_secs else 0.0
    api_budget_pct = est_per_sec / 400.0 * 100.0

    lines += [
        "",
        "── CAPTURE STATE ──",
        f"  active           {active:>5d} / {cap} ({cap_util:5.1f}%)",
        f"  pending          {pending:>5d}",
        f"  complete (24h)   {complete_24h:>5d}",
        f"  CLOB call est    ~{est_per_sec:>4.2f}/s "
        f"({api_budget_pct:.2f}% of 400/s budget)",
    ]

    # ── CAPTURE END REASONS (last 24h, completed only) ─────────────────
    cur = await dash.db.conn.execute(
        "SELECT capture_end_reason, COUNT(*) AS n FROM explorations "
        "WHERE capture_status='complete' "
        "AND datetime(capture_end_at) >= datetime('now','-24 hours') "
        "AND capture_end_reason IS NOT NULL "
        "GROUP BY capture_end_reason ORDER BY n DESC"
    )
    rows = await cur.fetchall()
    await cur.close()
    lines += [
        "",
        "── CAPTURE END REASONS (last 24h) ──",
    ]
    if not rows:
        lines.append("  (none yet — pre-migration rows or first captures still in window)")
    else:
        total_ended = sum(int(r["n"]) for r in rows)
        for r in rows:
            pct = (int(r["n"]) / total_ended * 100.0) if total_ended else 0.0
            lines.append(
                f"  {r['capture_end_reason']:<18} {int(r['n']):>5d}  ({pct:5.1f}%)"
            )

    # ── REJECT SAMPLE (last 24h) ───────────────────────────────────────
    rejected_24h = await scalar(
        dash.db,
        "SELECT COUNT(*) FROM explorations "
        "WHERE datetime(created_at) >= datetime('now','-24 hours') "
        f"AND gate_status='rejected'{cs}",
    )
    sampled_24h = await scalar(
        dash.db,
        "SELECT COUNT(*) FROM explorations "
        "WHERE datetime(created_at) >= datetime('now','-24 hours') "
        f"AND capture_sample_reason='rejected_sample'{cs}",
    )
    active_samples = await scalar(
        dash.db,
        "SELECT COUNT(*) FROM explorations "
        "WHERE capture_status IN ('pending','active') "
        "AND capture_sample_reason='rejected_sample'",
    )
    actual_rate = (sampled_24h / rejected_24h * 100.0) if rejected_24h else 0.0
    sample_cap_util = (
        (active_samples / reject_cap * 100.0) if reject_cap else 0.0
    )
    lines += [
        "",
        "── REJECT SAMPLE (last 24h) ──",
        f"  rejected         {rejected_24h:>5d}",
        f"  sampled          {sampled_24h:>5d}  "
        f"({actual_rate:5.2f}% of rejected; target {sample_rate*100:.1f}%)",
        f"  sample cap util  {active_samples:>5d} / {reject_cap} "
        f"({sample_cap_util:5.1f}%)",
    ]

    # ── CLUSTERS & FRESHNESS (last 24h) ────────────────────────────────
    distinct_clusters = await scalar(
        dash.db,
        "SELECT COUNT(DISTINCT cluster_id) FROM explorations "
        "WHERE datetime(created_at) >= datetime('now','-24 hours') "
        f"AND cluster_id IS NOT NULL{cs}",
    )
    cur = await dash.db.conn.execute(
        "SELECT cluster_id, COUNT(*) AS n FROM explorations "
        "WHERE datetime(created_at) >= datetime('now','-24 hours') "
        f"AND cluster_id IS NOT NULL{cs} "
        "GROUP BY cluster_id ORDER BY n DESC LIMIT 3"
    )
    top_clusters = await cur.fetchall()
    await cur.close()
    top_clusters_s = (
        ", ".join(f"{r['cluster_id']}:{int(r['n'])}" for r in top_clusters)
        or "(none)"
    )
    cur = await dash.db.conn.execute(
        "SELECT COALESCE(freshness,'(null)') AS f, COUNT(*) AS n "
        "FROM explorations "
        "WHERE datetime(created_at) >= datetime('now','-24 hours')"
        f"{cs} GROUP BY f ORDER BY n DESC"
    )
    fresh_rows = await cur.fetchall()
    await cur.close()
    fresh_s = (
        " ".join(f"{r['f']}:{int(r['n'])}" for r in fresh_rows) or "(none)"
    )
    lines += [
        "",
        "── CLUSTERS & FRESHNESS (last 24h) ──",
        f"  distinct clusters   {distinct_clusters}",
        f"  top 3 clusters      {top_clusters_s}",
        f"  freshness           {fresh_s}",
    ]

    # ── LATEST 15 EXPLORATIONS ─────────────────────────────────────────
    cur = await dash.db.conn.execute(
        """
        SELECT e.id, e.created_at, e.gate_status, e.rejected_by_gate,
               e.cluster_id, e.capture_sample_reason,
               a.title AS article_title,
               m.question AS market_question
        FROM explorations e
        LEFT JOIN articles a ON a.id           = e.article_id
        LEFT JOIN markets  m ON m.condition_id = e.market_condition_id
        ORDER BY e.id DESC LIMIT 15
        """
    )
    latest = await cur.fetchall()
    await cur.close()
    lines += [
        "",
        "── LATEST 15 EXPLORATIONS ──",
        f"  {'ID':>5} {'AGE':>5} {'STATUS':<8} {'GATE':<13} "
        f"{'CLUSTER':<8} TITLE · MARKET",
    ]
    if not latest:
        lines.append("  (none)")
    else:
        for r in latest:
            status = r["gate_status"] or "?"
            gate = r["rejected_by_gate"] or (
                "sampled" if r["capture_sample_reason"] else "-"
            )
            cluster = str(r["cluster_id"]) if r["cluster_id"] is not None else "-"
            title = _trunc(r["article_title"], 40)
            market = _trunc(r["market_question"], 36)
            body = f"{title} · {market}" if market else title
            lines.append(
                f"  {int(r['id']):>5d} "
                f"{_age(now, r['created_at']):>5} "
                f"{status:<8} "
                f"{gate:<13} "
                f"{cluster:<8} "
                f"{body}"
            )

    return "\n".join(lines) + "\n"
