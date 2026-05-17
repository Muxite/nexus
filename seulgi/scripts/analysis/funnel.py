"""funnel — decision funnel breakdown by guard.

Wraps `db.get_decision_funnel`-style SQL but runs over an arbitrary
window via `--since`. Default window: last 1h (matches the dashboard).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from scripts.analysis._common import add_common_args, parse_since
from scripts.analysis._format import Section


def add_parser(p) -> None:
    add_common_args(p)


def run(args, conn) -> Section:
    cutoff = parse_since(args.since) or (
        datetime.now(timezone.utc) - timedelta(hours=1)
    ).isoformat()
    row = conn.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN skip_reason LIKE 'freshness:%' THEN 1 ELSE 0 END)        AS f_freshness,
            SUM(CASE WHEN skip_reason LIKE 'low_similarity%' THEN 1 ELSE 0 END)    AS f_low_sim,
            SUM(CASE WHEN skip_reason LIKE 'not_relevant%' THEN 1 ELSE 0 END)      AS f_relevance,
            SUM(CASE WHEN skip_reason = 'neutral' THEN 1 ELSE 0 END)               AS f_neutral,
            SUM(CASE WHEN skip_reason LIKE 'pre_ai_rule%'
                      OR skip_reason LIKE 'RULE_%' THEN 1 ELSE 0 END)              AS f_pre_ai,
            SUM(CASE WHEN skip_reason LIKE 'low_confidence%' THEN 1 ELSE 0 END)    AS f_min_conf,
            SUM(CASE WHEN skip_reason LIKE 'low_agreement%' THEN 1 ELSE 0 END)     AS f_agreement,
            SUM(CASE WHEN skip_reason LIKE 'post_ai_rule%' THEN 1 ELSE 0 END)      AS f_post_ai,
            SUM(CASE WHEN skip_reason LIKE 'hard_rail%'
                      OR skip_reason LIKE 'rail_price%'
                      OR skip_reason IN ('no_token','no_price') THEN 1 ELSE 0 END) AS f_rail,
            SUM(CASE WHEN skip_reason LIKE 'min_entry_yes_mid%' THEN 1 ELSE 0 END) AS f_min_entry,
            SUM(CASE WHEN skip_reason LIKE 'edge_too_small%' THEN 1 ELSE 0 END)    AS f_edge,
            SUM(CASE WHEN skip_reason LIKE 'max_positions%'
                      OR skip_reason = 'duplicate_position'
                      OR skip_reason LIKE 'max_unique_markets%' THEN 1 ELSE 0 END) AS f_posguard,
            SUM(CASE WHEN skip_reason LIKE 'size_too_small%'
                      OR skip_reason = 'no_balance' THEN 1 ELSE 0 END)             AS f_size,
            SUM(CASE WHEN size_usdc > 0 AND skip_reason IS NULL THEN 1 ELSE 0 END) AS executed
        FROM trades WHERE decided_at >= ?
    """, (cutoff,)).fetchone()
    total = row["total"] or 1
    pairs = [
        ("freshness_skip",     row["f_freshness"]),
        ("low_similarity",     row["f_low_sim"]),
        ("relevance_dropped",  row["f_relevance"]),
        ("ai_neutral",         row["f_neutral"]),
        ("pre_ai_rule",        row["f_pre_ai"]),
        ("min_confidence",     row["f_min_conf"]),
        ("agreement",          row["f_agreement"]),
        ("post_ai_rule",       row["f_post_ai"]),
        ("rail",               row["f_rail"]),
        ("min_entry_yes_mid",  row["f_min_entry"]),
        ("edge_too_small",     row["f_edge"]),
        ("position_guard",     row["f_posguard"]),
        ("size",               row["f_size"]),
        ("executed",           row["executed"]),
    ]
    rows = [["total", row["total"], ""]]
    accounted = 0
    for label, n in pairs:
        n = int(n or 0)
        accounted += n
        rows.append([label, n, f"{n / total * 100.0:>5.1f}%"])
    other = max(0, (row["total"] or 0) - accounted)
    rows.append(["other_skipped", other, f"{other / total * 100.0:>5.1f}%"])
    return Section(
        title=f"decision funnel — since {cutoff[:19]}",
        headers=["bucket", "n", "pct"],
        rows=rows,
    )
