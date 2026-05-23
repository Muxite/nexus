"""funnel — decision outcomes by reason in a time window.

Wraps `decisions.reason` over `--since`. Default window: last 1h
(matches the dashboard panel).
"""

from __future__ import annotations

from scripts.analysis._common import add_common_args, parse_since
from scripts.analysis._format import Section


def add_parser(p) -> None:
    add_common_args(p)


def run(args, conn) -> Section:
    cutoff = parse_since(args.since)
    rows = conn.execute(
        """
        SELECT outcome, reason, COUNT(*) AS n
        FROM decisions
        WHERE created_at >= ?
        GROUP BY outcome, reason
        ORDER BY n DESC
        """,
        (cutoff or "1970-01-01T00:00:00",),
    ).fetchall()
    total = sum(r["n"] for r in rows) or 1
    out_rows = [
        [r["outcome"], r["reason"], r["n"], round(100.0 * r["n"] / total, 1)]
        for r in rows
    ]
    return Section(
        title=f"decision funnel (since {cutoff})",
        headers=["outcome", "reason", "n", "%"],
        rows=out_rows,
        notes=[
            f"total decisions in window: {total}",
            "outcome = 'execute' means the cascade ALL passed; 'reject' carries "
            "the gate name in `reason`.",
        ],
    )
