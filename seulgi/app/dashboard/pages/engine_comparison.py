"""Engine comparison summary — top-line table only.

A1.5-follow-up cull (2026-05-24): previously this file was a 4.5 MB
monster rewritten every 30s because it enumerated every engine variant
× every skip reason. That's WAL-churn on the writer for almost no
operator value — per-engine skip detail is better surfaced by the
analyst tier (`seulgi/scripts/analysis/funnel.py --per-engine`).

We keep the top-line table (one row per engine) capped at 25 rows. Full
history remains in `engine_results` table and `variant_compare.txt`.
"""

from __future__ import annotations

from ..io import _header

FILENAME = "engine_comparison.txt"
TITLE = "ENGINE COMPARISON"

_MAX_ENGINES = 25  # ≤25 rows × ~110 chars ≈ 3 KB; well under the 8 KB target


async def render(dash) -> str:
    lines = [
        _header("ENGINE COMPARISON"),
        "  One row per engine. Sorted by executed_count desc.",
        "  Per-engine skip-reason detail → seulgi/scripts/analysis/funnel.py --per-engine",
        "",
    ]

    if not dash._engine_summary:
        lines.append("  (no engine data yet — waiting for first trades)")
        lines.append("")
        lines.append("  See engine_results table in seulgi.db for full history.")
        return "\n".join(lines) + "\n"

    rows = sorted(
        dash._engine_summary,
        key=lambda r: r.get("executed_count", 0),
        reverse=True,
    )[:_MAX_ENGINES]

    lines.append(
        f"  {'ENGINE':<20} {'EXEC':>6} {'SKIP':>6} {'STAKED':>9}  "
        f"{'AVG EDGE':>9}  {'AVG CONF':>9}  {'LAT ms':>7}  "
        f"{'BALANCE':>10}  FIRST → LAST"
    )
    lines.append("  " + "─" * 110)
    for row in rows:
        name = row["engine_name"]
        bal = dash._engine_balances.get(name, 0.0)
        lat = row.get("avg_latency_ms") or 0.0
        lines.append(
            f"  {name:<20} {row['executed_count']:>6} {row['skipped_count']:>6} "
            f"${row['total_staked']:>8.2f}  "
            f"{row['avg_edge']:>+9.4f}  "
            f"{row['avg_confidence']:>8.1%}  "
            f"{lat:>6.0f}ms  "
            f"${bal:>9.2f}  "
            f"{(row['first_trade'] or '')[:16]} → {(row['last_trade'] or '')[:16]}"
        )

    truncated = len(dash._engine_summary) - len(rows)
    if truncated > 0:
        lines.append(f"  … {truncated} more engines (see variant_compare.txt for full table)")

    lines.append("")
    lines.append("  See engine_results table in seulgi.db for full history.")
    return "\n".join(lines) + "\n"
