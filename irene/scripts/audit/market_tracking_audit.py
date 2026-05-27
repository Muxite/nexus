"""Audit market-tracking integrity (cycle 25, task #7).

Surfaces the questions the cycle-35 audit raised about "are we discovering
markets correctly":

  1. How many markets does our DB hold, and what's the active/closed mix?
  2. Any active=1+closed_at NOT NULL flicker rows? (Cycle-25-H target.)
  3. Any orphan trades — trades whose market_id has no markets row?
  4. Zombie open trades on dead markets (cycle-25-A target).
  5. Open trades on the same Polymarket binary event (cycle-25-B target).
  6. Markets staleness — when was the table last refreshed?
  7. Decisions rejected with `market_vanished` (cycle-25 gate firing rate).

The script is read-only. Output is a single markdown report; with
`--print` it also prints to stdout. Exit code 0 if all checks pass at
default thresholds; non-zero if any critical finding is present.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path


def _connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    return con


def _section_markets_state(con: sqlite3.Connection) -> list[str]:
    out = ["## markets table state", ""]
    cur = con.cursor()
    rows = cur.execute(
        """
        SELECT 'TOTAL' AS k, COUNT(*) AS n FROM markets
        UNION ALL SELECT 'tradable (active=1 AND closed_at IS NULL)', COUNT(*)
            FROM markets WHERE active=1 AND closed_at IS NULL
        UNION ALL SELECT 'flicker (active=1 AND closed_at IS NOT NULL)', COUNT(*)
            FROM markets WHERE active=1 AND closed_at IS NOT NULL
        UNION ALL SELECT 'clean-closed (active=0 AND closed_at IS NOT NULL)', COUNT(*)
            FROM markets WHERE active=0 AND closed_at IS NOT NULL
        UNION ALL SELECT 'orphan-state (active=0 AND closed_at IS NULL)', COUNT(*)
            FROM markets WHERE active=0 AND closed_at IS NULL
        """
    ).fetchall()
    out += [f"- **{r['k']}**: {r['n']}" for r in rows]
    return out


def _section_staleness(con: sqlite3.Connection) -> list[str]:
    out = ["", "## markets refresh staleness (`updated_at`)", ""]
    rows = con.execute(
        """
        SELECT CASE
            WHEN updated_at >= datetime('now','-15 minutes') THEN '<15m (fresh)'
            WHEN updated_at >= datetime('now','-1 hour') THEN '15m-1h'
            WHEN updated_at >= datetime('now','-24 hours') THEN '1-24h'
            WHEN updated_at >= datetime('now','-7 days') THEN '1-7d'
            ELSE '>7d (very stale)' END AS bucket,
        COUNT(*) AS n,
        SUM(CASE WHEN active=1 AND closed_at IS NULL THEN 1 ELSE 0 END) AS tradable
        FROM markets GROUP BY 1
        """
    ).fetchall()
    out.append("| Bucket | Total | Tradable |")
    out.append("|---|---|---|")
    for r in rows:
        out.append(f"| {r['bucket']} | {r['n']} | {r['tradable']} |")
    return out


def _section_orphan_trades(con: sqlite3.Connection) -> tuple[list[str], int]:
    """Trades whose market_id has no row in `markets` — would mean market
    discovery LOST a market we previously traded. Should be 0."""
    out = ["", "## orphan trades (market row deleted)", ""]
    rows = con.execute(
        """
        SELECT t.id, t.market_id, t.decided_at, t.direction, t.size_usdc,
               t.decision_path
        FROM trades t
        LEFT JOIN markets m ON m.condition_id = t.market_id
        WHERE m.condition_id IS NULL AND t.is_tracer = 0
        ORDER BY t.decided_at DESC
        """
    ).fetchall()
    if not rows:
        out.append("None.")
        return out, 0
    out.append("| Trade ID | Path | Market ID (truncated) | Decided | Direction | Size |")
    out.append("|---|---|---|---|---|---|")
    for r in rows:
        out.append(
            f"| {r['id']} | {r['decision_path']} | `{(r['market_id'] or '')[:20]}…` "
            f"| {(r['decided_at'] or '')[:16]} | {r['direction']} | ${r['size_usdc']:.2f} |"
        )
    return out, len(rows)


def _section_zombies(con: sqlite3.Connection) -> tuple[list[str], int]:
    """Open trades on closed/inactive markets — cycle-25-A target."""
    out = ["", "## zombie open trades (open + market dead)", ""]
    rows = con.execute(
        """
        SELECT t.id, t.market_id, t.decided_at, t.direction, t.size_usdc,
               t.decision_path, m.question, m.closed_at, m.active,
               m.last_resolution_status, m.uma_finalized_at
        FROM trades t
        LEFT JOIN trade_closures c ON c.trade_id = t.id
        LEFT JOIN markets m ON m.condition_id = t.market_id
        WHERE c.id IS NULL AND t.is_tracer = 0
          AND (m.closed_at IS NOT NULL OR m.active != 1)
        ORDER BY t.decided_at DESC
        """
    ).fetchall()
    if not rows:
        out.append("None.")
        return out, 0
    out.append("| Trade | Path | Direction | Size | Question | Closed at | UMA status |")
    out.append("|---|---|---|---|---|---|---|")
    for r in rows:
        q = (r['question'] or '<no row>')[:50]
        out.append(
            f"| {r['id']} | {r['decision_path']} | {r['direction']} | ${r['size_usdc']:.2f} "
            f"| {q} | {(r['closed_at'] or '-')[:16]} "
            f"| {r['last_resolution_status'] or '(none)'} |"
        )
    return out, len(rows)


def _section_binary_dedup(con: sqlite3.Connection) -> tuple[list[str], int]:
    """Open trades on multiple markets that share the SAME canonical event.

    `market_links` is a many-to-many discovery layer: a single market is
    linked to dozens of events with varying scores. The canonical "binary
    pair" event is the one with the HIGHEST `final_score` per market; if
    two open trades share that top-event, they're on different legs of the
    same binary. Cycle-25-B target.
    """
    out = ["", "## binary-event dedup violations (open positions on the same canonical event)", ""]
    rows = con.execute(
        """
        WITH ranked AS (
            SELECT ml.market_condition_id, ml.event_id, ml.final_score,
                   ROW_NUMBER() OVER (
                       PARTITION BY ml.market_condition_id
                       ORDER BY ml.final_score DESC, ml.event_id ASC
                   ) AS rk
            FROM market_links ml
        ),
        top_event_per_market AS (
            SELECT market_condition_id, event_id, final_score
            FROM ranked WHERE rk = 1
        ),
        open_trade_top AS (
            SELECT t.id AS trade_id, t.market_id, t.size_usdc, t.direction,
                   t.decision_path, te.event_id
            FROM trades t
            LEFT JOIN trade_closures c ON c.trade_id = t.id
            LEFT JOIN top_event_per_market te ON te.market_condition_id = t.market_id
            WHERE c.id IS NULL AND t.is_tracer = 0 AND te.event_id IS NOT NULL
        )
        SELECT event_id,
               COUNT(DISTINCT market_id) AS distinct_markets,
               COUNT(DISTINCT trade_id) AS open_trades,
               GROUP_CONCAT(DISTINCT trade_id) AS trade_ids,
               GROUP_CONCAT(DISTINCT direction) AS directions,
               GROUP_CONCAT(DISTINCT decision_path) AS paths
        FROM open_trade_top
        GROUP BY event_id
        HAVING distinct_markets >= 2
        ORDER BY distinct_markets DESC
        """
    ).fetchall()
    if not rows:
        out.append("None.")
        return out, 0
    out.append("| Event ID | Distinct markets | Open trades | Trades | Directions | Paths |")
    out.append("|---|---|---|---|---|---|")
    for r in rows:
        out.append(
            f"| {r['event_id']} | {r['distinct_markets']} | {r['open_trades']} "
            f"| {r['trade_ids']} | {r['directions']} | {r['paths']} |"
        )
    return out, len(rows)


def _section_vanished_rate(con: sqlite3.Connection) -> list[str]:
    """How often is the cycle-25 `market_vanished` gate firing?"""
    out = ["", "## `market_vanished` gate fire rate (last 24h)", ""]
    rows = con.execute(
        """
        SELECT COALESCE(reason, '(execute)') AS reason, COUNT(*) AS n
        FROM decisions
        WHERE created_at >= datetime('now','-24 hours') AND is_tracer = 0
        GROUP BY reason
        ORDER BY n DESC
        """
    ).fetchall()
    out.append("| Reason | Count |")
    out.append("|---|---|")
    for r in rows:
        marker = " ⚠️" if r["reason"] == "market_vanished" else ""
        out.append(f"| {r['reason']}{marker} | {r['n']} |")
    return out


def run_audit(db_path: str, *, output: Path | None, print_to_stdout: bool) -> int:
    con = _connect(db_path)
    parts: list[str] = []
    parts.append(f"# Market-tracking audit ({datetime.now(timezone.utc).isoformat()})")
    parts.append("")
    parts.append(f"DB: `{db_path}`")
    parts += _section_markets_state(con)
    parts += _section_staleness(con)
    orphan_lines, n_orphan = _section_orphan_trades(con)
    parts += orphan_lines
    zombie_lines, n_zombie = _section_zombies(con)
    parts += zombie_lines
    binary_lines, n_binary = _section_binary_dedup(con)
    parts += binary_lines
    parts += _section_vanished_rate(con)

    report = "\n".join(parts) + "\n"
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(report, encoding="utf-8")
    if print_to_stdout:
        sys.stdout.write(report)

    # Exit code: 1 if any critical finding present (orphan trades, zombies,
    # binary-dedup violations). Soft warnings (vanished rate, staleness)
    # do not flip the exit code.
    return 1 if (n_orphan or n_zombie or n_binary) else 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Market-tracking audit (cycle 25)")
    p.add_argument("--db", default="/data/irene.db",
                   help="Path to the irene SQLite DB.")
    p.add_argument("--out", default=None,
                   help="Path to write the markdown report. Default: stdout only.")
    p.add_argument("--print", action="store_true",
                   help="Also write report to stdout.")
    args = p.parse_args(argv)

    out_path = Path(args.out) if args.out else None
    return run_audit(args.db, output=out_path, print_to_stdout=args.print or out_path is None)


if __name__ == "__main__":
    sys.exit(main())
