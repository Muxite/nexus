"""
## Specification

**Purpose.** Wipe a tracer-burst's rows from the live DB. Pair with
`scripts.tracer.fire --tracer-id-prefix <prefix>` and
`scripts.tracer.audit_burst --tracer-id-prefix <prefix>` for the
fire → audit → cleanup loop.

The canonical list of `is_tracer`-aware tables is discovered by grepping
`is_tracer INTEGER` across `app/`. As of cycle 24:

  articles, article_clusters, signals, predictions, decisions, trades,
  trade_closures, decision_inputs, llm_calls, llm_parse_failures,
  price_impacts, events, extraction_runs, facts, dkg_trade_links

Plus `trade_features`, which lacks an `is_tracer` column but is joined
to tracer trades via `trade_id`; we delete via subquery.

**Public surface.**

- `plan_deletes(conn, prefix) -> dict[table, int]` — pure-compute,
  returns would-be delete counts per table. Used by --dry-run + tests.
- `execute_deletes(conn, prefix) -> dict[table, int]` — performs the
  deletes inside a single transaction, returns actual delete counts.
- `main(argv=None) -> int` — CLI.

**Safety.**

- `--tracer-id-prefix` is REQUIRED; no default. Catches the
  catastrophic `DELETE FROM articles WHERE is_tracer=1` typo.
- Inside a single transaction; either all tables wipe or none.
- Per-table FK ordering matters: trade_features → trades, decisions →
  trades, etc. We delete children first.

**Owned tables.** None — write-only against tables owned by other stages.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from typing import Optional

from scripts.tracer._queries import open_readonly


# Order matters: children before parents so FK references (where they
# exist) don't block the delete. The list is the canonical set of
# is_tracer-aware tables in cycle-24 schema. `trade_features` is special
# (no is_tracer column; deleted by subquery against `trades`).
DELETE_ORDER: tuple[str, ...] = (
    # Wallet / closure overlay first
    "trade_closures",
    "price_impacts",
    "trade_features",
    "dkg_trade_links",
    # Decision/trade graph
    "decision_inputs",
    "trades",
    "decisions",
    # LLM evidence
    "predictions",
    "llm_parse_failures",
    "llm_calls",
    # Match / signal layer
    "signals",
    # Extraction layer
    "facts",
    "extraction_runs",
    "events",
    # Cluster / article layer (parents go last)
    "article_clusters",
    "articles",
)


def _count_via_prefix(
    conn: sqlite3.Connection,
    table: str,
    prefix_pat: str,
) -> int:
    """Count rows that would match the prefix-aware predicate for `table`."""
    sql, params = _select_sql_for(table, prefix_pat)
    try:
        cur = conn.execute(sql, params)
        row = cur.fetchone()
        cur.close()
    except sqlite3.OperationalError:
        return 0
    return int(row[0] if row else 0)


def _select_sql_for(table: str, prefix_pat: str) -> tuple[str, tuple]:
    """Return a (COUNT-SELECT, params) tuple for the dry-run probe.
    `trade_features` joins via `trades`; everything else filters directly."""
    if table == "trade_features":
        return (
            "SELECT COUNT(*) FROM trade_features WHERE trade_id IN ("
            "SELECT id FROM trades WHERE is_tracer = 1 AND tracer_id LIKE ?)",
            (prefix_pat,),
        )
    return (
        f"SELECT COUNT(*) FROM {table} "
        f"WHERE is_tracer = 1 AND tracer_id LIKE ?",
        (prefix_pat,),
    )


def _delete_sql_for(table: str, prefix_pat: str) -> tuple[str, tuple]:
    if table == "trade_features":
        return (
            "DELETE FROM trade_features WHERE trade_id IN ("
            "SELECT id FROM trades WHERE is_tracer = 1 AND tracer_id LIKE ?)",
            (prefix_pat,),
        )
    return (
        f"DELETE FROM {table} "
        f"WHERE is_tracer = 1 AND tracer_id LIKE ?",
        (prefix_pat,),
    )


def plan_deletes(conn: sqlite3.Connection, prefix: str) -> dict[str, int]:
    """Return per-table candidate-row counts without mutating anything."""
    pat = f"{prefix}%"
    return {tbl: _count_via_prefix(conn, tbl, pat) for tbl in DELETE_ORDER}


def execute_deletes(
    conn: sqlite3.Connection,
    prefix: str,
) -> dict[str, int]:
    """Perform every delete inside a single transaction. Returns the
    per-table count actually removed (sqlite `cur.rowcount`)."""
    pat = f"{prefix}%"
    counts: dict[str, int] = {}
    # Single explicit BEGIN so a mid-flight error rolls everything back.
    conn.execute("BEGIN IMMEDIATE")
    try:
        for tbl in DELETE_ORDER:
            sql, params = _delete_sql_for(tbl, pat)
            try:
                cur = conn.execute(sql, params)
                counts[tbl] = int(cur.rowcount or 0)
                cur.close()
            except sqlite3.OperationalError:
                # Missing table — record 0 and continue. The DDL set on
                # different deployments diverges (e.g. local sandbox DB
                # without the full chunk-3 schema).
                counts[tbl] = 0
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return counts


def format_report(counts: dict[str, int], *, dry_run: bool) -> str:
    label = "WOULD delete" if dry_run else "Deleted"
    lines = [f"{label} per table:"]
    total = 0
    for tbl, n in counts.items():
        lines.append(f"  {tbl:<24} {n}")
        total += n
    lines.append(f"  {'TOTAL':<24} {total}")
    return "\n".join(lines)


def _open_writable(path: str) -> sqlite3.Connection:
    if not path or path == ":memory:":
        raise ValueError(
            "cleanup.py requires a file-backed DB path; got :memory:/empty."
        )
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    # Foreign keys are not enforced by default; we still respect our
    # hand-curated DELETE_ORDER (children-first) so FK enabling would
    # not surprise us either way.
    return conn


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m scripts.tracer.cleanup",
        description=(
            "Wipe a tracer-burst's rows from the live DB. "
            "REQUIRES --tracer-id-prefix; no default for safety."
        ),
    )
    parser.add_argument(
        "--tracer-id-prefix",
        required=True,
        help="Prefix used in the matching fire.py burst. REQUIRED.",
    )
    parser.add_argument(
        "--db",
        default="data/irene.db",
        help="DB path (default data/irene.db — the live bot's DB).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Print would-be delete counts and exit without mutating. "
            "Use first to confirm scope before the real run."
        ),
    )
    args = parser.parse_args(argv)

    if not args.tracer_id_prefix.strip():
        print("cleanup: --tracer-id-prefix may not be empty", file=sys.stderr)
        return 2

    if args.dry_run:
        try:
            conn = open_readonly(args.db)
        except Exception as exc:  # noqa: BLE001
            print(f"cleanup: cannot open {args.db}: {exc}", file=sys.stderr)
            return 2
        try:
            counts = plan_deletes(conn, args.tracer_id_prefix)
        finally:
            conn.close()
        print(format_report(counts, dry_run=True))
        return 0

    try:
        conn = _open_writable(args.db)
    except Exception as exc:  # noqa: BLE001
        print(f"cleanup: cannot open {args.db}: {exc}", file=sys.stderr)
        return 2
    try:
        counts = execute_deletes(conn, args.tracer_id_prefix)
    finally:
        conn.close()
    print(format_report(counts, dry_run=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
