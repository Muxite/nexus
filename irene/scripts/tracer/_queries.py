"""Shared query helpers for `scripts/tracer/fire.py` and `scripts/tracer/check.py`.

Lives under `scripts/tracer/` (not in `app/`) because it's a script-side concern
— modularization rule "no cross-stage imports" doesn't apply to scripts/, but
we keep the read-only queries in one place so the harness and the checker
cannot drift apart.

Only synchronous `sqlite3` is used here — `check.py` runs standalone against
an existing DB file and has no asyncio loop to thread through.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Optional


# Tables tracer rows fan out into, in the order the funnel processes them.
TRACER_TABLES: tuple[str, ...] = (
    "articles",
    "article_clusters",
    "signals",
    "decisions",
    "trades",
    "decision_inputs",
)


@dataclass(frozen=True)
class TracerCounts:
    """Per-table row counts for tracer rows.

    `decision_inputs` is filtered to `kind='signal'` because that's the only
    kind a P1 tracer populates (per `docs/p1_contracts.md` §7).
    """

    articles: int
    article_clusters: int
    signals: int
    decisions: int
    trades: int
    decision_inputs: int

    def as_dict(self) -> dict[str, int]:
        return {
            "articles": self.articles,
            "article_clusters": self.article_clusters,
            "signals": self.signals,
            "decisions": self.decisions,
            "trades": self.trades,
            "decision_inputs": self.decision_inputs,
        }


def _count(
    conn: sqlite3.Connection,
    table: str,
    tracer_predicate: Optional[str] = None,
) -> int:
    where = " WHERE " + tracer_predicate if tracer_predicate else ""
    try:
        cur = conn.execute(f"SELECT COUNT(*) FROM {table}{where}")
    except sqlite3.OperationalError:
        return 0
    row = cur.fetchone()
    cur.close()
    return int(row[0] if row else 0)


def query_tracer_counts(conn: sqlite3.Connection) -> TracerCounts:
    """Read the tracer-row counts off an open sqlite3 connection.

    Every table that carries `is_tracer` is filtered on `is_tracer=1`;
    `decision_inputs` additionally filters on `kind='signal'`.
    """
    return TracerCounts(
        articles=_count(conn, "articles", "is_tracer = 1"),
        article_clusters=_count(conn, "article_clusters", "is_tracer = 1"),
        signals=_count(conn, "signals", "is_tracer = 1"),
        decisions=_count(conn, "decisions", "is_tracer = 1"),
        trades=_count(conn, "trades", "is_tracer = 1"),
        decision_inputs=_count(
            conn,
            "decision_inputs",
            "kind = 'signal' AND is_tracer = 1",
        ),
    )


def open_readonly(db_path: str) -> sqlite3.Connection:
    """Open a sqlite connection for inspection.

    Used by `check.py` against a soak DB. `:memory:` is rejected — the
    checker is a tool for inspecting persisted state.
    """
    if db_path == ":memory:" or not db_path:
        raise ValueError(
            "check.py requires a file-backed DB path; got :memory:/empty. "
            "Pass --db to fire.py to spill state to a file the checker can read."
        )
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn
