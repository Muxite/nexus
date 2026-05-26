"""
## Specification

**Purpose.** Post-burst funnel report + per-pair CSV for a tracer prefix.
Run after `scripts.tracer.fire ... --tracer-id-prefix <prefix>` to see
how the burst flowed through the live pipeline:

  articles fired → signals matched → decisions made → trades executed
                                  → LLM verdict distribution
                                  → reject-reason breakdown

The CLI is read-only — it never mutates the DB. Pair with
`scripts.tracer.cleanup` to wipe the burst when done auditing.

**Public surface.**

- `funnel(conn, prefix) -> Funnel` — pure-compute aggregator used by tests.
- `write_csv(conn, prefix, path)` — per-pair flat dump.
- `main(argv=None) -> int` — CLI entry point.

**Why a separate file?** `fire.py` is a CI ship-gate that must not pull in
read-back queries (those introduce cross-stage coupling). Audit is a
post-burst tool whose only DB contact is SELECTs — keeping it standalone
keeps fire.py minimal.

**Owned tables.** None — read-only.
"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from scripts.tracer._queries import open_readonly


# ---------------------------------------------------------------------------
# Funnel data model
# ---------------------------------------------------------------------------


@dataclass
class Funnel:
    prefix: str
    window_start: Optional[str]
    window_end: Optional[str]
    articles: int = 0
    signals: int = 0
    decisions: int = 0
    decisions_execute: int = 0
    decisions_reject: int = 0
    reject_reasons: dict[str, int] = field(default_factory=dict)
    llm_reached: int = 0
    llm_label_counts: dict[str, int] = field(default_factory=dict)
    llm_label_avg_conf: dict[str, float] = field(default_factory=dict)
    trades: int = 0


def _scalar(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> Any:
    try:
        cur = conn.execute(sql, params)
        row = cur.fetchone()
        cur.close()
    except sqlite3.OperationalError:
        return None
    return row[0] if row else None


def _rows(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> list:
    try:
        cur = conn.execute(sql, params)
        rows = cur.fetchall()
        cur.close()
    except sqlite3.OperationalError:
        return []
    return rows


def funnel(conn: sqlite3.Connection, prefix: str) -> Funnel:
    """Aggregate every is_tracer-aware table down to one funnel snapshot."""
    pat = f"{prefix}%"
    f = Funnel(
        prefix=prefix,
        window_start=_scalar(
            conn,
            "SELECT MIN(ingested_at) FROM articles "
            "WHERE is_tracer = 1 AND tracer_id LIKE ?",
            (pat,),
        ),
        window_end=_scalar(
            conn,
            "SELECT MAX(ingested_at) FROM articles "
            "WHERE is_tracer = 1 AND tracer_id LIKE ?",
            (pat,),
        ),
        articles=int(
            _scalar(
                conn,
                "SELECT COUNT(*) FROM articles "
                "WHERE is_tracer = 1 AND tracer_id LIKE ?",
                (pat,),
            )
            or 0
        ),
        signals=int(
            _scalar(
                conn,
                "SELECT COUNT(*) FROM signals "
                "WHERE is_tracer = 1 AND tracer_id LIKE ?",
                (pat,),
            )
            or 0
        ),
        decisions=int(
            _scalar(
                conn,
                "SELECT COUNT(*) FROM decisions "
                "WHERE is_tracer = 1 AND tracer_id LIKE ?",
                (pat,),
            )
            or 0
        ),
        trades=int(
            _scalar(
                conn,
                "SELECT COUNT(*) FROM trades "
                "WHERE is_tracer = 1 AND tracer_id LIKE ?",
                (pat,),
            )
            or 0
        ),
    )

    for outcome, n in _rows(
        conn,
        "SELECT outcome, COUNT(*) FROM decisions "
        "WHERE is_tracer = 1 AND tracer_id LIKE ? GROUP BY outcome",
        (pat,),
    ):
        if outcome == "execute":
            f.decisions_execute = int(n)
        elif outcome == "reject":
            f.decisions_reject = int(n)

    for reason, n in _rows(
        conn,
        "SELECT reason, COUNT(*) FROM decisions "
        "WHERE is_tracer = 1 AND tracer_id LIKE ? AND outcome = 'reject' "
        "GROUP BY reason ORDER BY COUNT(*) DESC",
        (pat,),
    ):
        f.reject_reasons[str(reason)] = int(n)

    # LLM verdict — `predictions` carries direction + confidence; we
    # filter to tracer rows by joining via signal_id → signals.tracer_id.
    pred_rows = _rows(
        conn,
        """
        SELECT p.direction, COUNT(*), AVG(p.confidence)
        FROM predictions p
        WHERE p.is_tracer = 1 AND p.tracer_id LIKE ?
        GROUP BY p.direction
        """,
        (pat,),
    )
    for direction, n, avg_conf in pred_rows:
        label = str(direction)
        f.llm_label_counts[label] = int(n)
        f.llm_label_avg_conf[label] = round(float(avg_conf or 0.0), 3)
        f.llm_reached += int(n)

    return f


def format_funnel(f: Funnel) -> str:
    lines = [
        f"== Tracer burst audit — prefix: {f.prefix} ==",
        f"Window: {f.window_start} -> {f.window_end}",
        "",
        f"Articles fired:                  {f.articles}",
    ]
    if f.articles > 0:
        match_rate = (f.signals / f.articles) * 100 if f.signals else 0
        lines.append(
            f"  -> Signals matched:             {f.signals} "
            f"({match_rate:.1f}% match rate)"
        )
    else:
        lines.append(f"  -> Signals matched:             {f.signals}")

    lines.append(f"  -> Decisions made:              {f.decisions}")
    for reason, n in f.reject_reasons.items():
        pct = (n / f.decisions) * 100 if f.decisions else 0
        lines.append(
            f"       -- reject  {reason:<26} {n} ({pct:.1f}% of decisions)"
        )
    if f.decisions_execute:
        lines.append(
            f"       -- execute (paper, tracer): {f.decisions_execute}"
        )
    lines.append(f"  -> Trades executed:             {f.trades}")
    if f.signals > 0:
        llm_pct = (f.llm_reached / f.signals) * 100
        lines.append(
            f"  -> LLM-reached:                 {f.llm_reached} "
            f"({llm_pct:.1f}% of signals)"
        )
    else:
        lines.append(f"  -> LLM-reached:                 {f.llm_reached}")
    if f.llm_label_counts:
        lines.append("  -> LLM verdict distribution:")
        for label, n in f.llm_label_counts.items():
            conf = f.llm_label_avg_conf.get(label, 0.0)
            lines.append(f"       {label}: {n}  avg_conf={conf:.2f}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Per-pair CSV
# ---------------------------------------------------------------------------

# Columns mandated by the handoff. `gate_history_snippet` is a JSON blob
# truncated to keep cell widths sane for spreadsheet inspection.
CSV_COLUMNS = [
    "tracer_id",
    "article_id",
    "article_title",
    "market_id",
    "market_question",
    "similarity",
    "freshness",
    "llm_label",
    "llm_direction",
    "llm_confidence",
    "decision_outcome",
    "reject_reason",
    "gate_history_snippet",
]


def write_csv(conn: sqlite3.Connection, prefix: str, path: Path) -> int:
    """Write one row per (article, market) pair to `path`. Returns row count."""
    pat = f"{prefix}%"
    path.parent.mkdir(parents=True, exist_ok=True)
    # The connection may have been opened by a caller that didn't set
    # row_factory; force it here so we can address columns by name.
    conn.row_factory = sqlite3.Row
    rows = _rows(
        conn,
        """
        SELECT
          s.tracer_id        AS tracer_id,
          s.article_id       AS article_id,
          a.title            AS article_title,
          s.market_id        AS market_id,
          COALESCE(m.question, '') AS market_question,
          s.similarity       AS similarity,
          COALESCE(s.freshness, '') AS freshness,
          COALESCE(p.direction, '') AS llm_direction,
          COALESCE(p.direction, '') AS llm_label,
          COALESCE(p.confidence, 0) AS llm_confidence,
          COALESCE(d.outcome, '')   AS decision_outcome,
          COALESCE(d.reason, '')    AS reject_reason,
          COALESCE(d.gate_history, '') AS gate_history
        FROM signals s
        LEFT JOIN articles a ON a.id = s.article_id
        LEFT JOIN markets  m ON m.condition_id = s.market_id
        LEFT JOIN decisions d ON d.signal_id = s.id
        LEFT JOIN predictions p ON p.signal_id = s.id
        WHERE s.is_tracer = 1 AND s.tracer_id LIKE ?
        ORDER BY s.id
        """,
        (pat,),
    )
    n = 0
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for r in rows:
            snippet = str(r["gate_history"])[:240] if r["gate_history"] else ""
            writer.writerow(
                {
                    "tracer_id": r["tracer_id"],
                    "article_id": r["article_id"],
                    "article_title": r["article_title"],
                    "market_id": r["market_id"],
                    "market_question": r["market_question"],
                    "similarity": r["similarity"],
                    "freshness": r["freshness"],
                    "llm_label": r["llm_label"],
                    "llm_direction": r["llm_direction"],
                    "llm_confidence": r["llm_confidence"],
                    "decision_outcome": r["decision_outcome"],
                    "reject_reason": r["reject_reason"],
                    "gate_history_snippet": snippet,
                }
            )
            n += 1
    return n


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m scripts.tracer.audit_burst",
        description=(
            "Tracer-burst funnel report + per-pair CSV. "
            "Read-only; pair with cleanup.py to wipe the burst."
        ),
    )
    parser.add_argument(
        "--tracer-id-prefix",
        required=True,
        help="Prefix used in the matching fire.py burst.",
    )
    parser.add_argument(
        "--db",
        default="data/irene.db",
        help="DB path (default data/irene.db — the live bot's DB).",
    )
    parser.add_argument(
        "--csv-out",
        default=None,
        help=(
            "Override the per-pair CSV path. Default "
            "`data/dashboard/plots/tracer_burst_<prefix>.csv`."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON instead of the human funnel.",
    )
    args = parser.parse_args(argv)

    try:
        conn = open_readonly(args.db)
    except Exception as exc:  # noqa: BLE001
        print(f"audit_burst: cannot open {args.db}: {exc}", file=sys.stderr)
        return 2

    try:
        f = funnel(conn, args.tracer_id_prefix)
        csv_path = (
            Path(args.csv_out)
            if args.csv_out
            else Path("data/dashboard/plots") / f"tracer_burst_{args.tracer_id_prefix}.csv"
        )
        n_rows = write_csv(conn, args.tracer_id_prefix, csv_path)
    finally:
        conn.close()

    if args.json:
        out = {
            "prefix": f.prefix,
            "window_start": f.window_start,
            "window_end": f.window_end,
            "articles": f.articles,
            "signals": f.signals,
            "decisions": f.decisions,
            "decisions_execute": f.decisions_execute,
            "decisions_reject": f.decisions_reject,
            "reject_reasons": f.reject_reasons,
            "trades": f.trades,
            "llm_reached": f.llm_reached,
            "llm_label_counts": f.llm_label_counts,
            "llm_label_avg_conf": f.llm_label_avg_conf,
            "csv_rows": n_rows,
            "csv_path": str(csv_path),
        }
        print(json.dumps(out, indent=2, default=str))
    else:
        print(format_funnel(f))
        print(f"\nPer-pair CSV: {csv_path} ({n_rows} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
