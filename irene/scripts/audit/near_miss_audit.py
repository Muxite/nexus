"""Audit "near-miss" decisions — leaning YES/NO but cut from final trading.

Cycle 24 handoff F. Produces a markdown report and a sortable CSV so
the operator can sweep the band of LLM-decisive signals that the trade
gate cascade refused, and decide whether to (a) lower
`cfg.trade.min_confidence`, (b) tweak the direction prompt, or (c)
relax a downstream microstructure / sizing gate.

Two sections:
  A. direction_lowconf rejects in [audit_floor, trade_floor).
  B. confidence >= trade_floor rejects killed by a DOWNSTREAM gate.

Usage:
    python -m scripts.audit.near_miss_audit \\
        [--hours 24] [--min-confidence 0.35] [--db PATH] \\
        [--reports-dir PATH] [--csv-path PATH]
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_DB = "/home/muk/projects/jennie/irene/data/irene.db"
DEFAULT_REPORTS_DIR = "/home/muk/projects/jennie/irene/reports"
DEFAULT_CSV_PATH = (
    "/home/muk/projects/jennie/irene/data/dashboard/plots/"
    "near_miss_breakdown.csv"
)

# Mirror the dashboard page's exclusion set so the CLI and the live
# panel agree on what counts as "downstream."
SECTION_B_EXCLUDED_REASONS = (
    "similarity_floor",
    "freshness_stale",
    "entity_overlap",
    "entity_mismatch",
    "direction_neutral",
    "direction_lowconf",
    "article_missing",
    "market_missing",
    "market_closed",
    "market_inactive",
    "market_near_expiry",
    "execution_disabled",
)


def _excluded_sql() -> str:
    return "(" + ",".join(f"'{r}'" for r in SECTION_B_EXCLUDED_REASONS) + ")"


SQL_SECTION_A = """
SELECT
    d.created_at        AS decision_at,
    d.reason            AS reason,
    COALESCE(t.decision_path, 'mvp') AS decision_path,
    p.direction         AS direction,
    p.confidence        AS confidence,
    p.label             AS label,
    p.raw_response      AS raw_response,
    d.gate_history      AS gate_history,
    a.id                AS article_id,
    a.title             AS article_title,
    m.condition_id      AS market_id,
    m.question          AS market_question
FROM decisions d
JOIN predictions p ON p.id = d.prediction_id
LEFT JOIN signals  s ON s.id = d.signal_id
LEFT JOIN articles a ON a.id = s.article_id
LEFT JOIN markets  m ON m.condition_id = s.market_id
LEFT JOIN trades   t ON t.decision_id = d.id
WHERE d.outcome   = 'reject'
  AND d.reason    = 'direction_lowconf'
  AND d.is_tracer = 0
  AND p.direction IN ('YES','NO')
  AND p.confidence >= ?
  AND datetime(d.created_at) >= datetime('now', ?)
ORDER BY p.confidence DESC
"""


SQL_SECTION_B = f"""
SELECT
    d.created_at        AS decision_at,
    d.reason            AS reason,
    COALESCE(t.decision_path, 'mvp') AS decision_path,
    p.direction         AS direction,
    p.confidence        AS confidence,
    p.label             AS label,
    p.raw_response      AS raw_response,
    d.gate_history      AS gate_history,
    a.id                AS article_id,
    a.title             AS article_title,
    m.condition_id      AS market_id,
    m.question          AS market_question
FROM decisions d
JOIN predictions p ON p.id = d.prediction_id
LEFT JOIN signals  s ON s.id = d.signal_id
LEFT JOIN articles a ON a.id = s.article_id
LEFT JOIN markets  m ON m.condition_id = s.market_id
LEFT JOIN trades   t ON t.decision_id = d.id
WHERE d.outcome    = 'reject'
  AND d.reason NOT IN {_excluded_sql()}
  AND d.is_tracer  = 0
  AND p.direction IN ('YES','NO')
  AND p.confidence >= ?
  AND datetime(d.created_at) >= datetime('now', ?)
ORDER BY p.confidence DESC
"""


def _gate_snippet(gate_history_json: str | None) -> str:
    """Compact single-line summary of the gate that killed the trade."""
    if not gate_history_json:
        return ""
    try:
        gates = json.loads(gate_history_json)
    except (TypeError, ValueError):
        return ""
    for g in gates:
        if g.get("outcome") == "reject":
            name = g.get("gate", "?")
            value = g.get("value")
            threshold = g.get("threshold")
            return f"{name}: value={value!r} threshold={threshold!r}"
    return ""


def _fmt_row_for_stdout(r: sqlite3.Row, *, include_reason: bool) -> str:
    parts = [
        r["decision_at"][:19] if r["decision_at"] else "?",
        f"{r['decision_path']:<3}",
        f"{r['direction'] or '?':<3}",
        f"conf={r['confidence']:.2f}" if r["confidence"] is not None else "conf=?",
        f"label={r['label'] or '-'}",
    ]
    if include_reason:
        parts.append(f"reason={r['reason']}")
    head = "  ".join(parts)
    snippet = _gate_snippet(r["gate_history"]) if include_reason else ""
    title = (r["article_title"] or "")[:80]
    question = (r["market_question"] or "")[:80]
    body = f'\n    art: "{title}"\n    mkt: "{question}"'
    if snippet:
        body += f"\n    gate: {snippet}"
    return head + body


def _row_dict(r: sqlite3.Row) -> dict:
    return {
        "decision_at": r["decision_at"],
        "decision_path": r["decision_path"],
        "reason": r["reason"],
        "direction": r["direction"],
        "confidence": r["confidence"],
        "label": r["label"],
        "article_id": r["article_id"],
        "article_title": (r["article_title"] or "").replace("\n", " "),
        "market_id": r["market_id"],
        "market_question": (r["market_question"] or "").replace("\n", " "),
        "gate_history_snippet": _gate_snippet(r["gate_history"]),
    }


def _summarise(rows: list[sqlite3.Row]) -> dict:
    if not rows:
        return {"n": 0, "by_reason": {}, "by_direction": {}, "conf": (0.0, 0.0)}
    confs = [r["confidence"] for r in rows if r["confidence"] is not None]
    return {
        "n": len(rows),
        "by_reason": dict(Counter(r["reason"] for r in rows).most_common()),
        "by_direction": dict(Counter(r["direction"] for r in rows).most_common()),
        "conf": (min(confs), max(confs)) if confs else (0.0, 0.0),
    }


def _write_report(
    report_path: Path,
    *,
    hours: float,
    audit_floor: float,
    trade_floor: float,
    rows_a: list[sqlite3.Row],
    rows_b: list[sqlite3.Row],
) -> None:
    sum_a = _summarise(rows_a)
    sum_b = _summarise(rows_b)
    when = datetime.now(timezone.utc).isoformat(timespec="seconds")
    lines = [
        f"# Near-miss audit — {when}",
        "",
        f"- window: last {hours:g}h",
        f"- audit_floor: {audit_floor:.2f}",
        f"- trade_floor (cfg.trade.min_confidence): {trade_floor:.2f}",
        "",
        "## Section A — direction_lowconf in near-miss band",
        "",
        (
            f"- n = {sum_a['n']}  conf range ="
            f" [{sum_a['conf'][0]:.2f}, {sum_a['conf'][1]:.2f}]"
        ),
        f"- by direction: {sum_a['by_direction'] or '(empty)'}",
        "",
        "## Section B — decisive YES/NO killed downstream",
        "",
        (
            f"- n = {sum_b['n']}  conf range ="
            f" [{sum_b['conf'][0]:.2f}, {sum_b['conf'][1]:.2f}]"
        ),
        f"- by reason: {sum_b['by_reason'] or '(empty)'}",
        f"- by direction: {sum_b['by_direction'] or '(empty)'}",
        "",
    ]
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines) + "\n")


def _write_csv(csv_path: Path, rows: list[sqlite3.Row]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "decision_at",
        "decision_path",
        "reason",
        "direction",
        "confidence",
        "label",
        "article_id",
        "article_title",
        "market_id",
        "market_question",
        "gate_history_snippet",
    ]
    with csv_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(_row_dict(r))


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", default=DEFAULT_DB)
    p.add_argument("--hours", type=float, default=24.0)
    p.add_argument(
        "--min-confidence",
        type=float,
        default=0.35,
        help=(
            "Audit-band floor for Section A (rows below this aren't"
            " 'leaning'). Default matches cfg.trade.audit_confidence_floor."
        ),
    )
    p.add_argument(
        "--trade-floor",
        type=float,
        default=0.55,
        help=(
            "Trade-execute floor for Section B (decisive predictions are"
            " >= this). Default matches cfg.trade.min_confidence."
        ),
    )
    p.add_argument("--reports-dir", default=DEFAULT_REPORTS_DIR)
    p.add_argument("--csv-path", default=DEFAULT_CSV_PATH)
    args = p.parse_args(argv)

    window = f"-{args.hours} hours"
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    rows_a = conn.execute(
        SQL_SECTION_A, (args.min_confidence, window),
    ).fetchall()
    rows_b = conn.execute(
        SQL_SECTION_B, (args.trade_floor, window),
    ).fetchall()
    conn.close()

    print(f"near-miss audit — window=last {args.hours:g}h")
    print(
        f"  audit_floor={args.min_confidence:.2f}"
        f"  trade_floor={args.trade_floor:.2f}"
    )
    print()

    sum_a = _summarise(rows_a)
    print(
        f"== Section A: direction_lowconf in"
        f" [{args.min_confidence:.2f}, {args.trade_floor:.2f}) =="
    )
    print(f"  total: {sum_a['n']}  by direction: {sum_a['by_direction']}")
    for r in rows_a[:10]:
        print(_fmt_row_for_stdout(r, include_reason=False))
    if len(rows_a) > 10:
        print(f"  … and {len(rows_a) - 10} more (see CSV)")
    print()

    sum_b = _summarise(rows_b)
    print(
        f"== Section B: decisive YES/NO killed downstream"
        f" (conf >= {args.trade_floor:.2f}) =="
    )
    print(f"  total: {sum_b['n']}  by reason: {sum_b['by_reason']}")
    for r in rows_b[:10]:
        print(_fmt_row_for_stdout(r, include_reason=True))
    if len(rows_b) > 10:
        print(f"  … and {len(rows_b) - 10} more (see CSV)")
    print()

    today = datetime.now(timezone.utc).date().isoformat()
    report_path = Path(args.reports_dir) / f"{today}-near-miss-audit.md"
    csv_path = Path(args.csv_path)
    _write_report(
        report_path,
        hours=args.hours,
        audit_floor=args.min_confidence,
        trade_floor=args.trade_floor,
        rows_a=rows_a,
        rows_b=rows_b,
    )
    _write_csv(csv_path, list(rows_a) + list(rows_b))
    print(f"wrote report: {report_path}")
    print(f"wrote CSV:    {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
