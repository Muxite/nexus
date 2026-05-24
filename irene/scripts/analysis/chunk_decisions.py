"""Split the decisions ledger into bite-sized chunks for analysis.

The dashboard's `decisions.txt` page grows unbounded as the soak
accumulates audit data. For prompt-tuning we want to read decisions
in chunks small enough to hold in our head — 25 decisions × ~10 lines
per decision = ~250 lines per chunk, comfortable to scan in one pass.

`CHUNK_SIZE` (25) is the standard examination unit going forward.
Don't change it without updating downstream analysis docs that quote
"N decisions per chunk".

Usage:
    cd /home/muk/projects/jennie/irene
    PYTHONPATH=. python3 -m scripts.analysis.chunk_decisions \\
        --output-dir data/decisions_chunks

    # Limit + custom size for ad-hoc work:
    PYTHONPATH=. python3 -m scripts.analysis.chunk_decisions \\
        --chunk-size 50 --limit 200
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

CHUNK_SIZE = 25
DEFAULT_DB = "data/irene.db"
DEFAULT_OUT = "data/decisions_chunks"


def _truncate(s: str | None, n: int) -> str:
    if not s:
        return ""
    s = s.replace("\n", " ").strip()
    return s if len(s) <= n else s[: n - 1] + "…"


def _fmt_value(v) -> str:
    if v is None:
        return "-"
    if isinstance(v, float):
        return f"{v:.3f}"
    if isinstance(v, list):
        return "{" + ", ".join(str(x) for x in v) + "}"
    return str(v)


def _render_gate_history(gate_history_json: str | None) -> list[str]:
    if not gate_history_json:
        return ["  process: (no gate history)"]
    try:
        gates = json.loads(gate_history_json)
    except (TypeError, ValueError):
        return ["  process: (gate_history unparseable)"]
    if not gates:
        return ["  process: (no gates evaluated)"]
    out = ["  process:"]
    rejected_at = None
    for i, g in enumerate(gates, start=1):
        name = g.get("gate", "?")
        outcome = g.get("outcome", "?")
        value = _fmt_value(g.get("value"))
        threshold = _fmt_value(g.get("threshold"))
        marker = "✓" if outcome == "ok" else "✗"
        op = "≥" if (outcome == "ok" and "_floor" in name) else "vs"
        if name == "freshness":
            line = f"    {i}. {name:<16} {marker} {outcome:<7}  {value} ∈ {threshold}"
        else:
            line = f"    {i}. {name:<16} {marker} {outcome:<7}  {value} {op} {threshold}"
        out.append(line)
        if outcome == "reject":
            rejected_at = name
            break
    out.append(f"       └─ stopped at `{rejected_at}`" if rejected_at else "       └─ EXECUTED")
    return out


def _render_decision(r: sqlite3.Row) -> list[str]:
    ts = (r["created_at"] or "")[11:19]
    path = r["path"] or "?"
    outcome = r["outcome"] or "?"
    if outcome == "execute":
        head = "execute              "
        extras = ""
        if r["edge"] is not None and r["size_usdc"] is not None:
            extras = f"  edge={r['edge']:+.3f}  size=${r['size_usdc']:.2f}"
    else:
        head = f"reject:{(r['reason'] or '?')[:20]:<13}"
        extras = ""
    lines = [f"[{ts}]  {path:<3}  {head}{extras}"]

    title = _truncate(r["article_title"], 100)
    if title and len(title) < 50 and r["article_summary"]:
        title = f'{title}  · {_truncate(r["article_summary"], 60)}'
    lines.append(f'  art: "{title}"' if title else "  art: (missing)")
    mkt = _truncate(r["market_question"], 100)
    lines.append(f'  mkt: "{mkt}"' if mkt else "  mkt: (missing)")
    raw = (r["raw_response"] or "").replace("\n", " ").strip()
    if len(raw) > 60:
        raw = raw[:59] + "…"
    if raw or r["direction"]:
        cached_str = "yes" if r["cached"] else "no"
        lines.append(
            f"  ai:  raw={raw!r}  cached={cached_str}  prompt_v={r['prompt_version'] or '?'}"
        )
    lines.extend(_render_gate_history(r["gate_history"]))
    lines.append("")
    return lines


def fetch_decisions(db_path: str, limit: int | None) -> list[sqlite3.Row]:
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    sql = """
        SELECT
            d.id, d.created_at, d.outcome, d.reason, d.gate_history,
            COALESCE(t.decision_path, 'mvp') AS path,
            s.similarity, p.direction, p.confidence, p.cached,
            p.prompt_version, p.raw_response,
            t.edge, t.size_usdc,
            a.title    AS article_title,
            a.summary  AS article_summary,
            m.question AS market_question
        FROM decisions d
        LEFT JOIN signals     s ON s.id = d.signal_id
        LEFT JOIN predictions p ON p.id = d.prediction_id
        LEFT JOIN trades      t ON t.decision_id = d.id
        LEFT JOIN articles    a ON a.id = s.article_id
        LEFT JOIN markets     m ON m.condition_id = s.market_id
        WHERE d.is_tracer = 0
        ORDER BY d.created_at DESC
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    return list(db.execute(sql))


def write_chunks(rows: list[sqlite3.Row], out_dir: Path, chunk_size: int) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    # Wipe any prior chunks so stale files don't survive across runs.
    for old in out_dir.glob("chunk_*.txt"):
        old.unlink()
    written: list[Path] = []
    total = len(rows)
    n_chunks = (total + chunk_size - 1) // chunk_size if total else 0
    for idx, start in enumerate(range(0, total, chunk_size), start=1):
        end = min(start + chunk_size, total)
        chunk = rows[start:end]
        path = out_dir / f"chunk_{idx:03d}.txt"
        lines = [
            f"# irene decisions — chunk {idx}/{n_chunks}",
            f"  decisions {start + 1}..{end} of {total} (newest first)",
            f"  standard chunk size = {CHUNK_SIZE}",
            "",
        ]
        for r in chunk:
            lines.extend(_render_decision(r))
        path.write_text("\n".join(lines))
        written.append(path)
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--output-dir", default=DEFAULT_OUT)
    parser.add_argument("--chunk-size", type=int, default=CHUNK_SIZE)
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args(argv)

    rows = fetch_decisions(args.db, args.limit)
    if not rows:
        print("no decisions found", file=sys.stderr)
        return 1
    written = write_chunks(rows, Path(args.output_dir), args.chunk_size)
    print(f"wrote {len(written)} chunks ({len(rows)} decisions, size={args.chunk_size}) → {args.output_dir}")
    for p in written:
        sz = p.stat().st_size
        print(f"  {p.name}  {sz:>6d}B")
    return 0


if __name__ == "__main__":
    sys.exit(main())
