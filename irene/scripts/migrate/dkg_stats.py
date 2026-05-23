"""
## Specification

**Purpose.** Read the `irene_dkg_migration.db` produced by
`scripts/migrate/dkg_bulk_sample.py` and emit a comprehensive stats
report covering everything we collected — throughput, cost, latency,
fact distributions, entity/relation breakdowns, time-of-day coverage.

This is the "what did we learn from the bulk run" surface. Output goes
to stdout (text) or a markdown file.

**Public interface.**
- CLI: `python -m scripts.migrate.dkg_stats --db <path> [--batch-id <id>] [--format text|md]`
- `run_inproc(db_path, batch_id=None) -> StatsReport` — programmatic
  entry returning a dict.

**Non-goals.**
- Doesn't modify the migration DB. Read-only.
- Doesn't run any LLM calls.
"""

from __future__ import annotations

import argparse
import collections
import json
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


def _percentiles(values: list[float]) -> dict:
    if not values:
        return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "min": 0.0, "max": 0.0, "n": 0}
    s = sorted(values)

    def _q(p: float) -> float:
        idx = max(0, min(len(s) - 1, int(round((len(s) - 1) * p))))
        return s[idx]

    return {
        "p50": _q(0.50),
        "p95": _q(0.95),
        "p99": _q(0.99),
        "min": s[0],
        "max": s[-1],
        "n": len(s),
    }


def _open(db_path: str) -> sqlite3.Connection:
    if not Path(db_path).exists():
        raise FileNotFoundError(f"migration DB not found: {db_path}")
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _batch_meta(conn, batch_id: Optional[str]) -> dict:
    if batch_id:
        row = conn.execute(
            "SELECT * FROM dkg_migration_batches WHERE batch_id = ?",
            (batch_id,),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT * FROM dkg_migration_batches ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
    if row is None:
        raise SystemExit("no batch found")
    return dict(row)


def _status_counts(conn, batch_id: str) -> dict:
    rows = conn.execute(
        "SELECT status, COUNT(*) AS n FROM dkg_migration_runs "
        "WHERE batch_id = ? GROUP BY status",
        (batch_id,),
    ).fetchall()
    return {r["status"]: r["n"] for r in rows}


def _latency_overall(conn, batch_id: str) -> dict:
    rows = conn.execute(
        "SELECT latency_ms FROM dkg_migration_runs WHERE batch_id = ?",
        (batch_id,),
    ).fetchall()
    return _percentiles([r["latency_ms"] for r in rows if r["latency_ms"] is not None])


def _latency_by_status(conn, batch_id: str) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for status_row in conn.execute(
        "SELECT DISTINCT status FROM dkg_migration_runs WHERE batch_id = ?",
        (batch_id,),
    ).fetchall():
        status = status_row["status"]
        vals = conn.execute(
            "SELECT latency_ms FROM dkg_migration_runs WHERE batch_id=? AND status=?",
            (batch_id, status),
        ).fetchall()
        out[status] = _percentiles([r["latency_ms"] for r in vals if r["latency_ms"] is not None])
    return out


def _cost_summary(conn, batch_id: str) -> dict:
    row = conn.execute(
        """
        SELECT
          SUM(cost_usd)    AS total_cost,
          SUM(tokens_in)   AS tokens_in,
          SUM(tokens_out)  AS tokens_out,
          AVG(cost_usd)    AS avg_cost,
          COUNT(*)         AS n
        FROM dkg_migration_runs
        WHERE batch_id = ? AND status = 'ok'
        """,
        (batch_id,),
    ).fetchone()
    return {
        "total_cost_usd_ok": float(row["total_cost"] or 0.0),
        "tokens_in_total": int(row["tokens_in"] or 0),
        "tokens_out_total": int(row["tokens_out"] or 0),
        "avg_cost_usd": float(row["avg_cost"] or 0.0),
        "n_ok": int(row["n"] or 0),
    }


def _fact_count_distribution(conn, batch_id: str) -> dict[str, int]:
    """Histogram of `fact_count` across `ok` rows."""
    rows = conn.execute(
        "SELECT fact_count, COUNT(*) AS n FROM dkg_migration_runs "
        "WHERE batch_id=? AND status='ok' GROUP BY fact_count ORDER BY fact_count",
        (batch_id,),
    ).fetchall()
    return {str(r["fact_count"]): r["n"] for r in rows}


def _top_relations(conn, batch_id: str, top_n: int = 20) -> list[tuple[str, int]]:
    """Pull every (subj, relation, obj) out of parsed_facts_json and count."""
    counter: collections.Counter = collections.Counter()
    for row in conn.execute(
        "SELECT parsed_facts_json FROM dkg_migration_runs "
        "WHERE batch_id=? AND status='ok' AND parsed_facts_json IS NOT NULL",
        (batch_id,),
    ).fetchall():
        try:
            facts = json.loads(row["parsed_facts_json"])
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(facts, list):
            continue
        for f in facts:
            if not isinstance(f, dict):
                continue
            rel = f.get("relation")
            if rel:
                counter[str(rel)] += 1
    return counter.most_common(top_n)


def _top_entities(conn, batch_id: str, top_n: int = 30) -> dict:
    """Most-mentioned entities split by subject vs object position."""
    subj_counter: collections.Counter = collections.Counter()
    obj_counter: collections.Counter = collections.Counter()
    type_counter: collections.Counter = collections.Counter()
    for row in conn.execute(
        "SELECT parsed_facts_json FROM dkg_migration_runs "
        "WHERE batch_id=? AND status='ok' AND parsed_facts_json IS NOT NULL",
        (batch_id,),
    ).fetchall():
        try:
            facts = json.loads(row["parsed_facts_json"])
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(facts, list):
            continue
        for f in facts:
            if not isinstance(f, dict):
                continue
            subj = f.get("subj")
            if subj:
                subj_counter[str(subj)] += 1
            obj = f.get("obj")
            if obj:
                obj_counter[str(obj)] += 1
            t = f.get("subj_type")
            if t:
                type_counter[str(t)] += 1
    return {
        "top_subjects": subj_counter.most_common(top_n),
        "top_objects": obj_counter.most_common(top_n),
        "subj_type_counts": dict(type_counter),
    }


def _time_distribution(conn, batch_id: str) -> dict[str, int]:
    """Bucket the sampled headlines' detected_at by date — verifies random
    sampling didn't accidentally cluster on a window."""
    rows = conn.execute(
        """
        SELECT substr(seulgi_detected_at, 1, 10) AS day, COUNT(*) AS n
        FROM dkg_migration_runs
        WHERE batch_id=? AND seulgi_detected_at IS NOT NULL
        GROUP BY day ORDER BY day
        """,
        (batch_id,),
    ).fetchall()
    return {r["day"]: r["n"] for r in rows}


def _site_distribution(conn, batch_id: str, top_n: int = 15) -> list[tuple[str, int]]:
    rows = conn.execute(
        """
        SELECT seulgi_site_url AS site, COUNT(*) AS n
        FROM dkg_migration_runs
        WHERE batch_id=? GROUP BY site ORDER BY n DESC LIMIT ?
        """,
        (batch_id, top_n),
    ).fetchall()
    return [(r["site"] or "NULL", r["n"]) for r in rows]


def _freshness_distribution(conn, batch_id: str) -> dict[str, int]:
    rows = conn.execute(
        """
        SELECT seulgi_freshness AS f, COUNT(*) AS n
        FROM dkg_migration_runs WHERE batch_id=? GROUP BY f
        """,
        (batch_id,),
    ).fetchall()
    return {(r["f"] or "NULL"): r["n"] for r in rows}


def _empty_vs_facty(conn, batch_id: str) -> dict:
    """How many 'ok' rows produced 0 facts vs >0? The 0-fact rate is a
    proxy for headline-quality (sports/gossip/etc. yield no ontology
    facts; political/financial yield ≥1)."""
    rows = conn.execute(
        """
        SELECT
          SUM(CASE WHEN fact_count = 0 THEN 1 ELSE 0 END) AS empty_n,
          SUM(CASE WHEN fact_count > 0 THEN 1 ELSE 0 END) AS facty_n,
          AVG(CAST(fact_count AS REAL)) AS avg_fact_count
        FROM dkg_migration_runs WHERE batch_id=? AND status='ok'
        """,
        (batch_id,),
    ).fetchone()
    total = (row_["empty_n"] or 0) + (row_["facty_n"] or 0) if (row_ := rows) else 0
    if total == 0:
        return {"empty_pct": 0.0, "facty_pct": 0.0, "avg_fact_count": 0.0,
                "empty_n": 0, "facty_n": 0}
    return {
        "empty_n": rows["empty_n"] or 0,
        "facty_n": rows["facty_n"] or 0,
        "empty_pct": 100.0 * (rows["empty_n"] or 0) / total,
        "facty_pct": 100.0 * (rows["facty_n"] or 0) / total,
        "avg_fact_count": float(rows["avg_fact_count"] or 0.0),
    }


def run_inproc(db_path: str, batch_id: Optional[str] = None) -> dict:
    conn = _open(db_path)
    try:
        meta = _batch_meta(conn, batch_id)
        b_id = meta["batch_id"]
        return {
            "batch": meta,
            "status_counts": _status_counts(conn, b_id),
            "latency_overall_ms": _latency_overall(conn, b_id),
            "latency_by_status_ms": _latency_by_status(conn, b_id),
            "cost": _cost_summary(conn, b_id),
            "facts_per_article": _fact_count_distribution(conn, b_id),
            "empty_vs_facty": _empty_vs_facty(conn, b_id),
            "top_relations": _top_relations(conn, b_id),
            "entities": _top_entities(conn, b_id),
            "time_distribution": _time_distribution(conn, b_id),
            "freshness_distribution": _freshness_distribution(conn, b_id),
            "site_distribution_top15": _site_distribution(conn, b_id, 15),
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Text renderer
# ---------------------------------------------------------------------------


def _render_text(stats: dict) -> str:
    b = stats["batch"]
    lines = []
    lines.append(f"# DKG bulk-extraction stats — batch {b['batch_id']}")
    lines.append("")
    lines.append("## Run metadata")
    lines.append(f"  started:        {b['started_at']}")
    lines.append(f"  finished:       {b.get('finished_at') or '(running)'}")
    lines.append(f"  model:          {b['model']}")
    lines.append(f"  prompt_version: {b['prompt_version']}")
    lines.append(f"  target:         {b['target_count']:,}")
    lines.append(f"  completed:      {b['completed_count']:,}")
    lines.append(f"  concurrency:    {b['concurrency']}")
    lines.append(f"  budget cap:     ${b['budget_cap_usd']:.2f}")
    lines.append(f"  total cost:     ${b.get('total_cost_usd', 0.0):.4f}")
    lines.append(f"  aborted reason: {b.get('aborted_reason') or '—'}")
    lines.append("")
    lines.append("## Status counts")
    sc = stats["status_counts"]
    total = sum(sc.values()) or 1
    for k, v in sorted(sc.items(), key=lambda kv: -kv[1]):
        lines.append(f"  {k:<18} {v:>8,}  ({100.0 * v / total:.1f}%)")
    lines.append("")
    lines.append("## Latency (ms) — overall")
    p = stats["latency_overall_ms"]
    lines.append(f"  p50 = {p['p50']:>7.0f}")
    lines.append(f"  p95 = {p['p95']:>7.0f}")
    lines.append(f"  p99 = {p['p99']:>7.0f}")
    lines.append(f"  min = {p['min']:>7.0f}")
    lines.append(f"  max = {p['max']:>7.0f}")
    lines.append(f"  n   = {p['n']:>7d}")
    lines.append("")
    lines.append("## Latency by status (ms)")
    for status, p in stats["latency_by_status_ms"].items():
        lines.append(f"  {status:<18} p50={p['p50']:>6.0f} p95={p['p95']:>6.0f} p99={p['p99']:>6.0f} n={p['n']:>7d}")
    lines.append("")
    lines.append("## Cost + tokens")
    c = stats["cost"]
    lines.append(f"  total cost (ok-only): ${c['total_cost_usd_ok']:.4f}")
    lines.append(f"  avg cost / call:      ${c['avg_cost_usd']:.6f}")
    lines.append(f"  total tokens_in:      {c['tokens_in_total']:>10,}")
    lines.append(f"  total tokens_out:     {c['tokens_out_total']:>10,}")
    if c["n_ok"]:
        lines.append(f"  avg tokens_in/call:   {c['tokens_in_total'] / c['n_ok']:>10.1f}")
        lines.append(f"  avg tokens_out/call:  {c['tokens_out_total'] / c['n_ok']:>10.1f}")
    lines.append("")
    lines.append("## Empty vs facty articles (ok-only)")
    e = stats["empty_vs_facty"]
    lines.append(f"  empty (0 facts):   {e['empty_n']:>7,}  ({e['empty_pct']:.1f}%)")
    lines.append(f"  facty (≥1 fact):   {e['facty_n']:>7,}  ({e['facty_pct']:.1f}%)")
    lines.append(f"  avg facts/article: {e['avg_fact_count']:.2f}")
    lines.append("")
    lines.append("## Facts-per-article distribution")
    for k, v in sorted(stats["facts_per_article"].items(), key=lambda x: int(x[0])):
        lines.append(f"  {k:<3} facts  →  {v:>7,} articles")
    lines.append("")
    lines.append("## Top relations")
    for rel, n in stats["top_relations"]:
        lines.append(f"  {rel:<20} {n:>7,}")
    lines.append("")
    lines.append("## Subject entity-type breakdown")
    for t, n in sorted(stats["entities"]["subj_type_counts"].items(), key=lambda kv: -kv[1]):
        lines.append(f"  {t:<20} {n:>7,}")
    lines.append("")
    lines.append("## Top 30 subjects")
    for ent, n in stats["entities"]["top_subjects"]:
        lines.append(f"  {ent[:45]:<45}  {n:>7,}")
    lines.append("")
    lines.append("## Top 30 objects")
    for ent, n in stats["entities"]["top_objects"]:
        lines.append(f"  {ent[:45]:<45}  {n:>7,}")
    lines.append("")
    lines.append("## Time-of-day coverage (sample dates)")
    for day, n in sorted(stats["time_distribution"].items()):
        lines.append(f"  {day}  {n:>7,}")
    lines.append("")
    lines.append("## Freshness label distribution (of seulgi's signals.freshness_label)")
    for f, n in sorted(stats["freshness_distribution"].items(), key=lambda kv: -kv[1]):
        lines.append(f"  {f:<20} {n:>7,}")
    lines.append("")
    lines.append("## Top 15 sites (of seulgi's signals.site_url)")
    for site, n in stats["site_distribution_top15"]:
        lines.append(f"  {site[:60]:<60}  {n:>7,}")
    return "\n".join(lines) + "\n"


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--db",
        default="/home/muk/projects/jennie/irene/data/irene_dkg_migration.db",
    )
    parser.add_argument("--batch-id", default=None,
                        help="Defaults to the most recent batch.")
    parser.add_argument("--format", choices=("text", "md", "json"), default="text")
    args = parser.parse_args(argv)
    try:
        stats = run_inproc(args.db, args.batch_id)
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    if args.format == "json":
        sys.stdout.write(json.dumps(stats, indent=2, default=str))
    else:
        sys.stdout.write(_render_text(stats))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
