"""Per-stage throughput in the last hour: rows/min for each pipeline stage."""

from __future__ import annotations

from ..db_helpers import scalar
from ..io import _now

FILENAME = "throughput.txt"


async def render(dash) -> str:
    # Cycle 4: wrap timestamp cols with datetime() so the comparison
    # parses both sides instead of doing a lex match (see main.py).
    # Add cycle filter for tables that have cycle_id (per-cycle rows);
    # leave clusters/entity_resolutions unscoped (shared across cycles).
    cycle_id = dash.current_cycle_id
    cs = f" AND cycle_id = {int(cycle_id)}" if cycle_id is not None else ""
    stages = [
        ("ingest  (articles)        ",
         f"SELECT COUNT(*) FROM articles WHERE datetime(ingested_at) >= datetime('now','-1 hour'){cs}"),
        ("dedupe  (clusters)        ",
         "SELECT COUNT(*) FROM article_clusters WHERE datetime(created_at) >= datetime('now','-1 hour')"),
        ("match   (signals)         ",
         f"SELECT COUNT(*) FROM signals WHERE datetime(created_at) >= datetime('now','-1 hour'){cs}"),
        ("extract (runs)            ",
         f"SELECT COUNT(*) FROM extraction_runs WHERE datetime(created_at) >= datetime('now','-1 hour'){cs}"),
        ("resolve (entity_resolutions)",
         "SELECT COUNT(*) FROM entity_resolutions WHERE datetime(created_at) >= datetime('now','-1 hour')"),
        ("decide  (decisions)       ",
         f"SELECT COUNT(*) FROM decisions WHERE datetime(created_at) >= datetime('now','-1 hour'){cs}"),
        ("trade   (trades)          ",
         f"SELECT COUNT(*) FROM trades WHERE datetime(decided_at) >= datetime('now','-1 hour'){cs}"),
        ("close   (trade_closures)  ",
         f"SELECT COUNT(*) FROM trade_closures WHERE datetime(closed_at) >= datetime('now','-1 hour'){cs}"),
    ]
    lines = [
        f"# irene throughput ({_now().isoformat()})",
        "  rows/min per stage, last 1h",
        "",
        f"  {'STAGE':<32} {'/HR':>6} {'/MIN':>6}",
        "  " + "─" * 50,
    ]
    for label, sql in stages:
        n = await scalar(dash.db, sql)
        per_min = n / 60.0
        lines.append(f"  {label:<32} {n:>6d} {per_min:>6.2f}")
    return "\n".join(lines) + "\n"
