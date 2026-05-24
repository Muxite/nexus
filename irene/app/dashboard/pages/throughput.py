"""Per-stage throughput in the last hour: rows/min for each pipeline stage."""

from __future__ import annotations

from ..db_helpers import scalar
from ..io import _now

FILENAME = "throughput.txt"


async def render(dash) -> str:
    stages = [
        ("ingest  (articles)        ",
         "SELECT COUNT(*) FROM articles WHERE ingested_at >= datetime('now','-1 hour')"),
        ("dedupe  (clusters)        ",
         "SELECT COUNT(*) FROM article_clusters WHERE created_at >= datetime('now','-1 hour')"),
        ("match   (signals)         ",
         "SELECT COUNT(*) FROM signals WHERE created_at >= datetime('now','-1 hour')"),
        ("extract (runs)            ",
         "SELECT COUNT(*) FROM extraction_runs WHERE created_at >= datetime('now','-1 hour')"),
        ("resolve (entity_resolutions)",
         "SELECT COUNT(*) FROM entity_resolutions WHERE created_at >= datetime('now','-1 hour')"),
        ("decide  (decisions)       ",
         "SELECT COUNT(*) FROM decisions WHERE created_at >= datetime('now','-1 hour')"),
        ("trade   (trades)          ",
         "SELECT COUNT(*) FROM trades WHERE decided_at >= datetime('now','-1 hour')"),
        ("close   (trade_closures)  ",
         "SELECT COUNT(*) FROM trade_closures WHERE closed_at >= datetime('now','-1 hour')"),
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
