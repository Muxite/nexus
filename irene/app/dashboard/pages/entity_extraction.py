"""Shadow-DKG extraction stats: runs, entities, facts in the last hour."""

from __future__ import annotations

from ..db_helpers import scalar
from ..io import _now

FILENAME = "entity_extraction.txt"


async def render(dash) -> str:
    lines = [
        f"# irene entity extraction (P2 shadow) ({_now().isoformat()})",
        "  last 1h",
        "",
    ]
    # Run-level counts by status — scoped to current cycle
    cycle_id = dash.current_cycle_id
    cs = f"AND cycle_id = {int(cycle_id)}" if cycle_id is not None else ""
    cur = await dash.db.conn.execute(
        f"""
        SELECT status, COUNT(*) AS n,
               SUM(COALESCE(fact_count, 0))   AS facts,
               SUM(COALESCE(entity_count, 0)) AS ents
        FROM extraction_runs
        WHERE datetime(created_at) >= datetime('now','-1 hour') {cs}
        GROUP BY status
        ORDER BY n DESC
        """
    )
    rows = await cur.fetchall()
    await cur.close()

    if not rows:
        lines.append("  (no extraction runs in last hour)")
    else:
        lines.append(f"  {'STATUS':<16} {'RUNS':>6} {'FACTS':>7} {'ENTS':>6}")
        lines.append("  " + "─" * 40)
        for r in rows:
            lines.append(
                f"  {r['status']:<16} {int(r['n']):>6d} "
                f"{int(r['facts'] or 0):>7d} {int(r['ents'] or 0):>6d}"
            )

    lines.append("")
    # Lifetime entity tier
    n_ent = await scalar(dash.db, "SELECT COUNT(*) FROM entities")
    n_alias = await scalar(dash.db, "SELECT COUNT(*) FROM entity_aliases")
    n_facts_active = await scalar(
        dash.db, "SELECT COUNT(*) FROM facts WHERE is_active = 1"
    )
    lines.append("  Lifetime:")
    lines.append(f"    entities (canonical)       : {n_ent}")
    lines.append(f"    entity_aliases             : {n_alias}")
    lines.append(f"    facts (active)             : {n_facts_active}")

    # Entity-type breakdown (top 7)
    cur = await dash.db.conn.execute(
        """
        SELECT entity_type, COUNT(*) AS n
        FROM entities
        GROUP BY entity_type
        ORDER BY n DESC
        LIMIT 7
        """
    )
    type_rows = await cur.fetchall()
    await cur.close()
    if type_rows:
        lines.append("")
        lines.append("  Entities by type:")
        for r in type_rows:
            lines.append(f"    {r['entity_type']:<16} {int(r['n']):>6d}")
    return "\n".join(lines) + "\n"
