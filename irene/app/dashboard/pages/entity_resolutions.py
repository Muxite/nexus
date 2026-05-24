"""Shadow-DKG resolver stats: counts by resolution_kind, top canonicals."""

from __future__ import annotations

from ..io import _now

FILENAME = "entity_resolutions.txt"


async def render(dash) -> str:
    lines = [
        f"# irene entity resolutions (P3 shadow) ({_now().isoformat()})",
        "",
    ]
    cur = await dash.db.conn.execute(
        """
        SELECT resolution_kind, COUNT(*) AS n
        FROM entity_resolutions
        GROUP BY resolution_kind
        ORDER BY n DESC
        """
    )
    kind_rows = await cur.fetchall()
    await cur.close()

    if not kind_rows:
        lines.append("  (no entity resolutions yet)")
        return "\n".join(lines) + "\n"

    lines.append("  Resolutions by kind:")
    lines.append(f"  {'KIND':<10} {'N':>6}")
    lines.append("  " + "─" * 18)
    total = 0
    for r in kind_rows:
        n = int(r["n"])
        total += n
        lines.append(f"  {r['resolution_kind']:<10} {n:>6d}")
    lines.append(f"  {'TOTAL':<10} {total:>6d}")
    lines.append("")

    # Top 10 canonical entities by how many surface forms resolve to them
    cur = await dash.db.conn.execute(
        """
        SELECT e.canonical_name, e.entity_type, COUNT(*) AS n
        FROM entity_resolutions r
        JOIN entities e ON e.id = r.resolved_to_id
        WHERE r.resolution_kind != 'self'
        GROUP BY r.resolved_to_id
        ORDER BY n DESC
        LIMIT 10
        """
    )
    top_rows = await cur.fetchall()
    await cur.close()
    if top_rows:
        lines.append("  Top canonical entities by inbound resolutions (non-self):")
        lines.append(f"  {'CANONICAL':<40} {'TYPE':<10} {'N':>4}")
        lines.append("  " + "─" * 58)
        for r in top_rows:
            lines.append(
                f"  {(r['canonical_name'] or '?')[:40]:<40} "
                f"{(r['entity_type'] or '?'):<10} {int(r['n']):>4d}"
            )
    return "\n".join(lines) + "\n"
