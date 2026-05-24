"""Current cycle context: id, opened_at, articles/signals/trades so far."""

from __future__ import annotations

from ..io import _now

FILENAME = "cycle_summary.txt"


async def render(dash) -> str:
    cur = await dash.db.conn.execute(
        "SELECT id, name, opened_at, closed_at FROM cycles "
        "WHERE closed_at IS NULL ORDER BY id DESC LIMIT 1"
    )
    row = await cur.fetchone()
    await cur.close()
    lines = [
        f"# irene cycle summary ({_now().isoformat()})",
        "",
    ]
    if row is None:
        lines.append("  (no open cycle)")
        return "\n".join(lines) + "\n"

    cycle_id = row["id"]
    lines += [
        f"  cycle id     : {cycle_id}",
        f"  cycle name   : {row['name']}",
        f"  opened at    : {row['opened_at']}",
        "",
    ]
    for label, sql in (
        ("articles ingested", "SELECT COUNT(*) FROM articles WHERE cycle_id = ?"),
        ("signals matched ", "SELECT COUNT(*) FROM signals WHERE cycle_id = ?"),
        ("decisions made  ", "SELECT COUNT(*) FROM decisions WHERE cycle_id = ?"),
        ("trades decided  ", "SELECT COUNT(*) FROM trades WHERE cycle_id = ?"),
        ("trades executed ",
         "SELECT COUNT(*) FROM decisions WHERE cycle_id = ? AND outcome = 'execute'"),
        ("trade closures  ",
         "SELECT COUNT(*) FROM trade_closures tc "
         "JOIN trades t ON t.id = tc.trade_id WHERE t.cycle_id = ?"),
    ):
        cur = await dash.db.conn.execute(sql, (cycle_id,))
        row = await cur.fetchone()
        await cur.close()
        n = int(row[0] or 0) if row else 0
        lines.append(f"  {label} : {n}")
    return "\n".join(lines) + "\n"
