"""DB helpers reused across irene dashboard pages."""

from __future__ import annotations


async def scalar(db, sql: str) -> int:
    cur = await db.conn.execute(sql)
    row = await cur.fetchone()
    await cur.close()
    if row is None:
        return 0
    return int(row[0] or 0)


async def fetch_ms(db, sql: str) -> list[float]:
    """Run SQL whose first column is named `ms`; return non-negative samples."""
    cur = await db.conn.execute(sql)
    rows = await cur.fetchall()
    await cur.close()
    return [float(r["ms"]) for r in rows if r["ms"] is not None and r["ms"] >= 0]


async def end_to_end_latency_ms(db, cycle_id=None) -> list[float]:
    cs = f"AND t.cycle_id = {int(cycle_id)}" if cycle_id is not None else ""
    return await fetch_ms(
        db,
        f"""
        SELECT (julianday(t.decided_at) - julianday(a.ingested_at)) * 86400000.0 AS ms
        FROM trades t
        JOIN signals s ON s.id = t.signal_id
        JOIN articles a ON a.id = s.article_id
        WHERE datetime(t.decided_at) >= datetime('now','-1 hour') {cs}
        """,
    )


def latency_panel_sqls(cycle_id=None) -> list[tuple[str, str]]:
    """Each row: (label, SELECT that returns a column named ms).

    The five canonical adjacent-stage diffs:
      ingest → dedupe → match → trade → close
    """
    # Cycle filters for tables that have cycle_id. article_clusters
    # is shared across cycles, so the dedupe filter only scopes
    # the JOIN side via the article-cluster member's source.
    cs_s = f"AND s.cycle_id = {int(cycle_id)}" if cycle_id is not None else ""
    cs_t = f"AND t.cycle_id = {int(cycle_id)}" if cycle_id is not None else ""
    cs_d = f"AND d.cycle_id = {int(cycle_id)}" if cycle_id is not None else ""
    return [
        (
            "ingest → dedupe",
            """
            SELECT (julianday(c.created_at) - julianday(a.ingested_at)) * 86400000.0 AS ms
            FROM article_clusters c
            JOIN article_cluster_members m ON m.cluster_id = c.id
            JOIN articles a ON a.id = m.article_id
            WHERE datetime(c.created_at) >= datetime('now','-1 hour')
            """,
        ),
        (
            "ingest → match",
            f"""
            SELECT (julianday(s.created_at) - julianday(a.ingested_at)) * 86400000.0 AS ms
            FROM signals s
            JOIN articles a ON a.id = s.article_id
            WHERE datetime(s.created_at) >= datetime('now','-1 hour') {cs_s}
            """,
        ),
        (
            "ingest → trade",
            f"""
            SELECT (julianday(t.decided_at) - julianday(a.ingested_at)) * 86400000.0 AS ms
            FROM trades t
            JOIN signals s ON s.id = t.signal_id
            JOIN articles a ON a.id = s.article_id
            WHERE datetime(t.decided_at) >= datetime('now','-1 hour') {cs_t}
            """,
        ),
        (
            "match → trade (gate cascade)",
            f"""
            SELECT (julianday(d.created_at) - julianday(s.created_at)) * 86400000.0 AS ms
            FROM decisions d
            JOIN signals s ON s.id = d.signal_id
            WHERE datetime(d.created_at) >= datetime('now','-1 hour') {cs_d}
            """,
        ),
        (
            "news age at trade-time",
            f"""
            SELECT (julianday(t.decided_at) - julianday(a.published_at)) * 86400000.0 AS ms
            FROM trades t
            JOIN signals s ON s.id = t.signal_id
            JOIN articles a ON a.id = s.article_id
            WHERE datetime(t.decided_at) >= datetime('now','-1 hour') {cs_t}
              AND a.published_at IS NOT NULL
            """,
        ),
    ]
