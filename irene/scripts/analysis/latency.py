"""latency — per-stage timing p50/p95/p99 in milliseconds.

This is the most important diagnostic per `docs/philosophy.md` — irene's
edge is time-to-trade, so we surface end-to-end latency + each adjacent
stage diff.
"""

from __future__ import annotations

from scripts.analysis._common import add_common_args, parse_since
from scripts.analysis._format import Section


def add_parser(p) -> None:
    add_common_args(p)


_PANELS = [
    ("ingest → dedupe", """
        SELECT (julianday(c.created_at) - julianday(a.ingested_at)) * 86400000.0 AS ms
        FROM article_clusters c
        JOIN article_cluster_members m ON m.cluster_id = c.id
        JOIN articles a ON a.id = m.article_id
        WHERE c.created_at >= ?
    """),
    ("ingest → match", """
        SELECT (julianday(s.created_at) - julianday(a.ingested_at)) * 86400000.0 AS ms
        FROM signals s JOIN articles a ON a.id = s.article_id
        WHERE s.created_at >= ?
    """),
    ("match → decision", """
        SELECT (julianday(d.created_at) - julianday(s.created_at)) * 86400000.0 AS ms
        FROM decisions d JOIN signals s ON s.id = d.signal_id
        WHERE d.created_at >= ?
    """),
    ("ingest → trade", """
        SELECT (julianday(t.decided_at) - julianday(a.ingested_at)) * 86400000.0 AS ms
        FROM trades t
        JOIN signals s ON s.id = t.signal_id
        JOIN articles a ON a.id = s.article_id
        WHERE t.decided_at >= ?
    """),
    ("news age at trade-time", """
        SELECT (julianday(t.decided_at) - julianday(a.published_at)) * 86400000.0 AS ms
        FROM trades t
        JOIN signals s ON s.id = t.signal_id
        JOIN articles a ON a.id = s.article_id
        WHERE t.decided_at >= ?
          AND a.published_at IS NOT NULL
    """),
]


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    idx = max(0, min(len(s) - 1, int(round((len(s) - 1) * p))))
    return s[idx]


def run(args, conn) -> Section:
    cutoff = parse_since(args.since) or "1970-01-01T00:00:00"
    rows = []
    for label, sql in _PANELS:
        cur = conn.execute(sql, (cutoff,))
        ms = [float(r["ms"]) for r in cur.fetchall() if r["ms"] is not None and r["ms"] >= 0]
        rows.append([
            label,
            round(_percentile(ms, 0.50), 0),
            round(_percentile(ms, 0.95), 0),
            round(_percentile(ms, 0.99), 0),
            len(ms),
        ])
    return Section(
        title=f"per-stage latency (since {cutoff})",
        headers=["stage diff", "p50 ms", "p95 ms", "p99 ms", "n"],
        rows=rows,
        notes=[
            "irene's edge is speed. Watch the 'ingest → trade' end-to-end "
            "and 'news age at trade-time' rows — those are the cash money.",
        ],
    )
