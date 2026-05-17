"""missed — skip × price_impacts join (would-have-won attribution).

Honest about its limits: when sample n < 30 for a bucket the row is
flagged in notes. Move magnitude is the actual 5-minute post-headline
mid delta from `price_impacts.delta_t5m`.
"""
from __future__ import annotations

from scripts.analysis._common import add_common_args, parse_since
from scripts.analysis._format import Section


def add_parser(p) -> None:
    add_common_args(p)
    p.add_argument("--min-move", type=float, default=0.02,
                   help="absolute 5m move threshold to count as 'moved' (default: 0.02)")


def run(args, conn) -> Section:
    cutoff = parse_since(args.since)
    params: list = [args.min_move]
    where_extra = ""
    if cutoff:
        where_extra += " AND t.decided_at >= ?"
        params.append(cutoff)

    sql = f"""
        SELECT t.skip_reason AS reason,
            COUNT(*)                                        AS n,
            ROUND(AVG(ABS(p.delta_t5m)), 4)                 AS avg_move,
            ROUND(MAX(ABS(p.delta_t5m)), 3)                 AS max_move,
            SUM(CASE WHEN (t.direction='YES' AND p.delta_t5m > 0)
                      OR (t.direction='NO'  AND p.delta_t5m < 0)
                      THEN 1 ELSE 0 END)                    AS right_dir_n
        FROM trades t
        JOIN price_impacts p
          ON p.headline_id = t.headline_id
         AND p.condition_id = t.condition_id
        WHERE t.size_usdc = 0
          AND t.skip_reason IS NOT NULL
          AND p.delta_t5m IS NOT NULL
          AND ABS(p.delta_t5m) >= ?
          AND t.direction IN ('YES','NO')
          {where_extra}
        GROUP BY reason
        ORDER BY n DESC
        LIMIT 20
    """
    rows = []
    for r in conn.execute(sql, params).fetchall():
        rd_pct = (r["right_dir_n"] / r["n"] * 100.0) if r["n"] else 0
        rows.append([
            (r["reason"] or "")[:50], r["n"],
            f"{rd_pct:.0f}%", r["avg_move"], r["max_move"],
        ])
    # Sample-size guard
    total_sample = sum(r[1] for r in rows)
    notes = []
    if total_sample < 30:
        notes.append(
            f"low sample (n={total_sample}); attributions are suggestive only. "
            f"price_impacts has limited backfill — see impact_tracker logs."
        )
    return Section(
        title=f"missed opportunities — skipped trades whose market moved ≥{args.min_move}",
        headers=["skip_reason", "n", "right_dir%", "avg|Δ5m|", "max|Δ5m|"],
        rows=rows,
        notes=notes,
    )
