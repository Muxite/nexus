"""drag — open-position drag + capital lock-up + worst held losers."""
from __future__ import annotations

from scripts.analysis._common import add_common_args
from scripts.analysis._format import Section


def add_parser(p) -> None:
    add_common_args(p)
    p.add_argument("--top", type=int, default=10,
                   help="number of worst-by-unrealized positions to list")


def run(args, conn) -> Section:
    # ── headline drag summary
    summary_row = conn.execute("""
        SELECT COUNT(*) n,
            SUM(CASE WHEN unrealized_pnl > 0 THEN 1 ELSE 0 END) AS up,
            SUM(CASE WHEN unrealized_pnl <= 0 THEN 1 ELSE 0 END) AS dn,
            ROUND(SUM(unrealized_pnl), 2) AS sum_un,
            ROUND(SUM(size_usdc), 2)      AS staked,
            ROUND(MIN(unrealized_pnl), 2) AS worst,
            ROUND(MAX(unrealized_pnl), 2) AS best
        FROM positions
    """).fetchone()
    n = summary_row["n"] or 0
    underwater_pct = (summary_row["dn"] / n * 100.0) if n else None

    head_rows = [
        ["open_n",          n],
        ["up",              summary_row["up"] or 0],
        ["underwater",      summary_row["dn"] or 0],
        ["underwater_pct",  f"{underwater_pct:.1f}%" if underwater_pct is not None else "—"],
        ["sum_unrealized",  summary_row["sum_un"]],
        ["staked",          summary_row["staked"]],
        ["best",            summary_row["best"]],
        ["worst",           summary_row["worst"]],
    ]

    # ── age buckets
    age_rows = list(conn.execute("""
        SELECT
            CASE WHEN (JULIANDAY('now') - JULIANDAY(opened_at)) * 24 < 1 THEN '<1h'
                 WHEN (JULIANDAY('now') - JULIANDAY(opened_at)) * 24 < 6 THEN '1-6h'
                 WHEN (JULIANDAY('now') - JULIANDAY(opened_at)) * 24 < 24 THEN '6-24h'
                 ELSE '>24h' END AS age,
            COUNT(*) AS n,
            ROUND(SUM(size_usdc), 2)      AS staked,
            ROUND(SUM(unrealized_pnl), 2) AS sum_un
        FROM positions
        GROUP BY age
        ORDER BY MIN((JULIANDAY('now') - JULIANDAY(opened_at)) * 24)
    """).fetchall())

    # ── worst N positions
    top = args.top
    worst_rows = list(conn.execute("""
        SELECT engine_name, ROUND(entry_price, 3) AS ep,
            ROUND(unrealized_pnl, 2) AS un, size_usdc,
            ROUND((JULIANDAY('now') - JULIANDAY(opened_at)) * 24, 1) AS age_h,
            substr(question, 1, 70) AS q
        FROM positions
        ORDER BY unrealized_pnl ASC
        LIMIT ?
    """, (top,)).fetchall())

    # Compose into one section as a stacked table.
    rows = [["── summary ──", ""]]
    rows += head_rows
    rows.append(["── age buckets ──", ""])
    for r in age_rows:
        rows.append([f"{r['age']} (n={r['n']}, staked=${r['staked']:.0f})",
                     f"unreal {r['sum_un']:+.2f}"])
    rows.append([f"── worst {top} ──", ""])
    for r in worst_rows:
        rows.append([
            f"{r['un']:+7.2f}  age={r['age_h']:>5.1f}h  $"
            f"{r['size_usdc']:>5.0f}  entry={r['ep']:.3f}  {r['engine_name'][:30]}",
            r['q'],
        ])

    return Section(
        title="drag — open-position capital lock-up + worst held losers",
        headers=["metric/position", "value"],
        rows=rows,
        aligns=["l", "l"],
    )
