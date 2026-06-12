"""Per-cluster PnL rollup for reports (read-only).

Cycle 28 companion to `app/dashboard/pages/pnl_clusters.py`, but across
ALL cycles — the dashboard page filters to the live cycle. Future
retrospectives should quote these numbers, never per-trade stats: the
2026-06-12 retro showed 845 trades collapse to ~52 clusters.

Usage:
    python3 scripts/analysis/pnl_clusters.py [--db data/irene.db] [--top 20]
"""
from __future__ import annotations

import argparse
import sqlite3


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default="data/irene.db")
    ap.add_argument("--top", type=int, default=20)
    args = ap.parse_args()

    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    # The cycle-28 `trades.cluster_id` column lands at container boot; a
    # pre-migration snapshot resolves clusters via signals only.
    has_col = any(
        r[1] == "cluster_id" for r in conn.execute("PRAGMA table_info(trades)")
    )
    cluster_expr = (
        "COALESCE(t.cluster_id, s.cluster_id)" if has_col else "s.cluster_id"
    )
    try:
        rows = conn.execute(
            f"""
            SELECT {cluster_expr} AS cluster_id,
                   COUNT(*)                              AS trades,
                   COUNT(c.id)                           AS closed,
                   SUM(t.size_usdc)                      AS deployed,
                   SUM(c.realized_pnl_usdc)              AS pnl
            FROM trades t
            LEFT JOIN signals s ON s.id = t.signal_id
            LEFT JOIN trade_closures c ON c.trade_id = t.id
            WHERE t.is_tracer = 0
            GROUP BY 1
            ORDER BY ABS(COALESCE(pnl, 0)) DESC
            """
        ).fetchall()
    finally:
        pass
    n_trades = sum(r["trades"] for r in rows)
    n_clusters = len(rows)
    settled = [r for r in rows if r["closed"] > 0 and r["pnl"] is not None]
    winners = sum(1 for r in settled if r["pnl"] > 0)
    total_pnl = sum(r["pnl"] for r in settled)
    print(f"effective N: {n_trades} trades / {n_clusters} clusters "
          f"= {n_trades / max(1, n_clusters):.1f} trades per bet")
    print(f"cluster win rate: {winners}/{len(settled)} "
          f"({winners / max(1, len(settled)):.1%})   total PnL {total_pnl:+.2f}")
    print(f"\n{'cluster':<9}{'trades':<8}{'closed':<8}{'deployed$':<11}{'pnl$':<10}")
    for r in rows[: args.top]:
        cid = r["cluster_id"] if r["cluster_id"] is not None else "(none)"
        pnl = f"{r['pnl']:+.2f}" if r["pnl"] is not None else "-"
        print(f"{str(cid):<9}{r['trades']:<8}{r['closed']:<8}"
              f"{(r['deployed'] or 0):<11.2f}{pnl:<10}")
    conn.close()


if __name__ == "__main__":
    main()
