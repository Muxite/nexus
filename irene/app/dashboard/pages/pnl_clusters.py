"""Realised PnL per ARTICLE CLUSTER — the unit of risk.

Cycle 28: the 2026-06-12 retrospective showed the wallet's 845 trades
collapse to ~52 article clusters; per-trade stats overstate the sample
by ~16x and per-trade win rates are meaningless under that correlation.
This page is the canonical PnL view: one row per cluster, win rate
counted per CLUSTER (sum of its closed PnL > 0), and the effective-N
headline (trades / clusters) so nobody quotes per-trade numbers again.

Cluster resolution: `trades.cluster_id` (cycle 28 denormalized column),
falling back to `signals.cluster_id` for legacy rows. Trades with no
cluster at all are bucketed as `(none)` — DKG-path rows land there.
Tracer trades are excluded.
"""

from __future__ import annotations

from ..io import _now

FILENAME = "pnl_clusters.txt"


async def render(dash) -> str:
    cycle_id = dash.current_cycle_id
    cycle_sql = f"AND t.cycle_id = {int(cycle_id)}" if cycle_id is not None else ""
    lines = [
        f"# irene pnl per cluster ({_now().isoformat()})",
        f"  one row per article cluster; closed PnL only; non-tracer; cycle_id={cycle_id}",
        "",
    ]

    cur = await dash.db.conn.execute(
        f"""
        SELECT COALESCE(t.cluster_id, s.cluster_id) AS cluster_id,
               t.id                AS trade_id,
               t.size_usdc         AS size_usdc,
               c.realized_pnl_usdc AS pnl
        FROM trades t
        LEFT JOIN signals s ON s.id = t.signal_id
        LEFT JOIN trade_closures c ON c.trade_id = t.id
        WHERE t.is_tracer = 0 {cycle_sql}
        """
    )
    rows = await cur.fetchall()
    await cur.close()

    if not rows:
        lines.append("  (no trades yet)")
        return "\n".join(lines) + "\n"

    by_cluster: dict[object, dict] = {}
    for r in rows:
        key = r["cluster_id"] if r["cluster_id"] is not None else "(none)"
        b = by_cluster.setdefault(
            key, {"trades": 0, "closed": 0, "open": 0, "size": 0.0, "pnl": 0.0}
        )
        b["trades"] += 1
        b["size"] += float(r["size_usdc"] or 0.0)
        if r["pnl"] is not None:
            b["closed"] += 1
            b["pnl"] += float(r["pnl"])
        else:
            b["open"] += 1

    n_trades = len(rows)
    n_clusters = len(by_cluster)
    settled = {k: b for k, b in by_cluster.items() if b["closed"] > 0}
    winners = sum(1 for b in settled.values() if b["pnl"] > 0)
    lines += [
        f"  EFFECTIVE N: {n_trades} trades across {n_clusters} clusters "
        f"({n_trades / max(1, n_clusters):.1f} trades/cluster — the cluster"
        f" is the bet)",
        f"  cluster win rate: {winners}/{len(settled)} settled clusters"
        + (f" ({winners / len(settled):.1%})" if settled else ""),
        f"  total closed PnL: {sum(b['pnl'] for b in settled.values()):+.2f}",
        "",
        f"  {'cluster':<9} {'trades':<7} {'closed':<7} {'open':<5} "
        f"{'deployed$':<10} {'pnl$':<9}",
    ]
    # Largest absolute PnL first; (none) bucket last.
    def _sort_key(item):
        key, b = item
        return (key == "(none)", -abs(b["pnl"]))

    for key, b in sorted(by_cluster.items(), key=_sort_key)[:30]:
        pnl_str = f"{b['pnl']:+9.2f}" if b["closed"] else "        -"
        lines.append(
            f"  {str(key):<9} {b['trades']:<7d} {b['closed']:<7d} "
            f"{b['open']:<5d} {b['size']:<10.2f} {pnl_str}"
        )
    if n_clusters > 30:
        lines.append(f"  … {n_clusters - 30} more clusters")
    return "\n".join(lines) + "\n"
