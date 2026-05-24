"""Per-variant pivot: trades, wins, losses, hit-rate, sum PnL.

Runs a fresh `get_variant_stats()` on the writer connection each tick
(no cache scope). The §C cleanup step moves this onto the heavy-cache
to remove the per-tick writer hit.
"""

from __future__ import annotations

import logging

from ..io import _header

logger = logging.getLogger(__name__)

FILENAME = "variant_compare.txt"
TITLE = "VARIANT COMPARISON"


async def render(dash) -> str:
    if dash._db is None:
        return ""
    try:
        stats = await dash._db.get_variant_stats()
    except Exception as exc:  # noqa: BLE001
        logger.warning("[dash] variant_stats query failed: %s", exc)
        return ""
    lines = [
        _header("VARIANT COMPARISON  (all engines × variants, sorted by sum PnL)"),
        "  Each row is one strategy variant. Variants of the same engine share the AI",
        "  call but differ in entry/exit rules — compare to find which rule set wins.",
        "",
    ]
    if not stats:
        lines.append("  (no executed trades yet)")
    else:
        lines.append(
            f"  {'VARIANT':<28}  {'TRADES':>6} {'OPEN':>5} {'WINS':>5} {'LOSS':>5}  "
            f"{'HIT%':>6}  {'SUM PnL':>9}  {'AVG PnL':>8}  {'AVG CONF':>8}  "
            f"{'AVG EDGE':>9}  {'STAKED':>9}  {'INV':>4}"
        )
        lines.append("  " + "─" * 130)
        for r in stats:
            closed = (r["n_wins"] or 0) + (r["n_losses"] or 0)
            hit = (r["n_wins"] / closed * 100.0) if closed else 0.0
            lines.append(
                f"  {r['variant']:<28}  {r['n_trades']:>6} {r['n_open']:>5} "
                f"{r['n_wins']:>5} {r['n_losses']:>5}  "
                f"{hit:>5.1f}%  {r['sum_pnl']:>+9.2f}  {r['avg_pnl']:>+8.3f}  "
                f"{r['avg_conf']:>7.1%}  {r['avg_edge']:>+9.4f}  "
                f"{r['total_staked']:>9.2f}  {r['n_inverted']:>4}"
            )
    return "\n".join(lines) + "\n"
