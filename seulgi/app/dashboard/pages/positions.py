from __future__ import annotations

from ..io import _header

FILENAME = "positions_open.txt"
TITLE = "OPEN POSITIONS"


async def render(dash) -> str:
    lines = [_header("OPEN POSITIONS")]
    if not dash._open_positions:
        lines.append("  (none)")
    else:
        lines.append(
            f"  {'#':<3} {'DIRECTION':<10} {'SIZE':>8}  "
            f"{'ENTRY':>6}  {'CURRENT':>7}  {'PnL':>8}  "
            f"{'AGE':>6}  CONDITION"
        )
        lines.append("  " + "-" * 90)
        for i, p in enumerate(dash._open_positions, 1):
            sign = "+" if p.unrealized_pnl >= 0 else ""
            lines.append(
                f"  {i:<3} {p.direction:<10} ${p.size_usdc:>7.2f}  "
                f"{p.entry_price:>6.3f}  {p.current_price:>7.3f}  "
                f"{sign}${abs(p.unrealized_pnl):>7.2f}  "
                f"{p.age_hours:>5.1f}h  {p.condition_id[:20]}"
            )
            lines.append(f"       {p.question[:95]}")
    return "\n".join(lines) + "\n"
