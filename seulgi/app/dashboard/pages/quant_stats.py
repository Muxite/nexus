"""Prediction accuracy + signal latency (A1.5 culled to two blocks)."""

from __future__ import annotations

from ..io import _header

FILENAME = "quant_stats.txt"
TITLE = "QUANT STATS"


async def render(dash) -> str:
    lines = [_header("QUANT STATS  — prediction accuracy + signal latency")]
    if not dash._quant_stats:
        lines.append("  (no data yet — run for at least 1 hour to collect price impacts)")
        return "\n".join(lines) + "\n"

    impact = dash._quant_stats.get("impact", [])
    impact_24h = dash._quant_stats.get("impact_24h", [])
    lat = dash._quant_stats.get("latency", {})

    # PREDICTION ACCURACY (lifetime + 24h rolling)
    # IC / calibration / source / alpha-decay tiles were removed in A1.5.
    lines.append("")
    lines.append("  PREDICTION ACCURACY  (did price move in predicted direction within 5m?)")
    lines.append(
        f"  {'DIR':<4} {'N':>5} {'PREC':>7}  "
        f"{'AVG Δ5m':>9} {'AVG|Δ5m|':>9}   {'24h N':>6} {'24h PREC':>9}"
    )
    lines.append("  " + "─" * 66)
    by_dir_24h = {r["direction"]: r for r in impact_24h}
    for row in impact:
        n = row["n"] or 0
        hits = row["hits"] or 0
        prec = hits / n if n else 0.0
        avg_d = row["avg_delta_t5m"] or 0.0
        avg_abs = row["avg_abs_delta_t5m"] or 0.0
        r24 = by_dir_24h.get(row["direction"], {})
        n24 = r24.get("n") or 0
        prec24 = (r24.get("hits") or 0) / n24 if n24 else 0.0
        lines.append(
            f"  {row['direction']:<4} {n:>5} {prec:>6.1%}  "
            f"{avg_d:>+8.4f} {avg_abs:>8.4f}   {n24:>6} {prec24:>8.1%}"
        )

    # SIGNAL LATENCY
    lines.append("")
    lines.append("  SIGNAL LATENCY  (headline detection → trade decision, includes AI call)")
    n_lat = lat.get("n") or 0
    if n_lat:
        lines.append(f"    Samples : {n_lat}")
        lines.append(f"    Average : {lat.get('avg_ms') or 0:.0f} ms")
        lines.append(f"    Min     : {lat.get('min_ms') or 0:.0f} ms")
        lines.append(f"    Max     : {lat.get('max_ms') or 0:.0f} ms")
    else:
        lines.append("    (no latency data yet)")

    return "\n".join(lines) + "\n"
