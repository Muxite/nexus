"""Aggregate classified rows by feature bucket → reveals where signal lives/dies."""
from __future__ import annotations

from collections import defaultdict

from _common import ANALYSIS_DIR, all_classified, classify_missed_alpha, open_db


SIM_BUCKETS = [(0.35, 0.45), (0.45, 0.55), (0.55, 0.65), (0.65, 0.75), (0.75, 1.01)]
CONF_BUCKETS = [(0.5, 0.55), (0.55, 0.65), (0.65, 0.75), (0.75, 0.85), (0.85, 0.95), (0.95, 1.01)]
P0_BUCKETS = [(0.0, 0.05), (0.05, 0.10), (0.10, 0.30), (0.30, 0.70), (0.70, 0.90), (0.90, 0.95), (0.95, 1.01)]


def bucket_of(value, ranges):
    if value is None:
        return None
    for lo, hi in ranges:
        if lo <= value < hi:
            return f"[{lo:.2f},{hi:.2f})"
    return None


def aggregate(rows, key_fn):
    agg: dict[str, dict] = defaultdict(lambda: {
        "n": 0, "no_signal": 0, "wrong_dir": 0, "captured": 0, "too_slow": 0,
        "sum_signed_d5m": 0.0,
    })
    for r in rows:
        k = key_fn(r)
        if k is None:
            continue
        b = classify_missed_alpha(r["direction"], r["delta_pre_60s"], r["delta_t5m"])
        if b is None:
            continue
        cell = agg[k]
        cell["n"] += 1
        cell[b] += 1
        sign = 1 if r["direction"] == "YES" else -1
        cell["sum_signed_d5m"] += (r["delta_t5m"] or 0) * sign
    return agg


def render(title: str, agg: dict, sort_key=None) -> list[str]:
    lines = [f"\n── {title} ─────────────────────────────────────────────", ""]
    lines.append(
        f"  {'bucket':<16} {'n':>5} {'no_sig%':>8} {'wrong%':>7} {'captured%':>10} "
        f"{'too_slow%':>10} {'mean_signed_Δ5m':>16}"
    )
    keys = sorted(agg.keys(), key=sort_key) if sort_key else sorted(agg.keys())
    for k in keys:
        c = agg[k]
        n = c["n"] or 1
        lines.append(
            f"  {k:<16} {c['n']:>5} "
            f"{100*c['no_signal']/n:>7.1f}% "
            f"{100*c['wrong_dir']/n:>6.1f}% "
            f"{100*c['captured']/n:>9.1f}% "
            f"{100*c['too_slow']/n:>9.1f}% "
            f"{c['sum_signed_d5m']/n:>+15.5f}"
        )
    return lines


def main() -> None:
    conn = open_db()
    rows = all_classified(conn)

    out: list[str] = []
    out.append("FEATURE BREAKDOWN — price_impacts rows with pre60s and t5m present")
    out.append(f"n total = {len(rows)}")
    out.append("")
    out.append("Buckets:")
    out.append("  no_signal  — |pre60|<0.005 & |post5|<0.005   (market never moved)")
    out.append("  wrong_dir  — pre or post moved AGAINST predicted direction")
    out.append("  captured   — pre quiet, post moved WITH us (real alpha)")
    out.append("  too_slow   — pre already moved WITH us (latency-limited)")
    out.append("  mean_signed_Δ5m  — average Δ5m signed by direction (+ = correct on avg)")

    out += render("By SIMILARITY", aggregate(rows, lambda r: bucket_of(r["similarity"], SIM_BUCKETS)))
    out += render("By AI CONFIDENCE", aggregate(rows, lambda r: bucket_of(r["ai_confidence"], CONF_BUCKETS)))
    out += render("By DIRECTION", aggregate(rows, lambda r: r["direction"] if r["direction"] in ("YES", "NO") else None))
    out += render("By price_t0", aggregate(rows, lambda r: bucket_of(r["price_t0"], P0_BUCKETS)))

    # Combined: high-confidence × direction (where calibration goes wrong)
    out += render(
        "By CONF × DIRECTION",
        aggregate(rows, lambda r: f"{r['direction']}@{bucket_of(r['ai_confidence'], CONF_BUCKETS)}"
                  if r['direction'] in ('YES', 'NO') and bucket_of(r['ai_confidence'], CONF_BUCKETS)
                  else None),
    )

    # Combined: sim × price_t0 — does high-sim + mid-price = real alpha?
    out += render(
        "By SIM × price_t0",
        aggregate(rows, lambda r: f"sim{bucket_of(r['similarity'], SIM_BUCKETS)}|p0{bucket_of(r['price_t0'], P0_BUCKETS)}"
                  if bucket_of(r['similarity'], SIM_BUCKETS) and bucket_of(r['price_t0'], P0_BUCKETS) else None),
    )

    # Source breakdown
    out += render("By SOURCE", aggregate(rows, lambda r: r["source"] or "news"))

    ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    path = ANALYSIS_DIR / "feature_breakdown.txt"
    path.write_text("\n".join(out) + "\n")
    print(f"Wrote {path}")


if __name__ == "__main__":
    main()
