"""For each wrong_dir sample, see what every engine decided on the same headline×market."""
from __future__ import annotations

import random
from collections import defaultdict

from _common import ANALYSIS_DIR, all_classified, classify_missed_alpha, open_db

SEED = 42
K = 5


def main() -> None:
    conn = open_db()
    rows = all_classified(conn)
    wrong = [
        r for r in rows
        if classify_missed_alpha(r["direction"], r["delta_pre_60s"], r["delta_t5m"]) == "wrong_dir"
    ]
    rng = random.Random(SEED)
    sample = rng.sample(wrong, min(K, len(wrong)))

    lines = ["REVERSE-SIGNAL DRILLDOWN — engine agreement on wrong_dir cases", ""]
    lines.append(f"wrong_dir total = {len(wrong)} / {len(rows)} classified")
    lines.append(f"sampled = {len(sample)} (seed={SEED})")
    lines.append("")

    for i, r in enumerate(sample, 1):
        lines.append("─" * 80)
        lines.append(f"#{i}  impact_id={r['id']}  hid={r['headline_id'][:8]}")
        lines.append(f"  headline : {r['headline']}")
        lines.append(f"  market   : {r['question']}")
        lines.append(
            f"  recorded : dir={r['direction']}  conf={r['ai_confidence']:.2f}  "
            f"sim={r['similarity']:.2f}  p0={r['price_t0']:.4f}  "
            f"pre60={r['delta_pre_60s']:+.4f}  Δ5m={r['delta_t5m']:+.4f}"
        )
        signed_post = (r["delta_t5m"] or 0) * (1 if r["direction"] == "YES" else -1)
        lines.append(f"  signed post5m = {signed_post:+.4f}  (negative = market moved AGAINST us)")

        engine_rows = conn.execute(
            """
            SELECT engine_name, direction, confidence, similarity, p_hat, mid_price,
                   edge, size_usdc, executed, skip_reason
            FROM engine_results
            WHERE condition_id=? AND headline_id=?
            ORDER BY engine_name
            """,
            (r["condition_id"], r["headline_id"]),
        ).fetchall()

        if not engine_rows:
            lines.append("  (no engine_results joined on this condition_id+headline_id)")
        else:
            lines.append("")
            lines.append(
                f"  {'engine':<14} {'dir':>4} {'conf':>5} {'sim':>5} {'p_hat':>6} "
                f"{'mid':>6} {'edge':>7} {'size':>7} {'exec':>5}  skip_reason"
            )
            seen_dirs = set()
            for er in engine_rows:
                seen_dirs.add(er["direction"])
                lines.append(
                    f"  {er['engine_name']:<14} {er['direction']:>4} "
                    f"{er['confidence']:>5.2f} {er['similarity']:>5.2f} "
                    f"{er['p_hat']:>6.3f} {(er['mid_price'] or 0):>6.3f} "
                    f"{er['edge']:>+7.4f} {er['size_usdc']:>7.2f} "
                    f"{'YES' if er['executed'] else 'no':>5}  {er['skip_reason'] or ''}"
                )
            if len(seen_dirs - {"NEUTRAL", ""}) > 1:
                lines.append("  ⚠ engines disagreed on direction")
            else:
                lines.append("  ↳ all engines agreed on direction — miscall is model-wide, not engine-specific")
        lines.append("")

    ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    path = ANALYSIS_DIR / "reverse_signal_drilldown.txt"
    path.write_text("\n".join(lines) + "\n")
    print(f"Wrote {path}")


if __name__ == "__main__":
    main()
