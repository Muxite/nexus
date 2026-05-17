"""Sample examples from missed-alpha buckets and write a markdown report.

Two selection modes:
  --k N         random sample N rows per bucket (default 5; seed=42)
  --every N     deterministic stride: keep every Nth row sorted by
                detected_at DESC. Increase N → fewer examples.

Filter to one bucket with --bucket {captured,no_signal,wrong_dir,too_slow}.
By default all three "trade-worthy" buckets render (too_slow is summarized
but not sampled — it's a latency story, not a decision story).

Cycle-12: each example now carries staked/realized PnL aggregated across
every variant that executed the same (headline, market), plus the
cycle-11 market_context (mkt_vol_30m), freshness label, variant list,
and the AI's confidence + p_alpha.

Standalone:
    python scripts/analysis/sample_failures.py [--k 5 | --every 20] ...

Or via the main analysis CLI:
    python -m scripts.analysis sample-failures [args]
"""
from __future__ import annotations

import argparse
import random
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# Support both `python scripts/analysis/sample_failures.py` and
# `python -m scripts.analysis sample-failures` invocations.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from scripts.analysis._common import (  # type: ignore[no-redef]
        ANALYSIS_DIR, all_classified, classify_missed_alpha, open_db,
        trade_outcome_for,
    )
else:
    from scripts.analysis._common import (
        ANALYSIS_DIR, all_classified, classify_missed_alpha, open_db,
        trade_outcome_for,
    )


SEED = 42
DEFAULT_K = 5
SPARK_HEIGHT = 5
SPARK_WIDTH = 60
CATS_DEFAULT = ("captured", "no_signal", "wrong_dir")


def fmt(v, w=7):
    if v is None:
        return f"{'n/a':>{w}}"
    return f"{v:+.4f}".rjust(w)


def render_sparkline(samples: list[tuple[int, float]], detected_unix: int) -> str:
    """Multi-line ascii price chart spanning the available window."""
    if not samples:
        return "(no timeseries data)"
    times = [t for t, _ in samples]
    prices = [p for _, p in samples]
    pmin, pmax = min(prices), max(prices)
    rng = pmax - pmin or 1e-9
    tmin, tmax = min(times), max(times)
    tspan = tmax - tmin or 1

    grid = [[" "] * SPARK_WIDTH for _ in range(SPARK_HEIGHT)]
    for t, p in samples:
        x = int((t - tmin) / tspan * (SPARK_WIDTH - 1))
        y = SPARK_HEIGHT - 1 - int((p - pmin) / rng * (SPARK_HEIGHT - 1))
        x = max(0, min(SPARK_WIDTH - 1, x))
        y = max(0, min(SPARK_HEIGHT - 1, y))
        grid[y][x] = "•"

    x0 = int((detected_unix - tmin) / tspan * (SPARK_WIDTH - 1))
    x0 = max(0, min(SPARK_WIDTH - 1, x0))
    for y in range(SPARK_HEIGHT):
        if grid[y][x0] == " ":
            grid[y][x0] = "|"

    lines = []
    for i, row in enumerate(grid):
        label_p = pmax - (pmax - pmin) * i / (SPARK_HEIGHT - 1)
        lines.append(f"    {label_p:.3f} │ {''.join(row)}")
    mins_lo = (tmin - detected_unix) / 60.0
    mins_hi = (tmax - detected_unix) / 60.0
    lines.append(f"          └{'─' * SPARK_WIDTH}")
    lines.append(f"           {mins_lo:+.0f}m{' ' * (SPARK_WIDTH - 8)}{mins_hi:+.0f}m  (| = headline detected)")
    return "\n".join(lines)


def _money_line(outcome: dict) -> str:
    if not outcome:
        return "- **staked / realized**: (not executed)"
    reasons = outcome.get("close_reasons") or {}
    reasons_s = ", ".join(f"{r}×{n}" for r, n in sorted(reasons.items()))
    return (
        f"- **staked / realized**: ${outcome['staked']:.0f} "
        f"({outcome['n']} variants) → "
        f"{('+' if outcome['realized'] >= 0 else '')}{outcome['realized']:.2f} "
        f"({reasons_s or 'open'})"
    )


def _market_context_line(outcome: dict) -> str:
    if not outcome:
        return ""
    if outcome.get("mkt_vol_30m") is None:
        return ""
    v = outcome["mkt_vol_30m"]
    d = outcome.get("mkt_drift_30m")
    drift_s = f"  drift={d:+.3f}" if d is not None else ""
    return f"- **market_context_30m**: vol={v:.4f}{drift_s}"


def _signal_freshness(conn, signal_id):
    if signal_id is None:
        return None
    r = conn.execute(
        "SELECT freshness_label FROM signals WHERE id = ?", (signal_id,)
    ).fetchone()
    return r["freshness_label"] if r else None


def _select_samples(buckets: dict, cats: tuple, args) -> dict:
    samples: dict = {}
    if args.every:
        for cat in cats:
            pool = sorted(
                buckets.get(cat, []),
                key=lambda r: r["detected_at"] or "",
                reverse=True,
            )
            samples[cat] = pool[::args.every]
    else:
        rng = random.Random(args.seed)
        for cat in cats:
            pool = buckets.get(cat, [])
            samples[cat] = rng.sample(pool, min(args.k, len(pool))) if pool else []
    return samples


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        prog="sample_failures",
        description=__doc__.split("\n\n")[0],
    )
    p.add_argument("--k", type=int, default=DEFAULT_K,
                   help="random samples per bucket (default 5); ignored when --every is set")
    p.add_argument("--every", type=int, default=None,
                   help="stride/modulo selection: keep every Nth row per bucket")
    p.add_argument("--bucket", choices=("captured", "no_signal", "wrong_dir", "too_slow"),
                   default=None, help="restrict to one bucket (default: captured + no_signal + wrong_dir)")
    p.add_argument("--seed", type=int, default=SEED,
                   help="random seed for --k mode")
    p.add_argument("--out", type=Path, default=None,
                   help="output markdown path (default: data/analysis/failures_report.md)")
    p.add_argument("--db", type=Path, default=None,
                   help="path to seulgi.db (default: data/seulgi.db)")
    return p.parse_args(argv)


def _signal_id_for(conn, headline_id):
    if not headline_id:
        return None
    r = conn.execute(
        "SELECT id FROM signals WHERE headline_id = ? LIMIT 1",
        (headline_id,),
    ).fetchone()
    return r["id"] if r else None


def main(argv=None) -> None:
    args = parse_args(argv)
    ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = args.out or (ANALYSIS_DIR / "failures_report.md")

    conn = open_db(args.db)
    rows = all_classified(conn)
    buckets: dict[str, list] = defaultdict(list)
    for r in rows:
        b = classify_missed_alpha(r["direction"], r["delta_pre_60s"], r["delta_t5m"])
        if b is not None:
            buckets[b].append(r)

    cats = (args.bucket,) if args.bucket else CATS_DEFAULT
    samples = _select_samples(buckets, cats, args)

    mode = f"every={args.every}" if args.every else f"k={args.k} (seed={args.seed})"
    lines: list[str] = []
    lines.append("# Failure / Success Sample Report")
    lines.append("")
    lines.append(f"Generated by `scripts/analysis/sample_failures.py` ({mode}).")
    lines.append("")
    lines.append("Bucket totals (across all classified rows):")
    lines.append("")
    for cat in ("captured", "no_signal", "wrong_dir", "too_slow"):
        lines.append(f"- **{cat}**: {len(buckets.get(cat, []))}")
    lines.append("")

    for cat in cats:
        lines.append("---")
        lines.append(f"## Category: `{cat}` — {len(samples[cat])} examples")
        lines.append("")
        for i, r in enumerate(samples[cat], 1):
            detected_dt = r["detected_at"]
            detected_unix = int(datetime.fromisoformat(detected_dt).timestamp())
            ts = conn.execute(
                "SELECT t_unix, price FROM price_timeseries WHERE impact_id=? "
                "ORDER BY t_unix",
                (r["id"],),
            ).fetchall()
            samples_xy = [(row["t_unix"], row["price"]) for row in ts]
            art = conn.execute(
                "SELECT article_url, body_chars FROM headline_articles WHERE headline_id=?",
                (r["headline_id"],),
            ).fetchone()
            outcome = trade_outcome_for(conn, r["headline_id"], r["condition_id"])

            sig_id = _signal_id_for(conn, r["headline_id"])
            freshness = _signal_freshness(conn, sig_id)

            lines.append(f"### {cat} #{i}  (impact_id={r['id']}, hid={(r['headline_id'] or '')[:8]})")
            lines.append("")
            lines.append(f"- **detected_at**: {detected_dt}")
            lines.append(f"- **headline**: {r['headline']}")
            lines.append(f"- **site**: {r['site_url']}")
            if freshness:
                lines.append(f"- **freshness**: {freshness}")
            if art:
                lines.append(f"- **article_url**: {art['article_url']}  (body_chars={art['body_chars']})")
            lines.append(f"- **market**: {r['question']}")
            lines.append(f"- **condition_id**: `{(r['condition_id'] or '')[:18]}…`")
            lines.append(
                f"- **dir/conf/sim/source**: {r['direction']}  "
                f"conf={r['ai_confidence']:.2f}  sim={r['similarity']:.2f}  src={r['source']}"
            )
            if outcome:
                ai_conf = outcome.get("ai_conf")
                p_a = outcome.get("p_alpha")
                if ai_conf is not None or p_a is not None:
                    lines.append(
                        f"- **trader saw**: ai_conf={ai_conf or 0:.2f}  p_alpha={p_a or 0:.2f}"
                    )
            # Money + market_context + variants
            lines.append(_money_line(outcome))
            mc = _market_context_line(outcome)
            if mc:
                lines.append(mc)
            if outcome and outcome.get("variants"):
                vs = outcome["variants"][:6]
                more = f"  (+{len(outcome['variants']) - len(vs)} more)" if len(outcome["variants"]) > len(vs) else ""
                lines.append(f"- **variants traded**: {', '.join(vs)}{more}")
            lines.append(f"- **price_t0**: {r['price_t0']:.4f}" if r["price_t0"] is not None else "- price_t0: n/a")
            lines.append(
                "- **deltas**: "
                f"pre300={fmt(r['delta_pre_300s'])}  pre60={fmt(r['delta_pre_60s'])}  "
                f"t1m={fmt(r['delta_t1m'])}  t5m={fmt(r['delta_t5m'])}  "
                f"t15m={fmt(r['delta_t15m'])}  t30m={fmt(r['delta_t30m'])}  "
                f"t60m={fmt(r['delta_t60m'])}"
            )
            signed_pre = (r["delta_pre_60s"] or 0) * (1 if r["direction"] == "YES" else -1)
            signed_post = (r["delta_t5m"] or 0) * (1 if r["direction"] == "YES" else -1)
            lines.append(f"- **signed pre/post (vs predicted dir)**: pre={signed_pre:+.4f}  post5m={signed_post:+.4f}")
            lines.append("")
            lines.append("```")
            lines.append(render_sparkline(samples_xy, detected_unix))
            lines.append("```")
            lines.append("")

    out_path.write_text("\n".join(lines))
    n_total = sum(len(v) for v in samples.values())
    print(f"Wrote {out_path}  ({n_total} examples; mode={mode})")


# Subcommand contract — same signature as the rest of scripts.analysis.
def add_parser(p) -> None:
    p.add_argument("--k", type=int, default=DEFAULT_K)
    p.add_argument("--every", type=int, default=None)
    p.add_argument("--bucket", choices=("captured", "no_signal", "wrong_dir", "too_slow"),
                   default=None)
    p.add_argument("--seed", type=int, default=SEED)
    p.add_argument("--out", type=Path, default=None)
    p.add_argument("--db", type=Path, default=None)
    # Tolerate the analysis-CLI's shared flags so a uniform invocation works.
    p.add_argument("--since", default=None, help="(ignored — kept for CLI symmetry)")
    p.add_argument("--variant", default=None, help="(ignored — kept for CLI symmetry)")
    p.add_argument("--format", default="text", help="(ignored — always markdown)")
    p.add_argument("--limit", type=int, default=None, help="(ignored)")


def run(args, conn):
    """Subcommand entry point: render the report and return a Section
    that points to the file.

    Re-uses `main()` for the heavy lifting but routes through the
    passed-in connection where possible. Returns a one-line Section
    so the analysis CLI can render it like every other subcommand.
    """
    from scripts.analysis._format import Section
    main(_argv_from_namespace(args))
    out_path = args.out or (ANALYSIS_DIR / "failures_report.md")
    return Section(
        title="sample-failures — written to file",
        headers=["artefact", "path"],
        rows=[["failures_report", str(out_path)]],
        notes=["Open the file for the per-bucket markdown report."],
    )


def _argv_from_namespace(ns) -> list[str]:
    argv: list[str] = []
    if ns.every:
        argv += ["--every", str(ns.every)]
    elif ns.k != DEFAULT_K:
        argv += ["--k", str(ns.k)]
    if ns.bucket:
        argv += ["--bucket", ns.bucket]
    if ns.seed != SEED:
        argv += ["--seed", str(ns.seed)]
    if ns.out:
        argv += ["--out", str(ns.out)]
    if ns.db:
        argv += ["--db", str(ns.db)]
    return argv


if __name__ == "__main__":
    main()
