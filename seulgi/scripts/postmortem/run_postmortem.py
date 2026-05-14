"""
Recurrent post-mortem orchestrator.

Reads `data/seulgi.db`, writes a versioned directory under
`data/postmortems/<YYYY-MM-DD-HHMM>/` containing:

  - metrics.json             scalar summary (diff target)
  - bucket_stats.md          captured/no_signal/wrong_dir/too_slow tables
                             sliced by source, confidence, p0, market_type
  - dashboard_health.md      observability canary per widget
  - feature_breakdown.md     wraps scripts/analysis/feature_breakdown.py
  - failure_samples.md       wraps scripts/analysis/sample_failures.py
  - reverse_signal.md        wraps scripts/analysis/reverse_signal_drilldown.py
  - delta_vs_previous.md     scalar diff against the previous post-mortem
  - recommendations.md       AI-authored, ranked changes [OBS|HYPOTHESIS|...|RULE]
  - rule_proposals.md        AI-authored, ready-to-promote rule files

Usage
─────
  python3 -m scripts.postmortem.run_postmortem               # full run
  python3 -m scripts.postmortem.run_postmortem --no-ai       # skip the LLM call
  python3 -m scripts.postmortem.run_postmortem --hours 6     # window the data
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Allow `python3 -m scripts.postmortem.run_postmortem` (package-relative
# imports break) and `python3 scripts/postmortem/run_postmortem.py` alike.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from _metrics import (   # noqa: E402
    DB_PATH, POSTMORTEM_ROOT,
    bucket_counts_by_dim, collect_metrics, dashboard_health, has_column, open_db,
)
from _ai_recommendations import write_recommendations   # noqa: E402


REPO = Path(__file__).resolve().parents[2]


# ---------------------------------------------------------------------------
# bucket_stats.md
# ---------------------------------------------------------------------------

_P0_BUCKETS = ((0.0, 0.05), (0.05, 0.10), (0.10, 0.30), (0.30, 0.70),
               (0.70, 0.90), (0.90, 0.95), (0.95, 1.01))
_CONF_BUCKETS = ((0.5, 0.55), (0.55, 0.65), (0.65, 0.75), (0.75, 0.85),
                 (0.85, 0.95), (0.95, 1.01))


def _bucket_label(value, ranges):
    if value is None:
        return None
    for lo, hi in ranges:
        if lo <= value < hi:
            return f"[{lo:.2f},{hi:.2f})"
    return None


def _render_table(rows: list[dict], key: str) -> list[str]:
    """Markdown table with one row per slice. Empty list if no rows."""
    if not rows:
        return ["_(no rows)_", ""]
    cols = (key, "n", "captured", "no_signal", "wrong_dir", "too_slow",
            "captured_share", "no_signal_share")
    out = ["| " + " | ".join(cols) + " |",
           "|" + "|".join(["---"] * len(cols)) + "|"]
    for r in rows:
        out.append("| " + " | ".join([
            str(r[key]),
            f"{r['n']}",
            f"{r['captured']}", f"{r['no_signal']}",
            f"{r['wrong_dir']}", f"{r['too_slow']}",
            f"{r['captured_share']*100:.1f}%",
            f"{r['no_signal_share']*100:.1f}%",
        ]) + " |")
    out.append("")
    return out


def write_bucket_stats(conn, out_path: Path) -> None:
    """Per-slice bucket stats. Adds the market_type slice when the column exists."""
    has_mt = has_column(conn, "markets", "market_type")
    if has_mt:
        rows = conn.execute(
            "SELECT pi.*, COALESCE(m.market_type, 'untagged') AS market_type "
            "FROM price_impacts pi "
            "LEFT JOIN markets m ON m.condition_id = pi.condition_id "
            "WHERE pi.delta_t5m IS NOT NULL AND pi.delta_pre_60s IS NOT NULL"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT pi.*, 'untagged' AS market_type FROM price_impacts pi "
            "WHERE pi.delta_t5m IS NOT NULL AND pi.delta_pre_60s IS NOT NULL"
        ).fetchall()

    lines = ["# bucket_stats.md", "",
             f"Classified rows: **{len(rows)}**", ""]

    # ── By source
    lines += ["## By source", ""]
    by_source = bucket_counts_by_dim(rows, key_fn=lambda r: r["source"] or "(none)",
                                     label="source")
    lines += _render_table(by_source, "source")

    # ── By confidence bucket
    lines += ["## By AI confidence bucket", ""]
    by_conf = bucket_counts_by_dim(
        rows,
        key_fn=lambda r: _bucket_label(r["ai_confidence"], _CONF_BUCKETS),
        label="confidence",
    )
    lines += _render_table(by_conf, "confidence")

    # ── By p0 bucket
    lines += ["## By entry-price bucket (p0)", ""]
    by_p0 = bucket_counts_by_dim(
        rows,
        key_fn=lambda r: _bucket_label(r["price_t0"], _P0_BUCKETS),
        label="p0",
    )
    lines += _render_table(by_p0, "p0")

    # ── By market_type (the strategic slice)
    if has_mt:
        lines += ["## By market_type", ""]
        by_type = bucket_counts_by_dim(rows, key_fn=lambda r: r["market_type"],
                                       label="market_type")
        lines += _render_table(by_type, "market_type")
    else:
        lines += [
            "## By market_type",
            "",
            "_(skipped — markets.market_type column not yet present;"
            " restart the bot to apply the migration, then run"
            " `python3 -m scripts.postmortem.tag_market_types`)_",
            "",
        ]

    out_path.write_text("\n".join(lines))


# ---------------------------------------------------------------------------
# dashboard_health.md
# ---------------------------------------------------------------------------

def write_dashboard_health(conn, metrics: dict, out_path: Path) -> None:
    rows = dashboard_health(conn, metrics)
    lines = ["# dashboard_health.md", "",
             "Per-widget status — answers 'is this chart trustworthy right now?'", "",
             "| widget | status | detail |", "|---|---|---|"]
    for r in rows:
        lines.append(f"| {r['widget']} | {r['status']} | {r['detail']} |")
    lines.append("")
    out_path.write_text("\n".join(lines))


# ---------------------------------------------------------------------------
# Reuse existing analysis scripts as subprocess calls
# ---------------------------------------------------------------------------

def _run_analysis_subscript(module: str, expected_filename: str, dest_dir: Path) -> Optional[Path]:
    """
    Run `python3 -m scripts.analysis.<module>`, then move the produced file
    from `data/analysis/` into `dest_dir`. Returns the new path or None.
    """
    analysis_dir = REPO / "data" / "analysis"
    src = analysis_dir / expected_filename
    # Snapshot mtime so we can detect "not produced this run"
    pre_mtime = src.stat().st_mtime if src.exists() else 0.0

    env = os.environ.copy()
    # Make sure the existing scripts can import `_common` (their internal helper)
    env["PYTHONPATH"] = f"{REPO / 'scripts' / 'analysis'}:{env.get('PYTHONPATH', '')}"
    try:
        subprocess.run(
            [sys.executable, "-m", f"scripts.analysis.{module}"],
            cwd=REPO, env=env, check=True, capture_output=True, text=True, timeout=120,
        )
    except subprocess.CalledProcessError as exc:
        print(f"[postmortem] {module} failed: {exc.stderr}", file=sys.stderr)
        return None
    except subprocess.TimeoutExpired:
        print(f"[postmortem] {module} timed out", file=sys.stderr)
        return None

    if not src.exists() or src.stat().st_mtime <= pre_mtime:
        print(f"[postmortem] {module} produced no fresh {expected_filename}", file=sys.stderr)
        return None

    # Rename target into the post-mortem dir, normalizing the extension to .md
    dest = dest_dir / Path(expected_filename).with_suffix(".md").name
    shutil.copy2(src, dest)
    return dest


# ---------------------------------------------------------------------------
# delta_vs_previous.md
# ---------------------------------------------------------------------------

def write_delta_vs_previous(this_metrics: dict, this_dir: Path) -> None:
    """Find the most recent prior post-mortem and diff scalar metrics."""
    siblings = sorted(
        (d for d in POSTMORTEM_ROOT.iterdir() if d.is_dir() and d != this_dir),
        reverse=True,
    )
    out_path = this_dir / "delta_vs_previous.md"
    if not siblings:
        out_path.write_text(
            "# delta_vs_previous.md\n\n"
            "_(no prior post-mortem to diff against — this is the first run)_\n"
        )
        return
    prior_dir = siblings[0]
    prior_metrics_path = prior_dir / "metrics.json"
    if not prior_metrics_path.exists():
        out_path.write_text(
            f"# delta_vs_previous.md\n\n"
            f"_(prior post-mortem {prior_dir.name} has no metrics.json)_\n"
        )
        return
    prior = json.loads(prior_metrics_path.read_text())

    def _fmt(x) -> str:
        if x is None:
            return "—"
        if isinstance(x, float):
            return f"{x:.4f}"
        return str(x)

    def _delta(a, b) -> str:
        if a is None or b is None:
            return "—"
        try:
            return f"{(b - a):+.4f}"
        except Exception:
            return "—"

    lines = [f"# delta_vs_previous.md", "",
             f"Diffing against `{prior_dir.name}` (generated {prior.get('generated_at')})", ""]

    # Scalar diffs
    pairs = (
        ("price_impacts_classified", prior.get("price_impacts_classified"),
         this_metrics.get("price_impacts_classified")),
        ("ic_global",                prior.get("ic_global"),
         this_metrics.get("ic_global")),
        ("ai_neutral_rate_1h",       prior.get("ai_neutral_rate_1h"),
         this_metrics.get("ai_neutral_rate_1h")),
        ("latency_p50_ms",           (prior.get("latency_ms") or {}).get("p50"),
         (this_metrics.get("latency_ms") or {}).get("p50")),
        ("latency_p99_ms",           (prior.get("latency_ms") or {}).get("p99"),
         (this_metrics.get("latency_ms") or {}).get("p99")),
    )
    lines += ["## Scalars", "",
              "| metric | prior | now | Δ |", "|---|---|---|---|"]
    for name, a, b in pairs:
        lines.append(f"| {name} | {_fmt(a)} | {_fmt(b)} | {_delta(a, b)} |")
    lines.append("")

    # Bucket shares
    lines += ["## Bucket shares", "",
              "| bucket | prior share | now share | Δ |",
              "|---|---|---|---|"]
    p_buckets = prior.get("buckets", {})
    n_buckets = this_metrics.get("buckets", {})
    for bucket in ("captured", "no_signal", "wrong_dir", "too_slow"):
        a = (p_buckets.get(bucket) or {}).get("share")
        b = (n_buckets.get(bucket) or {}).get("share")
        lines.append(f"| {bucket} | {_fmt(a)} | {_fmt(b)} | {_delta(a, b)} |")
    lines.append("")

    # Per-engine PnL diff
    lines += ["## Per-engine realized PnL", "",
              "| engine | prior | now | Δ |", "|---|---|---|---|"]
    engines = sorted(set(prior.get("per_engine_pnl", {}))
                     | set(this_metrics.get("per_engine_pnl", {})))
    for eng in engines:
        a = (prior.get("per_engine_pnl", {}).get(eng) or {}).get("pnl")
        b = (this_metrics.get("per_engine_pnl", {}).get(eng) or {}).get("pnl")
        lines.append(f"| {eng} | {_fmt(a)} | {_fmt(b)} | {_delta(a, b)} |")
    lines.append("")

    # Sourcing for the AI: the prior dir's recommendations.md is passed
    # separately so the LLM can avoid repeating shipped advice.
    out_path.write_text("\n".join(lines))


def _prior_recommendations_path() -> Optional[Path]:
    if not POSTMORTEM_ROOT.exists():
        return None
    siblings = sorted(
        (d for d in POSTMORTEM_ROOT.iterdir() if d.is_dir()),
        reverse=True,
    )
    for d in siblings:
        p = d / "recommendations.md"
        if p.exists():
            return p
    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _load_api_key(config_path: Optional[Path]) -> str:
    """OpenRouter key from config.yaml ai.api_key, env, or empty string."""
    env_key = os.getenv("OPENROUTER_API_KEY") or os.getenv("GEMINI_API_KEY") or ""
    if env_key:
        return env_key
    sys.path.insert(0, str(REPO / "app"))
    try:
        from config import load_config
        cfg = load_config(str(config_path) if config_path
                          else os.getenv("SEULGI_CONFIG", "config.yaml"))
        return cfg.ai.api_key or ""
    except Exception:
        return ""


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    p.add_argument("--db", default=None, help="path to seulgi.db (default: data/seulgi.db)")
    p.add_argument("--config", default=None, help="path to config.yaml")
    p.add_argument("--out", default=None,
                   help="output dir (default: data/postmortems/<timestamp>)")
    p.add_argument("--no-ai", action="store_true",
                   help="skip the LLM call (writes placeholder recommendations.md)")
    p.add_argument("--recommender-model", default="google/gemini-2.5-flash",
                   help="OpenRouter model ID for recommendations synthesis "
                        "(default matches the live AI classifier — same key, "
                        "same provider, predictable cost)")
    p.add_argument("--hours", type=float, default=None,
                   help="reserved — orchestrator currently uses full DB window")
    args = p.parse_args()

    # ── Output dir
    if args.out:
        out_dir = Path(args.out)
    else:
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M")
        out_dir = POSTMORTEM_ROOT / stamp
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"[postmortem] writing to {out_dir}")

    # ── DB
    db_path = Path(args.db) if args.db else DB_PATH
    if not db_path.exists():
        print(f"[postmortem] DB not found at {db_path}", file=sys.stderr)
        return 1
    conn = open_db(db_path)

    # ── Deterministic metrics & supporting files
    print("[postmortem] collecting metrics…")
    metrics = collect_metrics(conn)
    (out_dir / "metrics.json").write_text(json.dumps(metrics, indent=2, default=str))

    print("[postmortem] writing bucket_stats.md…")
    write_bucket_stats(conn, out_dir / "bucket_stats.md")

    print("[postmortem] writing dashboard_health.md…")
    write_dashboard_health(conn, metrics, out_dir / "dashboard_health.md")

    print("[postmortem] writing delta_vs_previous.md…")
    write_delta_vs_previous(metrics, out_dir)

    # ── Pull in the existing analysis scripts (subprocess + copy)
    print("[postmortem] running feature_breakdown…")
    _run_analysis_subscript("feature_breakdown", "feature_breakdown.txt", out_dir)
    print("[postmortem] running sample_failures…")
    _run_analysis_subscript("sample_failures",  "failures_report.md",   out_dir)
    print("[postmortem] running reverse_signal_drilldown…")
    _run_analysis_subscript("reverse_signal_drilldown",
                            "reverse_signal_drilldown.txt", out_dir)

    conn.close()

    # ── AI recommendations
    if args.no_ai:
        (out_dir / "recommendations.md").write_text(
            "# Recommendations — SKIPPED (--no-ai)\n"
        )
        (out_dir / "rule_proposals.md").write_text(
            "# Rule proposals — SKIPPED (--no-ai)\n"
        )
    else:
        api_key = _load_api_key(Path(args.config) if args.config else None)
        prior = _prior_recommendations_path()
        print(f"[postmortem] AI recommender (model={args.recommender_model}, "
              f"prior={prior.parent.name if prior else 'none'})…")
        write_recommendations(
            out_dir,
            api_key=api_key,
            model=args.recommender_model,
            prior_reco_path=prior,
        )

    print(f"[postmortem] done → {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
