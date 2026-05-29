"""Prompt stability harness — run the bundled gate prompt N times on a
hand-crafted set of (article, market) pairs and measure consistency.

    # 20 runs on the default 30-pair seed
    python -m irene.scripts.sandbox.prompt_stability --runs 20

    # Quick smoke (5 runs)
    python -m irene.scripts.sandbox.prompt_stability --runs 5

    # Custom pairs file + temperature sweep
    python -m irene.scripts.sandbox.prompt_stability \
        --pairs /path/to/pairs.yaml --runs 10 --temperature 0.7

Each run sends the same 30 labeled pairs in one bundled LLM call,
varying `extra_key` per run so the on-disk cache doesn't short-circuit.
Reports per-pair agreement rate, modal answer, accuracy vs expected
labels (where provided), and a list of pairs the LLM flipped on.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import statistics
import sys
from collections import Counter
from typing import Optional

import yaml

from irene.app.gates.base import Candidate
from irene.app.gates.llm_batch import _SYSTEM, _labels, _parse, _render_user
from irene.sandbox import llm
from irene.sandbox.paths import DATA_ROOT, ensure_data_dirs

DEFAULT_PAIRS = os.path.join(
    os.path.dirname(__file__), "..", "..", "sandbox", "eval", "pairs_default.yaml"
)
DEFAULT_PAIRS = os.path.abspath(DEFAULT_PAIRS)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pairs", default=DEFAULT_PAIRS,
                    help=f"YAML file with `pairs: [...]` (default {DEFAULT_PAIRS})")
    ap.add_argument("--runs", type=int, default=20)
    ap.add_argument("--model", default="google/gemini-2.5-flash")
    ap.add_argument("--max-output-tokens", type=int, default=120,
                    help="Larger than the gate's default (40) because 30 "
                         "labels can need more output room.")
    ap.add_argument("--prompt-version", default="stability-v1",
                    help="Cache namespace; bump to invalidate all cached runs")
    ap.add_argument("--temperature", type=float, default=0.0,
                    help="0 to measure deterministic agreement, "
                         "0.7+ to probe sampling stability")
    ap.add_argument("--out-dir", default=None,
                    help="Where to write the report (default: data/sandbox/eval/)")
    args = ap.parse_args()

    pairs = _load_pairs(args.pairs)
    if not pairs:
        sys.exit(f"no pairs found in {args.pairs}")

    cands = [_candidate(p) for p in pairs]
    labels = _labels(len(cands))
    labeled = list(zip(labels, cands))
    user = _render_user(labeled)

    print(f"pairs:       {len(cands)} ({len(labels)} labels, batch_size = {len(cands)})")
    print(f"runs:        {args.runs}")
    print(f"model:       {args.model}")
    print(f"temperature: {args.temperature}")
    print(f"output cap:  {args.max_output_tokens} tokens")
    print()

    per_pair_results: dict[str, list[bool]] = {p["id"]: [] for p in pairs}
    raw_per_run: list[dict] = []
    valid_labels = set(labels)
    pair_by_label = {label: p for (label, _), p in zip(labeled, pairs)}

    for i in range(args.runs):
        extra_key = f"stability-r{i}"
        result = llm.call(
            system=_SYSTEM,
            user=user,
            model=args.model,
            strategy_version=args.prompt_version,
            max_tokens=args.max_output_tokens,
            temperature=args.temperature,
            extra_key=extra_key,
        )
        accepted, err = _parse(result.content, valid_labels)
        raw_per_run.append({
            "run": i,
            "cached": result.cached,
            "status": result.status_code,
            "err": err,
            "content": result.content[:300],
            "accepted_labels": sorted(accepted) if accepted else [],
        })
        print(f"  run {i:>2}  cached={result.cached!s:<5}  "
              f"status={result.status_code}  "
              f"accepted={len(accepted) if accepted else 0}/{len(labels)}  "
              f"err={err or '-'}")
        if accepted is None:
            for p in pairs:
                per_pair_results[p["id"]].append(False)
            continue
        for label, p in zip(labels, pairs):
            per_pair_results[p["id"]].append(label in accepted)

    report = _build_report(pairs, per_pair_results, raw_per_run, args)
    out_dir = args.out_dir or os.path.join(DATA_ROOT, "eval")
    os.makedirs(out_dir, exist_ok=True)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    base = f"stability_{args.prompt_version}_n{args.runs}_t{args.temperature}_{stamp}"
    md_path = os.path.join(out_dir, base + ".md")
    json_path = os.path.join(out_dir, base + ".json")
    with open(md_path, "w") as f:
        f.write(report["markdown"])
    with open(json_path, "w") as f:
        json.dump({
            "args": vars(args),
            "per_pair": {
                pid: results for pid, results in per_pair_results.items()
            },
            "runs": raw_per_run,
            "summary": report["summary"],
        }, f, indent=2)

    print()
    print(report["markdown"].split("---", 1)[0])
    print(f"wrote {md_path}")
    print(f"wrote {json_path}")


def _load_pairs(path: str) -> list[dict]:
    with open(path) as f:
        obj = yaml.safe_load(f)
    return obj.get("pairs") or []


def _candidate(p: dict) -> Candidate:
    return Candidate(
        article_id=0,
        market_condition_id=p["id"],
        final_score=1.0,  # not used by the bundled gate's prompt
        article_title=p.get("article_title", ""),
        article_body=p.get("article_body"),
        market_question=p.get("market_question", ""),
        market_description=p.get("market_description"),
        market_end_date=p.get("market_end_date"),
    )


def _build_report(pairs, per_pair, raw_per_run, args) -> dict:
    overall_agreement = []
    overall_accuracy_w_expected = []
    rows = []
    for p in pairs:
        results = per_pair[p["id"]]
        n = len(results)
        if not n:
            continue
        n_true = sum(results)
        n_false = n - n_true
        modal = True if n_true >= n_false else False
        modal_count = max(n_true, n_false)
        agreement = modal_count / n
        overall_agreement.append(agreement)
        expected = p.get("expected_related")
        accuracy = None
        if expected is not None:
            n_correct = sum(1 for r in results if r == expected)
            accuracy = n_correct / n
            overall_accuracy_w_expected.append(accuracy)
        rows.append({
            "id": p["id"],
            "expected": expected,
            "modal": modal,
            "agreement": agreement,
            "n_true": n_true,
            "n_false": n_false,
            "accuracy": accuracy,
            "title": p.get("article_title", "")[:60],
            "market": p.get("market_question", "")[:60],
        })

    flipped = [r for r in rows if 0.05 < r["agreement"] < 0.95]
    wrong_modal = [
        r for r in rows
        if r["expected"] is not None and r["modal"] != r["expected"]
    ]

    parse_errors = sum(1 for r in raw_per_run if r["err"])
    summary = {
        "pairs": len(rows),
        "runs": args.runs,
        "temperature": args.temperature,
        "model": args.model,
        "parse_errors": parse_errors,
        "perfect_agreement_pairs": sum(1 for r in rows if r["agreement"] == 1.0),
        "flipped_pairs": len(flipped),
        "median_agreement": (statistics.median(overall_agreement)
                             if overall_agreement else None),
        "mean_agreement": (statistics.mean(overall_agreement)
                           if overall_agreement else None),
        "median_accuracy_w_expected": (
            statistics.median(overall_accuracy_w_expected)
            if overall_accuracy_w_expected else None),
        "mean_accuracy_w_expected": (
            statistics.mean(overall_accuracy_w_expected)
            if overall_accuracy_w_expected else None),
        "wrong_modal_count": len(wrong_modal),
    }

    lines = [
        f"# Prompt stability — {args.prompt_version}",
        f"_runs: {args.runs} · model: {args.model} · temperature: {args.temperature}_",
        "",
        "## Summary",
        f"- **pairs:** {summary['pairs']}  (parse errors across runs: {parse_errors})",
        f"- **perfect-agreement pairs** (100% same answer): {summary['perfect_agreement_pairs']}",
        f"- **flipped pairs** (between 5%-95% agreement): {summary['flipped_pairs']}",
        f"- **median per-pair agreement:** {_pct(summary['median_agreement'])}",
        f"- **mean per-pair agreement:** {_pct(summary['mean_agreement'])}",
    ]
    if summary["mean_accuracy_w_expected"] is not None:
        lines.extend([
            f"- **mean per-pair accuracy** (vs `expected_related`): {_pct(summary['mean_accuracy_w_expected'])}",
            f"- **wrong-modal pairs** (modal answer disagrees with expected): {summary['wrong_modal_count']}",
        ])

    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## Per-pair results")
    lines.append("")
    lines.append("| id | expected | modal | agreement | accuracy | T/F | article → market |")
    lines.append("|---|---|---|---:|---:|---|---|")
    for r in rows:
        agree = _pct(r["agreement"])
        acc = _pct(r["accuracy"]) if r["accuracy"] is not None else "-"
        exp = ("T" if r["expected"] is True
               else "F" if r["expected"] is False else "?")
        mod = "T" if r["modal"] else "F"
        flag = ""
        if r["expected"] is not None and r["modal"] != r["expected"]:
            flag = " ⚠"
        lines.append(
            f"| {r['id']} | {exp} | {mod}{flag} | {agree} | {acc} | "
            f"{r['n_true']}/{r['n_false']} | "
            f"{r['title']} → {r['market']} |"
        )

    if flipped:
        lines.extend(["", "## Flipped (inconsistent) pairs", ""])
        for r in sorted(flipped, key=lambda x: x["agreement"]):
            lines.append(
                f"- **{r['id']}** ({_pct(r['agreement'])} agreement, "
                f"T/F={r['n_true']}/{r['n_false']}): "
                f"\"{r['title']}\" vs \"{r['market']}\""
            )

    return {"markdown": "\n".join(lines) + "\n", "summary": summary}


def _pct(x: Optional[float]) -> str:
    return f"{x*100:.0f}%" if isinstance(x, (int, float)) else "-"


if __name__ == "__main__":
    ensure_data_dirs()
    main()
