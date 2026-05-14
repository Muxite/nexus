"""
AI-authored synthesis of `recommendations.md` and `rule_proposals.md` from
the deterministic markdown files produced by `run_postmortem.py`.

Uses the OpenRouter API key already configured in seulgi/config.yaml so we
don't need a separate secret. Model defaults to anthropic/claude-sonnet-4
through OpenRouter — recommendations is a synthesis task where a stronger
model is worth the cost; override via `--recommender-model` if needed.

If the API key is missing or the call fails, the orchestrator falls back to
writing a placeholder file so the rest of the run completes.
"""
from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Optional


REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "app"))


_RECO_SYSTEM = """You are an expert quantitative trading systems analyst. You're reading the latest
post-mortem of a paper-trading bot called seulgi that ingests news headlines and trades
prediction markets on Polymarket. Your job: write the next ranked list of recommendations
the developer should ship, plus a separate list of rule-shaped proposals for the bot's
manual-planning rules engine.

Conventions
───────────
The seulgi system classifies every (headline, market) pair into one of four buckets:
  - captured: we entered before a real post-headline move in the right direction
  - no_signal: headline produced no measurable market move
  - wrong_dir: post-move went against our predicted direction
  - too_slow: market had already moved before we detected the headline

IC (information coefficient) is Pearson r between signed AI confidence and Δ5m price.
IC > 0 means the AI is informative; IC < 0 means anti-informative.

Output format
─────────────
Produce TWO markdown documents, separated by a line containing exactly "=== rule_proposals.md ===".

The first document is recommendations.md:
  - Lead with a "TL;DR" of 3-5 bullets
  - Then a numbered list "## Recommended changes" ordered by expected ROI on signal quality
  - Each item: a one-line title, then 2-4 lines of justification grounded in the data files
  - Tag each item with one of: [OBS] [HYPOTHESIS] [CLASSIFIER] [AI] [ENGINE] [RULE]
  - Cite specific metrics or rows from the input files (e.g. "no_signal=89%", "IC=-0.11")
  - Do NOT re-recommend items already shipped per `recommendations_prior.md` (if provided)

The second document is rule_proposals.md:
  - Each entry is a single rule the user could promote into seulgi/rules/<name>.md
  - Use YAML-like frontmatter: name / market_type_match / source_match / default_action / has_exceptions
  - Then a "## Rationale" section pointing at specific failure samples
  - Examples:
      * skip sports markets unless the headline names a specific player AND is an injury/transfer
      * skip price_threshold markets dominated by price-history (opinion articles, generic macro takes)
      * favor event markets when the headline names a specific named entity
  - Be concrete — propose 3-7 rules max, each justified by something visible in the data
"""


def _build_user_prompt(postmortem_dir: Path, prior_reco: Optional[str]) -> str:
    """Concatenate every relevant input file into a single user message."""
    parts: list[str] = ["Inputs from the post-mortem directory:\n"]

    # Order matters — put the cheapest-to-skim files first.
    file_order = (
        "metrics.json",
        "delta_vs_previous.md",
        "dashboard_health.md",
        "bucket_stats.md",
        "feature_breakdown.md",
        "failure_samples.md",
        "reverse_signal.md",
    )
    for fname in file_order:
        p = postmortem_dir / fname
        if not p.exists():
            continue
        body = p.read_text()
        parts.append(f"### {fname}\n```\n{body}\n```\n")

    if prior_reco:
        parts.append(
            "### recommendations_prior.md (DO NOT re-recommend items from this list)\n"
            f"```\n{prior_reco}\n```\n"
        )
    parts.append(
        "Now produce recommendations.md and rule_proposals.md per the system spec."
    )
    return "\n".join(parts)


def _call_openrouter(
    *, api_key: str, model: str, api_base: str, system: str, user: str
) -> Optional[str]:
    """Synchronous one-shot chat completion via OpenRouter. Returns the text or None."""
    body = json.dumps({
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user",   "content": user},
        ],
        "max_tokens": 4000,
        "temperature": 0.3,
    }).encode("utf-8")
    req = urllib.request.Request(
        url=api_base.rstrip("/") + "/chat/completions",
        data=body,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://seulgi.local/postmortem",
            "X-Title": "seulgi post-mortem recommender",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        return data["choices"][0]["message"]["content"]
    except (urllib.error.URLError, urllib.error.HTTPError, KeyError, json.JSONDecodeError) as exc:
        print(f"[reco] API call failed: {exc}", file=sys.stderr)
        return None


def write_recommendations(
    postmortem_dir: Path,
    *,
    api_key: str,
    model: str = "google/gemini-2.5-flash",
    api_base: str = "https://openrouter.ai/api/v1",
    prior_reco_path: Optional[Path] = None,
) -> tuple[Path, Path]:
    """
    Call the LLM, split its response on the "=== rule_proposals.md ===" delimiter,
    write `recommendations.md` and `rule_proposals.md` into `postmortem_dir`.
    Returns the two paths. On API failure, writes a placeholder.
    """
    prior_reco = prior_reco_path.read_text() if (prior_reco_path and prior_reco_path.exists()) else None
    user = _build_user_prompt(postmortem_dir, prior_reco)

    reco_path = postmortem_dir / "recommendations.md"
    rules_path = postmortem_dir / "rule_proposals.md"

    if not api_key:
        reco_path.write_text(
            "# Recommendations — UNAVAILABLE\n\n"
            "No OpenRouter API key configured. Set ai.api_key in config.yaml or "
            "OPENROUTER_API_KEY env var, then re-run the post-mortem.\n"
        )
        rules_path.write_text(
            "# Rule proposals — UNAVAILABLE\n\n"
            "(generated by the AI recommender; see recommendations.md for the same reason)\n"
        )
        return reco_path, rules_path

    text = _call_openrouter(
        api_key=api_key, model=model, api_base=api_base,
        system=_RECO_SYSTEM, user=user,
    )
    if text is None:
        reco_path.write_text(
            "# Recommendations — API ERROR\n\n"
            "The OpenRouter call failed. Check the orchestrator log for details.\n"
        )
        rules_path.write_text(
            "# Rule proposals — API ERROR\n\n"
            "(generated by the AI recommender; see recommendations.md)\n"
        )
        return reco_path, rules_path

    # Split on the delimiter the system prompt asked the model to emit.
    delim = "=== rule_proposals.md ==="
    if delim in text:
        reco_body, _, rules_body = text.partition(delim)
    else:
        # Model didn't honour the split — keep everything in recommendations,
        # leave rules empty with a note.
        reco_body = text
        rules_body = (
            "_(no rule_proposals section emitted by the model; see "
            "recommendations.md for everything it produced)_"
        )

    reco_path.write_text(reco_body.strip() + "\n")
    rules_path.write_text(rules_body.strip() + "\n")
    return reco_path, rules_path
