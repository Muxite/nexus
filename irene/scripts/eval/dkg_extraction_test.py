"""
## Specification

**Purpose.** Iterate the DKG bulk-extraction prompt before we commit
production code to it. Reads the gold YAML at
`scripts/eval/dkg_examples.yaml`, calls OpenRouter against each
headline, and prints expected-vs-actual side by side. The reviewer (me
— Claude Opus 4.7) judges whether the cheap model produced something
worth trusting.

This is NOT integrated into the live pipeline. It's a tuning loop.

**Public interface.**
- CLI: `python -m scripts.eval.dkg_extraction_test \\
            --prompt v1|v2|v3 \\
            --model google/gemini-2.5-flash-lite \\
            [--limit N]`
- Requires `OPENROUTER_API_KEY` in env. Estimated cost per full
  25-example run with flash-lite: ~$0.003. Cheap; iterate freely.

**Non-goals.**
- Doesn't write to the live DB.
- Doesn't validate against the locked ontology — that's the production
  extractor's job. Here we want to SEE what the model emits, including
  off-ontology relations / entity types, so we can decide whether to
  tighten the prompt or relax the validator.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import httpx
import yaml


# ---------------------------------------------------------------------------
# Prompt variants — three candidates, evaluated head-to-head.
# ---------------------------------------------------------------------------

# The Pydantic schema the cheap model is asked to emit. JSON-schema'd
# response_format works on Gemini + OpenAI; Anthropic ignores the
# schema arg but the prompt-level instruction still steers it.
_PYDANTIC_SCHEMA = {
    "type": "object",
    "properties": {
        "facts": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "subject":     {"type": "string"},
                    "subj_type":   {"type": "string", "enum": [
                        "COMPANY", "PERSON", "ORG", "COUNTRY",
                        "ECON_INDICATOR", "ASSET", "EVENT",
                    ]},
                    "relation":    {"type": "string", "enum": [
                        "announces", "decides", "elects", "acquires",
                        "launches", "accuses", "signs", "changes",
                        "resigns", "nominates",
                    ]},
                    "object":      {"type": ["string", "null"]},
                    "obj_type":    {"type": ["string", "null"], "enum": [
                        "COMPANY", "PERSON", "ORG", "COUNTRY",
                        "ECON_INDICATOR", "ASSET", "EVENT", None,
                    ]},
                    "obj_value":   {"type": ["string", "null"]},
                },
                "required": ["subject", "subj_type", "relation"],
            },
        },
    },
    "required": ["facts"],
}


_PROMPT_V1_SYSTEM = (
    "You extract structured facts from financial news headlines.\n"
    "Each fact uses the subject-relation-object pattern with typed entities.\n"
    "\n"
    "Entity types (use exactly one for subj_type and obj_type):\n"
    "  COMPANY, PERSON, ORG, COUNTRY, ECON_INDICATOR, ASSET, EVENT\n"
    "\n"
    "Relations (use exactly one for relation):\n"
    "  announces, decides, elects, acquires, launches,\n"
    "  accuses, signs, changes, resigns, nominates\n"
    "\n"
    "Rules:\n"
    " - Use the shortest widely-recognised name (FDA not 'U.S. Food and Drug Administration').\n"
    " - object is null when there's no second entity (e.g. resigns).\n"
    " - obj_value carries the numeric/textual target for relations like\n"
    "   'changes' (e.g. 'fed funds rate -25bp') or 'launches' a product.\n"
    " - If the headline doesn't fit the ontology, emit an empty facts list.\n"
    " - Output JSON ONLY matching the schema. No prose, no markdown fence."
)

_PROMPT_V2_SYSTEM = (
    "Extract subject-relation-object facts from the headline.\n"
    "subj_type/obj_type ∈ {COMPANY,PERSON,ORG,COUNTRY,ECON_INDICATOR,ASSET,EVENT}.\n"
    "relation ∈ {announces,decides,elects,acquires,launches,accuses,signs,changes,resigns,nominates}.\n"
    "Use shortest-form names (FDA not 'U.S. Food and Drug Administration').\n"
    "object null when no second entity. obj_value for numeric targets.\n"
    "JSON only matching the schema. No prose."
)

_PROMPT_V3_SYSTEM = (
    _PROMPT_V1_SYSTEM
    + "\n\nExamples (input → output):\n"
    + "INPUT: FDA rejects Pfizer's diabetes drug application\n"
    + "OUTPUT: "
    + json.dumps({"facts": [{
        "subject": "FDA", "subj_type": "ORG",
        "relation": "decides",
        "object": "Pfizer", "obj_type": "COMPANY",
        "obj_value": None,
    }]})
    + "\n\n"
    + "INPUT: Federal Reserve cuts benchmark rate by 25 basis points\n"
    + "OUTPUT: "
    + json.dumps({"facts": [{
        "subject": "Fed", "subj_type": "ORG",
        "relation": "changes",
        "object": None, "obj_type": None,
        "obj_value": "federal funds rate -25bp",
    }]})
    + "\n\n"
    + "INPUT: Apple stock falls 3% on weak iPhone sales\n"
    + "OUTPUT: " + json.dumps({"facts": []})
)


_PROMPT_VARIANTS = {
    "v1": _PROMPT_V1_SYSTEM,  # full ontology, no few-shot
    "v2": _PROMPT_V2_SYSTEM,  # ultra-short
    "v3": _PROMPT_V3_SYSTEM,  # v1 + 3 inline examples
}


_USER_TEMPLATE = "Headline: {headline}\nOutput:"


# ---------------------------------------------------------------------------
# OpenRouter call
# ---------------------------------------------------------------------------


_OR_URL = "https://openrouter.ai/api/v1/chat/completions"


@dataclass
class LLMOutcome:
    status: int
    raw_body: str
    parsed_facts: list[dict] = field(default_factory=list)
    parse_error: Optional[str] = None
    latency_ms: float = 0.0
    tokens_in: Optional[int] = None
    tokens_out: Optional[int] = None
    cost_estimate_usd: float = 0.0


# Approximate per-million pricing snapshot 2026-05-23 (USD).
# We use these to estimate cost — not billing-precise, but close enough
# to know whether the bulk migration will cost $20 or $200.
_PRICE_PER_MTOK = {
    "google/gemini-2.5-flash-lite":  {"in": 0.10, "out": 0.40},
    "google/gemini-2.5-flash":       {"in": 0.30, "out": 2.50},
    "openai/gpt-4o-mini":            {"in": 0.15, "out": 0.60},
    "openai/gpt-4.1-mini":           {"in": 0.40, "out": 1.60},
}


def _est_cost(model: str, tokens_in: Optional[int], tokens_out: Optional[int]) -> float:
    if tokens_in is None or tokens_out is None:
        return 0.0
    p = _PRICE_PER_MTOK.get(model)
    if p is None:
        return 0.0
    return (tokens_in / 1_000_000) * p["in"] + (tokens_out / 1_000_000) * p["out"]


def _strip_fence(s: str) -> str:
    """Some providers wrap JSON in ```json ... ``` despite the prompt."""
    import re
    m = re.match(r"^\s*```(?:json)?\s*\n?(.*?)\n?\s*```\s*$", s, re.DOTALL | re.IGNORECASE)
    return m.group(1) if m else s


def _extract_content(body: str) -> Optional[str]:
    try:
        obj = json.loads(body)
    except json.JSONDecodeError:
        return None
    choices = obj.get("choices") or []
    if not choices:
        return None
    msg = choices[0].get("message") or {}
    return msg.get("content")


async def _call(
    client: httpx.AsyncClient,
    *,
    api_key: str,
    model: str,
    system: str,
    user: str,
) -> LLMOutcome:
    t0 = time.monotonic()
    try:
        resp = await client.post(
            _OR_URL,
            headers={"Authorization": f"Bearer {api_key}"},
            json={
                "model": model,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                "temperature": 0,
                "max_tokens": 600,  # bulk extractor needs more headroom than direction
                "response_format": {"type": "json_object"},
            },
        )
    except httpx.HTTPError as exc:
        return LLMOutcome(
            status=0,
            raw_body="",
            parse_error=f"http: {exc.__class__.__name__}",
            latency_ms=(time.monotonic() - t0) * 1000.0,
        )
    latency = (time.monotonic() - t0) * 1000.0
    outcome = LLMOutcome(status=resp.status_code, raw_body=resp.text, latency_ms=latency)
    if resp.status_code != 200:
        outcome.parse_error = f"http_status_{resp.status_code}"
        return outcome
    try:
        parsed = resp.json()
    except json.JSONDecodeError:
        outcome.parse_error = "non_json_response"
        return outcome
    usage = parsed.get("usage") or {}
    outcome.tokens_in = usage.get("prompt_tokens")
    outcome.tokens_out = usage.get("completion_tokens")
    outcome.cost_estimate_usd = _est_cost(model, outcome.tokens_in, outcome.tokens_out)
    content = _extract_content(outcome.raw_body) or ""
    content = _strip_fence(content)
    try:
        parsed_inner = json.loads(content)
    except json.JSONDecodeError:
        outcome.parse_error = "inner_json_decode"
        return outcome
    # Flash-lite often returns the list directly instead of {"facts":[...]};
    # we accept either shape so the experiment shows the MODEL'S behaviour,
    # not the prompt's JSON discipline. Production code will normalise.
    if isinstance(parsed_inner, list):
        facts = parsed_inner
    elif isinstance(parsed_inner, dict):
        facts = parsed_inner.get("facts")
        if not isinstance(facts, list):
            outcome.parse_error = "facts_not_list"
            return outcome
    else:
        outcome.parse_error = "outer_shape_not_dict_or_list"
        return outcome
    # Normalise key names: model may emit `subject`/`subj`, `object`/`obj`,
    # `type`/`subj_type`/`obj_type`. We store the merged shape.
    normalised: list[dict] = []
    for f in facts:
        if not isinstance(f, dict):
            continue
        normalised.append({
            "subj":      f.get("subj") or f.get("subject"),
            "subj_type": f.get("subj_type") or f.get("subject_type") or f.get("type"),
            "relation":  f.get("relation"),
            "obj":       f.get("obj") if "obj" in f else f.get("object"),
            "obj_type":  f.get("obj_type") or f.get("object_type"),
            "obj_value": f.get("obj_value"),
        })
    outcome.parsed_facts = normalised
    return outcome


# ---------------------------------------------------------------------------
# Scoring (qualitative, not strict)
# ---------------------------------------------------------------------------


def _norm(s) -> str:
    if not isinstance(s, str):
        return ""
    return s.strip().lower()


def _fact_key(f: dict) -> tuple:
    """Subject + relation match, ignoring case / type. Object handled
    separately — we tolerate type-only OR name-only matches because the
    cheap model is allowed to differ in form."""
    return (_norm(f.get("subj")), _norm(f.get("relation")))


def _fact_obj_overlap(expected: dict, actual: dict) -> str:
    """Return 'match', 'mismatch', or 'either_null'."""
    e_obj = expected.get("obj")
    a_obj = actual.get("object")
    if e_obj is None and a_obj is None:
        return "match"
    if e_obj is None or a_obj is None:
        return "either_null"
    return "match" if _norm(e_obj) == _norm(a_obj) else "mismatch"


@dataclass
class ExampleScore:
    id: str
    headline: str
    expected: list[dict]
    actual: list[dict]
    matched: int
    missed: list[dict]
    extra: list[dict]
    parse_error: Optional[str]
    latency_ms: float
    cost: float


def _score_example(expected: list[dict], actual: list[dict]) -> tuple[int, list[dict], list[dict]]:
    """Match by (subject, relation) keys; report missed + extras."""
    actual_remaining = list(actual)
    matched = 0
    missed: list[dict] = []
    for e in expected:
        e_key = _fact_key(e)
        found_idx = None
        for i, a in enumerate(actual_remaining):
            if _fact_key(a) == e_key:
                found_idx = i
                break
        if found_idx is None:
            missed.append(e)
        else:
            matched += 1
            actual_remaining.pop(found_idx)
    extras = actual_remaining  # whatever's left = hallucinations
    return matched, missed, extras


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


async def _run(*, prompt_name: str, model: str, limit: Optional[int]) -> dict:
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY not set")
    system = _PROMPT_VARIANTS[prompt_name]
    examples_path = Path(__file__).resolve().parent / "dkg_examples.yaml"
    doc = yaml.safe_load(examples_path.read_text())
    examples = doc.get("examples", [])
    if limit:
        examples = examples[:limit]

    scores: list[ExampleScore] = []
    total_cost = 0.0
    total_latency = 0.0
    async with httpx.AsyncClient(timeout=30.0) as client:
        for ex in examples:
            user = _USER_TEMPLATE.format(headline=ex["headline"])
            outcome = await _call(
                client,
                api_key=api_key,
                model=model,
                system=system,
                user=user,
            )
            expected = ex.get("expected_facts") or []
            matched, missed, extras = _score_example(expected, outcome.parsed_facts)
            scores.append(ExampleScore(
                id=ex["id"],
                headline=ex["headline"],
                expected=expected,
                actual=outcome.parsed_facts,
                matched=matched,
                missed=missed,
                extra=extras,
                parse_error=outcome.parse_error,
                latency_ms=outcome.latency_ms,
                cost=outcome.cost_estimate_usd,
            ))
            total_cost += outcome.cost_estimate_usd
            total_latency += outcome.latency_ms

    total_expected = sum(len(s.expected) for s in scores)
    total_actual = sum(len(s.actual) for s in scores)
    total_matched = sum(s.matched for s in scores)
    parse_errors = sum(1 for s in scores if s.parse_error)

    return {
        "prompt": prompt_name,
        "model": model,
        "n_examples": len(scores),
        "total_expected_facts": total_expected,
        "total_actual_facts": total_actual,
        "total_matched": total_matched,
        "recall": total_matched / total_expected if total_expected else 1.0,
        "precision": total_matched / total_actual if total_actual else 1.0,
        "parse_errors": parse_errors,
        "total_cost_usd": round(total_cost, 4),
        "avg_latency_ms": round(total_latency / max(len(scores), 1), 1),
        "scores": scores,
    }


def _print_report(result: dict, *, show_each: bool = True) -> None:
    print(f"\n=== prompt={result['prompt']}  model={result['model']} ===")
    print(f"  n_examples:    {result['n_examples']}")
    print(f"  expected:      {result['total_expected_facts']}")
    print(f"  actual:        {result['total_actual_facts']}")
    print(f"  matched:       {result['total_matched']}")
    print(f"  precision:     {result['precision']:.3f}")
    print(f"  recall:        {result['recall']:.3f}")
    print(f"  parse_errors:  {result['parse_errors']}")
    print(f"  total_cost:    ${result['total_cost_usd']:.4f}")
    print(f"  avg_latency:   {result['avg_latency_ms']:.0f} ms")
    if show_each:
        print()
        for s in result["scores"]:
            tag = "PARSE-ERR" if s.parse_error else f"{s.matched}/{len(s.expected)}"
            print(f"  [{s.id}] {tag}  {s.headline[:64]}")
            if s.parse_error:
                print(f"      ! {s.parse_error}")
                continue
            for m in s.missed:
                print(f"      - MISS: ({m.get('subj')}) -{m.get('relation')}-> ({m.get('obj')})")
            for x in s.extra:
                print(f"      + EXTRA: ({x.get('subj')}) -{x.get('relation')}-> ({x.get('obj')})  v={x.get('obj_value')}")


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--prompt", choices=list(_PROMPT_VARIANTS), default="v1")
    parser.add_argument("--model", default="google/gemini-2.5-flash-lite")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--quiet", action="store_true",
                        help="suppress per-example detail; show only the summary line.")
    args = parser.parse_args(argv)
    try:
        result = asyncio.run(_run(
            prompt_name=args.prompt,
            model=args.model,
            limit=args.limit,
        ))
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    _print_report(result, show_each=not args.quiet)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
