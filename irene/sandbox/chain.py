"""Chain executor for multi-step strategies.

Each step's output is parsed as JSON. Later steps may reference earlier
step outputs in their `user_template` via `{{stepname.field}}` (or
`{{stepname.field.sub}}` for nested dicts). Three step types:

  - single             : one call, parsed as JSON, stored at chain[name]
  - self_consistency   : N calls (temperature>0), aggregate one field
                         (mean/median), stored at chain[name][field]
  - critic             : one call that reviews `base` step's output and
                         optionally revises it. Output schema is the same
                         as the base; if accept=false, the critic's
                         fields override.

Cache keys are per-(system, user, model, strategy_version, temperature,
extra_key). Self-consistency uses extra_key=str(sample_index) so the N
samples each get their own cache entry.

Returns a ChainResult with the parsed step map + any per-step errors.
"""

from __future__ import annotations

import copy
import json
import re
import statistics
import sys
from dataclasses import dataclass, field
from typing import Any, Optional

from irene.sandbox import llm
from irene.sandbox.loader import SandboxRow
from irene.sandbox.strategies import StepConfig, Strategy, render_user_prompt

_PATH = re.compile(r"\{\{\s*([a-zA-Z_][\w.\[\]]*)\s*\}\}")


@dataclass
class ChainResult:
    steps: dict[str, Any] = field(default_factory=dict)   # name -> parsed obj
    errors: dict[str, str] = field(default_factory=dict)  # name -> error
    calls_made: int = 0
    cache_hits: int = 0
    raw_traces: list[dict] = field(default_factory=list)  # per-call audit log

    def resolve(self, dotted: str) -> Optional[Any]:
        """Resolve a dotted path like 'p_yes.p_yes' or 'critic.p_yes'."""
        parts = dotted.split(".")
        cur: Any = self.steps
        for p in parts:
            if isinstance(cur, dict) and p in cur:
                cur = cur[p]
            else:
                return None
        return cur


def _base_ctx(strategy: Strategy, row: SandboxRow) -> dict:
    # Reuse the single-step user-prompt context builder, then we just
    # interpolate {{stepname.field}} from the live chain state.
    user_only = render_user_prompt(
        Strategy(
            name=strategy.name, version=strategy.version,
            description=strategy.description, model=strategy.model,
            max_output_tokens=strategy.max_output_tokens,
            system_prompt="", user_template="",  # not used
            article_body_max_chars=strategy.article_body_max_chars,
            require_direction=False, require_p_yes=False,
            derive_direction_from_p_yes=False,
        ),
        row,
    )
    # `render_user_prompt` returns the empty string when user_template is
    # empty; instead we want the ctx dict it built. Inline a copy here.
    body = (row.article_body or row.article_summary or "").strip()
    if len(body) > strategy.article_body_max_chars:
        body = body[:strategy.article_body_max_chars].rstrip() + " […truncated]"
    description = (row.market_description or "").strip()
    if len(description) > 400:
        description = description[:400].rstrip() + " […truncated]"
    return {
        "article_title": (row.article_title or "").strip(),
        "article_body": body,
        "article_body_or_summary": body,
        "article_source": row.article_source or "",
        "article_tier": row.article_tier or "",
        "article_breaking_score": _fmt(row.article_breaking_score),
        "market_question": row.market_question.strip(),
        "market_description": description,
        "market_end_date": row.market_end_date or "n/a",
        "market_mid_t0": _fmt(row.market_mid_t0),
        "hours_to_expiry": _fmt(row.hours_to_expiry, "{:.1f}"),
        "freshness": row.orig_freshness or "",
    }


def _fmt(v, fmt="{:.3f}"):
    return fmt.format(v) if isinstance(v, (int, float)) else "n/a"


def _interpolate(template: str, ctx: dict) -> str:
    def repl(m):
        raw = m.group(1)
        # Walk dotted path through ctx (which contains both row fields
        # and chain step outputs as nested dicts).
        parts = raw.split(".")
        cur: Any = ctx
        for p in parts:
            if isinstance(cur, dict) and p in cur:
                cur = cur[p]
            elif isinstance(cur, list) and p.isdigit():
                idx = int(p)
                if 0 <= idx < len(cur):
                    cur = cur[idx]
                else:
                    return ""
            else:
                return ""
        if isinstance(cur, (list, dict)):
            return json.dumps(cur, ensure_ascii=False)
        return str(cur)
    return _PATH.sub(repl, template)


def _parse_json(content: str) -> tuple[Optional[dict], str]:
    if not isinstance(content, str) or not content.strip():
        return None, "empty_response"
    s = content.replace("```json", "").replace("```", "").strip()
    try:
        obj = json.loads(s)
    except (json.JSONDecodeError, ValueError) as exc:
        return None, f"json_decode: {exc.__class__.__name__}"
    if not isinstance(obj, dict):
        return None, "not_object"
    return obj, ""


def run_chain(
    strategy: Strategy,
    row: SandboxRow,
    *,
    max_calls_remaining: int,
    dry_run: bool = False,
    log_progress: bool = False,
) -> ChainResult:
    """Execute the chain serially. Stops at the first hard error
    (no later step can run if its declared input failed)."""
    out = ChainResult()
    ctx = _base_ctx(strategy, row)

    for step in strategy.steps:
        # Verify declared inputs exist (allows skipping later steps if
        # an earlier one errored).
        missing = [inp for inp in step.inputs if inp not in out.steps]
        if missing:
            out.errors[step.name] = f"missing_inputs: {missing}"
            continue

        if step.type == "single":
            obj, err = _run_one(
                step, strategy, ctx, out,
                max_calls_remaining=max_calls_remaining,
                dry_run=dry_run, log_progress=log_progress, row=row,
            )
            if obj is not None:
                out.steps[step.name] = obj
                ctx[step.name] = obj
            else:
                out.errors[step.name] = err
        elif step.type == "self_consistency":
            obj, err = _run_self_consistency(
                step, strategy, ctx, out,
                max_calls_remaining=max_calls_remaining,
                dry_run=dry_run, log_progress=log_progress, row=row,
            )
            if obj is not None:
                out.steps[step.name] = obj
                ctx[step.name] = obj
            else:
                out.errors[step.name] = err
        elif step.type == "critic":
            obj, err = _run_critic(
                step, strategy, ctx, out,
                max_calls_remaining=max_calls_remaining,
                dry_run=dry_run, log_progress=log_progress, row=row,
            )
            if obj is not None:
                out.steps[step.name] = obj
                ctx[step.name] = obj
            else:
                out.errors[step.name] = err
        max_calls_remaining = max(0, max_calls_remaining - 0)  # noop alias
    return out


def _run_one(step, strategy, ctx, out, *, max_calls_remaining, dry_run,
             log_progress, row, temperature=0.0, extra_key=""):
    user = _interpolate(step.user_template, ctx)
    sha = llm.prompt_sha(
        system=step.system, user=user,
        model=strategy.model, strategy_version=strategy.version,
        temperature=temperature, extra_key=extra_key,
    )
    cached = llm.load_cached(sha)
    if cached is None:
        if dry_run:
            return None, "dry_run_miss"
        if max_calls_remaining <= 0:
            return None, "max_calls_reached"
        if log_progress:
            print(f"      step={step.name}  type={step.type}  CALL (sha {sha[:8]})",
                  file=sys.stderr, flush=True)
        result = llm.call(
            system=step.system, user=user,
            model=strategy.model, strategy_version=strategy.version,
            max_tokens=step.max_output_tokens,
            temperature=temperature, extra_key=extra_key,
        )
        out.calls_made += 1
    else:
        if log_progress:
            print(f"      step={step.name}  type={step.type}  cache  (sha {sha[:8]})",
                  file=sys.stderr, flush=True)
        result = cached
        out.cache_hits += 1

    out.raw_traces.append({
        "step": step.name, "sha": sha, "cached": result.cached,
        "content": result.content,
    })
    obj, err = _parse_json(result.content)
    return obj, err


def _run_self_consistency(step, strategy, ctx, out, *, max_calls_remaining,
                          dry_run, log_progress, row):
    samples = []
    parse_errors = []
    temp = step.temperature if step.temperature > 0 else 0.7
    for i in range(step.n):
        remaining = max_calls_remaining - out.calls_made
        obj, err = _run_one(
            step, strategy, ctx, out,
            max_calls_remaining=remaining,
            dry_run=dry_run, log_progress=log_progress, row=row,
            temperature=temp, extra_key=f"sc{i}",
        )
        if obj is None:
            parse_errors.append(err)
            continue
        samples.append(obj)

    if not samples:
        return None, parse_errors[0] if parse_errors else "no_samples"

    vals = [s.get(step.aggregate_field) for s in samples
            if isinstance(s.get(step.aggregate_field), (int, float))]
    if not vals:
        return None, "no_numeric_aggregate_field"
    agg = statistics.median(vals) if step.aggregate == "median" else statistics.mean(vals)
    return {
        step.aggregate_field: agg,
        "samples": samples,
        "n_samples": len(samples),
        "n_parse_errors": len(parse_errors),
    }, ""


def _run_critic(step, strategy, ctx, out, *, max_calls_remaining,
                dry_run, log_progress, row):
    if not step.base or step.base not in out.steps:
        return None, f"base_step_missing: {step.base!r}"
    obj, err = _run_one(
        step, strategy, ctx, out,
        max_calls_remaining=max_calls_remaining,
        dry_run=dry_run, log_progress=log_progress, row=row,
    )
    if obj is None:
        return None, err
    accept = obj.get("accept", True)
    if accept:
        # Pass through the base step's values but record that a critic ran.
        merged = copy.deepcopy(out.steps[step.base])
        merged["critic_accepted"] = True
        merged["critic_reason"] = obj.get("reason", "")
        return merged, ""
    # Critic overrides: take the critic's fields and let it replace base's.
    merged = copy.deepcopy(out.steps[step.base])
    merged.update({k: v for k, v in obj.items() if k != "accept"})
    merged["critic_accepted"] = False
    return merged, ""
