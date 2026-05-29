"""Strategy YAML: prompt template + decision schema + PnL params.

Strategies live under `irene/sandbox/strategies/*.yaml` (versioned). The
strategy file defines:
  - the LLM prompt (system + user template with {{placeholders}})
  - which row fields get inlined
  - the decision-parser config (require direction?)
  - the PnL backtest knobs (entry quote, fee, notional, exit horizon)
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from typing import Any

import yaml

from irene.sandbox.loader import SandboxRow
from irene.sandbox.paths import STRATEGIES_DIR

_PLACEHOLDER = re.compile(r"\{\{\s*([a-zA-Z_][a-zA-Z_0-9]*)\s*\}\}")


@dataclass
class StepConfig:
    name: str
    type: str                  # "single" | "self_consistency" | "critic"
    system: str
    user_template: str
    max_output_tokens: int
    inputs: list[str] = field(default_factory=list)
    # self_consistency only
    n: int = 1
    temperature: float = 0.0
    aggregate: str = "mean"    # "mean" | "median"
    aggregate_field: str = "p_yes"
    # critic only
    base: str = ""


@dataclass
class Strategy:
    name: str
    version: str
    description: str
    model: str
    max_output_tokens: int
    system_prompt: str
    user_template: str
    article_body_max_chars: int
    require_direction: bool
    require_p_yes: bool
    derive_direction_from_p_yes: bool
    backtest: dict[str, Any] = field(default_factory=dict)
    # Multi-step. Empty list => single-step legacy mode.
    steps: list[StepConfig] = field(default_factory=list)
    # Dot-path into the chain output for the value Kelly should size on
    # (e.g. "p_yes.p_yes" or "critic.p_yes"). Required when steps is set.
    final_field: str = ""
    # Non-LLM strategies set kind to a known token; run_strategy then
    # branches before any prompt rendering. Empty => standard LLM path.
    kind: str = ""
    # Per-kind sub-config bag (e.g. momentum: {window_s, threshold}).
    kind_config: dict[str, Any] = field(default_factory=dict)
    # Optional second-stage pre-trade gate. Rows failing any predicate are
    # skipped (skip_reason "pretrade:<reason>") before the LLM/sizing path.
    # Keys: min_volume_usdc, mid_band [lo,hi], min_pre_stdev,
    # require_llm_related (bool), max_entry_spread.
    pretrade: dict[str, Any] = field(default_factory=dict)


def strategy_path(name: str) -> str:
    return os.path.join(STRATEGIES_DIR, f"{name}.yaml")


def load(name: str) -> Strategy:
    with open(strategy_path(name)) as f:
        raw = yaml.safe_load(f)
    prompt = raw.get("prompt", {})
    features = raw.get("features", {})
    parse = raw.get("parse", {})
    raw_steps = raw.get("steps") or []
    steps = [_step_from_yaml(s) for s in raw_steps]
    # Single-step strategies leave prompt/user_template at the top level;
    # multi-step strategies live in steps and may omit the top-level prompt.
    kind = str(raw.get("kind", "") or "")
    kind_config: dict[str, Any] = {}
    if kind == "deterministic_momentum":
        kind_config = dict(raw.get("momentum", {}) or {})
    return Strategy(
        name=raw["name"],
        version=raw.get("version", "v1"),
        description=raw.get("description", ""),
        model=raw.get("model", "google/gemini-2.5-flash"),
        max_output_tokens=int(raw.get("max_output_tokens", 120)),
        system_prompt=prompt.get("system", "").rstrip() if prompt else "",
        user_template=prompt.get("user_template", "").rstrip() if prompt else "",
        article_body_max_chars=int(features.get("article_body_max_chars", 800)),
        require_direction=bool(parse.get("require_direction", True)),
        require_p_yes=bool(parse.get("require_p_yes", False)),
        derive_direction_from_p_yes=bool(parse.get("derive_direction_from_p_yes", False)),
        backtest=dict(raw.get("backtest", {})),
        steps=steps,
        final_field=str(parse.get("final_field", "")),
        kind=kind,
        kind_config=kind_config,
        pretrade=dict(raw.get("pretrade", {}) or {}),
    )


def _step_from_yaml(s: dict) -> StepConfig:
    t = s.get("type", "single")
    if t not in ("single", "self_consistency", "critic"):
        raise ValueError(f"step {s.get('name')!r}: unknown type {t!r}")
    return StepConfig(
        name=s["name"],
        type=t,
        system=s["system"].rstrip(),
        user_template=s["user_template"].rstrip(),
        max_output_tokens=int(s.get("max_output_tokens", 80)),
        inputs=list(s.get("inputs") or []),
        n=int(s.get("n", 1)),
        temperature=float(s.get("temperature", 0.0)),
        aggregate=str(s.get("aggregate", "mean")),
        aggregate_field=str(s.get("aggregate_field", "p_yes")),
        base=str(s.get("base", "")),
    )


def list_strategies() -> list[str]:
    if not os.path.isdir(STRATEGIES_DIR):
        return []
    return sorted(
        os.path.splitext(f)[0]
        for f in os.listdir(STRATEGIES_DIR)
        if f.endswith(".yaml")
    )


def render_user_prompt(strategy: Strategy, row: SandboxRow) -> str:
    body = (row.article_body or row.article_summary or "").strip()
    if len(body) > strategy.article_body_max_chars:
        body = body[:strategy.article_body_max_chars].rstrip() + " […truncated]"
    description = (row.market_description or "").strip()
    if len(description) > 400:
        description = description[:400].rstrip() + " […truncated]"
    ctx = {
        "article_title": (row.article_title or "").strip(),
        "article_body": body,
        "article_body_or_summary": body,
        "article_source": row.article_source or "",
        "market_question": row.market_question.strip(),
        "market_description": description,
        "market_end_date": row.market_end_date or "n/a",
        "market_mid_t0": _fmt(row.market_mid_t0),
        "market_spread_t0": _fmt(row.market_spread_t0),
        "hours_to_expiry": _fmt(row.hours_to_expiry, "{:.1f}"),
        "article_tier": row.article_tier or "",
        "article_breaking_score": _fmt(row.article_breaking_score),
        "freshness": row.orig_freshness or "",
        "cosine_similarity": _fmt(row.orig_cosine_similarity),
        "dkg_final_score": _fmt(row.orig_dkg_final_score),
    }
    return _PLACEHOLDER.sub(lambda m: str(ctx.get(m.group(1), "")), strategy.user_template)


def _fmt(v, fmt="{:.3f}"):
    return fmt.format(v) if isinstance(v, (int, float)) else "n/a"


# ----- response parsing -----

@dataclass
class Decision:
    related: bool
    direction: str           # "YES" | "NO" | "SKIP" | ""
    confidence: float
    reason: str
    p_yes: float = -1.0      # -1.0 sentinel = absent
    parse_error: str = ""    # non-empty => parse failed; other fields default


def parse_response(
    raw: str, *, require_direction: bool = False, require_p_yes: bool = False,
) -> Decision:
    if not isinstance(raw, str) or not raw.strip():
        return Decision(False, "", 0.0, "", -1.0, "empty_response")
    s = raw.replace("```json", "").replace("```", "").strip()
    try:
        obj = json.loads(s)
    except (json.JSONDecodeError, ValueError):
        return Decision(False, "", 0.0, "", -1.0, "json_decode")
    if not isinstance(obj, dict):
        return Decision(False, "", 0.0, "", -1.0, "not_object")

    p_yes = _p_yes(obj)
    if require_p_yes and p_yes < 0:
        return Decision(False, "", 0.0, _reason(obj), -1.0, "missing_p_yes")

    if require_p_yes:
        # p_yes-driven strategy: related/direction not required from LLM
        return Decision(
            related=True,
            direction="",
            confidence=_confidence(obj, True),
            reason=_reason(obj),
            p_yes=p_yes,
        )

    rel_raw = obj.get("related")
    related = _coerce_bool(rel_raw)
    if related is None:
        return Decision(False, "", 0.0, "", p_yes, "missing_related")

    dir_raw = (obj.get("direction") or "").strip().upper() if isinstance(obj.get("direction"), str) else ""
    if dir_raw not in ("YES", "NO", "SKIP", ""):
        dir_raw = ""
    if require_direction and related and dir_raw == "":
        return Decision(related, "", _confidence(obj, related), _reason(obj), p_yes, "missing_direction")

    return Decision(
        related=related,
        direction=dir_raw if related else "",
        confidence=_confidence(obj, related),
        reason=_reason(obj),
        p_yes=p_yes,
    )


def derive_direction(p_yes: float, mid: float, min_edge: float) -> str:
    """Pick YES/NO/'' from a p_yes estimate vs current mid."""
    if p_yes < 0 or mid is None:
        return ""
    if p_yes > mid + min_edge:
        return "YES"
    if p_yes < mid - min_edge:
        return "NO"
    return ""


def _p_yes(obj: dict) -> float:
    raw = obj.get("p_yes")
    if raw is None:
        return -1.0
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return -1.0
    return max(0.0, min(1.0, v))


def _coerce_bool(v):
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        t = v.strip().lower()
        if t in ("true", "yes", "related", "1"):
            return True
        if t in ("false", "no", "unrelated", "0"):
            return False
    return None


def _confidence(obj: dict, related: bool) -> float:
    raw = obj.get("confidence")
    try:
        c = float(raw) if raw is not None else (0.5 if related else 0.0)
    except (TypeError, ValueError):
        c = 0.5 if related else 0.0
    return max(0.0, min(1.0, c))


def _reason(obj: dict) -> str:
    r = obj.get("reason") or ""
    if not isinstance(r, str):
        return ""
    return r.strip()[:300]
