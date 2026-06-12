"""Resolution-rules gate — H8 lane (cycle 30). Batched LLM classifier.

The retro's one live bet (§8.1): gate on "does this article increment a
STILL-OPEN, rule-relevant perceived probability?" — read the market's
resolution criteria (`markets.description`, shipped by gamma) against
the news and require BOTH:

  - the resolution question is still genuinely open (not already
    determined / merely awaiting formal settlement), and
  - the news bears on the SPECIFIC resolution criteria, not just the
    topic.

Mechanics are cloned from `llm_batch.py` (label batching, JSON parse,
budget/cache flow through the injected `llm_caller`, prompt_version
cache-busting). Runs LAST in the H8 pipeline, only on survivors of the
free gates, so the spend is a small slice of main-pipeline survivors.
Model stays google/gemini-2.5-flash (locked).
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from app.gates.base import Candidate, Gate, GateDecision, GateOutcome
from app.gates.llm_batch import LLMCaller, _chunks, _labels, _trunc

logger = logging.getLogger(__name__)

_SYSTEM_V1 = (
    "You are a resolution-rules analyst for prediction markets. Each "
    "labeled item below pairs a news article with a market's QUESTION "
    "and RESOLUTION CRITERIA. For each, decide whether the news "
    "increments a STILL-OPEN, rule-relevant probability:\n"
    "- `still_open`: the resolution question is genuinely undecided — "
    "NOT already determined in the world and merely awaiting formal "
    "settlement.\n"
    "- `rule_relevant`: the news bears on the SPECIFIC resolution "
    "criteria (the thing the rules actually measure), not merely the "
    "market's topic.\n"
    "- `opens` = still_open AND rule_relevant.\n"
    "Reply JSON only, no markdown fences:\n"
    '{"verdicts": [\n'
    '  {"label": "A", "opens": true, "still_open": true,\n'
    '   "rule_relevant": true, "short_reason": "<≤20 words>"},\n'
    "  ...\n"
    "]}\n"
    "One verdict object per label; do not omit any."
)


def _render_user(labeled: list[tuple[str, Candidate]]) -> str:
    lines: list[str] = []
    for label, c in labeled:
        lines.append(f"[{label}]")
        lines.append(f"  News:     {_trunc(c.article_title, 160)}")
        body = _trunc(c.article_body, 300)
        if body:
            lines.append(f"  Detail:   {body}")
        lines.append(f"  Market:   {_trunc(c.market_question, 160)}")
        lines.append(f"  Rules:    {_trunc(c.market_description, 600)}")
        lines.append("")
    return "\n".join(lines).rstrip()


def _parse(
    raw: str, valid: set[str]
) -> tuple[Optional[set[str]], dict[str, dict], str]:
    """Parse the verdicts response into (accepted_labels, features, err)."""
    if not isinstance(raw, str) or not raw.strip():
        return None, {}, "empty_response"
    s = raw.replace("```json", "").replace("```", "").strip()
    try:
        obj = json.loads(s)
    except (json.JSONDecodeError, ValueError) as exc:
        return None, {}, f"json_decode: {exc.__class__.__name__}"
    if not isinstance(obj, dict):
        return None, {}, "not_object"
    verdicts = obj.get("verdicts")
    if not isinstance(verdicts, list):
        return None, {}, "missing_verdicts_list"
    accepted: set[str] = set()
    features: dict[str, dict] = {}
    for v in verdicts:
        if not isinstance(v, dict):
            continue
        label = v.get("label")
        if not isinstance(label, str):
            continue
        label = label.strip().upper()
        if label not in valid:
            continue
        features[label] = {
            k: v[k] for k in (
                "still_open", "rule_relevant", "short_reason",
            ) if k in v
        }
        if bool(v.get("opens")):
            accepted.add(label)
    return accepted, features, ""


class ResolutionRulesGate(Gate):
    def __init__(
        self,
        *,
        name: str = "resolution_rules",
        llm_caller: LLMCaller,
        model: str = "google/gemini-2.5-flash",
        batch_size: int = 10,
        max_output_tokens: int = 600,
        prompt_version: str = "rr-v1",
    ) -> None:
        self.name = name
        self._llm = llm_caller
        self._model = model
        self._batch_size = max(1, int(batch_size))
        self._max_tokens = int(max_output_tokens)
        self._prompt_version = str(prompt_version)

    async def evaluate(
        self, candidates: list[Candidate]
    ) -> list[GateDecision]:
        out: list[GateDecision] = []
        for batch in _chunks(candidates, self._batch_size):
            out.extend(await self._evaluate_one_batch(batch))
        return out

    async def _evaluate_one_batch(
        self, batch: list[Candidate]
    ) -> list[GateDecision]:
        labels = _labels(len(batch))
        labeled = list(zip(labels, batch))
        valid = set(labels)
        try:
            raw = await self._llm(
                system=_SYSTEM_V1,
                user=_render_user(labeled),
                model=self._model,
                max_tokens=self._max_tokens,
                prompt_version=self._prompt_version,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "resolution_rules caller raised %s", exc.__class__.__name__
            )
            return [
                GateDecision(
                    candidate=c, gate_name=self.name,
                    outcome=GateOutcome.REJECTED,
                    reason=f"llm_error: {exc.__class__.__name__}",
                )
                for c in batch
            ]
        accepted, features, err = _parse(raw, valid)
        if accepted is None:
            return [
                GateDecision(
                    candidate=c, gate_name=self.name,
                    outcome=GateOutcome.REJECTED,
                    reason=f"parse_error: {err}",
                    metadata={"raw": raw[:200]},
                )
                for c in batch
            ]
        results: list[GateDecision] = []
        for label, c in labeled:
            meta: dict = {"label": label, "batch_size": len(batch)}
            f = features.get(label)
            if f:
                meta["features"] = f
            if label in accepted:
                results.append(GateDecision(
                    candidate=c, gate_name=self.name,
                    outcome=GateOutcome.PASSED, metadata=meta,
                ))
            else:
                results.append(GateDecision(
                    candidate=c, gate_name=self.name,
                    outcome=GateOutcome.REJECTED,
                    reason="not_rule_opening", metadata=meta,
                ))
        return results
