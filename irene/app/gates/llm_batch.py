"""Bundled LLM binary-classifier gate.

Takes up to `batch_size` candidates, labels them with short codes
(A, B, ..., Z, AA, AB, ...), asks the LLM in one call for the list of
labels that are related to their paired markets, and parses the JSON
response back into per-candidate decisions.

Token cost is amortized across the batch: one system prompt and one
short JSON output for ~20 (article, market) pairs vs N separate calls.

`llm_caller` is an async function: `await llm_caller(system, user,
model, max_tokens, prompt_version) -> str` returning the LLM's text
response. Injected so the gate doesn't depend on a specific client.
"""

from __future__ import annotations

import json
import logging
import re
import string
from typing import Awaitable, Callable, Optional

from app.gates.base import Candidate, Gate, GateDecision, GateOutcome

logger = logging.getLogger(__name__)

LLMCaller = Callable[..., Awaitable[str]]

_SYSTEM_V1 = (
    "You are a relevance classifier for prediction markets. For each "
    "labeled (news, market) pair below, decide whether the news is "
    "plausibly relevant to the market — i.e. could it move the market's "
    "price. Reply JSON only, no markdown fences: "
    '{"related": ["A", "C", ...]}. '
    "Include only labels whose pair is plausibly related. Be strict: "
    "off-entity, off-topic, or too-generic news is not related."
)

# v2: same pass/fail criterion as v1 (the `related` bool gates capture),
# but emit per-candidate descriptive features alongside it. These are
# persisted into `explorations.gate_history` so downstream sandbox
# strategies can filter without re-LLM-ing. Fields are intentionally
# *descriptive*, not prescriptive — no field is a "should we trade"
# verdict, so future strategies aren't constrained by this prompt's
# point of view.
_SYSTEM_V2 = (
    "You are a relevance classifier for prediction markets. For each "
    "labeled (news, market) pair below, return a verdict object. "
    "`related`=true iff the news plausibly moves the market's price "
    "(strict: off-entity / off-topic / too-generic news is NOT "
    "related). For every pair — related or not — also fill in the "
    "descriptive features.\n"
    "Reply JSON only, no markdown fences:\n"
    '{"verdicts": [\n'
    '  {"label": "A",\n'
    '   "related": true,\n'
    '   "entities_overlap": ["GOOGL", "Alphabet"],\n'
    '   "claim_horizon_hours": 4,\n'
    '   "directional_lean": "bearish",\n'
    '   "novelty": "breaking",\n'
    '   "short_reason": "<≤20 words>"},\n'
    "  ...\n"
    "]}\n"
    "`entities_overlap`: list of entities (companies, tickers, people, "
    "countries, events) that appear in BOTH the news and the market; "
    'empty list if none.\n'
    "`claim_horizon_hours`: integer estimate of how many hours until "
    "the news's impact on the market should be priced in. Use 0 for "
    "instant, -1 if unclear.\n"
    "`directional_lean` ∈ "
    '{"bullish","bearish","neutral","unclear"} — direction the news '
    'pushes the YES side of the market. "neutral" = informational only.\n'
    "`novelty` ∈ "
    '{"breaking","developing","recap","opinion"} — best guess at the '
    "news article's type.\n"
    "Include ONE verdict object per label; do not omit unrelated ones."
)


def _system_for(prompt_version: str) -> str:
    if prompt_version == "v1":
        return _SYSTEM_V1
    return _SYSTEM_V2


def _labels(n: int) -> list[str]:
    """A, B, ..., Z, AA, AB, ..., ZZ. 26 + 676 = 702 labels."""
    out = []
    letters = string.ascii_uppercase
    for c in letters:
        out.append(c)
        if len(out) >= n:
            return out
    for c1 in letters:
        for c2 in letters:
            out.append(c1 + c2)
            if len(out) >= n:
                return out
    raise ValueError(f"batch size {n} exceeds label space (702)")


def _trunc(s: Optional[str], n: int) -> str:
    if not s:
        return ""
    s = s.strip()
    if len(s) <= n:
        return s
    return s[:n].rstrip() + " […truncated]"


def _render_user(labeled: list[tuple[str, Candidate]]) -> str:
    lines = []
    for label, c in labeled:
        lines.append(f"[{label}]")
        lines.append(f"  News:   {_trunc(c.article_title, 160)}")
        lines.append(f"  Market: {_trunc(c.market_question, 160)}")
        lines.append("")
    return "\n".join(lines).rstrip()


def _parse(
    raw: str, valid: set[str]
) -> tuple[Optional[set[str]], dict[str, dict], str]:
    """Parse v1 or v2 LLM response into (accepted_labels, features, err).

    - v1 shape: `{"related": ["A", "C", ...]}` — features is `{}`.
    - v2 shape: `{"verdicts": [{"label": "A", "related": true, ...}, ...]}`
      — features is a dict keyed by label with the descriptive fields.
    """
    if not isinstance(raw, str) or not raw.strip():
        return None, {}, "empty_response"
    s = raw.replace("```json", "").replace("```", "").strip()
    try:
        obj = json.loads(s)
    except (json.JSONDecodeError, ValueError) as exc:
        return None, {}, f"json_decode: {exc.__class__.__name__}"
    if not isinstance(obj, dict):
        return None, {}, "not_object"
    # v2 shape first — `verdicts` list of per-candidate objects.
    verdicts = obj.get("verdicts")
    if isinstance(verdicts, list):
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
                    "entities_overlap",
                    "claim_horizon_hours",
                    "directional_lean",
                    "novelty",
                    "short_reason",
                ) if k in v
            }
            if bool(v.get("related")):
                accepted.add(label)
        return accepted, features, ""
    # v1 fallback shape — plain list of accepted labels.
    related = obj.get("related")
    if not isinstance(related, list):
        return None, {}, "missing_related_list"
    accepted_v1: set[str] = set()
    for x in related:
        if not isinstance(x, str):
            continue
        label = x.strip().upper()
        if label in valid:
            accepted_v1.add(label)
    return accepted_v1, {}, ""


class LLMBatchGate(Gate):
    def __init__(
        self,
        *,
        name: str = "llm_batch",
        llm_caller: LLMCaller,
        model: str = "google/gemini-2.5-flash",
        batch_size: int = 20,
        max_output_tokens: int = 40,
        prompt_version: str = "v1",
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
        user = _render_user(labeled)
        valid = set(labels)
        try:
            raw = await self._llm(
                system=_system_for(self._prompt_version),
                user=user,
                model=self._model,
                max_tokens=self._max_tokens,
                prompt_version=self._prompt_version,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("llm_batch caller raised %s", exc.__class__.__name__)
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
            # Attach per-candidate v2 descriptive features when present.
            # Stored under `metadata.features` so callers don't have to
            # care which prompt version produced the row.
            f = features.get(label)
            if f:
                meta["features"] = f
            if label in accepted:
                results.append(GateDecision(
                    candidate=c, gate_name=self.name,
                    outcome=GateOutcome.PASSED,
                    metadata=meta,
                ))
            else:
                results.append(GateDecision(
                    candidate=c, gate_name=self.name,
                    outcome=GateOutcome.REJECTED,
                    reason="llm_unrelated",
                    metadata=meta,
                ))
        return results


def _chunks(seq: list, n: int):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]
