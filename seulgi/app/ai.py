"""Seulgi AI direction classifier — OpenRouter backend.

See the `## Specification` block below for the canonical surface. Top-line:
batched + chunked classification (one API call per `batch_size` markets),
N-sample ensemble at higher temperature with majority-vote + 2/3 agreement
gate, tiered model routing (premium when max sim ≥ threshold), strict
`response_format=json_schema` with `<scratch>` CoT preamble + regex-extract
fallback, and `SEULGI_BYPASS_LLM=1` for zero-spend dry-runs.

OpenRouter API reference: https://openrouter.ai/docs
"""

# ## Specification ──────────────────────────────────────────────────────────────
# Purpose:
#   OpenRouter chat-completion client + Stage-3 ensemble direction
#   classifier. Given a headline + candidate Polymarket questions (already
#   culled by similarity/relevance), emits a per-market probability tuple
#   (p_yes, p_no, p_neutral, p_alpha, risk) plus an aggregated direction +
#   confidence. Stage 2 (relevance LLM) was retired 2026-05-22; this is the
#   only remote LLM call in the hot path.
#
# Public interface:
#   DirectionClassifier(api_key, model, premium_model=None,
#                       premium_sim_threshold=0.70, ensemble_samples=3,
#                       ensemble_temperature=0.5, batch_size=10)
#     .classify_batch(headline, matches, *, is_republish, similar_to,
#                     source, freshness, relevance_verdicts)
#         -> (list[ClassifyResult], prompt_meta: dict)
#     .classify(headline, question, ...) -> ClassifyResult   # legacy fallback
#     .close()   # closes the aiohttp session
#   Helpers: `_BATCH_PROMPT_TEMPLATE`, `_parse_one`, `_parse_batch_response`,
#   `_aggregate_ensemble`, `_regex_extract_partial`, `_RESPONSE_SCHEMA`.
#
# A4E.4 (2026-05-23) — prompt + parse upgrade:
#   - `response_format=json_schema` (strict) replaces `json_object` when
#     the provider supports it; a 400 with a schema-unsupported marker
#     auto-flips the instance to `json_object` for its lifetime.
#   - `<scratch>...</scratch>` chain-of-thought block in the prompt;
#     `_extract_text` strips everything before the first `{`/`[` so the
#     scratch content is discarded (memory `feedback_reasoning_budget`).
#   - `_aggregate_ensemble` enforces a hard 2/3 agreement gate
#     (`_AGREEMENT_DEMOTE_THRESHOLD`). agreement < 2/3 → NEUTRAL with
#     confidence=0.0, regardless of stated p_alpha.
#   - `batch_size` (default 10) chunks `matches` into parallel sub-batches.
#     Results are concatenated in input order.
#   - `_regex_extract_partial` recovers per-market tuples from malformed
#     JSON; accepted only when ≥ 50% of expected markets are recovered.
#   - Few-shot exemplars (4E.4(e)) DEFERRED — TODO at prompt-build path.
#
# Invariants:
#   - Ensemble = N parallel calls at `ensemble_temperature`; per-market
#     mean(p_*) + majority-vote argmax; ties → NEUTRAL.
#   - `agreement` = max_votes/N. agreement < 2/3 → NEUTRAL, conf=0.0
#     (A4E.4 belt-and-braces; trader's `min_agreement` is the soft gate).
#   - `confidence` = `p_alpha` when present (cycle-7 contract); else
#     mean(P[winner]). KNOWN AMBIGUITY (A0.3): trader's `2·conf − 1`
#     edge silently changes semantics between samples.
#   - All N samples fail to parse AND regex recovers < 50% → per-market
#     single-call classify(); those failing → NEUTRAL conf=0.0. Never raises.
#   - SEULGI_BYPASS_LLM=1 short-circuits before any HTTP (A1.6).
#
# Side effects: HTTPS POSTs to OpenRouter `/chat/completions` (one per
# ensemble sample, semaphore-throttled); A2.1 (2026-05-22) DB writes to
# `llm_calls` + `llm_parse_failures` when `db` is passed (errors swallowed
# so logging never blocks the trade path). engine.py writes the paired
# `usage_events` row.
#
# Failure modes: 429 → expo backoff 3 attempts; other non-200 → None
# (HTTP 402 silently treated as generic failure — budget.py owns pause).
# Timeout/connection error → None. Parse error → regex-extract fallback;
# < 50% recovered → per-market classify() fallback; that fails → NEUTRAL.
#
# Non-goals: pre-AI rules, sizing/gating/Kelly/fees, credit polling,
# decision_inputs persistence.
# ───────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import asyncio
import datetime as _dt
import hashlib
import json
import logging
import os
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import aiohttp

from profiler import prof

logger = logging.getLogger(__name__)

_DEFAULT_API_BASE = "https://openrouter.ai/api/v1"
_CHAT_PATH = "/chat/completions"

# A2.1 (2026-05-22): caps for response_text writes. Truncated responses get
# the `[...truncated]` suffix so post-hoc tooling can detect truncation.
_LLM_CALL_RESPONSE_CAP = 8 * 1024
_LLM_CALL_TRUNC_SUFFIX = "[...truncated]"
_LLM_PARSE_FAILURE_RESPONSE_CAP = 4 * 1024

# A4E.4 (2026-05-23): strict JSON schema for Stage-3. OpenRouter passes
# this to providers that support OpenAI-compatible `json_schema`. A 400
# matching `_SCHEMA_UNSUPPORTED_MARKERS` flips the instance to `json_object`
# for the remainder of its lifetime.
_RESPONSE_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "headline_classification",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "decisions": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "idx": {"type": "integer"},
                            "p_yes": {"type": "number", "minimum": 0, "maximum": 1},
                            "p_no": {"type": "number", "minimum": 0, "maximum": 1},
                            "p_neutral": {"type": "number", "minimum": 0, "maximum": 1},
                            "p_alpha": {"type": "number", "minimum": 0, "maximum": 1},
                            "reason": {"type": "string"},
                        },
                        "required": ["idx", "p_yes", "p_no", "p_neutral",
                                     "p_alpha", "reason"],
                    },
                },
            },
            "required": ["decisions"],
        },
    },
}

# A4E.4: hard 2/3 agreement gate enforced in `_aggregate_ensemble`. Below
# this, direction is NEUTRAL and confidence is 0.0 (independent of trader's
# `min_agreement` soft skip).
_AGREEMENT_DEMOTE_THRESHOLD = 2.0 / 3.0

# A4E.4: regex-extract fallback for malformed JSON.
_PARTIAL_OBJECT_RE = re.compile(
    r'"idx"\s*:\s*(?P<idx>\d+).*?'
    r'"p_yes"\s*:\s*(?P<p_yes>[0-9.]+).*?'
    r'"p_no"\s*:\s*(?P<p_no>[0-9.]+).*?'
    r'"p_neutral"\s*:\s*(?P<p_neutral>[0-9.]+)'
    r'(?:.*?"p_alpha"\s*:\s*(?P<p_alpha>[0-9.]+))?',
    re.DOTALL,
)

# A4E.4: case-insensitive substrings indicating a provider rejected json_schema.
_SCHEMA_UNSUPPORTED_MARKERS = (
    "json_schema", "json schema", "response_format.*schema",
    "schema is not supported", "unsupported response_format",
)


def _truncate_response(text: str, cap: int) -> str:
    """Cap text at `cap` bytes, preserving the truncation suffix verbatim.

    Operates on string length (not byte length) because sqlite stores
    TEXT as UTF-8 and our prompts/responses are ASCII-dominant; this is
    a 1:1 approximation for the common case and stays cheap for the hot
    path. If text is already within cap, returns it unchanged.
    """
    if text is None:
        return ""
    if len(text) <= cap:
        return text
    head = cap - len(_LLM_CALL_TRUNC_SUFFIX)
    if head < 0:  # cap is so small the suffix wouldn't fit — emit just the suffix
        return _LLM_CALL_TRUNC_SUFFIX[:cap]
    return text[:head] + _LLM_CALL_TRUNC_SUFFIX

# A1.6: operator toggle — SEULGI_BYPASS_LLM=1 short-circuits Stage-3 with
# deterministic favorite-side results (zero OpenRouter spend; trader gates
# still apply). Read fresh each call so tests can toggle via monkeypatch.
_BYPASS_ENV_VAR = "SEULGI_BYPASS_LLM"
_BYPASS_MODEL_TAG = "llm_bypass"


def _bypass_enabled() -> bool:
    return os.environ.get(_BYPASS_ENV_VAR, "0") == "1"


_BYPASS_STARTUP_LOGGED = False  # reset to False in tests to re-observe


def _log_bypass_startup_once() -> None:
    global _BYPASS_STARTUP_LOGGED
    if _BYPASS_STARTUP_LOGGED:
        return
    _BYPASS_STARTUP_LOGGED = True
    logger.info(
        "[ai] LLM_BYPASS active — env %s=1; Stage-3 returns favorite-side "
        "synthetic results, $0 OpenRouter spend", _BYPASS_ENV_VAR,
    )

_SYSTEM_MESSAGE = """\
You are a quantitative prediction market analyst. Your job is to determine whether
a news headline is directionally informative for Polymarket binary questions.

Output ONLY valid JSON — no prose, no markdown, no code fences.
"""

# Single-market prompt (legacy path, kept for backwards-compat)
_PROMPT_TEMPLATE = """\
Headline: {headline}

Question: {question}
Current market price (mid): {mid_price}
Headline-question similarity: {similarity:.2f}
{republish_block}
Output:
{{"direction": "YES"|"NO"|"NEUTRAL", "confidence": 0.0-1.0, "reason": "<one sentence>"}}

Rules:
- YES  = headline increases likelihood of YES outcome
- NO   = headline decreases likelihood of YES outcome
- NEUTRAL = not clearly related, ambiguous, or already priced in
- confidence = certainty (0.5 = barely, 1.0 = certain); use full range
- If mid_price already reflects the headline (≥0.85 or ≤0.15), lean NEUTRAL
"""

# Batched prompt — evaluates N markets in one call (main path)
#
# A4E.4 (2026-05-23): the system message now permits a `<scratch>...</scratch>`
# chain-of-thought block before the JSON. The parser strips everything before
# the first `{` so the scratch content is discarded — token spend goes to
# reasoning, the trader only sees the final structured output. Per memory
# `feedback_reasoning_budget`: spend on thinking, constrain output.
_BATCH_SYSTEM_MESSAGE = """\
You are a quantitative prediction market analyst. Evaluate a news headline against
multiple Polymarket questions.

FIRST, in a <scratch>...</scratch> block, think step-by-step through the
headline → market relationship for each question. Consider: does the headline
introduce new information, is the market already priced for it, what is the
source's trustworthiness. The scratch block will be DISCARDED after parsing —
only the JSON is consumed downstream.

THEN, output the final JSON object. No prose between the </scratch> tag and the
opening `{`. No markdown fences.
"""

_BATCH_PROMPT_TEMPLATE = """\
Headline: {headline}
{source_block}{lessons_block}{republish_block}{mkt_ctx_block}
Evaluate the headline against each of the following Polymarket questions:

{questions_block}

After your <scratch> reasoning, emit a JSON object with one entry per question:
{{"decisions": [{{"idx": <0-based index>, "p_yes": <0.0-1.0>, "p_no": <0.0-1.0>, "p_neutral": <0.0-1.0>, "p_alpha": <0.0-1.0>, "reason": "<one sentence>"}}, ...]}}

Rules:
- p_yes  = probability the headline increases YES outcome likelihood
- p_no   = probability the headline decreases YES outcome likelihood
- p_neutral = probability the headline is not clearly related, ambiguous, or already priced in
- The three probabilities MUST sum to 1.0. Express genuine uncertainty here, do not collapse to one bucket.
- If current mid price already reflects the news (≥0.85 or ≤0.15), shift mass to p_neutral.
- Trust: state-media (Xinhua, TASS, RT, People's Daily) and partisan/tabloid sources move markets less — shift mass to p_neutral on contested claims.

- p_alpha = probability that a trade in the argmax direction (the highest of p_yes/p_no/p_neutral, ignoring NEUTRAL) makes money inside 5–30 minutes after this headline.
    Lower it when mid is already pinned near 0/1, the headline is a restatement, or the source is low-trust.
    Raise it when the headline introduces a hard fact (numbers, names, decisions) and the market is mid-range and reactive.
    p_alpha ≤ 0.5 means "do not bet, even though I have a directional guess".

- The "decisions" array must have EXACTLY {n} entries, ordered by idx.
"""

_REPUBLISH_BLOCK = """\
IMPORTANT — possible republish: a semantically similar headline was seen before:
  "{similar_to}"
If no new information is added vs. the original, return NEUTRAL for all markets.
"""

# Cycle-9 freshness block — replaces the binary republish hint with a labelled
# signal plus top-N prior coverage so the model can reason "is this fact new?"
_FRESHNESS_BLOCK = """\
FRESHNESS: {label} (nearest prior: {age_str}, cosine_dist={dist_str})
{context_block}\
"""


def _mkt_ctx_to_dict(ctx) -> Optional[dict]:
    """Serialize MarketContext for prompt_meta. None passes through. Inlined
    to avoid a circular import with market_context.py."""
    if ctx is None:
        return None
    return {
        "vol": ctx.vol, "mean": ctx.mean, "lo": ctx.lo, "hi": ctx.hi,
        "current": ctx.current, "drift": ctx.drift, "n_samples": ctx.n_samples,
    }


def _build_prompt_meta(
    *, model, n_samples, samples_ok, temperature, is_republish, similar_to,
    source, freshness, relevance_verdicts, matches,
) -> dict:
    """A4E.4: shared prompt_meta builder for bypass + real-LLM paths."""
    return {
        "model": model, "n_samples": n_samples, "samples_ok": samples_ok,
        "temperature": temperature, "is_republish": bool(is_republish),
        "similar_to": (similar_to or "")[:200] if similar_to else None,
        "source": source,
        "freshness_label": getattr(freshness, "label", None) if freshness else None,
        "nearest_age_secs": (
            getattr(freshness, "nearest_age_secs", None) if freshness else None
        ),
        "nearest_dist": getattr(freshness, "nearest_dist", None) if freshness else None,
        "relevance_scores": (
            [getattr(v, "score", None) for v in relevance_verdicts]
            if relevance_verdicts else None
        ),
        "market_context": [
            _mkt_ctx_to_dict(getattr(m, "_mkt_ctx", None)) for m in matches
        ],
    }


def _build_bypass_results(matches: list) -> list["ClassifyResult"]:
    """SEULGI_BYPASS_LLM=1 synthetic results — favorite-side direction,
    confidence/p_alpha pinned high enough to clear trader gates. Pure,
    deterministic, no HTTP. Direction = favorite of mid_yes (None→NO)."""
    out: list[ClassifyResult] = []
    for m in matches:
        mid = getattr(m, "mid_yes", None)
        is_yes = mid is not None and mid >= 0.5
        direction = Direction.YES if is_yes else Direction.NO
        p_yes = 0.9 if is_yes else 0.1
        p_no = 0.9 if not is_yes else 0.1
        out.append(ClassifyResult(
            direction=direction, confidence=0.9,
            reason="LLM bypass: favorite-side synthetic", tokens_used=0,
            p_yes=p_yes, p_no=p_no, p_neutral=0.0,
            agreement=1.0, p_alpha=1.0, risk_score=0.0,
            raw_response="", votes={direction.value: 1}, bypass=True,
        ))
    return out


def _format_freshness_block(freshness) -> str:
    """Render FreshnessResult as a single prompt block. Empty string if None."""
    if freshness is None:
        return ""
    age = getattr(freshness, "nearest_age_secs", None)
    if age is None:
        age_str = "no prior"
    elif age >= 3600:
        age_str = f"{int(age // 3600)}h ago"
    elif age >= 60:
        age_str = f"{int(age // 60)}m ago"
    else:
        age_str = f"{int(age)}s ago"
    dist = getattr(freshness, "nearest_dist", None)
    dist_str = f"{dist:.2f}" if dist is not None else "—"

    ctx_lines = []
    for c in (getattr(freshness, "recent_context", None) or [])[:3]:
        cage = c.get("age_secs") or 0
        cage_s = (
            f"{int(cage // 3600)}h" if cage >= 3600
            else f"{int(cage // 60)}m"
        )
        ctx_lines.append(f"  - {cage_s} ago: {(c.get('headline') or '')[:140]}")
    context_block = (
        "PRIOR COVERAGE (similar headlines in last 24h):\n" + "\n".join(ctx_lines) + "\n"
        if ctx_lines else ""
    )
    return _FRESHNESS_BLOCK.format(
        label=getattr(freshness, "label", "BREAKING"),
        age_str=age_str,
        dist_str=dist_str,
        context_block=context_block,
    )


class Direction(str, Enum):
    YES = "YES"
    NO = "NO"
    NEUTRAL = "NEUTRAL"


@dataclass
class ClassifyResult:
    # Production-facing fields the trader consumes.
    direction: Direction
    confidence: float      # cycle-7+: alias of p_alpha when emitted
    reason: str
    tokens_used: int = 0
    # Ensemble-aware fields — filled by classify_batch when ensemble_samples>1;
    # single-sample calls leave these defaulted. p_*: mean of per-sample
    # distributions. agreement: max_votes/N (1.0=unanimous, 1/N=no signal).
    # p_alpha: probability the argmax-direction trade is +EV in 5–30 min.
    # risk_score: expected |Δ5m| on [0,1].
    p_yes: float = 0.0
    p_no: float = 0.0
    p_neutral: float = 0.0
    agreement: float = 0.0
    p_alpha: float = 0.0
    risk_score: float = 0.0
    # Per-market raw JSON the model emitted (≤2KB). Persisted on the trade
    # row so post-hoc analysis can re-parse the distribution.
    raw_response: str = ""
    # Per-market vote counts {"YES":n,"NO":n,"NEUTRAL":n}.
    votes: dict = field(default_factory=dict)
    # A1.6 (2026-05-22): True when synthesised by SEULGI_BYPASS_LLM=1.
    bypass: bool = False


class DirectionClassifier:
    """
    Async OpenRouter wrapper for headline → direction classification.

    Main path: classify_batch() — one API call for ALL markets on a headline.
    Legacy path: classify() — one API call per market (kept for compatibility).

    Semaphore caps concurrent requests (default=5).
    Retries on 429 with exponential backoff (up to 3 attempts).

    Usage:
        clf = DirectionClassifier(api_key="sk-or-...", model="google/gemini-2.5-flash-lite")
        results = await clf.classify_batch("Fed cuts rates", matches)
    """

    def __init__(
        self,
        api_key: str,
        model: str = "google/gemini-2.5-flash-lite",
        api_base: str = _DEFAULT_API_BASE,
        max_concurrent: int = 5,
        premium_model: Optional[str] = None,
        premium_sim_threshold: float = 0.70,
        ensemble_samples: int = 3,
        ensemble_temperature: float = 0.5,
        batch_size: int = 10,
        db=None,
    ) -> None:
        self._api_key = api_key
        self._model = model
        self._premium_model = premium_model
        self._premium_sim_threshold = premium_sim_threshold
        self._url = api_base.rstrip("/") + _CHAT_PATH
        # Semaphore — caps API concurrency; max() with ensemble_samples*2 so
        # a single ensembled call doesn't starve its peers.
        self._semaphore = asyncio.Semaphore(max(max_concurrent, ensemble_samples * 2))
        self._session: Optional[aiohttp.ClientSession] = None
        # Ensemble: N parallel calls at ensemble_temperature; aggregated by
        # majority-vote + mean distribution. N=1 reproduces single-call path.
        self._ensemble_samples = max(1, ensemble_samples)
        self._ensemble_temperature = ensemble_temperature
        # A4E.4: batch_size chunks matches into sub-batches per LLM call.
        self._batch_size = max(1, batch_size)
        # A4E.4: prefer json_schema; first schema-unsupported 400 flips False.
        self._schema_supported = True
        # A2.1: optional db for logging every round-trip to llm_calls.
        self._db = db

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    # ------------------------------------------------------------------
    # Batched path (preferred) — one API call for all markets
    # ------------------------------------------------------------------

    @prof.traced("ai/classify_batch")
    async def classify_batch(
        self,
        headline: str,
        matches: list,           # list[MarketMatch] — avoids circular import
        is_republish: bool = False,
        similar_to: Optional[str] = None,
        source: Optional[str] = None,
        freshness=None,                          # FreshnessResult | None
        relevance_verdicts: Optional[list] = None,  # list[_SimVerdict] aligned to matches
        headline_id: Optional[str] = None,       # A2.1 — for llm_calls correlation
    ) -> tuple[list[ClassifyResult], dict]:
        """Classify headline against `matches`. Returns (results, prompt_meta).
        Results are in input order, one per match. A4E.4: chunks into
        sub-batches of `batch_size`. Falls back to per-market classify()
        when both the JSON parse and the regex-extract recovery fail."""
        if not matches:
            return [], {}

        # A1.6: SEULGI_BYPASS_LLM=1 short-circuits BEFORE any HTTP — zero
        # token spend, deterministic favorite-side results.
        if _bypass_enabled():
            _log_bypass_startup_once()
            results = _build_bypass_results(matches)
            logger.info("[ai] LLM_BYPASS active — synthesizing %d results", len(results))
            meta = _build_prompt_meta(
                model=_BYPASS_MODEL_TAG, n_samples=0, samples_ok=0, temperature=0.0,
                is_republish=is_republish, similar_to=similar_to, source=source,
                freshness=freshness, relevance_verdicts=relevance_verdicts,
                matches=matches,
            )
            meta["bypass"] = True
            return results, meta

        # Premium model gate
        max_sim = max(getattr(m, "similarity", 0.0) for m in matches)
        model = (
            self._premium_model
            if self._premium_model and max_sim >= self._premium_sim_threshold
            else self._model
        )
        if model != self._model:
            logger.info("[ai] routing batch to premium model=%s (max_sim=%.2f)", model, max_sim)

        # A4E.4: chunk into batch_size sub-batches and run them concurrently.
        n = len(matches)
        bs = self._batch_size
        chunk_indices = [list(range(i, min(i + bs, n))) for i in range(0, n, bs)]
        chunk_results: list[list[ClassifyResult]] = [[] for _ in chunk_indices]
        chunk_tokens = [0] * len(chunk_indices)
        chunk_samples_ok = [0] * len(chunk_indices)

        async def _run_chunk(ci: int) -> None:
            indices = chunk_indices[ci]
            sub_matches = [matches[j] for j in indices]
            sub_rv = (
                [relevance_verdicts[j] for j in indices if j < len(relevance_verdicts)]
                if relevance_verdicts else None
            )
            res, tokens, samples_ok = await self._classify_chunk(
                headline=headline, matches=sub_matches, model=model,
                is_republish=is_republish, similar_to=similar_to, source=source,
                freshness=freshness, relevance_verdicts=sub_rv,
                headline_id=headline_id,
            )
            chunk_results[ci] = res
            chunk_tokens[ci] = tokens
            chunk_samples_ok[ci] = samples_ok

        await asyncio.gather(*[_run_chunk(ci) for ci in range(len(chunk_indices))])

        aggregated: list[ClassifyResult] = []
        for chunk in chunk_results:
            aggregated.extend(chunk)

        total_tokens = sum(chunk_tokens)
        meta = _build_prompt_meta(
            model=model, n_samples=self._ensemble_samples,
            samples_ok=sum(chunk_samples_ok),
            temperature=(self._ensemble_temperature if self._ensemble_samples > 1 else 0.1),
            is_republish=is_republish, similar_to=similar_to, source=source,
            freshness=freshness, relevance_verdicts=relevance_verdicts,
            matches=matches,
        )
        meta["n_chunks"] = len(chunk_indices)
        meta["batch_size"] = self._batch_size

        logger.info(
            "[ai] ensemble classify: %d markets in %d chunk(s) of %d, "
            "samples_ok=%d, model=%s, tokens=%d",
            n, len(chunk_indices), bs, sum(chunk_samples_ok), model, total_tokens,
        )
        return aggregated, meta

    async def _classify_chunk(
        self,
        *,
        headline: str,
        matches: list,
        model: str,
        is_republish: bool,
        similar_to: Optional[str],
        source: Optional[str],
        freshness,
        relevance_verdicts: Optional[list],
        headline_id: Optional[str],
    ) -> tuple[list[ClassifyResult], int, int]:
        """A4E.4: classify one chunk (≤ batch_size markets). Returns
        (per_market_results, total_tokens, samples_ok)."""
        # Freshness replaces legacy binary republish hint when available.
        freshness_block = _format_freshness_block(freshness)
        if freshness_block:
            republish_block = ""
        else:
            republish_block = (
                _REPUBLISH_BLOCK.format(similar_to=similar_to[:200])
                if is_republish and similar_to else ""
            )
        source_block = f"Source: {source}\n" if source else ""
        # Lessons retired 2026-05-22; template still references {lessons_block}.
        lessons_block = ""

        # TODO(A4E.4(e)): few-shot exemplars from recent EXECUTED+profitable
        # trades — DEFERRED. Needs a nightly mining job + sample cache so the
        # ~1500-token exemplar block doesn't tax every prediction call.

        rv_by_idx: dict[int, object] = {}
        if relevance_verdicts:
            for i, v in enumerate(relevance_verdicts[:len(matches)]):
                rv_by_idx[i] = v
        lines = []
        for i, m in enumerate(matches):
            mid = getattr(m, "mid_yes", None)
            mid_str = f"{mid:.3f}" if mid is not None else "unknown"
            rv = rv_by_idx.get(i)
            rv_str = f" | relevance={rv.score}:{rv.reason[:70]}" if rv is not None else ""
            lines.append(
                f"{i}. [YES mid={mid_str} | sim={m.similarity:.2f}{rv_str}] {m.question}"
            )
        questions_block = "\n".join(lines)

        try:
            from market_context import format_market_context_block
            mkt_ctx_block = format_market_context_block(matches)
        except Exception:  # noqa: BLE001
            mkt_ctx_block = ""

        freshness_block_norm = (
            freshness_block + "\n"
            if freshness_block and not freshness_block.endswith("\n")
            else freshness_block
        )
        prompt = _BATCH_PROMPT_TEMPLATE.format(
            headline=headline,
            source_block=source_block,
            lessons_block=lessons_block,
            republish_block=republish_block or freshness_block_norm,
            mkt_ctx_block=mkt_ctx_block,
            questions_block=questions_block,
            n=len(matches),
        )

        N = self._ensemble_samples
        temp = self._ensemble_temperature if N > 1 else 0.1

        def _build_body() -> dict:
            body = {
                "model": model,
                "messages": [
                    {"role": "system", "content": _BATCH_SYSTEM_MESSAGE},
                    {"role": "user",   "content": prompt},
                ],
                # Headroom for the <scratch> block + the JSON body.
                "max_tokens": 200 * max(1, len(matches)) + 200,
                "temperature": temp,
            }
            body["response_format"] = (
                _RESPONSE_SCHEMA if self._schema_supported
                else {"type": "json_object"}
            )
            return body

        prompt_sha = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
        request_ts = _dt.datetime.now(_dt.timezone.utc).isoformat()

        sample_responses = await asyncio.gather(
            *[self._call_api(_build_body()) for _ in range(N)],
            return_exceptions=False,
        )

        per_sample_results: list[list[ClassifyResult]] = []
        per_sample_raws: list[list[str]] = []
        total_tokens = 0
        for data in sample_responses:
            if data is None:
                continue
            parsed = _parse_batch_response(data, len(matches))
            if parsed is None:
                # A4E.4(b): regex-extract fallback. Tries to recover
                # per-market tuples from malformed JSON. Accepts the
                # partial if ≥ 50% of expected markets are covered.
                parsed = _regex_extract_partial(_extract_text(data), len(matches))
            parse_ok = parsed is not None
            response_text_raw = _extract_text(data)
            usage = data.get("usage", {}) or {}
            await self._log_llm_call_safe(
                ts=request_ts,
                model=model,
                prompt_sha=prompt_sha,
                prompt_tokens=usage.get("prompt_tokens"),
                completion_tokens=usage.get("completion_tokens"),
                response_text=response_text_raw,
                parse_ok=parse_ok,
                headline_id=headline_id,
                batch_size=len(matches),
            )
            if not parse_ok:
                await self._log_llm_parse_failure_safe(
                    ts=request_ts,
                    model=model,
                    error_msg="batch_parse_failed",
                    response_text=response_text_raw,
                    headline_id=headline_id,
                )
                continue
            per_sample_results.append(parsed[0])
            per_sample_raws.append(parsed[1])
            total_tokens += int(usage.get("total_tokens", 0))

        if not per_sample_results:
            logger.warning("[ai] all %d ensemble samples failed in chunk — "
                           "per-market fallback", N)
            fallback = await asyncio.gather(*[
                self.classify(headline, m.question,
                              mid_price=getattr(m, "mid_price", None),
                              similarity=m.similarity,
                              is_republish=is_republish, similar_to=similar_to)
                for m in matches
            ])
            return list(fallback), total_tokens, 0

        aggregated = _aggregate_ensemble(
            per_sample_results, n_markets=len(matches),
            raw_samples=per_sample_raws,
        )
        per_market_tokens = max(1, total_tokens // max(1, len(aggregated)))
        for r in aggregated:
            r.tokens_used = per_market_tokens
        return aggregated, total_tokens, len(per_sample_results)

    # ------------------------------------------------------------------
    # Single-market path (legacy / fallback)
    # ------------------------------------------------------------------

    @prof.traced("ai/classify")
    async def classify(
        self,
        headline: str,
        question: str,
        mid_price: Optional[float] = None,
        similarity: float = 0.0,
        is_republish: bool = False,
        similar_to: Optional[str] = None,
    ) -> ClassifyResult:
        """
        Classify a headline against a single Polymarket question.

        Prefer classify_batch() for multiple markets — it's ~10-15× cheaper.
        Returns NEUTRAL with confidence=0.0 on any unrecoverable error.
        """
        if is_republish and similar_to:
            republish_block = _REPUBLISH_BLOCK.format(similar_to=similar_to[:200])
        else:
            republish_block = ""

        mid_str = f"{mid_price:.3f}" if mid_price is not None else "unknown"
        prompt = _PROMPT_TEMPLATE.format(
            headline=headline,
            question=question,
            mid_price=mid_str,
            similarity=similarity,
            republish_block=republish_block,
        )
        body = {
            "model": self._model,
            "messages": [
                {"role": "system", "content": _SYSTEM_MESSAGE},
                {"role": "user",   "content": prompt},
            ],
            "max_tokens": 150,
            "temperature": 0.1,
            "response_format": {"type": "json_object"},
        }

        data = await self._call_api(body)
        if data is None:
            return ClassifyResult(Direction.NEUTRAL, 0.0, "api_error")
        return _parse_response(data)

    # ------------------------------------------------------------------
    # Shared HTTP helper
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # A2.1: capture helpers — fire-and-forget by spec, but awaited
    # synchronously inside classify_batch so row ordering is deterministic
    # for replay tooling. The try/except keeps a logging failure from ever
    # blocking the trade path.
    # ------------------------------------------------------------------

    async def _log_llm_call_safe(
        self, *, ts: str, model: str, prompt_sha: str,
        prompt_tokens, completion_tokens, response_text: str,
        parse_ok: bool, headline_id: Optional[str], batch_size: int,
    ) -> None:
        """Best-effort db.log_llm_call wrapper. No-op when db is None."""
        if self._db is None:
            return
        try:
            await self._db.log_llm_call(
                ts=ts, model=model, prompt_sha=prompt_sha,
                prompt_tokens=(int(prompt_tokens) if prompt_tokens is not None else None),
                completion_tokens=(int(completion_tokens) if completion_tokens is not None else None),
                response_text=_truncate_response(response_text or "", _LLM_CALL_RESPONSE_CAP),
                parse_ok=parse_ok, headline_id=headline_id, batch_size=batch_size,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("[ai] log_llm_call failed (non-fatal): %r", exc)

    async def _log_llm_parse_failure_safe(
        self, *, ts: str, model: str, error_msg: str,
        response_text: str, headline_id: Optional[str],
    ) -> None:
        """Best-effort db.log_llm_parse_failure wrapper. No-op when db is None."""
        if self._db is None:
            return
        try:
            await self._db.log_llm_parse_failure(
                ts=ts, model=model, error_msg=error_msg,
                response_text=_truncate_response(
                    response_text or "", _LLM_PARSE_FAILURE_RESPONSE_CAP),
                headline_id=headline_id,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("[ai] log_llm_parse_failure failed (non-fatal): %r", exc)

    @staticmethod
    def _is_schema_unsupported_error(text: str) -> bool:
        """A4E.4: True when a 400 body indicates the provider rejected
        json_schema. Permissive — a false-positive only costs strict-schema
        reasoning, never correctness."""
        if not text:
            return False
        lowered = text.lower()
        return any(marker in lowered for marker in _SCHEMA_UNSUPPORTED_MARKERS)

    async def _call_api(self, body: dict) -> Optional[dict]:
        """POST to OpenRouter, retry up to 3× on 429. Returns None on failure."""
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/seulgi",
            "X-Title": "Seulgi",
        }
        for attempt in range(3):
            try:
                async with self._semaphore:
                    session = self._get_session()
                    async with session.post(
                        self._url,
                        json=body,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=30),
                    ) as resp:
                        if resp.status == 429:
                            wait = 2 ** attempt
                            logger.debug("[ai] rate limited, sleeping %ds", wait)
                            await asyncio.sleep(wait)
                            continue
                        if resp.status != 200:
                            text = await resp.text()
                            # A4E.4: detect "json_schema unsupported" 400s
                            # and auto-flip the instance to json_object for
                            # the remainder of its lifetime. Retry the same
                            # request once with the fallback format so this
                            # call still succeeds (not just the next one).
                            if (
                                resp.status == 400
                                and self._schema_supported
                                and self._is_schema_unsupported_error(text)
                                and isinstance(body.get("response_format"), dict)
                                and body["response_format"].get("type") == "json_schema"
                            ):
                                logger.warning(
                                    "[ai] provider rejected json_schema; "
                                    "falling back to json_object for this "
                                    "classifier (model=%s)", body.get("model"),
                                )
                                self._schema_supported = False
                                body["response_format"] = {"type": "json_object"}
                                continue
                            logger.warning("[ai] OpenRouter %d: %s", resp.status, text[:200])
                            return None
                        return await resp.json()
            except Exception as exc:
                logger.warning("[ai] API error: %s", exc)
                return None
        logger.warning("[ai] giving up after 3 rate-limit retries")
        return None


# ---------------------------------------------------------------------------
# Response parsers
# ---------------------------------------------------------------------------


def _extract_text(data: dict) -> str:
    """Pull the content string from a chat completion response. A4E.4: drops
    `<scratch>` blocks + any prose preamble by trimming everything before
    the first `{` or `[`."""
    choices = data.get("choices", [])
    if not choices:
        return ""
    text = choices[0].get("message", {}).get("content", "").strip()
    text = re.sub(r"^```[a-z]*\n?", "", text).rstrip("`").strip()
    first_brace = text.find("{")
    first_bracket = text.find("[")
    candidates = [p for p in (first_brace, first_bracket) if p != -1]
    if candidates and min(candidates) > 0:
        text = text[min(candidates):]
    return text


def _parse_one(obj: dict) -> ClassifyResult:
    """Parse one classification object. Accepts the new distribution form
    ({p_yes,p_no,p_neutral}, normalised to sum=1, argmax→direction) or
    the legacy {direction,confidence} form (models occasionally regress).
    """
    reason = str(obj.get("reason", ""))[:200]
    # Cycle-7 fields optional — default 0 so a regression doesn't poison
    # the trader edge model with a bogus high p_alpha.
    p_alpha = max(0.0, min(1.0, float(obj.get("p_alpha", 0.0) or 0.0)))
    risk_score = max(0.0, min(1.0, float(obj.get("risk", 0.0) or 0.0)))

    # New distribution form (preferred)
    p_yes_raw = obj.get("p_yes")
    p_no_raw = obj.get("p_no")
    p_neu_raw = obj.get("p_neutral")
    if p_yes_raw is not None or p_no_raw is not None or p_neu_raw is not None:
        p_yes = max(0.0, float(p_yes_raw or 0.0))
        p_no = max(0.0, float(p_no_raw or 0.0))
        p_neu = max(0.0, float(p_neu_raw or 0.0))
        total = p_yes + p_no + p_neu
        if total <= 0:
            return ClassifyResult(Direction.NEUTRAL, 0.0, reason,
                                  p_yes=0.0, p_no=0.0, p_neutral=1.0,
                                  p_alpha=p_alpha, risk_score=risk_score)
        p_yes /= total; p_no /= total; p_neu /= total
        # Pick argmax. Ties between a directional and p_neutral resolve to
        # the directional — the LLM clearly leaned (one side has more mass
        # than the other) and hedging with p_neutral shouldn't blank that
        # signal. Pure ties between p_yes and p_no still resolve to NEUTRAL.
        if p_yes > p_no and p_yes >= p_neu:
            direction, conf = Direction.YES, p_yes
        elif p_no > p_yes and p_no >= p_neu:
            direction, conf = Direction.NO, p_no
        else:
            direction, conf = Direction.NEUTRAL, p_neu
        # Cycle-7 contract: `confidence` becomes the model's stated p_alpha
        # (probability the chosen-direction trade is +EV). If the model
        # forgot to emit p_alpha, fall back to argmax-mass so we don't fire
        # on an implicit zero. The legacy edge formula `2·conf − 1` is
        # exactly right under this interpretation (≤0.5 → zero edge → skip).
        prod_conf = p_alpha if p_alpha > 0.0 else conf
        return ClassifyResult(direction, prod_conf, reason,
                              p_yes=p_yes, p_no=p_no, p_neutral=p_neu,
                              p_alpha=p_alpha, risk_score=risk_score)

    # Legacy form
    direction_str = str(obj.get("direction", "NEUTRAL")).upper()
    try:
        direction = Direction(direction_str)
    except ValueError:
        direction = Direction.NEUTRAL
    confidence = float(obj.get("confidence", 0.0))
    confidence = max(0.0, min(1.0, confidence))
    # Synthesise a plausible distribution from (direction, confidence) so the
    # ensemble aggregator can still work with legacy responses.
    rest = (1.0 - confidence) / 2.0
    if direction == Direction.YES:
        p_yes, p_no, p_neu = confidence, rest, rest
    elif direction == Direction.NO:
        p_yes, p_no, p_neu = rest, confidence, rest
    else:
        p_yes, p_no, p_neu = rest, rest, confidence
    return ClassifyResult(direction, confidence, reason,
                          p_yes=p_yes, p_no=p_no, p_neutral=p_neu,
                          p_alpha=p_alpha, risk_score=risk_score)


def _parse_response(data: dict) -> ClassifyResult:
    """Parse a single-market OpenRouter response."""
    try:
        text = _extract_text(data)
        if not text:
            return ClassifyResult(Direction.NEUTRAL, 0.0, "no_candidates")
        obj = json.loads(text)
        result = _parse_one(obj)
        result.tokens_used = int(data.get("usage", {}).get("total_tokens", 0))
        return result
    except Exception as exc:
        logger.warning("[ai] parse error: %s", exc)
        return ClassifyResult(Direction.NEUTRAL, 0.0, "parse_error")


def _aggregate_ensemble(
    samples: list[list[ClassifyResult]],
    n_markets: int,
    raw_samples: Optional[list[list[str]]] = None,
) -> list[ClassifyResult]:
    """Collapse N ensemble samples into one per-market ClassifyResult.

    Per-market: distribution = mean(p_*); direction = majority-vote
    argmax (ties → NEUTRAL); agreement = max_votes/N; confidence = p_alpha
    when emitted else mean(P[winner]). A4E.4 hard gate: agreement < 2/3 →
    NEUTRAL with confidence=0.0, regardless of stated p_alpha.
    """
    if not samples:
        return [ClassifyResult(Direction.NEUTRAL, 0.0, "empty_ensemble") for _ in range(n_markets)]

    N = len(samples)
    out: list[ClassifyResult] = []
    for i in range(n_markets):
        votes = {Direction.YES: 0, Direction.NO: 0, Direction.NEUTRAL: 0}
        sum_yes = sum_no = sum_neu = 0.0
        sum_alpha = sum_risk = 0.0
        reasons: list[str] = []
        for s in samples:
            if i >= len(s):
                votes[Direction.NEUTRAL] += 1
                sum_neu += 1.0
                continue
            r = s[i]
            votes[r.direction] = votes.get(r.direction, 0) + 1
            sum_yes += r.p_yes
            sum_no += r.p_no
            sum_neu += r.p_neutral
            sum_alpha += r.p_alpha
            sum_risk += r.risk_score
            if r.reason:
                reasons.append(r.reason)
        p_yes = sum_yes / N
        p_no = sum_no / N
        p_neu = sum_neu / N
        p_alpha = sum_alpha / N
        risk_score = sum_risk / N

        # Majority vote; ties resolve to NEUTRAL (most conservative).
        top_count = max(votes.values())
        winners = [d for d, c in votes.items() if c == top_count]
        if len(winners) != 1:
            direction = Direction.NEUTRAL
        else:
            direction = winners[0]
        agreement = top_count / N

        # Cycle-7: production confidence = mean p_alpha across samples.
        # Falls back to argmax-mass when the model didn't emit p_alpha (it
        # can sometimes regress to the old schema mid-run). The legacy edge
        # formula `2·conf − 1` is exactly the right interpretation under
        # p_alpha — break-even at 0.5, zero edge below.
        if direction == Direction.YES:
            mean_p = p_yes
        elif direction == Direction.NO:
            mean_p = p_no
        else:
            mean_p = p_neu
        confidence = p_alpha if p_alpha > 0.0 else mean_p

        # A4E.4: hard 2/3 agreement gate. Below threshold → NEUTRAL,
        # confidence=0.0. Distribution + votes still surfaced for dashboards.
        demoted_reason = ""
        if agreement + 1e-9 < _AGREEMENT_DEMOTE_THRESHOLD:
            demoted_reason = f"agreement_below_threshold({agreement:.2f}<2/3)"
            direction = Direction.NEUTRAL
            confidence = 0.0

        # Pick one representative raw response (the first sample's text for
        # this market). Keeping all N would balloon DB rows; one is enough to
        # recover the model's stated distribution + reason verbatim.
        raw_for_market = ""
        if raw_samples:
            for rs in raw_samples:
                if i < len(rs) and rs[i]:
                    raw_for_market = rs[i]
                    break

        out.append(ClassifyResult(
            direction=direction,
            confidence=confidence,
            reason=demoted_reason or (reasons[0] if reasons else ""),
            p_yes=p_yes, p_no=p_no, p_neutral=p_neu,
            agreement=agreement,
            p_alpha=p_alpha,
            risk_score=risk_score,
            raw_response=raw_for_market,
            votes={d.value: c for d, c in votes.items()},
        ))
    return out


def _parse_batch_response(
    data: dict, n: int
) -> Optional[tuple[list[ClassifyResult], list[str]]]:
    """Parse a batched response: expects `{"decisions":[...]}` (A4E.4
    schema) but also accepts legacy `{"results":[...]}` /
    `{"classifications":[...]}` / `{"answers":[...]}` / a top-level
    array. Returns None on failure so the caller can fall back."""
    try:
        text = _extract_text(data)
        if not text:
            return None
        parsed = json.loads(text)

        # Expected shape: {"decisions": [...]} (A4E.4 strict schema) or the
        # legacy {"results": [...]}. Some models still wrap differently;
        # accept a small set of fallback keys before giving up.
        if isinstance(parsed, dict):
            inner = (
                parsed.get("decisions")
                or parsed.get("results")
                or parsed.get("classifications")
                or parsed.get("answers")
            )
            if isinstance(inner, list):
                parsed = inner
            elif "direction" in parsed:
                # Single object pretending to be a one-element array
                parsed = [parsed]
            else:
                return None

        if not isinstance(parsed, list):
            return None

        # Sort by idx if present (model may reorder)
        try:
            parsed.sort(key=lambda x: int(x.get("idx", 0)))
        except Exception:
            pass

        results = [_parse_one(obj) for obj in parsed]
        # Preserve the per-object JSON text (truncated) so downstream code can
        # persist what the model actually said per market, not just the parsed
        # ClassifyResult. Keys outside the schema (the model's own scratch)
        # round-trip naturally because we dump the dict back to text.
        raws: list[str] = []
        for obj in parsed:
            try:
                raws.append(json.dumps(obj, separators=(",", ":"))[:2000])
            except Exception:  # noqa: BLE001
                raws.append("")

        # Pad with NEUTRAL if model returned fewer items than expected
        while len(results) < n:
            results.append(ClassifyResult(Direction.NEUTRAL, 0.0, "missing"))
            raws.append("")

        return results[:n], raws[:n]

    except Exception as exc:
        logger.warning("[ai] batch parse error: %s | text=%s", exc,
                       _extract_text(data)[:200] if data else "")
        return None


def _regex_extract_partial(
    text: str, n: int,
) -> Optional[tuple[list[ClassifyResult], list[str]]]:
    """A4E.4: regex-extract fallback for malformed JSON. Recovers per-market
    tuples via a permissive regex; accepts only when ≥ 50% of expected
    markets are recovered. Missing indices in [0,n) are padded NEUTRAL."""
    if not text:
        return None
    recovered: dict[int, dict] = {}
    for match in _PARTIAL_OBJECT_RE.finditer(text):
        try:
            idx = int(match.group("idx"))
        except (TypeError, ValueError):
            continue
        if idx < 0 or idx >= n or idx in recovered:
            continue
        try:
            recovered[idx] = {
                "idx": idx,
                "p_yes": float(match.group("p_yes")),
                "p_no": float(match.group("p_no")),
                "p_neutral": float(match.group("p_neutral")),
                "p_alpha": float(match.group("p_alpha")) if match.group("p_alpha") else 0.0,
                "reason": "regex_extract",
            }
        except (TypeError, ValueError):
            continue

    if len(recovered) < (n + 1) // 2:
        return None

    results: list[ClassifyResult] = []
    raws: list[str] = []
    for i in range(n):
        obj = recovered.get(i)
        if obj is None:
            results.append(ClassifyResult(Direction.NEUTRAL, 0.0, "missing_regex"))
            raws.append("")
            continue
        results.append(_parse_one(obj))
        try:
            raws.append(json.dumps(obj, separators=(",", ":"))[:2000])
        except Exception:  # noqa: BLE001
            raws.append("")
    return results, raws
