"""
Seulgi AI direction classifier — OpenRouter backend.

Uses OpenRouter's OpenAI-compatible chat completions API so any model
available on OpenRouter can be used without changing this file.

Default model: google/gemini-2.5-flash-lite  (fast, cheap, great at structured JSON)

Performance design
──────────────────
• Batched classification: a single API call evaluates ALL market questions for
  a headline at once, reducing cost by up to 15× vs one call per market.
• System message primes the LLM to output valid JSON without preamble.
• response_format=json_object enforces JSON output (no code-fence stripping needed).
• Price + similarity context is included so the model can see current market
  pricing and how relevant the headline is, improving accuracy.
• Tiered model: classify_batch() uses the configured cheap model by default.
  Set ai.premium_model in config to route high-similarity (≥0.7) batches through
  a better model (e.g. google/gemini-2.5-flash) without changing the default.

Config (config.yaml → ai section):
    model          : OpenRouter model ID, e.g. "google/gemini-2.5-flash-lite"
    premium_model  : optional higher-quality model for high-sim batches
    premium_sim_threshold : similarity threshold to trigger premium model (default 0.7)
    api_key        : OpenRouter API key (or set OPENROUTER_API_KEY env var)
    api_base       : override base URL (default: https://openrouter.ai/api/v1)
    min_confidence : below this → treat result as NEUTRAL

OpenRouter API reference: https://openrouter.ai/docs
"""

# ## Specification ──────────────────────────────────────────────────────────────
# Purpose:
#     OpenRouter chat-completion client + Stage-3 ensemble direction
#     classifier. Given a headline + a list of candidate Polymarket
#     questions (already culled by similarity/relevance), emits a
#     per-market probability tuple (p_yes, p_no, p_neutral, p_alpha,
#     risk) plus an aggregated direction + confidence. Stage 2
#     (relevance LLM) was retired 2026-05-22; this is now the only
#     remote LLM call in the hot path.
#
# Public interface:
#     DirectionClassifier(api_key, model, premium_model=None,
#                         premium_sim_threshold=0.70,
#                         ensemble_samples=3, ensemble_temperature=0.5)
#       .classify_batch(headline, matches, *, is_republish, similar_to,
#                       source, freshness, relevance_verdicts)
#           -> (list[ClassifyResult], prompt_meta: dict)
#       .classify(headline, question, …) -> ClassifyResult  # legacy fallback
#       .close()  # closes the aiohttp session
#     Module helpers: `_BATCH_PROMPT_TEMPLATE`, `_parse_one`,
#     `_parse_batch_response`, `_aggregate_ensemble`,
#     `_format_freshness_block`, `_mkt_ctx_to_dict`.
#
# Invariants:
#   - Ensemble = N=ensemble_samples parallel calls at
#     ensemble_temperature; aggregated per-market by mean(p_yes/p_no/
#     p_neutral/p_alpha/risk) and majority-vote on the per-sample
#     argmax direction. Ties resolve to NEUTRAL.
#   - `agreement` = max_votes / N (1/N → no signal; 1.0 → unanimous).
#   - `confidence` = `p_alpha` when the model emitted it (cycle-7
#     contract); else falls back to mean(P[winning_direction]).
#     KNOWN AMBIGUITY (A0.3): the trader's `2·conf − 1` edge rescale
#     reads `confidence`, so the production edge silently switches
#     semantics between samples that emit p_alpha and ones that don't.
#     `agreement` is NOT folded into `confidence` despite the docstring
#     at `_aggregate_ensemble` claiming it is. Phase 1 must pick one.
#   - If ALL N samples fail to parse, the classifier falls back to
#     per-market single-call classify(); if those also fail, every
#     market gets NEUTRAL with confidence=0.0 ("api_error"/"parse_error").
#     classify_batch never raises to the caller.
#   - Distributions are L1-normalised to 1.0 in `_parse_one` before
#     argmax; legacy `{direction, confidence}` responses synthesise a
#     plausible distribution so downstream aggregation still works.
#
# Side effects:
#   - HTTPS POSTs to OpenRouter `/chat/completions` (one per ensemble
#     sample; semaphore = max(max_concurrent, 2·ensemble_samples)).
#   - No DB writes from this module. The caller (engine.py) writes:
#       * `usage_events` (kind='ai_tokens', stage='prediction'),
#       * `llm_parse_failures` rows (fire-and-forget, raw response
#         capped to ~4KB) when `_parse_batch_response` returns None,
#       * `llm_calls` rows — NOT YET WIRED (master plan A2.1).
#
# Failure modes:
#   - HTTP 429 → exponential backoff up to 3 attempts, then None.
#   - HTTP 5xx / other → logged + None on first failure (no retry).
#   - HTTP 402 (insufficient credits) → currently SWALLOWED as a
#     generic non-200 (no special surface); the OpenRouter budget
#     poller is the only mechanism that notices and toggles
#     `data/budget_paused.flag` (engine.py reads, ai.py does not).
#   - aiohttp timeout (30s) / connection error → logged + None, no retry.
#   - JSON parse error (truncation at max_tokens, prose preamble,
#     trailing punctuation in `reason`, schema regression to legacy
#     {direction,confidence}) → batch returns None, falls back to
#     per-market path; all-failed batches log a single warning.
#
# Non-goals:
#   - Pre-AI rules (engine.py / rules.py).
#   - Trade sizing, gating, Kelly, fees (trader.py / simulate.py).
#   - OpenRouter credit polling / budget enforcement (budget.py).
#   - Persisting decision telemetry (engine.py owns decision_inputs).
# ───────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import asyncio
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

# A1.6 (2026-05-22): operator toggle that short-circuits the Stage-3
# prediction LLM with a deterministic favorite-side synthetic result.
# Set `SEULGI_BYPASS_LLM=1` to run the trade-filter pipeline at full
# volume with $0 OpenRouter spend; trader gates (mid_band, min_edge,
# max_positions, freshness, etc.) still apply. Default 0 = LLM enabled.
_BYPASS_ENV_VAR = "SEULGI_BYPASS_LLM"
_BYPASS_MODEL_TAG = "llm_bypass"


def _bypass_enabled() -> bool:
    """True when `SEULGI_BYPASS_LLM=1`. Read fresh each call so tests can
    toggle the env var via monkeypatch without recreating the classifier."""
    return os.environ.get(_BYPASS_ENV_VAR, "0") == "1"


# Emit the startup banner exactly once per process so log volume stays
# proportional to ensemble batches, not headlines. Reset to False in tests
# via `ai._BYPASS_STARTUP_LOGGED = False` if you want to re-observe it.
_BYPASS_STARTUP_LOGGED = False


def _log_bypass_startup_once() -> None:
    global _BYPASS_STARTUP_LOGGED
    if _BYPASS_STARTUP_LOGGED:
        return
    _BYPASS_STARTUP_LOGGED = True
    logger.info(
        "[ai] LLM_BYPASS active — env %s=1; Stage-3 prediction will return "
        "favorite-side synthetic results with 0 OpenRouter spend",
        _BYPASS_ENV_VAR,
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
_BATCH_SYSTEM_MESSAGE = """\
You are a quantitative prediction market analyst. Evaluate a news headline against
multiple Polymarket questions. Output ONLY a JSON array, one object per question,
in the same order as the input list. No prose, no markdown.
"""

_BATCH_PROMPT_TEMPLATE = """\
Headline: {headline}
{source_block}{lessons_block}{republish_block}{mkt_ctx_block}
Evaluate the headline against each of the following Polymarket questions:

{questions_block}

For each question output exactly:
{{"idx": <0-based index>, "p_yes": <0.0-1.0>, "p_no": <0.0-1.0>, "p_neutral": <0.0-1.0>, "p_alpha": <0.0-1.0>, "risk": <0.0-1.0>, "reason": "<one sentence>"}}

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
- risk = expected magnitude of the realized 5-minute |Δprice| on a 0–1 scale.
    0 ≈ ±0.5¢ noise; 0.5 ≈ ±2–3¢ typical; 1 ≈ bimodal ±10¢, hard to predict direction.

- Respond with EXACTLY this JSON shape: {{"results": [<array of {n} objects, ordered by idx>]}}
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
    """Serialize a MarketContext for prompt_meta. None passes through.

    Inlined here to avoid a circular import between ai.py and
    market_context.py — both ultimately depend on each other via the
    runner, so this tiny duplicate keeps the import graph clean.
    """
    if ctx is None:
        return None
    return {
        "vol": ctx.vol, "mean": ctx.mean, "lo": ctx.lo, "hi": ctx.hi,
        "current": ctx.current, "drift": ctx.drift, "n_samples": ctx.n_samples,
    }


def _build_bypass_results(matches: list) -> list["ClassifyResult"]:
    """
    Build deterministic favorite-side synthetic ClassifyResults for the
    `SEULGI_BYPASS_LLM=1` runtime toggle. Pure function — no I/O, no token
    spend. Direction = favorite of `mid_yes` (NO when mid is None so the
    direction is at least defined). Confidence/p_alpha = 0.9 / 1.0 clear
    any plausible min_confidence / EV floors; the trader's mid_band /
    min_edge / max_positions / freshness gates still apply.

    Determinism contract: same `matches` (same `mid_yes` values) → same
    results, byte-for-byte. The aggregator/parser is intentionally NOT
    used here — synthesised results bypass that whole code path.
    """
    out: list[ClassifyResult] = []
    for m in matches:
        mid = getattr(m, "mid_yes", None)
        # Favorite-side: YES when mid >= 0.5, NO otherwise. None → NO (the
        # "favorite" is undefined; pick a stable side so the result is
        # deterministic and the trader can still evaluate its own gates).
        is_yes = mid is not None and mid >= 0.5
        direction = Direction.YES if is_yes else Direction.NO
        p_yes = 0.9 if is_yes else 0.1
        p_no = 0.9 if not is_yes else 0.1
        out.append(ClassifyResult(
            direction=direction,
            confidence=0.9,
            reason="LLM bypass: favorite-side synthetic",
            tokens_used=0,
            p_yes=p_yes,
            p_no=p_no,
            p_neutral=0.0,
            agreement=1.0,
            p_alpha=1.0,
            risk_score=0.0,
            raw_response="",
            votes={direction.value: 1},
            bypass=True,
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
    direction: Direction
    confidence: float      # 0.0–1.0  — production-facing (alias of p_alpha in cycle 7+)
    reason: str
    tokens_used: int = 0
    # Ensemble-aware fields (filled by classify_batch when ensemble_samples > 1;
    # a single-sample call leaves these defaulted). The dataclass carries them
    # so callers can downsample what they log without parsing JSON twice.
    #
    # p_yes / p_no / p_neutral: average of per-sample model-emitted distributions
    # agreement: fraction of ensemble samples that voted for the winning direction
    #            (1.0 = unanimous, 1/N = no signal — model can't decide)
    # p_alpha: probability the argmax-direction trade is +EV in 5–30 min
    # risk_score: expected |Δ5m| magnitude on a [0,1] scale (0=tiny, 1=bimodal big)
    p_yes: float = 0.0
    p_no: float = 0.0
    p_neutral: float = 0.0
    agreement: float = 0.0
    p_alpha: float = 0.0
    risk_score: float = 0.0
    # Per-market raw JSON object the model emitted (one entry of the batch
    # "results" array, dumped back to text and truncated to ~2KB). Persisted
    # on the trade row so post-hoc analysis can re-parse the full distribution
    # without a separate log file. Populated for ensembled batch calls; left
    # empty on the legacy single-market path.
    raw_response: str = ""
    # Per-market ensemble vote counts {"YES":n, "NO":n, "NEUTRAL":n}. Carries
    # the same information as `agreement` but lets the dashboard show 2/3 vs
    # 3/3 separately, and `agreement` discards which directions lost.
    votes: dict = field(default_factory=dict)
    # A1.6 (2026-05-22): True when this result was synthesised by the
    # `SEULGI_BYPASS_LLM=1` operator toggle instead of an OpenRouter call.
    # The trader's pre-AI rules and post-AI gates still apply — this flag
    # only marks "no LLM was consulted" so post-mortem queries can filter
    # bypass rows out of any model-quality analysis.
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
    ) -> None:
        self._api_key = api_key
        self._model = model
        self._premium_model = premium_model
        self._premium_sim_threshold = premium_sim_threshold
        self._url = api_base.rstrip("/") + _CHAT_PATH
        # Semaphore caps API concurrency. Multiply by ensemble_samples so a
        # single ensembled call doesn't monopolise the budget on its peers.
        self._semaphore = asyncio.Semaphore(max(max_concurrent, ensemble_samples * 2))
        self._session: Optional[aiohttp.ClientSession] = None
        # Ensemble config: N parallel calls at ensemble_temperature, then
        # average the per-sample distributions and majority-vote the
        # direction. ensemble_samples=1 falls back to legacy behaviour.
        self._ensemble_samples = max(1, ensemble_samples)
        self._ensemble_temperature = ensemble_temperature

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
    ) -> tuple[list[ClassifyResult], dict]:
        """
        Classify a headline against ALL matching markets in a single API call.

        Up to 15× cheaper than calling classify() per market.
        Returns `(results, prompt_meta)` where results has one entry per
        match in input order, and `prompt_meta` captures the runtime context
        (model, samples, republish flag, source, lessons) so the trader can
        persist it on every trade row.
        Falls back to per-market classify() if the batch parse fails.
        """
        if not matches:
            return [], {}

        # A1.6 (2026-05-22): SEULGI_BYPASS_LLM=1 short-circuits BEFORE any
        # OpenRouter HTTP — zero token spend, deterministic favorite-side
        # results. Trader gates downstream still apply. Checked first so
        # the entire prompt-building path below is skipped on bypass.
        if _bypass_enabled():
            _log_bypass_startup_once()
            results = _build_bypass_results(matches)
            logger.info(
                "[ai] LLM_BYPASS active — synthesizing %d results", len(results),
            )
            prompt_meta = {
                "model": _BYPASS_MODEL_TAG,
                "bypass": True,
                "n_samples": 0,
                "samples_ok": 0,
                "temperature": 0.0,
                "is_republish": bool(is_republish),
                "similar_to": (similar_to or "")[:200] if similar_to else None,
                "source": source,
                "freshness_label": getattr(freshness, "label", None) if freshness else None,
                "nearest_age_secs": getattr(freshness, "nearest_age_secs", None) if freshness else None,
                "nearest_dist": getattr(freshness, "nearest_dist", None) if freshness else None,
                "relevance_scores": (
                    [getattr(v, "score", None) for v in relevance_verdicts]
                    if relevance_verdicts else None
                ),
                "market_context": [
                    _mkt_ctx_to_dict(getattr(m, "_mkt_ctx", None)) for m in matches
                ],
            }
            return results, prompt_meta

        # Freshness block replaces the legacy binary republish hint when
        # we have a labelled FreshnessResult. The legacy block stays as a
        # fallback for callers that haven't migrated to the assess() API.
        freshness_block = _format_freshness_block(freshness)
        if freshness_block:
            republish_block = ""
        else:
            republish_block = (
                _REPUBLISH_BLOCK.format(similar_to=similar_to[:200])
                if is_republish and similar_to else ""
            )
        source_block = f"Source: {source}\n" if source else ""
        # Lessons system retired 2026-05-22 — the seeded "treat as NEUTRAL"
        # advisories were biasing every output toward NEUTRAL while
        # providing no empirical lift. The template still references
        # {lessons_block} so we pass an empty string to keep the format()
        # call happy.
        lessons_block = ""

        # Build the numbered question block. mid_yes is the YES-token mid,
        # which is what "current likelihood of YES" maps to in [0,1].
        # When relevance verdicts are available, surface the Stage-2 score +
        # short reason so the prediction model can defer/agree explicitly.
        rv_by_idx = {}
        if relevance_verdicts:
            for i, v in enumerate(relevance_verdicts[:len(matches)]):
                rv_by_idx[i] = v
        lines = []
        for i, m in enumerate(matches):
            mid = getattr(m, "mid_yes", None)
            mid_str = f"{mid:.3f}" if mid is not None else "unknown"
            rv = rv_by_idx.get(i)
            if rv is not None:
                rv_str = f" | relevance={rv.score}:{rv.reason[:70]}"
            else:
                rv_str = ""
            lines.append(
                f"{i}. [YES mid={mid_str} | sim={m.similarity:.2f}{rv_str}] {m.question}"
            )
        questions_block = "\n".join(lines)

        # Cycle-11: optional pre-headline market behaviour block.
        # Rendered from per-match `_mkt_ctx` attrs (populated by the
        # runner via market_context.fetch_context_for_matches). Empty
        # string when no match has context, so unbreached.
        try:
            from market_context import format_market_context_block
            mkt_ctx_block = format_market_context_block(matches)
        except Exception:  # noqa: BLE001
            mkt_ctx_block = ""

        # Prepend freshness on its own line so blocks don't run together.
        if freshness_block and not freshness_block.endswith("\n"):
            freshness_block_norm = freshness_block + "\n"
        else:
            freshness_block_norm = freshness_block
        prompt = _BATCH_PROMPT_TEMPLATE.format(
            headline=headline,
            source_block=source_block,
            lessons_block=lessons_block,
            republish_block=republish_block or freshness_block_norm,
            mkt_ctx_block=mkt_ctx_block,
            questions_block=questions_block,
            n=len(matches),
        )

        # Use premium model if max similarity crosses threshold
        max_sim = max(getattr(m, "similarity", 0.0) for m in matches)
        model = (
            self._premium_model
            if self._premium_model and max_sim >= self._premium_sim_threshold
            else self._model
        )
        if model != self._model:
            logger.info("[ai] routing batch to premium model=%s (max_sim=%.2f)", model, max_sim)

        # Ensemble: N parallel calls at higher temperature, then aggregate
        # per-market across samples. N=1 reproduces the single-call path.
        N = self._ensemble_samples
        temp = self._ensemble_temperature if N > 1 else 0.1

        def _build_body() -> dict:
            return {
                "model": model,
                "messages": [
                    {"role": "system", "content": _BATCH_SYSTEM_MESSAGE},
                    {"role": "user",   "content": prompt},
                ],
                "max_tokens": 160 * len(matches) + 100,
                "temperature": temp,
                "response_format": {"type": "json_object"},
            }

        sample_responses = await asyncio.gather(
            *[self._call_api(_build_body()) for _ in range(N)],
            return_exceptions=False,
        )

        # Parse each sample. Drop ones that errored or failed to parse — if
        # we're left with zero, fall back to per-market on a single sample.
        per_sample_results: list[list[ClassifyResult]] = []
        per_sample_raws: list[list[str]] = []
        total_tokens = 0
        for data in sample_responses:
            if data is None:
                continue
            parsed = _parse_batch_response(data, len(matches))
            if parsed is None:
                continue
            per_sample_results.append(parsed[0])
            per_sample_raws.append(parsed[1])
            total_tokens += int(data.get("usage", {}).get("total_tokens", 0))

        prompt_meta = {
            "model": model,
            "n_samples": N,
            "samples_ok": len(per_sample_results),
            "temperature": temp,
            "is_republish": bool(is_republish),
            "similar_to": (similar_to or "")[:200] if similar_to else None,
            "source": source,
            "freshness_label": getattr(freshness, "label", None) if freshness else None,
            "nearest_age_secs": getattr(freshness, "nearest_age_secs", None) if freshness else None,
            "nearest_dist": getattr(freshness, "nearest_dist", None) if freshness else None,
            "relevance_scores": (
                [getattr(v, "score", None) for v in relevance_verdicts]
                if relevance_verdicts else None
            ),
            "market_context": [
                _mkt_ctx_to_dict(getattr(m, "_mkt_ctx", None)) for m in matches
            ],
        }

        if not per_sample_results:
            logger.warning("[ai] all %d ensemble samples failed — per-market fallback", N)
            fallback = await asyncio.gather(*[
                self.classify(headline, m.question,
                              mid_price=getattr(m, "mid_price", None),
                              similarity=m.similarity,
                              is_republish=is_republish, similar_to=similar_to)
                for m in matches
            ])
            return fallback, prompt_meta

        aggregated = _aggregate_ensemble(
            per_sample_results, n_markets=len(matches), raw_samples=per_sample_raws,
        )
        per_market_tokens = max(1, total_tokens // max(1, len(aggregated)))
        for r in aggregated:
            r.tokens_used = per_market_tokens

        logger.info(
            "[ai] ensemble classify: %d markets × %d/%d samples, model=%s, tokens=%d",
            len(matches), len(per_sample_results), N, model, total_tokens,
        )
        return aggregated, prompt_meta

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
    """Pull the content string from an OpenRouter chat completion response."""
    choices = data.get("choices", [])
    if not choices:
        return ""
    text = choices[0].get("message", {}).get("content", "").strip()
    # Strip markdown code fences (response_format=json_object usually prevents these)
    text = re.sub(r"^```[a-z]*\n?", "", text).rstrip("`").strip()
    return text


def _parse_one(obj: dict) -> ClassifyResult:
    """
    Parse one classification object. Accepts either the new distribution form
    (`p_yes`, `p_no`, `p_neutral`) or the legacy form (`direction`,
    `confidence`). Models occasionally regress to the legacy form; we accept
    both rather than throwing the row away.

    The distribution is normalized to sum 1 (defensive against models that
    don't quite hit unity), then we pick the argmax as the direction and use
    its probability as the confidence. argmax ties resolve to NEUTRAL.
    """
    reason = str(obj.get("reason", ""))[:200]
    # Cycle-7 fields: optional in the response. Defaulted to "no signal"
    # values so a model that regresses to the old schema doesn't poison the
    # downstream edge model with a bogus high p_alpha.
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
    """
    Collapse N ensemble samples into one per-market ClassifyResult.

    Inputs: `samples` is a list of N lists, each of length `n_markets`,
    holding one ClassifyResult per market per sample.

    Aggregation:
      * Distribution = mean of the per-sample (p_yes, p_no, p_neutral) tuples.
      * Direction = majority vote of per-sample argmaxes; tie → NEUTRAL.
        (Picking direction from the *averaged* distribution would silently
        prefer the largest-mean even when 2/3 samples disagreed; we want
        agreement to gate execution, so the vote count matters here.)
      * confidence = mean(P[winning_direction]) × agreement_fraction.
        This composes "how strongly does the model believe in its pick" with
        "how often does the model reach the same pick under resampling".
      * agreement = max(vote_count_yes, vote_count_no, vote_count_neutral) / N.
        1/N = no signal; 1.0 = unanimous.
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
            reason=reasons[0] if reasons else "",
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
    """
    Parse a batched response that should contain a JSON array of n objects.

    The LLM may return:
      - A top-level array: [{...}, ...]
      - A wrapper object: {"results": [...]}  or  {"classifications": [...]}

    Returns None if parsing fails so the caller can fall back.
    """
    try:
        text = _extract_text(data)
        if not text:
            return None
        parsed = json.loads(text)

        # Expected shape: {"results": [...]}. Some models still wrap differently;
        # accept a small set of fallback keys before giving up.
        if isinstance(parsed, dict):
            inner = (
                parsed.get("results")
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
