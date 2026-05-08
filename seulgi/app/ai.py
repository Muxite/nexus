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

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import aiohttp

from profiler import prof

logger = logging.getLogger(__name__)

_DEFAULT_API_BASE = "https://openrouter.ai/api/v1"
_CHAT_PATH = "/chat/completions"

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
{republish_block}
Evaluate the headline against each of the following Polymarket questions:

{questions_block}

For each question output exactly:
{{"idx": <0-based index>, "direction": "YES"|"NO"|"NEUTRAL", "confidence": 0.0-1.0, "reason": "<one sentence>"}}

Rules:
- YES  = headline increases likelihood of YES outcome
- NO   = headline decreases likelihood of YES outcome
- NEUTRAL = not clearly related, ambiguous, or already priced in
- confidence = how certain you are (0.5 = barely, 1.0 = certain)
- If current mid price already reflects the news (≥0.85 or ≤0.15), return NEUTRAL
- Respond with a JSON array of {n} objects, one per question, ordered by idx.
"""

_REPUBLISH_BLOCK = """\
IMPORTANT — possible republish: a semantically similar headline was seen before:
  "{similar_to}"
If no new information is added vs. the original, return NEUTRAL for all markets.
"""


class Direction(str, Enum):
    YES = "YES"
    NO = "NO"
    NEUTRAL = "NEUTRAL"


@dataclass
class ClassifyResult:
    direction: Direction
    confidence: float      # 0.0–1.0
    reason: str
    tokens_used: int = 0


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
    ) -> None:
        self._api_key = api_key
        self._model = model
        self._premium_model = premium_model
        self._premium_sim_threshold = premium_sim_threshold
        self._url = api_base.rstrip("/") + _CHAT_PATH
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._session: Optional[aiohttp.ClientSession] = None

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
    ) -> list[ClassifyResult]:
        """
        Classify a headline against ALL matching markets in a single API call.

        Up to 15× cheaper than calling classify() per market.
        Returns one ClassifyResult per match, in the same order as `matches`.
        Falls back to per-market classify() if the batch parse fails.
        """
        if not matches:
            return []

        republish_block = (
            _REPUBLISH_BLOCK.format(similar_to=similar_to[:200])
            if is_republish and similar_to else ""
        )

        # Build the numbered question block
        lines = []
        for i, m in enumerate(matches):
            mid_str = f"{m.mid_price:.3f}" if getattr(m, "mid_price", None) is not None else "unknown"
            lines.append(
                f"{i}. [{mid_str} mid | sim={m.similarity:.2f}] {m.question}"
            )
        questions_block = "\n".join(lines)

        prompt = _BATCH_PROMPT_TEMPLATE.format(
            headline=headline,
            republish_block=republish_block,
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

        body = {
            "model": model,
            "messages": [
                {"role": "system", "content": _BATCH_SYSTEM_MESSAGE},
                {"role": "user",   "content": prompt},
            ],
            "max_tokens": 80 * len(matches) + 50,
            "temperature": 0.1,
            "response_format": {"type": "json_object"},
        }

        data = await self._call_api(body)
        if data is None:
            # Fall back to per-market
            return await asyncio.gather(*[
                self.classify(headline, m.question,
                              mid_price=getattr(m, "mid_price", None),
                              similarity=m.similarity,
                              is_republish=is_republish, similar_to=similar_to)
                for m in matches
            ])

        results = _parse_batch_response(data, len(matches))
        if results is None:
            logger.warning("[ai] batch parse failed — falling back to per-market classify")
            return await asyncio.gather(*[
                self.classify(headline, m.question,
                              mid_price=getattr(m, "mid_price", None),
                              similarity=m.similarity,
                              is_republish=is_republish, similar_to=similar_to)
                for m in matches
            ])

        usage = data.get("usage", {})
        tokens = int(usage.get("total_tokens", 0))
        logger.info(
            "[ai] batch classify: %d markets, model=%s, tokens=%d",
            len(matches), model, tokens,
        )
        # Distribute token cost evenly across results
        per_market = max(1, tokens // len(results))
        for r in results:
            r.tokens_used = per_market
        return results

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
    """Parse a single {direction, confidence, reason} dict into a ClassifyResult."""
    direction_str = str(obj.get("direction", "NEUTRAL")).upper()
    try:
        direction = Direction(direction_str)
    except ValueError:
        direction = Direction.NEUTRAL
    confidence = float(obj.get("confidence", 0.0))
    confidence = max(0.0, min(1.0, confidence))
    reason = str(obj.get("reason", ""))[:200]
    return ClassifyResult(direction, confidence, reason)


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


def _parse_batch_response(data: dict, n: int) -> Optional[list[ClassifyResult]]:
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

        # Unwrap common wrapper keys
        if isinstance(parsed, dict):
            for key in ("results", "classifications", "answers", "items", "data"):
                if key in parsed and isinstance(parsed[key], list):
                    parsed = parsed[key]
                    break
            else:
                # Maybe it's a single object pretending to be the whole array
                if "direction" in parsed:
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

        # Pad with NEUTRAL if model returned fewer items than expected
        while len(results) < n:
            results.append(ClassifyResult(Direction.NEUTRAL, 0.0, "missing"))

        return results[:n]

    except Exception as exc:
        logger.warning("[ai] batch parse error: %s | text=%s", exc,
                       _extract_text(data)[:200] if data else "")
        return None
