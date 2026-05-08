"""
AI-powered signal direction classifier using Google Gemini Flash (REST API).

Given a news signal and a Polymarket market question, determines whether
the signal suggests the market will resolve YES, NO, or is NEUTRAL/unclear.

Uses the Gemini REST API directly via aiohttp to avoid gRPC complications.
Falls back to NEUTRAL on API errors so the pipeline never stalls.
Results are cached in-memory keyed by (truncated signal text, question).
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional

import aiohttp

from irene_shared.models import Direction
from direction import DirectionResult

logger = logging.getLogger(__name__)

_GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"

_SYSTEM_INSTRUCTION = (
    "You are a prediction market analyst. "
    "Given a news snippet and a Polymarket question, classify the direction.\n"
    "Respond with EXACTLY one line: DIRECTION|CONFIDENCE\n"
    "  DIRECTION = YES, NO, or NEUTRAL\n"
    "  CONFIDENCE = float 0.0-1.0\n"
    "YES = news makes YES resolution more likely. "
    "NO = news makes NO resolution more likely. "
    "NEUTRAL = news is not clearly directional for this question."
)

_USER_TEMPLATE = "News: {signal_text}\n\nMarket question: {question}\n\nDirection|Confidence:"


class AIDirectionClassifier:
    """
    Async AI-based direction classifier using Google Gemini Flash via REST.

    Concurrent calls are throttled via a semaphore.
    Results are cached in-memory to avoid redundant calls for the same pair.
    """

    def __init__(
        self,
        api_key: str,
        model: str = "gemini-2.0-flash",
        max_concurrent: int = 8,
    ) -> None:
        self._api_key = api_key
        self._model = model
        self._url = _GEMINI_API_URL.format(model=model)
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._session: Optional[aiohttp.ClientSession] = None
        # (signal_text_prefix, question_prefix) → DirectionResult
        self._cache: dict[tuple[str, str], DirectionResult] = {}
        self._cache_max = 2000

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    async def classify(
        self,
        signal_text: str,
        question: str,
        min_confidence: float = 0.50,
    ) -> DirectionResult:
        """Classify signal direction relative to a Polymarket question."""
        cache_key = (signal_text[:200], question[:200])
        if cache_key in self._cache:
            return self._cache[cache_key]

        result = await self._call_api(signal_text, question, min_confidence)

        if len(self._cache) >= self._cache_max:
            oldest = next(iter(self._cache))
            del self._cache[oldest]
        self._cache[cache_key] = result
        return result

    async def classify_batch(
        self,
        signal_text: str,
        markets: list[tuple[str, str]],  # [(condition_id, question), ...]
        min_confidence: float = 0.50,
    ) -> dict[str, DirectionResult]:
        """Classify direction for multiple markets concurrently."""
        tasks = {
            cid: asyncio.create_task(
                self.classify(signal_text, question, min_confidence)
            )
            for cid, question in markets
        }
        results: dict[str, DirectionResult] = {}
        for cid, task in tasks.items():
            try:
                results[cid] = await task
            except Exception as exc:
                logger.warning("AI classify error for %s: %s", cid, exc)
                results[cid] = DirectionResult(Direction.NEUTRAL, 0.0, 0, 0)
        return results

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15),
            )
        return self._session

    async def _call_api(
        self,
        signal_text: str,
        question: str,
        min_confidence: float,
    ) -> DirectionResult:
        prompt = _USER_TEMPLATE.format(
            signal_text=signal_text[:600],
            question=question[:300],
        )
        payload = {
            "system_instruction": {"parts": [{"text": _SYSTEM_INSTRUCTION}]},
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "maxOutputTokens": 200,
                "temperature": 0.1,
                # Disable thinking budget for speed and determinism
                "thinkingConfig": {"thinkingBudget": 0},
            },
        }
        # Retry with exponential backoff on 429
        for attempt in range(3):
            try:
                rate_limited = False
                async with self._semaphore:
                    session = self._get_session()
                    async with session.post(
                        self._url,
                        params={"key": self._api_key},
                        json=payload,
                    ) as resp:
                        if resp.status == 429:
                            rate_limited = True
                        elif resp.status != 200:
                            body = await resp.text()
                            logger.warning("Gemini API HTTP %d: %s", resp.status, body[:200])
                            return DirectionResult(Direction.NEUTRAL, 0.0, 0, 0)
                        else:
                            data = await resp.json()

                if rate_limited:
                    wait = 2 ** attempt
                    logger.debug("Gemini 429 — retrying in %ds (attempt %d)", wait, attempt + 1)
                    await asyncio.sleep(wait)
                    continue

                # Extract text from response
                candidates = data.get("candidates", [])
                if not candidates:
                    return DirectionResult(Direction.NEUTRAL, 0.0, 0, 0)
                parts = candidates[0].get("content", {}).get("parts", [])
                raw = (parts[0].get("text", "") if parts else "").strip()

                result = _parse_response(raw, min_confidence)
                if result.direction != Direction.NEUTRAL:
                    logger.info(
                        "Gemini: %s|%.2f for %r",
                        result.direction.value, result.confidence, question[:70],
                    )
                else:
                    logger.debug(
                        "Gemini: NEUTRAL(%.2f) for %r [raw=%r]",
                        result.confidence, question[:60], raw,
                    )
                return result

            except Exception as exc:
                logger.warning("Gemini direction API error: %s", exc)
                return DirectionResult(Direction.NEUTRAL, 0.0, 0, 0)

        return DirectionResult(Direction.NEUTRAL, 0.0, 0, 0)


def _parse_response(raw: str, min_confidence: float) -> DirectionResult:
    """
    Parse Gemini response for direction and confidence.

    Tries the 'DIRECTION|CONFIDENCE' format first, then falls back to
    scanning for YES/NO/NEUTRAL keywords anywhere in the text.
    """
    import re

    raw_upper = raw.upper()

    # --- Primary: structured DIRECTION|CONFIDENCE parse ---
    try:
        first_line = raw.split("\n")[0].strip()
        parts = first_line.split("|")
        if len(parts) >= 2:
            direction_str = parts[0].strip().upper()
            # Extract first float-like token
            conf_match = re.search(r"[\d.]+", parts[1])
            if conf_match and direction_str in ("YES", "NO", "NEUTRAL"):
                confidence = float(conf_match.group())
                confidence = max(0.0, min(1.0, confidence))
                if direction_str == "YES":
                    direction = Direction.YES
                elif direction_str == "NO":
                    direction = Direction.NO
                else:
                    direction = Direction.NEUTRAL
                if direction != Direction.NEUTRAL and confidence < min_confidence:
                    direction = Direction.NEUTRAL
                return DirectionResult(direction, confidence, 0, 0)
    except Exception:
        pass

    # --- Fallback: scan text for YES/NO/NEUTRAL keywords + nearby numbers ---
    try:
        # Find direction keyword
        direction = Direction.NEUTRAL
        confidence = 0.5  # default mid-confidence if no number found

        if re.search(r"\bYES\b", raw_upper):
            direction = Direction.YES
        elif re.search(r"\bNO\b", raw_upper):
            direction = Direction.NO

        # Find a confidence number
        numbers = re.findall(r"0\.\d+|1\.0|1", raw)
        if numbers:
            confidence = float(numbers[0])
            confidence = max(0.0, min(1.0, confidence))

        if direction != Direction.NEUTRAL and confidence < min_confidence:
            direction = Direction.NEUTRAL

        return DirectionResult(direction, confidence, 0, 0)
    except Exception:
        return DirectionResult(Direction.NEUTRAL, 0.0, 0, 0)
