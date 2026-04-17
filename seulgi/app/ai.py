"""
Seulgi AI direction classifier — OpenRouter backend.

Uses OpenRouter's OpenAI-compatible chat completions API so any model
available on OpenRouter can be used without changing this file.

Default model: google/gemini-2.5-flash  (fast, cheap, great at structured JSON)

Config (config.yaml → ai section):
    model     : OpenRouter model ID, e.g. "google/gemini-2.5-flash"
    api_key   : OpenRouter API key (or set OPENROUTER_API_KEY env var)
    api_base  : override base URL (default: https://openrouter.ai/api/v1)
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

logger = logging.getLogger(__name__)

_DEFAULT_API_BASE = "https://openrouter.ai/api/v1"
_CHAT_PATH = "/chat/completions"

_PROMPT_TEMPLATE = """\
You are a prediction market analyst. Given a news headline and a Polymarket question,
decide whether the headline is bullish (YES), bearish (NO), or irrelevant (NEUTRAL)
for the YES outcome of the question.

Headline: {headline}

Question: {question}

Reply with a JSON object only, no prose:
{{"direction": "YES"|"NO"|"NEUTRAL", "confidence": 0.0-1.0, "reason": "one sentence"}}

Rules:
- YES = headline makes the YES outcome more likely
- NO  = headline makes the NO outcome more likely (i.e. YES outcome less likely)
- NEUTRAL = not clearly related, too vague, or mixed signals
- confidence = how sure you are (0.5 = barely, 1.0 = certain)
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

    Semaphore caps concurrent requests (default=5).
    Retries on 429 with exponential backoff (up to 3 attempts).

    Usage:
        clf = DirectionClassifier(api_key="sk-or-...", model="google/gemini-2.5-flash")
        result = await clf.classify("Fed cuts rates", "Will Fed cut rates in 2025?")
    """

    def __init__(
        self,
        api_key: str,
        model: str = "google/gemini-2.5-flash",
        api_base: str = _DEFAULT_API_BASE,
        max_concurrent: int = 5,
    ) -> None:
        self._api_key = api_key
        self._model = model
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

    async def classify(self, headline: str, question: str) -> ClassifyResult:
        """
        Classify a headline against a Polymarket question.

        Returns NEUTRAL with confidence=0.0 on any unrecoverable error.
        Retries up to 3 times on rate-limit (HTTP 429).
        """
        prompt = _PROMPT_TEMPLATE.format(headline=headline, question=question)
        body = {
            "model": self._model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 200,
            "temperature": 0.1,
        }
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/seulgi",
            "X-Title": "Seulgi",
        }

        for attempt in range(3):
            rate_limited = False
            data: dict = {}
            try:
                async with self._semaphore:
                    session = self._get_session()
                    async with session.post(
                        self._url,
                        json=body,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=25),
                    ) as resp:
                        if resp.status == 429:
                            rate_limited = True
                        elif resp.status != 200:
                            text = await resp.text()
                            logger.warning("[ai] OpenRouter %d: %s", resp.status, text[:200])
                            return ClassifyResult(Direction.NEUTRAL, 0.0, "api_error")
                        else:
                            data = await resp.json()

                if rate_limited:
                    wait = 2 ** attempt
                    logger.debug("[ai] rate limited, sleeping %ds", wait)
                    await asyncio.sleep(wait)
                    continue

                return _parse_response(data)

            except Exception as exc:
                logger.warning("[ai] classify error: %s", exc)
                return ClassifyResult(Direction.NEUTRAL, 0.0, "exception")

        logger.warning("[ai] giving up after 3 rate-limit retries")
        return ClassifyResult(Direction.NEUTRAL, 0.0, "rate_limited")


# ---------------------------------------------------------------------------
# Response parser — OpenAI chat completions format
# ---------------------------------------------------------------------------


def _parse_response(data: dict) -> ClassifyResult:
    """Extract direction/confidence/reason from an OpenRouter chat completion response."""
    try:
        choices = data.get("choices", [])
        if not choices:
            return ClassifyResult(Direction.NEUTRAL, 0.0, "no_candidates")

        text = (
            choices[0]
            .get("message", {})
            .get("content", "")
            .strip()
        )

        # Strip markdown code fences
        text = re.sub(r"^```[a-z]*\n?", "", text).rstrip("`").strip()

        obj = json.loads(text)
        direction_str = str(obj.get("direction", "NEUTRAL")).upper()
        try:
            direction = Direction(direction_str)
        except ValueError:
            direction = Direction.NEUTRAL

        confidence = float(obj.get("confidence", 0.0))
        confidence = max(0.0, min(1.0, confidence))
        reason = str(obj.get("reason", ""))[:200]

        usage = data.get("usage", {})
        tokens = int(usage.get("total_tokens", 0))

        return ClassifyResult(direction, confidence, reason, tokens)

    except Exception as exc:
        logger.warning("[ai] parse error: %s", exc)
        return ClassifyResult(Direction.NEUTRAL, 0.0, "parse_error")
