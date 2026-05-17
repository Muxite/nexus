"""
Stage-2 relevance gate — cheap LLM cull between embedding match and prediction.

Stage 1 (`MarketIndex.find_matches`) returns top-15 markets by cosine similarity
≥ 0.35. Many are topically adjacent but not market-moving. Stage 3
(`DirectionClassifier.classify_batch`) is the expensive prediction call.

This module sits between them. One batched LLM call per headline asks
YES/NO+reason for each candidate market. Only markets scored STRONG_YES or
YES (config-driven) survive to Stage 3.

The relevance gate runs at temperature ~0.1 on the cheapest available model
(`gemini-2.5-flash-lite` by default). On API/parse failure we fail-open
(keep every match) so a relevance hiccup never silently hides trades.

Per-market verdicts (including drops) are persisted by the runner so the
dashboard funnel still accounts for every decision.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass
from typing import Literal, Optional

import aiohttp


logger = logging.getLogger(__name__)


RelevanceScore = Literal["STRONG_YES", "YES", "WEAK", "NO"]


@dataclass
class RelevanceVerdict:
    keep: bool
    score: RelevanceScore
    reason: str
    tokens_used: int = 0


_SYSTEM_MSG = """\
You are a relevance filter for a Polymarket prediction-market trading bot.
For each numbered market, judge whether the headline carries information
that should *move that market's price*. Output ONLY valid JSON, no prose.
"""

_PROMPT = """\
HEADLINE: {headline}
{source_block}{freshness_block}{context_block}
For each numbered prediction market below, classify whether THIS headline
contains information that should move that market's price.

Scores:
  STRONG_YES — direct, market-moving information about the question
  YES        — meaningful but indirect (e.g. signals timing or sentiment)
  WEAK       — topically adjacent; unlikely to move price
  NO         — semantic overlap only; not about this market

Markets:
{questions_block}

Respond with EXACTLY this JSON: {{"results": [<{n} objects, ordered by idx>]}}
Each object: {{"idx": <int>, "score": "STRONG_YES"|"YES"|"WEAK"|"NO", "reason": "<one short clause>"}}
"""


class RelevanceFilter:
    """Shares the OpenRouter HTTP client style with DirectionClassifier."""

    def __init__(
        self,
        api_key: str,
        model: str = "google/gemini-2.5-flash-lite",
        api_base: str = "https://openrouter.ai/api/v1",
        temperature: float = 0.1,
        keep_scores: tuple = ("STRONG_YES", "YES"),
        max_tokens_per_market: int = 60,
        max_concurrent: int = 5,
        enabled: bool = True,
        min_top_similarity: float = 0.0,
    ) -> None:
        self._api_key = api_key
        self._model = model
        self._url = api_base.rstrip("/") + "/chat/completions"
        self._temperature = temperature
        self._keep_scores = set(keep_scores)
        self._max_tokens_per_market = max_tokens_per_market
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._session: Optional[aiohttp.ClientSession] = None
        self._enabled = enabled and bool(api_key)
        # Cycle-13 token reduction: when max(survivor.similarity) is below
        # this, the runner short-circuits Stage 2 + Stage 3 entirely.
        # Read via attribute access from engine.py — kept here so the
        # relevance config is the single source of truth.
        self._min_top_similarity = float(min_top_similarity)

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    async def filter(
        self,
        headline: str,
        matches: list,                 # list[MarketMatch]
        freshness=None,                # FreshnessResult | None
        source: Optional[str] = None,
    ) -> tuple[list, list[RelevanceVerdict], dict]:
        """
        Score each market for relevance; return (kept_matches, verdicts, meta).

        verdicts has one entry per input match, in the same order. On
        failure the gate fails open: every match is kept with score=YES
        and reason="relevance_unavailable".
        """
        if not matches:
            return [], [], {}

        # Fail-open default verdicts — used when the gate is disabled or errors.
        fail_open = [
            RelevanceVerdict(keep=True, score="YES", reason="relevance_unavailable")
            for _ in matches
        ]
        meta_base = {
            "model": self._model,
            "n_in": len(matches),
            "enabled": bool(self._enabled),
        }

        if not self._enabled:
            return list(matches), fail_open, {
                **meta_base, "fail_open": True,
                "n_kept": len(matches), "tokens": 0,
            }

        # Build prompt blocks.
        source_block = f"SOURCE: {source}\n" if source else ""
        freshness_block = ""
        context_block = ""
        if freshness is not None:
            age = freshness.nearest_age_secs
            age_str = (
                f"{int(age // 3600)}h ago" if age and age >= 3600
                else f"{int((age or 0) // 60)}m ago" if age else "no prior"
            )
            freshness_block = (
                f"FRESHNESS: {freshness.label}"
                f" (nearest prior: {age_str},"
                f" cosine_dist={(freshness.nearest_dist or 0.0):.2f})\n"
            )
            if freshness.recent_context:
                ctx_lines = []
                for c in freshness.recent_context:
                    cage = c.get("age_secs") or 0
                    cage_s = (
                        f"{int(cage // 3600)}h" if cage >= 3600
                        else f"{int(cage // 60)}m"
                    )
                    ctx_lines.append(
                        f"  - {cage_s} ago: {(c.get('headline') or '')[:120]}"
                    )
                context_block = "PRIOR COVERAGE (last 24h, same topic):\n" + "\n".join(ctx_lines) + "\n"

        # Numbered questions block.
        qlines = []
        for i, m in enumerate(matches):
            mid = getattr(m, "mid_yes", None)
            mid_str = f"{mid:.3f}" if mid is not None else "unknown"
            qlines.append(
                f"{i}. [YES mid={mid_str} | sim={m.similarity:.2f}] {m.question}"
            )
        questions_block = "\n".join(qlines)

        prompt = _PROMPT.format(
            headline=headline,
            source_block=source_block,
            freshness_block=freshness_block,
            context_block=context_block,
            questions_block=questions_block,
            n=len(matches),
        )

        body = {
            "model": self._model,
            "messages": [
                {"role": "system", "content": _SYSTEM_MSG},
                {"role": "user", "content": prompt},
            ],
            "max_tokens": self._max_tokens_per_market * len(matches) + 200,
            "temperature": self._temperature,
            "response_format": {"type": "json_object"},
        }

        data = await self._call_api(body)
        if data is None:
            logger.warning("[relevance] API call failed — fail-open (keep all %d)", len(matches))
            return list(matches), fail_open, {
                **meta_base, "fail_open": True,
                "n_kept": len(matches), "tokens": 0,
            }

        verdicts = _parse_response(data, len(matches))
        if verdicts is None:
            logger.warning("[relevance] parse failed — fail-open (keep all %d)", len(matches))
            return list(matches), fail_open, {
                **meta_base, "fail_open": True,
                "n_kept": len(matches), "tokens": 0,
            }

        tokens = int(data.get("usage", {}).get("total_tokens", 0))
        # Amortise tokens across markets so usage_events totals reconcile.
        per_market_tokens = tokens // max(1, len(matches))
        for v in verdicts:
            v.tokens_used = per_market_tokens
            v.keep = v.score in self._keep_scores

        kept = [m for m, v in zip(matches, verdicts) if v.keep]
        meta = {
            **meta_base,
            "n_kept": len(kept),
            "n_strong": sum(1 for v in verdicts if v.score == "STRONG_YES"),
            "tokens": tokens,
            "fail_open": False,
        }
        return kept, verdicts, meta

    async def _call_api(self, body: dict) -> Optional[dict]:
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/seulgi",
            "X-Title": "Seulgi-Relevance",
        }
        for attempt in range(3):
            try:
                async with self._semaphore:
                    session = self._get_session()
                    async with session.post(
                        self._url,
                        json=body,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=20),
                    ) as resp:
                        if resp.status == 429:
                            await asyncio.sleep(2 ** attempt)
                            continue
                        if resp.status != 200:
                            text = await resp.text()
                            logger.warning("[relevance] %d: %s", resp.status, text[:200])
                            return None
                        return await resp.json()
            except Exception as exc:
                logger.warning("[relevance] error: %s", exc)
                return None
        return None


_VALID_SCORES = {"STRONG_YES", "YES", "WEAK", "NO"}


def _parse_response(data: dict, n: int) -> Optional[list[RelevanceVerdict]]:
    """Pull the JSON results list out of an OpenRouter response."""
    choices = data.get("choices") or []
    if not choices:
        return None
    text = choices[0].get("message", {}).get("content", "").strip()
    text = re.sub(r"^```[a-z]*\n?", "", text).rstrip("`").strip()
    try:
        parsed = json.loads(text)
    except Exception:
        return None
    rows = parsed.get("results") if isinstance(parsed, dict) else parsed
    if not isinstance(rows, list):
        return None

    # Map idx → row so out-of-order responses still align.
    by_idx: dict[int, dict] = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        try:
            idx = int(r.get("idx"))
        except Exception:
            continue
        by_idx[idx] = r

    out: list[RelevanceVerdict] = []
    for i in range(n):
        r = by_idx.get(i, {})
        score = str(r.get("score", "")).upper().strip()
        if score not in _VALID_SCORES:
            # Conservative default for missing rows — keep, mark WEAK so
            # the trader still gets the AI's verdict downstream.
            out.append(RelevanceVerdict(keep=True, score="WEAK",
                                        reason="missing_from_response"))
            continue
        reason = str(r.get("reason", ""))[:200]
        out.append(RelevanceVerdict(keep=False, score=score, reason=reason))
    return out
