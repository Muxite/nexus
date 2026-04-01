"""
Keyword-based signal direction classifier.

Classifies a news headline/text as YES, NO, or NEUTRAL relative to a
prediction market. Zero external dependencies — no LLM, no network.

Config can extend the keyword lists via signals.yes_keywords and
signals.no_keywords. The built-in lists cover the most common resolution
language for political, economic, and crypto markets.
"""

from __future__ import annotations

import re
from typing import NamedTuple

from irene_shared.models import Direction

# ---------------------------------------------------------------------------
# Built-in keyword lists
# ---------------------------------------------------------------------------

_YES_KEYWORDS: list[str] = [
    "passes", "passed", "approved", "approves", "wins", "won", "victory",
    "elected", "signed", "confirmed", "certified", "reaches", "reached",
    "hits", "exceeded", "exceeds", "above", "rises above", "surpasses",
    "granted", "awarded", "launched", "completed", "achieved", "advanced",
    "clinches", "secures", "agrees", "agreed", "ratified", "enacted",
    "announces", "confirmed", "verified", "validated",
]

_NO_KEYWORDS: list[str] = [
    "fails", "failed", "rejected", "rejects", "loses", "lost", "defeat",
    "defeated", "vetoed", "vetoes", "suspended", "cancels", "cancelled", "canceled",
    "falls below", "below", "under", "drops below", "misses", "missed",
    "denied", "blocks", "blocked", "overturned", "withdrawn", "scrapped",
    "postponed", "delayed", "collapses", "collapsed", "abandoned", "dismissed",
    "acquitted", "not guilty",
]


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


class DirectionResult(NamedTuple):
    direction: Direction
    confidence: float  # 0.0 – 1.0
    yes_hits: int
    no_hits: int


# ---------------------------------------------------------------------------
# Classifier
# ---------------------------------------------------------------------------


class DirectionClassifier:
    """
    Keyword classifier for signal direction.

    Instantiate once; config lists are merged on each classify() call so
    config hot-reload works without re-instantiation.
    """

    def classify(
        self,
        text: str,
        extra_yes_keywords: list[str] | None = None,
        extra_no_keywords: list[str] | None = None,
        min_confidence: float = 0.30,
    ) -> DirectionResult:
        """
        Classify a text snippet as YES, NO, or NEUTRAL.

        Args:
            text:                Text to classify (title + description).
            extra_yes_keywords:  Additional YES keywords from config.
            extra_no_keywords:   Additional NO keywords from config.
            min_confidence:      Minimum |confidence| to return YES/NO;
                                 below this returns NEUTRAL.

        Returns:
            DirectionResult with direction, confidence, and raw hit counts.
        """
        text_lower = text.lower()

        yes_kws = _YES_KEYWORDS + (extra_yes_keywords or [])
        no_kws = _NO_KEYWORDS + (extra_no_keywords or [])

        yes_hits = self._count_hits(text_lower, yes_kws)
        no_hits = self._count_hits(text_lower, no_kws)

        total = yes_hits + no_hits
        if total == 0:
            return DirectionResult(Direction.NEUTRAL, 0.0, 0, 0)

        # Confidence = net signal normalised by total hits, clamped to [0, 1]
        raw_confidence = (yes_hits - no_hits) / total
        confidence = abs(raw_confidence)

        if confidence < min_confidence:
            return DirectionResult(Direction.NEUTRAL, confidence, yes_hits, no_hits)

        direction = Direction.YES if raw_confidence > 0 else Direction.NO
        return DirectionResult(direction, confidence, yes_hits, no_hits)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    def _count_hits(text: str, keywords: list[str]) -> int:
        """Count how many keywords appear in text (whole-word or phrase match)."""
        count = 0
        for kw in keywords:
            # Multi-word phrases: simple substring match
            # Single words: require word boundary to avoid false partials
            if " " in kw:
                if kw in text:
                    count += 1
            else:
                if re.search(r"\b" + re.escape(kw) + r"\b", text):
                    count += 1
        return count
