"""
Tag-based routing for macro headlines (Phase 4D.2).

## Specification
**Purpose.** Deterministic keyword pre-classifier that tags news headlines
with coarse macro categories (`interest_rates`, `geopolitics`, `elections`,
`sports`, `weather`, `tech`, `crypto`, `earnings`). Tagged headlines bypass
the SBERT cosine gate's strict lexical match requirement and route directly
to prediction markets whose questions mention the tag's bucket keywords.

Per master-plan A0.9 + 4D.2: macro headlines (Fed, FOMC, central banks)
die at the cosine gate because their text doesn't lexically resemble
prediction-market questions. A keyword tagger preserves them at near-zero
cost (<1ms per headline; no LLM, no network, no DB).

**Public interface.**
  - `HeadlineTags` dataclass: tags + confidence map + provenance method.
  - `tag_headline(headline) -> HeadlineTags` — deterministic regex match.
  - `route_market_bucket(tags) -> list[str]` — map tags to keyword buckets
    that the augment-step in `MarketIndex.find_matches` scans market
    questions for.

**Invariants.**
  - Regex match is case-insensitive (`re.IGNORECASE`).
  - `tag_headline` is pure; no I/O.
  - `route_market_bucket(["unknown_tag"]) == []` (no partial matches).
  - Confidence is 1.0 for every matched tag — the keyword tagger is binary
    by design. A future Phase-5 BERT tagger may emit graded scores; the
    `method` field distinguishes them.

**Non-goals.**
  - Tag confidence scoring beyond binary matched/unmatched.
  - Tag conflict resolution / priority ordering.
  - Discovery / curation tooling (operator edits this file via PR review).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass
class HeadlineTags:
    """Result of a single tag_headline() call."""
    tags: list[str] = field(default_factory=list)         # e.g. ["interest_rates"]
    confidence: dict[str, float] = field(default_factory=dict)  # per-tag 0-1
    method: str = "keyword"                               # "keyword" | "model"


# ---------------------------------------------------------------------------
# Keyword map — operator-curated, code (not config) so changes go through
# PR review. Each tag has 1+ regex patterns; ANY match → tag fires.
# Keep patterns conservative: false positives leak macro headlines into
# unrelated market buckets and slowly poison the per-tag PnL stats.
# ---------------------------------------------------------------------------


_TAG_KEYWORDS: dict[str, list[str]] = {
    "interest_rates": [
        r"\b(?:fed|federal reserve|fomc|rate cut|rate hike|interest rate|powell|"
        r"monetary policy|inflation target)\b",
        r"\b(?:ECB|BoE|Bank of England|RBA|BOJ)\b",
    ],
    "geopolitics": [
        r"\b(?:war|invasion|sanctions|nato|israel|iran|ukraine|russia|china taiwan)\b",
        r"\b(?:north korea|missile|nuclear)\b",
    ],
    "elections": [
        r"\b(?:election|poll|ballot|candidate|primary|senate|congress|trump|harris)\b",
    ],
    "sports": [
        r"\b(?:NFL|NBA|MLB|NHL|world cup|olympic|super bowl)\b",
    ],
    "weather": [
        r"\b(?:hurricane|storm|wildfire|earthquake|flood|tornado)\b",
    ],
    "tech": [
        r"\b(?:apple|google|microsoft|nvidia|openai|chatgpt|tesla|AI chip)\b",
    ],
    "crypto": [
        r"\b(?:bitcoin|btc|ethereum|crypto|coinbase|sec ruling)\b",
    ],
    "earnings": [
        r"\b(?:earnings|quarterly report|EPS|revenue beat|stock surge|stock plunge)\b",
    ],
}


# Pre-compile every pattern at module import. Case-insensitive across the
# board — headlines come capitalised/lower/mixed and the keyword list is
# written in mixed case (ECB, BoE, RBA, NFL...). Compiling once avoids the
# ~200-pattern recompile per call.
_TAG_PATTERNS: dict[str, list[re.Pattern]] = {
    tag: [re.compile(p, re.IGNORECASE) for p in patterns]
    for tag, patterns in _TAG_KEYWORDS.items()
}


# ---------------------------------------------------------------------------
# Market-bucket keyword map: tag → list of substrings to scan market
# questions for. These are matched lower-case substring (NOT regex) by the
# `MarketIndex.find_matches` augment step. Keep the lists short — every
# extra keyword broadens the recall but also picks up noise (e.g. "rate"
# alone catches "exchange rate", "approval rate", etc.).
# ---------------------------------------------------------------------------


_MARKET_BUCKETS: dict[str, list[str]] = {
    "interest_rates": ["fed", "rate", "fomc", "inflation", "bond", "powell", "ecb"],
    "geopolitics":    ["war", "invasion", "sanction", "nato", "israel", "iran",
                       "ukraine", "russia", "china", "taiwan", "missile", "nuclear"],
    "elections":      ["election", "vote", "ballot", "senate", "congress",
                       "primary", "trump", "harris", "president"],
    "sports":         ["nfl", "nba", "mlb", "nhl", "world cup", "olympic", "super bowl"],
    "weather":        ["hurricane", "storm", "wildfire", "earthquake", "flood",
                       "tornado"],
    "tech":           ["apple", "google", "microsoft", "nvidia", "openai",
                       "chatgpt", "tesla", "ai chip"],
    "crypto":         ["bitcoin", "btc", "ethereum", "eth", "crypto", "coinbase"],
    "earnings":       ["earnings", "quarterly report", "eps", "revenue", "stock"],
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def tag_headline(headline: str) -> HeadlineTags:
    """Deterministic keyword tagging.

    Returns `HeadlineTags` with one entry in `tags` per category that
    matched at least one regex. Confidence is 1.0 for matched tags and the
    set order follows `_TAG_KEYWORDS` insertion order so two callers with
    the same headline always see the same tag list.

    Empty / whitespace headlines short-circuit to an empty result.
    """
    if not headline or not headline.strip():
        return HeadlineTags(tags=[], confidence={}, method="keyword")

    tags: list[str] = []
    confidence: dict[str, float] = {}
    for tag, patterns in _TAG_PATTERNS.items():
        for pat in patterns:
            if pat.search(headline):
                tags.append(tag)
                confidence[tag] = 1.0
                break  # one match per tag is enough — don't re-scan

    return HeadlineTags(tags=tags, confidence=confidence, method="keyword")


def route_market_bucket(tags: list[str]) -> list[str]:
    """Map tags to a deduped list of market-question keywords.

    Example: `route_market_bucket(["interest_rates"])` →
    `["fed", "rate", "fomc", "inflation", "bond", "powell", "ecb"]`.

    Unknown tags contribute zero keywords (no KeyError, no warning — the
    caller may pass tag names from a newer tagger version against an older
    bucket map and we don't want that to crash ingress).

    Order follows the input tag order; duplicates collapse to the first
    occurrence (so the caller sees a stable, dedup'd keyword list).
    """
    seen: set[str] = set()
    result: list[str] = []
    for tag in tags:
        bucket = _MARKET_BUCKETS.get(tag, [])
        for kw in bucket:
            if kw not in seen:
                seen.add(kw)
                result.append(kw)
    return result


# ---------------------------------------------------------------------------
# Module-level helpers for callers / tests
# ---------------------------------------------------------------------------


def supported_tags() -> list[str]:
    """Return the curated tag list in declared order. Used by dashboards
    and tests that want to assert against the full set without importing
    the private `_TAG_KEYWORDS` dict."""
    return list(_TAG_KEYWORDS.keys())
