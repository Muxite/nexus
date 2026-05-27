"""
## Specification

**Purpose.** OpenRouter chat-completions wrapper that turns a fully-composed
prompt into a `DirectionResult` (YES / NO / NEUTRAL + confidence). Owns the
LLM I/O — every LLM call in irene goes through this module, so budget
tracking, response caching, and capture writes have exactly one place to
live.

**Public interface.**
- `DirectionResult` — frozen dataclass (direction / confidence /
  prediction_id / cached / raw_response).
- `AIClient`:
  - `AIClient(*, db: DB, budget: Budget, config: Config)` — construct.
  - `await ai.direction(*, signal_id, prompt, is_tracer=False, tracer_id=None)`
    — the only entry point P1 uses. Reads `cycle.current_cycle_id()`
    internally.
  - `await ai.close()` — close the underlying HTTP client.
- `PROMPT_VERSION` (class constant) — bumped any time the JSON-suffix
  contract changes so old cache rows are invalidated naturally.

**Owned tables** (`docs/modularization.md` §4): `llm_calls`, `predictions`,
`llm_parse_failures`. DDL is registered with `db.register_schema()` at
import time.

**Invariants.**
- One `predictions` row per `(signal_id, model, prompt_version)` —
  `UNIQUE(signal_id, model, prompt_version)`. A re-call returns the
  existing row.
- `llm_calls` is keyed by `prompt_sha = sha1(prompt + model + prompt_version)`.
  A cache hit re-uses the row's `response_body` and avoids the HTTP call.
  Cache misses always insert a new `llm_calls` row.
- `is_tracer` / `tracer_id` are stamped on every owned-table row from the
  caller's arguments. ai.py does not look at any context for these — it
  takes them explicitly.
- `cycle_id` is read from `cycle.current_cycle_id()`. An unset context
  raises (programmer error, not a runtime condition).
- Budget paused short-circuits BEFORE the HTTP call:
  `predictions` (NEUTRAL, 0.0) and `llm_parse_failures(reason='budget_paused')`
  are written, no `llm_calls` row, no network I/O.
- A 402 response: `budget.note_402()`, written as
  `llm_parse_failures(reason='402_credits')`, returns NEUTRAL. A 2xx with
  parseable JSON: `budget.note_ok()`.
- A 2xx with malformed JSON: `llm_parse_failures(reason='parse_error')`,
  returns NEUTRAL.

**Side effects.** HTTP calls to `https://openrouter.ai/api/v1/chat/completions`.
SQLite writes to the three owned tables. Bumps `budget` streak counters.

**Failure modes.**
- Network error (`httpx.HTTPError`): logged, written as
  `llm_parse_failures(reason='http_error')`, returns NEUTRAL.
- Non-2xx, non-402 response: same as network error, with the status code
  in `reason`.
- Budget paused: short-circuit (see invariants).
- Malformed JSON in the response: `reason='parse_error'`, NEUTRAL.

**Non-goals.**
- Prompt composition (callers compose the full prompt; ai.py only appends
  the JSON-format suffix that pins the response shape).
- Multi-shot / function-calling / tool use — `direction()` is the only
  shape P1 needs.
- Streaming. The chat completion is a single request/response.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import httpx

from app import db as db_module
from app.budget import Budget
from app.config import Config
from app.cycle import current_cycle_id
from app.db import DB

logger = logging.getLogger(__name__)


_OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
_VALID_DIRECTIONS = ("YES", "NO", "NEUTRAL")
# v5: raw single-character labels emitted by the LLM. The trade-side
# `direction` column collapses these into 3 buckets (Y→YES, N→NO,
# P/U/R→NEUTRAL) so the gate cascade is unchanged. The raw label lives
# on `predictions.label` for diagnostics — it distinguishes
# "weak evidence" (P), "uncertain" (U), and "retrieval mismatch" (R).
_VALID_LABELS = ("Y", "N", "P", "U", "R")

# Per `docs/philosophy.md`: irene is a speed bot, not a forecaster. The
# system prompt below is intentionally tiny + STATIC so providers that
# do prompt-prefix caching (Anthropic / Gemini-via-OpenRouter / vLLM)
# can keep it warm. The user message carries only Condition + News.
#
# We force `max_tokens=1`, `temperature=0`, and a `logit_bias` that
# concentrates probability on `Y` / `N` / `U` — three token IDs the
# parse layer maps to direction. The savings vs. the old JSON-parse
# path are dominated by tokens-out: ~250 → ~1, cutting time-to-final-
# token by roughly an order of magnitude.
_SYSTEM_MESSAGE = (
    "You are a fast-trading market evaluator. Read the News and choose "
    "one label for the Market Condition.\n"
    "Reply with JSON only — exactly one of:\n"
    '  {"a":"Y","c":<0-9>}  trade YES — news strongly supports condition is TRUE. c=0 weakest, c=9 strongest.\n'
    '  {"a":"N","c":<0-9>}  trade NO  — news strongly supports condition is FALSE. c=0 weakest, c=9 strongest.\n'
    '  {"a":"P"}            pass     — news IS about this market but evidence is weak.\n'
    '  {"a":"U"}            uncertain — you can\'t tell which way this news cuts.\n'
    '  {"a":"R"}            mismatch — news is NOT actually about this market.\n'
    "Use the field 'a' for the letter (Y/N/P/U/R) and 'c' for the confidence digit on Y/N.\n"
    "No prose, no explanation — JSON only."
)

# Token-level forcing. `logit_bias` keys are token IDs — for chat
# providers we pass the bare characters and rely on the provider to map
# them. Providers that don't honour `logit_bias` ignore the field; the
# `max_tokens=1` + `temperature=0` constraints are universal.
# Best-effort token forcing — most providers ignore logit_bias.
# Concentrates probability on all 5 v5 labels.
_LOGIT_BIAS_YNU = {"Y": 100, "N": 100, "P": 100, "U": 100, "R": 100}


# Noisy-explorer relatedness call — a cheap binary classifier that runs
# AFTER the permissive DKG gate but BEFORE odds capture. The goal is to
# burn as few tokens as possible while filtering out slop pairs the
# embeddings happened to score high on. Output is `{related, confidence,
# reason}` not direction; we never feed this into a trade gate.
_RELATEDNESS_SYSTEM_MESSAGE = (
    "You are a relevance classifier for prediction markets. Decide if "
    "the News article is directly relevant to the Market — i.e. could "
    "this news plausibly move the market's price.\n"
    'Reply with JSON only: {"related": true|false, "confidence": 0.0-1.0, '
    '"reason": "<=30 words"}. '
    "related=true only when the news could move the market price. "
    "related=false when the topics are unrelated, the news is about a "
    "different entity, or the news is too generic to move the market.\n"
    "No prose outside JSON; no markdown fences."
)

# Bumping invalidates the relatedness cache namespace (`llm_calls`
# rows keyed on prompt_sha that includes this version).
RELATEDNESS_PROMPT_VERSION: str = "v1"


_AI_DDL = [
    """
    CREATE TABLE IF NOT EXISTS llm_calls (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        prompt_sha      TEXT NOT NULL UNIQUE,
        model           TEXT NOT NULL,
        prompt_version  TEXT NOT NULL,
        prompt          TEXT NOT NULL,
        response_body   TEXT,
        status_code     INTEGER,
        latency_ms      REAL,
        tokens_in       INTEGER,
        tokens_out      INTEGER,
        is_tracer       INTEGER NOT NULL DEFAULT 0,
        tracer_id       TEXT,
        created_at      TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_llm_calls_prompt_sha ON llm_calls(prompt_sha)",
    # NOTE (P3 chunk 3): `signal_id` is nullable because the DKG trade
    # path has no signal backing it (`app/stages/dkg_trade.py`). The
    # MVP path's per-signal idempotency is enforced by a partial
    # unique index on (signal_id, model, prompt_version) WHERE
    # signal_id IS NOT NULL. DKG calls don't dedupe at the
    # predictions level — they get a fresh row per (event, market)
    # pair, and the LLM cache (`llm_calls.prompt_sha`) avoids the
    # actual HTTP round-trip.
    """
    CREATE TABLE IF NOT EXISTS predictions (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id       INTEGER,
        llm_call_id     INTEGER REFERENCES llm_calls(id),
        model           TEXT NOT NULL,
        prompt_version  TEXT NOT NULL,
        direction       TEXT NOT NULL CHECK(direction IN ('YES','NO','NEUTRAL')),
        confidence      REAL NOT NULL CHECK(confidence >= 0.0 AND confidence <= 1.0),
        cached          INTEGER NOT NULL DEFAULT 0,
        raw_response    TEXT,
        -- v5: raw single-char label (Y/N/P/U/R). '?' for parse failures
        -- that still write a NEUTRAL prediction. Nullable for back-compat
        -- with the v4-and-earlier rows that don't have it.
        label           TEXT,
        cycle_id        INTEGER NOT NULL,
        is_tracer       INTEGER NOT NULL DEFAULT 0,
        tracer_id       TEXT,
        created_at      TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_predictions_signal_id ON predictions(signal_id)",
    "CREATE INDEX IF NOT EXISTS idx_predictions_cycle_id ON predictions(cycle_id)",
    "CREATE INDEX IF NOT EXISTS idx_predictions_label ON predictions(label)",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_predictions_signal_model_version "
    "ON predictions(signal_id, model, prompt_version) "
    "WHERE signal_id IS NOT NULL",
    """
    CREATE TABLE IF NOT EXISTS llm_parse_failures (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id       INTEGER,
        model           TEXT,
        prompt_sha      TEXT,
        reason          TEXT NOT NULL,
        raw_response    TEXT,
        cycle_id        INTEGER,
        is_tracer       INTEGER NOT NULL DEFAULT 0,
        tracer_id       TEXT,
        created_at      TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_llm_parse_failures_reason ON llm_parse_failures(reason)",
    # Cycle 14: A/B probe lane. One row per (production call, challenger
    # provider) when the sampled-shadow probe fires. NEVER consulted by
    # trade decisions — pure observability. See
    # `irene/plans/llm-prompt-cache.md` §1.
    #
    # `prediction_id` is the production prediction this row is shadowing
    # (nullable on failure to record per-prediction context, e.g. probes
    # that fire on a NEUTRAL/parse-error production result). `signal_id`
    # mirrors the production call (nullable for DKG path). `prompt_hash`
    # is SHA-256 of the exact prompt text — independent of the
    # `llm_calls.prompt_sha` (SHA-1 over a different envelope) so the
    # two cache spaces stay distinct and a future re-probe can dedupe
    # cleanly.
    #
    # On failure: `ttft_ms`, `total_ms`, `direction`, `confidence`, and
    # `raw_response` are NULL and `error` is populated ("timeout",
    # "http_error: …", "rate_limit", "parse_error", etc.). Successful
    # rows have `error IS NULL`.
    """
    CREATE TABLE IF NOT EXISTS llm_probe_calls (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id       INTEGER,
        prediction_id   INTEGER REFERENCES predictions(id),
        prompt_hash     TEXT NOT NULL,
        provider        TEXT NOT NULL,
        model           TEXT NOT NULL,
        ttft_ms         REAL,
        total_ms        REAL,
        direction       TEXT,
        confidence      REAL,
        raw_response    TEXT,
        error           TEXT,
        created_at      TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_llm_probe_calls_prediction "
    "ON llm_probe_calls(prediction_id)",
    "CREATE INDEX IF NOT EXISTS idx_llm_probe_calls_provider "
    "ON llm_probe_calls(provider)",
    "CREATE INDEX IF NOT EXISTS idx_llm_probe_calls_prompt_hash "
    "ON llm_probe_calls(prompt_hash)",
]
db_module.register_schema("app/ai.py", _AI_DDL)


@dataclass(frozen=True)
class DirectionResult:
    direction: str       # "YES" | "NO" | "NEUTRAL" — what the gate cascade reads
    confidence: float
    prediction_id: int
    cached: bool
    raw_response: str
    # Raw v5 label (Y/N/P/U/R/?). Defaults to "?" so test fakes built
    # before v5 keep working — production paths always pass a real label.
    label: str = "?"


@dataclass(frozen=True)
class RelatednessResult:
    """Output of `AIClient.relatedness()` — the noisy-explorer's cheap
    binary classifier on (article ↔ market). The caller embeds the
    fields inline on the `explorations` row; we deliberately do not
    write to `predictions` (different output schema, different
    analytics) — only to `llm_calls` for cache reuse."""

    related: bool
    confidence: float    # 0.0 on related=False, ≤1.0 on True
    reason: str          # ≤30-word LLM-emitted reason, possibly empty
    prompt_hash: str     # sha1 of the relatedness prompt envelope
    cached: bool
    raw_response: str


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


_LABEL_TO_DIRECTION = {
    "Y": ("YES", 0.80),       # default conf; raised by digit 'c' if present
    "N": ("NO", 0.80),
    # Pass / Uncertain / Mismatch all reduce to NEUTRAL for the gate
    # cascade — the raw letter lives on `predictions.label` for the
    # dashboard + chunk analysis to read.
    "P": ("NEUTRAL", 0.0),
    "U": ("NEUTRAL", 0.0),
    "R": ("NEUTRAL", 0.0),
}

# Multi-letter words the model is most likely to write instead of the
# single-char label. ANY OTHER VALUE rejects with None — this is what
# fixes the v5 bug where {"decision":"NEUTRAL"} got misread as NO
# (first char 'N' of the value).
_WORD_TO_LABEL = {
    "YES": "Y",
    "NO": "N",
    "PASS": "P",
    "UNCERTAIN": "U",
    "NEUTRAL": "U",  # alias — model often says NEUTRAL meaning U
    "MISMATCH": "R",
    "RETRIEVAL": "R",
}


def _confidence_from_digit(c) -> float:
    """Map 0-9 → [0.55, 0.95] so Kelly actually varies. None / out-of-range
    → default 0.80 so v6-without-digit answers still trade.
    """
    if c is None:
        return 0.80
    try:
        n = int(c)
    except (TypeError, ValueError):
        return 0.80
    if n < 0 or n > 9:
        return 0.80
    val = 0.50 + n * 0.05
    return max(0.55, min(0.95, val))


def _interpret_value(s: str) -> str | None:
    """Return a normalised single-char label (Y/N/P/U/R) or None.

    Strictly:
    - exact 1 char, case-insensitive, in {Y, N, P, U, R}
    - OR exact full word matching _WORD_TO_LABEL (case-insensitive)
    Any other string → None. This is what stopped the v5 parser from
    misreading {"decision":"NEUTRAL"} as a NO trade.
    """
    if not isinstance(s, str):
        return None
    t = s.strip().upper()
    if not t:
        return None
    if len(t) == 1 and t in _LABEL_TO_DIRECTION:
        return t
    if t in _WORD_TO_LABEL:
        return _WORD_TO_LABEL[t]
    return None


def _parse_response(raw: str) -> tuple[str, float, str] | None:
    """Return (direction, confidence, label) on success, None on parse error.

    The LLM is supposed to emit a single character: Y / N / U.
    In practice some providers (notably Gemini-2.5-flash over OpenRouter)
    ignore `logit_bias` and prefix the answer with a markdown fence or
    leading whitespace. v3 is tolerant: strip ``` / whitespace / control
    chars, then scan for the first Y/N/U.

    - first 'Y' (any case) → ('YES', 1.0)
    - first 'N' (any case) → ('NO', 1.0)
    - first 'U' (any case) → ('NEUTRAL', 0.0)
    - empty or none of Y/N/U found → None
    """
    if not isinstance(raw, str):
        return None
    # Strip markdown fences (gemini-2.5-flash wraps in ```json ... ```)
    # and any leading "json" language hint.
    s = raw.replace("```json", "").replace("```", "").strip()
    if not s:
        return None
    # Try JSON first — preferred path per v4 prompt: {"a":"Y|N|U"}.
    # Accept any value field whose first character is Y/N/U.
    try:
        obj = json.loads(s)
    except (json.JSONDecodeError, ValueError):
        obj = None
    if isinstance(obj, dict):
        # v6 strict: look for the answer in a preferred field ('a' or 'answer'),
        # else any value matching _interpret_value. Then look for 'c' digit.
        label: str | None = None
        for key in ("a", "answer", "decision", "resolution", "result"):
            if key in obj:
                label = _interpret_value(obj[key])
                if label is not None:
                    break
        if label is None:
            for v in obj.values():
                label = _interpret_value(v if isinstance(v, str) else "")
                if label is not None:
                    break
        if label is None:
            return None
        direction, default_conf = _LABEL_TO_DIRECTION[label]
        # Y/N can be paired with a confidence digit; P/U/R stay at 0.0.
        if direction in ("YES", "NO"):
            conf = _confidence_from_digit(obj.get("c"))
        else:
            conf = default_conf
        return direction, conf, label
    # Fallback: bare-string output without JSON envelope. Must be an
    # exact single-letter or exact full word — no scanning.
    label = _interpret_value(s)
    if label is None:
        return None
    direction, conf = _LABEL_TO_DIRECTION[label]
    return direction, conf, label


def _compose_relatedness_prompt(
    *,
    article: dict,
    market: dict,
    max_article_chars: int,
) -> str:
    """Build the user-message body for `relatedness()`. Kept tiny —
    headline + truncated body + market question + truncated market
    description + deadline. Capped at ~`max_article_chars` chars of
    article body to keep token cost predictable."""
    title = (article.get("title") or "").strip()
    body = (article.get("body") or "").strip()
    if len(body) > max_article_chars:
        body = body[:max_article_chars].rstrip() + " […truncated]"
    question = (market.get("question") or "").strip()
    description = (market.get("description") or "").strip()
    if len(description) > 400:
        description = description[:400].rstrip() + " […truncated]"
    end_date = market.get("end_date") or "n/a"
    return (
        f"News headline: {title}\n"
        f"News body: {body}\n"
        f"---\n"
        f"Market question: {question}\n"
        f"Market description: {description}\n"
        f"Market deadline: {end_date}\n"
    )


def _parse_relatedness_response(
    raw: str,
) -> Optional[tuple[bool, float, str]]:
    """Parse a relatedness LLM response into `(related, confidence,
    reason)` or return `None` on parse failure. Tolerant of:
      - markdown fences (```json ... ```)
      - extra fields the model emits
      - boolean as 'true'/'false', 0/1, or 'related'/'unrelated'
      - missing confidence (defaults to 0.5 on related=True, 0.0 otherwise)
      - missing reason (defaults to '')
    """
    if not isinstance(raw, str):
        return None
    s = raw.replace("```json", "").replace("```", "").strip()
    if not s:
        return None
    try:
        obj = json.loads(s)
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(obj, dict):
        return None
    rel_raw = obj.get("related")
    if rel_raw is None:
        # Some providers omit the key; treat absence as parse-failure.
        return None
    if isinstance(rel_raw, bool):
        related = rel_raw
    elif isinstance(rel_raw, (int, float)):
        related = bool(rel_raw)
    elif isinstance(rel_raw, str):
        t = rel_raw.strip().lower()
        if t in ("true", "yes", "related", "1"):
            related = True
        elif t in ("false", "no", "unrelated", "0"):
            related = False
        else:
            return None
    else:
        return None
    conf_raw = obj.get("confidence")
    try:
        confidence = float(conf_raw) if conf_raw is not None else (
            0.5 if related else 0.0
        )
    except (TypeError, ValueError):
        confidence = 0.5 if related else 0.0
    confidence = max(0.0, min(1.0, confidence))
    reason_raw = obj.get("reason") or ""
    if not isinstance(reason_raw, str):
        reason_raw = ""
    reason = reason_raw.strip()
    if len(reason) > 300:
        reason = reason[:300]
    return related, confidence, reason


class AIClient:
    # v3-fences-tolerant: max_tokens raised to 8, parser strips markdown
    # fences before scanning for Y/N/U, system prompt broadened from
    # "strictly entails" to "strongly supports". Bumping the version
    # invalidates the llm_calls cache so the 47%-parse-failure rows from
    # v2-fast don't poison the new regime.
    PROMPT_VERSION: str = "v6-strict"

    def __init__(self, *, db: DB, budget: Budget, config: Config) -> None:
        self._db = db
        self._budget = budget
        self._config = config
        self._model = config.openrouter.direction_model
        self._api_key = config.openrouter.api_key
        self._timeout = config.openrouter.timeout_seconds
        # Cycle 24 (handoff H — LLM context richness): high-level prompt
        # truncation guard. The trade-stage `compose_direction_prompt`
        # already caps the article slice at ~3000 chars; this is the
        # safety net for any future caller that builds an unbounded
        # prompt. Read defensively for test fakes that only set the
        # legacy three OpenRouter fields. Token budget → char budget
        # via the standard ~4 chars/token estimate.
        self._max_input_tokens: int = int(
            getattr(config.openrouter, "max_input_tokens", 4000)
        )
        self._max_input_chars: int = self._max_input_tokens * 4
        # One-shot WARN per process so a runaway prompt is loud without
        # spamming the log on every signal.
        self._truncate_warn_emitted: bool = False
        # The client is created lazily so tests can patch httpx without
        # paying connection costs during construction.
        self._client: Optional[httpx.AsyncClient] = None
        # Cycle 14: A/B probe lane. Optional section; absent in test fakes
        # that only set `config.openrouter`. Read defensively so legacy
        # call-sites don't break.
        probe = getattr(config, "probe", None)
        self._probe_enabled: bool = bool(getattr(probe, "probe_enabled", False))
        self._probe_sample_rate: float = float(
            getattr(probe, "probe_sample_rate", 0.0)
        )
        self._probe_providers: list[str] = list(
            getattr(probe, "probe_providers", []) or []
        )
        self._probe_timeout: float = float(
            getattr(probe, "probe_timeout_seconds", 10.0)
        )
        # Background probe tasks (fire-and-forget). We keep references so
        # `close()` can drain them and they aren't GC'd mid-flight; per
        # PEP 458 / asyncio docs, dropping a `Task` reference can cancel it.
        self._probe_tasks: set[asyncio.Task[None]] = set()

    async def close(self) -> None:
        # Drain any outstanding fire-and-forget probe tasks before closing
        # the HTTP client. Probes are wrapped to never raise, so this is
        # bounded by the per-provider timeout.
        if self._probe_tasks:
            pending = list(self._probe_tasks)
            await asyncio.gather(*pending, return_exceptions=True)
            self._probe_tasks.clear()
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def direction(
        self,
        *,
        signal_id: Optional[int],
        prompt: str,
        is_tracer: bool = False,
        tracer_id: Optional[str] = None,
    ) -> DirectionResult:
        """Call the direction model; `signal_id=None` is legal for the
        DKG trade path which has no signal backing it. With a NULL
        signal_id the per-signal predictions UNIQUE no longer applies,
        so every DKG call inserts a fresh `predictions` row — but the
        `llm_calls` cache (prompt_sha) still suppresses the actual HTTP
        round-trip when the prompt repeats."""
        cycle_id = current_cycle_id()
        # `prompt` here is just the user content (Condition + News). The
        # system message is appended to the cache key so a future change
        # to it forces a fresh round-trip even at the same PROMPT_VERSION.
        # Cycle 24 (handoff H): provider-level safety truncation. The
        # callers in `compose_direction_prompt` already cap the article
        # slice well below this threshold; this only fires if a future
        # call-site builds a runaway prompt. Truncate BEFORE hashing so
        # the cache key is stable.
        if len(prompt) > self._max_input_chars:
            if not self._truncate_warn_emitted:
                logger.warning(
                    "ai.direction: prompt %d chars exceeds max_input_chars "
                    "%d (max_input_tokens=%d); truncating. This warning "
                    "fires once per process — investigate the call-site.",
                    len(prompt), self._max_input_chars,
                    self._max_input_tokens,
                )
                self._truncate_warn_emitted = True
            prompt = prompt[: self._max_input_chars] + " […truncated]"
        full_prompt = prompt
        prompt_sha = _sha1(
            f"{_SYSTEM_MESSAGE}|{full_prompt}|{self._model}|{self.PROMPT_VERSION}"
        )

        if self._budget.is_paused():
            result = await self._neutral_with_failure(
                signal_id=signal_id,
                prompt_sha=prompt_sha,
                reason="budget_paused",
                raw_response="",
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            self._maybe_fire_probe(
                signal_id=signal_id, prediction_id=result.prediction_id,
                full_prompt=full_prompt,
            )
            return result

        cached_row = await self._lookup_cache(prompt_sha)
        if cached_row is not None:
            result = await self._record_from_cache(
                signal_id=signal_id,
                cached_row=cached_row,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            self._maybe_fire_probe(
                signal_id=signal_id, prediction_id=result.prediction_id,
                full_prompt=full_prompt,
            )
            return result

        response = await self._call_openrouter(full_prompt)
        result = await self._record_fresh_call(
            signal_id=signal_id,
            prompt_sha=prompt_sha,
            full_prompt=full_prompt,
            response=response,
            cycle_id=cycle_id,
            is_tracer=is_tracer,
            tracer_id=tracer_id,
        )
        self._maybe_fire_probe(
            signal_id=signal_id, prediction_id=result.prediction_id,
            full_prompt=full_prompt,
        )
        return result

    # ------------------------------------------------------------------
    # Noisy-explorer relatedness (cycle 25)
    # ------------------------------------------------------------------

    async def relatedness(
        self,
        *,
        article: dict,
        market: dict,
        max_article_chars: int = 800,
        max_output_tokens: int = 80,
        model: str = "google/gemini-2.5-flash",
        is_tracer: bool = False,
        tracer_id: Optional[str] = None,
    ) -> RelatednessResult:
        """Cheap binary relevance classifier on (article ↔ market).

        Always returns a RelatednessResult; never raises. On budget
        pause, network error, non-2xx response, or parse error,
        returns `related=False` (conservative — drops the exploration
        rather than capturing odds for an uncertain pair).

        Caches via `llm_calls.prompt_sha` keyed on
        `sha1(system | prompt | model | RELATEDNESS_PROMPT_VERSION)`
        — repeated calls on the same (article, market) pair skip the
        HTTP round-trip.
        """
        full_prompt = _compose_relatedness_prompt(
            article=article,
            market=market,
            max_article_chars=max_article_chars,
        )
        prompt_sha = _sha1(
            f"{_RELATEDNESS_SYSTEM_MESSAGE}|{full_prompt}|{model}|"
            f"{RELATEDNESS_PROMPT_VERSION}"
        )

        if self._budget.is_paused():
            return RelatednessResult(
                related=False,
                confidence=0.0,
                reason="budget_paused",
                prompt_hash=prompt_sha,
                cached=False,
                raw_response="",
            )

        cached_row = await self._lookup_cache(prompt_sha)
        if cached_row is not None:
            content = _extract_content(cached_row["response_body"])
            parsed = _parse_relatedness_response(content) if content else None
            if parsed is None:
                return RelatednessResult(
                    related=False,
                    confidence=0.0,
                    reason="cache_reparse_error",
                    prompt_hash=prompt_sha,
                    cached=True,
                    raw_response=content or "",
                )
            related, confidence, reason = parsed
            return RelatednessResult(
                related=related,
                confidence=confidence,
                reason=reason,
                prompt_hash=prompt_sha,
                cached=True,
                raw_response=content or "",
            )

        response = await self._call_openrouter_relatedness(
            full_prompt=full_prompt,
            model=model,
            max_tokens=max_output_tokens,
        )
        await self._insert_relatedness_llm_call(
            prompt_sha=prompt_sha,
            full_prompt=full_prompt,
            response=response,
            model=model,
            is_tracer=is_tracer,
            tracer_id=tracer_id,
        )
        status = response["status_code"]
        if status == 402:
            self._budget.note_402()
            return RelatednessResult(
                related=False,
                confidence=0.0,
                reason="402_credits",
                prompt_hash=prompt_sha,
                cached=False,
                raw_response=response["body"],
            )
        if status < 200 or status >= 300:
            return RelatednessResult(
                related=False,
                confidence=0.0,
                reason=response["error"] or f"http_status_{status}",
                prompt_hash=prompt_sha,
                cached=False,
                raw_response=response["body"],
            )
        content = _extract_content(response["body"])
        parsed = _parse_relatedness_response(content) if content else None
        if parsed is None:
            return RelatednessResult(
                related=False,
                confidence=0.0,
                reason="parse_error",
                prompt_hash=prompt_sha,
                cached=False,
                raw_response=response["body"],
            )
        self._budget.note_ok()
        related, confidence, reason = parsed
        return RelatednessResult(
            related=related,
            confidence=confidence,
            reason=reason,
            prompt_hash=prompt_sha,
            cached=False,
            raw_response=content,
        )

    async def _call_openrouter_relatedness(
        self,
        *,
        full_prompt: str,
        model: str,
        max_tokens: int,
    ) -> dict:
        """Mirror of `_call_openrouter` but parameterized on model +
        max_tokens and without logit_bias (the relatedness output is
        JSON, not a single-character label)."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self._timeout)
        t0 = time.monotonic()
        try:
            resp = await self._client.post(
                _OPENROUTER_URL,
                headers={"Authorization": f"Bearer {self._api_key}"},
                json={
                    "model": model,
                    "messages": [
                        {"role": "system",
                         "content": _RELATEDNESS_SYSTEM_MESSAGE},
                        {"role": "user", "content": full_prompt},
                    ],
                    "max_tokens": max_tokens,
                    "temperature": 0,
                },
            )
        except httpx.HTTPError as exc:
            return {
                "status_code": 0,
                "body": "",
                "latency_ms": (time.monotonic() - t0) * 1000.0,
                "tokens_in": None,
                "tokens_out": None,
                "error": f"http_error: {exc.__class__.__name__}",
            }
        latency_ms = (time.monotonic() - t0) * 1000.0
        body = resp.text
        try:
            parsed = resp.json()
        except json.JSONDecodeError:
            parsed = {}
        usage = parsed.get("usage") or {}
        return {
            "status_code": resp.status_code,
            "body": body,
            "latency_ms": latency_ms,
            "tokens_in": usage.get("prompt_tokens"),
            "tokens_out": usage.get("completion_tokens"),
            "error": None,
        }

    async def _insert_relatedness_llm_call(
        self,
        *,
        prompt_sha: str,
        full_prompt: str,
        response: dict,
        model: str,
        is_tracer: bool,
        tracer_id: Optional[str],
    ) -> None:
        """Write the relatedness call to `llm_calls`. Same table as
        `direction()` — disambiguated by `model` + `prompt_version`
        columns. ON CONFLICT(prompt_sha) DO NOTHING preserves the
        cache invariant when two concurrent calls compose the same
        prompt."""
        async with self._db.batch() as cur:
            await cur.execute(
                """
                INSERT INTO llm_calls
                    (prompt_sha, model, prompt_version, prompt, response_body,
                     status_code, latency_ms, tokens_in, tokens_out,
                     is_tracer, tracer_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(prompt_sha) DO NOTHING
                """,
                (
                    prompt_sha,
                    model,
                    RELATEDNESS_PROMPT_VERSION,
                    full_prompt,
                    response["body"],
                    response["status_code"],
                    response["latency_ms"],
                    response["tokens_in"],
                    response["tokens_out"],
                    1 if is_tracer else 0,
                    tracer_id,
                    _now_iso(),
                ),
            )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _lookup_cache(self, prompt_sha: str) -> Optional[dict]:
        cur = await self._db.conn.execute(
            "SELECT id, response_body, status_code FROM llm_calls WHERE prompt_sha=?",
            (prompt_sha,),
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return None
        return {"id": row["id"], "response_body": row["response_body"], "status_code": row["status_code"]}

    async def _call_openrouter(self, full_prompt: str) -> dict:
        """Returns {status_code, body, latency_ms, tokens_in, tokens_out, error}.

        Catches all network errors and returns a synthetic dict with
        `status_code=0` and `error` populated rather than raising. This
        keeps the caller's transaction shape uniform.

        v2-fast: enforces `max_tokens=1`, `temperature=0`,
        `logit_bias` concentrated on `Y/N/U`. Sends the system message
        as a separate `role: system` entry so providers that cache the
        prompt prefix (Anthropic, Gemini) keep it warm across calls.
        """
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self._timeout)
        t0 = time.monotonic()
        try:
            resp = await self._client.post(
                _OPENROUTER_URL,
                headers={"Authorization": f"Bearer {self._api_key}"},
                json={
                    "model": self._model,
                    "messages": [
                        {"role": "system", "content": _SYSTEM_MESSAGE},
                        {"role": "user", "content": full_prompt},
                    ],
                    # v4: gemini-2.5-flash insists on emitting
                    # ```json\n{"answer":"..."}\n``` regardless of
                    # logit_bias / prompt rules. So we embrace it:
                    # prompt asks for {"a":"Y|N|U"}, parser strips fences
                    # and JSON-parses. 32 tokens covers ```json\n
                    # {"answer":"Y"}\n``` with headroom.
                    "max_tokens": 32,
                    "temperature": 0,
                    "logit_bias": _LOGIT_BIAS_YNU,
                },
            )
        except httpx.HTTPError as exc:
            return {
                "status_code": 0,
                "body": "",
                "latency_ms": (time.monotonic() - t0) * 1000.0,
                "tokens_in": None,
                "tokens_out": None,
                "error": f"http_error: {exc.__class__.__name__}",
            }
        latency_ms = (time.monotonic() - t0) * 1000.0
        body = resp.text
        try:
            parsed = resp.json()
        except json.JSONDecodeError:
            parsed = {}
        usage = parsed.get("usage") or {}
        return {
            "status_code": resp.status_code,
            "body": body,
            "latency_ms": latency_ms,
            "tokens_in": usage.get("prompt_tokens"),
            "tokens_out": usage.get("completion_tokens"),
            "error": None,
        }

    async def _record_from_cache(
        self,
        *,
        signal_id: Optional[int],
        cached_row: dict,
        cycle_id: int,
        is_tracer: bool,
        tracer_id: Optional[str],
    ) -> DirectionResult:
        content = _extract_content(cached_row["response_body"])
        parsed = _parse_response(content) if content else None
        if parsed is None:
            return await self._neutral_with_failure(
                signal_id=signal_id,
                prompt_sha=None,
                reason="cache_reparse_error",
                raw_response=content,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
                llm_call_id=cached_row["id"],
            )
        direction, confidence, label = parsed
        pred_id = await self._insert_prediction(
            signal_id=signal_id,
            llm_call_id=cached_row["id"],
            direction=direction,
            confidence=confidence,
            cached=True,
            raw_response=content,
            label=label,
            cycle_id=cycle_id,
            is_tracer=is_tracer,
            tracer_id=tracer_id,
        )
        return DirectionResult(
            direction=direction,
            confidence=confidence,
            prediction_id=pred_id,
            cached=True,
            raw_response=content,
            label=label,
        )

    async def _record_fresh_call(
        self,
        *,
        signal_id: Optional[int],
        prompt_sha: str,
        full_prompt: str,
        response: dict,
        cycle_id: int,
        is_tracer: bool,
        tracer_id: Optional[str],
    ) -> DirectionResult:
        status = response["status_code"]

        if status == 402:
            self._budget.note_402()
            llm_call_id = await self._insert_llm_call(
                prompt_sha=prompt_sha,
                full_prompt=full_prompt,
                response=response,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return await self._neutral_with_failure(
                signal_id=signal_id,
                prompt_sha=prompt_sha,
                reason="402_credits",
                raw_response=response["body"],
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
                llm_call_id=llm_call_id,
            )

        if status < 200 or status >= 300:
            reason = response["error"] or f"http_status_{status}"
            llm_call_id = await self._insert_llm_call(
                prompt_sha=prompt_sha,
                full_prompt=full_prompt,
                response=response,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return await self._neutral_with_failure(
                signal_id=signal_id,
                prompt_sha=prompt_sha,
                reason=reason,
                raw_response=response["body"],
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
                llm_call_id=llm_call_id,
            )

        content = _extract_content(response["body"])
        parsed = _parse_response(content) if content else None
        llm_call_id = await self._insert_llm_call(
            prompt_sha=prompt_sha,
            full_prompt=full_prompt,
            response=response,
            is_tracer=is_tracer,
            tracer_id=tracer_id,
        )
        if parsed is None:
            return await self._neutral_with_failure(
                signal_id=signal_id,
                prompt_sha=prompt_sha,
                reason="parse_error",
                raw_response=response["body"],
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
                llm_call_id=llm_call_id,
            )

        self._budget.note_ok()
        direction, confidence, label = parsed
        pred_id = await self._insert_prediction(
            signal_id=signal_id,
            llm_call_id=llm_call_id,
            direction=direction,
            confidence=confidence,
            cached=False,
            raw_response=content,
            label=label,
            cycle_id=cycle_id,
            is_tracer=is_tracer,
            tracer_id=tracer_id,
        )
        return DirectionResult(
            direction=direction,
            confidence=confidence,
            prediction_id=pred_id,
            cached=False,
            raw_response=content,
            label=label,
        )

    async def _insert_llm_call(
        self,
        *,
        prompt_sha: str,
        full_prompt: str,
        response: dict,
        is_tracer: bool,
        tracer_id: Optional[str],
    ) -> int:
        async with self._db.batch() as cur:
            await cur.execute(
                """
                INSERT INTO llm_calls
                    (prompt_sha, model, prompt_version, prompt, response_body,
                     status_code, latency_ms, tokens_in, tokens_out,
                     is_tracer, tracer_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(prompt_sha) DO NOTHING
                """,
                (
                    prompt_sha,
                    self._model,
                    self.PROMPT_VERSION,
                    full_prompt,
                    response["body"],
                    response["status_code"],
                    response["latency_ms"],
                    response["tokens_in"],
                    response["tokens_out"],
                    1 if is_tracer else 0,
                    tracer_id,
                    _now_iso(),
                ),
            )
            await cur.execute(
                "SELECT id FROM llm_calls WHERE prompt_sha=?",
                (prompt_sha,),
            )
            row = await cur.fetchone()
        if row is None:
            raise RuntimeError(f"llm_calls insert failed for prompt_sha={prompt_sha}")
        return int(row[0])

    async def _insert_prediction(
        self,
        *,
        signal_id: Optional[int],
        llm_call_id: Optional[int],
        direction: str,
        confidence: float,
        cached: bool,
        raw_response: str,
        label: str,
        cycle_id: int,
        is_tracer: bool,
        tracer_id: Optional[str],
    ) -> int:
        # MVP path (signal_id != None): use the partial unique index
        # uq_predictions_signal_model_version for per-signal idempotency,
        # then look the row up by (signal_id, model, prompt_version).
        # DKG path (signal_id IS None): the partial unique no longer
        # applies, so every call inserts a fresh row and we return
        # `lastrowid` directly. Cost of an extra `predictions` row per
        # repeat DKG call is acceptable; the `llm_calls` cache still
        # suppresses the actual HTTP round-trip.
        async with self._db.batch() as cur:
            if signal_id is None:
                await cur.execute(
                    """
                    INSERT INTO predictions
                        (signal_id, llm_call_id, model, prompt_version,
                         direction, confidence, cached, raw_response, label,
                         cycle_id, is_tracer, tracer_id, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        signal_id,
                        llm_call_id,
                        self._model,
                        self.PROMPT_VERSION,
                        direction,
                        confidence,
                        1 if cached else 0,
                        raw_response,
                        label,
                        cycle_id,
                        1 if is_tracer else 0,
                        tracer_id,
                        _now_iso(),
                    ),
                )
                rid = cur.lastrowid
                if rid is None:
                    raise RuntimeError(
                        "predictions insert failed for DKG path (signal_id=None)"
                    )
                return int(rid)
            await cur.execute(
                """
                INSERT INTO predictions
                    (signal_id, llm_call_id, model, prompt_version,
                     direction, confidence, cached, raw_response, label, cycle_id,
                     is_tracer, tracer_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(signal_id, model, prompt_version)
                    WHERE signal_id IS NOT NULL
                    DO NOTHING
                """,
                (
                    signal_id,
                    llm_call_id,
                    self._model,
                    self.PROMPT_VERSION,
                    direction,
                    confidence,
                    1 if cached else 0,
                    raw_response,
                    label,
                    cycle_id,
                    1 if is_tracer else 0,
                    tracer_id,
                    _now_iso(),
                ),
            )
            await cur.execute(
                "SELECT id FROM predictions WHERE signal_id=? AND model=? AND prompt_version=?",
                (signal_id, self._model, self.PROMPT_VERSION),
            )
            row = await cur.fetchone()
        if row is None:
            raise RuntimeError(f"predictions insert failed for signal_id={signal_id}")
        return int(row[0])

    async def _neutral_with_failure(
        self,
        *,
        signal_id: Optional[int],
        prompt_sha: Optional[str],
        reason: str,
        raw_response: str,
        cycle_id: int,
        is_tracer: bool,
        tracer_id: Optional[str],
        llm_call_id: Optional[int] = None,
    ) -> DirectionResult:
        async with self._db.batch() as cur:
            await cur.execute(
                """
                INSERT INTO llm_parse_failures
                    (signal_id, model, prompt_sha, reason, raw_response,
                     cycle_id, is_tracer, tracer_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal_id,
                    self._model,
                    prompt_sha,
                    reason,
                    raw_response,
                    cycle_id,
                    1 if is_tracer else 0,
                    tracer_id,
                    _now_iso(),
                ),
            )
        pred_id = await self._insert_prediction(
            signal_id=signal_id,
            llm_call_id=llm_call_id,
            direction="NEUTRAL",
            confidence=0.0,
            cached=False,
            raw_response=raw_response,
            label="?",                # '?' marks unparseable / failure / 402 / cache-reparse-err
            cycle_id=cycle_id,
            is_tracer=is_tracer,
            tracer_id=tracer_id,
        )
        return DirectionResult(
            direction="NEUTRAL",
            confidence=0.0,
            prediction_id=pred_id,
            cached=False,
            raw_response=raw_response,
            label="?",
        )

    # ------------------------------------------------------------------
    # A/B probe lane (cycle 14)
    # ------------------------------------------------------------------

    def _maybe_fire_probe(
        self,
        *,
        signal_id: Optional[int],
        prediction_id: int,
        full_prompt: str,
    ) -> None:
        """Sampled fire-and-forget shadow probes. NEVER blocks the caller.

        Gate cascade:
        1. Master switch (`probe_enabled`) — default False.
        2. Non-empty `probe_providers`.
        3. `random() < probe_sample_rate`.

        On pass, schedules ONE probe task per provider via
        `asyncio.create_task` and returns immediately. The production
        `direction()` return value and timing are unchanged.
        """
        if not self._probe_enabled:
            return
        if not self._probe_providers:
            return
        if random.random() >= self._probe_sample_rate:
            return
        prompt_hash = _sha256(full_prompt)
        for provider in self._probe_providers:
            task = asyncio.create_task(
                self._run_probe(
                    signal_id=signal_id,
                    prediction_id=prediction_id,
                    provider=provider,
                    full_prompt=full_prompt,
                    prompt_hash=prompt_hash,
                )
            )
            self._probe_tasks.add(task)
            task.add_done_callback(self._probe_tasks.discard)

    async def _run_probe(
        self,
        *,
        signal_id: Optional[int],
        prediction_id: int,
        provider: str,
        full_prompt: str,
        prompt_hash: str,
    ) -> None:
        """Run one shadow call. NEVER raises out — failures land in the
        `error` column. TTFT measured via streaming when supported."""
        try:
            probe = await self._call_probe_provider(provider, full_prompt)
        except Exception as exc:  # noqa: BLE001 — defence-in-depth; the
            # inner call already catches everything we know about, but a
            # task that raises would only log to the asyncio default
            # handler and we'd lose the row.
            probe = {
                "ttft_ms": None,
                "total_ms": None,
                "direction": None,
                "confidence": None,
                "raw_response": None,
                "error": f"probe_unexpected: {exc.__class__.__name__}",
            }
        try:
            await self._insert_probe_row(
                signal_id=signal_id,
                prediction_id=prediction_id,
                prompt_hash=prompt_hash,
                provider=provider,
                model=provider,
                probe=probe,
            )
        except Exception as exc:  # noqa: BLE001 — DB write failure on the
            # probe path is non-fatal; log and move on.
            logger.warning(
                "probe insert failed for provider=%s: %s", provider, exc
            )

    async def _call_probe_provider(
        self, provider: str, full_prompt: str
    ) -> dict:
        """Single streaming call to an OpenRouter-hosted challenger.

        Returns a dict with `ttft_ms`, `total_ms`, `direction`,
        `confidence`, `raw_response`, `error`. NEVER raises — all errors
        (timeout, HTTP, parse) are converted into populated `error` field.
        """
        if self._client is None:
            # Reuse the main httpx client; its timeout is set to the
            # main-call timeout, but we ALSO apply a per-request override
            # via the probe timeout below.
            self._client = httpx.AsyncClient(timeout=self._timeout)
        t0 = time.monotonic()
        ttft_ms: Optional[float] = None
        chunks: list[str] = []
        payload = {
            "model": provider,
            "messages": [
                {"role": "system", "content": _SYSTEM_MESSAGE},
                {"role": "user", "content": full_prompt},
            ],
            "max_tokens": 32,
            "temperature": 0,
            "logit_bias": _LOGIT_BIAS_YNU,
            "stream": True,
        }
        headers = {"Authorization": f"Bearer {self._api_key}"}
        try:
            async with self._client.stream(
                "POST",
                _OPENROUTER_URL,
                headers=headers,
                json=payload,
                timeout=self._probe_timeout,
            ) as resp:
                if resp.status_code == 429:
                    return _probe_error("rate_limit", t0)
                if resp.status_code < 200 or resp.status_code >= 300:
                    return _probe_error(
                        f"http_status_{resp.status_code}", t0
                    )
                async for line in resp.aiter_lines():
                    if not line:
                        continue
                    # OpenRouter SSE — lines prefixed with "data: " carry
                    # JSON deltas; "data: [DONE]" terminates the stream.
                    if not line.startswith("data:"):
                        continue
                    payload_str = line[len("data:"):].strip()
                    if not payload_str or payload_str == "[DONE]":
                        continue
                    if ttft_ms is None:
                        ttft_ms = (time.monotonic() - t0) * 1000.0
                    try:
                        chunk = json.loads(payload_str)
                    except json.JSONDecodeError:
                        continue
                    delta = (
                        (chunk.get("choices") or [{}])[0]
                        .get("delta", {})
                        .get("content")
                    )
                    if delta:
                        chunks.append(delta)
        except httpx.TimeoutException:
            return _probe_error("timeout", t0)
        except httpx.HTTPError as exc:
            return _probe_error(f"http_error: {exc.__class__.__name__}", t0)
        total_ms = (time.monotonic() - t0) * 1000.0
        content = "".join(chunks)
        parsed = _parse_response(content) if content else None
        if parsed is None:
            return {
                "ttft_ms": ttft_ms,
                "total_ms": total_ms,
                "direction": None,
                "confidence": None,
                # Truncate to keep the column lean — 500 chars per the
                # plan spec.
                "raw_response": content[:500] if content else None,
                "error": "parse_error",
            }
        direction, confidence, _label = parsed
        # Confidence: probe uses the simple v2-fast 1.0 / 0.0 convention
        # (per task brief), not the v6-strict digit mapping. Raw response
        # is preserved verbatim (first 500 chars) for downstream analysis.
        return {
            "ttft_ms": ttft_ms,
            "total_ms": total_ms,
            "direction": direction,
            "confidence": 1.0 if direction in ("YES", "NO") else 0.0,
            "raw_response": content[:500] if content else None,
            "error": None,
        }

    async def _insert_probe_row(
        self,
        *,
        signal_id: Optional[int],
        prediction_id: int,
        prompt_hash: str,
        provider: str,
        model: str,
        probe: dict,
    ) -> None:
        async with self._db.batch() as cur:
            await cur.execute(
                """
                INSERT INTO llm_probe_calls
                    (signal_id, prediction_id, prompt_hash, provider, model,
                     ttft_ms, total_ms, direction, confidence, raw_response,
                     error, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal_id,
                    prediction_id,
                    prompt_hash,
                    provider,
                    model,
                    probe.get("ttft_ms"),
                    probe.get("total_ms"),
                    probe.get("direction"),
                    probe.get("confidence"),
                    probe.get("raw_response"),
                    probe.get("error"),
                    _now_iso(),
                ),
            )


def _probe_error(reason: str, t0: float) -> dict:
    """Synthesise a failure dict for `_call_probe_provider`. All metric
    fields NULL; `error` populated and `total_ms` records how long we
    waited before giving up (handy for timeout vs early-fail distinction).
    """
    return {
        "ttft_ms": None,
        "total_ms": (time.monotonic() - t0) * 1000.0,
        "direction": None,
        "confidence": None,
        "raw_response": None,
        "error": reason,
    }


def _extract_content(response_body: str) -> str:
    """Pull the assistant content out of an OpenRouter chat-completion response.

    Returns "" if the body is empty or the expected fields are missing.
    A separate parse step (`_parse_response`) handles whether the content
    matches our JSON-suffix contract.
    """
    if not response_body:
        return ""
    try:
        obj = json.loads(response_body)
    except json.JSONDecodeError:
        return ""
    choices = obj.get("choices") or []
    if not choices:
        return ""
    msg = choices[0].get("message") or {}
    return msg.get("content") or ""
