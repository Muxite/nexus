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

import hashlib
import json
import logging
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
    "You are a high-speed financial resolution engine. "
    "Task: Determine if the News strictly entails the Market Condition. "
    "Rules:\n"
    "- Output exactly ONE character.\n"
    "- Output 'Y' if the news proves the condition is TRUE.\n"
    "- Output 'N' if the news proves the condition is FALSE.\n"
    "- Output 'U' if the news is UNRELATED, AMBIGUOUS, or INCONCLUSIVE.\n"
    "- Do not output any other text, whitespace, or punctuation."
)

# Token-level forcing. `logit_bias` keys are token IDs — for chat
# providers we pass the bare characters and rely on the provider to map
# them. Providers that don't honour `logit_bias` ignore the field; the
# `max_tokens=1` + `temperature=0` constraints are universal.
_LOGIT_BIAS_YNU = {"Y": 100, "N": 100, "U": 100}


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
        cycle_id        INTEGER NOT NULL,
        is_tracer       INTEGER NOT NULL DEFAULT 0,
        tracer_id       TEXT,
        created_at      TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_predictions_signal_id ON predictions(signal_id)",
    "CREATE INDEX IF NOT EXISTS idx_predictions_cycle_id ON predictions(cycle_id)",
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
]
db_module.register_schema("app/ai.py", _AI_DDL)


@dataclass(frozen=True)
class DirectionResult:
    direction: str
    confidence: float
    prediction_id: int
    cached: bool
    raw_response: str


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_response(raw: str) -> tuple[str, float] | None:
    """Return (direction, confidence) on success, None on parse error.

    Post-v2-fast the LLM emits a single character: Y / N / U.
    - 'Y' (any case) → ('YES', 1.0)
    - 'N' (any case) → ('NO', 1.0)
    - 'U' (any case) → ('NEUTRAL', 0.0)
    Anything else (e.g. provider returned extra text) → None.

    The `confidence` field stays in the contract surface for backward
    compat with the gate cascade, but per `docs/philosophy.md` it
    represents decisiveness (binary in the new regime), not probability.
    """
    if not isinstance(raw, str):
        return None
    s = raw.strip().upper()
    if not s:
        return None
    ch = s[0]
    if ch == "Y":
        return "YES", 1.0
    if ch == "N":
        return "NO", 1.0
    if ch == "U":
        return "NEUTRAL", 0.0
    return None


class AIClient:
    # v2-fast: single-char Y/N/U output. Bump invalidates the llm_calls
    # cache so old JSON-parse rows aren't mis-interpreted by the new
    # single-char parser.
    PROMPT_VERSION: str = "v2-fast"

    def __init__(self, *, db: DB, budget: Budget, config: Config) -> None:
        self._db = db
        self._budget = budget
        self._config = config
        self._model = config.openrouter.direction_model
        self._api_key = config.openrouter.api_key
        self._timeout = config.openrouter.timeout_seconds
        # The client is created lazily so tests can patch httpx without
        # paying connection costs during construction.
        self._client: Optional[httpx.AsyncClient] = None

    async def close(self) -> None:
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
        full_prompt = prompt
        prompt_sha = _sha1(
            f"{_SYSTEM_MESSAGE}|{full_prompt}|{self._model}|{self.PROMPT_VERSION}"
        )

        if self._budget.is_paused():
            return await self._neutral_with_failure(
                signal_id=signal_id,
                prompt_sha=prompt_sha,
                reason="budget_paused",
                raw_response="",
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )

        cached_row = await self._lookup_cache(prompt_sha)
        if cached_row is not None:
            return await self._record_from_cache(
                signal_id=signal_id,
                cached_row=cached_row,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )

        response = await self._call_openrouter(full_prompt)
        return await self._record_fresh_call(
            signal_id=signal_id,
            prompt_sha=prompt_sha,
            full_prompt=full_prompt,
            response=response,
            cycle_id=cycle_id,
            is_tracer=is_tracer,
            tracer_id=tracer_id,
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
                    "max_tokens": 1,
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
        direction, confidence = parsed
        pred_id = await self._insert_prediction(
            signal_id=signal_id,
            llm_call_id=cached_row["id"],
            direction=direction,
            confidence=confidence,
            cached=True,
            raw_response=content,
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
        direction, confidence = parsed
        pred_id = await self._insert_prediction(
            signal_id=signal_id,
            llm_call_id=llm_call_id,
            direction=direction,
            confidence=confidence,
            cached=False,
            raw_response=content,
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
                         direction, confidence, cached, raw_response,
                         cycle_id, is_tracer, tracer_id, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                     direction, confidence, cached, raw_response, cycle_id,
                     is_tracer, tracer_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        )


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
