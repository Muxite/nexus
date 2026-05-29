"""OpenRouter caller with on-disk cache.

Cache key = SHA1(system | user | model | strategy_version). Mirrors the
production llm_calls schema (in irene/app/ai.py) but writes to JSON files
under irene/data/sandbox/cache/llm/ so the sandbox never touches the
production DB.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import time
from dataclasses import dataclass
from typing import Optional

import httpx

from irene.sandbox.paths import CACHE_DIR, ensure_data_dirs

_OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"


@dataclass
class CallResult:
    content: str               # extracted text from `choices[0].message.content`
    raw_body: str              # full HTTP body (or empty on error)
    status_code: int           # 0 on network error
    error: str                 # non-empty on failure
    latency_ms: float
    tokens_in: Optional[int]
    tokens_out: Optional[int]
    cached: bool
    cache_path: str            # absolute path to the cache file
    prompt_sha: str


def prompt_sha(
    *, system: str, user: str, model: str, strategy_version: str,
    temperature: float = 0.0, extra_key: str = "",
) -> str:
    blob = (
        f"{system}|{user}|{model}|{strategy_version}|"
        f"t={temperature}|x={extra_key}"
    ).encode("utf-8")
    return hashlib.sha1(blob).hexdigest()


def cache_path_for(sha: str) -> str:
    return os.path.join(CACHE_DIR, f"{sha}.json")


def get_api_key() -> str:
    for k in ("IRENE__OPENROUTER__API_KEY", "OPENROUTER_API_KEY"):
        v = os.environ.get(k)
        if v:
            return v
    raise RuntimeError(
        "OpenRouter API key not set. Export IRENE__OPENROUTER__API_KEY "
        "or OPENROUTER_API_KEY before running."
    )


def load_cached(sha: str) -> Optional[CallResult]:
    p = cache_path_for(sha)
    if not os.path.exists(p):
        return None
    with open(p) as f:
        obj = json.load(f)
    return CallResult(
        content=obj.get("content", ""),
        raw_body=obj.get("raw_body", ""),
        status_code=int(obj.get("status_code", 0)),
        error=obj.get("error", ""),
        latency_ms=float(obj.get("latency_ms", 0.0)),
        tokens_in=obj.get("tokens_in"),
        tokens_out=obj.get("tokens_out"),
        cached=True,
        cache_path=p,
        prompt_sha=sha,
    )


def save_cache(
    result: CallResult, *, system: str, user: str, model: str,
    strategy_version: str, temperature: float = 0.0, extra_key: str = "",
) -> None:
    ensure_data_dirs()
    payload = {
        "system": system,
        "user": user,
        "model": model,
        "strategy_version": strategy_version,
        "temperature": temperature,
        "extra_key": extra_key,
        "content": result.content,
        "raw_body": result.raw_body,
        "status_code": result.status_code,
        "error": result.error,
        "latency_ms": result.latency_ms,
        "tokens_in": result.tokens_in,
        "tokens_out": result.tokens_out,
        "saved_at": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    with open(result.cache_path, "w") as f:
        json.dump(payload, f, indent=2)


def call(
    *,
    system: str,
    user: str,
    model: str,
    strategy_version: str,
    max_tokens: int,
    timeout_seconds: float = 30.0,
    api_key: Optional[str] = None,
    temperature: float = 0.0,
    extra_key: str = "",
) -> CallResult:
    sha = prompt_sha(
        system=system, user=user, model=model,
        strategy_version=strategy_version,
        temperature=temperature, extra_key=extra_key,
    )
    hit = load_cached(sha)
    if hit is not None:
        return hit

    key = api_key or get_api_key()
    t0 = time.monotonic()
    try:
        resp = httpx.post(
            _OPENROUTER_URL,
            headers={"Authorization": f"Bearer {key}"},
            json={
                "model": model,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                "max_tokens": max_tokens,
                "temperature": temperature,
            },
            timeout=timeout_seconds,
        )
    except httpx.HTTPError as exc:
        result = CallResult(
            content="",
            raw_body="",
            status_code=0,
            error=f"http_error: {exc.__class__.__name__}",
            latency_ms=(time.monotonic() - t0) * 1000.0,
            tokens_in=None,
            tokens_out=None,
            cached=False,
            cache_path=cache_path_for(sha),
            prompt_sha=sha,
        )
        save_cache(result, system=system, user=user, model=model,
                   strategy_version=strategy_version,
                   temperature=temperature, extra_key=extra_key)
        return result

    latency_ms = (time.monotonic() - t0) * 1000.0
    body = resp.text
    try:
        parsed = resp.json()
    except json.JSONDecodeError:
        parsed = {}
    usage = parsed.get("usage") or {}
    content = _extract_content(parsed)
    result = CallResult(
        content=content,
        raw_body=body,
        status_code=resp.status_code,
        error="" if 200 <= resp.status_code < 300 else f"http_status_{resp.status_code}",
        latency_ms=latency_ms,
        tokens_in=usage.get("prompt_tokens"),
        tokens_out=usage.get("completion_tokens"),
        cached=False,
        cache_path=cache_path_for(sha),
        prompt_sha=sha,
    )
    save_cache(result, system=system, user=user, model=model,
               strategy_version=strategy_version,
               temperature=temperature, extra_key=extra_key)
    return result


def _extract_content(parsed: dict) -> str:
    try:
        choices = parsed.get("choices") or []
        if not choices:
            return ""
        msg = choices[0].get("message") or {}
        return msg.get("content") or ""
    except (AttributeError, IndexError, TypeError):
        return ""
