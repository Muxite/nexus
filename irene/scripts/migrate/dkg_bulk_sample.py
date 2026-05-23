"""
## Specification

**Purpose.** Take a random sample of seulgi's `signals` table, run the
locked v3 extraction prompt (per `reports/2026-05-23-irene-dkg-prompt-
tuning.md`) against the cheap model, and persist every per-call result
+ timing into a new isolated DB. This is the **data-gathering step**
for the DKG bulk-population project — we collect LLM outputs now so we
can later replay them into the live `extract.py` pipeline once SBERT
entity-dedupe is in.

**The output is decoupled from the live DKG schema on purpose.** Run
this against a separate file (`data/irene_dkg_migration.db`) — once we
add SBERT entity-dedupe to `extract.py`, we replay these rows into
the production entities/facts tables with no additional LLM cost.

**Public interface.**
- CLI: `python -m scripts.migrate.dkg_bulk_sample \
            --seulgi-db <path> --irene-db <path> \
            --target N --concurrency C \
            --budget-cap-usd $ --model <model> \
            [--seed N]`
- Requires `OPENROUTER_API_KEY` in env.
- Resumable: re-run with the same `--irene-db` and it picks up where it
  left off via `dkg_migration_runs.UNIQUE(batch_id, seulgi_signal_id)`.

**Invariants.**
- Random sampling: get all signal IDs, shuffle with a seed, take first
  N. Uniform across time, source, freshness.
- Each LLM call is bounded by an `httpx.timeout` and the
  `--budget-cap-usd` watchdog — the script aborts cleanly if either
  trips.
- 402-streak watcher: bail after 5 consecutive Insufficient-credit
  responses. Per `app/budget.py`'s pattern.
- Writes are atomic per article via `db.batch()`. Resume safety comes
  from the UNIQUE constraint.

**Non-goals.**
- Doesn't write to the live `entities`, `facts`, `entity_resolutions`
  tables. That's the SBERT replay step, scheduled for next turn.
- Doesn't fetch article bodies; works against `signals.headline` only.
- Doesn't validate ontology compliance — stores whatever the model
  emits. Replay step does validation.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import re
import sqlite3
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import httpx


# ---------------------------------------------------------------------------
# Prompt (locked v3 — copied from scripts/eval/dkg_extraction_test.py).
# Duplicated rather than imported to keep the migrator standalone — once
# we promote v3 into production extract.py, we'll centralise.
# ---------------------------------------------------------------------------

_PROMPT_V3_SYSTEM = (
    "You extract structured facts from financial news headlines.\n"
    "Each fact uses the subject-relation-object pattern with typed entities.\n"
    "\n"
    "Entity types (use exactly one for subj_type and obj_type):\n"
    "  COMPANY, PERSON, ORG, COUNTRY, ECON_INDICATOR, ASSET, EVENT\n"
    "\n"
    "Relations (use exactly one for relation):\n"
    "  announces, decides, elects, acquires, launches,\n"
    "  accuses, signs, changes, resigns, nominates\n"
    "\n"
    "Rules:\n"
    " - Use the shortest widely-recognised name (FDA not 'U.S. Food and Drug Administration').\n"
    " - object is null when there's no second entity (e.g. resigns).\n"
    " - obj_value carries the numeric/textual target for relations like\n"
    "   'changes' (e.g. 'fed funds rate -25bp') or 'launches' a product.\n"
    " - If the headline doesn't fit the ontology, emit an empty facts list.\n"
    " - Output JSON ONLY matching the schema. No prose, no markdown fence.\n"
    "\n"
    "Examples (input → output):\n"
    "INPUT: FDA rejects Pfizer's diabetes drug application\n"
    "OUTPUT: " + json.dumps({"facts": [{
        "subject": "FDA", "subj_type": "ORG",
        "relation": "decides", "object": "Pfizer",
        "obj_type": "COMPANY", "obj_value": None,
    }]}) + "\n\n"
    "INPUT: Federal Reserve cuts benchmark rate by 25 basis points\n"
    "OUTPUT: " + json.dumps({"facts": [{
        "subject": "Fed", "subj_type": "ORG",
        "relation": "changes", "object": None,
        "obj_type": None,
        "obj_value": "federal funds rate -25bp",
    }]}) + "\n\n"
    "INPUT: Apple stock falls 3% on weak iPhone sales\n"
    "OUTPUT: " + json.dumps({"facts": []})
)

_USER_TEMPLATE = "Headline: {headline}\nOutput:"
_PROMPT_VERSION = "v3"
_OR_URL = "https://openrouter.ai/api/v1/chat/completions"

# Snapshot pricing as of 2026-05-23. Used to estimate cost in-flight.
_PRICE_PER_MTOK = {
    "google/gemini-2.5-flash-lite":  {"in": 0.10, "out": 0.40},
    "google/gemini-2.5-flash":       {"in": 0.30, "out": 2.50},
    "openai/gpt-4o-mini":            {"in": 0.15, "out": 0.60},
}


def _est_cost(model: str, tokens_in: Optional[int], tokens_out: Optional[int]) -> float:
    if tokens_in is None or tokens_out is None:
        return 0.0
    p = _PRICE_PER_MTOK.get(model)
    if p is None:
        return 0.0
    return (tokens_in / 1_000_000) * p["in"] + (tokens_out / 1_000_000) * p["out"]


# ---------------------------------------------------------------------------
# Schema for the migration DB (separate from the live irene.db)
# ---------------------------------------------------------------------------

_SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS dkg_migration_batches (
        batch_id            TEXT PRIMARY KEY,
        started_at          TEXT NOT NULL,
        finished_at         TEXT,
        target_count        INTEGER NOT NULL,
        completed_count     INTEGER NOT NULL DEFAULT 0,
        total_cost_usd      REAL NOT NULL DEFAULT 0.0,
        budget_cap_usd      REAL NOT NULL,
        model               TEXT NOT NULL,
        prompt_version      TEXT NOT NULL,
        concurrency         INTEGER NOT NULL,
        sample_strategy     TEXT NOT NULL,
        random_seed         INTEGER,
        aborted_reason      TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dkg_migration_runs (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id            TEXT NOT NULL,
        seulgi_signal_id    INTEGER NOT NULL,
        seulgi_headline_id  TEXT,
        seulgi_headline     TEXT NOT NULL,
        seulgi_detected_at  TEXT,
        seulgi_site_url     TEXT,
        seulgi_freshness    TEXT,
        status              TEXT NOT NULL CHECK(status IN
                                ('ok','parse_error','http_error','budget_paused','timeout','empty')),
        raw_response        TEXT,
        parsed_facts_json   TEXT,
        fact_count          INTEGER NOT NULL DEFAULT 0,
        latency_ms          REAL,
        tokens_in           INTEGER,
        tokens_out          INTEGER,
        cost_usd            REAL,
        error_detail        TEXT,
        started_at          TEXT NOT NULL,
        finished_at         TEXT NOT NULL,
        UNIQUE(batch_id, seulgi_signal_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_dkg_migration_runs_batch ON dkg_migration_runs(batch_id)",
    "CREATE INDEX IF NOT EXISTS idx_dkg_migration_runs_status ON dkg_migration_runs(status)",
    "CREATE INDEX IF NOT EXISTS idx_dkg_migration_runs_signal_id ON dkg_migration_runs(seulgi_signal_id)",
]


# ---------------------------------------------------------------------------
# Sampling
# ---------------------------------------------------------------------------


def _sample_ids(seulgi_db: str, *, target: int, seed: Optional[int]) -> list[dict]:
    """Pull every signals row, shuffle deterministically, take the first
    `target`. Returns rows sorted by id ASC (the caller doesn't actually
    care about order — workers grab from this list in parallel)."""
    conn = sqlite3.connect(f"file:{seulgi_db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT id, headline_id, headline, detected_at, site_url, freshness_label "
            "FROM signals WHERE headline IS NOT NULL AND length(headline) > 5"
        ).fetchall()
    finally:
        conn.close()
    pool = [dict(r) for r in rows]
    rng = random.Random(seed)
    rng.shuffle(pool)
    return pool[: max(0, target)]


def _already_processed(irene_db: str, batch_id: str) -> set[int]:
    if not Path(irene_db).exists():
        return set()
    conn = sqlite3.connect(irene_db)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT seulgi_signal_id FROM dkg_migration_runs WHERE batch_id = ?",
            (batch_id,),
        ).fetchall()
    except sqlite3.OperationalError:
        return set()
    finally:
        conn.close()
    return {int(r["seulgi_signal_id"]) for r in rows}


# ---------------------------------------------------------------------------
# Parser (mirrors the test-harness shape)
# ---------------------------------------------------------------------------


_FENCE_RE = re.compile(r"^\s*```(?:json)?\s*\n?(.*?)\n?\s*```\s*$", re.DOTALL | re.IGNORECASE)


def _strip_fence(s: str) -> str:
    m = _FENCE_RE.match(s)
    return m.group(1) if m else s


def _extract_content(body: str) -> Optional[str]:
    try:
        obj = json.loads(body)
    except json.JSONDecodeError:
        return None
    choices = obj.get("choices") or []
    if not choices:
        return None
    msg = choices[0].get("message") or {}
    return msg.get("content")


def _normalise_facts(parsed: Any) -> Optional[list[dict]]:
    """Accept {"facts":[...]} or [...] or even {"fact":[...]} variants.
    Returns the normalised list or None on shape error.

    Note: empty list `[]` is valid (correct extraction = "no facts in
    this headline"). The `or` chain we used originally treated `[]` as
    falsy and fell through to the next key — fixed by checking each
    candidate explicitly.
    """
    if isinstance(parsed, list):
        facts_raw = parsed
    elif isinstance(parsed, dict):
        facts_raw = None
        for k in ("facts", "fact", "extractions"):
            if k in parsed and isinstance(parsed[k], list):
                facts_raw = parsed[k]
                break
        if facts_raw is None:
            return None
    else:
        return None
    out: list[dict] = []
    for f in facts_raw:
        if not isinstance(f, dict):
            continue
        out.append({
            "subj":      f.get("subj") or f.get("subject"),
            "subj_type": f.get("subj_type") or f.get("subject_type") or f.get("type"),
            "relation":  f.get("relation"),
            "obj":       f.get("obj") if "obj" in f else f.get("object"),
            "obj_type":  f.get("obj_type") or f.get("object_type"),
            "obj_value": f.get("obj_value"),
        })
    return out


# ---------------------------------------------------------------------------
# Per-call shape
# ---------------------------------------------------------------------------


@dataclass
class CallResult:
    status: str               # 'ok','parse_error','http_error','budget_paused','timeout','empty'
    raw_response: str = ""
    parsed_facts: list[dict] = field(default_factory=list)
    latency_ms: float = 0.0
    tokens_in: Optional[int] = None
    tokens_out: Optional[int] = None
    cost_usd: float = 0.0
    error_detail: Optional[str] = None
    http_status: Optional[int] = None


async def _call_one(
    client: httpx.AsyncClient,
    *,
    api_key: str,
    model: str,
    headline: str,
) -> CallResult:
    t0 = time.monotonic()
    try:
        resp = await client.post(
            _OR_URL,
            headers={"Authorization": f"Bearer {api_key}"},
            json={
                "model": model,
                "messages": [
                    {"role": "system", "content": _PROMPT_V3_SYSTEM},
                    {"role": "user", "content": _USER_TEMPLATE.format(headline=headline)},
                ],
                "temperature": 0,
                "max_tokens": 600,
                "response_format": {"type": "json_object"},
            },
        )
    except httpx.TimeoutException:
        return CallResult(
            status="timeout",
            latency_ms=(time.monotonic() - t0) * 1000.0,
            error_detail="timeout",
        )
    except httpx.HTTPError as exc:
        return CallResult(
            status="http_error",
            latency_ms=(time.monotonic() - t0) * 1000.0,
            error_detail=f"http: {exc.__class__.__name__}",
        )
    latency = (time.monotonic() - t0) * 1000.0
    raw = resp.text
    if resp.status_code == 402:
        return CallResult(
            status="budget_paused",
            raw_response=raw,
            latency_ms=latency,
            http_status=402,
            error_detail="402_insufficient_credits",
        )
    if resp.status_code != 200:
        return CallResult(
            status="http_error",
            raw_response=raw,
            latency_ms=latency,
            http_status=resp.status_code,
            error_detail=f"http_status_{resp.status_code}",
        )
    try:
        body = resp.json()
    except json.JSONDecodeError:
        return CallResult(
            status="parse_error",
            raw_response=raw,
            latency_ms=latency,
            http_status=200,
            error_detail="outer_non_json",
        )
    usage = body.get("usage") or {}
    tokens_in = usage.get("prompt_tokens")
    tokens_out = usage.get("completion_tokens")
    cost = _est_cost(model, tokens_in, tokens_out)
    content = _extract_content(raw) or ""
    content = _strip_fence(content)
    if not content.strip():
        return CallResult(
            status="empty",
            raw_response=raw,
            latency_ms=latency,
            tokens_in=tokens_in,
            tokens_out=tokens_out,
            cost_usd=cost,
            http_status=200,
        )
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        return CallResult(
            status="parse_error",
            raw_response=raw,
            latency_ms=latency,
            tokens_in=tokens_in,
            tokens_out=tokens_out,
            cost_usd=cost,
            http_status=200,
            error_detail="inner_json_decode",
        )
    facts = _normalise_facts(parsed)
    if facts is None:
        return CallResult(
            status="parse_error",
            raw_response=raw,
            latency_ms=latency,
            tokens_in=tokens_in,
            tokens_out=tokens_out,
            cost_usd=cost,
            http_status=200,
            error_detail="shape_unrecognised",
        )
    return CallResult(
        status="ok",
        raw_response=raw,
        parsed_facts=facts,
        latency_ms=latency,
        tokens_in=tokens_in,
        tokens_out=tokens_out,
        cost_usd=cost,
        http_status=200,
    )


# ---------------------------------------------------------------------------
# DB writers
# ---------------------------------------------------------------------------


def _open_db(path: str) -> sqlite3.Connection:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, isolation_level=None)  # autocommit; we use explicit txns
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    for stmt in _SCHEMA:
        conn.execute(stmt)
    return conn


def _record_batch_start(conn, *, batch_id, target, budget_cap, model, concurrency, seed) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO dkg_migration_batches
            (batch_id, started_at, target_count, budget_cap_usd, model,
             prompt_version, concurrency, sample_strategy, random_seed)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            batch_id,
            datetime.now(timezone.utc).isoformat(),
            target,
            budget_cap,
            model,
            _PROMPT_VERSION,
            concurrency,
            "random",
            seed,
        ),
    )


def _record_batch_finish(conn, *, batch_id, completed, total_cost, aborted_reason=None) -> None:
    conn.execute(
        """
        UPDATE dkg_migration_batches
        SET finished_at = ?, completed_count = ?, total_cost_usd = ?, aborted_reason = ?
        WHERE batch_id = ?
        """,
        (datetime.now(timezone.utc).isoformat(), completed, total_cost, aborted_reason, batch_id),
    )


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


@dataclass
class RunState:
    target: int
    budget_cap: float
    completed: int = 0
    total_cost: float = 0.0
    consecutive_402: int = 0
    aborted_reason: Optional[str] = None
    started_at: float = field(default_factory=time.monotonic)
    # Per-status counters for live progress logs
    n_ok: int = 0
    n_parse_error: int = 0
    n_http_error: int = 0
    n_timeout: int = 0
    n_empty: int = 0
    n_budget_paused: int = 0


async def _worker(
    *,
    client: httpx.AsyncClient,
    api_key: str,
    model: str,
    queue: asyncio.Queue,
    db_lock: asyncio.Lock,
    conn: sqlite3.Connection,
    batch_id: str,
    state: RunState,
    progress_every: int,
) -> None:
    while True:
        row = await queue.get()
        if row is None:
            queue.task_done()
            return
        # Abort check before issuing the call
        if state.aborted_reason:
            queue.task_done()
            continue
        started = datetime.now(timezone.utc).isoformat()
        result = await _call_one(
            client,
            api_key=api_key,
            model=model,
            headline=row["headline"],
        )
        finished = datetime.now(timezone.utc).isoformat()
        # 402 streak watcher
        if result.status == "budget_paused":
            state.consecutive_402 += 1
            if state.consecutive_402 >= 5:
                state.aborted_reason = "402_streak"
        else:
            state.consecutive_402 = 0
        # Status counter
        attr = {
            "ok": "n_ok", "parse_error": "n_parse_error",
            "http_error": "n_http_error", "timeout": "n_timeout",
            "empty": "n_empty", "budget_paused": "n_budget_paused",
        }.get(result.status)
        if attr:
            setattr(state, attr, getattr(state, attr) + 1)
        state.total_cost += result.cost_usd
        # Budget cap check
        if state.total_cost > state.budget_cap and not state.aborted_reason:
            state.aborted_reason = "budget_cap_exceeded"
        # Write row
        async with db_lock:
            try:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO dkg_migration_runs
                        (batch_id, seulgi_signal_id, seulgi_headline_id,
                         seulgi_headline, seulgi_detected_at, seulgi_site_url,
                         seulgi_freshness, status, raw_response, parsed_facts_json,
                         fact_count, latency_ms, tokens_in, tokens_out,
                         cost_usd, error_detail, started_at, finished_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        batch_id, row["id"], row.get("headline_id"),
                        row["headline"], row.get("detected_at"), row.get("site_url"),
                        row.get("freshness_label"), result.status,
                        result.raw_response[:5000] if result.raw_response else None,
                        json.dumps(result.parsed_facts) if result.parsed_facts else None,
                        len(result.parsed_facts),
                        result.latency_ms, result.tokens_in, result.tokens_out,
                        result.cost_usd, result.error_detail, started, finished,
                    ),
                )
            except Exception as exc:  # noqa: BLE001
                print(f"db write failed for signal_id={row['id']}: {exc}", file=sys.stderr)
        state.completed += 1
        if state.completed % progress_every == 0:
            _print_progress(state)
        queue.task_done()


def _print_progress(state: RunState) -> None:
    elapsed = time.monotonic() - state.started_at
    rate = state.completed / max(elapsed, 0.001)
    eta_s = (state.target - state.completed) / max(rate, 0.001)
    print(
        f"[{state.completed}/{state.target}] "
        f"ok={state.n_ok} parse={state.n_parse_error} "
        f"http={state.n_http_error} timeout={state.n_timeout} "
        f"empty={state.n_empty} 402={state.n_budget_paused} | "
        f"cost=${state.total_cost:.4f}/{state.budget_cap:.2f} "
        f"rate={rate:.1f}/s eta={eta_s/60:.1f}m",
        flush=True,
    )


async def run_inproc(
    *,
    seulgi_db: str,
    irene_db: str,
    target: int,
    concurrency: int,
    budget_cap_usd: float,
    model: str,
    seed: Optional[int],
    batch_id: Optional[str] = None,
) -> dict:
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY not set")
    batch_id = batch_id or f"batch-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:6]}"
    conn = _open_db(irene_db)
    _record_batch_start(
        conn,
        batch_id=batch_id,
        target=target,
        budget_cap=budget_cap_usd,
        model=model,
        concurrency=concurrency,
        seed=seed,
    )

    pool = _sample_ids(seulgi_db, target=target, seed=seed)
    skip = _already_processed(irene_db, batch_id)
    todo = [row for row in pool if row["id"] not in skip]
    print(f"batch {batch_id}: sampled {len(pool)} signals from seulgi.db, "
          f"{len(skip)} already processed, {len(todo)} remaining", flush=True)
    print(f"model={model} concurrency={concurrency} budget_cap=${budget_cap_usd:.2f}", flush=True)

    state = RunState(target=len(todo), budget_cap=budget_cap_usd)
    queue: asyncio.Queue = asyncio.Queue(maxsize=concurrency * 4)
    db_lock = asyncio.Lock()

    timeout = httpx.Timeout(connect=10.0, read=45.0, write=10.0, pool=10.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        workers = [
            asyncio.create_task(_worker(
                client=client, api_key=api_key, model=model,
                queue=queue, db_lock=db_lock, conn=conn,
                batch_id=batch_id, state=state,
                progress_every=max(50, len(todo) // 100),
            ))
            for _ in range(concurrency)
        ]
        for row in todo:
            await queue.put(row)
        # Sentinel per worker so they exit cleanly
        for _ in workers:
            await queue.put(None)
        await asyncio.gather(*workers)

    _record_batch_finish(
        conn,
        batch_id=batch_id,
        completed=state.completed,
        total_cost=state.total_cost,
        aborted_reason=state.aborted_reason,
    )
    elapsed = time.monotonic() - state.started_at
    summary = {
        "batch_id": batch_id,
        "target": target,
        "completed": state.completed,
        "skip_prior": len(skip),
        "elapsed_seconds": round(elapsed, 1),
        "rate_per_sec": round(state.completed / max(elapsed, 0.001), 2),
        "total_cost_usd": round(state.total_cost, 4),
        "aborted_reason": state.aborted_reason,
        "status_counts": {
            "ok": state.n_ok,
            "parse_error": state.n_parse_error,
            "http_error": state.n_http_error,
            "timeout": state.n_timeout,
            "empty": state.n_empty,
            "budget_paused": state.n_budget_paused,
        },
    }
    conn.close()
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Random-sample seulgi.signals and run DKG bulk extraction."
    )
    parser.add_argument("--seulgi-db", default="/home/muk/projects/jennie/seulgi/data/seulgi.db")
    parser.add_argument("--irene-db", default="/home/muk/projects/jennie/irene/data/irene_dkg_migration.db")
    parser.add_argument("--target", type=int, required=True,
                        help="How many headlines to sample.")
    parser.add_argument("--concurrency", type=int, default=15)
    parser.add_argument("--budget-cap-usd", type=float, default=3.0,
                        help="Abort if cumulative spend exceeds this.")
    parser.add_argument("--model", default="google/gemini-2.5-flash-lite")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--batch-id", default=None,
                        help="Reuse a prior batch_id to resume.")
    args = parser.parse_args(argv)

    try:
        summary = asyncio.run(run_inproc(
            seulgi_db=args.seulgi_db,
            irene_db=args.irene_db,
            target=args.target,
            concurrency=args.concurrency,
            budget_cap_usd=args.budget_cap_usd,
            model=args.model,
            seed=args.seed,
            batch_id=args.batch_id,
        ))
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print()
    print("=== summary ===")
    print(json.dumps(summary, indent=2))
    return 0 if summary["aborted_reason"] is None else 1


if __name__ == "__main__":
    raise SystemExit(main())
