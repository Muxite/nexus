"""
## Specification

**Purpose.** Production DKG update pipeline. Reads articles from
`irene.articles`, runs the locked v3 extraction prompt (per
`reports/2026-05-23-irene-dkg-prompt-tuning.md`) against an OpenRouter
model, SBERT-dedupes the extracted entities against irene's existing
`entities` table, and writes `entities`, `entity_aliases`, `facts`,
and `extraction_runs` rows directly into irene's production tables.

This is the operational successor to `scripts/migrate/dkg_bulk_sample.py`,
which staged LLM output in an isolated DB. With v3 + SBERT both
locked, we cut out the staging hop: extraction writes straight into
production, idempotent via the `extraction_runs` UNIQUE constraint.

**Public interface.**
- CLI: `python3 -m scripts.dkg.extract_to_irene
        --irene-db <path>
        [--source-filter <SQL WHERE clause>]
        [--limit N] [--concurrency C] [--budget-cap-usd $]
        [--model <model>] [--sample-rate <0..1>]`
- `run_inproc(irene_db, *, model, source_filter, limit, concurrency,
  budget_cap_usd, sample_rate) -> dict`

**Invariants.**
- Reads only `articles` and (via `find_or_create_entity`) `entities`.
  Writes `entities`, `entity_aliases`, `facts`, `extraction_runs`.
- Idempotent on `(article_id, model, prompt_version, ontology_version)`
  — re-runs are no-ops over already-processed articles.
- Every fact carries the cycle of the article it was extracted from.
  We do NOT create a new cycle for the extraction run.
- Each LLM call respects the budget cap; the worker aborts cleanly
  after 5 consecutive 402 responses (same pattern as the sandbox
  bulk-sample script).
- SBERT entity dedup runs at threshold 0.85 (default), partitioned by
  entity_type. Surface forms that merge to a differing canonical land
  as `entity_aliases` rows.
- Latency is sampled at `sample_rate` via `SampledTimer` so the
  decision loop isn't measurably affected by always-on timing.

**Non-goals.**
- Doesn't run the resolver (entity_resolutions). That's the live
  `ResolveStage` or a separate backfill script.
- Doesn't compute embeddings for articles. Article-text embeddings
  are owned by `match.py` and aren't on the DKG critical path here.
- Doesn't trigger any bus topic. This is a CLI script, not a stage.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sqlite3
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import httpx

# Register all owner DDLs by importing schema-bearing modules.
import app.main  # noqa: F401
import app.stages.dedupe  # noqa: F401
import app.stages.eventize  # noqa: F401 — registers events/event_edges DDL
import app.stages.extract  # noqa: F401
import app.stages.ingest  # noqa: F401
import app.stages.markets  # noqa: F401
import app.stages.match  # noqa: F401
import app.stages.resolve  # noqa: F401
import app.stages.trade  # noqa: F401
import app.ai  # noqa: F401
from app.db import DB
from app.embed import Embedder
from app.sbert_dedupe import EntityIndex, find_or_create_entity
from app.timing import (
    ProfileStore,
    SampledTimer,
    calibrate_timer_overhead,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Locked constants
# ---------------------------------------------------------------------------


_MODEL_DEFAULT = "google/gemini-2.5-flash"
_PROMPT_VERSION = "v3"
_ONTOLOGY_VERSION = "v1"
_OR_URL = "https://openrouter.ai/api/v1/chat/completions"

_ENTITY_TYPES = (
    "COMPANY", "PERSON", "ORG", "COUNTRY",
    "ECON_INDICATOR", "ASSET", "EVENT",
)
_RELATIONS = (
    "announces", "decides", "elects", "acquires", "launches",
    "accuses", "signs", "changes", "resigns", "nominates",
)

_PRICE_PER_MTOK = {
    "google/gemini-2.5-flash-lite": {"in": 0.10, "out": 0.40},
    "google/gemini-2.5-flash":      {"in": 0.30, "out": 2.50},
    "openai/gpt-4o-mini":           {"in": 0.15, "out": 0.60},
}


# The v3 system prompt is shared with scripts/migrate/dkg_bulk_sample.py.
# Duplicated here on purpose so this script stays self-contained; the
# centralised home for the locked prompt belongs in the live
# `app/stages/extract.py` refactor (step 6 of the bulk-population plan).
_SYSTEM_PROMPT = (
    "You extract structured facts from financial news headlines.\n"
    "Each fact uses the subject-relation-object pattern with typed entities.\n"
    "\n"
    "Entity types (use exactly one for subj_type and obj_type):\n"
    f"  {', '.join(_ENTITY_TYPES)}\n"
    "\n"
    "Relations (use exactly one for relation):\n"
    f"  {', '.join(_RELATIONS)}\n"
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


def _est_cost(model: str, tok_in: Optional[int], tok_out: Optional[int]) -> float:
    p = _PRICE_PER_MTOK.get(model)
    if not p or tok_in is None or tok_out is None:
        return 0.0
    return tok_in / 1_000_000 * p["in"] + tok_out / 1_000_000 * p["out"]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


@dataclass
class CallResult:
    status: str  # ok | parse_error | http_error | budget_paused | timeout | empty
    raw_response: str = ""
    parsed_facts: list[dict] = field(default_factory=list)
    latency_ms: float = 0.0
    tokens_in: Optional[int] = None
    tokens_out: Optional[int] = None
    cost_usd: float = 0.0
    error_detail: Optional[str] = None


def _extract_content(raw: str) -> Optional[str]:
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        return None
    choices = obj.get("choices") or []
    if not choices:
        return None
    msg = choices[0].get("message") or {}
    return msg.get("content")


def _normalise_facts(parsed: Any) -> Optional[list[dict]]:
    if isinstance(parsed, list):
        raw_facts = parsed
    elif isinstance(parsed, dict):
        raw_facts = None
        for k in ("facts", "fact", "extractions"):
            if k in parsed and isinstance(parsed[k], list):
                raw_facts = parsed[k]
                break
        if raw_facts is None:
            return None
    else:
        return None
    out: list[dict] = []
    for f in raw_facts:
        if not isinstance(f, dict):
            continue
        relation = f.get("relation")
        if relation not in _RELATIONS:
            continue
        subj = f.get("subj") or f.get("subject")
        subj_type = f.get("subj_type") or f.get("subject_type") or f.get("type")
        if not isinstance(subj, str) or not subj.strip():
            continue
        if subj_type not in _ENTITY_TYPES:
            continue
        obj = f.get("obj") if "obj" in f else f.get("object")
        obj_type = f.get("obj_type") or f.get("object_type")
        if obj is not None and not isinstance(obj, str):
            obj = None
        if obj and obj_type not in _ENTITY_TYPES:
            obj = None
            obj_type = None
        obj_value = f.get("obj_value")
        out.append({
            "subj": subj.strip(),
            "subj_type": subj_type,
            "relation": relation,
            "obj": obj.strip() if obj else None,
            "obj_type": obj_type,
            "obj_value": obj_value if obj_value is None or isinstance(obj_value, str) else json.dumps(obj_value),
        })
    return out


# ---------------------------------------------------------------------------
# LLM call
# ---------------------------------------------------------------------------


async def _call_openrouter(
    client: httpx.AsyncClient,
    *,
    api_key: str,
    model: str,
    title: str,
) -> CallResult:
    t0 = time.monotonic()
    try:
        resp = await client.post(
            _OR_URL,
            headers={"Authorization": f"Bearer {api_key}"},
            json={
                "model": model,
                "messages": [
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": f"Headline: {title}\nOutput:"},
                ],
                "temperature": 0,
                "max_tokens": 600,
                "response_format": {"type": "json_object"},
            },
        )
    except httpx.TimeoutException:
        return CallResult(status="timeout", latency_ms=(time.monotonic() - t0) * 1000.0,
                          error_detail="timeout")
    except httpx.HTTPError as exc:
        return CallResult(status="http_error", latency_ms=(time.monotonic() - t0) * 1000.0,
                          error_detail=f"http: {exc.__class__.__name__}")
    latency = (time.monotonic() - t0) * 1000.0
    if resp.status_code == 402:
        return CallResult(status="budget_paused", raw_response=resp.text,
                          latency_ms=latency, error_detail="402_insufficient_credits")
    if resp.status_code != 200:
        return CallResult(status="http_error", raw_response=resp.text,
                          latency_ms=latency, error_detail=f"http_status_{resp.status_code}")
    raw = resp.text
    content = _extract_content(raw)
    if not content:
        return CallResult(status="parse_error", raw_response=raw,
                          latency_ms=latency, error_detail="no_content")
    try:
        body_json = json.loads(raw)
    except json.JSONDecodeError:
        body_json = {}
    usage = body_json.get("usage") or {}
    tokens_in = usage.get("prompt_tokens")
    tokens_out = usage.get("completion_tokens")
    cost = _est_cost(model, tokens_in, tokens_out)
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        return CallResult(status="parse_error", raw_response=raw, latency_ms=latency,
                          tokens_in=tokens_in, tokens_out=tokens_out, cost_usd=cost,
                          error_detail="inner_json_decode")
    facts = _normalise_facts(parsed)
    if facts is None:
        return CallResult(status="parse_error", raw_response=raw, latency_ms=latency,
                          tokens_in=tokens_in, tokens_out=tokens_out, cost_usd=cost,
                          error_detail="shape_unrecognised")
    if not facts:
        return CallResult(status="empty", raw_response=raw, latency_ms=latency,
                          tokens_in=tokens_in, tokens_out=tokens_out, cost_usd=cost)
    return CallResult(status="ok", raw_response=raw, parsed_facts=facts,
                      latency_ms=latency, tokens_in=tokens_in, tokens_out=tokens_out,
                      cost_usd=cost)


# ---------------------------------------------------------------------------
# Article selection
# ---------------------------------------------------------------------------


def _select_articles_to_process(
    irene_conn: sqlite3.Connection,
    *,
    source_filter: Optional[str],
    model: str,
    limit: Optional[int],
) -> list[sqlite3.Row]:
    """Articles that don't yet have an extraction_runs row for our
    (model, prompt_version, ontology_version) tuple. Idempotent
    resume: re-running picks up only the unprocessed slice."""
    where = []
    if source_filter:
        where.append(f"({source_filter})")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
        SELECT a.id, a.title, a.summary, a.published_at, a.cycle_id,
               a.is_tracer, a.tracer_id
        FROM articles a
        LEFT JOIN extraction_runs er ON er.article_id = a.id
            AND er.model = ?
            AND er.prompt_version = ?
            AND er.ontology_version = ?
        {where_sql}
        {'AND' if where_sql else 'WHERE'} er.id IS NULL
        ORDER BY a.id
    """
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    cur = irene_conn.execute(sql, (model, _PROMPT_VERSION, _ONTOLOGY_VERSION))
    return cur.fetchall()


# ---------------------------------------------------------------------------
# DB writes — sync per article. Wrapped in BEGIN..COMMIT.
# ---------------------------------------------------------------------------


def _persist_extraction(
    irene_conn: sqlite3.Connection,
    *,
    embedder: Embedder,
    entity_index: Optional[EntityIndex],
    article_id: int,
    article_published_at: Optional[str],
    cycle_id: int,
    is_tracer: int,
    tracer_id: Optional[str],
    model: str,
    result: CallResult,
    sbert_timer: SampledTimer,
    db_timer: SampledTimer,
) -> int:
    """Write extraction_runs + entities + facts for one article.
    Returns the extraction_runs.id (or 0 on duplicate/skip).

    `article_published_at` is stamped on `facts.observed_at` so the
    downstream eventize time-window math is anchored to when the article
    was published, not when extraction ran. Falls back to `now` if the
    article row has no published_at (e.g. tracer rows).
    """
    now = _now_iso()
    fact_observed_at = article_published_at or now
    status = result.status
    dropped = 0
    kept_facts: list[dict] = []
    for f in result.parsed_facts:
        if f.get("subj") and f.get("relation") in _RELATIONS:
            kept_facts.append(f)
        else:
            dropped += 1
    fact_count = len(kept_facts)
    entity_set: dict[tuple[str, str], None] = {}
    for f in kept_facts:
        entity_set[(f["subj_type"], f["subj"])] = None
        if f["obj"] and f["obj_type"]:
            entity_set[(f["obj_type"], f["obj"])] = None
    entity_count = len(entity_set)

    irene_conn.execute("BEGIN")
    try:
        cur = irene_conn.execute(
            """
            INSERT OR IGNORE INTO extraction_runs
                (article_id, model, prompt_version, ontology_version, status,
                 raw_response, latency_ms, tokens_in, tokens_out,
                 fact_count, entity_count, dropped_fact_count,
                 cycle_id, is_tracer, tracer_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                article_id, model, _PROMPT_VERSION, _ONTOLOGY_VERSION, status,
                result.raw_response[:5000] if result.raw_response else None,
                result.latency_ms, result.tokens_in, result.tokens_out,
                fact_count, entity_count, dropped,
                cycle_id, is_tracer, tracer_id, now,
            ),
        )
        if cur.rowcount == 0:
            # Already processed under this version tuple — UNIQUE win.
            irene_conn.execute("ROLLBACK")
            return 0
        run_id = cur.lastrowid

        # Entities (via SBERT) — keep a surface-form → entity_id map.
        ent_id_by_surface: dict[tuple[str, str], int] = {}
        for (etype, surface) in entity_set:
            with sbert_timer:
                eid, _kind = find_or_create_entity(
                    irene_conn.cursor(),
                    entity_type=etype,
                    surface_name=surface,
                    embedder=embedder,
                    now_iso=now,
                    index=entity_index,
                )
            ent_id_by_surface[(etype, surface)] = eid

        # Facts.
        with db_timer:
            for f in kept_facts:
                subj_eid = ent_id_by_surface[(f["subj_type"], f["subj"])]
                obj_eid: Optional[int] = None
                if f["obj"] and f["obj_type"]:
                    obj_eid = ent_id_by_surface[(f["obj_type"], f["obj"])]
                irene_conn.execute(
                    """
                    INSERT INTO facts
                        (extraction_run_id, article_id, subj_entity_id,
                         relation, obj_entity_id, obj_value, confidence,
                         observed_at, evidence_span, cycle_id,
                         is_tracer, tracer_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id, article_id, subj_eid,
                        f["relation"], obj_eid, f["obj_value"],
                        1.0,  # v3 prompt doesn't emit confidence; default
                        fact_observed_at,
                        f"{f['subj']} {f['relation']} {f['obj'] or ''}".strip(),
                        cycle_id, is_tracer, tracer_id,
                    ),
                )
        irene_conn.execute("COMMIT")
        return int(run_id) if run_id is not None else 0
    except Exception:
        irene_conn.execute("ROLLBACK")
        raise


# ---------------------------------------------------------------------------
# Worker pool
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
    n_ok: int = 0
    n_empty: int = 0
    n_parse_error: int = 0
    n_http_error: int = 0
    n_timeout: int = 0
    n_budget_paused: int = 0
    n_dup_skip: int = 0


def _print_progress(state: RunState) -> None:
    elapsed = time.monotonic() - state.started_at
    rate = state.completed / max(elapsed, 0.001)
    eta_s = (state.target - state.completed) / max(rate, 0.001)
    print(
        f"[{state.completed}/{state.target}] "
        f"ok={state.n_ok} empty={state.n_empty} parse={state.n_parse_error} "
        f"http={state.n_http_error} timeout={state.n_timeout} 402={state.n_budget_paused} "
        f"dup={state.n_dup_skip} | "
        f"cost=${state.total_cost:.4f}/{state.budget_cap:.2f} "
        f"rate={rate:.1f}/s eta={eta_s/60:.1f}m",
        flush=True,
    )


async def _worker(
    *,
    client: httpx.AsyncClient,
    api_key: str,
    model: str,
    queue: asyncio.Queue,
    db_lock: asyncio.Lock,
    irene_conn: sqlite3.Connection,
    embedder: Embedder,
    entity_index: Optional[EntityIndex],
    state: RunState,
    progress_every: int,
    llm_timer: SampledTimer,
    sbert_timer: SampledTimer,
    db_timer: SampledTimer,
    e2e_timer: SampledTimer,
) -> None:
    while True:
        row = await queue.get()
        if row is None:
            queue.task_done()
            return
        if state.aborted_reason:
            queue.task_done()
            continue

        with e2e_timer:
            with llm_timer:
                result = await _call_openrouter(
                    client, api_key=api_key, model=model, title=row["title"],
                )

            # 402 streak watcher / budget cap watcher.
            if result.status == "budget_paused":
                state.consecutive_402 += 1
                if state.consecutive_402 >= 5:
                    state.aborted_reason = "402_streak"
            else:
                state.consecutive_402 = 0
            state.total_cost += result.cost_usd
            if state.total_cost > state.budget_cap and not state.aborted_reason:
                state.aborted_reason = "budget_cap_exceeded"

            attr = {
                "ok": "n_ok", "empty": "n_empty", "parse_error": "n_parse_error",
                "http_error": "n_http_error", "timeout": "n_timeout",
                "budget_paused": "n_budget_paused",
            }.get(result.status)
            if attr:
                setattr(state, attr, getattr(state, attr) + 1)

            # Even non-ok statuses get an extraction_runs row written (for
            # idempotent resume). The SBERT+facts path only fires on ok/empty.
            async with db_lock:
                run_id = _persist_extraction(
                    irene_conn,
                    embedder=embedder,
                    entity_index=entity_index,
                    article_id=int(row["id"]),
                    article_published_at=row["published_at"],
                    cycle_id=int(row["cycle_id"]),
                    is_tracer=int(row["is_tracer"] or 0),
                    tracer_id=row["tracer_id"],
                    model=model,
                    result=result,
                    sbert_timer=sbert_timer,
                    db_timer=db_timer,
                )
                if run_id == 0:
                    state.n_dup_skip += 1

        state.completed += 1
        if state.completed % progress_every == 0:
            _print_progress(state)
        queue.task_done()


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


async def _run_async(
    *,
    irene_db: str,
    model: str,
    source_filter: Optional[str],
    limit: Optional[int],
    concurrency: int,
    budget_cap_usd: float,
    sample_rate: float,
    embedder_model: str = "sentence-transformers/all-MiniLM-L6-v2",
    embedder_dim: int = 384,
) -> dict:
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY not set")

    # Bootstrap irene schema (also fires the seulgi-path assertion).
    db = DB(path=irene_db)
    await db.connect()
    await db.close()

    irene_conn = sqlite3.connect(irene_db, isolation_level=None)
    irene_conn.row_factory = sqlite3.Row
    irene_conn.execute("PRAGMA foreign_keys=ON")
    irene_conn.execute("PRAGMA journal_mode=WAL")
    irene_conn.execute("PRAGMA synchronous=NORMAL")

    try:
        articles = _select_articles_to_process(
            irene_conn,
            source_filter=source_filter,
            model=model,
            limit=limit,
        )
        if not articles:
            return {
                "model": model,
                "completed": 0,
                "skipped_already_processed": 0,
                "status_counts": {},
                "total_cost_usd": 0.0,
                "elapsed_seconds": 0.0,
                "aborted_reason": None,
                "perf_ms": {},
            }
        logger.info("extract_to_irene: %d articles pending", len(articles))

        # Calibrate the timer overhead once; share with every probe.
        overhead_ns = calibrate_timer_overhead(iters=1000)
        store = ProfileStore(window=2000)
        llm_timer = SampledTimer("llm_call", store, sample_rate=sample_rate,
                                 overhead_ns=overhead_ns)
        sbert_timer = SampledTimer("sbert_lookup", store, sample_rate=sample_rate,
                                   overhead_ns=overhead_ns)
        db_timer = SampledTimer("facts_write", store, sample_rate=sample_rate,
                                overhead_ns=overhead_ns)
        e2e_timer = SampledTimer("article_end_to_end", store, sample_rate=sample_rate,
                                 overhead_ns=overhead_ns)

        embedder = Embedder(embedder_model, embedder_dim)
        # Warm the embedder so the first SBERT lookup doesn't time the
        # one-shot model load — that would be a huge outlier in p50.
        _ = embedder.embed("warmup")

        # EntityIndex: lazy per-type in-memory matrix. Pre-warm every type
        # the LLM might emit so the first article doesn't pay the load cost
        # under the timer.
        entity_index = EntityIndex()
        for etype in (
            "COMPANY", "PERSON", "ORG", "COUNTRY",
            "ECON_INDICATOR", "ASSET", "EVENT",
        ):
            entity_index.load(irene_conn.cursor(), etype)
        logger.info(
            "extract_to_irene: entity index warmed (%s)",
            ", ".join(f"{t}={entity_index.size(t)}" for t in (
                "COMPANY", "PERSON", "ORG", "COUNTRY",
                "ECON_INDICATOR", "ASSET", "EVENT",
            )),
        )

        state = RunState(target=len(articles), budget_cap=budget_cap_usd)
        queue: asyncio.Queue = asyncio.Queue(maxsize=concurrency * 4)
        db_lock = asyncio.Lock()
        timeout = httpx.Timeout(connect=10.0, read=45.0, write=10.0, pool=10.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            workers = [
                asyncio.create_task(_worker(
                    client=client, api_key=api_key, model=model,
                    queue=queue, db_lock=db_lock, irene_conn=irene_conn,
                    embedder=embedder, entity_index=entity_index,
                    state=state,
                    progress_every=max(50, len(articles) // 100),
                    llm_timer=llm_timer, sbert_timer=sbert_timer,
                    db_timer=db_timer, e2e_timer=e2e_timer,
                ))
                for _ in range(concurrency)
            ]
            for row in articles:
                await queue.put(row)
            for _ in workers:
                await queue.put(None)
            await asyncio.gather(*workers)

        elapsed = time.monotonic() - state.started_at
        perf_summary = {op: store.percentiles(op) for op in store.ops()}

        return {
            "model": model,
            "completed": state.completed,
            "status_counts": {
                "ok": state.n_ok, "empty": state.n_empty,
                "parse_error": state.n_parse_error, "http_error": state.n_http_error,
                "timeout": state.n_timeout, "budget_paused": state.n_budget_paused,
                "duplicate_skip": state.n_dup_skip,
            },
            "total_cost_usd": round(state.total_cost, 6),
            "elapsed_seconds": round(elapsed, 2),
            "rate_per_sec": round(state.completed / max(elapsed, 0.001), 2),
            "aborted_reason": state.aborted_reason,
            "perf_ms": {
                op: {k: round(v, 3) if isinstance(v, float) else v
                     for k, v in stats.items()}
                for op, stats in perf_summary.items()
            },
            "timer_overhead_ns": overhead_ns,
            "sample_rate": sample_rate,
        }
    finally:
        irene_conn.close()


def run_inproc(
    *,
    irene_db: str,
    model: str = _MODEL_DEFAULT,
    source_filter: Optional[str] = None,
    limit: Optional[int] = None,
    concurrency: int = 15,
    budget_cap_usd: float = 5.0,
    sample_rate: float = 0.1,
) -> dict:
    return asyncio.run(_run_async(
        irene_db=irene_db,
        model=model,
        source_filter=source_filter,
        limit=limit,
        concurrency=concurrency,
        budget_cap_usd=budget_cap_usd,
        sample_rate=sample_rate,
    ))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Production DKG update: irene.articles → entities/facts.",
    )
    parser.add_argument(
        "--irene-db",
        default="/home/muk/projects/jennie/irene/data/irene.db",
    )
    parser.add_argument("--model", default=_MODEL_DEFAULT)
    parser.add_argument("--source-filter", default=None,
                        help="Extra SQL WHERE clause on `articles`, e.g. \"source='seulgi_migration'\".")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--concurrency", type=int, default=15)
    parser.add_argument("--budget-cap-usd", type=float, default=5.0)
    parser.add_argument("--sample-rate", type=float, default=0.1,
                        help="Fraction of articles to time precisely (SampledTimer).")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    try:
        summary = run_inproc(
            irene_db=args.irene_db,
            model=args.model,
            source_filter=args.source_filter,
            limit=args.limit,
            concurrency=args.concurrency,
            budget_cap_usd=args.budget_cap_usd,
            sample_rate=args.sample_rate,
        )
    except RuntimeError as exc:
        logger.error("extract_to_irene failed: %s", exc)
        return 2
    print()
    print("=== summary ===")
    print(json.dumps(summary, indent=2))
    return 0 if summary["aborted_reason"] is None else 1


if __name__ == "__main__":
    raise SystemExit(main())
