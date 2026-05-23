"""
## Specification

**Purpose.** Subscriber on `article.deduped` that runs the P2 extraction
(shadow) layer: each deduped article is passed to an LLM with an
ontology-constrained prompt; the structured JSON response is parsed,
validated against `docs/ontology.md` v1, and persisted as `entities`,
`entity_aliases`, and `facts` rows (with one `extraction_runs` row per
article-model-prompt-ontology tuple).

**The P2 hard invariant (`docs/p2_contracts.md` §1).** The trade path
**NEVER** reads `facts`, `entities`, `entity_aliases`, or
`extraction_runs` in P2. This stage is **pure data accumulation** —
nothing here feeds the funnel. Quality is measured at the end of P2 by
a held-out human-labelled batch before any of these tables enter the
trade path (and only via the P3 eventize/link layer, never `facts`
directly).

**Public interface.**
- `ExtractStage` — standard stage surface:
  - `ExtractStage(deps: StageDeps)` — raises if `deps.budget` is None
    (every LLM call must go through the cap watcher).
  - `await start()` — opens the lazy `httpx.AsyncClient`.
  - `await stop()` — closes the client; idempotent.
  - `await handle(payload: dict)` — subscriber on `TOPIC_ARTICLE_DEDUPED`;
    payload shape per `docs/p1_contracts.md` §2.
- `ONTOLOGY_VERSION` / `PROMPT_VERSION` (class constants) — both bump
  when their respective contract changes, invalidating prior runs by
  the UNIQUE key on `extraction_runs`.

**Owned tables** (`docs/modularization.md` §4): `entities`,
`entity_aliases`, `extraction_runs`, `facts`. DDL is registered with
`db.register_schema()` at import time per the exact schemas in
`docs/p2_contracts.md` §4.

**Invariants.**
- Per `(article_id, model, prompt_version, ontology_version)` exactly
  one `extraction_runs` row ever exists (UNIQUE constraint). Re-running
  the same article with the same versions is a no-op.
- Every row owned by this stage carries `cycle_id` from
  `cycle.current_cycle_id()` (read once per `handle()`).
- `is_tracer` / `tracer_id` thread from the inbound payload onto
  `extraction_runs` and `facts`. Entities are NOT tracer-flagged —
  they're a global concept ("Pfizer" is "Pfizer" regardless of which
  bullet first surfaced it).
- All DB writes for one article share **one** `db.batch()`. If any
  validated insert raises, the entire batch (including the
  `extraction_runs` row) rolls back. Idempotency is provided by the
  UNIQUE on `extraction_runs`, not by a per-article transaction guard.
- Budget paused short-circuits BEFORE the HTTP call: a
  `extraction_runs(status='budget_paused')` row is written, no facts,
  no entities, no network I/O.
- A 2xx with parseable structured JSON: `budget.note_ok()`.
- A 402: `budget.note_402()` and `status='http_error'`.
- Read-only on `articles`. Stage NEVER touches any table owned by
  another stage.

**LLM I/O.** Direct HTTP POST to
`https://openrouter.ai/api/v1/chat/completions`. The prompt embeds the
v1 ontology vocabulary inline so the LLM stays in-vocabulary; we then
trust-but-verify at the parse layer. JSON-mode is requested via
`response_format`; providers that don't honour it still emit a JSON
body for the v1 prompt shape (validated tightly in `_parse_llm_json`).

**Side effects.** HTTP calls to OpenRouter. SQLite writes to the four
owned tables. Publishes `TOPIC_EXTRACTION_COMPLETED` (one per article,
status-agnostic — even budget_paused / http_error publish so P3 can
skip dead articles).

**Failure modes** (each surfaces as a specific `extraction_runs.status`):
- `budget_paused` — budget watcher said pause; no HTTP call made.
- `http_error` — `httpx.HTTPError` or non-2xx response (status code
  recorded in the row). 402 additionally calls `budget.note_402()`.
- `parse_error` — chat-completion shape malformed, OR the inner JSON
  doesn't parse, OR it parses but isn't the `{entities, facts}` dict
  the prompt asked for.
- `empty` — parses cleanly but yields zero surviving facts (no rows
  written; the run row records that the LLM found nothing).
- `ok` — at least one fact survived validation.

**Non-goals.**
- Cross-article entity clustering / linkage — P3 territory.
- Free-form NER — the ontology is closed; out-of-vocab types/relations
  are dropped at the parse layer (counted in `dropped_fact_count`).
- Confidence calibration — the LLM's emitted confidence is clamped to
  [0,1] and stored verbatim; calibration is a separate concern.
- Reading any of its own tables from another stage (forbidden by §1).
"""

from __future__ import annotations

import json
import logging
import math
import re
import time
from datetime import datetime, timezone
from typing import Any, Optional

import httpx

from app import db as db_module
from app.cycle import current_cycle_id
from app.deps import StageDeps
from app.topics import TOPIC_ARTICLE_DEDUPED, TOPIC_EXTRACTION_COMPLETED

logger = logging.getLogger(__name__)


_OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"


# Ontology vocab — pinned to `docs/ontology.md` v1. Module-level FROZENSETS
# are immutable; the lists themselves are the documentation contract and
# never mutate at runtime (modularization rule 5 forbids module-level
# *mutable* state, not constants).
_ENTITY_TYPES: frozenset[str] = frozenset({
    "COMPANY", "PERSON", "ORG", "COUNTRY",
    "ECON_INDICATOR", "ASSET", "EVENT",
})

_RELATIONS: frozenset[str] = frozenset({
    "announces", "decides", "elects", "acquires", "launches",
    "accuses", "signs", "changes", "resigns", "nominates",
})

_OBJ_VALUE_KINDS: frozenset[str] = frozenset({
    "percentage", "currency", "count", "date", "string",
})

_ALIAS_KINDS: frozenset[str] = frozenset({
    "synonym", "ticker", "slug", "wikidata", "other",
})


_PROMPT_TEMPLATE = """\
Extract entities and relational facts from the news article below.

You MUST stay strictly within this closed ontology. Anything outside
these vocabularies is silently dropped — emit only what fits.

ENTITY TYPES (use exactly one of these for every entity):
{entity_types}

RELATIONS (use exactly one of these for every fact):
{relations}

For each entity name, use the **shortest widely-recognised form**:
- "Apple", not "Apple Inc."
- "Pfizer", not "Pfizer Inc."
- "FDA", not "U.S. Food and Drug Administration"
- "SEC", not "U.S. Securities and Exchange Commission"
- "Fed", not "Federal Reserve"
If you're unsure whether an acronym is widely recognised, prefer the longer form.

For each fact:
- "subj" MUST be the canonical name of an entity you listed in "entities".
- "obj" is OPTIONAL; if present, it must also be the name of a listed entity.
- "obj_value" is OPTIONAL; use it for numeric / date / percentage targets
  (e.g. `changes` on an `ECON_INDICATOR`). Shape: {{"kind":"percentage"|"currency"|"count"|"date"|"string","value":<v>,"unit":<u>,"raw":<verbatim>}}.
- "confidence" is a float in [0.0, 1.0].
- "evidence_span" is a verbatim quote from the article supporting the fact.

ARTICLE TITLE:
{title}

ARTICLE BODY:
{body}

Respond with JSON ONLY (no prose, no markdown fence):
{{
  "entities": [
    {{"name": "<canonical>", "type": "<TYPE>", "aliases": ["<alias1>", ...]}}
  ],
  "facts": [
    {{
      "subj": "<canonical>",
      "relation": "<relation>",
      "obj": "<canonical or null>",
      "obj_value": <object or null>,
      "confidence": <float>,
      "evidence_span": "<verbatim quote>"
    }}
  ]
}}
"""


_EXTRACT_DDL = [
    """
    CREATE TABLE IF NOT EXISTS entities (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        entity_type     TEXT NOT NULL CHECK(entity_type IN
                            ('COMPANY','PERSON','ORG','COUNTRY',
                             'ECON_INDICATOR','ASSET','EVENT')),
        canonical_name  TEXT NOT NULL,
        external_refs   TEXT,
        status          TEXT NOT NULL DEFAULT 'active',
        embedding       BLOB,
        embedder_id     TEXT,
        first_seen_at   TEXT NOT NULL,
        last_seen_at    TEXT NOT NULL,
        UNIQUE(entity_type, canonical_name)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(entity_type)",
    "CREATE INDEX IF NOT EXISTS idx_entities_canonical ON entities(canonical_name)",
    """
    CREATE TABLE IF NOT EXISTS entity_aliases (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        entity_id   INTEGER NOT NULL REFERENCES entities(id),
        alias       TEXT NOT NULL,
        alias_kind  TEXT NOT NULL CHECK(alias_kind IN
                        ('synonym','ticker','slug','wikidata','other')),
        UNIQUE(entity_id, alias, alias_kind)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_entity_aliases_alias ON entity_aliases(alias)",
    """
    CREATE TABLE IF NOT EXISTS extraction_runs (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        article_id          INTEGER NOT NULL,
        model               TEXT NOT NULL,
        prompt_version      TEXT NOT NULL,
        ontology_version    TEXT NOT NULL,
        status              TEXT NOT NULL CHECK(status IN
                                ('ok','parse_error','budget_paused',
                                 'http_error','no_content','empty')),
        raw_response        TEXT,
        latency_ms          REAL,
        tokens_in           INTEGER,
        tokens_out          INTEGER,
        fact_count          INTEGER NOT NULL DEFAULT 0,
        entity_count        INTEGER NOT NULL DEFAULT 0,
        dropped_fact_count  INTEGER NOT NULL DEFAULT 0,
        cycle_id            INTEGER NOT NULL,
        is_tracer           INTEGER NOT NULL DEFAULT 0,
        tracer_id           TEXT,
        created_at          TEXT NOT NULL,
        UNIQUE(article_id, model, prompt_version, ontology_version)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_extraction_runs_article ON extraction_runs(article_id)",
    "CREATE INDEX IF NOT EXISTS idx_extraction_runs_status ON extraction_runs(status)",
    "CREATE INDEX IF NOT EXISTS idx_extraction_runs_cycle_id ON extraction_runs(cycle_id)",
    """
    CREATE TABLE IF NOT EXISTS facts (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        extraction_run_id   INTEGER NOT NULL REFERENCES extraction_runs(id),
        article_id          INTEGER NOT NULL,
        subj_entity_id      INTEGER NOT NULL REFERENCES entities(id),
        relation            TEXT NOT NULL,
        obj_entity_id       INTEGER REFERENCES entities(id),
        obj_value           TEXT,
        confidence          REAL NOT NULL CHECK(confidence >= 0.0 AND confidence <= 1.0),
        valid_from          TEXT,
        valid_to            TEXT,
        observed_at         TEXT NOT NULL,
        evidence_span       TEXT NOT NULL,
        is_active           INTEGER NOT NULL DEFAULT 1,
        cycle_id            INTEGER NOT NULL,
        is_tracer           INTEGER NOT NULL DEFAULT 0,
        tracer_id           TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_facts_extraction_run ON facts(extraction_run_id)",
    "CREATE INDEX IF NOT EXISTS idx_facts_subj ON facts(subj_entity_id)",
    "CREATE INDEX IF NOT EXISTS idx_facts_relation ON facts(relation, observed_at)",
    "CREATE INDEX IF NOT EXISTS idx_facts_article ON facts(article_id)",
]
db_module.register_schema("app/stages/extract.py", _EXTRACT_DDL)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_finite_number(x: Any) -> bool:
    if isinstance(x, bool):  # bool is a subclass of int; reject up-front
        return False
    if not isinstance(x, (int, float)):
        return False
    try:
        return math.isfinite(float(x))
    except (TypeError, ValueError):
        return False


def _clamp01(x: float) -> float:
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


def _normalise_obj_value(raw: Any) -> Optional[str]:
    """Encode `obj_value` as a JSON string per `docs/ontology.md` §obj_value.

    Returns None if the input is None (the column is nullable). A dict with
    a recognised `kind` round-trips as-is; anything else falls back to
    `{"kind":"string","raw":<verbatim>}` so the fact stays alive.
    """
    if raw is None:
        return None
    if isinstance(raw, str):
        # The LLM occasionally emits a bare string; wrap it per the
        # ontology's "kind=string" fallback shape.
        return json.dumps({"kind": "string", "raw": raw}, sort_keys=True)
    if isinstance(raw, dict):
        kind = raw.get("kind")
        if kind in _OBJ_VALUE_KINDS:
            return json.dumps(raw, sort_keys=True, default=str)
        return json.dumps(
            {"kind": "string", "raw": json.dumps(raw, sort_keys=True, default=str)},
            sort_keys=True,
        )
    # Numbers / lists / etc. — store verbatim under the string-fallback.
    return json.dumps({"kind": "string", "raw": str(raw)}, sort_keys=True)


def _extract_content(response_body: str) -> Optional[str]:
    """Pull the assistant content out of an OpenRouter chat-completion body.

    Returns None on any structural malformation so the caller can charge a
    `parse_error` once and move on.
    """
    if not response_body:
        return None
    try:
        obj = json.loads(response_body)
    except json.JSONDecodeError:
        return None
    if not isinstance(obj, dict):
        return None
    choices = obj.get("choices")
    if not isinstance(choices, list) or not choices:
        return None
    msg = choices[0].get("message") if isinstance(choices[0], dict) else None
    if not isinstance(msg, dict):
        return None
    content = msg.get("content")
    if not isinstance(content, str):
        return None
    return content


_FENCE_RE = re.compile(r"^\s*```(?:json)?\s*\n?(.*?)\n?\s*```\s*$", re.DOTALL | re.IGNORECASE)


def _strip_markdown_fence(s: str) -> str:
    """Some providers (notably Gemini-flash) wrap JSON in ```json ... ```
    despite the prompt asking otherwise. Strip the fence non-destructively
    if present, else return unchanged."""
    m = _FENCE_RE.match(s)
    return m.group(1) if m else s


def _parse_llm_json(content: str) -> Optional[dict]:
    """Parse the LLM's content into the `{entities, facts}` dict.

    Returns None if either the JSON is malformed or the top-level shape
    isn't a dict with list-typed `entities` and `facts` keys. The caller
    treats all three failure modes (raw JSON parse error / wrong shape /
    missing keys) as the same `parse_error` status.
    """
    try:
        obj = json.loads(_strip_markdown_fence(content))
    except json.JSONDecodeError:
        return None
    if not isinstance(obj, dict):
        return None
    ents = obj.get("entities")
    facts = obj.get("facts")
    if not isinstance(ents, list) or not isinstance(facts, list):
        return None
    return {"entities": ents, "facts": facts}


# Common legal/structural suffixes that the LLM emits but the canonical
# entity is the same with or without — "Pfizer Inc." and "Pfizer" should
# be the same row. Order matters: longer suffixes first so "Inc." doesn't
# match before ", Inc." gets a chance.
_LEGAL_SUFFIXES = (
    ", Incorporated", " Incorporated",
    ", Inc.", " Inc.", ", Inc", " Inc",
    ", Corporation", " Corporation",
    ", Corp.", " Corp.", ", Corp", " Corp",
    ", Company", " Company", ", Co.", " Co.", " & Co.",
    ", Limited", " Limited", ", Ltd.", " Ltd.", " Ltd",
    ", LLC", " LLC",
    ", plc", " plc", ", PLC", " PLC",
    ", AG", " AG",
    ", S.A.", " S.A.",
    ", N.V.", " N.V.",
    " GmbH",
    ", Holdings", " Holdings",
    ", Group", " Group",
)


def _normalise_canonical(name: str, entity_type: str) -> str:
    """Strip common legal suffixes from COMPANY names.

    Only COMPANY gets this — ORG / COUNTRY / etc. are left alone (e.g.
    "U.S. Food and Drug Administration" stays as-is; acronym resolution
    to "FDA" is a P3 entity-resolver concern, not a parse-layer one)."""
    n = name.strip()
    if entity_type != "COMPANY":
        return n
    for sfx in _LEGAL_SUFFIXES:
        if n.endswith(sfx):
            n = n[: -len(sfx)].rstrip(", .").strip()
            break
    return n


def _validate_entities(entities: list[Any]) -> dict[str, dict]:
    """Walk the LLM's `entities` list, keeping only in-ontology entries.

    Returns an as-emitted name -> {type, canonical, aliases} map. The
    `canonical` is the form we'll write to the entities table (after
    `_normalise_canonical`); `aliases` retains the raw emitted name as
    an alias when it differs from the canonical, so downstream lookups
    against the original surface form still resolve.
    """
    out: dict[str, dict] = {}
    for raw in entities:
        if not isinstance(raw, dict):
            continue
        name = raw.get("name")
        etype = raw.get("type")
        if not isinstance(name, str) or not name.strip():
            continue
        if etype not in _ENTITY_TYPES:
            continue
        name = name.strip()
        if name in out:
            continue
        canonical = _normalise_canonical(name, etype)
        aliases_raw = raw.get("aliases") or []
        aliases: list[str] = []
        if isinstance(aliases_raw, list):
            for a in aliases_raw:
                if isinstance(a, str) and a.strip():
                    aliases.append(a.strip())
        # Preserve the as-emitted form as an alias when normalisation
        # changed it; lets a downstream lookup that uses the raw form
        # still resolve.
        if canonical != name and name not in aliases:
            aliases.append(name)
        out[name] = {"type": etype, "canonical": canonical, "aliases": aliases}
    return out


def _validate_facts(
    facts: list[Any],
    entity_map: dict[str, dict],
) -> tuple[list[dict], int]:
    """Apply the §5 validation rules. Returns (kept_facts, dropped_count)."""
    kept: list[dict] = []
    dropped = 0
    for raw in facts:
        if not isinstance(raw, dict):
            dropped += 1
            continue
        relation = raw.get("relation")
        subj = raw.get("subj")
        obj = raw.get("obj")
        evidence = raw.get("evidence_span")
        conf_raw = raw.get("confidence")

        if relation not in _RELATIONS:
            dropped += 1
            continue
        if not isinstance(subj, str) or subj.strip() not in entity_map:
            dropped += 1
            continue
        if not isinstance(evidence, str) or not evidence.strip():
            dropped += 1
            continue
        if not _is_finite_number(conf_raw):
            dropped += 1
            continue

        subj_name = subj.strip()
        obj_name: Optional[str] = None
        if obj is not None:
            if not isinstance(obj, str) or obj.strip() not in entity_map:
                # obj is present but doesn't resolve — drop the fact rather
                # than degrade silently to obj=None (would change semantics).
                dropped += 1
                continue
            obj_name = obj.strip()

        confidence = _clamp01(float(conf_raw))
        kept.append({
            "subj_name": subj_name,
            "relation": relation,
            "obj_name": obj_name,
            "obj_value": _normalise_obj_value(raw.get("obj_value")),
            "confidence": confidence,
            "evidence_span": evidence.strip(),
        })
    return kept, dropped


class ExtractStage:
    ONTOLOGY_VERSION: str = "v1"
    PROMPT_VERSION: str = "v1"

    def __init__(self, deps: StageDeps) -> None:
        if deps.budget is None:
            raise RuntimeError("ExtractStage requires deps.budget")
        self._db = deps.db
        self._bus = deps.bus
        self._budget = deps.budget
        self._cfg = deps.config.extract
        self._api_key = deps.config.openrouter.api_key
        self._model = self._cfg.model
        self._timeout = self._cfg.timeout_seconds
        self._max_tokens = self._cfg.max_tokens
        self._max_chars = self._cfg.max_article_chars
        # Lazy — tests patch the HTTP method, never need a real client.
        self._client: Optional[httpx.AsyncClient] = None
        self._stopped = False

    # ---- lifecycle -----------------------------------------------------

    async def start(self) -> None:
        # The client opens lazily on first HTTP use so a stage that only
        # ever sees budget_paused payloads pays no connection cost.
        return

    async def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # ---- main entry ----------------------------------------------------

    async def handle(self, payload: dict) -> None:
        article_id_raw = payload.get("article_id")
        if article_id_raw is None:
            logger.warning("extract: payload missing article_id: %r", payload)
            return
        article_id = int(article_id_raw)
        is_tracer = bool(payload.get("is_tracer", False))
        tracer_id = payload.get("tracer_id")
        cycle_id = current_cycle_id()

        if await self._already_extracted(article_id):
            return

        article = await self._fetch_article(article_id)
        if article is None:
            logger.warning("extract: article %d not found in articles table", article_id)
            return

        if self._budget.is_paused():
            await self._persist_run_only(
                article_id=article_id,
                status="budget_paused",
                raw_response=None,
                latency_ms=None,
                tokens_in=None,
                tokens_out=None,
                fact_count=0,
                entity_count=0,
                dropped_fact_count=0,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return

        body = (article.get("summary") or "")[: self._max_chars]
        title = article.get("title") or ""
        prompt = self._compose_prompt(title=title, body=body)

        response = await self._call_openrouter(prompt)
        status_code = response["status_code"]

        if status_code == 402:
            self._budget.note_402()
            await self._persist_run_only(
                article_id=article_id,
                status="http_error",
                raw_response=response["body"],
                latency_ms=response["latency_ms"],
                tokens_in=response["tokens_in"],
                tokens_out=response["tokens_out"],
                fact_count=0,
                entity_count=0,
                dropped_fact_count=0,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return

        if status_code < 200 or status_code >= 300:
            await self._persist_run_only(
                article_id=article_id,
                status="http_error",
                raw_response=response["body"],
                latency_ms=response["latency_ms"],
                tokens_in=response["tokens_in"],
                tokens_out=response["tokens_out"],
                fact_count=0,
                entity_count=0,
                dropped_fact_count=0,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return

        content = _extract_content(response["body"])
        if content is None:
            await self._persist_run_only(
                article_id=article_id,
                status="parse_error",
                raw_response=response["body"],
                latency_ms=response["latency_ms"],
                tokens_in=response["tokens_in"],
                tokens_out=response["tokens_out"],
                fact_count=0,
                entity_count=0,
                dropped_fact_count=0,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return

        parsed = _parse_llm_json(content)
        if parsed is None:
            await self._persist_run_only(
                article_id=article_id,
                status="parse_error",
                raw_response=response["body"],
                latency_ms=response["latency_ms"],
                tokens_in=response["tokens_in"],
                tokens_out=response["tokens_out"],
                fact_count=0,
                entity_count=0,
                dropped_fact_count=0,
                cycle_id=cycle_id,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return

        self._budget.note_ok()
        entity_map = _validate_entities(parsed["entities"])
        kept_facts, dropped = _validate_facts(parsed["facts"], entity_map)

        # Trim the entity map to those referenced by at least one surviving
        # fact — unreferenced entities don't get inserted (entities only
        # exist as the subject/object of facts in P2).
        referenced: set[str] = set()
        for f in kept_facts:
            referenced.add(f["subj_name"])
            if f["obj_name"] is not None:
                referenced.add(f["obj_name"])
        entity_map = {n: e for n, e in entity_map.items() if n in referenced}

        await self._persist_run_with_data(
            article_id=article_id,
            entity_map=entity_map,
            kept_facts=kept_facts,
            dropped=dropped,
            response=response,
            cycle_id=cycle_id,
            is_tracer=is_tracer,
            tracer_id=tracer_id,
        )

    # ---- internals: DB lookups ----------------------------------------

    async def _already_extracted(self, article_id: int) -> bool:
        cur = await self._db.conn.execute(
            "SELECT 1 FROM extraction_runs WHERE article_id=? AND model=? "
            "AND prompt_version=? AND ontology_version=? LIMIT 1",
            (article_id, self._model, self.PROMPT_VERSION, self.ONTOLOGY_VERSION),
        )
        row = await cur.fetchone()
        await cur.close()
        return row is not None

    async def _fetch_article(self, article_id: int) -> Optional[dict]:
        cur = await self._db.conn.execute(
            "SELECT title, summary FROM articles WHERE id=?",
            (article_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return None
        return {"title": row["title"], "summary": row["summary"]}

    # ---- internals: prompt + HTTP --------------------------------------

    def _compose_prompt(self, *, title: str, body: str) -> str:
        return _PROMPT_TEMPLATE.format(
            entity_types=", ".join(sorted(_ENTITY_TYPES)),
            relations=", ".join(sorted(_RELATIONS)),
            title=title,
            body=body,
        )

    async def _call_openrouter(self, prompt: str) -> dict:
        """Returns {status_code, body, latency_ms, tokens_in, tokens_out, error}.

        Mirrors `ai.py`'s shape so the same test-patching idiom (mock the
        method on the instance) works here.
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
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": self._max_tokens,
                    "response_format": {"type": "json_object"},
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
        usage = parsed.get("usage") or {} if isinstance(parsed, dict) else {}
        return {
            "status_code": resp.status_code,
            "body": body,
            "latency_ms": latency_ms,
            "tokens_in": usage.get("prompt_tokens"),
            "tokens_out": usage.get("completion_tokens"),
            "error": None,
        }

    # ---- internals: persistence ----------------------------------------

    async def _persist_run_only(
        self,
        *,
        article_id: int,
        status: str,
        raw_response: Optional[str],
        latency_ms: Optional[float],
        tokens_in: Optional[int],
        tokens_out: Optional[int],
        fact_count: int,
        entity_count: int,
        dropped_fact_count: int,
        cycle_id: int,
        is_tracer: bool,
        tracer_id: Optional[str],
    ) -> None:
        """Write just the `extraction_runs` row, then publish completion.

        Used for every non-`ok` terminal status (budget_paused, http_error,
        parse_error). The `extraction_runs.id` is read back so the
        completion event can carry it; even failed runs are addressable.
        """
        async with self._db.batch() as cur:
            await cur.execute(
                """
                INSERT OR IGNORE INTO extraction_runs
                    (article_id, model, prompt_version, ontology_version, status,
                     raw_response, latency_ms, tokens_in, tokens_out,
                     fact_count, entity_count, dropped_fact_count,
                     cycle_id, is_tracer, tracer_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    article_id, self._model, self.PROMPT_VERSION,
                    self.ONTOLOGY_VERSION, status, raw_response, latency_ms,
                    tokens_in, tokens_out, fact_count, entity_count,
                    dropped_fact_count, cycle_id, 1 if is_tracer else 0,
                    tracer_id, _now_iso(),
                ),
            )
            await cur.execute(
                "SELECT id FROM extraction_runs WHERE article_id=? AND model=? "
                "AND prompt_version=? AND ontology_version=?",
                (article_id, self._model, self.PROMPT_VERSION, self.ONTOLOGY_VERSION),
            )
            row = await cur.fetchone()
        run_id = int(row[0]) if row else None
        await self._publish_completion(
            run_id=run_id,
            article_id=article_id,
            status=status,
            fact_count=fact_count,
            entity_count=entity_count,
            is_tracer=is_tracer,
            tracer_id=tracer_id,
        )

    async def _persist_run_with_data(
        self,
        *,
        article_id: int,
        entity_map: dict[str, dict],
        kept_facts: list[dict],
        dropped: int,
        response: dict,
        cycle_id: int,
        is_tracer: bool,
        tracer_id: Optional[str],
    ) -> None:
        """Insert the run, the surviving entities, and the surviving facts —
        all inside one `db.batch()` so a mid-flow error rolls everything
        back to a clean slate. The UNIQUE on `extraction_runs` is what
        makes the second handle() a no-op rather than this method's logic.
        """
        status = "ok" if kept_facts else "empty"
        fact_count = len(kept_facts)
        entity_count = len(entity_map)
        now = _now_iso()

        async with self._db.batch() as cur:
            await cur.execute(
                """
                INSERT INTO extraction_runs
                    (article_id, model, prompt_version, ontology_version, status,
                     raw_response, latency_ms, tokens_in, tokens_out,
                     fact_count, entity_count, dropped_fact_count,
                     cycle_id, is_tracer, tracer_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    article_id, self._model, self.PROMPT_VERSION,
                    self.ONTOLOGY_VERSION, status, response["body"],
                    response["latency_ms"], response["tokens_in"],
                    response["tokens_out"], fact_count, entity_count,
                    dropped, cycle_id, 1 if is_tracer else 0,
                    tracer_id, now,
                ),
            )
            run_id = cur.lastrowid

            # Resolve every kept entity to an entities.id (insert-or-fetch).
            # `name` is the LLM's surface form; `meta["canonical"]` is what
            # we actually write into entities.canonical_name (suffix-stripped
            # for COMPANY, unchanged for other types). entity_ids is keyed
            # by the surface form so fact resolution downstream still works.
            entity_ids: dict[str, int] = {}
            for name, meta in entity_map.items():
                etype = meta["type"]
                canonical = meta["canonical"]
                await cur.execute(
                    """
                    INSERT INTO entities
                        (entity_type, canonical_name, first_seen_at, last_seen_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(entity_type, canonical_name) DO UPDATE
                        SET last_seen_at=excluded.last_seen_at
                    """,
                    (etype, canonical, now, now),
                )
                await cur.execute(
                    "SELECT id FROM entities WHERE entity_type=? AND canonical_name=?",
                    (etype, canonical),
                )
                erow = await cur.fetchone()
                if erow is None:
                    # Defensive: an ON CONFLICT path that didn't yield a row
                    # means the constraint logic is wrong — fail loudly.
                    raise RuntimeError(
                        f"entities insert/lookup failed for ({etype}, {canonical})"
                    )
                eid = int(erow[0])
                entity_ids[name] = eid
                for alias in meta["aliases"]:
                    await cur.execute(
                        """
                        INSERT OR IGNORE INTO entity_aliases
                            (entity_id, alias, alias_kind)
                        VALUES (?, ?, ?)
                        """,
                        (eid, alias, "synonym"),
                    )

            for f in kept_facts:
                subj_id = entity_ids[f["subj_name"]]
                obj_id = entity_ids[f["obj_name"]] if f["obj_name"] else None
                await cur.execute(
                    """
                    INSERT INTO facts
                        (extraction_run_id, article_id, subj_entity_id,
                         relation, obj_entity_id, obj_value, confidence,
                         observed_at, evidence_span, cycle_id,
                         is_tracer, tracer_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id, article_id, subj_id, f["relation"], obj_id,
                        f["obj_value"], f["confidence"], now,
                        f["evidence_span"], cycle_id,
                        1 if is_tracer else 0, tracer_id,
                    ),
                )

        await self._publish_completion(
            run_id=run_id,
            article_id=article_id,
            status=status,
            fact_count=fact_count,
            entity_count=entity_count,
            is_tracer=is_tracer,
            tracer_id=tracer_id,
        )

    async def _publish_completion(
        self,
        *,
        run_id: Optional[int],
        article_id: int,
        status: str,
        fact_count: int,
        entity_count: int,
        is_tracer: bool,
        tracer_id: Optional[str],
    ) -> None:
        await self._bus.publish(
            TOPIC_EXTRACTION_COMPLETED,
            {
                "extraction_run_id": run_id,
                "article_id": article_id,
                "status": status,
                "fact_count": fact_count,
                "entity_count": entity_count,
                "is_tracer": is_tracer,
                "tracer_id": tracer_id,
            },
        )


if __name__ == "__main__":
    # Per modularization rule 8: every stage runs standalone for dev poking.
    # No real budget here; the stage simply raises on instantiation without
    # one, which is the contract.
    import sys

    print(
        "ExtractStage requires deps.budget; instantiate via app.main wiring.",
        file=sys.stderr,
    )
    _ = TOPIC_ARTICLE_DEDUPED  # silence import-unused under the lint
