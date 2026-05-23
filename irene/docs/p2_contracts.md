# P2 cross-module contracts

> Extension of `docs/p1_contracts.md` for the **extraction (shadow)**
> layer. Same conventions; same rules. Anything not covered here
> inherits from P1.

- **Owner.** Whoever lands the next contract-touching PR.
- **Status as of 2026-05-23 (P2 kickoff).** ExtractStage interface
  locked, schema for 4 new tables locked, ontology v1 locked
  (`docs/ontology.md`), the **no-read-from-facts** invariant locked.

---

## 1. Hard invariant (P2's defining constraint)

> **The trade path NEVER reads `facts`, `entities`, `entity_aliases`,
> or `extraction_runs` in P2.** P2 is pure data accumulation.

Reasoning: extraction quality is unknown until we measure it on
held-out samples. Routing the trade decision through unmeasured
extractions risks regressing the funnel without us noticing. The
end-of-P2 report quantifies precision/recall; only THEN do we
discuss reading any of these tables into the trade path (and only
through the P3 eventize + link layer — never `facts` directly).

Enforced by:
1. **Code review.** Any PR that adds a SELECT against one of the four
   tables from a file under `app/stages/trade.py`, `app/stages/match.py`,
   `app/stages/closer.py`, or `app/ai.py` is reject-on-review.
2. **A grep-based smoke check** in CI (lands with the lint suite).

---

## 2. Topic additions

| Constant | Topic string | Publisher | Subscribers | Payload |
|---|---|---|---|---|
| `TOPIC_EXTRACTION_COMPLETED` | `extraction.completed` | `stages/extract.py` | (none in P2; P3 eventize will subscribe) | `{extraction_run_id: int, article_id: int, status: str, fact_count: int, entity_count: int, is_tracer: bool, tracer_id: str \| None}` |

`extraction.completed` is trade-path (back-pressured, not telemetry).
Even though nothing subscribes in P2, future P3 subscribers will
expect the trade-path delivery guarantees from day one.

---

## 3. ExtractStage interface

```python
class ExtractStage:
    """Subscriber on TOPIC_ARTICLE_DEDUPED."""

    ONTOLOGY_VERSION: str = "v1"
    PROMPT_VERSION: str = "v1"

    def __init__(self, deps: StageDeps) -> None:
        # raises if deps.budget is None (the extract path always needs
        # budget tracking — every call goes through the cap watcher).
        ...

    async def start(self) -> None: ...   # opens httpx.AsyncClient
    async def stop(self) -> None: ...    # idempotent close
    async def handle(self, payload: dict) -> None:
        # payload = article.deduped shape (see p1_contracts §2)
        ...
```

**Wires alongside match** on `TOPIC_ARTICLE_DEDUPED` — both stages
receive every deduped article and run independently. `wire_pipeline()`
in `app/main.py` registers both subscribers before `bus.start()`.

**LLM I/O.** Extract runs its own OpenRouter HTTP path (separate from
`ai.py`'s `direction()` path). Justification: prompts are
non-overlapping (extraction vs direction), so cache sharing buys
nothing; the table boundaries stay clean (extract owns
`extraction_runs`, ai owns `llm_calls`). Budget integration is shared:
extract calls `deps.budget.is_paused()` before every HTTP call and
`note_402` / `note_ok` after the response, exactly like ai.py.

---

## 4. Tables owned by `app/stages/extract.py`

### `entities`

```sql
CREATE TABLE entities (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type     TEXT NOT NULL CHECK(entity_type IN
                        ('COMPANY','PERSON','ORG','COUNTRY',
                         'ECON_INDICATOR','ASSET','EVENT')),
    canonical_name  TEXT NOT NULL,
    external_refs   TEXT,                -- JSON: ticker/wiki/etc.
    status          TEXT NOT NULL DEFAULT 'active',
    first_seen_at   TEXT NOT NULL,
    last_seen_at    TEXT NOT NULL,
    UNIQUE(entity_type, canonical_name)
);
CREATE INDEX idx_entities_type ON entities(entity_type);
CREATE INDEX idx_entities_canonical ON entities(canonical_name);
```

Insert-or-fetch on `(entity_type, canonical_name)`. `last_seen_at`
updates on every re-sighting; `first_seen_at` is set once.

### `entity_aliases`

```sql
CREATE TABLE entity_aliases (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id   INTEGER NOT NULL REFERENCES entities(id),
    alias       TEXT NOT NULL,
    alias_kind  TEXT NOT NULL CHECK(alias_kind IN
                    ('synonym','ticker','slug','wikidata','other')),
    UNIQUE(entity_id, alias, alias_kind)
);
CREATE INDEX idx_entity_aliases_alias ON entity_aliases(alias);
```

Many-to-one to `entities`. Resolution at scale lands in P3; for P2 we
just record what the LLM gave us and the dedupe falls out of UNIQUE.

### `extraction_runs`

```sql
CREATE TABLE extraction_runs (
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
);
CREATE INDEX idx_extraction_runs_article ON extraction_runs(article_id);
CREATE INDEX idx_extraction_runs_status ON extraction_runs(status);
CREATE INDEX idx_extraction_runs_cycle_id ON extraction_runs(cycle_id);
```

Idempotent per `(article_id, model, prompt_version, ontology_version)`
— re-running extraction with the same versions is a no-op. Bump
ontology or prompt version to force re-extraction.

### `facts`

```sql
CREATE TABLE facts (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    extraction_run_id   INTEGER NOT NULL REFERENCES extraction_runs(id),
    article_id          INTEGER NOT NULL,
    subj_entity_id      INTEGER NOT NULL REFERENCES entities(id),
    relation            TEXT NOT NULL,
    obj_entity_id       INTEGER REFERENCES entities(id),
    obj_value           TEXT,                -- JSON for non-entity targets
    confidence          REAL NOT NULL CHECK(confidence >= 0.0 AND confidence <= 1.0),
    valid_from          TEXT,
    valid_to            TEXT,
    observed_at         TEXT NOT NULL,
    evidence_span       TEXT NOT NULL,
    is_active           INTEGER NOT NULL DEFAULT 1,
    cycle_id            INTEGER NOT NULL,
    is_tracer           INTEGER NOT NULL DEFAULT 0,
    tracer_id           TEXT
);
CREATE INDEX idx_facts_extraction_run ON facts(extraction_run_id);
CREATE INDEX idx_facts_subj ON facts(subj_entity_id);
CREATE INDEX idx_facts_relation ON facts(relation, observed_at);
CREATE INDEX idx_facts_article ON facts(article_id);
```

Append-only. `is_active=0` is a soft-tombstone for the P3+ event
builder; P2 only writes `is_active=1`.

---

## 5. Prompt shape (v1)

The LLM is given the article title + summary plus the ontology vocab,
and is asked to respond with the strict JSON shape:

```json
{
  "entities": [
    {"name": "Pfizer", "type": "COMPANY", "aliases": ["PFE"]}
  ],
  "facts": [
    {
      "subj": "FDA",
      "relation": "decides",
      "obj": "Pfizer",
      "obj_value": null,
      "confidence": 0.91,
      "evidence_span": "the FDA today rejected Pfizer's application for..."
    }
  ]
}
```

Validation rules at the parse layer:

- `entities[].type` must be in the ontology entity-type list; otherwise the entity is dropped and `dropped_fact_count` is bumped for each fact that referenced it.
- `facts[].relation` must be in the ontology relation list; otherwise the fact is dropped and counted.
- `facts[].subj` must resolve to one of the extracted entities; otherwise the fact is dropped.
- `facts[].obj` is optional and must resolve to an extracted entity if present.
- `facts[].obj_value` is optional; if present, must parse as JSON-shaped per ontology.md §"obj_value".
- `facts[].confidence` is clamped to `[0, 1]`; severely malformed (NaN, non-numeric) drops the fact.
- `facts[].evidence_span` must be non-empty; otherwise dropped.

Only the surviving facts and the entities they reference get written.

---

## 6. End-of-P2 report (the ship gate for P3)

P2 ends with a dated `reports/YYYY-MM-DD-irene-p2-extraction.md` covering:

- **Volume**: number of articles processed; fact count; entity count.
- **Status distribution**: how often extractions land in each status.
- **Precision sample**: a held-out batch (~50 articles) human-labelled; agreement-rate per relation.
- **Recall sample**: same batch; what the model missed.
- **Coverage by entity type**: are some types over-represented (everything is COMPANY)?
- **Cost**: total OpenRouter spend; tokens-in / tokens-out distribution.

The report's recommendation gates P3. If precision < 0.6 or coverage is
catastrophically skewed, the ontology gets revisited (potentially with
a new ONTOLOGY_VERSION) before P3 starts.

---

## 7. Inheritance from P1

Everything not stated here inherits from P1:
- StageDeps shape (extract takes the same `StageDeps`; uses `db`, `bus`, `config`, `budget`, `cycle.current_cycle_id()`).
- Cycle stamping rules.
- `is_tracer` / `tracer_id` propagation.
- Bus topic conventions (`extraction.completed` is trade-path).
- Strict JSON roundtrip in bus tests.
- `db.batch()` atomicity (every extraction run + its facts/entities commit together).
