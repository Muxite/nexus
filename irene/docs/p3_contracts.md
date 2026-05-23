# P3 cross-module contracts

> Extension of `docs/p1_contracts.md` and `docs/p2_contracts.md` for the
> **events + linker** phase. Same conventions; same rules. Anything not
> covered here inherits.

- **Owner.** Whoever lands the next contract-touching PR.
- **Status as of 2026-05-23 (P3 chunk 1: entity resolver).** ResolveStage
  interface locked. `entity_resolutions` schema locked. Acronym-table
  format locked. Alias-aware metric contract locked.

---

## 1. Hard invariant (inherited from P2 + extended)

> Trade path still NEVER reads `facts` / `entities` / `entity_aliases` /
> `extraction_runs`, **nor** the new `entity_resolutions` table.

P3 adds DKG-side outputs but does NOT yet route any DKG output into the
trade path. That's the explicit master-plan decision for P3 itself; the
P3 report measures DKG-vs-MVP overlap before any wiring is even
considered.

The CI lint (`scripts/lint/no_facts_in_trade_path.py`) gets one more
table added to its forbidden list — see §6 below.

---

## 2. Why the resolver is its own stage (not folded into extract)

Three reasons:
- **Idempotence on retry.** Resolution rules can be re-run against the
  full corpus when the acronym table grows; if it were inside extract,
  we'd have to re-extract to get the benefit. As a stage with its own
  background pass, resolver re-runs cheaply.
- **Auditability.** Every resolution decision lands in `entity_resolutions`
  with a `resolution_kind` (`acronym | alias | suffix | self | manual`),
  so we can audit what merged with what. If we mutated `entities`
  in-place from inside extract, the history would be lost.
- **DKG spec alignment.** The spec's queue topics include
  `kg.entity_resolution.completed.v1`; modelling that as a real stage
  keeps the codebase walking the same path the spec walks.

---

## 3. Topic additions

| Constant | Topic string | Publisher | Subscribers | Payload |
|---|---|---|---|---|
| `TOPIC_RESOLUTION_COMPLETED` | `resolution.completed` | `stages/resolve.py` | (none in P3 chunk 1; eventize will subscribe in chunk 2) | `{extraction_run_id: int, article_id: int, entities_resolved: int, merges: int, is_tracer: bool, tracer_id: str \| None}` |

`resolution.completed` is trade-path (back-pressured), same as
`extraction.completed`.

---

## 4. `ResolveStage` interface

```python
class ResolveStage:
    """Subscriber on TOPIC_EXTRACTION_COMPLETED."""

    RESOLVER_VERSION: str = "v1"
    # YAML path is relative to the irene/ repo root; resolver loads the
    # table once at start() and keeps the in-memory copy for the run.
    ACRONYM_TABLE_PATH: str = "app/stages/data/acronyms.yaml"

    def __init__(self, deps: StageDeps) -> None: ...
    async def start(self) -> None: ...   # loads the acronym table; no background task
    async def stop(self) -> None: ...    # idempotent
    async def handle(self, payload: dict) -> None:
        # payload = extraction.completed shape (§3 above + p2_contracts §2)
        ...
```

**Resolution flow per `handle()` call:**
1. Read the `(extraction_run_id, article_id)` from the payload.
2. SELECT every entity that's a `subj_entity_id` or `obj_entity_id` on a
   fact from this article AND doesn't already have an
   `entity_resolutions` row.
3. For each, apply the rule chain in order:
   - **acronym**: case-insensitive match of either form against the loaded
     acronym table. If matched, find the existing entity whose
     `canonical_name` is the OTHER form (case-insensitive on the same
     entity_type); resolve to that entity's id.
   - **alias**: SELECT entities whose `id` ≠ this entity's id AND share
     an `entity_aliases.alias` (case-insensitive) AND share
     `entity_type`. If exactly one matches, resolve.
   - **suffix**: if this entity is COMPANY-type and its `canonical_name`
     differs only by a known legal suffix from another COMPANY entity's
     `canonical_name`, resolve to the shorter-named entity. (Redundant
     with extract.py's suffix-strip in practice; safety net for legacy
     rows.)
   - **self**: no merge target found; the resolution row points to the
     entity itself (`resolved_to_id == entity_id`).
4. Write one `entity_resolutions` row per entity (UNIQUE on `entity_id`,
   so re-running on a re-fired payload is a no-op).
5. Publish `TOPIC_RESOLUTION_COMPLETED`.

**What the resolver does NOT do (P3 chunk 1 boundary):**
- Doesn't mutate `entities`, `entity_aliases`, or `facts`. Facts remain
  immutable; the overlay table is the truth.
- Doesn't do fuzzy / Levenshtein matching. Deterministic-only.
- Doesn't resolve across `entity_type`. An ORG and a COMPANY with the
  same surface name stay distinct.
- Doesn't do cross-article entity resolution beyond what falls out of
  the acronym table and aliases. A new "Pfizer" mention finds the
  existing "Pfizer" because both share `(COMPANY, "Pfizer")` UNIQUE.
- Doesn't write back to `entities.status='merged'`. Leaves rows
  active; the overlay is the source of truth for "current canonical".

---

## 5. `entity_resolutions` schema (owned by `app/stages/resolve.py`)

```sql
CREATE TABLE entity_resolutions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id       INTEGER NOT NULL UNIQUE REFERENCES entities(id),
    resolved_to_id  INTEGER NOT NULL REFERENCES entities(id),
    resolution_kind TEXT NOT NULL CHECK(resolution_kind IN
                        ('self','acronym','alias','suffix','manual')),
    confidence      REAL NOT NULL CHECK(confidence >= 0.0 AND confidence <= 1.0),
    resolver_version TEXT NOT NULL,
    created_at      TEXT NOT NULL
);
CREATE INDEX idx_entity_resolutions_resolved_to ON entity_resolutions(resolved_to_id);
CREATE INDEX idx_entity_resolutions_kind ON entity_resolutions(resolution_kind);
```

- `entity_id` is the original entity row (preserved as-written by extract).
- `resolved_to_id` is the canonical (may equal `entity_id` for self-resolved).
- `resolution_kind` records which rule fired.
- `confidence` is hardcoded by the rule that wins (acronym/alias/suffix = 1.0; self = 1.0; manual ≤ caller's discretion).
- `resolver_version` lets a future rule-set rewrite re-run the resolver while keeping prior decisions queryable.

**No tombstones.** A resolution is permanent — if you want to revisit,
write a new row with a higher `resolver_version` and have queries
prefer the highest version (`ORDER BY resolver_version DESC LIMIT 1`).

---

## 6. CI lint update

`scripts/lint/no_facts_in_trade_path.py`'s `FORBIDDEN_TABLES` tuple gains
`"entity_resolutions"`. The lint already covers the four P2 tables;
adding one more keeps the invariant clean as P3 scope grows.

---

## 7. Acronym table (`app/stages/data/acronyms.yaml`)

```yaml
version: v1
created_at: 2026-05-23
# Each entry is a (entity_type, short, long) tuple. Resolver maps EITHER
# direction: short → long OR long → short, whichever survives in the
# entities table. Order doesn't matter at resolve time.
acronyms:
  - {type: ORG, short: "FDA", long: "U.S. Food and Drug Administration"}
  - {type: ORG, short: "SEC", long: "U.S. Securities and Exchange Commission"}
  - {type: ORG, short: "Fed", long: "Federal Reserve"}
  # ... see the actual file for the full list
```

Adding a row is the only mechanism for taking on new acronyms in P3
chunk 1. The agent that lands a row also lands a regression test
demonstrating the merge fires.

---

## 8. Alias-aware quality metric v2

`scripts/eval/extraction_quality.py` gains a `--alias-aware` flag.

**Matching rules when `--alias-aware` is on:**

For each pair of (extracted entity X, expected entity G):

1. **Type must match.** No cross-type fudge.
2. **Direct CI canonical match** — same as the strict metric.
3. **Alias match** — if X's canonical equals one of G's aliases (CI), or
   vice versa, count as a match.
4. **Resolution match** — if X resolves (via `entity_resolutions`) to an
   entity whose canonical equals G's canonical (CI), or vice versa, count
   as a match.
5. **Acronym match** — if (X's canonical, G's canonical) are paired in the
   loaded acronym table (either direction), count as a match.

For each pair of (extracted fact X, expected fact G):

- Same as before, but apply alias-aware matching to `subj` and `obj`.

**Output**: when `--alias-aware` is on, the report prints BOTH the
strict and the alias-aware P/R columns. The strict numbers stay so we
can see the gap close over time.

**Gate to enter eventize (P3 chunk 2)**: alias-aware fact P/R ≥ 0.75 on
the gold set v1. The strict numbers can stay below this — they remain a
useful diagnostic but are not the gate.
