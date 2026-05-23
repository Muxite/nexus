# Modularization principles

> **User directive 2026-05-23.** Every piece of irene must be modular. This document is the canonical statement of what that means — the rules every module ships under, no exceptions.

This is not a "nice to have." Seulgi's biggest debts come from violations of these rules (god-objects in `engine.py`, mutable state hidden in module globals, schema migrations bolted onto whichever PR happened to touch the file). Irene starts modular and stays modular.

---

## The eight rules

### 1. One responsibility per module

Each file under `app/` and `app/stages/` does **one thing** and exposes a tight public surface.

A stage module (`app/stages/<name>.py`) exposes exactly:

```python
class Stage:
    """## Specification block goes here."""

    def __init__(self, deps: StageDeps) -> None: ...
    async def handle(self, payload: dict) -> None: ...
    async def close(self) -> None: ...
```

A utility module (`app/db.py`, `app/embed.py`, `app/budget.py`) exposes a single class or a tight set of pure functions. **No catch-all `helpers.py` or `utils.py`.** If a function doesn't belong to a clear module, the module is missing.

### 2. The bus is the only inter-stage channel

Stages never import each other. Stages never call each other's functions. A stage's only ways to communicate are:

- **Read** from the DB (read-only, no writes that another stage owns).
- **Publish** to `bus.publish(topic, payload)` where `payload` is a JSON-serialisable dict.
- **Write** to its own tables (see rule 4).

This is what makes the in-process → Redis swap a swap, not a rewrite. The day we flip `bus.py`'s backend, no stage code changes.

### 3. JSON-dict payloads only

Bus payloads contain primitives, lists, and nested dicts. **Forbidden:** Python objects, Pydantic models, numpy arrays, datetime objects (use ISO-8601 strings), pickled state, file handles.

Embeddings cross the bus as **row IDs** — the receiver fetches the float vector from the DB. Same for any large object.

The bus enforces this with a pre-publish `json.dumps` round-trip in test/dev mode. Production skips the round-trip for speed but the test suite catches violations.

### 4. Table ownership

Every table has exactly one **owner stage**. The owner is the only stage that writes to that table. Other stages may read (with restraint).

| Table | Owner |
|---|---|
| `articles` | `stages/ingest.py` |
| `article_clusters`, `article_cluster_members` | `stages/dedupe.py` |
| `markets` | `stages/markets.py` |
| `signals` | `stages/match.py` |
| `predictions`, `llm_calls`, `llm_parse_failures` | `ai.py` (the LLM client) |
| `decisions` | written by **the gate that rejects** — owner is the gate's stage |
| `trades`, `trade_features` | `stages/trade.py` |
| `decision_inputs` | written **inline** as part of the upstream stage's `db.batch()` — the bus middleware does this; no stage owns the table |
| `cycles` | `app/main.py` (singleton: cycle is global lifecycle state) |
| `entities`, `entity_aliases`, `extraction_runs`, `facts` (P2) | `stages/extract.py` |
| `events`, `event_edges` (P3) | `stages/eventize.py` |
| `market_links` (P3) | `stages/link.py` |

Schema migrations that add a column to table X live in the owner module — they don't accumulate in a central `db.py` migration list. `db.py` provides the migration mechanism; modules provide the migrations themselves.

### 5. No shared mutable state across messages

A stage's `handle()` method must be safe to call concurrently and must not mutate module-level state.

**Sole exception:** the `MarketIndex` in `stages/markets.py`. Cosine math wants ndarray locality; the index is a process-local singleton with explicit thread-safety. **This exception is documented in the module's spec block and nowhere else.** No other stage gets to add an exception without a reports/ entry justifying it.

Per-message state lives on the stack or in DB rows. Connection pools, embedder caches, and HTTP clients are stage-instance state, not module-level globals.

### 6. Dependencies are injected, not imported

Every stage's `__init__` takes a `StageDeps` dataclass with the resources it needs (DB connection, bus, embedder, AI client, config). Stages **never** import singletons.

```python
@dataclass(frozen=True)
class StageDeps:
    db: DB
    bus: Bus
    config: Config
    embedder: Embedder | None = None
    ai: AIClient | None = None
    budget: Budget | None = None
```

This makes every stage testable in isolation — instantiate it with mock deps, fire a payload, assert on the DB / bus / external calls.

### 7. Tests sit next to the code they test

Every module under `app/` has a sibling test file under `app/tests/test_<module>.py`. Every stage gets unit tests (mocked deps), integration tests (real DB, mocked external services), and a tracer round-trip test (fire 1 bullet through this stage in isolation, assert it landed in the owned tables).

The test directory layout mirrors the source directory exactly:

```
app/
  stages/
    ingest.py
    match.py
    trade.py
app/tests/
  stages/
    test_ingest.py
    test_match.py
    test_trade.py
```

### 8. Each module is independently runnable for development

Every stage module includes a `if __name__ == "__main__":` block that runs the stage standalone with a SQLite in-memory DB and a no-op bus. This lets us iterate on one module without spinning up the whole pipeline.

```python
# app/stages/ingest.py
if __name__ == "__main__":
    import asyncio
    from app.bus import InProcBus
    from app.db import DB
    from app.config import Config
    deps = StageDeps(
        db=DB.ephemeral(),
        bus=InProcBus(),
        config=Config.minimal(),
    )
    stage = Stage(deps)
    asyncio.run(stage.handle({"url": "https://example.com/test"}))
```

---

## Anti-patterns (forbidden)

- A `utils.py` or `helpers.py` catch-all.
- Module-level mutable state (`_cache = {}` at module scope).
- A stage importing another stage to call its method directly.
- A stage writing to a table it doesn't own.
- A stage reading config via `os.environ` instead of via the injected `Config`.
- A schema migration that lives in `db.py` rather than the owner module.
- A test file that touches multiple stages without explicit `@pytest.mark.integration`.
- An LLM call outside `ai.py` (every LLM call goes through one module so budget + capture + cache work).
- A bus payload containing a non-JSON-serialisable value.

---

## Cross-cutting concerns

These are the **only** concerns that span modules. They're implemented as **bus middleware** or **DB middleware**, not as imports:

- **Logging** — structured JSON via Python `logging`; module names auto-tag.
- **Profiling** — `timing.Timer` context manager; samples flushed to `metrics_minutely` (or equivalent in irene).
- **Capture** — `decision_inputs` and `llm_calls` writes are inserted by the bus middleware around each stage's `handle()` call, **inside the same `db.batch()`**.
- **Cycle stamping** — every insert that carries `cycle_id` reads it from a thread-local set by `main.py`.
- **Tracer flag propagation** — `is_tracer` and `tracer_id` are read from the inbound payload and written to every owned-table row by the bus middleware (stages don't manually thread the flag through).

The point: stages don't know any of this is happening. They handle a payload; the middleware ensures the right rows materialise in the right tables with the right metadata.

---

## How to add a new stage

1. Pick a name. The stage lives at `app/stages/<name>.py`.
2. Decide what table(s) it owns. Add the `CREATE TABLE` to this stage's module (not `db.py`).
3. Decide its bus contract: what topic it subscribes to, what topic(s) it publishes to. Document this in the spec block.
4. Write the `## Specification` block first. Then the code.
5. Add `app/tests/stages/test_<name>.py`. Include at minimum: a unit test, an integration test, and a tracer round-trip test.
6. Register the stage in `app/main.py`'s wiring (one line).
7. Update `architecture.md` Modules table with the new row.
8. Commit. The schema migration ships in the same PR.

---

## Verification

CI runs three modularization checks before any P1 build is green:

1. **Import graph check**: a static analysis script (`scripts/lint/no_cross_stage_imports.py`) fails the build if any file under `app/stages/` imports any other file under `app/stages/`.
2. **JSON payload check**: every test that publishes to the bus is wrapped in a fixture that asserts `json.loads(json.dumps(payload))` round-trips.
3. **Table-ownership check**: a script reads every `INSERT` / `UPDATE` SQL string in the codebase and asserts the table being written is owned by the module containing the SQL. Built from the table-ownership table above.

If any check fails, the build is red. There is no override.
