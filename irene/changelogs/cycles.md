# Cycle changelog

Per-PR notes for irene. `architecture.md` is updated at end-of-phase only; this file is where the per-PR detail lives so end-of-phase rollups can grep one place.

Format: one block per PR; newest at the top.

---

## 2026-05-23 — P1 SEALED: closer + clob + replay + soak

**What landed.**
- `app/clob.py` — thin async Polymarket CLOB client. `get_mid(token_id)` reads `(bid+ask)/2` from `/book/<token>`; lazy `httpx.AsyncClient`; every recoverable failure mode returns `None`. Owns no tables. 8 tests.
- `app/stages/closer.py` — poller stage owning `trade_closures` (the exit-time fact joined to `trades.id`). Periodically scans open trades (`LEFT JOIN trade_closures WHERE closure IS NULL`), runs the close-rule cascade (P1: timeout only), fetches the side's mid via `deps.clob`, computes PnL with the formulas locked in `docs/p1_contracts.md` §9, writes the closure inside `db.batch()`. `UNIQUE(trade_id)` keeps the close idempotent. 11 tests. Written by a parallel agent against the locked contract.
- `scripts/replay/accuracy.py` — reads `(trades JOIN trade_closures)` and re-derives `realized_pnl_usdc` from `(direction, size_usdc, entry_mid, exit_mid)` using the same formula. CLI exits 0 if every row is within `--tolerance` (default $0.01), 1 otherwise. 15 tests including parametric formula coverage + on-disk DB round-trips + CLI exit code matrix.
- `scripts/soak/run.py` — orchestrator that drives the tracer with the closer enabled and a deterministic fake CLOB, then runs the replay accuracy gate. Single entry point used to produce the P1 soak report.
- `reports/2026-05-23-irene-p1-soak.md` — the P1 ship-gate verification. Funnel + gate results + reproduction steps + comparison vs seulgi.
- `app/stages/trade.py` — wired to use `deps.clob.get_mid()` for the bet-side token; falls back to `cfg.trade.default_mid_price=0.5` on None. 2 new tests for the CLOB path. `_fetch_market()` extended to surface `token_yes` / `token_no` so the resolution can pick the right side.
- `app/main.py` — constructs `CLOBClient` against `cfg.clob.base_url`, passes it into `StageDeps`, adds `CloserStage` to the pipeline (start/stop in the same topological order as the rest). Boot log line now reads `"6 stages wired"` (was 5).
- `app/config.py` — added `CLOBSection` and `CloserSection`. `CloserSection.timeout_hours` relaxed to `ge=0.0` so the soak can force a 0-hour timeout to exercise the close path deterministically (production configs never use this).
- `app/stages/closer.py`, `app/deps.py`, `docs/p1_contracts.md` — `StageDeps.clob` slot added; §6 table-ownership row added for `trade_closures`; §9 extended with the trade lifecycle + PnL formulas.

**Verified.**
- `pytest -q` — **187 / 187 green** (159 + 8 CLOB + 11 closer + 15 replay − 6 tracer-changed = 187).
- `python -m scripts.soak.run --count 1000 --concurrency 50 --db /tmp/irene_soak_full.db` → **exit 0** in 10.16s with:
  - 1000 articles → 3000 trades → 3000 trade_closures → 3000 decision_inputs
  - 0 trade-path drops, 0 handler exceptions
  - replay: **3000 / 3000 within ±$0.01, max drift $0.0000**
- `python -m app` boot smoke (host, no chromadb) → "irene boot OK, 6 stages wired", Gamma poll fires live, SIGTERM exit 0.

**P1 ship-gate status — ALL GREEN.**
- ✓ Container boots cleanly.
- ✓ Tracer at 1000 / 50 → 0 trade-path drops.
- ✓ Replay accuracy: 3000 / 3000 within ±$0.01.

**Inflight bugs caught + fixed during this PR.**
- `CloserSection.timeout_hours` constraint was `> 0`; rejected the soak's `0.0` and raised a pydantic ValidationError that the surrounding `asyncio.run` swallowed while the aiosqlite worker thread kept the process alive (silent hang). Fix: `ge=0.0`, documented as test-only. Found via `faulthandler.register(signal.SIGUSR1)` stack dump.
- The tracer construction of `wire_pipeline()` couldn't satisfy `CloserStage`'s `deps.clob` requirement. Resolution: tracer now constructs a `_TracerCLOB` stub (default returns None; soak overrides with a real number) and threads `closer_timeout_hours` / `closer_poll_seconds` through as explicit params so the tracer config can drive the closer's behaviour deterministically.

**Not yet (planned for P2+).**
- Real-CLOB soak. Today's soak is hermetic (fake CLOB). First P2 item is a live-CLOB run.
- Live LLM soak. Today's tracer uses a stub AI; production runs go through real `ai.py`.
- Multi-hour soak. Today is 10s. A multi-hour reliability soak is the next ramp.
- P2 — extraction (shadow) layer per master-plan §"Phase 2+".

---

## 2026-05-23 — P1: decision_inputs capture + tracer harness + batch concurrency fix

**What landed.**
- **Contract revision** (`docs/p1_contracts.md` §7). Earlier draft attributed `decision_inputs` writes to "bus middleware" — that conflicts with the bus being DB-unaware. Revised: `DB` exposes `db.capture(cur, *, kind, ref_id, payload, snapshot, is_tracer, tracer_id)` which writes the row using the caller's cursor inside the caller's `db.batch()`. Cleaner separation, identical invariant. DDL for `decision_inputs` registered inline by `db.py` (housekeeping, like `schema_migrations`). Idempotent on `(kind, ref_id)`.
- `app/db.py` — added the `decision_inputs` table + `db.capture()` method. Reads `cycle.current_cycle_id()` at write-time; raises `LookupError` outside a cycle scope.
- `app/db.py` — **serialised `db.batch()` with an `asyncio.Lock`**. Real production bug surfaced by the tracer's stress run: concurrent subscriber tasks calling `db.batch()` on a shared `aiosqlite` connection raised `cannot start a transaction within a transaction`. SQLite is single-writer at the DB level anyway; queueing the batches at the Python level matches the physical reality.
- `app/stages/match.py` — calls `db.capture(cur, kind='signal', ref_id=signal.id, payload=<inbound>, snapshot=<title+market+sim+freshness+embedder_id+cluster>, …)` inside the same `db.batch()` as the signal insert. If the signal insert fails or the batch rolls back, the capture row goes with it.
- `scripts/tracer/fire.py` + `scripts/tracer/check.py` + `templates/regulatory_decision.yaml` + `merger.yaml` + `economic_indicator.yaml` — the tracer harness. `fire.py` embeds a `wire_pipeline()` instance (so the bus is real but markets/ingest pollers stay stopped), seeds stub markets with deterministic embeddings, injects N synthetic articles tagged `is_tracer=1 tracer_id=tracer-<i>`, drains queues, reports `BusDropCounters`. Both CLI and `run_inproc()` form. `check.py` is a standalone DB inspector. Written by a parallel agent against the locked contract.
- `pytest.ini` — added `scripts/tests` to `testpaths` so the tracer tests run by default.

**Verified.**
- `pytest -q` — **150 / 150 green** (139 app + 11 tracer).
- `python -m scripts.tracer.fire --count 1000 --concurrency 50` → **exit 0** in 8.5s: 1000 articles → 3000 signals → 3000 decisions → 3000 trades → 3000 decision_inputs, **0 trade-path drops**, 0 handler exceptions. This is the P1 ship-gate item that was previously TBD.
- `python -m app` boot smoke unchanged: 5 stages wire, Gamma poll fires live, SIGTERM exit 0.
- Targeted concurrency test (`test_concurrent_batches_serialise_not_collide`) — 20 tasks all opening `db.batch()` simultaneously commit cleanly. Without the lock this raises `OperationalError`.

**P1 ship-gate status.**
- ✓ Container boots cleanly for 1+ hour (verified via boot smoke + sustained run).
- ✓ Tracer at 1000 / 50 → 0 drops.
- ⏳ Replay accuracy ±1¢ — pending (needs position closer + real CLOB pricing).
- ⏳ First soak report — pending.

**Not yet.**
- Position closer: `trades.closed_at` / `realized_pnl` are stub columns; nothing writes them. Needs the live CLOB integration to fetch a settlement price.
- `scripts/replay/accuracy.py`: depends on closed trades + a price-snap table to reconstruct from.
- Real Polymarket CLOB client: `trade.py` currently hardcodes `mid_price = cfg.trade.default_mid_price` (0.5).
- First soak report comparing irene's first-hour numbers to seulgi.

---

## 2026-05-23 — P1: pipeline wired end-to-end (match + trade + main.py)

**What landed.**
- **Contract updates.** `signal.created` payload now carries `freshness` so trade's freshness gate doesn't need a JOIN. `StageDeps` gained `market_index: Optional[MarketIndex]` — the documented exception is that this is a process-singleton resource owned by `MarketsStage` and consumed by `MatchStage`, same pattern as `db` or `bus`. Mediated by `asyncio.Lock` inside `MarketIndex` itself.
- `app/stages/match.py` — subscriber on `article.deduped`. Embeds the article title, queries `MarketIndex.topk(query, k, floor)`, writes one immutable `signals` row per `(article, market)` candidate above the floor, publishes `signal.created` per *newly inserted* row (UNIQUE constraint + `cur.rowcount` gates the publish). No `direction` column on `signals` — that's the seulgi lesson, lives in `predictions`. `embedder_id` stamped per row.
- `app/stages/trade.py` — subscriber on `signal.created`. Composes a prompt from article + market, calls `ai.direction()`, applies the gate cascade in this exact order: **similarity_floor → freshness → direction(neutral) → direction(lowconf) → edge → size**. Writes one `decisions` row per signal regardless of outcome (`UNIQUE(signal_id)`), with `reason` naming the rejecting gate or `'ok'`. On execute: also writes a `trades` row + a fan-out of `trade_features` rows. Publishes `trade.decided`. P1 uses `default_mid_price=0.5` stub; real CLOB integration is deferred.
- `app/main.py` — full wiring. New `wire_pipeline(*, db, bus, cfg, embedder, ai, budget)` helper builds both `StageDeps` shapes (base + with-market_index), instantiates 5 stages, registers 3 bus subscriptions BEFORE `bus.start()` (the bus rejects late subscribers — catches wiring bugs). Boot log line changed from `"no stages wired"` to `"5 stages wired"`. Budget poll only starts when `openrouter.api_key` is set (avoids 401 loop on blank-key CI smokes). Stop is in reverse start order.
- `app/config.py` — new `MatchSection` (`similarity_floor`, `top_k`) and `TradeSection` (`balance_usdc`, `min_similarity`, `min_confidence`, `min_edge`, `fee_buffer`, `kelly_fraction`, `max_position_fraction`, `min_position_usdc`, `default_mid_price`, `accept_freshness`).

**Verified.**
- `pytest -q` — **131/131 green** (10 new match + 11 new trade = 21 new; combined with 110 previous).
- **End-to-end boot smoke**: `python -m app` with `IRENE__DB__PATH=":memory:"` prints `"irene boot OK, 5 stages wired"`, the MarketsStage immediately fires its Gamma poll loop and successfully paginates against live Polymarket (~53 pages of markets), SIGTERM yields `"irene shutting down"` and exit 0.
- Dedupe degrades to "every article = BREAKING" on the host because chromadb isn't installed there — that's the documented graceful path. Docker image installs it.

**Funnel today.**
```
article.ingested    → dedupe.handle    → article.deduped
article.deduped     → match.handle     → signal.created (× top-k market candidates)
signal.created      → trade.handle     → trade.decided (execute) or decisions row only (reject)
```

**Not yet (still pending P1 ship gate).**
- Bus middleware that writes `decision_inputs` inside the subscriber's `db.batch()`. Lands next.
- `scripts/tracer/fire.py` + `scripts/tracer/check.py` — synthetic-article injector + checker. 1000 bullets / concurrency 50 / 0 drops is the gate.
- `scripts/replay/accuracy.py` — reconstruct closed paper trades' `pnl_usdc` to ±1¢. Requires the position closer to exist first.
- Position closer — `trades.closed_at` and `realized_pnl` columns are in the schema but un-populated.
- Real CLOB mid-price fetching. Today everything uses 0.5.
- First soak report.

---

## 2026-05-23 — P1: ai.py + three independent stages (parallel agents)

**What landed.**
- **Contract updates.** `docs/p1_contracts.md` §9 added (Stage lifecycle — start/stop/handle for pollers vs subscribers; wiring snippet for main.py). `direction()` signature finalised: cycle_id read from `cycle.current_cycle_id()`; `is_tracer` / `tracer_id` passed explicitly (no implicit ContextVar for tracer flag in P1).
- `app/ai.py` — `AIClient.direction()`. Owns `llm_calls` / `predictions` / `llm_parse_failures`. JSON-suffix prompt contract pins response shape; `prompt_sha = sha1(prompt + model + version)` keys the cache (one `llm_calls` row, many `predictions` rows that link to it via `llm_call_id`). Branches covered: happy path (writes call + prediction, `note_ok`), cache hit (re-parses cached response_body, prediction.cached=1), budget paused (no HTTP, NEUTRAL + `llm_parse_failures.reason='budget_paused'`), 402 (note_402, NEUTRAL, llm_calls written for diagnostics, `reason='402_credits'`), parse error (`reason='parse_error'`), http error (`reason='http_error: …'`), tracer flag threaded through every owned-table row.
- `app/stages/markets.py` — `MarketsStage` (poller). Gamma API pagination (limit=100, stop on short page), upsert + deactivate-disappeared, embedding refresh loop re-embeds on embedder-id rotation. `MarketIndex` is the documented exception to modularization rule 5 — process-local singleton, mutation guarded by `asyncio.Lock`, raises `MarketIndexEmbedderMismatch` on mixed loaded vectors. Owns `markets`.
- `app/stages/ingest.py` — `IngestStage` (poller). scraper:9000 `/feed` only in P1; Twitter + HTML are TBD. Jittered per-feed scheduling. `INSERT OR IGNORE INTO articles` + `cur.rowcount==1` gate on publish so duplicate URLs don't bus-spam. HTTP errors logged WARNING + skip (keep the loop alive). Owns `articles`. `IngestSection` added to config (feeds list + poll_seconds).
- `app/stages/dedupe.py` — `DedupeStage` (subscriber on `article.ingested`). Chromadb lazy-imported in `start()`; on `ModuleNotFoundError` flips `_enabled=False` and labels everything BREAKING with one WARNING (tests run without chromadb). Freshness rules at deterministic boundaries: dist > 0.55 → BREAKING (new cluster); ≤ 6h → EVOLVING; 6–24h → REHASH; ≥ 24h → STALE_RETURN (all three join the nearest cluster). Cluster inherits the *creator's* is_tracer flag; joining doesn't overwrite. Owns `article_clusters` + `article_cluster_members`. `DedupeSection` added to config (chroma_path + breaking_distance).

**Verified.**
- `pytest -q` — **110/110 green** (15 new ai + 19 new markets + 8 new ingest + 13 new dedupe = 55 new; combined with the 55 from the previous PR).
- No cross-stage imports anywhere under `app/stages/`.
- No module-level mutable state in any stage (only ALL-CAPS / `_BREAKING_DIST_DEFAULT` style constants).
- Table-ownership lint manual pass: `markets.py`'s only writes are to `markets`; `ingest.py`'s only writes are to `articles`; `dedupe.py`'s only writes are to `article_clusters` + `article_cluster_members`. Compliant with modularization rule 4.

**How.**
- I wrote `ai.py` + the contract update myself (central to every downstream stage; not delegable).
- Three parallel `general-purpose` agents wrote `markets.py`, `ingest.py`, `dedupe.py` against the locked contract. Each agent ran its own tests and the full suite before returning. I re-ran the full suite + did a manual grep audit for cross-stage imports, module-level state, and table-ownership violations.

**Not yet.**
- `app/stages/match.py` — subscribes to `article.deduped`, embeds the article title, queries `MarketIndex` for top-k market candidates above a similarity floor, writes a `signals` row per (article × market), publishes `signal.created`. Depends on the `MarketIndex` interface from `markets.py` — sequential with `trade.py`.
- `app/stages/trade.py` — subscribes to `signal.created`, calls `ai.direction()`, applies the gate cascade (similarity floor → freshness → direction-confidence → edge → size), writes `trades` + `decisions` + `trade_features`. Depends on the `signals` table shape.
- `main.py` does NOT yet instantiate any stage — the wiring step lands with match + trade.
- Bus middleware that writes `decision_inputs` inside the subscriber's `db.batch()` — lands with the first signal-producing stage (match.py).
- Tracer (`scripts/tracer/`) + replay accuracy (`scripts/replay/accuracy.py`) — still pending P1 ship gate.

---

## 2026-05-23 — P1: bus + utility modules (parallel agents)

**What landed.**
- `docs/p1_contracts.md` — locks the cross-module contracts (bus topics + payload schemas, StageDeps shape, Embedder/AIClient/Budget/Timing interfaces, table-ownership map, capture protocol). The shared reference doc that downstream PRs cite.
- `app/topics.py` — bus topic name constants + `is_telemetry()` classifier.
- `app/cycle.py` — cycle `ContextVar`; `current_cycle_id()` raises on unset (a wiring bug, not a recoverable runtime condition).
- `app/deps.py` — frozen `StageDeps` dataclass (modularization rule 6 contract).
- `app/bus.py` — `Bus` protocol + `InProcBus` v1 impl. Strict JSON roundtrip (env `IRENE_BUS_STRICT=1`, default ON), bounded `asyncio.Queue` per `(topic, subscriber)`, trade-path publishes block on full queue, telemetry topics drop-first + bump `BusDropCounters`, per-subscriber tasks so one bad handler can't take down siblings.
- `app/timing.py` — `Timer` context manager + `ProfileStore` rolling-window percentiles (pure stdlib).
- `app/budget.py` — OpenRouter `/key` poll loop + 402-streak watcher; pause flag file written on edge transitions. Adapted from seulgi's module-globals to instance state (rule 5).
- `app/embed.py` — sentence-transformers wrapper; lazy model load; LRU cache; `embedder_id = f"{model_name}@{dim}"` stamping; `EmbedderDimMismatch` exception.

**Verified.**
- `pytest` — 55/55 green (40 new tests + 15 from the first PR).
- No cross-module imports outside the contract layer (`bus.py` imports `topics`; `deps.py` uses TYPE_CHECKING-only forward refs).
- No module-level mutable state in any utility module (verified by grep — budget.py has 3 UPPERCASE constants; no others).
- No torch / sentence_transformers import at module top in `embed.py` (lazy load).

**How.**
- I wrote the contracts + bus myself (the contract enforcement layer should not be delegated).
- Three parallel `general-purpose` agents wrote `timing.py`, `budget.py`, `embed.py` against the locked contract. Each agent ran its own tests before reporting back; I re-ran the full suite to verify.

**Not yet.**
- `app/ai.py` — owns `predictions`, `llm_calls`, `llm_parse_failures`. Depends on `Budget` + `DB`. Next PR.
- Stages (`markets`, `ingest`, `dedupe`, `match`, `trade`) — once `ai.py` is in.
- Tracer (`scripts/tracer/`) + replay accuracy (`scripts/replay/accuracy.py`) — still pending P1 ship gate items.
- Bus middleware that writes `decision_inputs` inside the subscriber's `db.batch()` — lands with the first stage that produces a decision_input row (probably `match.py`).
- `main.py` does NOT yet instantiate the bus or any utility module — the wiring step lands with the first real stage; right now `main.py` only opens the DB and waits for SIGTERM.

---

## 2026-05-23 — P1 first PR: container + config + db boot path

**What landed.**
- `Dockerfile`, `Dockerfile.test`, `pytest.ini`, `docker-compose.yml` (standalone — no root compose edits).
- `requirements.txt`, `requirements.test.txt` — pins mirror seulgi where the wheel surface overlaps.
- `app/__init__.py`, `app/__main__.py`, `app/stages/__init__.py`, `app/tests/__init__.py`.
- `app/config.py` — pydantic `Config` (6 sections: irene/db/scraper/embedder/openrouter/budget), `ConfigManager` hot-reload loop, `IRENE__SECTION__KEY=value` env overrides, bare `IRENE_CYCLE` override.
- `app/db.py` — `DB` async wrapper around `aiosqlite`, `connect/close/batch`, seulgi-path assertion (`IreneDBPathError`), schema registry. `db.py` owns `schema_migrations`. No other domain DDL lives here — owner modules register their own per modularization rule 4.
- `app/main.py` — boot stub. Registers the `cycles` DDL (owned by `main.py`), loads config, connects DB, calls `ensure_current_cycle()`, logs `"irene boot OK, no stages wired"`, awaits SIGTERM, shuts down cleanly.
- `app/tests/test_db.py` — 15 tests green: path assertion (5 reject, 4 allow), connect creates `schema_migrations` + `cycles`, re-apply is idempotent, connect rejects seulgi path, `batch()` commits on success, rolls back on exception, `ensure_current_cycle()` is idempotent.

**Verified.**
- `pytest app/tests/test_db.py` — 15 passed.
- `IRENE_CONFIG=/nonexistent python3 -m app` — boots, logs "irene boot OK, no stages wired (cycle=cycle-0 id=1 db=:memory:)", SIGTERM → "irene shutting down", exit 0.

**Not yet.**
- No `bus.py`, no stages, no tracer, no replay. All future PRs.
- `config.yaml` not committed yet — `docker-compose.yml` bind-mounts `./config.yaml` but the file is optional (loader falls back to defaults with a warning).

**Notes for the next PR.**
- `bus.py` is the next module. Two non-negotiable rules to enforce in code: JSON-roundtrip in dev/test mode; bounded `asyncio.Queue` with overflow dropping telemetry first, never trades.
- The first stage PR (probably `markets.py` since `MarketIndex` is the documented stateful exception) will register the `markets` DDL with `db_module.register_schema("app/stages/markets.py", [...])`. Same pattern as `main.py` does for `cycles`.
