# Irene architecture — what we have NOW

> This is the **current-state** doc. For next steps, see `master-plan.md`.
>
> **Discipline.** Updated at the end of every phase (not every PR — that aspiration kills momentum; per-PR notes go to `changelogs/cycles.md` instead).

- **Last updated:** 2026-05-23 (**philosophy locked + observability port + v2-fast prompt**)
- **Synthesized from:** the approved plan, the seulgi audit reports, the user's DKG spec at `docs/dkg_spec.md`, the irene ontology at `docs/ontology.md`, and the P3 contracts at `docs/p3_contracts.md`.
- **Status:** P1 + P2 sealed. **P3 chunk 1 landed.** `app/stages/resolve.py` subscribes to `extraction.completed`, runs deterministic acronym → alias → suffix → self rule chain, owns the `entity_resolutions` overlay (facts stay immutable). Acronym table covers 37 common name-pairs. `--alias-aware` metric in `extraction_quality.py` extends with expansion-overlap matching. **Median alias-aware fact P/R = 0.71 / 0.65** across 3 real-LLM runs; the [P3 chunk 1 report](reports/2026-05-23-irene-p3-resolver.md) recommends chunk 2 (eventize + linker) proceeds — most of the gap to the 0.75 gate is gold-set ambiguity, not extractor failure, and chunk 2's actual success criterion is dual-path PnL parity not absolute P/R. **8 stages wired**, 247 tests green, P1 ship-gate still passing.

---

## System overview

Irene is a paper-trading bot for Polymarket designed to run **in parallel with seulgi** on the same scraper inputs but with completely separate state. The target pipeline is article → dedupe → match → predict → paper-trade, with later DKG layers (extract → eventize → link) attaching at P2 and P3. The comparison axis between irene's two trade paths (MVP vs DKG) is `trades.decision_path`; the comparison axis between irene-as-a-whole and seulgi is whatever counterfactual report we write in P4.

Today: nothing runs. The directory tree exists; five docs exist; everything else is `(TBD P1)`.

```
target shape — none of this is live yet
═══════════════════════════════════════

                       ┌──────────────────────────┐
   shared              │  scraper:9000  (HTTP)    │  ← unchanged, shared
   ─────────           │  /fetch /feed /twitter   │
                       └────────────┬─────────────┘
                                    │
       ┌────────────────────────────┴────────────────────────────┐
       ▼                                                          ▼
┌──────────────┐                                          ┌──────────────┐
│   seulgi     │  (live, unchanged)                       │    irene     │  (this repo, P0)
│              │                                          │              │
│  data/       │                                          │  irene/data/ │
│   seulgi.db  │                                          │   irene.db   │
│   chroma/    │                                          │   chroma/    │
└──────────────┘                                          └──────┬───────┘
                                                                 │
                                                ┌────────────────┴───────────────┐
                                                │  irene single-process app      │
                                                │  (P1 target — currently empty) │
                                                │                                │
                                                │   ingest    → articles         │
                                                │   dedupe    → clusters         │
                                                │   markets   → gamma cache      │
                                                │   match     → signals          │
                                                │   predict   → predictions      │
                                                │   trade     → trades, decisions│
                                                │                                │
                                                │   bus.py: in-process v1,       │
                                                │   redis v2 (same interface)    │
                                                └────────────────────────────────┘
```

---

## Modules

Every load-bearing module gets a `## Specification` block at the top of its docstring (Purpose / Public interface / Invariants / Side effects / Failure modes / Non-goals). The convention is lifted from seulgi (`seulgi/master-plan.md` §"Module specifications"). Any PR that changes module behaviour without updating its spec block is reject-on-review.

**All modules also follow the eight modularization rules in [`docs/modularization.md`](docs/modularization.md)** (user directive 2026-05-23). The headlines: one responsibility per module; the bus is the only inter-stage channel; JSON-dict payloads only; tables have one owner stage; no shared mutable state across messages; dependencies injected, never imported; tests sit next to code; each module is independently runnable. CI lints against violations.

**Today's status:** boot path is live (`main.py`, `config.py`, `db.py`); everything else is still a placeholder. The table below shows the target P1 shape.

| Module | Phase | Spec block? | Status |
|---|---|---|---|
| `app/main.py` | P1 | ✓ | **boot stub live** — registers `cycles` DDL, opens DB, waits for SIGTERM. No stages wired. |
| `app/config.py` | P1 | ✓ | **live** — pydantic `Config`, hot-reload `ConfigManager`, `IRENE__*` env overrides, `IRENE_CYCLE` bare-var support. |
| `app/db.py` | P1 | ✓ | **mechanism live** — `DB.connect/close/batch` (batch serialised by `asyncio.Lock`), seulgi-path assertion, `register_schema()` registry, `db.capture(cur, …)` writer for `decision_inputs`. Owns `schema_migrations` and `decision_inputs`. Domain tables register from owner modules. |
| `app/topics.py` | P1 | ✓ | **live** — bus topic name constants + `is_telemetry()` classifier. |
| `app/cycle.py` | P1 | ✓ | **live** — `ContextVar` cycle context; `current_cycle_id()` / `set_cycle_id()` / `cycle_scope()`. |
| `app/deps.py` | P1 | ✓ | **live** — frozen `StageDeps` dataclass (the injection contract). |
| `app/bus.py` | P1 | ✓ | **live** — `Bus` protocol + `InProcBus`. JSON-roundtrip strict mode, trade-path back-pressure, telemetry drop-first, per-subscriber task isolation. |
| `app/timing.py` | P1 | ✓ | **live** — `Timer` context manager + `ProfileStore` rolling window + percentiles. |
| `app/embed.py` | P1 | ✓ | **live** — sentence-transformers wrapper, lazy load, LRU cache, `embedder_id` stamping, `EmbedderDimMismatch`. |
| `app/budget.py` | P1 | ✓ | **live** — OpenRouter `/key` poll loop + 402-streak watcher; pause flag file written on edge transitions. |
| `app/ai.py` | P1 | ✓ | **live** — `AIClient.direction()`. Owns `llm_calls`, `predictions`, `llm_parse_failures`. JSON-suffix prompt contract, `prompt_sha` cache, budget-paused short-circuit to NEUTRAL, 402 → `note_402` + NEUTRAL. |
| `app/stages/markets.py` | P1 | ✓ | **live** — Gamma poll loop + embedding refresh loop. `MarketIndex` (documented rule-5 exception) for cosine top-k. Owns `markets`. |
| `app/stages/ingest.py` | P1 | ✓ | **live** — scraper:9000 `/feed` poller, jittered per-feed. Owns `articles`. Twitter/HTML deferred. |
| `app/stages/dedupe.py` | P1 | ✓ | **live** — subscribes to `article.ingested`. Chroma cosine search → freshness label (BREAKING/EVOLVING/REHASH/STALE_RETURN). Chromadb-missing degrades to all-BREAKING. Owns `article_clusters` + `article_cluster_members`. |
| `app/stages/match.py` | P1 | ✓ | **live** — subscribes to `article.deduped`. Embeds the article title, queries `MarketIndex.topk` (cosine), writes one immutable signal per `(article × market)` candidate. Owns `signals`. No `direction` column — that lives in `predictions`. |
| `app/stages/trade.py` | P1 | ✓ | **live** — subscribes to `signal.created`. Composes prompt from article + market, calls `ai.direction()`, applies the gate cascade (similarity → freshness → direction → edge → size), writes the funnel. Owns `decisions`, `trades`, `trade_features`. P1 uses `mid_price=0.5` stub; real CLOB integration deferred. |
| `app/main.py` | P1 | ✓ | **fully wired** — constructs every resource (Embedder / Budget / AIClient / InProcBus), then both `StageDeps` shapes (base + with `market_index`), wires 3 bus subscriptions, starts the Budget poll only if `api_key` is set, runs to SIGTERM, stops in reverse. |
| `app/stages/ingest.py` | P1 | (TBD) | not created |
| `app/stages/dedupe.py` | P1 | (TBD) | not created |
| `app/stages/markets.py` | P1 | (TBD) | not created |
| `app/stages/match.py` | P1 | (TBD) | not created |
| `app/stages/trade.py` | P1 | (TBD) | not created |
| `app/stages/extract.py` | P2 | ✓ | **live** — subscribes to `article.deduped`. Ontology-constrained LLM call → markdown-fence-strip → JSON parse → ontology validation → COMPANY-suffix normalisation → entity insert-or-fetch → facts. Acronym-preference in the prompt. Owns `entities`, `entity_aliases`, `extraction_runs`, `facts`. Independent HTTP path (shares `deps.budget` with `ai.py`). |
| `app/stages/resolve.py` | P3 ch.1 | ✓ | **live** — subscribes to `extraction.completed`. Deterministic acronym → alias → suffix → self rule chain. Owns `entity_resolutions` (the overlay; facts/entities stay immutable). 37-row `data/acronyms.yaml`; `RESOLVER_VERSION="v1"`. Publishes `resolution.completed`. |
| `app/stages/data/acronyms.yaml` | P3 ch.1 | ✓ | **live** — 37 name-pairs across ORG, COUNTRY, ECON_INDICATOR, ASSET. Lookup is bidirectional + case-insensitive. Adding a row requires a regression test under `test_resolve.py`. |
| `scripts/lint/no_facts_in_trade_path.py` | P2 | ✓ | **live** — mechanically enforces the P2 hard invariant. Scans `ai.py` + `trade.py` + `match.py` + `closer.py` for SELECT/JOIN/INTO/UPDATE/DELETE against the four extraction tables. Word-boundary patterns + an opt-in `# lint: allow-extraction-table-ref` escape. |
| `scripts/eval/gold_set/v1.yaml` + `scripts/eval/extraction_quality.py` | P2 | ✓ | **live** — 18 Reuters-style gold articles covering all 10 relations. Strict CI canonical-name matcher reports per-relation precision/recall. Mock and `--real-llm` modes. |
| `app/stages/eventize.py` | P3 ch.2a / ch.3 | ✓ | **live — wired in wire_pipeline()** — subscribes to `resolution.completed`. Hybrid entity-overlap + cosine-≥-0.75 clustering on templated triples (`subj relation obj obj_value`); per-event-type window/staleness from `data/event_types.yaml`. Owns `events`, `event_edges`; column-adds `facts.event_id`. Publishes `event.updated`. `EVENTIZE_VERSION="v1"`. |
| `app/stages/link.py` | P3 ch.2b / ch.3 | ✓ | **live — wired in wire_pipeline()** — subscribes to `event.updated`. Scores each updated event against every active market on `directness`/`novelty`/`deadline_fit`/`resolution_fit` (arithmetic mean), persists top-10 above the `CANDIDATE_FLOOR=0.20` cosine to `market_links`. Owns `market_links`. `start()` loads all market embeddings into memory. Publishes `market_links.updated`. `LINKER_VERSION="v1"`. |
| `app/clob.py` | P1 | ✓ | **live** — Polymarket CLOB mid-price client. Pure utility; no tables. Lazy `httpx.AsyncClient`; recoverable errors → `None`. |
| `app/stages/closer.py` | P1 | ✓ | **live** — poller stage. Owns `trade_closures`. Reads open trades, evaluates close rules (P1: timeout only), fetches exit mid via `deps.clob`, computes PnL, writes closure. |
| `scripts/tracer/fire.py` | P1 | ✓ | **live** — embeds a `wire_pipeline()` instance, seeds stub markets, injects N synthetic articles with unique tracer_ids, drains the bus, reports counts. CLI + `run_inproc()` for testability. **Ship-gate verified at `--count 1000 --concurrency 50` → 0 drops.** |
| `scripts/tracer/check.py` | P1 | ✓ | **live** — standalone DB checker. Counts owned-table rows where `is_tracer=1`; reports MISSING if any table is empty when articles>0. |
| `scripts/tracer/templates/*.yaml` | P1 | ✓ | three minimal templates: `regulatory_decision`, `merger`, `economic_indicator`. |
| `scripts/replay/accuracy.py` | P1 | ✓ | **live** — reads `(trades JOIN trade_closures)` from a soak DB and reconstructs `realized_pnl_usdc` per the closer's formula. CLI + `run_inproc()`. **Ship-gate verified at 3000/3000 within ±$0.01.** |
| `scripts/soak/run.py` | P1 | ✓ | **live** — orchestrator that calls the tracer (with closer enabled) and then the replay accuracy gate. Used to generate the soak report. |
| `scripts/replay/predict.py` | P1 | (TBD) | not created |
| `scripts/replay/trade.py` | P1 | (TBD) | not created |
| `scripts/replay/full.py` | P2+ | (TBD) | not created |

---

## Schema (target — none live yet)

P1 target tables (full DDL in `master-plan.md`):

| Table | Phase | Purpose | Notes |
|---|---|---|---|
| `articles` | P1 | Immutable normalised news items | + `is_tracer` / `tracer_id` from day 1 |
| `article_clusters` | P1 | Near-duplicate event clusters | — |
| `article_cluster_members` | P1 | Cluster ↔ article join | — |
| `markets` | P1 | Polymarket cache + embeddings | + `embedder_id` / `embedding_dim` to prevent silent space mixing |
| `signals` | P1 | Immutable article × market match | NULL `direction` is forbidden — splits into match (signals) + LLM call (predictions) |
| `predictions` | P1 | Immutable LLM direction row | one row per signal × model × prompt_version |
| `decisions` | P1 | Every gate's outcome ("why we skipped") | partial index on `outcome='reject'` — keeps the high-volume skip rows queryable |
| `trades` | P1 | Paper-trade decisions, executed or skipped | `decision_path` distinguishes mvp vs dkg (P3+) |
| `trade_features` | P1 | Long-table sidecar to `trades.features_snapshot` | so postmortems can GROUP BY feature |
| `cycles` | P1 | Named runs (pots reset each cycle) | `cycle_id` stamped on every decision/prediction/trade |
| `decision_inputs` | P1 | Per-signal capture for replay | shares `db.batch()` with the live write |
| `llm_calls` | P1 | LLM call audit + cache | indexed by `prompt_sha` |
| `llm_parse_failures` | P1 | Parse errors | — |
| `predictions_replay` | P1 | Counterfactual predictions from `replay/predict.py` | grouped by `replay_id` |
| `trades_replay` | P1 | Counterfactual trades from `replay/trade.py` | — |
| `entities`, `entity_aliases`, `extraction_runs`, `facts` | P2 | DKG entity/fact layer | shadow only — never read by the trade path in P2 |
| `events`, `event_edges`, `market_links` | P3 | DKG event + linker | introduces the alternate trade path |

Every traveller-of-the-pipeline table also carries `is_tracer` (INT NOT NULL DEFAULT 0) and `tracer_id` (TEXT) so synthetic bullets can be split from real data with a partial index for free.

---

## External dependencies (target)

| Dep | Used for | Notes |
|---|---|---|
| `scraper:9000` (HTTP) | News ingestion | shared with seulgi; only shared dependency |
| Polymarket Gamma API | Market discovery + metadata | own poller (no shared cache) |
| Polymarket CLOB | Live mid prices + price history | own client + cache |
| OpenRouter | LLM direction (+ extraction in P2) | own budget tracking via `budget.py` |
| sentence-transformers | Article + market embeddings | `embedder_id` stamped on every embedding row |
| ChromaDB | News freshness / dedup | **own store** at `irene/data/news_chroma/` — dual-container hazard means we never share seulgi's |

---

## Configuration (target)

`config.yaml` ships in P1. Hot-reload pattern stolen from old-irene (`irene_shared/config.py` at commit `122720a` in this repo's history). Env-var override via `IRENE__SECTION__KEY=value`, same as seulgi.

**No engine/variant fan-out in v1** (user decision 2026-05-22). The schema keeps `engine_name`/`variant_name` columns for forward-compat but only `'default'` ships.

---

## Decision funnel (target — none live yet)

Once P1 ships, every headline runs the cascade:

```
ingest      → article row (always written)
dedupe      → cluster_id (chroma freshness label: BREAKING / EVOLVING / REHASH / STALE_RETURN)
markets     → candidate market IDs (cosine top-K above similarity floor)
match       → signal rows (one per article × candidate)
predict     → prediction rows (one per signal; LLM direction + confidence)
trade gate  → trade row or decision='reject' row with explicit reason
```

Drop rates land here once we have data. Until then: nothing to report.

---

## Known limitations (today)

- **Live CLOB pricing is wired but not exercised by the soak.** `trade.py` calls `deps.clob.get_mid()` for the bet-side token and falls back to `cfg.trade.default_mid_price=0.5` on None. `main.py` constructs a real `CLOBClient` against `https://clob.polymarket.com`. The soak harness uses a stub CLOB so the soak is hermetic; a real-CLOB soak is the first item in P2.
- **No position closer beyond timeout.** P1 ships timeout-only. Target / stop / market_resolved rules slot into `closer._evaluate_rules` in later phases.
- **No position closer.** `trades.closed_at` and `trades.realized_pnl` columns exist but nothing populates them yet.
- **Dedupe degrades to all-BREAKING when chromadb is missing.** Documented and intentional — keeps tests fast (no chromadb required) and survives a container that lost the dep. Docker image installs chromadb; degradation is for dev/CI only.
- **DKG layers (P2/P3)** are unspecified at the code level — the spec lives at `docs/dkg_spec.md` but the eventizer clustering thresholds and `directness_score`'s actual function are open research questions per the master-plan critique.
- **`docs/decision_funnel.md`, `docs/pitfalls.md`, `docs/errors_and_fixes.md`** are TBD — they land alongside the modules they describe.

---

## Hard rules baked into the design

(See `README.md` §"Hard rules" for the full list — replicated here so this doc stands alone.)

1. No DB sharing with seulgi (runtime assertion in `db.py`).
2. Standalone docker-compose only (no edits to root compose).
3. Single-decision-per-headline (no variant fan-out in v1).
4. Tracers and live trades cannot mix (`is_tracer=1 AND paper=0` is a hard write rejection).
5. Capture (`decision_inputs`, `llm_calls`) lives inside the live `db.batch()` — never lags.
