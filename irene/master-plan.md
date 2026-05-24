# Master plan — Irene

> The canonical roadmap. For current state, see [`architecture.md`](architecture.md). For the design north star, see [`docs/dkg_spec.md`](docs/dkg_spec.md). The full plan that authorised this build lives at `~/.claude/plans/based-on-the-new-lovely-coral.md` — this file is the in-repo working copy.

- **Last updated:** 2026-05-24 (**P3 chunk 3 + 4 complete** — dual-path trader is live, scraper-v2 with HTML ingestion wired, accounting + per-cycle isolation shipped; long soak in progress)
- **Current phase:** post-P3-ch4 long soak under `cycle3-fresh`. Next session picks a cycle from `handoffs/2026-05-24.md` §"Open follow-ups", implements it in full, then runs a 4h+ soak before re-planning. The user's directive this session was **implement-then-soak-long, gather the most data possible** — not many small mid-soak iterations.

---

## Context

We're building **irene** to run in parallel with [seulgi](../seulgi/), sharing only the `scraper:9000` HTTP service. The goals:

1. Iterate on a cleaner architecture without breaking the live seulgi pipeline.
2. Layer in a **DKG (Dynamic Knowledge Graph) design** over multiple phases.
3. Compare old-vs-new before promoting any irene decision into the trading path.

Seulgi's lessons (banked over months of cycles): WAL deadlocks, dual-container HNSW corruption, confidence/p_alpha overloading, NEUTRAL drought, migration sprawl, variant attribution collapse. Irene is designed to avoid each of those traps from day one — see `docs/pitfalls.md` (TBD P1) for the full list.

The user studies-while-building. Every paper they drop gets a folder under `docs/papers/`. Every phase ends with a dated report. Every module gets a `## Specification` block (format below).

### What irene actually does (and what it doesn't)

> **Authoritative philosophy doc:** [`docs/philosophy.md`](docs/philosophy.md).
> If the rest of this plan ever contradicts that file, that file wins.

**Irene does not predict anything.** Irene reads news, confirms the
wording is decisive for a known market, and buys before the market
reprices. The edge is **time**, not insight.

The LLM's role is **natural-language understanding** — "does this
article say the FDA approved or rejected drug X?" — not probability
estimation. The `direction` + `confidence` fields on `predictions`
are interpretation outputs, not forecasts. Every downstream gate
uses them as if they were probabilities because the math is the
same, but their *meaning* is "how decisively does this news resolve
the market".

What this means for the plan:

- **Latency observability is the first-class diagnostic.** Per-stage
  p50/p95/p99 and end-to-end article→trade time matter more than
  almost anything else. The plan reserves a dashboard surface for
  this from P1 forward.
- **The impact tracker is how we know we have edge.** For every
  executed trade we poll CLOB at +1m / +5m / +15m / +30m / +60m and
  ask "did the price move our way?". Lands in P3+ alongside the
  observability port from seulgi.
- **We de-emphasise calibration.** A miscalibrated LLM lets noisy
  trades through the gate — fixable with a stricter confidence floor.
  It is not a six-month research project.
- **Short-horizon markets are our home.** Long-horizon resolution
  markets give other actors time to absorb news; we don't compete
  there.

---

## User decisions locked in (2026-05-22)

| Question | Decision |
|---|---|
| Starting point | **Start fresh.** Old irene (`122720a`) stays deleted; steal patterns only. |
| Build scope | **Minimum viable clone first.** MVP pipeline (article → embed → match → LLM-direction → paper-trade) before any DKG layer. |
| Sharing with seulgi | **Scraper HTTP service only.** Own DB, chroma, everything else. |
| Bus / messaging | **Redis pub/sub design, in-process v1 impl.** Payloads JSON-dicts only; stages stateless across messages. |
| P0 scope | **Slim** (~6 files: README, architecture, master-plan, dkg_spec, first handoff, folder placeholders). |
| Docker-compose | **Standalone only** — no root compose edits ever. |
| Variant fan-out | **DROP entirely.** Single-decision-per-headline. Comparison axis is `decision_path` (mvp vs dkg). |
| Database | **SQLite v1, Postgres P4.** Migration drafted in P4 once volume justifies it. |
| Tracer bullets | **P1 first-class deliverable.** Synthetic articles tagged via `is_tracer`/`tracer_id`; injector + checker + CI stress test. |
| Replay/backtest | **P1 first-class deliverable.** Capture tables (`decision_inputs`, `llm_calls`) inside the live `db.batch()`; replay scripts under `scripts/replay/`. |

---

## Two living documents (in-repo)

### `architecture.md` — what we have NOW

Updated at the **end of every phase** (not every PR — that aspiration drifted stale in seulgi within days; we adopt the looser cadence up front). Per-PR notes live in `changelogs/cycles.md`. Owner: whichever agent last touched a module is responsible for sections describing that module.

Required sections:
- System overview (one paragraph + ASCII data-flow).
- Modules (one row per load-bearing module; pointer to its in-file `## Specification` block).
- Schema (table list, primary indexes, writer attribution).
- External dependencies.
- Configuration knobs.
- Decision funnel drop rates (refreshed after each soak).
- Known limitations.

### `master-plan.md` — what we do NEXT

This file. The phased roadmap, the open questions, the spec-block convention. Edited freely; this is the canonical source.

---

## Modularization

The full rules are in [`docs/modularization.md`](docs/modularization.md) (user directive 2026-05-23, canonical). Every PR is held to those rules. CI enforces:

1. No cross-stage imports (`app/stages/*.py` cannot import each other).
2. JSON-round-trip on every bus payload in tests.
3. Table-ownership lint (each `INSERT`/`UPDATE` matches the table's declared owner module).

Headlines: one responsibility per module, bus is the only inter-stage channel, JSON-dict payloads only, one owner per table, no shared mutable state, dependencies injected, tests sit next to code, each module independently runnable.

## Module specification convention

Every load-bearing module gets a `## Specification` block at the top of its docstring. Format:

```
## Specification
**Purpose.** One sentence.
**Public interface.** Functions / classes other modules import.
**Invariants.** Must-hold properties.
**Side effects.** DB tables read/written, files, external APIs.
**Failure modes.** What can crash, how it recovers.
**Non-goals.** What this module is NOT responsible for.
```

Specs are written in the same PR that creates or substantially modifies the module. Any PR that changes a module's public behaviour without updating its spec is reject-on-review.

---

## Phase plan

Each phase ends with: (a) code merged, (b) a dated `reports/YYYY-MM-DD-phase-N.md`, (c) `architecture.md` updated, (d) a handoff describing what's running and what's next.

| Phase | Theme | Deliverable | Status |
|---|---|---|---|
| **P0** | Scaffold + docs | `irene/` directory tree, 5 root docs, dkg_spec verbatim, first handoff | **✓ complete 2026-05-23** |
| **P1** | MVP pipeline + tracer + replay | Dockerfile, compose entry, schema, all P1 modules, `scripts/tracer/`, `scripts/replay/accuracy.py`. Runs in parallel with seulgi. | **✓ complete 2026-05-23** ([soak report](reports/2026-05-23-irene-p1-soak.md)) |
| **P2** | Extraction layer (shadow) | `stages/extract.py` + entities/facts tables. Trade path STILL uses MVP. Reports compare extraction precision/recall on held-out samples. | **✓ complete 2026-05-23** ([extraction report](reports/2026-05-23-irene-p2-extraction.md)) |
| **P3 ch1** | Entity resolver | `stages/resolve.py` + alias-aware metric. Acronym/alias/suffix/self rule chain over the extracted entities. | **✓ complete 2026-05-23** ([resolver report](reports/2026-05-23-irene-p3-resolver.md)) |
| **P3 ch1.5** | DKG data fill | seulgi → irene one-time migration (27,914 articles + 10,462 markets); production DKG update pipeline (`scripts/dkg/extract_to_irene.py`); SBERT EntityIndex for hot-path dedup; SampledTimer for sampled critical-path latency. | **✓ complete 2026-05-24** ([migration report](reports/2026-05-23-irene-seulgi-migration.md)) |
| **P3 ch2** | Eventize + linker | `stages/eventize.py` + `stages/link.py`. Clustering algorithm locked in [`docs/papers/eventization/notes.md`](docs/papers/eventization/notes.md). Live in `wire_pipeline()`. | **✓ complete 2026-05-24** ([linker report](reports/2026-05-24-irene-link-backfill.md)) |
| **P3 ch3** | Dual-path trader + wallet + closer | `decision_path='dkg'` wired alongside `'mvp'`. Paper-trade wallet with cash + equity + identity check. Closer rules: edge_decay → take_profit → stop_loss → stagnation → timeout. Real Kelly via v6 confidence digit. Per-cycle dashboard isolation. | **✓ complete 2026-05-24** ([handoff](handoffs/2026-05-24.md)) |
| **P3 ch4** | scraper-v2 migration + HTML ingestion | Switched to `jennie-scraper-v2:9001`. `/feed` is shape-compatible; new `_run_html_loop` polls `/scrape mode=links` for 11 HTML homepages (apnews, reuters, bbc, etc.). Three-guard boilerplate filter. | **✓ complete 2026-05-24** ([handoff](handoffs/2026-05-24.md)) |
| **P3 ch5+** | Iterative cycle loop | Next session picks one of: DKG path firing, reranker on matcher top-K, /scrape mode=article body extraction, graph-driven invalidation, missed-edge review. **Implement then long-soak (4h+).** | open — see `handoffs/2026-05-24.md` §"Open follow-ups" |
| **P4** | Dual-write comparison + Postgres draft | Reports quantify mvp vs dkg per-trade-path. Postgres DDL drafted (not run). Schema-reviewer agent owns this. | sketch |
| **P5** | Cutover candidates | One non-critical read (market context) routed through DKG. Then candidate filtering. Final trade decision only after ≥5 consecutive 24h windows of DKG ≥ MVP. | TBD |

Phases are sized so each can be its own conversation. We don't pre-commit to P4+ details until P3 lands.

---

## Phase 1 — Detailed scope (next session entry point)

**Goal:** irene is making paper trades in parallel with seulgi by end of phase.

### Modules to implement (in order)

1. ~~`app/config.py` — pydantic config, hot-reload pattern stolen from old-irene `irene_shared/config.py` (git history at commit `122720a`).~~ **Done 2026-05-23.**
2. ~~`app/db.py` — schema creation, connection pool, `db.batch()` context manager, the seulgi-path assertion.~~ **Done 2026-05-23** as the *mechanism* — domain DDL is registered per-stage from each owner module (per modularization rule 4). Only `schema_migrations` (housekeeping) and `cycles` (owned by `main.py`) exist today.
3. ~~`app/bus.py` — `publish/subscribe`; v1 uses `asyncio.Queue` under the hood, v2 swaps to redis.~~ **Done 2026-05-23.** Strict JSON roundtrip; trade-path back-pressure; telemetry drop-first; per-subscriber task isolation. Also shipped: `app/topics.py` (topic name constants), `app/deps.py` (StageDeps dataclass), `app/cycle.py` (cycle ContextVar). Full payload schema list in `docs/p1_contracts.md` §2.
4. ~~`app/timing.py` — `Timer` + `ProfileStore`.~~ **Done 2026-05-23.**
5. ~~`app/budget.py` — OpenRouter budget tracking.~~ **Done 2026-05-23.** Two pause sources (monthly cap + 402 streak), pause-flag file for IPC-less status sharing.
6. ~~`app/embed.py` — sentence-transformers wrapper + embedding cache + `embedder_id` stamping.~~ **Done 2026-05-23.** Lazy model load; LRU cache; `EmbedderDimMismatch` on dim drift.
7. ~~`app/ai.py` — OpenRouter client; direction classifier; logs every call to `llm_calls`; cache lookup via `prompt_sha`.~~ **Done 2026-05-23.** JSON-suffix prompt contract; budget-paused short-circuit; 402 → `note_402` + NEUTRAL; cache via `sha1(prompt+model+version)`.
8. ~~`app/stages/markets.py` — Gamma API poller, market embeddings, in-memory `MarketIndex`.~~ **Done 2026-05-23.** `MarketIndex` is the documented rule-5 exception; asyncio.Lock guards mutation; mixed-embedder_id raises.
9. ~~`app/stages/ingest.py` — scraper poller (HTML + RSS + Twitter); writes `articles`.~~ **Done 2026-05-23** — RSS only via scraper:9000 `/feed`; Twitter + HTML deferred. Jittered per-feed polls; `INSERT OR IGNORE` + `rowcount` gate on publish.
10. ~~`app/stages/dedupe.py` — chroma-backed near-dup detection; writes `article_clusters`.~~ **Done 2026-05-23.** Freshness labels at deterministic distance + age boundaries; chromadb-missing degrades to all-BREAKING.
11. ~~`app/stages/match.py` — cosine match article → market candidates; writes `signals` (immutable; no NULL direction).~~ **Done 2026-05-23.** `MarketIndex` consumed via `StageDeps.market_index`; `embedder_id` stamped per row; `INSERT OR IGNORE` + `cur.rowcount` gate on publish.
12. ~~`app/stages/trade.py` — gate cascade (similarity floor → freshness → direction → edge → size); paper only; writes `trades` + `decisions`.~~ **Done 2026-05-23.** Five gates in sequence; one `decisions` row per signal regardless of outcome; `trades` only for execute; `trade_features` long-format fan-out per execute. `mid_price=0.5` stub for P1.
13. ~~`app/main.py` — boot, wire stages via bus, shutdown.~~ **Done 2026-05-23.** `wire_pipeline()` helper constructs every stage, registers subscriptions before `bus.start()`, starts in topological order, stops in reverse. Budget poll only fires when `openrouter.api_key` is set.
14. ~~`Dockerfile`, `requirements.txt`, `docker-compose.yml`~~ — done in the first P1 PR.
12. `app/stages/trade.py` — gate cascade (similarity floor → freshness → direction → edge → size); paper only; writes `trades` + `decisions`.
13. `app/main.py` — boot, wire stages via bus, shutdown.
14. `Dockerfile`, `requirements.txt`, `docker-compose.yml` — container infra.

### Cross-cutting (all of these ship in P1, not later)

- Every gate writes a `decisions` row with `outcome` + `reason`.
- Every trade writes a full `trader_trace` JSON + a fan-out of `trade_features` rows.
- `cycles` writer: `main.py` calls `db.ensure_current_cycle(name)` on boot; every decision/prediction/trade carries `cycle_id`.
- `budget.py` is wired in P1, not deferred — OpenRouter 402 / monthly cap must be respected from the first paid call.
- Runtime assertion in `db.py`: refuses to connect if path matches a known seulgi path.
- **Tracers** (`scripts/tracer/fire.py` + `scripts/tracer/check.py`) — synthetic articles travel the real pipeline; nightly liveness probe; CI stress test (1000 bullets / concurrency 50, 0 drops).
- **Capture** — `decision_inputs` and `llm_calls` writes share `db.batch()` with the live write. Capture cannot lag.
- **Replay accuracy harness** — `scripts/replay/accuracy.py` reconstructs `pnl_usdc` from `market_price_snaps` to ±1¢. This is the P1 ship gate.

### P1 verification (ship gate)

- `docker compose -f irene/docker-compose.yml up` boots and runs cleanly for 1+ hour.
- `irene.db` accumulates rows in `articles`, `markets`, `signals`, `predictions`, `trades`, `decisions`, `decision_inputs`, `llm_calls`.
- **Tracer gate:** `scripts/tracer/fire.py --count 1000 --concurrency 50` shows 0 dropped non-telemetry bullets.
- **Accuracy gate:** `scripts/replay/accuracy.py` reconstructs every closed paper trade's `pnl_usdc` within ±1¢.
- **Soak report:** `reports/YYYY-MM-DD-irene-p1-soak.md` compares first-hour numbers vs seulgi's first-hour: # articles ingested, # signals matched, # trades attempted, neutral-rate, # tracer drops (0), # capture rows vs # live rows (1:1).

---

## Phase 2+ (sketches only)

**P2 — Extraction (shadow).** `stages/extract.py` does structured-output LLM call per article, ontology-constrained (small fixed entity-type + relation list defined in `docs/ontology.md` when written). Writes `entities`, `entity_aliases`, `extraction_runs`, `facts`. **The trade path NEVER reads from `facts` in P2.** P2 is data accumulation. End-of-P2 report quantifies extraction precision/recall on a held-out sample and decides whether P3 can proceed.

**P3 — Event + linker.** Three open research problems must be answered in a dedicated research session at the start of P3 before any code:
1. **Eventization clustering** — facts → canonical events needs a similarity metric, time window, and tie-break rule. Research deliverable: `docs/papers/eventization/notes.md`.
2. **`directness_score` and friends** — each component of the DKG scoring formula needs a real implementation. If a component is provably uniform on real data, drop it from the formula rather than carry it as noise.
3. **Dual-path trader risk** — irene v1 ships MVP-only. P3 introduces `decision_path='dkg'` side-by-side. The failure mode to measure: DKG path makes the same trades with extra steps. P3 report quantifies overlap; if >85% with no PnL lift, kill the DKG path.

**P4 — Dual-write comparison + Postgres draft.** Reports quantify mvp vs dkg per-trade-path. Postgres DDL drafted (not run); schema-reviewer agent owns the SQLite-vs-Postgres diff.

**P5 — Cutover candidates.** Per DKG spec — start with one non-critical read (market context), then candidate filtering, then full trade decision only after sustained parity (operationalised: 5+ consecutive 24h windows where DKG PnL ≥ MVP PnL within noise band). Live trading still out of scope.

---

## Open questions

(See also `questions/design_questions.md` when written.)

1. **News freshness store** — separate chroma at `irene/data/news_chroma/` (current plan) vs lighter weight (hash-based) dedup. Doubled embedding writes are the cost; isolation from seulgi's chroma is the benefit. Cost grows with corpus size; revisit in P2.
2. **LLM key sharing** — current plan is shared `OPENROUTER_API_KEY` env var, separate budget tracking. Revisit if either bot hits the monthly cap and we need per-bot pre-allocation.
3. **Tracer template ontology** — what event-shapes should `scripts/tracer/templates/` cover? P1 starts with `regulatory_decision.yaml`, `merger.yaml`, `economic_indicator.yaml`; P2 adds extraction-targeted shapes (entity-rich vs entity-sparse).
4. **DKG scoring formula** — directness/novelty/resolution_fit/deadline_fit/evidence/confirmation components need concrete implementations. Research in P3.

---

## Subagent usage discipline

**Per-phase pattern:**

1. **Start of phase** — 1–3 parallel Explore agents load context (what seulgi does, what the DKG spec says, what irene already has).
2. **Design step** — Plan agent drafts the implementation strategy; for high-stakes design (P3), launch a second independent Plan agent for adversarial review.
3. **Build step** — write code in foreground. Spawn general-purpose agents only for independent parallel one-shots.
4. **End of phase** — three specialised agents:
   - **Diff-checklist agent (Explore):** audits the diff against the deliverables checklist.
   - **Schema-reviewer agent (Explore):** flags missing indexes, JSON casts, Postgres-incompatible types. Especially at P4.
   - **Data-shape audit agent (Explore):** reads `irene.db` after the first soak and reports row counts vs expected ratios.
5. **Every ~2 phases** — **doc-drift agent (Explore):** grep-tests doc claims against code. Output: `reports/YYYY-MM-DD-doc-drift.md`.

Never delegate understanding. The plan, the verification, the phase report — those stay in the main thread.

---

## Hard rules (replicated from README so this doc stands alone)

1. **No DB sharing with seulgi.** Runtime assertion in `db.py`.
2. **Standalone docker-compose only.** Root compose untouched.
3. **Single-decision-per-headline.** No variant fan-out in v1.
4. **Tracers and live trades cannot mix.** `is_tracer=1 AND paper=0` is a hard write rejection.
5. **Capture lives inside the live batch.** `decision_inputs` and `llm_calls` writes share `db.batch()`.
6. **(2026-05-24) Implement-then-soak-long.** Don't ship multiple changes mid-soak. Land a cycle, restart, soak ≥4h, then re-plan from the data — not from the previous plan.
7. **(2026-05-24) Per-cycle data hygiene.** `articles`/`markets`/`entities`/`facts`/`events`/`signals`/`market_links` are SHARED across cycles (news universe). `trades`/`decisions`/`predictions`/`trade_closures`/`llm_calls`/`llm_parse_failures`/`decision_inputs`/`trade_features` are PER-CYCLE; snapshot via `scripts/analysis/snapshot_cycle.py` before recycling.

---

## Cycle history (chunk-3 onwards — iterative loop)

Each row = one implementation cycle the user explicitly scoped. Detail in commit messages; full state-of-system snapshot at the start of every session in `handoffs/`.

| Date | Cycle | Headline change | Soak result |
|---|---|---|---|
| 2026-05-24 | chunk-3 base | Dual-path trader (`decision_path='dkg'`), live eventize+link in `wire_pipeline()`, dashboard PnL page. | 0 trades — ingest stage was broken (GET instead of POST on /feed). |
| 2026-05-24 | follow-up #1 | Ingest GET→POST, `MarketIndex.load()` on start (silent "irene never traded" bug), eventize COALESCE for null published_at. Dashboard observability (AI panel, decisions audit, fewer files). | 18 trades fired, all stuck open (CLOB integration broken — `/book/<id>` 404s). |
| 2026-05-24 | follow-up #2 | Prompt v4 (JSON envelope), matcher tightened (0.5 floor / top-K 8), Tighten title+summary embed, chunker tool. | 350 decisions, parser bug discovered: `{"decision":"NEUTRAL"}` misread as NO. |
| 2026-05-24 | cycle 1 | v5 5-label taxonomy (Y/N/P/U/R), entity-overlap stage-0 gate. | Healthy label spread (Y 2, N 40, P 9, U 21, R 10 in 82). |
| 2026-05-24 | cycle 2 | Wallet (cash/equity/identity), closer stop/target rules, cash-aware sizing, dashboard wallet panel. | Cash went negative immediately (cycle-1 over-allocation), exposed CLOB bug — all trades at 0.5 stub. |
| 2026-05-24 | CLOB fix | `/midpoint?token_id=<id>` is the real endpoint, not `/book/<id>`. | 14 trades closed in seconds; 5 target / 9 stop / -$600 net (cycle-1 mess unwound). |
| 2026-05-24 | cycle 3 | Parser bug fix (strict-match), `target_mid` + `edge_decay` rule, real Kelly via `c` digit, cash clips instead of rejects, 15s poll, stagnation rule, per-cycle dashboard isolation, missed-edge plot. | `cycle3-fresh` boots clean (cycle_id=20). |
| 2026-05-24 | target_mid fix | NO trades had a double-flip sign bug → `target = p_hat + fee_buffer` (was `(1-p_hat) + fee_buffer`). | Locked in by regression test. |
| 2026-05-24 | snapshot tool | `scripts/analysis/snapshot_cycle.py` archives per-cycle data to `data/snapshots/cycle_<id>/`. | cycle_0003 snapshot baseline: equity $435, realized -$565. |
| 2026-05-24 | chunk-4 | scraper-v2 migration — `/feed` shape-compatible; new HTML watcher via `/scrape mode=links` for 11 homepages. Three-guard boilerplate filter. | Live: 316 HTML + 1266 RSS articles in first 30 min (~10× cycle-1 ingest rate). |

**380 tests pass.** See `handoffs/2026-05-24.md` for the open-follow-up list and the implement-then-soak-long cadence the user wants for cycle 5+.
