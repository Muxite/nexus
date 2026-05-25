# Irene architecture — what we have NOW

> This is the **current-state** doc. For next steps, see `master-plan.md`.
>
> **Discipline.** Updated at the end of every phase (not every PR — that aspiration kills momentum; per-PR notes go to `changelogs/cycles.md` instead).

- **Last updated:** 2026-05-25 (**cycles 4-7 shipped: accounting fixes, scraper-v2 combined mode, ingest expansion, fees-in-PnL, market-news weight gate**)
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

## Decision funnel (live as of cycle 7, 2026-05-25)

Every headline runs the cascade. Each gate emits a `gate_history` JSON entry; the `decisions` table records every outcome with `reason` so the funnel is queryable per cycle.

```
ingest      → article row (sources: rss / html / combined / social-future)
              scraper-v2: combined mode = parallel RSS+HTML merge by canonical URL
              article body fan-out enriches top-N HTML-only links
dedupe      → cluster_id + freshness label (chroma cosine):
              BREAKING (nearest_dist > breaking_distance)
              EVOLVING (< 6h since cluster's first)
              REHASH (6-24h)  STALE_RETURN (> 24h)
markets     → gamma poll; markets table; CLOB token_yes/token_no
match       → signals = (article × top-K markets above similarity floor)

trade gate cascade (in order; first reject wins):
  1. similarity_floor (min_similarity = 0.35)
  2. freshness         (accept_freshness = {BREAKING, EVOLVING})
  2.5 entity_overlap   (LLM-extracted entities must appear in market question)
  3. direction         (LLM Y/N/P/U/R label + confidence digit ≥ min_confidence)
  4. mid_degenerate    (mid > min_mid AND mid < max_mid; default 0.02/0.98)
  4.b market_news_imbalance  ← cycle 7
       (a) thin_market: volume_usdc < $10k        (defensive: skipped on 0/NULL)
       (b) extreme_mid_no_breaking: mid in [0.15,0.85]ᶜ AND freshness != BREAKING
       (c) far_future_no_breaking: days_to_expiry > 180 AND freshness != BREAKING
       (d) near_expiry: days_to_expiry < 1
       (e) singleton_on_extreme_mid: cluster_size = 1 AND mid in extreme zone
  5. edge        (edge = |p_hat - mid| - fee_buffer ≥ min_edge=0.04)
  6. size        (Kelly-fraction sized; ≥ min_position_usdc and ≤ max_position_fraction × balance)
  7. cash        (clips to available cash; reject only if clipped < min_position_usdc)
  → trade row + 'execute' decision

closer (background poll every cfg.closer.poll_seconds):
  edge_decay → take_profit (cfg.closer.take_profit_pct) → stop_loss → stagnation → timeout
  exit_mid from CLOB get_mid; on None and skip_on_missing_mid=True: hold
  PnL via _compute_gross_pnl - round-trip Polymarket fee  ← cycle 7

markets-stage:
  _deactivate_missing flags Gamma-dropped markets active=0
  auto-closes orphaned trades on those markets with                     ← cycle 7
    close_reason='market_resolved'  exit_mid=entry_mid  PnL = -(fee)
```

### Polymarket fee model (cycle 7)

Per-leg taker fee folded into `realized_pnl_usdc` at close:

```
fee_usdc = shares * fee_rate * p * (1 - p)
```

where `p` = share price, `shares = size_usdc / position_price`. Maker = 0, settlement = 0.

| category | fee_rate | peak fee /100 shares at p=0.5 |
|---|---|---|
| crypto | 0.072 | $1.80 |
| economics/culture/weather/general | 0.05 | $1.25 |
| finance/politics/tech/mentions | 0.04 | $1.00 |
| sports | 0.03 | $0.75 |
| geopolitics | 0.00 | $0 (free) |

`trade_closures` carries `entry_fee_usdc` + `exit_fee_usdc` as audit columns; they're already folded into the net `realized_pnl_usdc`.

---

## Known limitations (2026-05-25)

- **`markets.volume_usdc` is 0 / NULL for every row.** Gamma returns it (e.g. Rihanna market: $750k) but the upsert path persists 0. Parser at `app/stages/markets.py:_parse_gamma_row:184` looks correct — needs a focused debug. Cycle 7's `thin_market` gate sub-rule is defensive and skipped when the field is empty, so it's a no-op until fixed.
- **`markets.category` is NULL for every row.** Gamma's top-level response doesn't have a `category` key; it has `feeType` ("general_fees", "politics_fees", etc.) plus a `tags` array. We need to derive category from `feeType` so the Polymarket fee resolver hits the real tier. Until then, default fee_rate=0.05 applies to every market.
- **Market-resolved auto-close settles at entry_mid, not the actual resolution.** Gross PnL = 0, net PnL = -(round-trip fee). The smaller hit. Real resolution requires a second Gamma call against `closed=true` for `outcomePrices`; ~20 lines, deferred.
- **DKG path lacks the cycle-7 market_news_imbalance gate.** DKG already has the cycle-4 `mid_degenerate` gate via Fix 7 / cycle 5.1, but the new market-weight check is MVP-only for now. DKG fires rarely (final_score ≥ 0.8 condition); low priority but worth porting for parity.
- **LLM R-rate stays around 70-80%.** Phase F (prompt fix) is queued but hasn't shipped — cycle-5/5.1 already dropped R from 87% via richer summaries. Re-evaluate if R climbs back.
- **No real-CLOB soak yet.** `trade.py` calls `deps.clob.get_mid()` for live mid; `main.py` wires a real `CLOBClient` against `https://clob.polymarket.com`. Cycle 29 hit it for real. The closer also hits it. We've seen real responses + the "token goes dark on resolution" path (Trade #37 motivated the cycle-7 auto-close fix).
- **Dedupe degrades to all-BREAKING when chromadb is missing.** Intentional dev fallback.
- **DKG layers (P2/P3)** scaffolding is live (extract / resolve / eventize / link stages); the link's `directness_score / novelty_score / deadline_fit / resolution_fit` are equally weighted today. DKG path fires when `market_links.final_score >= 0.8` — rare under live data.

---

## Hard rules baked into the design

(See `README.md` §"Hard rules" for the full list — replicated here so this doc stands alone.)

1. No DB sharing with seulgi (runtime assertion in `db.py`).
2. Standalone docker-compose only (no edits to root compose).
3. Single-decision-per-headline (no variant fan-out in v1).
4. Tracers and live trades cannot mix (`is_tracer=1 AND paper=0` is a hard write rejection).
5. Capture (`decision_inputs`, `llm_calls`) lives inside the live `db.batch()` — never lags.
6. **(2026-05-24)** Implement-then-soak-long. Don't ship multiple changes mid-soak.
7. **(2026-05-24)** Per-cycle data hygiene. `articles`/`markets`/`entities`/`facts`/`events`/`signals`/`market_links` are SHARED across cycles. `trades`/`decisions`/`predictions`/`trade_closures`/`llm_calls`/`llm_parse_failures` are PER-CYCLE.
8. **(2026-05-25, cycle 7)** PnL is NET of fees. `realized_pnl_usdc` already includes the round-trip Polymarket taker fee; `entry_fee_usdc` + `exit_fee_usdc` on `trade_closures` are audit-only.
9. **(2026-05-25, cycle 7)** Auto-close on market deactivation. `markets._deactivate_missing` writes `close_reason='market_resolved'` rows for orphaned trades — no trade should be held indefinitely on a resolved market.
