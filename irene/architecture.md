# Irene architecture — what we have NOW

> This is the **current-state** doc. For next steps, see `master-plan.md`.
> For the *why* behind the gates and math, see [`docs/theory.md`](docs/theory.md).
> For research questions whose answers come from external sources, see
> [`questions/research_questions.md`](questions/research_questions.md).
>
> **Discipline.** Updated at the end of every phase (not every PR — that aspiration kills momentum; per-PR notes go to `changelogs/cycles.md` instead).

- **Last updated:** 2026-05-25 (**cycles 4–7 shipped: accounting fixes, scraper-v2 combined mode, ingest expansion, fees-in-PnL, market-news weight gate**)
- **Synthesized from:** the approved plan, the seulgi audit reports, the user's DKG spec at `docs/dkg_spec.md`, the irene ontology at `docs/ontology.md`, and the P3 contracts at `docs/p3_contracts.md`.
- **Status:** P1 + P2 sealed. **P3 chunks 1 + 2 + 3 landed.** Live cycle `cycle8-quality` (cycle_id=31). 390 tests green. Wallet identity OK. Cycle 7 added Polymarket parabolic taker fees to net PnL, auto-close on market deactivation, and the `market_news_imbalance` gate (5 sub-rules). DKG path is wired but fires rarely under live data; MVP path is the dominant fire path.

---

## System overview

Irene is a paper-trading bot for Polymarket designed to run **in parallel with seulgi** on the same scraper inputs but with completely separate state. The pipeline is `article → dedupe → match → predict → paper-trade`, with the DKG layers (extract → resolve → eventize → link) attached as a parallel two-hop path. The comparison axis between irene's two trade paths (MVP vs DKG) is `trades.decision_path`. The thesis is **latency arbitrage**, not forecasting — see [`docs/philosophy.md`](docs/philosophy.md).

```
live shape (cycle 31)
═════════════════════

   ┌─────────────────────────────┐
   │  nexus-ujin:9001     │  shared HTTP, ~120 sources (RSS + combined HTML + sitemap_news)
   │   /feed  /scrape  /healthz  │
   └──────────────┬──────────────┘
                  │ jittered poll
                  ▼
   ┌──────────────────────────────────────────────────────────────────────┐
   │  irene single-process app  (irene/data/irene.db, irene/data/chroma/) │
   │                                                                       │
   │   ingest    ──article.ingested──▶ dedupe   ──article.deduped──▶ match │
   │     │                              │    (Chroma similarity →           │
   │     │                              │     BREAKING/EVOLVING/REHASH/     │
   │     │                              │     STALE_RETURN label)           │
   │     │                              │                                   │
   │     │                              └──article.deduped──▶ extract (P2) │
   │     │                                                       │          │
   │     │                                                       ▼          │
   │     │                                          resolve ─▶ eventize ─▶ link
   │     │                                                                   │
   │     ▼                                                                   ▼
   │   articles                       market_links.updated ───────▶ dkg_trade │
   │                                                                          │
   │   match  ──signal.created──▶ trade ──┬──▶ ai.direction (LLM Y/N/P/U/R)  │
   │                                       └──▶ gate cascade ─▶ trades + decisions
   │                                                                          │
   │   markets (Gamma poll loop) ── MarketIndex (in-memory cosine top-K)      │
   │       │                                                                   │
   │       └──▶ markets table + clob_token_ids                                 │
   │                                                                           │
   │   closer  (background poll every 15s)                                     │
   │       │   reads open trades, queries CLOB for exit_mid,                   │
   │       │   evaluates exit rules (edge_decay/take_profit/stop_loss/         │
   │       │   stagnation/timeout), writes trade_closures with                 │
   │       │   round-trip taker fees folded into realized_pnl_usdc             │
   │       ▼                                                                   │
   │   trade_closures                                                          │
   └──────────────────────────────────────────────────────────────────────────┘
            │                                              │
            ▼                                              ▼
   gamma-api.polymarket.com                       clob.polymarket.com
   (market discovery + metadata)                  (mid prices, token IDs)
            │                                              │
            ▼                                              ▼
              openrouter.ai/api/v1  (LLM: google/gemini-2.5-flash)
              budget polling, 402 streak → pause flag → NEUTRAL short-circuit
```

The bus (`app/bus.py`) is in-process JSON-roundtrip; Redis v2 is on the future roadmap with the same interface. Per-cycle data is isolated via `cycle_id` stamped on every row; shared embeddings (articles, markets) survive cycle restarts.

---

## Modules

Every load-bearing module gets a `## Specification` block at the top of its docstring (Purpose / Public interface / Invariants / Side effects / Failure modes / Non-goals). The convention is lifted from seulgi (`seulgi/master-plan.md` §"Module specifications"). Any PR that changes module behaviour without updating its spec block is reject-on-review.

**All modules also follow the eight modularization rules in [`docs/modularization.md`](docs/modularization.md)** (user directive 2026-05-23). The headlines: one responsibility per module; the bus is the only inter-stage channel; JSON-dict payloads only; tables have one owner stage; no shared mutable state across messages; dependencies injected, never imported; tests sit next to code; each module is independently runnable. CI lints against violations.

| Module | Phase | Status |
|---|---|---|
| `app/main.py` | P1 | **fully wired** — constructs every resource, wires bus subscriptions, runs to SIGTERM, stops in reverse. Both `StageDeps` shapes (base + with `market_index`). |
| `app/config.py` | P1 | **live** — pydantic `Config`, hot-reload `ConfigManager`, `IRENE__*` env overrides, `IRENE_CYCLE` bare-var. |
| `app/db.py` | P1 | **live** — `DB.connect/close/batch` (batch serialised by `asyncio.Lock`), seulgi-path assertion, `register_schema()` registry, `db.capture(cur, …)`. Owns `schema_migrations`, `decision_inputs`. |
| `app/topics.py` | P1 | **live** — bus topic constants + `is_telemetry()` classifier. |
| `app/cycle.py` | P1 | **live** — `ContextVar` cycle context; `current_cycle_id()` / `set_cycle_id()` / `cycle_scope()`. |
| `app/deps.py` | P1 | **live** — frozen `StageDeps` dataclass (the injection contract). |
| `app/bus.py` | P1 | **live** — `Bus` protocol + `InProcBus`. JSON-roundtrip strict mode, trade-path back-pressure, telemetry drop-first, per-subscriber task isolation. |
| `app/timing.py` | P1 | **live** — `Timer` context manager + `ProfileStore` rolling window + percentiles. |
| `app/embed.py` | P1 | **live** — sentence-transformers wrapper, lazy load, LRU cache, `embedder_id` stamping, `EmbedderDimMismatch`. |
| `app/budget.py` | P1 | **live** — OpenRouter `/key` poll loop + 402-streak watcher; pause flag file written on edge transitions. |
| `app/ai.py` | P1 | **live** — `AIClient.direction()` (Y/N/P/U/R + confidence digit). Owns `llm_calls`, `predictions`, `llm_parse_failures`. JSON-suffix prompt contract, `prompt_sha` cache, budget-paused short-circuit to NEUTRAL, 402 → `note_402` + NEUTRAL. |
| `app/clob.py` | P1 | **live** — Polymarket CLOB mid-price client. Pure utility; no tables. Lazy `httpx.AsyncClient`; recoverable errors → `None`. |
| `app/stages/markets.py` | P1 | **live** — Gamma poll loop + embedding refresh loop. `MarketIndex` (documented rule-5 exception) for cosine top-k. Owns `markets`. Also runs `_deactivate_missing` and triggers cycle-7 auto-close on resolved markets. |
| `app/stages/ingest.py` | P1 | **live** — scraper-v2 `/feed` and `/scrape` (mode=combined, cycle 5) poller, jittered. Owns `articles`. |
| `app/stages/dedupe.py` | P1 | **live** — Chroma cosine → freshness label. Chromadb-missing degrades to all-BREAKING. Owns `article_clusters` + `article_cluster_members`. |
| `app/stages/match.py` | P1 | **live** — embeds article title, `MarketIndex.topk(cosine, floor=0.35)`. Owns immutable `signals`. No `direction` column. |
| `app/stages/trade.py` | P1 | **live** — MVP path. Subscribes to `signal.created`, runs the 9-gate cascade (incl. cycle-7 `market_news_imbalance`), writes `decisions`, `trades`, `trade_features`. Uses real CLOB mid in `main.py`; 0.5 stub in tests. |
| `app/stages/closer.py` | P1 | **live** — background poller. Reads open trades, evaluates close rules (edge_decay → take_profit → stop_loss → stagnation → timeout). Owns `trade_closures`. Cycle 7: round-trip Polymarket taker fees folded into `realized_pnl_usdc`. |
| `app/stages/extract.py` | P2 | **live** — subscribes to `article.deduped`. Ontology-constrained LLM call. Owns `entities`, `entity_aliases`, `extraction_runs`, `facts`. Independent HTTP path (shares `deps.budget`). |
| `app/stages/resolve.py` | P3 ch.1 | **live** — deterministic acronym → alias → suffix → self rule chain. Owns `entity_resolutions` overlay (facts/entities immutable). `data/acronyms.yaml` (37 rows). |
| `app/stages/eventize.py` | P3 ch.2a | **live (wired)** — hybrid entity-overlap + cosine-≥-0.75 clustering on templated triples; per-type window from `data/event_types.yaml`. Owns `events`, `event_edges`; column-adds `facts.event_id`. |
| `app/stages/link.py` | P3 ch.2b | **live (wired)** — scores events × active markets on `directness`/`novelty`/`deadline_fit`/`resolution_fit` (arithmetic mean), persists above `CANDIDATE_FLOOR=0.20`. Owns `market_links`. |
| `app/stages/dkg_trade.py` | P3 ch.3 | **live** — DKG trade path. Subscribes to `market_links.updated`, fires when `final_score ≥ 0.8`. Re-uses `ai.direction()`, runs a reduced gate cascade (no similarity/freshness; has `mid_degenerate` since cycle-5.1 Fix 7; lacks cycle-7 `market_news_imbalance` — see Known limitations). Writes `trades` with `decision_path='dkg'`. |
| `scripts/lint/no_facts_in_trade_path.py` | P2 | **live** — mechanically enforces the hard invariant that `ai.py` / `trade.py` / `match.py` / `closer.py` never read from extraction tables. Word-boundary patterns + opt-in `# lint: allow-extraction-table-ref` escape. |
| `scripts/eval/extraction_quality.py` + `gold_set/v1.yaml` | P2 | **live** — 18 Reuters-style gold articles; per-relation precision/recall. Mock + `--real-llm` modes. Median P/R = 0.71/0.65 across 3 real-LLM runs. |
| `scripts/tracer/fire.py` + `check.py` + `templates/*.yaml` | P1 | **live** — synthetic-article injection. Ship-gate: 1000 tracers / concurrency 50, 0 drops. |
| `scripts/replay/accuracy.py` | P1 | **live** — reconstructs `realized_pnl_usdc` from `(trades JOIN trade_closures)`. Ship-gate verified at 3000/3000 within ±$0.01. |
| `scripts/soak/run.py` | P1 | **live** — orchestrator: tracer (with closer enabled) → replay accuracy gate. |

---

## Schema — tables by owner

The schema is no longer "target" — every table below is live. The columns marked **per-cycle** are reset / snapshotted on cycle restart; the **shared** ones survive across cycles (so an article doesn't get re-embedded every cycle).

| Table | Owner stage | Scope | Immutable? | Downstream consumers |
|---|---|---|---|---|
| `articles` | `ingest.py` | **shared** | yes (append-only) | `dedupe`, `match`, `extract` |
| `article_clusters` | `dedupe.py` | **shared** | yes (cluster_id immutable; member rows append) | `match` (via freshness label) |
| `article_cluster_members` | `dedupe.py` | **shared** | yes | `dedupe` (centroid recompute) |
| `markets` | `markets.py` | **shared** | mutable (price/active/category fields refreshed) | `match` (via `MarketIndex`), `trade`, `closer` |
| `signals` | `match.py` | **shared** | yes | `trade`, `decision_inputs` |
| `predictions` | `ai.py` | **per-cycle** | yes | `trade` (via cache lookup by `prompt_sha`) |
| `decisions` | `trade.py` / `dkg_trade.py` | **per-cycle** | yes | dashboard / funnel reports |
| `trades` | `trade.py` / `dkg_trade.py` | **per-cycle** | yes | `closer`, replay |
| `trade_features` | `trade.py` | **per-cycle** | yes | postmortems |
| `trade_closures` | `closer.py` / `markets.py` (auto-close) | **per-cycle** | yes (UNIQUE on `trade_id`) | wallet accounting |
| `cycles` | `main.py` | shared | mutable (active flag) | every row in every per-cycle table joins here |
| `decision_inputs` | `db.capture()` (called from `trade.py`) | **per-cycle** | yes | replay |
| `llm_calls` | `ai.py` | **per-cycle** | yes | cost / cache analytics |
| `llm_parse_failures` | `ai.py` | **per-cycle** | yes | LLM quality reports |
| `entities`, `entity_aliases`, `extraction_runs`, `facts` | `extract.py` | **shared** | yes (facts immutable; entity rows append) | `resolve`, `eventize` |
| `entity_resolutions` | `resolve.py` | **shared** | yes (overlay; doesn't mutate facts) | `eventize` |
| `events`, `event_edges` | `eventize.py` | **shared** | yes | `link` |
| `market_links` | `link.py` | **shared** | yes (top-K per event) | `dkg_trade` |
| `dkg_trade_links` | `dkg_trade.py` | **per-cycle** | yes (idempotency) | dashboards |
| `predictions_replay`, `trades_replay` | `scripts/replay/*` | scratch | yes (per `replay_id`) | counterfactual reports |

Every traveller-of-the-pipeline table also carries `is_tracer` (INT NOT NULL DEFAULT 0) and `tracer_id` (TEXT) so synthetic bullets are split from real data via a partial index.

Schema DDL is owned by each stage's `register_schema()` call; the registry lives in `app/db.py`.

---

## External dependencies — endpoints, rate limits, failure modes

| Service | Use | Endpoints | Rate-limit behaviour | Failure mode | Retry / fallback |
|---|---|---|---|---|---|
| `nexus-ujin:9001` | News ingestion | `/feed?source=…`, `/scrape?source=…&mode=combined`, `/healthz` | none documented; we jitter per-feed | HTTP error / timeout | logged + skipped; next poll picks up |
| `gamma-api.polymarket.com` | Market discovery + metadata | `/markets?active=true&closed=false&limit=…&offset=…` | not formally documented (`research_questions.md#B4`) | 5xx / timeout / shape change | logged + per-poll skip; embeddings retained |
| `clob.polymarket.com` | Live mid prices | `/midpoint?token_id=<id>`, internally via `app/clob.py` | not formally documented | `None` mid when token is dark (post-resolution); recoverable errors → `None` | `closer.py` `skip_on_missing_mid=True` holds the trade; cycle-7 auto-close fires from `markets._deactivate_missing` instead |
| `openrouter.ai/api/v1` | LLM direction + extraction | `/chat/completions`, `/key` (budget poll) | provider-side; we use budget polling + 402-streak watcher | 402 (out of credit) → `budget_paused` flag, NEUTRAL short-circuit; parse error → `llm_parse_failures` row | NEUTRAL → trade rejected at direction gate (no false fires); call cached by `prompt_sha` on success |
| ChromaDB (local) | News freshness / dedupe | embedded library at `irene/data/news_chroma/` | n/a (local) | missing collection → all-BREAKING degrade mode | intentional dev fallback |
| sentence-transformers | Article + market embeddings | local model | n/a | model load error → app exits at boot | n/a — boot dependency |

**No shared cache** with seulgi — separate Gamma poller, separate CLOB client, separate Chroma store, separate OpenRouter budget. The only shared external dependency is `nexus-ujin:9001`, which is read-only.

---

## Configuration

`config.yaml` ships at the repo root. Hot-reload pattern stolen from old-irene (`irene_shared/config.py` at commit `122720a`). Env-var override via `IRENE__SECTION__KEY=value`, same as seulgi. The bare `IRENE_CYCLE` variable names the active cycle.

**No engine/variant fan-out in v1** (user decision 2026-05-22). The schema keeps `engine_name`/`variant_name` columns for forward-compat but only `'default'` ships.

Key tunables (see `config.yaml` for the full list):

- `cfg.match.min_similarity` (default 0.35) — Gate 1
- `cfg.dedupe.breaking_distance` (default 0.55) — freshness boundary
- `cfg.match.top_k` (default 25, was 5 pre-cycle-5; see `docs/philosophy.md`)
- `cfg.trade.min_confidence` (default 0.4) — LLM digit floor
- `cfg.trade.min_mid` / `max_mid` (default 0.02 / 0.98) — Gate 4
- `cfg.trade.min_edge` (default 0.04) — Gate 5
- `cfg.trade.kelly_fraction` (default 0.25)
- `cfg.trade.min_position_usdc` (default $25)
- `cfg.trade.max_position_fraction` (default 0.10)
- `cfg.trade.polymarket_fee_rates` (cycle-7 category table; see §"Polymarket fee model")
- `cfg.closer.poll_seconds` (default 15)
- `cfg.closer.skip_on_missing_mid` (default true)
- `cfg.ai.model` (default `google/gemini-2.5-flash`)

---

## Per-cycle lifecycle

irene operates in named **cycles**. A cycle is a continuous container run between snapshots; the philosophy is *implement-then-soak-long* — one architectural change per cycle, soak ≥ 4h, snapshot, restart, re-plan from data.

```
┌──────────────────┐
│  cycle N − 1     │  active container; live writes to per-cycle tables
└────────┬─────────┘
         │  user: snapshot
         ▼
┌──────────────────┐
│  snapshot        │  scripts/snapshot/run.py → data/snapshots/cycle_NNNN_<name>/manifest.json
│                  │   - per-table row counts (trades, decisions, predictions,
│                  │     closures, llm_calls, llm_parse_failures, decision_inputs)
│                  │   - wallet state (balance, cash, open_invested, realized_pnl,
│                  │     equity, n_trades open/closed)
│                  │   - per-decision_path breakdown
└────────┬─────────┘
         │  user: docker compose down && up; new IRENE_CYCLE=cycle_N
         ▼
┌──────────────────┐
│  cycle N         │  per-cycle tables reset under the new cycle_id;
│                  │  shared tables (articles, markets, entities, facts,
│                  │  events, signals, market_links) carry across
│                  │
│  soak ≥ 4h       │  dashboard / decisions_chunks / pnl.txt monitored on the hour;
│                  │  no architectural changes during soak (cycle-4 directive)
└────────┬─────────┘
         │  re-plan from data: pick from open follow-ups; implement; repeat.
         ▼
       (cycle N + 1)
```

Live artifacts during a cycle (`data/dashboard/`):

- `dashboard.txt` — ingestion/trading summary (1h window), AI subsystem stats, wallet status, last 5 articles, last 5 decisions with gate trace.
- `pnl.txt` — realized PnL per `decision_path`; closed trades only; cycle-filtered.
- `funnel.txt` — outcome distribution per gate.
- `recent_trades.txt` — open trades with entry/exit targets.
- `throughput.txt` — rows/min per stage, latency p1/p50/mean/p95/p99, CLT stats (n, σ, SE, 95% CI, required-n for ±10%/±1% accuracy).
- `plots/throughput_overlay.png`, `plots/speed_overlay.png` — log-y overlays.
- `plots/missed_edge/trade_<id>.png` — per-trade entry-vs-exit-mid traces.
- `decisions_chunks/chunk_NNN.txt` — 25 decisions per file (queryable audit trail).

The cycle-history table in [`master-plan.md`](master-plan.md) records every cycle's headline + outcome. Cycle 7 (`11f8819`) is the latest landed code; cycle 31 (`cycle8-quality`) is the soaking-now container.

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

For the *why* behind each gate, see [`docs/theory.md`](docs/theory.md) §5.

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

`trade_closures` carries `entry_fee_usdc` + `exit_fee_usdc` as audit columns; they're already folded into the net `realized_pnl_usdc`. For the derivation, see [`docs/theory.md`](docs/theory.md) §2. For the open question of whether the table is sourced correctly, see [`questions/research_questions.md`](questions/research_questions.md) (O-A1, O-A2 online; i-A2, i-B3 internal).

---

## Known limitations (2026-05-25)

- **`markets.volume_usdc` is 0 / NULL for every row.** Gamma returns it (e.g. Rihanna market: $750k) but the upsert path persists 0. Parser at `app/stages/markets.py:_parse_gamma_row:184` looks correct — needs a focused debug. Cycle 7's `thin_market` gate sub-rule is defensive and skipped when the field is empty, so it's a no-op until fixed. → `research_questions.md` i-B1 + O-B1.
- **`markets.category` is NULL for every row.** Gamma's top-level response doesn't have a `category` key; it has `feeType` ("general_fees", "politics_fees", etc.) plus a `tags` array. We need to derive category from `feeType` so the Polymarket fee resolver hits the real tier. Until then, default fee_rate=0.05 applies to every market. → `research_questions.md` i-B3 + O-A2, O-B3.
- **Market-resolved auto-close settles at entry_mid, not the actual resolution.** Gross PnL = 0, net PnL = -(round-trip fee). The smaller hit. Real resolution requires a second Gamma call against `closed=true` for `outcomePrices`; ~20 lines, deferred. → `research_questions.md` i-B2 + O-B2, O-D1.
- **DKG path lacks the cycle-7 market_news_imbalance gate.** DKG already has the cycle-4 `mid_degenerate` gate via Fix 7 / cycle 5.1, but the new market-weight check is MVP-only for now. DKG fires rarely (final_score ≥ 0.8 condition); low priority but worth porting for parity.
- **LLM R-rate stays around 70–80%.** Phase F (prompt fix) is queued but hasn't shipped — cycle-5/5.1 already dropped R from 87% via richer summaries. Re-evaluate if R climbs back. → `research_questions.md` i-E2, i-H2 + O-H1, O-H2.
- **No real-CLOB soak yet at scale.** `trade.py` calls `deps.clob.get_mid()` for live mid; `main.py` wires a real `CLOBClient` against `https://clob.polymarket.com`. Cycle 29 hit it for real. The closer also hits it. We've seen real responses + the "token goes dark on resolution" path (Trade #37 motivated the cycle-7 auto-close fix).
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
