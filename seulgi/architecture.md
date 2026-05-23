# seulgi architecture — what we have NOW

> This is the **current-state** doc. For next steps, see `master-plan.md`.
>
> **Discipline.** Every PR that changes module behavior MUST update this
> file in the same commit. Owner: whichever agent last touched the module.

- **Last updated:** 2026-05-22 23:35 UTC (after Phase 3)
- **Synthesized from:** the 12 Phase-0 reports under
  `seulgi/reports/2026-05-22-*.md`, in-file `## Specification`
  blocks, live `data/seulgi.db`, `data/dashboard/dashboard.txt`,
  `seulgi/config.yaml`, and `git log`.
- **Status:** Phase 0 ✅, Phase 1 ✅, Phase 2 ✅, Phase 3 ✅ (sim correctness
  confirmed; data gap blocks variant validation). Phase 4 GO with conditions.
- **Parallel sibling: `irene/`** — knowledge-graph rebuild started in a
  separate session on 2026-05-22. Shares the reusable surface
  (`simulate.py`, pure trader functions, db schema, `backtest.py`,
  `calibration_prototype.py`, `impact_tracker.py`). Diverges on
  matching: irene replaces `markets.py:find_matches` + `news_memory.py`'s
  cosine/HNSW with a knowledge graph. Merge point is Phase 4E.2 — the
  backtest harness compares the cosine, cross-encoder, learned-MLP, and
  knowledge-graph matchers offline before any live promotion.

---

## System overview

Seulgi is a paper-trading bot that reads ~70 news endpoints, embeds each
incoming headline with SBERT, matches it against an in-memory index of
~10.1k Polymarket markets, asks Gemini (via OpenRouter) for a directional
forecast on each surviving market, and — for every active engine variant
— gates the forecast through a per-engine filter stack before writing a
simulated `trades` row + `positions` row. A monitor loop ticks every 60 s
to close on TP / SL / TRAIL / TIMEOUT / RESOLVE / cold-cull. Everything
is offline-replayable in principle but the Phase-5 capture (decision
inputs + LLM raw text) was scheduled today and is **not wired** yet —
the tables exist (FIX-3, commit `1b606c3`) but `engine.py` / `ai.py` do
not call `db.insert_decision_input` or `db.log_llm_call`. Cycle-4 status
on 2026-05-22 EOD: live paper trades flow (1 open position, the
container has been up 13 m at snapshot time); Phase-5 wiring is queued
for Phase 2 A2.1.

```
                              ┌────────────────────────────────────────────────┐
                              │ Polymarket Gamma (4h refresh, ≤ offset 10100) │
                              └─────────────────┬──────────────────────────────┘
                                                ▼
   news watchers ─► watcher.py ──► main.headline_pump ──► engine.process(headline)
   (~70 endpoints,                                          │
    1 RSS, 1 Truth                                          │
    Social)                                                 ▼
                                              ┌── embed (SBERT all-MiniLM-L6-v2)
                                              ├── news_memory.assess (chromadb HNSW)
                                              │     → BREAKING / EVOLVING / REHASH / STALE_RETURN
                                              ├── markets.find_matches (cosine, top-K=25, sim ≥ 0.55 + heat boost)
                                              ├── _prefetch_prices (CLOB mid only, no bid/ask)
                                              ├── insert_signal
                                              ├── pre-AI rules (rules.py)
                                              ├── low-similarity short-circuit (min_top_similarity=0.50)
                                              ├── deterministic sim-gate
                                              ├── market_context (30-min CLOB history)
                                              ├── ai.classify_batch  ◄─── OpenRouter (gemini-2.5-flash-lite + flash premium)
                                              │     (N=3 ensemble, mean + majority vote)
                                              └── fan-out → trader.evaluate × (all engines × variants)
                                                          │
                                                          ▼
                                                  TradeDecision → DB
                                                          │
   ┌──────────────────────────────────────────────────────┼──────────────────────────────────────┐
   ▼                                                      ▼                                      ▼
 trades / positions                              engine_results                       market_price_snaps
   ▲                                                                                  price_impacts
   │                                                                                  price_timeseries
 monitor_positions (every 60 s)
   │   → close_trade(exit_price)
   ▼
 dashboard.py (every 30 s) ──► data/dashboard/*.txt
 impact_tracker.py (continuous) ──► price_impacts / price_timeseries
```

The two pipelines that fan out from each headline — (a) per-engine trade
decisions, (b) headline-anchored impact telemetry — write the same
`market_price_snaps` table at ingress. There is **no continuous
price tape after entry**; A2.2 adds a sibling `position_price_snaps`
table.

---

## Modules

Spec block status: 11 of 23 load-bearing files carry an in-file
`## Specification` block today (committed in `510aa6c`). The remaining
12 will be specced as part of the Phase-1 rebuild PR for the module they
sit in.

| Module | Lines | Spec block? | Phase-1 approach (master-plan §"Rewrite vs refactor") |
|---|---:|---|---|
| `ai.py` | 1,197 | yes (L32) | A4E.4 LANDED (2026-05-23): `json_schema` + `<scratch>` CoT + 2/3 agreement gate + `batch_size=10` chunking + regex-extract fallback. A2.1 (`llm_calls` writer) earlier. |
| `article_fetch.py` | 182 | no | KEEP — fire-and-forget body fetch; production-validated. |
| `budget.py` | 185 | no | KEEP AS-IS — new (cycle-2), small, well-tested. |
| `config.py` | 733 | no | REFACTOR — collapse dataclass duplication; preserve override semantics. |
| `config_validate.py` | 125 | no | KEEP — pure validation. |
| `dashboard.py` | 1,980 | no | REFACTOR (heavy cull) — A0.7 plan targets ≥ 500 lines saved. |
| `db.py` | 2,929 | no | REFACTOR / EXTEND — never rewrite. Cycle-2 fixes are tribal. Drop `lessons`, archive skip rows, add indexes. |
| `debug.py` | 392 | no | REFACTOR — diagnostic helpers; trim with the rest. |
| `engine.py` | 1,147 | yes (L38) | REFACTOR + rename stages (INGRESS → SIGNAL → SIM_GATE → PREDICT → FAN_OUT → WRITE); delete dead branches D1-D9 from A0.5 report. |
| `impact_tracker.py` | 684 | yes (L12) | REFACTOR + EXTEND — add `_position_snap_loop` for trade-anchored tape (A2.2). |
| `main.py` | 610 | no | REFACTOR — boot orchestration; delete `plotter` glue when A1.5 ships. |
| `market_context.py` | 195 | yes (L11) | KEEP — 30-min CLOB history fetch, well-bounded. |
| `markets.py` | 453 | yes (L4) | HYBRID — rewrite `find_matches` for tag-based + cross-encoder routing (4D.2 + 4E.2); keep MarketIndex backbone. |
| `metrics.py` | 253 | no | KEEP — minutely metrics writer; owns `metrics_minutely`. |
| `news_memory.py` | 324 | yes (L4) | REFACTOR — add automatic HNSW corruption recovery (A0.9 §3). |
| `notify.py` | 96 | no | KEEP — Telegram + Discord stubs. |
| `plotter.py` | 1,404 | no | **DELETE** — disabled in prod via `SEULGI_DISABLE_PLOTTER=1`. Phase 1 A1.5. |
| `profiler.py` | 139 | no | KEEP — function-timing helper; cheap. |
| `rules.py` | 431 | yes (L4) | REFACTOR — drop stale `inverted_highconf` refs; verify `skip_sports`/`skip_weather` aren't silent no-ops (A0.10). |
| `scout_funnel_core.py` | 214 | no | KEEP — read-only attribution helper consumed by dashboard. |
| `simulate.py` | 269 | yes (L10) | **REWRITE** — pure functions, no DB/state. Drop `disable_below_usdc` floor; fit micro-notional slippage curve. Tests in `test_simulate_fidelity.py`. |
| `trader.py` | 1,594 | yes (L4) | HYBRID — rewrite `evaluate()` + `close_position` as pure functions (~650 lines); refactor orchestration glue (~800 lines). |
| `watcher.py` | 1,087 | no | REFACTOR — news scraping, production-validated against ~70 endpoints. |

`wc -l seulgi/app/*.py = 16,309` (target post-Phase-4: ~12,000).

The dashboard's PERF metrics tile observed today shows the system actively
writing to `db.insert_decision_input` (121 / 0 errors), `db.log_llm_parse_failure`
(2 / 0 errors), and `db.batch.rollback {exc:CancelledError}` (3) — confirming
FIX-3 cycle-2 fixes are live, but those rows do not persist because no
engine.py call site wires them yet (the metric is a diagnostic increment
inside the writer; row counts in the live DB are 0).

---

## Schema

13 tables in the live DB. Row counts sampled at 2026-05-22 18:30 UTC via
`sqlite3 file:?mode=ro`.

| Table | Rows (live) | Primary index(es) | Writer (file:line) | Notes |
|---|---:|---|---|---|
| `signals` | 91,573 | `idx_signals_freshness` | `engine.py:334` → `db.insert_signal` | One per headline that passes embed + match. Hot table. |
| `trades` | 6,612,246 | `idx_trades_{closed_at,decided_at,engine_decided,engine_closed,headline_id,condition_decided,variant_decided,variant_name,engine_name,freshness}` | `trader.py:_record` → `db.insert_trade` (executed + skip), `db.close_trade` | **6,611,748 of these are skip rows** — cycle-2 contract violation (A0.3 §8, A0.6 §6). Slated drop in Phase 1. |
| `engine_results` | 3,395,605 | `idx_engine_results_{decided,engine_decided}` | `engine.py:817` → `db.record_engine_decisions` | Per-engine per-market row; 3.395M skips + 498 executed. Cycle-2 canonical home for skip rows. |
| `positions` | 1 | (PK only) | `trader.py` → `db.open_position`, `db.update_position_price`, `db.close_position` | Live count = 1 (one balanced engine trade pending budget). |
| `markets` | 10,429 | `idx_markets_closed_at`, `idx_markets_market_type` | `markets.py` → `db.upsert_market`, `db.mark_markets_closed`, `db.prune_closed_markets`, `db.bump_market_heat` | 9,205 open; refreshed every 4 h from Gamma. |
| `headline_articles` | 137,413 | (PK on `headline_id`) | `engine.py:255` → fire-and-forget `db.upsert_headline_article` | UPSERT on headline_id. |
| `market_price_snaps` | 181,362 | `idx_market_price_snaps_t_unix`, `idx_mps_{condition_t,headline,t}` | `engine.py:_prefetch_prices` → `db.insert_market_price_snaps` | Capture started 2026-05-21. 0/497 closed trades have a snap within ±5 min of their decided_at (A0.1). |
| `price_impacts` | 328 | `idx_pi_{condition_id,detected_at,headline_id}` | `impact_tracker.py:record` + `update_price_impact_snapshot` | **Headline-anchored**, not trade-anchored. No `trade_id`. Phase 2 adds sibling `position_price_snaps`. |
| `price_timeseries` | 38,932 | `idx_pts_{headline_id,impact_id,t_unix}` | `impact_tracker.py` → `db.insert_price_timeseries_batch` | Minute fidelity, -1h..+1h around each impact. |
| `usage_events` | 22,038 | `idx_usage_{kind,ts}` | `engine.py:524` → `db.log_usage` | Token totals; today: 5,591 flash-lite + 3,391 flash calls. |
| `metrics_minutely` | 4,681 | `idx_metrics_{bucket,name_bucket}` | `metrics.py` (separate writer) | 1-minute buckets; dashboard reads. |
| `watcher_health` | 71 | (PK on URL) | `watcher.py` → `db.upsert_watcher_health` | One row per watched URL. |
| `cycles` | 5 | `idx_cycles_started_at` | `main.py` → `db.ensure_current_cycle`, `db.start_new_cycle` | Cycle bookkeeping; every `since_ts` query scopes by this. |
| `decision_inputs` | **0** | `idx_decision_inputs_{decided,headline}` | `db.py:2733` (writer exists) — **no engine.py caller wired** | FIX-3 (commit `1b606c3`) shipped schema only. Phase 2 A2.1 wires `engine.process` to write before gather START. |
| `llm_calls` | **0** | `idx_llm_calls_{prompt_sha,ts}` | `db.py` writer NOT present (handoff B writer was never committed) | Phase 2 A2.1 writes one row per ensemble sample. |
| `llm_parse_failures` | **0** | `idx_llm_parse_failures_ts` | `db.py:2824` (writer exists) — **no ai.py caller wired** | 67 rows landed in snapshot `pre_stop_20260522T180029Z` from a path since removed (A0.8 §3.2). |
| `lessons` | 0 | `idx_lessons_{active,action}` | (orphaned — `app/lessons.py` deleted cycle-4) | **DEAD** — slated drop in Phase 1 A1.4. `scripts/analysis/sample_classify.py` is the only stale reader. |
| `lesson_notes` | — | — | — | Table does NOT exist (referenced in scripts as a Python variable). |

**Indexes added today** (FIX-3 piggyback): `idx_trades_engine_closed`,
`idx_trades_condition_decided`. Still missing per A0.6 §3:
`idx_trades_headline_decided` (low priority; partial via
`idx_trades_headline_id`).

**`concept_links` table:** does NOT exist (A0.9 §1). The cycle-3 A.5
claim of "48 concept-link seeds" was aspirational; no code, config, or
schema reference. Phase 4E.2 unblocks.

---

## External dependencies

- **Polymarket Gamma API** (`https://gamma-api.polymarket.com`) — market
  list, refreshed every 4 h (`markets.refresh_hours`). Paginates
  100/page; **422 at offset 10100** is the hard ceiling and is conflated
  with transient 5xx, so the partial-fetch path then calls
  `mark_markets_closed` on the tail and closes real markets. Live DB:
  10,429 markets total, 10,129 fetched today (A0.9 §2). Configured cap
  `markets.max_tracked_markets=20000` is unreachable.
- **Polymarket CLOB** — `_fetch_mid_price` (mid only; no bid/ask),
  `_fetch_book` not used. 10-s in-memory mid cache. Used by
  `engine.py:_prefetch_prices`, `trader.py:_fetch_mid_price`, and
  `market_context.py` (30-min history).
- **OpenRouter** — chat completions only. Default
  `google/gemini-2.5-flash-lite`, premium fallback
  `google/gemini-2.5-flash` (fires on max_sim ≥ 0.70 OR BREAKING+STRONG).
  N=3 ensemble at temperature 0.5. `max_tokens = 160·n + 100` (ai.py:384)
  — too tight for premium flash, truncates 67/67 parse-failure samples.
  HTTP 402 not specially handled; relies on `budget.py` poller setting
  `data/budget_paused.flag` for the engine to short-circuit on next tick.
- **chromadb** (PersistentClient, HNSW) — `news_memory.py` only. Single
  collection at `/data/news_chroma`. **No automatic corruption
  recovery** — on `_init` exception, logs and disables; every subsequent
  `assess()` fail-opens as BREAKING. After a SIGSEGV on 2026-05-21 an
  operator renamed the directory manually.
- **sentence-transformers** — `Embedder` lazy-loads
  `sentence-transformers/all-MiniLM-L6-v2` (384-dim). Used by
  `markets.MarketIndex` (re-embedded on every 4 h refresh) and
  `engine.process` (per-headline embed). Model id hard-coded; no
  `model_version` column on `markets.embedding` → swap would silently
  mix embedding spaces.
- **News watcher endpoints**: 69 HTML scraper sites + 3 RSS feeds + 1
  Truth Social account (`@realDonaldTrump`). HTML scraping is delegated
  to an external scraper service. Last 24 h: 23 watchers report
  `last=never` (no headlines parsed); 4 report transient HTTP 504.

---

## Configuration

Read from `seulgi/config.yaml` (732 lines). Env override pattern
`SEULGI__SECTION__KEY=value` honored by `config.py`.

### Active engine grid (post cycle-4 cull)

`config.yaml:561-664` declares **3 engines, 3 variants total** (one each):

| Engine | Variant | Pot | min_conf | min_edge | TP | SL | trail | min_hold | max_position |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `balanced` | `conf40_tp025_sl045` | $100 | 0.40 | 0.03 | 0.025 | 0.045 | 0.012 | 300 s | $100 |
| `scout` | `all_signals` | $50 | 0.00 | −10 | 0.05 | 0.05 | 0.020 | 60 s | $1 |
| `super_scout` | `all_signals` | $50 | 0.00 | −10 | 0.05 | 0.05 | 0.020 | 60 s | $0.05 |

`simulate_total_usdc: 1600.0` (L535). The previous expansion to
`balanced + 11 variants / loose_relevance_yes / inverted_highconf` was
**reverted** in cycle-4 (L582-588, L594-598, L624-628); the variant
names visible in `data/dashboard/dashboard.txt` belong to a previous
boot's persisted state and don't match the current `config.yaml`.
The dashboard will reconcile on next engine roster refresh.

### Key knobs (file:line, current value)

- `trading.min_edge: 0.04` (L507) — minimum `edge = p_hat − mid` for any
  trade. Per-engine override.
- `trading.edge_increment: 0.12` (L508) — `edge = increment × (2·conf − 1)`
  in `trader.py:807-814`. Phase 4B.2 replaces with EV-based.
- `trading.min_trade_usdc: 5.0` (L506) — global minimum. Note
  `simulate_min_trade_usdc: 1.0` (L536) is set **but never wired** —
  trader reads `_cfg.min_trade_usdc` only (A0.3 §5b).
- `trading.position_timeout_hours: 24.0` (L510) — 0 lifetime fires; admin
  closes intercept (A0.4 §4).
- `trading.cold_position_hours: 4.0` (L513) — 0 lifetime fires (no
  COLD_CULL closes ever).
- `trading.rail_skip_threshold: 0.05`, `entry_rail_skip_threshold: 0.02`
  (L541, L546).
- `markets.similarity_threshold: 0.55`, `top_k: 25`,
  `min_top_similarity: 0.50` (L409-433).
- `ai.model: google/gemini-2.5-flash-lite`, `premium_model:
  google/gemini-2.5-flash`, `ensemble_samples: 3`,
  `ensemble_temperature: 0.5`, `min_agreement: 0.66`,
  `min_confidence: 0.35` (L471-490).
- `news_memory.short_circuit_labels: []` (L694) — **empty**; cycle-13
  token-saving short-circuit on REHASH/STALE_RETURN is **currently OFF**
  (A0.9 §5). Restore to `[REHASH, STALE_RETURN]` before re-enabling AI
  load.
- `news_memory.dedup_distance_threshold: 0.12`,
  `fresh_dist_threshold: 0.30`, `rehash_age_secs: 21600`,
  `stale_age_secs: 86400` (L683-687).

### Cost knobs

- `simulate_fees.default_rate: 0.05` (config.py defaults; trader
  applies via `simulate.compute_fee`). Pre-2026-05-19 trades used 0.04
  — A0.1 audit flags this as the FEE_CONFIG_DRIFT informational gap.
- `simulate_fees.gas_cost_per_trade_usdc: 0.005` (added 2026-05-19).
- `simulate_slippage.disable_below_usdc: 5.0` (`config.py:257`,
  `simulate.py:149`) — **the cause of the slippage NO-OP**: every
  current variant stakes < $5, so 497/497 audited trades have
  `walk=0` (A0.1, A0.2 §1).
- `trading.spread_cost_bps: 0.0` (default in `super_scout` at
  `config.yaml:661`; `config.py:185` default = 20.0 but production
  trader engine configs override to 0). Paper-mode round-trip drag is
  effectively zero on the trader's secondary path; `simulate.py` runs
  alongside but is itself disabled by the threshold above.

### Where each is read

- `engine.py:130` reads `cfg.markets.min_top_similarity` at construction.
- `engine.py:1128` resolves per-engine `spread_cost_bps`.
- `trader.py:1327, 1441, 1540` apply `_cfg.spread_cost_bps`.
- `simulate.py:149` applies `disable_below_usdc`.

---

## Decision funnel

Source: `data/dashboard/dashboard.txt`, snapshot 2026-05-22 18:01:07 UTC
(container up 13 m).

```
── DECISION FUNNEL (last 1h) ────────────────────────────────────
  decisions seen          : 65,975
  → freshness skip        :      0  ( 0.0%)
  → low_similarity bypass : 63,434  (96.1%)
  → relevance gate dropped:      0  ( 0.0%)
  → pre-AI rule dropped   :  2,183  ( 3.3%)
  → AI returned NEUTRAL   :    356  ( 0.5%)
  → other skipped         :      2  ( 0.0%)
  → executed              :      0  ( 0.0%)
```

Per-headline view (24 h, dashboard "AI 1h" tile): **100.0% NEUTRAL
(n=11,940)**, tagged `⚠ signal starved`.

A0.8 cross-checks this on the full 24 h `engine_results` corpus:

- `not_relevant:*` (Stage-2 sim-gate, after `low_similarity:` 0.50 floor): 249,539 (55.9%)
- `RULE_*_SKIP` (pre-AI rules): 193,838 (43.4%)
- Reached AI (post sim + rules): 196,609 rows
  - AI direction = NEUTRAL: **196,559 (99.97%)**
  - AI direction = YES: 34
  - AI direction = NO: 16

**Budget-paused state.** OpenRouter cap reportedly $0 in the morning
(handoff §1); the bot recovered enough to fire 11,940 LLM calls in the
last hour at snapshot time. The 0/0 executed in the last-1h funnel is
the combined effect of (a) `low_similarity` dominating (96.1%), (b)
post-AI NEUTRAL on virtually everything that reaches Stage 3, and (c)
`scout` having already opened its single $1 position earlier in the
session (cost = $1 visible in `positions_open` tile). `freshness skip`
shows 0 because `short_circuit_labels: []` retired the REHASH /
STALE_RETURN short-circuit (see Configuration §news_memory).

---

## Known limitations

Concrete list, ordered roughly by paper-PnL impact. Each item names the
file:line where the bug lives + which Phase plan-item unblocks it.

1. **Slippage layer is a NO-OP.** 100% of trades stake < $5;
   `simulate.apply_slippage` short-circuits via `disable_below_usdc=5.0`
   (`simulate.py:149`, `config.py:257`). 497/497 audited trades have
   `walk=0`. **Phase 1 A1.1** (simulate.py rewrite + curve refit).
2. **99.97% NEUTRAL emission rate.** Last 24 h: 196,559 NEUTRAL vs 50
   directional. Three causes in `ai.py`: (a) `_aggregate_ensemble`
   argmax ties resolve to NEUTRAL (`ai.py:597`); (b) prompt nudges
   toward NEUTRAL on three axes (mid extremes, low-trust source,
   uncertainty) with no counter-axis for high-trust; (c) truncated
   premium batches downgrade silently to per-market fallback.
   **Phase 1 (confidence semantic) + Phase 4E.4 (prompt rework + ensemble
   tie-break).**
3. **`confidence` semantic ambiguity.** `ai.py:719`
   `confidence = p_alpha if p_alpha > 0 else mean_p_winner` — silently
   swaps between two distributions. Mean(`confidence − p_alpha`) =
   +0.58 across 498 executed rows. Trader's `2·conf − 1` edge rescale
   then changes meaning row-to-row. **Phase 1 trader.evaluate rewrite.**
4. **TP / TRAIL fee-blind closes (PARTIALLY FIXED).** Pre-FIX-2A: 53
   TP rows and 58 TRAIL rows booked net losses on 2 ¢ TP / 1 ¢ trail
   thresholds smaller than round-trip $0.05 fee. **FIX-2A (commit
   `b22aa1f`) gates TP + TRAIL on net MTM**; SL intentionally left
   gross. Phase 1 paper-accuracy audit will retest.
5. **Mid drift / stale entry price.** `engine.py:_prefetch_prices`
   fires before the Stage-3 LLM (~1-3 s) and `trader.evaluate` never
   refetches before `insert_trade`. Empirical drift on hot headlines:
   200-500 bps (A0.3 §9). **Phase 4A.2** (JIT pricing + IOC/FOK).
6. **`exit_price` persistence (PARTIALLY FIXED).** `trades` had no
   `exit_price` column pre-2026-05-22. **FIX-2B (commit `849abbd`)**
   added `exit_price` + `entry_price_at_close` and `close_trade`
   persists both. Pre-migration closed rows remain NULL — historical
   PnL not reconstructible (A0.4 §5.1).
7. **Exit-leg `market_type` (FIXED).** `compute_fee(market_type=None)`
   on exit paid the 0.05 default rate even for geo/sports/politics
   markets. **FIX-1 (commit `c21a3dc`)** plumbs `market_type` through
   close paths.
8. **`super_scout` Kelly bypass (FIXED).**
   `is_scout = name.startswith("scout")` excluded super_scout from the
   Kelly bypass and the $0.05 stakes always failed
   `min_stake_usdc=1.0`. **Inline fix in commit `510aa6c`** changes to
   `"scout" in name`.
9. **NaN propagation through fractional_kelly (FIXED).** `p_true=NaN`
   reached `max(0, min(1, NaN)) = 1.0` → f_star=1.0 → max bet. **Inline
   `_is_finite` guard in commit `510aa6c`.**
10. **Skip-row contract violation.** `trades` carries 6,611,748 skip
    rows + 498 executed (~13,000:1). Cycle-2 contract: skips live in
    `engine_results` only. Stop-writes + archive migration. **Phase 1
    A1.4 + trader rewrite.**
11. **Phase-5 capture not wired.** Tables `decision_inputs`,
    `llm_calls`, `llm_parse_failures` exist (FIX-3 commit `1b606c3`)
    but `engine.py` doesn't call `db.insert_decision_input` and
    `ai.py` doesn't call `db.log_llm_parse_failure`. `llm_calls`
    writer is also still TODO. **Phase 2 A2.1.**
12. **No continuous price tape post-entry.** `market_price_snaps`
    fires only at ingress (engine.py:_prefetch_prices). Coverage
    starts 2026-05-21 05:48 UTC; 0/497 closed trades have a snap
    within ±5 min of decided_at (A0.1). **Phase 2 A2.2** (new
    `position_price_snaps` at +5m/+15m/+30m/+1h/+4h).
13. **NewsMemory no HNSW auto-recovery.** `news_memory.py:115-135`
    catches init exception, logs, disables; every `assess()` then
    fail-opens as BREAKING. Next dual-container race silently blinds
    freshness. **Phase 1 news_memory refactor** — rebuild from
    `signals.headline WHERE detected_at > now-7d` on corruption.
14. **Polymarket universe cap.** 10,429 markets total in DB; Gamma 422
    at offset 10100 is conflated with transient 5xx in `markets.py:110`
    bare `except`, and the resulting partial fetch triggers
    `mark_markets_closed` on real-but-overflowed markets. **Phase
    4E.3** (discovery + adapter) + immediate 422-vs-5xx fix in markets.py.
15. **Only ~4 days of usable historical data.** `signals.MIN(detected_at)
    = 2026-05-18T20:39` (cycle-2 reset truncated earlier rows). Phase 3
    backtest uses `engine_results.{direction, confidence, p_hat}` as
    the fallback oracle (A0.12 §2); ~95% of variant-grid backtests
    replayable, ~0% of prompt-rework experiments.
16. **`trades.ai_raw_response` keeps only chosen-idx for 1 market**, not
    the N=3 ensemble per-market distribution. Useless as historical
    oracle for prompt experiments. **Phase 2 A2.1** native `llm_calls`
    capture is the fix; no need to backfill `ai_raw_response`.
17. **`concept_links` table missing.** cycle-3 A.5 "48 seeds" claim is
    aspirational. **Phase 4E.2.**
18. **60-s monitor cadence systematically blows past SL.** Sample of
    NO/SL closes books realized losses 3-7× the `stop_loss` knob
    (A0.4 §3). Tight engines bleed because the next monitor tick sees
    a price already deep in the red. **Phase 1 trader rewrite** —
    shorter cadence (10-15 s) on high-vol markets.
19. **`max_tokens = 160·n + 100` truncates `gemini-2.5-flash`.** 67/67
    parse failures in snapshot were truncations. Truncated batches
    silently downgrade to non-premium per-market fallback with no
    telemetry. **Phase 1 config bump** (suggest ≥ `400 + 200·n`).
20. **HTTP 402 swallowed.** `ai.py:506-538` retries 429 only; 402
    returns None and the batch falls through to per-market retries
    against the same cap, burning more 402s. **Phase 1** wire a 402
    sentinel that flips `budget_paused.flag` immediately rather than
    waiting on the 5-min poll.
21. **Capital check after expensive work.** `engine.py:493`
    `_update_ai_pause_state` runs after embed + freshness + match +
    prefetch + signal + heat-bump + market-context. **Phase 4A.1**
    moves to stage 0a.
22. **GC-orphaned `create_task`.** `engine.py:255` (article_fetcher)
    and `engine.py:844` (impact_tracker.record) hold no task ref;
    Python may GC mid-await. **Phase 1 engine refactor.**
23. **`get_calibration_curve()` not read by trader.** Computed and
    plotted (`plotter.py:193`, `main.py:138`) but `trader.evaluate`
    sizes on raw `confidence`. **Phase 4B.1.**
24. **Hour gate hardcoded UTC 23/00/01.** Silently drops headlines
    pre-DB. Gates legitimate breaking news (`engine.py:246-250`).
    **Phase 4D.1** replaces with spread + book-depth filter.
25. **`duplicate_position` binary lockout.** `trader.py:873-880` —
    once you hold any size on (condition, engine), you can't add.
    **Phase 4D.3** replaces with position scaling.
26. **`simulate_min_trade_usdc` never wired.** `config.yaml:536` set
    but trader reads `_cfg.min_trade_usdc` (global $5) only (A0.3 §5b).
    Delete or plumb.
27. **`skip_sports.md` / `skip_weather.md` are silent no-ops.** Zero
    lifetime fires because `markets.market_type` is unpopulated at
    pre-AI gate time (A0.10 §3.7). **Phase 1 rules cleanup** — refactor
    to question-regex OR retire.
28. **`skip_high_confidence_anti_informative.md` stale.** Still in
    repo, references retired `inverted_highconf` in `engines_exclude`
    (A0.10 §3.1). **Phase 1.**
29. **Race in `MarketIndex.refresh()`.** `find_matches` reader walks
    `self._markets` + `self._matrix` without a lock; writer swaps both
    sequentially. Window is small but real. **Phase 1 news_memory
    refactor / markets.py refactor** — atomic-swap via single
    `(markets, matrix)` tuple.
30. **WAL bloat to ~524 MB on 7 GB DB.** `PRAGMA wal_autocheckpoint=1000`
    is broken because a long-lived reader (dashboard or impact_tracker)
    holds the txn open (bugs-cycle2 §5). **Phase 1 db.py refactor.**

---

## What's already shipped today (cycle 4, 2026-05-22)

In order of landing. All offline, no live LLM consumed for these.

| Commit | Scope | What it changed |
|---|---|---|
| `a3062e2` | Phase 0 A0.1 paper-trade accuracy audit | `scripts/analysis/paper_accuracy_audit.py` + `reports/2026-05-22-paper-accuracy.md`. 497 closed trades, 6 checks, all green for current code; 8 pre-fix TRAIL failures localized to pre-`f0b1ca4`. |
| `b22aa1f` | **FIX-2A**: TP + TRAIL net-MTM gates | `trader.py:1357` — both gates now compare `(mtm × shares) − est_exit_fee` against the threshold instead of bare `mtm`. Prevents 53 TP + 58 TRAIL fee-blind net-loss closes seen historically. SL intentionally left gross. |
| `c21a3dc` | **FIX-1**: exit-leg `market_type` plumbing | `trader.py:1209, 1357` — `close_trade` paths and COLD_CULL now pass `market_type` through `compute_fee` / `apply_slippage`. Geo / sports / politics markets stop paying the 0.05 default rate on exit. |
| `849abbd` | **FIX-2B**: persist `exit_price` on close | Migration + writer: `trades.exit_price REAL`, `trades.entry_price_at_close REAL`. `close_trade(...)` accepts and persists both. Backfill = NULL for pre-migration rows. Unblocks A0.1's `PNL_FORMULA` check going forward. |
| `1b606c3` | **FIX-3**: cycle-2 DB completion + Phase-5 capture tables | (a) `aiosqlite.connect(..., isolation_level=None)`; (b) `batch()` ROLLBACK-on-entry; (c) `asyncio.shield()` around rollback/commit; (d) `decision_inputs`, `llm_calls`, `llm_parse_failures` schema (no engine.py/ai.py callers yet); (e) `idx_trades_engine_closed`, `idx_trades_condition_decided` indexes. Stops the recurrence of the cycle-2 "cannot start a transaction within a transaction" write blocker. |
| `510aa6c` | Phase 0 spec blocks + inline batch-1 fixes | (a) `## Specification` blocks added to ai/engine/impact_tracker/market_context/markets/news_memory/rules/simulate/trader (11 files); (b) `super_scout` Kelly bypass via `"scout" in name`; (c) NaN guard `_is_finite` in `fractional_kelly`; (d) 49 tests green. |

**Reports landed (uncommitted but on disk)**:
`reports/2026-05-22-{ai-bugs,bugs-cycle2,capture-coverage,dashboard-cull-plan,db-audit,engine-bugs,impact-tracker-extension-plan,markets-memory-bugs,paper-accuracy,rules-audit,simulate-bugs,trader-entry-bugs,trader-exit-bugs}.md`
— 13 files, 12 named in the Phase 0 plan + 1 cycle-2 cross-validation.

**What did NOT ship today**: the four engine.py changes the
2026-05-22-v2 handoff advertised (`decision_inputs` capture before
gather, `budget_paused.flag` short-circuit at top of process,
background `_engine_should_pause` refresher, gather instrumentation).
A0.5 §8 documents the doc-vs-code gap; Phase 1 must land these with
cycle-2 fixes built in.

---

*End of architecture.md. Phase 1 updates this file in the same PR that
modifies the affected module.*
