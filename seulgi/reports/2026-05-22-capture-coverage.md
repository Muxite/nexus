# Offline-replay capture coverage audit — 2026-05-22 (Phase 0 A0.12)

Scope: what historical-replay input the Phase-2/3 backtest needs vs.
what is captured in live `data/seulgi.db` (8.1 GB) *today*, before
FIX-3C lands the Phase-5 tables (`decision_inputs`, `llm_calls`,
`llm_parse_failures`). All evidence read-only via Python `sqlite3`
URI `?mode=ro`. Cross-refs: `2026-05-22-db-audit.md` (inventory),
`2026-05-22-engine-bugs.md` §1, `2026-05-22-ai-bugs.md` §3,
`2026-05-22-paper-accuracy.md` (skip-row contract).

---

## 1. Capture-surface table (per input)

| Input | Live source | Coverage | Verdict |
|---|---|---|---|
| Headline + source + ts | `signals(headline, site_url, detected_at, headline_id)` | 100% / 91,573 rows | OK |
| Headline embedding (SBERT 384) | **NOT in `signals`** (7 cols, no `embedding`). Vectors live only in chromadb HNSW. | 0% in SQL | Replay recomputes via SBERT (~3 ms × 91 k = 5 min) or FIX-3C `decision_inputs.embedding BLOB`. |
| Per-headline matches (cond_id, q, sim, mid_yes/no, relevance) | **`engine_results`** — one row per (engine × condition_id). NO `matches_json` column exists. 25 markets × ~29 engines ≈ 725 rows/hot headline. | 100% reconstructible via `WHERE headline_id=?` | OK after JOIN-and-GROUP. Master plan A2.3's "walks `decision_inputs`" assumes a row-per-headline JSON; today's source is row-per-(headline, engine, market). |
| Pre-AI rule skips | `engine_results.skip_reason` like `RULE_*`, `low_similarity:*`, `not_relevant:*`. NO `trades.pre_ai_skips` column. | 100% of skipped pairs (3.4 M rows / 14d) | OK |
| Freshness label + dist + age | `signals.freshness_label` + `nearest_age_secs` (100% since 2026-05-18). `nearest_dist` ONLY in `ai_prompt_meta` JSON. | Label/age 100%; dist partial (only when LLM ran) | Acceptable; FIX-3C adds `decision_inputs.freshness_dist`. |
| LLM prompt text | **NOT captured.** No `llm_calls` table in live DB; snapshot `pre_stop_20260522T180029Z` has the schema but 0 rows. Writer never wired (ai-bugs §3). | 0% | Reconstructible offline — `_BATCH_PROMPT_TEMPLATE` is deterministic given `(headline, matches, freshness, source, market_context)` and all those inputs ARE captured. |
| LLM raw response | `trades.ai_raw_response` — but stores ONLY the chosen-idx dict for ONE market, not the N=3 per-sample ensemble. | Per-day executed coverage: 5-18=3/376, 5-19=22/70, 5-20=50/50, 5-21=1/1, 5-22=1/1. Skip-row coverage: 8,333 / 6.6 M = 0.13%. | **Catastrophically sparse**; cannot serve as oracle. |
| Mids at decision | `trades.mid_yes` + `mid_no` (100%); `mid_price` (legacy) | 100% | OK |
| Variant config | `trades.engine_name` / `variant_name` (e.g. `balanced/conf40_tp025_sl045`) | 100% | OK; resolve against `config.yaml`@decided_at via git history. |
| Trader output (action+reason) | Executed: `trades` row + `trader_trace` (step-by-step JSON). Skipped: both `engine_results` and `trades` rows (the 6.6 M-row contract violation). | Trace coverage exec=100% (498/498), skip≈100% (6,611,747/6,611,748) | OK; recommend `engine_results` as canonical, treat `trades` skips as advisory. |
| Exit price + close reason | `trades.exit_price`, `entry_price_at_close`, `close_reason`, `realized_pnl`. FIX-2B columns exist but **0 rows populated post-migration** (no closes happened yet). `close_reason` populated on pre-migration closes. | Bridges forward from FIX-2B; pre-FIX-2B closes unreconstructible (A0.4). | Acceptable going forward. |

---

## 2. Backtest fallbacks (TODAY, before FIX-3C)

**`llm_calls` empty?** Use `engine_results.{direction, confidence, p_hat, similarity, mid_price}` (100% populated; sampled 10 recent rows). This is the *only* viable oracle.

What's lost vs. native `llm_calls`:
- Per-sample dispersion (N=3 raws) — only the post-aggregate `agreement` survives, on `trades.agreement` (NOT `engine_results`).
- Per-market `p_yes`/`p_no`/`p_neutral` decomposition — `engine_results` collapses to `direction` + `confidence` (where `confidence == p_alpha` post-cycle-7). Blocks Phase 4E calibration / prompt-rework experiments that need the raw distribution.
- Raw reason strings (debug-only).

**Note**: the task brief's hypothesized `engine_results.classify_result` column does NOT exist. The audit confirms via `PRAGMA table_info`. The triple above is the closest live equivalent.

**`decision_inputs` empty?** Reconstruct from `signals` + `engine_results` + `ai_prompt_meta` (on `trades`):

```sql
SELECT s.headline_id, s.headline, s.detected_at, s.freshness_label,
       s.nearest_age_secs,
       (SELECT t.ai_prompt_meta FROM trades t
         WHERE t.headline_id = s.headline_id LIMIT 1) AS meta_json,
       JSON_GROUP_ARRAY(JSON_OBJECT(
         'cond', er.condition_id, 'sim', er.similarity,
         'mid', er.mid_price, 'skip', er.skip_reason
       )) AS matches_json
  FROM signals s
  LEFT JOIN engine_results er ON er.headline_id = s.headline_id
 GROUP BY s.headline_id;
```

Lost:
- `embedding` — recompute via SBERT.
- `nearest_dist` — only when ≥1 `trades.ai_prompt_meta` exists for the hid.

Meta coverage on skip rows in last 24 h: RULE 50,027 / 200,485 (~25%); not_relevant 27,911 / 196,503 (~14%); low_similarity 0 / 824,723 (the LLM never ran — no oracle needed). Per-headline reconstruction is therefore complete *structurally* (every row reachable) but missing meta for ~70% of skip paths where the LLM was bypassed.

---

## 3. Phase 2 A2.x readiness scorecard

| ID | Blocked by FIX-3? | Notes |
|---|---|---|
| **A2.1** `llm_calls` writer | **TRUE** | Needs the table to exist. ~80 LoC around `classify_batch._call_api`. |
| **A2.2** Continuous price tape | **FALSE** | Independent. Extends `impact_tracker.py` + `market_price_snaps`. New `position_price_snaps` migration only. |
| **A2.3** Backtest harness architect (spec) | **FALSE** for the spec; **TRUE** for the implementation if it must walk `decision_inputs` rather than the engine_results join. Can design today against the snapshot's column shape. |
| **A2.4** Implement A2.3 | **TRUE** transitively (no `decision_inputs` rows pre-FIX-3 outside the snapshot). Workaround: bridge script (see §5). |
| **A2.5** Capture integration test | **TRUE** | Test asserts "every Phase-5 capture row writes" — impossible without the tables. |

---

## 4. 14-day-back replay feasibility (TODAY)

Live DB MIN(`signals.detected_at`) = 2026-05-18T20:39 — only ~4 days of history exist (cycle-2 reset truncated earlier rows). "14-day back" effectively means "available history".

| Day | signals | engine_results | exec | exec w/ meta | exec w/ raw |
|---|---:|---:|---:|---:|---:|
| 2026-05-18 | 2,286 | 595,714 | 376 | 376 | 3 |
| 2026-05-19 | 41,736 | 966,406 | 70 | 70 | 22 |
| 2026-05-20 | 33,050 | 1,387,335 | 50 | 50 | 50 |
| 2026-05-21 | 5,649 | 371,690 | 1 | 1 | 1 |
| 2026-05-22 | 8,852 | 74,460 | 1 | 1 | 1 |

Per-day replay feasibility using the fallback oracle (engine_results triple):

- **5-18 to 5-20**: ~95-100% replayable for variant-grid backtests (Phase 3 A3.3 — swap trader thresholds, hold oracle fixed). The 5-21 and 5-22 days are too thin (ai_paused_no_capital + simulate-mode pauses) for statistical power.
- **All days**: ~0% replayable for experiments requiring the raw per-market p_yes/p_no/p_neutral distribution (Phase 4E.4 prompt rework, full calibration curves). The data simply isn't there.

**Headline number**: ~95% of the variant-grid-relevant decision corpus is replayable today; ~0% of the prompt-rework-relevant corpus.

---

## 5. Verdict — prioritized fix list

| # | Fix | Phase | Effort | Why |
|---|---|---|---|---|
| 1 | FIX-3C: create `decision_inputs/llm_calls/llm_parse_failures` per the snapshot schema; wire `insert_decision_input` from `engine.process` BEFORE the gather batch | Phase 0 (in flight) | S (~50 LoC) | Unblocks A2.1 + A2.5; starts 14-day capture clock. |
| 2 | A2.1 — `llm_calls` writer in `ai.py` ensemble loop. One row per sample (prompt_sha, response_text ≤8 KB, tokens, parsed_ok, latency_ms) | Phase 2 | S (~80 LoC) | Replaces sparse `ai_raw_response` with full per-sample capture. |
| 3 | **Bridge script** `scripts/replay/bridge_decision_inputs.py` — reconstruct `decision_inputs` for last 4 days from `signals` + `engine_results` + `ai_prompt_meta`. Embedding NULL (recomputed at replay). | Phase 2 (A2.4 prereq) | M (~150 LoC) | Lets Phase 3 A3.1 actually backtest available history before native capture has 14 d depth. |
| 4 | A2.2 continuous price tape (independent of FIX-3) | Phase 2 | M | Required for ±10% A3.1 exit-PnL accuracy. |
| 5 | Fix `ai_prompt_meta.market_context` length to match per-headline match count (sampled: many rows have len=1 while engine_results has 25). Sanity-test invariant. | Phase 2 (A2.5) | S | Meta is stamped *per-trade* not per-headline; bug worth flagging. |
| 6 | Stop double-writing skip rows into `trades` (A0.3) — keep them in `engine_results` only | Phase 1 | M | Removes ~5-7 GB; makes `engine_results` the single source of truth. |
| 7 | Persist `signals.embedding BLOB` (or via FIX-3C `decision_inputs.embedding`) | Phase 2 (A2.1 piggyback) | S | chromadb HNSW UUID has rotated twice this cycle; SQL persistence makes replay deterministic. |

---

## 6. Specific-check answers

1. `engine_results.matches_json` — **does NOT exist.** Reconstruct with GROUP BY on `(headline_id)`.
2. `engine_results.classify_result` — **does NOT exist.** Closest: per-row `direction/confidence/p_hat` triple. Full `(p_yes/p_no/p_neutral/p_alpha)` tuple is computed in `_aggregate_ensemble` but only the winning idx survives to `trades.ai_raw_response`.
3. `signals.embedding` — **column absent.** Vectors only in chromadb HNSW. Replay must recompute via SBERT (deterministic given model checkpoint).
4. `trades.trader_trace` — **6.6 M rows populated.** Ideal for backtest *validation* (golden-test: rebuilt trader must reach `step:executed` in same order). Avg 688 chars on executed rows.

---

**Word count: ~1,200.**
