# Master plan — Aggressive offline rebuild → reimagined → profitability

## Context

Cycle-2/3 fixed the write blocker; cycle-4's cleanup already removed
`lessons.py` (392) + `relevance.py` (309) and trimmed engine.py / config.yaml.
First paper execution landed. We now have:

- ~15,417 lines of app code (down from 16,265).
- Phase-5 capture LIVE on `decision_inputs` (146+ rows) and `llm_parse_failures`.
- Continuous market_price_snaps at ingress only (no continuous tape).
- A simulator (`simulate.py`, 199 lines) and trader (`trader.py`, 1,432
  lines) whose accuracy has never been audited.
- A backtest harness that's only a regression-suite, not a PnL replay.
- 96.1% of decisions bypass at `low_similarity` (signal-starved).

The user's directives for this rewrite:

1. **Sim bugs first.** Find and fix every paper-trade accuracy bug
   before tuning anything for profitability.
2. **Rebuilding is allowed.** If a module is faster to rewrite from a
   clean spec than to refactor, rewrite it.
3. **Heavy agent parallelism.** Use many sub-agents to cover ground in
   parallel; each agent owns a module or a forensic slice.
4. **Offline-first.** Most work needs no live LLM and no live bot. Read
   existing DB rows, sample, reason, write fixes. Live runs are
   reserved for end-of-plan smoke + one regression hold. No long soaks.
5. **Reimagine structure + logic.** Don't just fix sim bugs — find
   better ways to: discover correlations (stats-based), connect news to
   markets, find markets, process LLM results, prompt LLMs.
6. **Maintain an up-to-date architecture doc** describing what
   currently exists. The master plan describes NEXT STEPS; the
   architecture doc describes NOW.

The intended outcome at end of plan: a smaller, audited, spec-backed
codebase with a reimagined news→market matching loop, calibrated LLM
processing, a backtest that predicts paper PnL within ±10%, and the
first profitability evidence visible in a single 24h regression hold
(the ONLY 24h soak in this plan).

---

## Two living documents (in-repo)

### `seulgi/architecture.md` — what we have NOW

A living document, updated at the end of every phase to reflect what
**actually** shipped (not what was planned). Owned by whichever agent
last touched a module; PRs that change module behavior MUST update the
architecture doc in the same commit.

Required sections:
- **System overview** — one-paragraph "what seulgi does" + a single
  ASCII data-flow diagram (news watcher → ingest → match → predict →
  fan-out → trader → DB → dashboard).
- **Modules** — one paragraph per load-bearing module, with a pointer
  to its in-file `## Specification` block. Includes line count target.
- **Schema** — table list with row counts (refreshed weekly), primary
  indexes, and which writers populate each table.
- **External dependencies** — Polymarket gamma-api, CLOB, OpenRouter,
  chromadb, sentence-transformers, watcher endpoints (50+ news sites).
- **Configuration** — engine grid, key knobs (min_confidence,
  similarity_floor, TP/SL), where each is read.
- **Decision funnel** — current per-stage drop rates (from
  dashboard/dashboard.txt) with timestamps.
- **Known limitations** — what the system does NOT do (e.g. "no
  continuous price tape post-entry until Phase 2 ships").

Created in Phase 0 by Agent A0.13 (new). Updated at end of every
subsequent phase.

### `seulgi/master-plan.md` — what we do NEXT

This plan, committed in-repo, the canonical source. The old
`~/.claude/plans/code-shipped-all-137-tranquil-turtle.md` becomes a
one-paragraph stub pointing here.

---

## Module specifications (load-bearing)

Every load-bearing module gets a `## Specification` block at the top
of its docstring. Format:

```
## Specification
**Purpose.** One sentence.
**Public interface.** Functions / classes other modules import.
**Invariants.** Must-hold properties.
**Side effects.** DB tables read/written, files, external APIs.
**Failure modes.** What can crash, how it recovers.
**Non-goals.** What this module is NOT responsible for.
```

Specs are written in Phase 0 from sampling + code reading. Phase 1
either refactors against the spec or rewrites the module to match it.
Phase 4 (reimagining) ships *new* modules with their spec from day one.
Any later PR that changes a module without updating its spec is
reject-on-review.

---

## Strategy overview

```
Phase 0  — Parallel forensics + arch doc + specs        (13 agents, ~2-3h)
Phase 1  — Sim + trader rebuild from specs              ( 5 agents, ~3-5h)
Phase 2  — Capture + backtest harness (offline)         ( 5 agents, ~3-5h)
Phase 3  — Offline validation: backtest vs history      ( 4 agents, ~2-3h)
Phase 4  — Structural + logic reimagining               (16 agents across 4A-4E, ~6-10h)
Phase 5  — Live smoke (4h) + regression hold (24h)
Phase 6  — Steady-state cadence
```

Soak budget: **1 × 4h smoke + 1 × 24h regression = 28h calendar**.
Active engineering: **6-9 sessions of mostly-parallel agent work**.
Total agent runs across the plan: **~43**, batched 3-5 per session.

---

## Phase 0 — Parallel forensics + arch doc + specs (no soak)

**Goal**: For every load-bearing module, produce a spec + bug list +
fix plan. Create the architecture doc. All offline.

**Sub-agent fan-out** (13 agents, 3-4 per session batch):

| ID | Owner | Deliverable |
|---|---|---|
| A0.1 | general-purpose | `scripts/analysis/paper_accuracy_audit.py` + `reports/<date>-paper-accuracy.md` (entry truth, slippage fit, fee schedule, TP/SL/timeout, PnL formula, book-depth feasibility) over last 30 days closed trades |
| A0.2 | general-purpose | `simulate.py` spec + bug list (clamp_price, is_at_rail, apply_slippage curve coefficients, compute_fee vs Polymarket schedule, quantize_shares) — sample 100 closed trades, back-derive each function call |
| A0.3 | general-purpose | `trader.py:evaluate` (entry path) spec + bug list (edge formula, confidence handling, fee accounting) |
| A0.4 | general-purpose | `trader.py` (exit / monitor path) spec + bug list (close_position, TP/SL/timeout, MTM unrealized, exit fee) |
| A0.5 | general-purpose | `engine.py` spec + bug list — map pipeline stages, identify dead branches |
| A0.6 | general-purpose | `db.py` spec + audit (table inventory, dead columns/tables after lessons removal, missing indexes) |
| A0.7 | general-purpose | `dashboard.py` + `plotter.py` cull plan — identify dead writers, target ≥ 500 lines saved |
| A0.8 | general-purpose | `ai.py` spec + bug list — sample llm_parse_failures, characterize failure modes |
| A0.9 | general-purpose | `markets.py` + `news_memory.py` spec + bug list — concept_link boost? HNSW recovery? |
| A0.10 | general-purpose | `rules.py` + `seulgi/rules/*.md` audit — stale evidence, dead engines_exclude refs |
| A0.11 | general-purpose | `impact_tracker.py` + `market_context.py` spec + extension plan for continuous tape |
| A0.12 | general-purpose | Capture coverage audit — is `decision_inputs`/`llm_parse_failures` data sufficient for offline replay? Identify gaps. |
| A0.13 | general-purpose | `seulgi/architecture.md` — initial draft from current code |

Verification (offline, no LLM):
- Sample 100 random closed trades; manually re-derive expected entry/exit/PnL.
- Use existing `seulgi/app/tests/` as starting point.
- Cross-reference findings across agents — A0.1 paper-accuracy and A0.2 simulate.py should agree on slippage bugs.

**Exit criteria**:
- 12 spec blocks committed to module files; `seulgi/architecture.md` created.
- 12 bug lists consolidated in `reports/<date>-phase0-forensics.md`.
- Prioritized sim-bug list separated by impact (entry-price, fee, slippage, exit, sizing).

### Phase 0 batch-1 findings (2026-05-22, cross-validated across A0.1-A0.4)

| # | Bug | Severity | Status |
|---|---|---|---|
| 1 | Slippage layer is a NO-OP (stakes < `disable_below_usdc=5.0`) | CRITICAL | Defer to Phase 1 simulate.py rewrite (config knob change) |
| 2 | TP/TRAIL gates fee-blind — 53 TP + 58 TRAIL closes were net losses | CRITICAL | **Fix in Phase 0** (`FIX-2` agent) |
| 3 | `confidence` field swaps between `p_alpha` and `mean_p_winner`; +0.58 mean delta | CRITICAL | Defer to Phase 1 trader.evaluate rewrite (semantic decision needed) |
| 4 | Mid drift: trader uses pre-LLM mid, never refetches | CRITICAL | Defer to Phase 4A (JIT pricing) |
| 5 | No `exit_price` persisted on `trades` — PnL unreconstructible | HIGH | **Fix in Phase 0** (`FIX-2` agent) — schema migration + writer |
| 6 | Exit `compute_fee(market_type=None)` → wrong fees | HIGH | **Fix in Phase 0** (`FIX-1` agent) |
| 7 | `super_scout` Kelly bypass broken (`startswith("scout")`) | HIGH | ✅ **Fixed inline** (trader.py:960, `"scout" in name`) |
| 8 | Skip-row contract violated — 6.6M skips in `trades` + dup writes | HIGH | Phase 1 cleanup + db.py migration |
| 9 | NaN propagation through `fractional_kelly` → max bet | MEDIUM | ✅ **Fixed inline** (simulate.py, `_is_finite` guard) |
| 10 | `market_price_snaps` coverage gap (capture only since 2026-05-21) | INFO | Unblocked by Phase 2 capture pipeline; no fix needed retroactively |

Bug 7 + 9 fixed inline 2026-05-22 (49 tests green). Bugs 2, 5, 6 fixed
by parallel fix-agents alongside batch-2 forensics. Bugs 1, 3, 4 are
larger-scope and feed into Phase 1 / Phase 4 as planned. Bug 8 is a
data-integrity cleanup (6.6M rows) for Phase 1 db.py migration. Bug 10
is data, not code — Phase 2 unblocks future audit runs.

### Phase 0 batch-2 findings (2026-05-22) — DOCS-VS-CODE GAP (URGENT)

The handoff `2026-05-22-v2.md` §1 describes cycle-2 DB fixes (Layer A
slow-read, Layer B rollback-shield, Layer C isolation_level=None, Layer
D no-mega-batch) as SHIPPED. **Reality: only Layer A + Layer D made it
into the tree.** A0.6 + A0.8 + the replay harness load-tier reproduce
the original `cannot start a transaction within a transaction` error
RIGHT NOW. Phase-5 tables (`decision_inputs`, `llm_calls`,
`llm_parse_failures`) don't exist in db.py OR the live DB. The
dashboard.txt's "decision_inputs=146 inserts" was either dead-counter
plumbing or stale snapshot data.

| # | Issue | Severity | Status |
|---|---|---|---|
| 11 | `db.connect()` opens without `isolation_level=None` — Python sqlite3 implicit BEGIN races explicit BEGIN, recreates "cannot start a transaction within a transaction" | CRITICAL | **Fix in Phase 0** (`FIX-3` agent) |
| 12 | `db.batch()` lacks ROLLBACK-on-entry + `asyncio.shield()` around rollback/commit. Cancellation mid-batch strands the connection | CRITICAL | **Fix in Phase 0** (`FIX-3` agent) |
| 13 | Phase-5 tables `decision_inputs`, `llm_calls`, `llm_parse_failures` absent from schema + live DB. Blocks all backtest/replay work | CRITICAL | **Fix in Phase 0** (`FIX-3` agent) |
| 14 | ai.py: 99.97% NEUTRAL emission rate (196,559 NEUTRAL vs 50 directional in 24h) — prompt has 3-axis NEUTRAL bias + argmax ties resolve to NEUTRAL | CRITICAL | Defer to Phase 1 + Phase 4E.4 (semantic fix + prompt rewrite) |
| 15 | ai.py: `max_tokens = 160·n + 100` truncates 67/67 sampled parse failures. Output-token budget too tight | HIGH | Defer to Phase 1 (config bump) |
| 16 | ai.py: HTTP 402 swallowed as generic non-200; account-credit watcher (B.9) not wired | HIGH | Defer to Phase 1 |
| 17 | engine.py: capital check at line 493 (post-LLM, post-CLOB). Should be stage 0a | HIGH | Defer to Phase 4A.1 |
| 18 | engine.py: GC-orphaned `create_task` at 255 + 844; `return_exceptions=True` swallows CancelledError | HIGH | Defer to Phase 1 engine refactor |
| 19 | dashboard.py: 8/23 output files have zero consumer (~500 lines deletable) | LOW | Defer to Phase 1 A1.5 cull |
| 20 | db.py: missing indexes on `(engine_name, closed_at)`, `(condition_id, decided_at)`, `(headline_id, decided_at)`, `t_unix` on price_snaps | HIGH | **Fix in Phase 0** (`FIX-3` agent piggybacks) |

**The cycle-2-completion gap (11/12/13) is the load-bearing fix**: until
isolation_level=None + shielded rollback ship, ANY return of LLM volume
re-triggers the write blocker. FIX-3 is non-negotiable before Phase 1
or any soak.

### Phase 0 batch-3 findings (2026-05-22) — more doc/code gaps

| # | Issue | Severity | Status |
|---|---|---|---|
| 21 | **`concept_links` table + 48 seeds (cycle-3 A.5) DOES NOT EXIST.** No table in DB, no references in app/ or config.yaml. The cycle-3 A.5 claim is aspirational | HIGH | Defer to Phase 4E.2 (matching rework). Remove from "cycle-3 shipped" list. |
| 22 | **`skip_high_confidence_anti_informative.md` is STILL in the repo** despite master-plan claiming cycle-3 deleted it. Also has dead `inverted_highconf` engines_exclude ref | MEDIUM | Defer to Phase 1 rules cleanup. |
| 23 | **`skip_sports.md` + `skip_weather.md` are silent no-ops** — zero fires lifetime because `markets.market_type` is unpopulated at pre-AI gate time | LOW | Phase 1 rules cleanup. Either refactor to question-regex OR retire. |
| 24 | **Polymarket universe at 10100 cap** — 10,429 markets in live DB; `max_tracked_markets: 20000` is unreachable. 422 conflated with transient 5xx → `mark_markets_closed` closes real markets | HIGH | Phase 4E.3 discovery + fix 422-vs-5xx confusion in markets.py. |
| 25 | **NewsMemory has no automatic HNSW corruption recovery** — `_init` catches exception, logs, disables. Every `assess()` then fail-opens as BREAKING. Next dual-container race silently blinds freshness | HIGH | Defer to Phase 1 news_memory refactor (rebuild from `signals.headline` on corruption signature). |
| 26 | **`price_impacts` is HEADLINE-anchored, NOT trade-anchored** — no `trade_id` column. Phase 2 A2.2 needs to add a sibling `position_price_snaps` table | INFO | Phase 2 A2.2 will add trade-anchored table; price_impacts stays for headline analytics. |
| 27 | **`trades.ai_raw_response` only stores chosen-idx for 1 market, not the N=3 ensemble** — useless as a historical LLM oracle for backtest | HIGH | Phase 2 A2.1 (`llm_calls` writer) is the fix; no need to backfill `ai_raw_response`. |
| 28 | **14-day backtest feasibility: only ~4 days of usable history** (signals MIN=2026-05-18 post cycle-2 reset). ~95% replayable via `engine_results.direction/confidence/p_hat` fallback oracle; ~0% replayable for experiments needing per-market `p_yes/p_no/p_neutral` distribution | INFO | Phase 3 backtest runs against the 4-day window using fallback oracle; full per-market distribution requires Phase 2 A2.1 capture going forward. |

### Status snapshot (post FIX-1/2/3 + Phase 0 spec blocks, 2026-05-22 EOD)

| Subsystem | Status |
|---|---|
| Cycle-2 DB fixes (isolation_level, shielded rollback) | ✅ Shipped (FIX-3, commit `1b606c3`) |
| Phase-5 capture tables + writers | ✅ Shipped (FIX-3, schema only; engine/ai wiring is Phase 2) |
| TP/TRAIL fee-aware close gates | ✅ Shipped (FIX-2A, commit `b22aa1f`) |
| `exit_price` persisted on `close_trade` | ✅ Shipped (FIX-2B, commit `849abbd`) |
| Exit-leg `market_type` plumbing | ✅ Shipped (FIX-1, commit `c21a3dc`) |
| `super_scout` Kelly bypass + NaN guard | ✅ Shipped inline (commit `510aa6c`) |
| 11 module spec blocks | ✅ Shipped (commit `510aa6c`) |
| 13 Phase-0 reports in `reports/` | ✅ Shipped (uncommitted — Phase 0 batch close) |
| Slippage NO-OP (size < disable threshold) | Pending Phase 1 simulate.py rewrite (config + curve refit) |
| `confidence` semantic ambiguity (`p_alpha` vs `mean_p_winner`) | Pending Phase 1 trader.evaluate rewrite |
| 6.6M skip rows in `trades` | Pending Phase 1 db migration + trader rewrite (stop dual-write) |
| Mid drift / stale entry price | Pending Phase 4A.2 JIT pricing |
| 99.97% NEUTRAL emission | Pending Phase 4E.4 prompt rework + Phase 1 confidence semantic |
| `architecture.md` populated | A0.13 (in flight) |

---

## Rewrite vs refactor — explicit per-module decision

The user asked: *is it better to write from scratch with compatible
data, or to modify the existing code?* The honest answer is
**per-module**, and the bug-persistence risk is the deciding factor.

### Bug-persistence risk by approach

| Approach | What it catches | What it misses | Net risk for this codebase |
|---|---|---|---|
| **Pure rewrite from spec** | Removes inherited bugs from cycles 1-3 patches. New code traceable to spec. | Re-derivation may reintroduce bugs the old code already solved (autocommit/shielded rollback, batch lock removal, etc.) Hard-won production fixes are tribal knowledge encoded in lines, not comments. | High new-bug risk; low persistent-bug risk |
| **Pure refactor** | Hard-won fixes preserved (they're already in the code). Lower wall-clock; existing tests still apply. | Silent bugs that nobody noticed (slippage formula off-by-one, fee mis-applied to one market type, race in close_position) survive untouched. Spec gets re-written from buggy code. | Low new-bug risk; high persistent-bug risk |
| **Hybrid** (rewrite small pure modules, refactor large stateful ones) | Pure functions are spec-traceable; stateful modules keep hard-won fixes. | Boundary between the two is where surprises hide. | Best of both, IF the boundary is drawn at clean function signatures |

The Phase 0 **paper-trade accuracy audit** (A0.1) is the safety net.
It runs against whatever code Phase 1 ships and quantifies divergence
from realized history. The audit catches:
- Persistent bugs that survived refactor (audit fails because new
  code reproduces the same wrong value as the old code).
- New bugs introduced by rewrite (audit fails because new code
  produces a value inconsistent with realized history).

Both failures look like ±N% paper-accuracy divergence; both block
Phase 5 until fixed.

### Per-module classification

The data layer stays compatible — `db.py` schema + writers do not get
rewritten. Historical rows remain usable for backtest end-to-end.

| Module | Size | Approach | Reason | Bug-persistence risk |
|---|---:|---|---|---|
| `simulate.py` | 199 | **REWRITE** | Pure functions, no DB, no state. Easy to spec, easy to test. New tests grow with new code (`test_simulate_fidelity.py` already exists as scaffold). | LOW (rewrite catches everything; tests catch new bugs immediately) |
| `trader.py:evaluate` | ~400 of 1,432 | **REWRITE** as pure function from spec | Entry path is the load-bearing decision logic; the most-bugged part of the codebase per Phase 0 forensics. Extract as `evaluate(match, classify, cfg) → Trade or Skip`. | LOW with audit; new function tested against 100 historical entries |
| `trader.py:close_position` | ~250 | **REWRITE** as pure function | Exit logic (TP/SL/timeout + MTM) is also hot for sim bugs. Extract as `close_position(pos, current_price, cfg) → Close or Hold`. | LOW with audit |
| `trader.py` orchestration | ~800 (post-extraction) | **REFACTOR** | Async glue, monitoring loop, recovery. Preserves cycle-2 batch-lock removal and shielded rollback callsites. | MEDIUM — caught only by replay regression suite |
| `engine.py` pipeline backbone | ~500 of 1,072 | **REFACTOR + rename stages** | Stage names (Stage 2 / Phase E11) get clean names (INGRESS/SIGNAL/SIM_GATE/PREDICT/FAN_OUT/WRITE). Cycle-2 gather-timeout safety belt + autocommit batch behavior preserved. | MEDIUM — replay regression covers |
| `engine.py` dead branches | ~570 | **DELETE** | Anything the Phase 0 audit identifies as unreachable. | LOW |
| `db.py` schema + writers | 2,652 | **REFACTOR / EXTEND** — never rewrite | Cycle-2 autocommit, shielded rollback, no-mega-batch are TRIBAL FIXES. Rewrite would reintroduce the write blocker. Cleanup (drop `lessons`/`lesson_notes` tables), add indexes, add new tables — but the writer code stays. | LOW for kept code; LOW for new tables |
| `ai.py` Stage-3 prompt + parse | 810 | **HYBRID** — rewrite parse pipeline (A2.1 llm_calls writer, A4E.4 json_schema), refactor prompt assembly | Parse pipeline is well-bounded; prompt assembly has cycle-2 freshness/lessons fixes worth preserving. | LOW for parse; MEDIUM for prompt assembly |
| `markets.py:find_matches` | ~150 of 407 | **HYBRID** — rewrite for tag-based + cross-encoder routing (4D.2 + 4E.2), keep MarketIndex backbone | Find_matches is performance-critical and currently SBERT-cosine-only; clean rewrite for new matching strategy. MarketIndex (storage + load) stays. | LOW (new code; tests required) |
| `impact_tracker.py` | 655 | **REFACTOR + EXTEND** | Continuous tape extension (A2.2) adds new snapshot intervals; existing telemetry preserved. | LOW |
| `dashboard.py` | 1,914 | **REFACTOR (heavy cull)** | Dashboard is read-only output; bugs here don't cost trades. Cull dead writers, keep working ones. | LOW |
| `plotter.py` | 1,404 | **DELETE** | Already disabled. No bug-persistence risk because no execution. | LOW |
| `news_memory.py` | 302 | **REFACTOR** | HNSW corruption recovery hard-won; preserve. | LOW |
| `watcher.py` | 1,087 | **REFACTOR** | News scraping; production-validated against 50+ endpoints. | LOW |
| `budget.py` | 185 | **KEEP AS-IS** | New (cycle-2), small, well-tested in production. | LOW |
| **NEW Phase 4 modules** | — | **WRITE from spec** | `execution.py`, `fastpath.py`, `calibration.py`, `tagger.py`, `scoring.py`, `discovery.py`, `counterfactual.py`. No existing code to inherit from, so the question is moot. | New-bug risk only |

### Bottom line

The plan is **hybrid by design**:
- **Pure rewrite** (clean slate from spec): `simulate.py`, `trader.evaluate`, `trader.close_position`, all Phase 4 new modules.
- **Heavy refactor with key fixes preserved**: `engine.py`, `trader.py` orchestration, `ai.py`, `dashboard.py`.
- **Cleanup + extend, never rewrite**: `db.py`, `impact_tracker.py`, `news_memory.py`, `watcher.py`.

The bug-persistence risk is moderated by **three independent gates**:

1. **Phase 0.1 paper accuracy audit** — empirical check against 30
   days of closed trades. Catches both persistent bugs (numbers
   match old wrong values) and rewrite bugs (numbers diverge from
   realized history).
2. **Phase 3 offline backtest within ±10% of historical PnL** —
   end-to-end correctness check across the rebuilt pipeline.
3. **Replay regression suite (row-id gap = 0)** — asserts the new
   code writes the same row count and IDs as historical replay,
   catching schema/ordering bugs.

If a module is REWRITTEN, its bug-persistence risk → 0 but new-bug
risk → moderate; gates 1+2 catch new bugs.

If a module is REFACTORED, its bug-persistence risk stays moderate;
gates 1+3 catch persistent bugs.

**You cannot avoid risk by choosing one approach over the other;
you can only choose which class of risk to absorb.** The plan chooses
based on whether the module's complexity is mostly *spec-derivable*
(rewrite) or mostly *production-hard-won* (refactor).

---

## Phase 1 — Sim + trader rebuild from specs (no soak)

**Goal**: Fix every sim bug from Phase 0. Where the cleanest path is a
rewrite, rewrite. Where it's a targeted fix, fix.

**Sub-agent fan-out** (5 agents, 2-3 per session batch):

| ID | Scope | Deliverable |
|---|---|---|
| A1.1 | `simulate.py` rewrite/fix | ≤ 250 lines. Slippage from `price_impacts` regression, fee schedule from Polymarket docs, share quantization with rail-snap, fractional-Kelly sizing. Tests in `seulgi/app/tests/test_simulate_fidelity.py`. |
| A1.2 | `trader.py` rewrite or refactor | Target ≤ 800 lines. Pure `evaluate(match, classify, cfg) → Trade or Skip` + `close_position(pos, current_price, cfg) → Close or Hold`. No DB calls inside evaluate; caller batches writes. |
| A1.3 | `engine.py` refactor | Target ≤ 700 lines. Stages renamed INGRESS → SIGNAL → SIM_GATE → PREDICT → FAN_OUT → WRITE. Single `[stage]` log prefix. |
| A1.4 | `db.py` cleanup + migration | Drop `lessons`/`lesson_notes` tables (archive → `data/archive/`). Add `(engine_name, closed_at)` index on trades. Add Phase-2 tables (`position_price_snaps`; bid/ask cols on `market_price_snaps`). |
| A1.5 | Delete `plotter.py` + `dashboard.py` cull | `rm plotter.py`; strip its config block + env. Cull dead writers; target dashboard.py ≤ 1,400 lines. |

Verification (no bot run, no LLM):
- `pytest seulgi/app/tests/ seulgi/tests/` green.
- `docker compose --profile test run --rm seulgi-replay` exit 0.
- Sample: for 10 random closed trades, new `simulate.apply_slippage` within 5% of realized `delta_t5m`.
- `seulgi/architecture.md` updated.

**Exit criteria**: `wc -l seulgi/app/*.py` ≤ 10,500. All tests green. Specs updated.

---

## Phase 2 — Capture + offline backtest harness (no soak)

**Goal**: Wire capture writes that block backtest, then build the
backtest itself. All offline; backtest reads cached LLM responses.

**Sub-agent fan-out** (5 agents):

| ID | Scope | Deliverable |
|---|---|---|
| A2.1 | `llm_calls` writer in `ai.py` | One row per ensemble sample: prompt_sha, response_text ≤8KB, tokens, parse_ok. Enables zero-cost replay. |
| A2.2 | Continuous price tape | Extend `impact_tracker.py` to write `position_price_snaps` at +5m/+15m/+30m/+1h/+4h. Extend `_prefetch_prices` to record bid/ask. |
| A2.3 (Plan) | Architect `scripts/replay/backtest.py` | Inputs: `--since/--until/--variant-config/--engines`. Walks `decision_inputs`, joins cached `llm_calls` by prompt_sha, re-runs new `trader.evaluate`, joins exits to `position_price_snaps`. Outputs `runs/<run_id>/{manifest.json, trades.parquet, summary.md}`. |
| A2.4 | Implement A2.3 | Per Plan spec. |
| A2.5 | `diff_variants.py` + capture integration test | Compares two backtest runs. Integration test asserts every Phase-5 capture row writes. |

Verification:
- Integration test in `test_pipeline.py` with mocked LLM runs one headline.
- Sampling: `backtest.py` over 3-day window produces sane non-zero summary.
- Fallback documented: if `llm_calls` historical coverage is sparse, use `engine_results.classify_result` as oracle.
- `seulgi/architecture.md` updated.

---

## Phase 3 — Offline validation (no soak)

**Goal**: Prove rebuilt sim + trader produce PnL within ±10% of
historical paper trades.

**Sub-agent fan-out** (4 agents, parallel):

| ID | Scope | Deliverable |
|---|---|---|
| A3.1 | Backtest historical 14d with current config | Per-engine PnL vs historical. Identify divergences > 10%. |
| A3.2 | Backtest with calibration enabled | Same window with calibration curve from closed trades. `trader.edge = increment × (2·c_calibrated − 1)`. |
| A3.3 | Backtest with proposed 1-D variant grid | 6-8 strict 1-D mutants from `balanced/conf40_tp025_sl045` baseline. Top 3 by PnL with CI. |
| A3.4 | Cross-check audit | Re-run `paper_accuracy_audit.py` on the same window. All checks ≥ 99%. Cross-reference A3.1 divergences. |

Verification:
- All 4 backtests from same DB snapshot.
- `reports/<date>-variant-grid-backtest.md` committed.
- Human-eyeball top 20 backtest-vs-history divergences for root cause.

**Exit criteria**: A3.1 within ±10% per engine. Variant grid committed to `config.yaml`. `architecture.md` updated.

---

## Phase 4 — Structural + logic reimagining (no soak)

**Goal**: Beyond fixing bugs — re-architect the runtime, calibrate the
quant, decouple the backend, and replace brittle heuristics. Each
idea is **designed + backtested offline before any live deployment**.

Phase 4 is organized as **five workstreams (4A–4E)**, four of which
follow the user's explicit technical architecture directives, plus a
fifth that absorbs the stats/matching/prompting/counterfactual items.

**The discipline**: every reimagining proposal ships as a "candidate
variant" in the backtest harness. If a variant doesn't beat the
calibrated baseline in offline backtest, it's not promoted. Live
testing is reserved for the variants that survive offline filtering.

**Important pre-requisite**: Phase 1 paper-trade simulator rebuild
MUST be done before Phase 4 starts. The Phase-2 backtest harness must
be extended in Phase 4 to **simulate the new runtime behavior**: BBO
refetch at execute-time (from `position_price_snaps` if mid-density,
else from a +0s ingress snap), IOC/FOK fail simulation against book
depth, async-queue write reorderings (write order = telemetry order,
not trade-execution order).

---

### Phase 4A — Systems fix: latency + state (3 agents)

Restructure into a **Fast Path** (trade-critical) and an
**Async Event Loop** (logging + analytics). No static prices on the
critical path; live BBO refetch at execute-time.

| ID | Scope | Deliverable |
|---|---|---|
| A4A.1 | **Filter reordering** | Move the capital-check (`available_capital < min_trade`) to **stage 0a**, BEFORE SBERT embedding and DB writes. Today it's checked after embedding (engine.py:266). Reorder engine.py pipeline; any signal failing 0a short-circuits with zero LLM/embed cost. |
| A4A.2 | **JIT pricing + IOC/FOK execution** | Stop passing static mid through the LLM gate. The LLM emits raw probability $p$ on the snapshot's view of the world; **right before `open_position`**, refetch the top-of-book (BBO) from the CLOB. Recompute EV against the live BBO; if still positive, fire as an **IOC** (Immediate-Or-Cancel) limit order with the trader's max acceptable price. If the market maker moved, IOC fails safely (no slippage worse than spec). Spec in `seulgi/app/execution.py` (NEW). In paper mode: simulate the IOC fail using `position_price_snaps[t=0]` vs `decided_at`-mid delta; reject the fill if delta > tolerance. |
| A4A.3 | **Decoupled price fetching** | Replace the static-mid pass-through (engine.py `_prefetch_prices`) with order-book state: capture `{bid, ask, bid_size, ask_size, book_depth_at_5_levels}` per match. LLM gate sees the spread + depth; trader sees the same. Foundation for A4D.1 (spread filter). |

---

### Phase 4B — Quant fix: calibration + edge (3 agents)

LLM "confidence" is not probability. Wrap it. Use real EV math.

| ID | Scope | Deliverable |
|---|---|---|
| A4B.1 | **Probability calibration** | Treat raw LLM `p_alpha` (and ensemble `confidence`) as features, not probabilities. Fit **Platt scaling** (logistic regression) AND **isotonic regression** nightly from `(closed_trades.confidence_raw, hit:=pnl>0)` pairs. Per-direction (YES, NO) split. Cache curve in memory, refresh every 4h. Expose `db.calibrate(c_raw, direction) → p_true`. Goes in `seulgi/app/calibration.py` (NEW). |
| A4B.2 | **True EV formula** | Replace `edge = increment × (2·conf − 1)` (trader.py:835) with: `EV_yes = p · (1 - P_ask) - (1 - p) · P_ask` (and the NO mirror). Only proceed to sizing if `EV > min_edge_threshold + fee_rate · 2` (round-trip fees). Update `trader.evaluate` to use the new formula end-to-end. |
| A4B.3 | **Fractional Kelly with hard cap** | Current `simulate_sizing.method == "fractional_kelly"` uses a per-engine multiplier; many variants run un-capped. New rule: **cap fractional Kelly at f∈{0.05, 0.10}** to absorb tail risk the LLM cannot foresee. Use the calibrated $p$ and exact payout odds in the Kelly formula. Spec in `simulate.py` + per-engine config. |

---

### Phase 4C — Backend fix: DB decoupling (2 agents)

The critical path must not block on a 400-row SQL transaction.

| ID | Scope | Deliverable |
|---|---|---|
| A4C.1 | **Critical-path slimming** | New `seulgi/app/fastpath.py`. The trade-critical sequence is exactly three steps: (1) calculate EV against live BBO, (2) call the exchange API (or simulator), (3) update in-memory state (free_cash, open_positions, exposure_by_condition). Nothing else on this path. Move all `record_engine_decisions`, `insert_signal`, and telemetry writes OFF the critical path. |
| A4C.2 | **Async telemetry queue** | Introduce `asyncio.Queue` (in-process; no Redis dependency for v1) consumed by a single background worker that batch-inserts to `engine_results`, `decision_inputs`, `trades`, `llm_calls`. Queue size bounded; backpressure drops the OLDEST telemetry row (never the trade-fast-path data). If the worker crashes or a write fails, the live trading loop continues; only logging is degraded. Surface queue depth + drop count in `db_health.txt`. |

---

### Phase 4D — Logic fix: dynamic heuristics + smart routing (3 agents)

Replace brittle binary rules with continuous, regime-adaptive checks.

| ID | Scope | Deliverable |
|---|---|---|
| A4D.1 | **Spread + book-depth filter replacing hour_gate** | Delete the hardcoded `hour_gate` (hours 23/00/01 UTC skip). Replace with: skip iff `spread_bps > max_spread_bps` OR `book_depth_top_5 < min_book_depth_usdc`. With A4A.3 capturing book state, this is computed per-trade. Late-night breaking news is no longer blinded; it's gated on liquidity reality. |
| A4D.2 | **Tag-based routing over cosine similarity** | A cheap pre-classifier (keyword map + small distil-bert if needed) tags each headline with macro categories: `{"interest_rates", "geopolitics", "elections", "sports", "weather", "tech", "crypto", "earnings"}`. Build a `tag → market_bucket` routing table (manual seed + active-learning over time). When the headline has a tag, those markets bypass the SBERT cosine gate and go straight to the Prediction LLM. Macro headlines (Fed, FOMC, central banks) stop dying at the cosine gate. Goes in `seulgi/app/tagger.py` (NEW). |
| A4D.3 | **Position scaling replacing duplicate_position lockout** | Today `duplicate_position` is binary (trader.py:873–880): once you hold any size on a (condition_id, engine), you can't add. Replace with: `current_exposure = sum(size_usdc WHERE condition_id=X AND engine=Y)`; if Kelly's new optimal is `kelly_size`, allow a `delta = max(0, kelly_size - current_exposure)` buy. Weighted-average SL/TP across the existing positions in that condition. |

---

### Phase 4E — Statistical scoring + matching + LLM (5 agents)

The reimagining items not subsumed by 4A-4D.

| ID | Scope | Deliverable |
|---|---|---|
| A4E.1 | **Stats-based scoring (composite signal)** | `seulgi/app/scoring.py` (NEW). Fit logistic regression on `(similarity, confidence_raw, p_alpha, freshness_dist, freshness_age, mid_yes, market_vol_30m, hour_utc, agreement_fraction, headline_age_secs, source_host, tag_one_hot, spread_bps, book_depth) → P(profitable_trade)`. Trader's entry gate uses this score (composed with 4B.2 EV). Cross-validated on closed trades. IC measured. Pre-AI rules whose statistical effect duplicates the model get retired. |
| A4E.2 | **News→market matching beyond cosine** | (a) Cross-encoder re-ranker fired on top-K matches when bi-encoder similarity ∈ [0.3, 0.55] band; (b) Learned matcher: small MLP on `(headline_emb, market_emb) → was_executed_profitably` from closed-trade labels; (c) Topic-routing prior (overlaps with 4D.2 tagger). Each backtested separately; promote the winner. |
| A4E.3 | **Market discovery beyond polymarket** | NEW `seulgi/app/discovery.py`. Adapters for Kalshi (~2k markets, different topic mix) and Predictit. Active discovery loop: when AI emits "relevant but no good match" + a tag, search polymarket/kalshi by keyword + queue for next ingress cycle. Per-market quality scoring (liquidity, volume, days-to-resolution) gates the universe at retrieval. |
| A4E.4 | **LLM result processing + prompting upgrade** | (a) Migrate `ai.py` from `response_format=json_object` to `json_schema` for the prediction LLM; consider function-calling. (b) Regex-extract probabilities as fallback (don't fail-open, fail-extract). (c) Tighten ensemble: require ≥ 2/3 agreement on direction or demote to NEUTRAL. (d) Add `<scratch>` chain-of-thought block discarded after parse (per memory `feedback_reasoning_budget`: spend on thinking, constrain output). (e) Few-shot exemplars from recent EXECUTED+profitable trades. (f) Per-market-type prompts. (g) Smaller batches (25 → 5-10). Each backtested for token-cost vs hit-rate trade-off. |
| A4E.5 | **Counterfactual sampling** | Wire into top-5 EV filters: 1% of would-have-skipped signals get through with tag `cf_bypassed_filter`. Closed outcomes feed per-filter marginal-EV report. Spec in `seulgi/app/counterfactual.py` (NEW). |

---

**Discipline rule**: each agent writes a *proposal* + *backtest result*
+ *go/no-go* in `reports/<date>-reimagine-<theme>.md`. Only proposals
with positive offline backtest delta get promoted to live in Phase 5.

**Aggregate report**: `reports/<date>-reimagine-summary.md` ranks all 16
proposals by offline-backtest PnL delta + cost. The top set is what
ships to F1.

Verification (all offline):
- Each agent's deliverable includes a backtest run vs the calibrated
  baseline from Phase 3.
- Aggregate report `reports/<date>-reimagine-summary.md` ranks all 8
  proposals by offline-backtest PnL delta.
- `seulgi/architecture.md` updated with whichever proposals are
  *adopted* (not just proposed).

**Exit criteria**:
- Top 3-5 reimagine proposals promoted to live in Phase 5.
- All proposed-but-not-adopted ideas archived in
  `reports/reimagine-archive/` with notes on why offline backtest
  didn't justify them.

---

## Phase 5 — Live smoke + regression hold (the only soaks)

**Goal**: Confirm the rebuilt + reimagined code runs against live news.

**Soak F1 (4h smoke)** — Confirm new code is alive.
- Start with rebuilt sim/trader + adopted Phase-4 changes + new variant grid.
- Watch: gather → evaluate → record_engine → insert_trade chain fires;
  no transaction errors; `paper_accuracy_audit.py` green at 4h mark.
- Pass: ≥ 50 closed trades, audit green, no `err > 0` on writers.
- Fail: root-cause, DO NOT proceed to F2.

**Soak F2 (24h regression hold)** — Profitability evidence + drift.
- Code freeze.
- Pass criteria (any 2 of 3):
  - ≥ 500 closed trades total.
  - ≥ 1 engine with positive realized PnL at p < 0.10 (one-sided t-test).
  - Realized PnL within ±15% of Phase 3 backtest prediction per engine.
- Fail: collect data, root-cause offline, propose Phase-1.x fix list, re-run F1+F2.

Total soak budget: **4h + 24h = 28h**.

---

## Phase 6 — Steady-state cadence (post-plan)

Once F2 passes:
- **Daily** (cron): `audit_lifecycle.py` + `paper_accuracy_audit.py`.
- **Weekly** (manual): `scout_funnel.py --hours 168` + a backtest
  against the proposed loosen/tighten. Changes go through Phase-3-style
  offline validation BEFORE paper.
- **Monthly**: re-run calibration fit, re-check 30-day audit.
- **Quarterly**: rule revalidation, unused-rule purge.
- **Continuous**: `seulgi/architecture.md` updated by every PR that
  changes module behavior.

Out-of-scope until F2 passes:
- Live trading (`SEULGI_LIVE=1`).
- Market universe expansion beyond what Phase 4 A4.4 delivers.
- Concept-graph LLM mining at scale (cheap version OK in Phase 4).

---

## Critical files (post-plan target)

| File | Now | Target | Phase |
|---|---:|---:|---|
| `db.py` | 2,652 | 2,200 | 0 spec, 1 cleanup, 2 new tables |
| `engine.py` | 1,072 | 700 | 0 spec, 1 refactor, 4A pipeline reorder |
| `ai.py` | 810 | 900 | 0 spec, 2 llm_calls writer, 4E prompt+schema rework |
| `trader.py` | 1,432 | 800 | 0 spec, 1 rewrite, 4B EV+calibration integration |
| `simulate.py` | 199 | 250 | 0 spec, 1 rewrite, 4B Kelly cap |
| `impact_tracker.py` | 655 | 500 | 0 spec, 2 continuous tape |
| `dashboard.py` | 1,914 | 1,400 | 0 spec, 1 cull |
| `plotter.py` | 1,404 | 0 | 1 delete |
| `markets.py` | 407 | 450 | 0 spec, 4E matching rework |
| `news_memory.py` | 302 | 302 | 0 spec |
| `execution.py` | — | 250 (NEW) | 4A JIT pricing + IOC/FOK |
| `fastpath.py` | — | 200 (NEW) | 4C critical-path slim |
| `calibration.py` | — | 200 (NEW) | 4B Platt + isotonic |
| `tagger.py` | — | 250 (NEW) | 4D tag-based routing |
| `scoring.py` | — | 250 (NEW) | 4E composite signal |
| `discovery.py` | — | 300 (NEW) | 4E Kalshi/Predictit + active discovery |
| `counterfactual.py` | — | 200 (NEW) | 4E |
| `config.yaml` | (post-clean) | new grid + spread/depth knobs | 3 grid, 1 migration, 4A-D knobs |
| `architecture.md` | — | living | 0 create, every phase updates |
| `master-plan.md` | — | this file | 0 commit |

Total app code target after Phase 4: **15,417 → ~11,000-12,000 lines**
(adds back ~1,650 for new modules, still net ~3,500 line reduction
with massively more capability per line).

---

## Agent count summary

| Phase | Parallel agents | Theme |
|---|---|---|
| 0 | 13 | Forensics + specs + arch doc |
| 1 | 5 | Sim + trader rebuild |
| 2 | 5 (1 Plan + 4 gen-purpose) | Capture + backtest harness |
| 3 | 4 | Offline validation |
| 4A | 3 | Systems fix (latency + state, JIT pricing, IOC, decoupled book state) |
| 4B | 3 | Quant fix (calibration, EV, Kelly cap) |
| 4C | 2 | Backend fix (critical-path slimming, async telemetry queue) |
| 4D | 3 | Logic fix (spread/depth filter, tag routing, position scaling) |
| 4E | 5 | Statistical scoring + matching + market discovery + LLM upgrades + counterfactual |
| 5 | 0 (live soaks) | — |

Total **~43 agent runs**, batched 3-5 per session.

---

## "No-LLM" workflow per the user's tip

The user observed: *"you might not even need to turn on the LLM... you
can do your own sampling and reporting."*

What this unlocks:
- **Phase 0** is 100% offline (specs + audits from code + DB sampling).
- **Phase 1** is 100% offline (unit tests + replay harness).
- **Phase 2** is 100% offline except one mocked-LLM integration test.
- **Phase 3** is 100% offline (backtest reads cached `llm_calls` or
  falls back to `engine_results.classify_result`).
- **Phase 4** is 100% offline (each reimagine proposal backtested
  against historical data; statistical models fit on closed trades).
- **Phase 5** is the ONLY live phase: 4h smoke + 24h regression.

If `llm_calls` historical coverage is sparse: Phase 2 documents what's
available, Phase 3/4 fall back to `engine_results.classify_result` as
the oracle, and Claude does its own sampling of recent closed trades
to validate the rebuilt logic.

---

## Verification per phase (offline-first)

**Phase 0**:
```bash
ls seulgi/app/*.py | xargs -I{} head -80 {} | grep -c "## Specification"
# ≥ 19
cat seulgi/architecture.md | head -5
ls reports/$(date +%Y-%m-%d)*phase0*.md
```

**Phase 1**:
```bash
wc -l seulgi/app/*.py | tail -1   # ≤ 10,500
pytest seulgi/app/tests/ seulgi/tests/
docker compose --profile test run --rm seulgi-replay
```

**Phase 2**:
```bash
pytest seulgi/app/tests/test_pipeline.py -k capture
python3 -m scripts.replay.backtest --since 2026-05-20 --until 2026-05-22 --variant-config config.yaml --out-dir /tmp/bt
```

**Phase 3**:
```bash
python3 -m scripts.replay.backtest --since $(date -d "-14 days" +%Y-%m-%d) --until $(date +%Y-%m-%d) --out-dir /tmp/bt-14d
python3 -m scripts.analysis.paper_accuracy_audit --since $(date -d "-14 days" +%Y-%m-%d)
ls reports/$(date +%Y-%m-%d)-variant-grid-backtest.md
```

**Phase 4**:
```bash
ls reports/$(date +%Y-%m-%d)-reimagine-*.md   # 16 proposals across 4A-4E + 1 summary
python3 -m scripts.replay.backtest --variant-config configs/reimagined.yaml --since $(date -d "-14 days" +%Y-%m-%d) --until $(date +%Y-%m-%d)
# Compare summary.md vs calibrated baseline; only promotes positive-delta items to Phase 5

# Spot-check 4A.2 (JIT pricing) by replaying recorded trades:
python3 -m scripts.replay.backtest --variant-config configs/jit_pricing.yaml --simulate-ioc-fail --since (now-7d)

# Spot-check 4B.1 calibration curves:
python3 -m scripts.analysis.calibration_report --since (now-30d)
```

**Phase 5**:
```bash
# F1 (4h)
bash seulgi/scripts/lifecycle.sh start
sleep 14400
python3 -m scripts.analysis.audit_lifecycle && python3 -m scripts.analysis.paper_accuracy_audit --since <start>
# F2 (24h)
# code freeze, then:
python3 -m scripts.replay.backtest --since <F2_start> --until <F2_end> --variant-config config.yaml
python3 -m scripts.analysis.pnl --since <F2_start>
```

---

## Implementation step (after approval)

When the user accepts via ExitPlanMode, the first actions are:

1. Create `seulgi/master-plan.md` with the body of this plan.
2. Create `seulgi/architecture.md` (Phase 0 A0.13 will populate it).
3. Replace `~/.claude/plans/code-shipped-all-137-tranquil-turtle.md` with a stub.
4. Update `seulgi/handoffs/2026-05-22-v2.md` `plan_file:` frontmatter.
5. Fire **Phase 0** with 3-4 parallel agents per batch (4 batches to cover all 13).

Phase 1 begins in the next session once Phase 0's consolidated bug
list + arch doc are on disk.

---

## Risks and mitigations

- **Phase 0 finds the sim is more broken than diagnosable.** 13 agents
  cross-check each other; A0.1 (paper audit) vs A0.2 (simulate.py) vs
  A0.3-4 (trader) triangulate root causes from multiple angles.
- **Phase 1 rewrite introduces new bugs.** Existing test suite + replay
  harness must stay green at every step. One commit per agent.
- **`llm_calls` capture is too sparse.** Fallback to
  `engine_results.classify_result` as oracle (already stores parsed
  LLM output).
- **Phase 3 backtest doesn't match history within ±10%.** Loop back to
  Phase 1 with new findings. Don't burn soak budget.
- **Phase 4 reimagining proposals all underperform offline.** That's
  *information* — keep the calibrated baseline + accept that the next
  cycle's profitability work is data collection, not algorithm change.
  No live deployment of an offline-loser.
- **F1 4h smoke crashes immediately.** Snapshot before F1 via
  lifecycle.sh; rollback to pre-F1 snapshot, debug offline.
- **Architecture doc rots.** Discipline: every module-changing PR must
  update arch doc in the same commit; reviewer enforces.
- **Phase 4A (JIT pricing + IOC) breaks paper trades in unexpected
  ways.** Mitigation: paper mode simulates IOC failure deterministically
  using the existing `position_price_snaps[t=0]` vs decided_at-mid
  delta; offline backtest exercises both fill-success and IOC-fail
  paths. No live deployment until paper IOC behavior matches recorded
  CLOB sequences within ±2% on 14-day backtest.
- **Phase 4C (async queue) loses telemetry under load.** Mitigation:
  bounded queue with explicit drop counter; drops are alarmed in
  `db_health.txt`. Drop-the-oldest policy means trade-fast-path data
  is never lost — only stale telemetry. Replay regression suite is
  extended in Phase 2 to assert telemetry row-count tolerance after
  the queue ships.
- **Calibration overfits to the small closed-trade sample early on.**
  Mitigation: bootstrap from minimum-50-closed-trades; until then,
  fall back to identity mapping (raw conf = true p). Per-direction
  curves can be pooled if either has < 25 samples.

---

## Why this beats the previous A-H plan

- **Theme→dependency ordering**: Phase 0 produces specs → Phase 1
  reads them → Phase 2 captures around the rebuilt trader → Phase 3
  validates offline → Phase 4 reimagines on top of the validated base
  → Phase 5 ships live.
- **Soak budget**: 28h vs the old 76h — by doing every step offline
  that *can* be offline per the user's tip.
- **Parallelism**: 13 + 5 + 5 + 4 + 8 = 35 agent runs, batched 3-5 per
  session.
- **Sim-first**: profitability work doesn't start until paper-trade
  accuracy is empirically proven (Phase 0.1) AND backtest matches
  history within ±10% (Phase 3.1).
- **Reimagining is offline-gated**: every new idea ships only if its
  offline backtest beats the calibrated baseline.
- **Two living docs**: `architecture.md` (now) and `master-plan.md`
  (next) keep the codebase navigable across sessions.

The old A-H items map cleanly:

| Old item | This plan |
|---|---|
| A.1 (market universe) | Phase 4E.3 (discovery.py: Kalshi + Predictit) |
| A.2 (super_scout) | Phase 6 cadence |
| A.3 (concept graph) | Phase 4E.2 (cross-encoder + learned matcher) |
| A.4 (LLM scoring schema) | Phase 4E.4 (json_schema + function-calling) |
| B.* (cost/efficiency) | Phase 1 cull + Phase 4E.4 (smaller batches) |
| C.2 (counterfactual) | Phase 4E.5 |
| C.7 (price corpus) | Phase 2 continuous tape + Phase 4A.3 (full book state) |
| D.* (backtest) | Phase 2/3 |
| E.* (testing) | Phase 0 spec + tests, Phase 1 rebuild |
| F.* (sim realism) | Phase 1 sim rewrite + Phase 4B (calibration + EV) |
| G.* (anti-overfit) | Phase 4E.5 + Phase 6 cadence |
| H.* (structural) | Phase 1 trader/engine refactor + Phase 4C (fastpath + queue) |

The four new technical directives the user added (after the
paper-trade simulator module is done) are absorbed as:

| User directive | This plan |
|---|---|
| **1. Re-architecting for Latency and State** | Phase 4A (filter reordering + JIT pricing + IOC/FOK + decoupled book state) |
| **2. Calibrating the LLM & Fixing Edge** | Phase 4B (Platt+isotonic calibration + true EV formula + fractional Kelly cap) |
| **3. Decoupling the Database** | Phase 4C (critical-path slim + async telemetry queue + failure isolation) |
| **4. Smarter Heuristics and Routing** | Phase 4D (spread/depth filter replacing hour_gate + tag-based routing + position scaling) |

Nothing dropped. Reordered to find sim bugs first, then re-architect
the runtime + quant + backend + heuristics on top of a validated
base, then soak only at the end.
