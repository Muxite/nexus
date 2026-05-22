# engine.py — Bug List & Pipeline Spec (Agent A0.5, 2026-05-22)

Scope: `seulgi/app/engine.py` (1,072 lines + 75-line spec block).
Method: full read + cross-ref with `handoffs/2026-05-22-v2.md`,
`reports/2026-05-22-bugs-cycle2.md`, `main.py`, `db.py`, `trader.py`,
`ai.py`. Offline.

## 1. Pipeline stages (`process()` + `_run_engine()`)

| # | Stage | File:line | Output |
|---|-------|-----------|--------|
| 0 | UTC hour gate (silent drop) | 246-250 | early return |
| 0a| Article body fire-and-forget | 254-262 | bg task |
| 0b| Embed headline | 264-270 | embedding |
| 0c| Freshness assess (NewsMemory) | 272-303 | FreshnessResult |
| 0d| Index match | 305-308 | list[MarketMatch] |
| 0e| Heat bump | 313 | `markets_heat` |
| 0f| Parallel mid-price prefetch | 319-323 | mid_yes/mid_no |
| 1 | Signal insert | 334-340 | `signals` row |
| 1a| Freshness short-circuit | 342-367 | per-engine skip rows |
| 1b| Pre-AI universal rule gate | 369-391 | survivors + pre_skips |
| 1c| Low-similarity short-circuit | 393-418 | per-engine skip rows |
| 2 | Deterministic sim-gate | 420-461 | SIM_PASS/SIM_FAIL |
| 2a| Market context fetch | 463-485 | `_mkt_ctx` on each |
| 3 | AI-paused short-circuit | 493-508 | per-engine skip rows |
| 3 | `classify_batch` (LLM) | 509-518 | ClassifyResults |
| 3a| Usage logging | 521-529 | `usage_events` |
| 3b| E.5b pause filter | 541-545 | active_engines |
| 4 | gather under 90s `wait_for` | 591-616 | results list |
| 4a| Per-engine scoped rule gate | 767-783 | engine-local survivors |
| 4b| `trader.evaluate` | 800-812 | TradeDecisions |
| 4c| `record_engine_decisions` | 814-823 | `engine_results` |
| 4d| Impact tracker (engines[0]) | 828-855 | impact rows |
| 4e| Dashboard hooks | 857-906 | dashboard files |

**Order smells.** Universal rules run before low-sim and sim-gate; the
cheap deterministic gates should run first. Capital-check
(`_update_ai_pause_state`, line 493) sits after embed + freshness +
match + prefetch + signal + heat-bump + market-context; Phase 4A.1
should move it just after the hour gate. Per-engine scoped rules
(767-783) run after `classify_batch`, so any market every engine culls
scoped-side still paid Stage-3 tokens (cycle-2 audit #12).

## 2. Dead branches

**D1.** `Engine.relevance_keep_scores` (96-100) is set in `build_engines`
(1004) and never read. Per-engine relevance re-filter was deleted cycle 2.
Drop the field + the build_engines `getattr`.

**D2.** Lines 746-752 — `rel_dropped_skips: list = []` + extend at 763 is a
permanent empty-list passthrough. Delete the comment + extend.

**D3.** `_SimVerdict.keep: bool` is never read; survivors are filtered by
tag-string. Inline `relevance_score` / `relevance_reason` onto MarketMatch
and drop the dataclass.

**D4.** `trader.evaluate(relevance_skips=...)` type (`Optional[list]`,
trader.py:425) is now only the SIM_FAIL list. Rename when D3 ships.

**D5.** `LEGACY_ENGINES_DISABLE` (962-964) — never set anywhere. Delete
unless someone documents intent.

**D6.** `MULTI_CORE_ENABLE` (968-978) short-circuits the YAML engine build
and returns early. Pick one path; today both live side-by-side.

**D7.** `aggressive_core.maybe_append` (1010-1019) has the same
`try ImportError container/app` fallback as 971-973. Factor.

**D8.** Lessons plumbing — already clean in this file (no `lesson_notes`
references). The cycle-2 bugs report claim was stale.

**D9.** Orphan `signals` rows: `insert_signal` runs at 334 before every
short-circuit. REHASH / low_similarity / ai_paused all return early
after writing the signal row → `signals` has rows with no
`engine_results`. Either keep with `freshness_short_circuit=1` column
or skip the insert on early return.

## 3. Logging cruft

- Prefix inconsistency: `[runner]` everywhere in `EngineRunner`,
  `[engine]` in `build_engines`. Unify.
- E.5b state-edge lines (188, 193) drop the `[hid=…]` prefix.
- Per-headline INFO lines (sim-gate kept, dispatch, market_context,
  hour_gate skip): 3-5 per headline × 20 engines × bursts = noisy. Drop
  sim-gate + market_context to DEBUG.
- Handoff §3 mentions gather START / END / evaluate START/END
  instrumentation. **Not in the current tree.** Land or stop citing.
- The 60-line archaeology block at 551-590 is load-bearing context but
  duplicates `reports/2026-05-22-bugs-cycle2.md §1`. Replace with a
  one-line reference.

## 4. Race conditions / async hazards

- **GC-orphaned tasks.** `asyncio.create_task` at lines 255
  (article_fetcher) and 844 (impact_tracker.record) do not retain task
  refs — Python can cancel them. Fix: shared `_bg_tasks: set` with
  `add_done_callback(s.discard)`.
- **`return_exceptions=True` everywhere** (356, 408, 499, 593, 912).
  The post-gather loop at 617-627 only logs `BaseException`; a
  CancelledError vs. AttributeError look identical. Document each
  callsite or split expected vs. unexpected.
- **90 s `wait_for` safety belt is present (line 608).** Confirms
  cycle-2 fix. Cycle-2 audit #10 claims it was bumped to 300 s — the
  tree disagrees. Trust the tree.
- **`_engine_should_pause` cold path is O(N) serial DB reads** behind
  one aiosqlite worker thread (171-197). Hot path is O(1) set lookup,
  but the first call per engine per TTL pays the slow read serially.
  Handoff §1 Layer-A measured 1.4 s × 20 engines = 28 s stall on
  cold-start. Move the read to a background warmer.
- **No CancelledError handler** in `_run_engine`. If a parent
  `wait_for` times out mid-`trader.evaluate`, the cancellation
  propagates as a generic Exception caught at 807 and decisions = [];
  the next call to `record_engine_decisions` runs on an empty list.
  Today this is benign; documenting the path matters when Phase 1
  re-adds the `db.batch()` wrap.
- **`_paused_engines` guard is NOT permanently off in the current tree.**
  Cycle-2 audit #9 claims `_refresh_engine_pause_cache` is a sleep
  stub — that function does not exist in this file; `_engine_should_pause`
  is live as written. Audit is ahead of / behind the tree.

## 5. Performance smells

- **Embed runs before any dedup** (265). A republish from the same
  site_url could be skipped via `news_memory`. Saves ~10 ms / republish.
- **Capital-check after expensive work** — see §1.
- **`_mkt_ctx_cache` is uncapped dict** (146). Add LRU cap (≈256).
- **`bump_market_heat` is awaited inline** (313); non-critical, could
  fire-and-forget.
- **`market_price_snaps` writes 25 small rows per headline forever**
  (700-721). Add archival policy or env knob.
- **Per-engine scoped rules re-evaluated for every variant** sharing the
  same `parent_root` (767-783). Memoize per `parent_root` per headline.

## 6. Coupling with trader / ai (boundary smells)

- `trader.evaluate` signature is stable but `relevance_skips: list` is
  loosely typed (`Optional[list[(MarketMatch, _SimVerdict)]]`). Promote
  to a named tuple in `trader.py`.
- `classify_batch` accepts `relevance_verdicts=` which `ai.py` only uses
  for prompt context. A `dict[condition_id, str]` would be cleaner.
- `_engine_should_pause` issues a DB query for hit-rate. That belongs in
  db.py as an O(1)-backed view or in a separate `MetricsService`, not in
  the orchestrator. **Boundary smell engine→db.**
- The impact-tracker fan-out re-derives `token_id` from `survivors`
  (834-842) when `TradeDecision` already knows it. Add
  `TradeDecision.token_id`; drop the re-walk. **Boundary smell
  engine→trader.**
- `_fetch_mid_price` imported from `trader` (58) is a CLOB-API call —
  move to a shared `clob.py` to break engine→trader unidirectional dep.
- `MarketContextConfig` is `Optional` with defensive `None`-guards at
  467-469. Make it required at construction.

## 7. Specific checks (from prompt)

- **90 s `wait_for` safety belt** — present (line 608). Cycle-2 fix
  preserved; A0.5 agrees the timeout could drop to 30-60 s once a 4 h
  soak confirms steady-state gathers < 30 s.
- **`_engine_should_pause` — O(1) set lookup?** Hot path yes (184);
  cold path no (DB read). The refresher background task referenced in
  the handoff is NOT in main.py. Phase 1 must add it or remove the
  feature.
- **`decision_inputs` capture — when?** Handoff §3 describes one row per
  signal before gather START. **Current engine.py contains zero
  references to `decision_inputs` or `insert_decision_input`.** A0.6 to
  confirm db.py has the writer; A0.5 confirms engine.py does not wire
  it. Phase 1 must wire it and shield from cancellation (cycle-2 #4).
- **Sim-gate floor (`min_top_similarity`)** — constructor default 0.45
  (130); main.py wires `cfg.markets.min_top_similarity` (0.50 in
  production). Read at construction; not hot-reloadable.

## 8. Missing wiring vs handoff (must reconcile in Phase 1)

The handoff `2026-05-22-v2.md` advertises four engine.py changes that
A0.5 cannot find in the working tree:

1. `decision_inputs` capture before gather START — MISSING.
2. Top-of-`process()` `budget_paused.flag` short-circuit — MISSING.
3. Engine-pause refresher as a background task in `main.py` — MISSING
   (only `_engine_should_pause` per-call exists).
4. gather instrumentation log lines — MISSING.

Either land them (with cycle-2 audit fixes built in: idempotent budget
flag, shielded `create_task`, batched skip writes, `INSERT OR IGNORE`
on `decision_inputs`), or update the handoff to match the tree. The
doc-vs-code divergence is itself a bug.

## 9. Phase-1 fix ordering

1. Drop `Engine.relevance_keep_scores` + the dead extend at 763 (D1, D2).
2. Move capital-check ahead of embed / freshness / match.
3. Land the four missing handoff items (§8) WITH cycle-2 fixes.
4. Wrap the three short-circuit `gather`s in `db.batch()` (cycle-2 #6).
5. Switch fire-and-forget tasks to held-ref set with done-callbacks.
6. Memoize per-engine scoped rule results by `parent_root`.
7. Harmonize log prefix; drop hot-path INFO lines to DEBUG.
8. Move `_engine_should_pause`'s DB read off the hot path (warmer task).
9. Move `_fetch_mid_price` to a shared `clob.py`.
10. Cap `_mkt_ctx_cache` (LRU 256).

End of report.
