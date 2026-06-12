# Cycle 30 — H8 slow-catalyst lane (capture-only)

**Date:** 2026-06-12
**Trigger:** retro §8 item 1 — "the one live bet: slow / narrative
catalysts … the only lane where reading beats speed (H8)." Fourth of
four post-retro cycles (27 → 28 → 29 → **30**).

**Scope decision (user, 2026-06-12): capture-only.** The lane annotates
and prioritizes explorations; it places NO wallet trades. A live
`decision_path='h8'` lane is a separate later cycle, gated on a sandbox
backtest at the cycle-29 horizons clearing `|signal| > spread + fees`
per cluster. Rationale: the days-horizon data the lane needs doesn't
exist until cycle 29 has soaked; betting before measuring is the exact
failure mode the retro documents.

## What changed

### 1. Four new gates (`app/gates/`, wired into `build_pipeline`)

1. `midband` — pure Python; pass iff `0.10 ≤ t0 book mid ≤ 0.90`
   (config min/max). `mid_unknown` is an audited reject, never a
   silent pass.
2. `already_moving` — `|mid_now − mid(24h ago)| ≥ 0.05` via one
   `get_price_history` call per token with a 1h in-process TTL memo
   (bounded at 2048 entries). Every failure path (no inputs, no
   history, no lookback sample) is an audited reject; the gate never
   raises. `build_pipeline` gained a `clob=` injection kwarg.
3. `document_heavy` — pure heuristic: triggering event type ∈ {TRIAL,
   REGULATORY_DECISION, LAWSUIT, MERGER, NOMINATION, ELECTION}
   (configurable; names from event_types.yaml) OR resolution
   description ≥ 800 chars.
4. `resolution_rules` — batched LLM gate (cloned llm_batch mechanics:
   label batching, cache/budget via the injected caller, prompt_version
   `rr-v1`). Verdict per pair: `opens = still_open AND rule_relevant`
   against the market's resolution criteria (`markets.description`).
   Model gemini-2.5-flash (locked). Runs LAST — it only sees survivors
   of the three free gates, a small slice of main-pipeline survivors
   (main pipeline passes ~5.6% of explorations; expect well under
   $0.50/day — measure in the soak report).

`Candidate` gained optional `market_mid` / `event_type` / `token_yes`,
populated only for the lane pipeline from data the explorer already has
(t0 book snapshot, events row, market row) — zero extra calls beyond
already_moving's memoized history.

### 2. Lane wiring (`ExplorerStage`)

- Second `GatePipeline` from `cfg.explorer.h8_gates` (default: the four
  gates above, in that order; kill switch `h8_lane_enabled`). Runs ONLY
  on main-pipeline survivors, after the t0 book snapshot. **Outcome
  never changes capture admission** — it writes
  `explorations.lane='h8'` (or leaves NULL) + `h8_gate_history` JSON
  (full decision audit for pass AND reject). A lane exception degrades
  to lane=NULL; admission and the row write are unconditional.
- Schema: `explorations.lane TEXT`, `h8_gate_history TEXT`, index on
  lane.
- `lane='h8'` rows get priority in cycle 29's long-horizon checkpoint
  loop (`ORDER BY lane='h8' DESC, t0_iso`).
- No CLOB wired → lane disabled with a loud one-time warning.

### 3. Deferred by design (documented, not built)

- **t0 = catalyst:** kept as exploration-creation time — for
  filings/rulings the triggering article IS the catalyst drop, so
  article-arrival t0 is the right measurement anchor.
  `events.scheduled_at` extraction (pre-positioning on scheduled
  rulings) belongs to the live-lane cycle: it costs LLM budget and
  capture-only doesn't need it.
- **Reserved dense-capture slice for h8 rows:** the admission cap check
  runs before the lane pipeline (lane-ness isn't known yet), so a
  reserved slice needs restructuring. At current volumes the cap
  rarely binds; revisit with the live-lane cycle if
  `evicted_by_cap` ever hits lane rows.
- **Closer days-profile** (`decision_path='h8'`, timeout ≥ 96h,
  stagnation off, wide stop): live-lane cycle, after the sandbox
  verdict.

## Exit criterion for the whole post-retro arc

After ~2 weeks of cycle-29 + cycle-30 data: sandbox backtest at the
+4h/+1d/+2d/+7d horizons, PnL per cluster, fills at bid/ask with real
fees (all instruments fixed in cycles 27–28). Only if it clears the
friction inequality does `decision_path='h8'` ever touch the wallet.

## Verification

863 tests green. New: per-gate units (boundary mids, move thresholds,
memoization, malformed-LLM fallbacks, fail-fast dispatch), explorer
integration (lane annotated on pass; lane reject keeps admission +
audit; lane never runs on rejected rows; kill switch; free gates
short-circuit the paid gate). Production config.yaml/explorer.yaml
parse with the new defaults. Post-deploy soak: lane volume, h8 gate
funnel, LLM spend after 48h → dated report.
