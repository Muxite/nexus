# Cycle 11 — D1 three-state close machine + W6 pre-news drift + long-tail impact buckets

- **Commit:** (pending push)
- **Theme:** Biggest behavioural safety improvement of the resolution plan (close at the UMA-final outcome, not entry_mid) + the observability surface that distinguishes "wrong wording" vs "priced-in" vs "no readers" (W6) + impact-tracker bucket extension out to 24h.

## What landed

### D1 three-state close machine (Agent A, `plans/resolution-oracle.md`)
- **Schema** (idempotent `ALTER TABLE … ADD COLUMN`): 4 new columns on `markets` — `uma_proposed_at`, `uma_finalized_at`, `last_resolution_status`, `last_outcome_prices` (JSON-encoded).
- **Parser** (`_parse_gamma_row`): now surfaces `umaResolutionStatus`, `resolvedBy`, `acceptingOrders`, `outcomePrices`. Handles both native list and JSON-encoded-string forms for `outcomePrices` (mirrors the `clobTokenIds` quirk).
- **Upsert path** (`_maybe_stamp_resolution_transition`): stamps `uma_proposed_at` / `uma_finalized_at` only on first observation of each status (no overwrite). `last_outcome_prices` uses `COALESCE` so a transient Gamma field drop doesn't clobber a captured payout.
- **State machine** in `_close_orphaned_trades_for`:
  - **State 1** (`closed=true`, no UMA status): WAIT — log; do not auto-close. Was: close at `entry_mid` (cycle-7 behaviour).
  - **State 2** (`umaResolutionStatus='proposed'`): WAIT — within the 2h challenge window; log.
  - **State 3** (`umaResolutionStatus='resolved'` + valid `outcomePrices`): close at **YES payout** from `outcomePrices[0]` (NO sign-flip happens inside `_compute_gross_pnl` already; the caller passes YES-side mid by convention).
  - **State 3 + malformed payload**: WARN + skip (no entry_mid fallback).
  - **Human-review fallback**: market in State 1 past `cfg.markets.resolution_timeout_hours` (default 168h = 7 days) → WARN tagged `RESOLUTION_TIMEOUT`, trade stays open.
- **9 new tests** in `app/tests/stages/test_markets.py` (6 state-machine + 3 parser).
- **Orchestration gap flagged for cycle 12**: `_close_orphaned_trades_for` only fires when a market drops from the Gamma active list, but Gamma drops at `closed=true` BEFORE UMA proposes/resolves. The state machine logic is correct and tested; we need a separate poll path for closed markets to actually exercise States 2 / 3 in production.

### W6 pre-news drift + long-tail impact buckets (Agent B, `plans/latency-repricing.md`)
- **Schema** (idempotent `ALTER TABLE`): 14 new columns on `price_impacts` —
  - `pre_news_drift_15m`, `pre_news_drift_60m` (W6).
  - `price_t{7200,14400,28800,86400}s`, `delta_t…s`, `edge_realised_t…s` (long-tail buckets at 2h / 4h / 8h / 24h).
- **Pre-news drift sign convention**: `signed_drift = sign * (mid_at_published - mid_at_published_minus_window)` where `sign = +1` for YES, `-1` for NO. Mid polled from the side-specific token via `CLOBClient.get_price_history`. Positive ALWAYS means "market moved our way before we traded" — matches the existing `edge_realised` convention.
- **Bucket ladder** extended from `(60, 300, 900, 1800, 3600)` → `(60, 300, 900, 1800, 3600, 7200, 14400, 28800, 86400)`. The existing polling loop is bucket-agnostic so no code change there; only the unfilled-row query needed widening.
- `cfg.impact.max_age_hours` default raised `2.0 → 24.0` so the long-tail buckets actually get filled. `config.yaml` updated to match.
- **8 new tests** in `app/tests/stages/test_impact_tracker.py`.

### Background data collection (still running from cycle 10 + new this cycle)
- **Spread soak** (cycle 10): 12+ min in, ~300 rows so far. Will auto-stop at 30 min.
- **CLOB latency probe**: 5-min run sampling `/midpoint` p50/p95/p99 from this host. Output `/tmp/clob_latency.log`. Feeds `latency-repricing.md` E2 question.

## Verified
- `pytest irene/app/tests/` → **450 / 450 passed** (was 433 baseline; +17).
- New columns idempotent: re-running tests against pre-existing schemas does not re-execute the `ADD COLUMN` statements.
- The cycle-7 `test_disappearing_market_auto_closes_open_trades` was renamed to `test_disappearing_market_without_uma_status_does_not_auto_close` reflecting the State-1 wait behaviour.

## Not yet (carried forward to cycle 12)
- **Orchestration: poll closed-but-not-finalized markets**. Without it, States 2/3 are tested but rarely exercised in production. Likely: a `_poll_resolved_markets_for_open_trades` task that walks markets where `closed=true` AND `uma_finalized_at IS NULL` AND `now - closed_at < resolution_timeout_hours`, and refetches each one's Gamma row.
- LLM A/B probe lane (Haiku vs Gemini vs DeepSeek).
- Per-market `min_order_size` read at arm-time.
- Markets-gamma: Retry-After backoff on Cloudflare 429s.
- Refresh cadence for per-market `fee_rate` (currently first-seen only).
- Soak / probe post-analysis once both finish.
