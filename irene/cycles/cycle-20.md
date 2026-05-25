# Cycle 20 — production rebuild for the 24h soak + monitoring harness

- **Commit:** (pending push)
- **Theme:** Take the cycle-7 production container, replace with cycle-18 code, validate the migrations, set up automated 2h-cadence health checks for 24h.

## What landed

### Production rebuild
- Stopped + removed the existing `irene` container (was running cycle-7 code, `Up 3 hours` at restart time).
- `docker compose -f irene/docker-compose.yml build` + `up -d` against cycle-18 code.
- All 11 new schema columns applied on first boot:
  - `markets.fee_rate`, `min_order_size`, `uma_proposed_at`, `uma_finalized_at`, `last_resolution_status`, `last_outcome_prices`, `fee_rate_fetched_at`
  - `trades.decision_latency_ms`, `decision_latency_breakdown`
  - `decisions.decision_latency_ms`, `decision_latency_breakdown`
- (Schema for `price_impacts` `pre_news_drift_15m/60m` + 12 long-tail bucket columns also applied — cycle 11 + 14.)
- Container `Up 2 minutes` post-restart, no boot errors, scraper-v2 feed traffic healthy.
- Open trades pre-restart: 2; preserved unchanged. The cycle-11 wait-don't-close behavior protects them on the markets-disappear path.

### Soak check script (`irene/scripts/soak_check.py`)
- Single re-runnable Python script. Outputs structured `[ok] / [ALERT]` lines + a SUMMARY counting alerts.
- Checks 10 dimensions:
  1. Container alive (docker ps)
  2. Decision rate last 2h (alert if < 50)
  3. Execute / reject mix + top 4 reject reasons
  4. Open trades count (alert if ≥ 30)
  5. Closures last 2h with avg PnL per `close_reason`
  6. `trades.decision_latency_ms` p50 / p95 (alert if p95 ≥ 5000 ms)
  7. Markets stuck in State 1/2 (closed, no UMA finalize) — should decay over time
  8. RESOLUTION_TIMEOUT markets (>7d closed, no finalize) — should be 0
  9. fee_rate fetch coverage (alert if ≥ 50% active markets NULL)
  10. ERROR-level lines in last-2h container logs (alert if ≥ 50)
- Idempotent. Appends to `/tmp/irene_soak_24h.log`.

### 0h baseline
3 expected post-restart alerts:
- `markets state 1/2: 1436` — the cycle-12 resolution poll has 1436 closed-not-finalized markets to walk on the first poll cycle. Will decay over time as the poll catches up.
- `RESOLUTION_TIMEOUT: 2` — 2 legacy stuck markets pre-dating cycle 11. Pre-existing data, not new behavior. Human review noted.
- `fee_rate coverage: 9024/9024 NULL (100%)` — every market needs first-seen fetch. The cycle-15 logic does this serially in the upsert path (~15 min for 9024 at ~100ms/call); will drop to single-digit % within the hour. Expected one-time burst per `cycle-15.md`.

All non-alert dimensions: green. 2171 decisions in pre-restart 2h window (13 execute / 2158 reject); 12 stagnation closures with $0 avg PnL (cycle 7 close path); 2 open trades; container up; 4 ERROR-level log lines (CLOB 404s on resolved-market tokens — handled gracefully by cycle 14 impact_tracker retry-next-poll).

### Two plan stubs filed
- `irene/plans/source-quality-weighting.md` — addresses 2026-05-25 user request: weight sources by importance (some sites breaking-news-heavy, others noise). Three-phase design: observability → weighting → feedback loop. Hard deferred to post-soak data accumulation.
- `irene/plans/asymmetric-stop-loss.md` — addresses 2026-05-25 user request: tight stop-loss + let winners run + DB-driven wrong-hypothesis pattern-mining. Four-phase design touching `cfg.closer.stop_loss_pct`, edge_decay vs trailing-stop, an analysis CLI, and feedback into source-quality. Earliest action: cycle 21+, gated on the soak producing ≥100 closed trades.

### Cron schedule
- Every 2h at :07 past the hour, in local time. 12 fires over 24h. Durable (`.claude/scheduled_tasks.json`) so it survives session restarts.
- Each fire runs `soak_check.py` and reports alerts. Recurring auto-expires at 7 days per the Cron tool's hard limit.
- The cycle-17 sub-$30k spread soak is killed (was competing for Polymarket API quota with the bot; collected 271 rows / 8 cycles before kill — enough to confirm spreads stay tight even below $30k; will fold into a future analysis report).

## Not yet (carried forward to cycle 21+)

- Implement `plans/source-quality-weighting.md` Phase A (observability view).
- Implement `plans/asymmetric-stop-loss.md` Phase A (turn on `stop_loss_pct` with empirical threshold from soak data).
- Audit the 2 RESOLUTION_TIMEOUT markets — what are they, do they need manual close, were they ever open positions of ours?
- After 24h soak completes: write the cycle-21 retrospective with measured trade outcomes + reject-rate diff vs the cycle-7 baseline.

## Files
- `irene/scripts/soak_check.py` (NEW)
- `irene/plans/source-quality-weighting.md` (NEW stub)
- `irene/plans/asymmetric-stop-loss.md` (NEW stub)
- `irene/cycles/cycle-20.md` (this)
- Container rebuild: no code-file changes; rebuild picked up cycles 8-18 commits.
