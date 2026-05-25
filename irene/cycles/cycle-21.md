# Cycle 21 — 12h soak audit + handoff to next session

- **Commit:** (pending push)
- **Theme:** Halt-and-investigate cycle. The cycle-20 24h soak surfaced persistent alerts that didn't decay; cycle 21 is the audit + diagnostic + handoff doc.

## What landed

### Audit (3 parallel Explore agents)
- Resolution-poll investigation → poll IS running correctly; alert metric is over-counting.
- Fee-fetcher investigation → cycle-15 `_maybe_fetch_market_info` confirmed silent. 0 CLOB `/markets/<cid>` calls in 24h vs 25,846 `/midpoint` calls (trade pipeline healthy).
- Dashboard audit → 12 panels exist; none surface cycle-12 or cycle-15 background-poll health.

### Verification (live SQL + log greps)
- 1438 closed-not-finalized markets — distribution: 1105 in 48-72h, 322 in 72-168h, 11 past 7d.
- 0 open trades on State 1/2 markets — explains why the cycle-12 poll has nothing to do.
- 9024/9024 active markets `fee_rate IS NULL` — the silent-fetcher confirmation.
- 11 markets past the 7d RESOLUTION_TIMEOUT cutoff (vs 3 at the last soak tick — natural aging into the bucket).

### Diagnosis correction
The cycle-21 planning agent had a fact error: stated "all 1438 markets are past the 7d cutoff." The verification SQL showed the opposite — 1427 are inside the window. The corrected handoff reflects the right story: the poll's SQL filter works; the alert metric just over-counts the passive (no-open-trade) population.

### Handoff doc (`irene/handoffs/2026-05-25-cycle20-soak-debug.md`)
Comprehensive audit + 5-fix queue for the next session:
- **Fix A (high):** wire `clob` into `MarketsStage` in `app/main.py`. One-line bug. Defensive WARN-once log in `_maybe_fetch_market_info`.
- **Fix B:** split the `state 1/2` alert into actionable vs passive metrics.
- **Fix C:** add `market_info_status.txt` + `resolution_status.txt` dashboard panels.
- **Fix D (cosmetic):** bump stale `IRENE_CYCLE=cycle3-fresh` env to `cycle-21`.
- **Fix E (low):** audit the 11 RESOLUTION_TIMEOUT markets.

## Verified
- `pytest irene/app/tests/` → 492 passed (cycle-18 code unchanged).
- Live verification SQL + log greps documented in the handoff doc.

## Not yet (carried forward)
- The 5 fixes in the handoff — for the next session.
- The 24h soak cron `b4b3e445` is still firing every 2h; the next session should `CronDelete` it before landing fixes (or it'll keep reporting the same stale alerts).
