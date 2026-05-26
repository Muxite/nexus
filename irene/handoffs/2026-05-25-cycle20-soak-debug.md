# 2026-05-25 — cycle 20 soak debug + cycle 21 handoff

> Sibling of `irene/handoffs/2026-05-25.md` (the prior cycle-7 handoff); disambiguated by the `-cycle20-soak-debug` suffix. Companion: `irene/cycles/cycle-20.md` (soak setup) + `irene/cycles/cycle-21.md` (this audit, pending commit).
>
> **Producing plan:** `~/.claude/plans/pure-hatching-corbato.md` (cycle 21 audit + fix queue).

## TL;DR

12h soak of the post-cycle-18 production container surfaced **two real production issues** and **one alert-design issue**. None destructive; no data loss.

- **Real bug:** the cycle-15 per-market `fee_rate` / `min_order_size` fetcher hasn't fired ONCE in 24h. 9024/9024 active markets remain `NULL`. Most likely: `MarketsStage` is getting `deps.clob = None` and `_maybe_fetch_market_info` silently early-returns.
- **Real diagnostic gap:** the soak check's `markets state 1/2` alert metric over-counts. The cycle-12 resolution poll IS running correctly; the count is "stuck" because the eligible subset (closed markets with open trades inside the 7d window) is genuinely empty most of the time.
- **Dashboard blind spot:** 12 panels exist; none surface cycle-12 or cycle-15 background-poll health. An operator reading `dashboard.txt` sees "0 executes" with no diagnostic for "fee_rate fetcher is dead and 1427 markets are awaiting UMA".

The bot is still safe to run on cycle-18 code. Production trades default to `cfg.trade.default_fee_rate = 0.10` (cycle 16) when per-market `fee_rate IS NULL`, so the closed-loop fee math is **conservative** — we're over-estimating fees on most markets (cycle-15 audit showed 17.8% are at 0% taker).

## What happened over 12h

| Tick | Time (UTC)        | Decisions/2h | Executes | Open | Closes | Alerts | Note                       |
|------|-------------------|-------------:|---------:|-----:|-------:|-------:|----------------------------|
| 0    | 09:25 baseline    |       (pre)  |        — |    2 |     12 | 3      | post-restart inventory     |
| 1    | 10:37             |        2171  |       13 |    2 |     12 | 3      | execute=13 stable          |
| 2    | 12:37             |        2325  |       13 |    2 |     12 | 3      | execute=13 stable          |
| 3    | 14:37             |        2605  |       13 |    2 |     12 | 3      | execute=13 stable          |
| 4    | 16:37             |        2903  |       13 |    2 |     12 | 3      | ERROR=0 first              |
| 5    | 18:37             |        3108  |       13 |    2 |     12 | 3      |                            |
| 6    | 20:37             |        3401  |       15 |    3 |     13 | 3      | **first non-stagnation close: timeout @ −$1.67** |

Cron `b4b3e445` (session-local, every 2h at :07) fired all 6 ticks. Each tick wrote a `SUMMARY:` line to `/tmp/irene_soak_24h.log`. Alert COUNT stayed at 3 throughout; alert COMPOSITION was identical every tick (RESOLUTION_TIMEOUT was 2 at baseline → 3 by tick 1 and held).

Live-verified at handoff time:
- Past-7d closed-unfinalized: **11 markets** (up from 3 at tick 6 — natural aging into the bucket as time advances past the 7d cutoff for markets closed in mid-May).
- Open trades: **3** (up from 2 — first new execute past baseline at tick 6).
- CLOB `/midpoint` calls in 24h: **25,846** (trade pipeline healthy).
- CLOB `/markets/<conditionId>` calls in 24h: **0** (fee fetcher dead).

## Issue 1 — fee_rate fetcher silent (real production bug)

### Evidence
- `markets.fee_rate IS NULL` for **9024/9024 active markets** after 11h uptime.
- `markets.fee_rate_fetched_at IS NULL` for the same 9024.
- Docker logs across 24h: **0 calls** to `clob.polymarket.com/markets/<conditionId>`. **25,846 calls** to `/midpoint` (so the CLOB host is reachable and the broader trade path uses it heavily — the cycle-15 path specifically is dead).
- Code path is wired correctly: `markets.py:_upsert_market` awaits `_maybe_fetch_market_info(condition_id)` on every upsert. `_maybe_fetch_market_info` early-returns silently when `self._clob is None`:

```
# markets.py around lines 1184-1185 (Explore agent finding)
if self._clob is None:
    return
```

### Most likely root cause
**`MarketsStage.__init__` is being constructed with `deps.clob = None`.** The cycle-10 / cycle-15 code paths assume `clob` is wired; the wiring update for `MarketsStage` may never have landed.

Other CLOB-using stages (`TradeStage` writes `/midpoint`, `CloserStage` writes `/midpoint`, `ImpactTrackerStage` writes `/midpoint`) are clearly getting `clob` correctly. `MarketsStage` is the odd one out.

### What the next session should check
1. **`irene/app/main.py`** — find `wire_pipeline(...)` or the equivalent and grep for `MarketsStage(`. Confirm whether `clob=...` is passed in `StageDeps` for that stage specifically.
2. **`irene/app/deps.py`** — is `StageDeps.clob` an `Optional[CLOBClient]`? If yes, MarketsStage's constructor pattern may be incidentally pulling `None` because it doesn't require a clob the way Closer / Impact do.
3. **Reproduce in isolation:** `docker exec irene python3 -c "from app.main import build_deps; import asyncio; print(asyncio.run(build_deps()).clob)"` to confirm whether `deps.clob` is `None` post-construction.
4. **Defensive add:** in `_maybe_fetch_market_info`, log a WARNING (once per process via a class-level flag) when `self._clob is None` so this never silently happens again.

### Expected behaviour post-fix
After fix + container restart: ~9024 CLOB `/markets/<cid>` calls in the first 15-20 min (one per market, serial), then `fee_rate` populated on ~95% of active rows within the hour. `soak_check.py` dimension-9 (`fee_rate coverage`) drops from 100% NULL to <5% NULL.

## Issue 2 — state 1/2 alert is an over-count, not a code bug

### Evidence
- 1438 markets total: `closed_at IS NOT NULL AND uma_finalized_at IS NULL`.
- Age distribution: 1105 in 48-72h, 322 in 72-168h, 11 past 7d.
- The cycle-12 `_resolution_poll_loop` (`markets.py:842-854`) requires **open trades on the market within the 7d window**: `JOIN trades t WHERE c.id IS NULL AND m.closed_at >= cutoff AND m.uma_finalized_at IS NULL`.
- **0 open trades on State 1/2 markets** (verified via SQL). Of our 3 currently-open trades, none are on closed markets.
- Loop runs every 10 min, returns 0 rows, logs nothing, sleeps. **Working as designed.**

### Why the alert metric is wrong
`soak_check.py` dimension 7 counts ALL `closed_at IS NOT NULL AND uma_finalized_at IS NULL` rows. That includes the entire legacy / passive population, not just the actionable subset.

Better split:
- `state_1_2_with_open_trades` — actually-actionable. Currently 0. Alert if non-zero AND poll isn't draining it.
- `state_1_2_passive` — closed markets we don't hold positions in. Reframe as a `legacy_uma_backlog` stat (informational), not an alert.

### Side effect of the original design
The cycle-12 poll's "needs at-least-one-open-trade" gate means UMA finalization is NEVER stamped for markets we don't hold. The `markets` table accumulates closed-not-finalized rows indefinitely. That's fine for safety (we don't waste CLOB calls on markets we don't care about), but it inflates the soak metric.

### What the next session should change
- `irene/scripts/soak_check.py` dimension 7: split the count; demote passive to non-alerting.
- Optionally: a periodic "passive prune" job that walks the legacy population on a much slower cadence (e.g. 1h) just to keep `uma_finalized_at` stamped for analytics purposes. Lower priority than the dashboard work.

## Issue 3 — dashboard is blind to both background-poll code paths

### Audit results
Dashboard panels at `irene/data/dashboard/*.txt` — 12 files, all updating on the 30s render tick:

| Panel                  | Surfaces                                         |
|------------------------|--------------------------------------------------|
| `dashboard.txt`        | Uptime, 1h counts, AI subsystem, wallet, last 5 articles/decisions |
| `cycle_summary.txt`    | Cycle id, article/signal/decision ingestion      |
| `decisions.txt`        | Full decision audit trail (last 10k)             |
| `funnel.txt`           | 1h decision outcomes by reason                   |
| `latency.txt`          | Per-stage pipeline latencies (cross-signal)      |
| `latency_breakdown.txt` (cycles 15+16) | Per-signal execute + reject latency  |
| `throughput.txt`       | 1h row counts + timings + CLT stats              |
| `recent_trades.txt`    | Last 20 trades                                   |
| `pnl.txt`              | Realised + open PnL by decision path             |
| `entity_extraction.txt` / `entity_resolutions.txt` | DKG entity stats |
| `news_staleness.txt`   | Trading delay vs article published_at            |

**Gaps:** none of these surface (a) cycle-12 resolution-poll health, (b) cycle-15 fee_rate fetch coverage, (c) per-source / per-reason funnel TRENDS (only point-in-time counts).

### Recommended new panels
1. **`market_info_status.txt`** — fee_rate coverage % + `fee_rate_fetched_at` freshness histogram + last successful CLOB `/markets/` timestamp. Would have caught Issue 1 at first dashboard tick.
2. **`resolution_status.txt`** — State 1/2 active count (with open trades) + passive count + RESOLUTION_TIMEOUT count + last resolution-poll cycle timestamp + count of state-transitions stamped in the last 24h.

Both pattern-match cycle-15's `latency_breakdown.py`: SQL → tabular render → `_atomic_write`.

## Cycle 21 fix queue (ordered)

Each is independently testable; can be done in parallel by agents.

### Fix A (HIGH priority) — wire `clob` into `MarketsStage`
- **Files:** `irene/app/main.py`, `irene/app/deps.py`.
- **Probable change:** one line — pass `clob=deps.clob` (or equivalent) in the `MarketsStage(...)` constructor call inside `wire_pipeline`.
- **Defensive code change:** `markets.py:_maybe_fetch_market_info` — emit a one-shot WARNING when `self._clob is None`, gated on a class-level `_warned_no_clob` flag.
- **Test:** add a regression test in `app/tests/stages/test_markets.py` asserting `MarketsStage` raises (or logs loudly) when constructed without clob.
- **Verification:** restart container; confirm CLOB `/markets/<cid>` call rate ≥ 50/min in first hour; confirm `fee_rate` populated on ≥ 95% of active rows within 90 min.

### Fix B — refine `markets state 1/2` alert metric
- **File:** `irene/scripts/soak_check.py` (dimension 7, around line 122).
- **Change:** split into active (with-open-trades, < 7d) vs passive (no open trades / > 7d). Only ALERT on active-with-open-trades > 0.
- **Verification:** rerun `soak_check.py` immediately — `state 1/2` alert should clear.

### Fix C — add two dashboard panels
- **New files:** `irene/app/dashboard/pages/market_info_status.py`, `irene/app/dashboard/pages/resolution_status.py`.
- **Wire into** `irene/app/dashboard/registry.py` PAGES list.
- **Template:** copy structure from `pages/latency_breakdown.py` (cycles 15+16).
- **Test:** extend `app/tests/test_dashboard_pages.py` with an empty-DB + seeded-rows test per panel.

### Fix D (cosmetic) — bump `IRENE_CYCLE` env
- **File:** `irene/.env` — change from `cycle3-fresh` to `cycle-21`.
- Not blocking; future cycle-history depends on this value being meaningful.

### Fix E (lower priority) — investigate the 11 RESOLUTION_TIMEOUT markets
- All 11 have `last_resolution_status = NULL` and `last_outcome_prices = NULL` — Gamma never set the UMA status before we lost touch. Sample 5 are Czech-Liga football outcomes from mid-May.
- **Action:** SQL audit of these 11. If any are on markets we held (verify: 0 currently, but check historical), determine outcome manually. Otherwise reframe as "expected long-tail" and ignore.

## Verification — reproduce the diagnosis before fixing

```bash
# Confirm fee fetcher dead
docker logs irene --since 24h 2>&1 | grep -c "clob.polymarket.com/markets/0x"
# Expected: 0

# Confirm trade pipeline alive (CLOB host reachable)
docker logs irene --since 24h 2>&1 | grep -c "clob.polymarket.com/midpoint"
# Expected: 20000+

# Confirm State 1/2 has no actionable rows
python3 -c "
import sqlite3
db = sqlite3.connect('/home/muk/projects/jennie/irene/data/irene.db')
cur = db.cursor()
cur.execute('''
  SELECT COUNT(*) FROM trades t
  LEFT JOIN trade_closures c ON c.trade_id=t.id
  LEFT JOIN markets m ON m.condition_id=t.market_id
  WHERE c.id IS NULL AND m.closed_at IS NOT NULL AND m.uma_finalized_at IS NULL
''')
print('actionable state 1/2:', cur.fetchone()[0])"
# Expected: 0

# Baseline test count
python3 -m pytest irene/app/tests/ --no-header -q | tail -3
# Expected: 492 passed
```

## Carry-forward from cycle 20

- `irene/plans/source-quality-weighting.md` — weight news sources by importance. Needs ≥1 week of `pre_news_drift` data first.
- `irene/plans/asymmetric-stop-loss.md` — tight stop_loss + let-winners-run + wrong-hypothesis pattern-mining. Needs ≥100 closed trades first; currently at ~13.
- LLM A/B probe lane (cycle 14) — default-off; needs a deliberate provider list + budget extension before enabling.
- CLOB hosting region experiment (eu-west-2 VPS) — infrastructure, not code.

## Stop-gap for the current session

The 24h cron `b4b3e445` is still session-local (despite the `durable: true` request — Cron tool flagged it as session-only). It will continue firing 2h ticks for the remaining 12h while this session is alive, then expire. After the next session lands Fix A:
1. `CronDelete b4b3e445` to stop the noisy stuck-alert reports.
2. Restart the container with the fixes.
3. Re-arm a fresh cycle-21 cron once `fee_rate coverage` and `state 1/2` alerts are clean at baseline.

---

## 2026-05-26 correction (cycle 22)

Two diagnoses above are wrong; correcting them so cycle 23 doesn't burn time on the wrong fix.

### Fix A is misdiagnosed — `MarketsStage` IS wired with `clob`

The handoff claims "`MarketsStage` is getting `deps.clob = None` … one-line fix in `app/main.py`." Verified in cycle 22: this is false.

- `app/main.py:228` constructs `base_deps = StageDeps(db=db, bus=bus, config=cfg, embedder=embedder, ai=ai, budget=budget, clob=clob)`.
- `app/main.py:237` constructs `markets = MarketsStage(base_deps)` — same path every other clob-using stage takes.
- `app/tests/stages/test_markets.py:829` is a regression test (`test_fee_rate_column_stays_null_when_clob_absent`) that asserts the silent-NULL behavior is **intentional**, not a bug.
- `CloserStage` and `ImpactTrackerStage` differ only in that they raise on `clob=None` (loud-fail). `MarketsStage` and `TradeStage` accept `None` and degrade.

The real bug surface is "`deps.clob` resolves to `None` at runtime in production for some non-wiring reason." Cycle-23 reproducer:

```bash
docker exec irene python3 -c \
  "from app.main import build_deps; import asyncio; print(asyncio.run(build_deps()).clob)"
```

Likely candidates: missing `POLY_PRIVATE_KEY` env in container, exception in `build_clob()` swallowed during startup, env mismatch between local and container. Investigate before patching.

### Fix D is not cosmetic — `IRENE_CYCLE` is functional

The handoff lists Fix D as "cosmetic." It is not:
- `config.py:412–414` reads `IRENE_CYCLE` and overrides the config-file value.
- `main.py:334–335` passes it to `db.ensure_current_cycle()` and `cycle.set_cycle_id()`, which stamps every decision/trade/closure row's `cycle_id` column.
- Stale value means cycle-history analytics conflate cycles together.

Cycle 22 bumped `.env` to `IRENE_CYCLE=cycle-22`. Cycle 23 should bump again on restart.

### Other cycle-22 deltas

- The session-local cron `b4b3e445` is replaced by a durable systemd-user timer (`irene/scripts/systemd/irene-soak.{service,timer}`, installed via `install.sh`). Soaks now survive Claude session boundaries — required to accumulate `market_price_snaps` for the seulgi backtest.
- Fix B (state 1/2 alert split) landed: actionable count is the new alert metric; passive count is reframed as `legacy_uma_backlog` (info). Total alerts dropped from 3 → 2 on the live DB.
- A backtest preflight gate (`scripts/replay/preflight.py`) runs before each soak tick. On first run against the live DB it flagged a real drift: trade 19's stored pnl ($-1.67) vs `accuracy.py`'s reconstruction ($+15.65), diff $17.32 ≈ entry+exit fees the closer applies but the P1 gate formula does not. The gate and the closer formulas have diverged — cycle-23 task: reconcile `scripts/replay/accuracy.py:reconstruct_pnl()` with `app/stages/closer.py`, or split into gross vs net reconstruction.
