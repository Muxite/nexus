# Cycle 23 — Halt blind trading + pre-trade safety + Gamma pagination fix

- **Commit:** (pending push)
- **Theme:** Stop the bot from buying blind, and find out why fee_rate was NULL for every market. Cycle-22 audit found **21/39 trades (54%) had `mid_price=0.5` (the default stub)** and two open positions were on near-expiry markets at stub prices. Cycle 23 lands hard pre-trade gates that replace silent fallbacks with loud rejects, then digs into the cycle-21 Fix A re-diagnosis to find the actual root cause.

## Headline: the real root cause was not what cycle 21/22 thought

The cycle-21 handoff and cycle-22 correction both reasoned about clob being None at runtime. Cycle 23 docker-exec'd the actual production stack and found:

- `MarketsStage._clob` is **not** None (`type(ms._clob)=CLOBClient` in prod).
- `clob.get_market_info(<active_market_id>)` returns the expected `{'fee_rate': 0.1, 'min_order_size': 5.0}`. The CLOB endpoint works fine for active markets.
- The actual root cause: **Gamma's `/markets` endpoint caps pagination at `offset=10000`** (verified — offset=10000 returns 200, offset=10001 returns **422 Unprocessable Entity**). On 2026-05-23 the bot's active-market count crossed the 10k threshold. From then on, every `poll_once` cycle fetched the first 101 pages successfully, hit 422 on page 102, and **the unhandled HTTPStatusError caused `poll_once` to drop EVERY accumulated market**. The markets table has been frozen since 2026-05-23T23:32 (last `updated_at`), which masked itself as "fee_rate fetcher never fires" because `_maybe_fetch_market_info` is called from `_upsert_market`, which never ran.

This single bug explains:
- All 9024 active markets having `fee_rate IS NULL`.
- All 9024 active markets having `volume_usdc IS NULL` (the post-2026-05-25 parser fix to read `volume24hr` never got a chance to run).
- The 12h soak alert composition staying identical across all 6 ticks in cycle 20.

## What landed

### Phase 0 — Kill switch (lands first; minutes to deploy)
- `app/config.py`: `trade.execution_enabled: bool = True`, `trade.min_hours_to_expiry: float = 6.0`.
- `app/stages/trade.py:handle()`: head-of-cascade early reject `reason='execution_disabled'` when the flag is False. Doesn't touch LLM, CLOB, or DB lookups.
- `irene/.env`: `IRENE_TRADE__EXECUTION_ENABLED=false` — bot still polls/embeds/closes; only NEW entries are blocked.

### Phase 1 — Pre-trade hard gates (the "near-zero" target)
- **Gate 1 `mid_unavailable`** — `_resolve_mid` now returns `Optional[float]` (was `float` with three silent-fallback paths to `default_mid_price=0.5`). New gate `_gate_mid_unavailable` rejects when CLOB returns None. The silent fallback that produced 21/39 stub trades is gone.
- **Gate 2 `market_state`** — new `_gate_market_state` rejects on `market_closed` (closed_at NOT NULL), `market_inactive` (active != 1), or `market_near_expiry` (end_date within 6h). Inserted before entity_overlap so it short-circuits the LLM call.
- **Gate 3 (post-condition)** — `_insert_trade` asserts `mid is not None and 0.0 < mid < 1.0`. Defense in depth; should be unreachable after Gates 1+2.
- **WARN-once instrumentation** — first occurrence of a clob-None silent path in `_resolve_mid` and `_maybe_fetch_market_info` produces a loud one-shot WARN log. Future regressions are impossible to overlook.
- **Both `handle()` AND `evaluate_gates()` updated** so the replay harness sees the same rejects as production.
- **`_fetch_market` SELECT widened** to include `active` and `closed_at` (otherwise the new gate would see them as None and be permissive).

### Phase 2 — Clob root-cause + startup assert
- **The real root cause:** `app/stages/markets.py:_fetch_all_gamma` now catches `HTTPStatusError` per-page. A 422 AFTER we've accumulated markets is treated as end-of-pagination — preserves what we have, logs INFO, returns. A 422 on the FIRST page still raises (it's a real misconfiguration). This restores the entire pipeline downstream: markets repopulate → `_upsert_market` fires → `_maybe_fetch_market_info` calls CLOB → fee_rate becomes non-NULL → volume_usdc + category get the 2026-05-25 parser fix.
- **Startup assert in `main.py`** — refuses to start when `cfg.trade.execution_enabled=True` AND clob is None. Belt-and-braces for any future regression that re-introduces an optional clob path.

### Phase 3 — Discovery repair
- **Volume / category NULL:** root-caused above. The Gamma 422 fix is the fix; on next successful poll, `volume_usdc` will populate for all 10k markets. (Gamma returns `category: None` natively for current markets — that one is Polymarket-side, not our bug.)
- **Startup-time resolution rescan** — `MarketsStage.start()` runs one synchronous `poll_resolved_markets_once()` before spawning the periodic loops. Closes the bot-restart-misses-finalization gap (was up to 10 min of blindness).
- **11-market RESOLUTION_TIMEOUT audit** — `irene/scripts/audit/resolution_timeout_audit.py` + `irene/reports/2026-05-26-resolution-timeout-audit.md`. The count had aged to 13 at execution time; **none had open trades**, **none had any historical position**. Conclusion: no action required, all are passive backlog.

### Tests
- `app/tests/stages/test_trade_safety_gates.py` — **10 new tests** covering mid_unavailable (2 cases), market_state (5 cases), kill switch (2 cases), and the post-condition assert.
- `app/tests/stages/test_markets.py` — **2 new tests** for the Gamma 422 pagination fix (preserves partial on mid-pagination 422; re-raises on first-page 422).
- Updated `app/tests/stages/test_trade.py`'s test that previously validated the silent 0.5 fallback — now asserts the new `reason='mid_unavailable'` reject.
- Updated `app/tests/stages/test_evaluate_gates.py`'s "raises when mid missing" — now asserts the `outcome='reject', reason='mid_unavailable'` return.

## Verified

- `pytest app/tests/ scripts/replay/tests/` → **655 passed** (643 cycle-22 baseline + 10 new safety gates + 2 new gamma tests).
- Live container probe confirms `clob.get_market_info(<active_cid>)` returns `{'fee_rate': 0.1, 'min_order_size': 5.0}` — the URL path is correct, the CLOB works.
- Gamma boundary verified: offset=10000 → 200/100 rows; offset=10001 → 422.
- Resolution-timeout audit ran clean: 13 markets, all `no-position`, all `last_resolution_status=NULL`.

## Still requires user action (out of scope for this turn — touches deployed state)

1. **Container restart** to pick up: `.env` kill switch, all `app/` code changes, and the startup-time resolution rescan.
2. After restart, **observe markets table repopulating**:
   ```python
   import sqlite3
   db = sqlite3.connect('/home/muk/projects/nexus/irene/data/irene.db')
   print(db.execute("SELECT COUNT(*) FROM markets WHERE active=1 AND volume_usdc > 0").fetchone())
   ```
   Expect non-zero within one 2-minute poll cycle.
3. After volume + fee_rate populate, **flip the kill switch off**:
   ```
   IRENE_TRADE__EXECUTION_ENABLED=true
   ```
   then restart again. Bot resumes trading; new trades should have `mid_price != 0.5` and `fee_rate IS NOT NULL`.
4. **24h backstop measurement** (the "near-zero" target made measurable):
   ```python
   print(db.execute(
     "SELECT COUNT(*) FROM trades WHERE mid_price = 0.5 AND decided_at >= datetime('now', '-24 hours')"
   ).fetchone())
   ```
   Expect: **0**.

## Carry-forward to cycle 24

- **Reconcile preflight gross-vs-net** — the cycle-22 surprise finding: `accuracy.reconstruct_pnl()` returns gross; `closer.py` stores net. Drift of ~$17 on trade 19. Either reconcile the formulas or split into gross-vs-net reconstruction.
- **Freshness gate audit** — 3248/3923 (83%) of last-day rejections are `freshness_stale`. Biggest market-finding unlock, but needs separate investigation into upstream stamping (the categorizer that decides BREAKING/EVOLVING/STALE).
- **Match similarity floor realignment** — `match.similarity_floor=0.50` is stricter than `trade.similarity_floor=0.35`. Double-filtering noise before the LLM; A/B test loosening it.
- **Dashboard panels** (cycle-21 Fix C): `market_info_status.txt` (fee_rate coverage + last successful CLOB call) and `resolution_status.txt` (state-1/2 actionable + passive + RESOLUTION_TIMEOUT). Would have caught the Gamma 422 break at first dashboard tick instead of 3 days later.
- **Investigate Polymarket's `umaResolutionStatus` source** — zero `close_reason='market_resolved'` rows in trade history. The 13 RESOLUTION_TIMEOUT markets all have NULL status. Verify whether a different endpoint exposes it.
