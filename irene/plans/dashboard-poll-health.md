# Plan — dashboard panels for background-poll health

> Source: cycle 21 Fix C (`handoffs/2026-05-25-cycle20-soak-debug.md`
> §"Issue 3 — dashboard is blind to both background-poll code paths").
> Carried forward through cycles 22+23. Filed as a plan because the
> cycle-22 Gamma 422 break — which froze the markets table for 3 days
> — would have been caught at the first 30s dashboard tick if these
> panels existed.

## Context

Twelve dashboard panels live under `irene/data/dashboard/*.txt`,
rendered every 30s by `app/dashboard/core.py:Dashboard.tick()`:

```
dashboard.txt          cycle_summary.txt      decisions.txt
funnel.txt             latency.txt            latency_breakdown.txt
throughput.txt         recent_trades.txt      pnl.txt
entity_extraction.txt  entity_resolutions.txt news_staleness.txt
```

None surface (a) cycle-15 `fee_rate` fetch coverage, (b) cycle-12
`_resolution_poll_loop` health. An operator reading `dashboard.txt`
during the cycle-20 → cycle-22 window saw "0 executes" with no
diagnostic for "fee_rate fetcher is dead and 1427 markets are awaiting
UMA finalization." Cycle 23 root-caused that to Gamma's 422 pagination
cap — but only because a human ran `docker logs irene | grep 422`.

Two new panels, each pattern-matched on existing
`app/dashboard/pages/latency_breakdown.py` (cycle 15) — same
`async def render(dash) → str` shape, same `_atomic_write` write path.

## Recommended approach

### Panel 1 — `market_info_status.txt`

Source: queries `markets` table.

Fields:
- **fee_rate coverage:** `populated / active_total` + percent.
- **fee_rate_fetched_at freshness histogram:** buckets <1h, 1-24h,
  24-168h, >168h (the 168h refresh cadence is
  `cfg.markets.market_info_refresh_hours`).
- **Last successful CLOB `/markets/<cid>` call:** max of
  `fee_rate_fetched_at`. Stale value here = fetcher dead.
- **Volume coverage:** same shape, but on `volume_usdc IS NOT NULL`.
- **Total active markets** + most recent `markets.updated_at` (catches
  a Gamma poll outage at the next tick).

### Panel 2 — `resolution_status.txt`

Source: queries `markets` + `trades` + `trade_closures`.

Fields:
- **State 1/2 actionable count:** markets with open trades, closed
  inside the 7-day window. ALERT-worthy if non-zero (mirrors cycle-22
  `soak_check.py` Fix B split).
- **`legacy_uma_backlog` count:** rest of the state-1/2 population
  (informational; the cycle-12 poll correctly does nothing with these).
- **RESOLUTION_TIMEOUT count:** age > 7d, uma_finalized_at NULL.
- **Last `_resolution_poll_loop` cycle:** track via a new column or
  via a side-effect timestamp (simplest: log a row in a new
  `poll_heartbeats` table on each successful poll; panel reads
  `MAX(at)` for `poll='resolution'` and `poll='markets'`).
- **State transitions stamped in the last 24h:** `COUNT(*) WHERE
  uma_proposed_at >= datetime('now', '-1 day')` + same for
  `uma_finalized_at`.

### Wiring

- `app/dashboard/pages/market_info_status.py` — exports `FILENAME` and
  `async def render(dash)`.
- `app/dashboard/pages/resolution_status.py` — same.
- Add both module names to the `PAGES` list in
  `app/dashboard/registry.py`.
- Tests: extend `app/tests/test_dashboard_pages.py` with one
  empty-DB and one seeded-data test per panel (mirror the existing
  `_seed_trade_with_latency` pattern).

### Optional: surface in `soak_check.py`

Once the panels exist, `soak_check.py` could parse them instead of
re-running the SQL (DRY). Lower priority — the SQL duplication is
small and the panels are the primary surface.

## Verification

1. `pytest app/tests/test_dashboard_pages.py` — both new panel tests
   pass (empty-DB + seeded paths).
2. Run `python3 -m app.dashboard.core` (or restart the container) and
   `cat /home/muk/projects/jennie/irene/data/dashboard/market_info_status.txt`
   — verify the numbers match a manually-run sqlite query.
3. **Failure-mode check:** simulate the cycle-22 Gamma break by
   stopping the container, manually setting `markets.updated_at` 2 days
   in the past, restarting. The new `market_info_status.txt` panel
   should show the stale `updated_at` immediately (within one 30s
   render tick) — that's the property cycle 22 lacked and cycle 23
   only found by accident.
4. After the panels ship, on the next soak: any operator (human or
   agent) opening `dashboard.txt` directory should be able to spot a
   broken fetcher / poll within ~30s, not 3 days.
