# Cycle 15 — market_info refresh + latency dashboard + market-floor audit

- **Commit:** (pending push)
- **Theme:** Close out the cycle-13 first-seen limitation (per-market data goes stale) + visualize the cycle-14 latency instrumentation + audit assumes the cycle-10 fee survey made about the wider population.

## What landed

### Market-info refresh cadence (Agent A, `plans/markets-gamma.md`)
- `markets` table: +1 idempotent column `fee_rate_fetched_at TEXT` (UTC ISO timestamp of last successful `get_market_info` call).
- `_maybe_fetch_market_info` fires when EITHER `fee_rate IS NULL` (first-seen, preserved) OR `fee_rate_fetched_at IS NULL OR now - fetched_at >= cfg.markets.market_info_refresh_hours` (stale refresh, new).
- On success: stamp `fee_rate_fetched_at = now_iso`. On failure: leave timestamp unchanged so we retry.
- `cfg.markets.market_info_refresh_hours: float = 168.0` (7 days). Long enough to keep CLOB HTTP budget bounded (1 call/market/week), short enough to catch real fee changes within a trading week.
- Legacy rows pre-cycle-15 have NULL `fetched_at` → all qualify on first post-deploy poll (acceptable one-time burst).
- +4 tests covering refresh-when-stale / skip-when-fresh / first-seen-preserved / failure-doesn't-advance.

### Latency dashboard panel (Agent B, `plans/latency-repricing.md`)
- `app/dashboard/pages/latency_breakdown.py` (NEW) — reads the cycle-14 `trades.decision_latency_breakdown` JSON for the last 500 execute-path trades; computes p50/p95/p99/max per stage; renders to `data/dashboard/latency_breakdown.txt`.
- Wired into `app/dashboard/registry.py` `PAGES` list (positioned next to `latency.py`). Complementary: `latency.txt` answers "how long between stages cross-signal?"; `latency_breakdown.txt` answers "where inside the chain is the time going for the last 500 executes?".
- Graceful empty case: "no instrumented trades yet — wait for first executed trade post-cycle 14."
- +3 tests (empty DB, 10 seeded trades with deterministic latencies, NULL-row filtering).

### Market-floor audit (foreground, this engineer)
- `irene/scripts/audit_market_floors.py` (NEW) — one-shot 500-market survey of `taker_base_fee` + `minimum_order_size` from CLOB, ordered by `volume24hr` descending.
- **Headline finding: 82.2% of top-500 markets charge 10% taker (1000 bps); 17.8% are 0%.** All other rate buckets are empty.
- ALL 500 markets have `minimum_order_size = 5 shares`. Per-market USDC floor (= 5 × yes_price): median $0.10, max $5.00. 0/500 above our $25 config floor.
- **Cycle-10's earlier "{0: 999, 200: 1}" survey was sample-biased**: default Gamma pagination order is by listing-id (≈random), dominated by long-tail zero-volume markets. The volume-weighted high-tail looks completely different.
- Already-handled-by-cycles-10/13: per-market `fee_rate` is fetched on first-seen + refreshed weekly (cycle 15), so production trades use real 10% (not 5% category default) automatically.
- **Recommend cycle 16**: raise `cfg.trade.default_fee_rate = 0.05 → 0.10` so the category-fallback table matches the dominant value on the volume tail.
- Full numbers + interpretation in `irene/reports/2026-05-25-cycle15-market-floor-audit.md`.
- `calibration_seeds.md` A2 entry added (NEW) tracking the per-market fee distribution.

### Low-volume spread sampler v2 (still running from cycle 14)
- 18 min in: 28/30 markets sampled per cycle, 509 rows. ~12 min remaining.

## Verified
- `pytest irene/app/tests/` → **482 / 482 passed** (was 475 baseline; +7: +4 refresh + +3 dashboard).
- Live `/markets/<cid>` audit completed cleanly: 500/500 markets probed, 0 rate-limit issues.

## Not yet (carried forward to cycle 16)
- **Raise `cfg.trade.default_fee_rate = 0.05 → 0.10`** — one-line config change driven by the cycle-15 audit; should also update the docstring above `polymarket_fee_rates` in `config.py`.
- **Low-vol soak v2 analysis** when it completes (~12 min from now); update `calibration_seeds.md` C1 with the thin-market spread tier.
- **Reject-path latency instrumentation** — cycle 14 only annotates execute paths.
- **LLM probe lane enablement** — currently default-OFF; needs a deliberate provider-list choice + budget guardrails before flipping.
- **CLOB hosting region experiment** (eu-west-2 VPS) — requires infra.
- **Per-signal latency aggregation against real data** — needs a soak run with live trades.
