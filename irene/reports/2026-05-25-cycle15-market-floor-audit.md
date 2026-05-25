# 2026-05-25 — cycle 15 market-floor audit (fee_rate + min_order_size)

- **Producing script:** `irene/scripts/audit_market_floors.py` (one-shot, ~2 min).
- **Raw artifact:** `/tmp/market_floors.csv` (500 markets).
- **Updates:** `irene/questions/calibration_seeds.md` A2 entry (NEW); compares against cycle-10 first-pass survey.

## Headline finding

**Of the top 500 active Polymarket markets by `volume24hr`:**

| `taker_base_fee` | count | % |
|------------------|------:|----:|
| 0% (0 bps)       |    89 | 17.8% |
| **10% (1000 bps)** | **411** | **82.2%** |
| any other rate   |     0 |  0%  |

**All 500 markets have `minimum_order_size = 5` shares.** Uniform across the entire high-volume universe.

USDC floor (= `min_shares × yes_price`):
- median: $0.10
- p90: $3.52
- max: $5.00
- **0 / 500 markets** have a per-market USDC floor above our `cfg.trade.min_position_usdc = $25`.

## Compare against cycle-10 survey

Cycle 10's agent ran a 1000-market survey and reported `taker_base_fee` distribution `{0: 999, 200: 1}`. That contradicts this audit, which finds 82% at 1000 bps on the high-volume tail.

The discrepancy is sample selection:
- Cycle 10 used the default Gamma pagination order (effectively random / by listing id), which is dominated by long-tail markets with no recent volume.
- Cycle 15 explicitly orders `volume24hr` descending and takes the top 500, which is the set of markets we'd actually want to trade.

**The high-volume tail is the only sample that matters for fee math.** Random sampling washed out the real fee distribution because zero-volume markets dominate the catalog by count.

## Implications

### Already handled by cycle 10 / 13
The per-market `fee_rate` fetcher (`app/clob.py:get_market_info` + `markets.py:_maybe_fetch_market_info`) writes the real per-market value into `markets.fee_rate` on first-seen. `closer.py:_resolve_fee_rate` (post-cycle 10) prefers the per-market value over the category fallback. So production trades on these top-500 markets will use the real 10% (or 0%) rate, NOT the 5% category default.

Cycle 15 also added a refresh cadence (default 168h) so fee changes get picked up within a trading week.

### Recommended follow-up (cycle 16 candidate)
- Raise `cfg.trade.default_fee_rate` from 0.05 → 0.10 to match the dominant value, and update the comment in `config.py:polymarket_fee_rates` to note that the table is a fallback for unfetched markets only.
- Re-run the cycle-10 audit query (`research_questions.md` i-A1) to confirm `fee_rate` is populated on every trade post-cycle 13; if any rows lack it, investigate why the first-seen fetch failed.

### `min_order_size` gate
- Cycle 13's per-market floor gate is well-designed but rarely binds: worst-case USDC floor on this sample is $5, our config floor is $25.
- Keep the gate as defense-in-depth for the long-tail markets we didn't sample (low-price markets where 5 shares × $0.50 = $2.50 — still under $25).

### `default_fee_rate` math
- At p_hat=0.7 / mid=0.5 / fee_rate=0.10: per-leg fee = $1.00 × 250 shares = $25. Round-trip $50 on a $100 trade. That's a 50% friction tax — but it's correct.
- At fee_rate=0.05: round-trip $25, half as much. The under-estimated default would have falsely shown profitable trades on high-volume markets that actually lose money at 10% fees.
- Cycle 7's `_compute_round_trip_fees` math is fee-rate-agnostic, so the change isn't an algorithm change — just a default-value tightening.

## Closing notes

- Audit ran cleanly in ~2 minutes against 500 markets; no rate-limit issues against either Gamma or CLOB.
- The `fee_rate` × `yes_price` × `(1-yes_price)` parabolic formula (cycle 7) means the worst fees hit markets near mid=0.5; markets that already resolved (mid near 0 or 1) face de-minimis fees regardless of rate.
- The audit confirms the cycle-13 `wide_spread_threshold` + cycle-12 `thin_market` defaults are well-tuned for the markets we'd trade — neither bound on this 500-market sample.
