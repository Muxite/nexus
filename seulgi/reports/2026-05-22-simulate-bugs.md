# simulate.py — Bug List (Agent A0.2, 2026-05-22)

Audit of `seulgi/app/simulate.py`. Method: line-by-line read, end-to-end
back-derivation against 100 closed trades from `data/seulgi.db`
(close_reason ∈ {TP, SL, TRAIL, TIMEOUT_BLEED}, ORDER BY id DESC LIMIT 100),
plus targeted edge-case probes for NaN / sign / rail conditions.

## TL;DR severity table

| # | Function          | Bug                                                                       | Severity |
| - | ----------------- | ------------------------------------------------------------------------- | -------- |
| 1 | `apply_slippage`  | 100/100 sampled trades have walk=0; `size_usdc<5.0` trips disable_below   | CRITICAL |
| 2 | `apply_slippage`  | Reports walk>0, applied=True when `effective_price` clamps back to mid    | HIGH     |
| 3 | `apply_slippage`  | Non-"YES" side string silently behaves as NO                              | HIGH     |
| 4 | `apply_slippage`  | `side=None` raises AttributeError (undocumented)                          | MEDIUM   |
| 5 | `apply_slippage`  | `walk` is unsigned magnitude; docstring says "signed against the trader"  | LOW      |
| 6 | `clamp_price`     | `clamp_price(NaN)` returns 0.99 silently                                  | HIGH     |
| 7 | `is_at_rail`      | `is_at_rail(NaN)` returns False                                           | MEDIUM   |
| 8 | `compute_fee`     | `price=NaN` clamps to 0.99 → small fake fee, no error                     | MEDIUM   |
| 9 | `compute_fee`     | `min_fee_usdc` floor bypassed when `rate == 0`                            | LOW      |
| 10| `fee_rate_for`    | Exit leg in trader.py L1212/1357 calls with `market_type=None` → geo/sports/politics overpay 0.05 default on exit | HIGH |
| 11| `quantize_shares` | `fill_price <= 0` clamps to 0.01 and inflates share count                 | MEDIUM   |
| 12| `quantize_shares` | Floor-rounds, never up — underfills by up to ~SHARE_INCREMENT             | LOW      |
| 13| `fractional_kelly`| `p_true=NaN` → f_star=1.0, capped to max_bankroll_pct ($2.50 on $50)      | HIGH     |
| 14| `fractional_kelly`| `kelly_at_rail` skip_reason is unreachable after `p_market` clamp         | LOW      |
| 15| `settlement_payout`| Negative shares yields negative payout; no guard                         | LOW      |
| 16| `fee_rate_for`    | Empty string `""` falls into `if not market_type` → default_rate          | INFO     |

Counts: 1 CRITICAL, 4 HIGH, 4 MEDIUM, 4 LOW, 1 INFO. Cross-coordination
note: bug #1 should show up on A0.1's "realized vs simulated PnL" side as
predicted-walk drag missing from every sub-$5 trade.

---

## Per-function findings

### `clamp_price` (simulate.py L31) / `is_at_rail` (L35)
NaN passes through both because Python's `<`, `>`, `<=`, `>=` against NaN
are all False. `clamp_price(NaN) -> 0.99`; `is_at_rail(NaN) -> False`.
Bugs #6, #7. Probed live: `sim.clamp_price(float('nan')) == 0.99`,
`sim.is_at_rail(float('nan')) is False`.

### `fee_rate_for` (L45)
`if not market_type: return cfg.default_rate` — falsy strings (None, ""),
unknown keys, all fall back to default. Bug #16 / #10. The fix that matters
for live PnL: trader.py passes `market_type=None` on the exit leg (L1212
cold-cull, L1357 TP/SL/TRAIL/TIMEOUT_BLEED), so geopolitics (rate=0),
sports (0.03), and politics (0.04) markets pay the 0.05 default on exit.

### `apply_slippage` (L65)
Five back-derivations against the DB:

| trade id | dir | size  | mid    | back-derived         | trace        | match |
| -------- | --- | ----- | ------ | -------------------- | ------------ | ----- |
| 5759927  | YES | 1.00  | 0.5850 | applied=F, walk=0    | walk=0       | ✓     |
| 5712282  | YES | 1.00  | 0.6650 | applied=F, walk=0    | walk=0       | ✓     |
| 5656182  | YES | 1.94  | 0.6950 | applied=F, walk=0    | walk=0       | ✓     |
| 5655118  | YES | 1.29  | 0.6750 | applied=F, walk=0    | walk=0       | ✓     |
| 5480832  | NO  | 1.00  | 0.3175 | applied=F, walk=0    | walk=0       | ✓     |

**100/100 sampled trades have walk=0** because every variant uses
$0.05–$2 stakes < `disable_below_usdc=5.0`. The fidelity layer is
turned OFF for every live trade. Bug #1 (CRITICAL): paper PnL never
sees the slippage drag the model was supposed to add. Fix candidate:
drop `disable_below_usdc` to 0.5–1.0, OR fit a `bps_per_dollar` curve
against `price_impacts.delta_t5m` for the small-notional bucket.

Rail bug (#2): `mid=0.99, side=YES, $100`: `walk_raw=0.02`, capped at
`max_price_move=0.02`, `eff = clamp(0.99+0.02) = 0.99`. Returned
`SlippageResult(walk=0.02, effective_price=0.99, applied=True)` — the
walk is fictional, downstream sees the unchanged price.

Sign / type bugs (#3–#5): `side="yes"` works (`.upper()`); `side="X"`
silently behaves as NO; `side=None` raises AttributeError. trader.py
guards via the Direction enum so production is safe, but the public
API is fragile.

### `compute_fee` (L109)
Entry-side back-derivation (formula `shares * rate * p * (1-p) + 0.005 gas`):

| trade id | shares | ep      | raw     | +gas    | db entry_fee | diff |
| -------- | ------ | ------- | ------- | ------- | ------------ | ---- |
| 5759927  | 1.66   | 0.5850  | 0.02015 | 0.02515 | 0.02515      | 0    |
| 5712282  | 1.47   | 0.6650  | 0.01637 | 0.02137 | 0.02137      | 0    |
| 5656182  | 2.73   | 0.6950  | 0.02894 | 0.03394 | 0.03394      | 0    |
| 5655118  | 1.87   | 0.6750  | 0.02051 | 0.02551 | 0.02551      | 0    |
| 5480832  | 3.02   | 0.3175  | 0.03272 | 0.03772 | 0.03772      | 0    |

Entry-side fees match the DB exactly. Exit-side back-derivation
mismatches 50/100 trades because trader.py passes `market_type=None`
(Bug #10). NaN price (#8): `compute_fee(100, NaN, None, fees) =>
fee=0.0495` (clamp → 0.99). `min_fee_usdc` floor is bypassed when
rate==0 (#9, intentional for geopolitics).

### `quantize_shares` (L132)
Floor-rounds to 0.01-share. Verified: $1.00/$0.7=1.4286 → 1.42 (Bug #12,
minor underfill). Rail-snap bug (#11): `fill_price=0.0` clamps to 0.01,
yielding 1000 shares per $1. `fee_estimate > size_usdc` returns 0.0
cleanly. Reservation order is correct per trader.py: naive shares →
naive fee → quantize with fee_est+gas → recompute fee with actual shares.

### `fractional_kelly` (L162)
Five probes:

| bankroll | p_true | p_market | f_star | fractional | capped_pct | stake | skip_reason          |
| -------- | ------ | -------- | ------ | ---------- | ---------- | ----- | -------------------- |
| 50       | 0.50   | 0.40     | 0.167  | 0.0417     | 0.0417     | 2.08  | None                 |
| 50       | 0.30   | 0.40     | -0.25  | 0          | 0          | 0     | kelly_negative_edge  |
| 50       | 0.95   | 0.40     | 0.917  | 0.229      | 0.05       | 2.50  | None                 |
| 50       | NaN    | 0.50     | 1.0    | 0.25       | 0.05       | 2.50  | **None**             |
| 0        | 0.60   | 0.50     | 0.20   | 0.05       | 0.05       | 0     | kelly_below_min      |

Bug #13: NaN p_true → `max(0, min(1, NaN)) = 1.0` (per the same NaN
quirk as clamp_price) → b=1, f_star=1.0 → max bet. The unreachable
`kelly_at_rail` (#14): after `p_market` clamps to ≥0.01, `b = 1/p − 1
≥ 0.0101`, so the `b<=0` branch is dead code; `p_market=1.0` actually
returns `kelly_negative_edge` because `f_star = (0.99*1.0101 - 1) /
0.0101 ≈ -0.99`.

### `settlement_payout` (L197)
`shares * 1.0 if win else 0.0`. Negative shares pass through (Bug #15).
Cannot happen given DB constraints, but no defensive guard.

---

## Recommendations

- **#1 (CRITICAL)**: lower `disable_below_usdc` to 1.0 (or fit a
  micro-notional curve from `price_impacts.delta_t5m`); rerun a 24h soak.
- **#2**: set `applied=False, walk=0` whenever `eff == mid` after clamp.
- **#3-5**: validate `side ∈ {"YES","NO"}` at the top; raise ValueError.
- **#6, #8, #13**: NaN-guard at module boundary (one helper).
- **#10**: persist `market_type` on trade rows; pass through on exit.
- **#11**: assert `fill_price > 0` in `quantize_shares`.

Word count: ~1,050 / 1,200 budget.
