# Sandbox theory sweep #2 — re-run on the 3.4× larger combined set

**Date:** 2026-05-28
**Universe:** 1,135 tradeable explorer rows (was 335 on 2026-05-27) — `explorer-0` legacy (279) + `cycle-23` firehose (9,830 candidates, 919 tradeable)
**Method:** same 9 strategies × 6 filter sets = 54 backtest runs via `irene.scripts.sandbox.run_strategy`
**Cost:** $1.52 (≈4,700 fresh `google/gemini-2.5-flash` calls; 335-row results reused from cache)

## TL;DR — the edge did not survive the larger sample

**Last week's winner is gone.** `t3-magnitude-classifier × short_dated` was the *only* positive combo on 2026-05-27 (+3.73% ROI, 66.7% win, N=9). On the 3.4× larger set it flips deeply negative:

| run | N | win% | PnL | ROI(bk) | sharpe | avg_edge |
|---|--:|--:|--:|--:|--:|--:|
| t3 × short_dated (2026-05-27) | 9 | 66.7% | +$37.32 | **+3.73%** | +1.83 | 0.310 |
| t3 × short_dated (2026-05-28) | 42 | 16.7% | −$91.95 | **−9.20%** | −1.74 | 0.137 |

This confirms the 2026-05-27 caveat: the 9 trades were ~3–4 independent events dominated by one correlated oil/Iran news cluster the LLM happened to fade correctly. With 33 more short-dated trades added, win-rate fell from 67% → 17% and the edge inverted.

**Nothing else has a real edge either.** Across all 54 runs, only two are positive, and both are flat-to-noise on tiny samples:

| run | N | ROI(bk) | note |
|---|--:|--:|---|
| t5-fast-momentum × midband_liquid | 11 | +0.36% | $+3.56 total — noise |
| t5-fast-momentum × liquid | 18 | +0.06% | $+0.64 total — noise |

Every strategy with a meaningful N loses money.

## Per-strategy (mean across the 6 sets it ran on)

| strategy | runs | N | total PnL | mean/t | win% | ROI(bk) | sharpe |
|---|--:|--:|--:|--:|--:|--:|--:|
| t1-fade-overreaction | 6 | 297 | −$429.76 | −$1.45 | 1.52% | **−7.16%** | −9.96 |
| v3b-kelly-fast | 6 | 295 | −$482.21 | −$1.63 | 0.00% | −8.04% | −12.22 |
| v3-kelly-pyes | 6 | 295 | −$490.03 | −$1.66 | 1.95% | −8.17% | −7.76 |
| t5-fast-momentum | 6 | 339 | −$842.41 | −$2.48 | 11.64% | −14.04% | −3.33 |
| t3-magnitude-classifier | 6 | 488 | −$1,031.41 | −$2.11 | 6.01% | −17.19% | −7.87 |
| t4-devil-advocate | 6 | 445 | −$1,123.97 | −$2.53 | 4.25% | −18.73% | −7.66 |
| t7-fade-momentum | 6 | 339 | −$1,286.09 | −$3.79 | 1.83% | −21.43% | −5.91 |
| t2-no-anchor | 6 | 1680 | −$4,580.70 | −$2.73 | 3.25% | −76.35% | −9.44 |
| v1-baseline | 6 | 2187 | −$12,471.70 | −$5.70 | 3.56% | −207.86% | −15.70 |

`t1-fade-overreaction` is now the "least-bad" prompt (−7.16%), edging out the production-style `v3`. `v1-baseline` (production relatedness gate, derive direction from mid) remains catastrophic — it trades far too much (915 trades on tradeable_all alone) and loses on nearly all of it.

## Per-set (best to worst)

| set | rows | runs | N | ROI(bk) | win% |
|---|--:|--:|--:|--:|--:|
| midband_liquid | 44 | 9 | 232 | −4.59% | 2.05% |
| liquid | 232 | 9 | 756 | −15.54% | 1.75% |
| short_dated | 300 | 9 | 658 | −22.86% | 7.81% |
| tight_spread | 681 | 9 | 1531 | −31.05% | 3.10% |
| midband | 312 | 9 | 776 | −63.45% | 4.80% |
| tradeable_all | 1135 | 9 | 2412 | −115.15% | 3.15% |

Same shape as last week: the smallest/cleanest set (midband_liquid) loses least; the broad universe wipes out hardest. The trend is monotone — more selectivity → less loss, never a profit.

## Why the previous edge was a mirage

1. **Tiny N + correlation.** 9 trades, ~3–4 independent. The 2026-05-27 report flagged this explicitly; the larger sample settled it.
2. **`short_dated` is no longer special.** Last week it had the best win-rate (28%); now 7.8%. The extra short-dated capture is dominated by losers, so the within-24h reversion thesis doesn't hold up.
3. **Post-news drift is non-predictive in both directions.** t5 (follow momentum) and t7 (fade it) both lose at scale — t5 −14%, t7 −21% mean ROI. The two positive t5 runs are 11- and 18-trade noise.

## Data-quality finding (surfaced during this sweep)

Of 10,165 candidate rows, only 1,138 are tradeable. The breakdown:

- **8,885 rejected at the gate stage** (`cheap_binary` 6,462 + `dkg` 2,433) — never captured. By design (audit rows).
- **1,280 passed gates and captured odds**, but only 919 are tradeable. The other **361 have `market_mid_t0 = NULL` and 0/36,801 live order-book snapshots** — because **274/361 markets had already ended before t0 and 261/361 are inactive.** A closed market has no resting book, so the CLOB returns empty all window; only the `history` timeseries (mid-only, never bid/ask) survives.

**Recommended fix:** add a cheap pre-capture gate `market.active = 1 AND (end_date IS NULL OR end_date > t0)` ahead of odds capture. Would eliminate ~361 wasted captures of dead markets at ~zero cost. (The `history` source never carries bid/ask, so entry-priced backtests are inherently live-source-only.)

## Bottom line / next steps

- **No deployable edge** in any prompt × filter combination on the current data. The 2026-05-27 "winner" was overfit.
- The captured signal (20-min post-news price action) does not appear to contain tradeable alpha at this sample size with these prompts.
- Worth trying before declaring the approach dead:
  1. **Cleaner capture** — land the active-market gate, then re-sweep; some of the loss is dead-market noise leaking into the larger sets via the 220 tradeable-but-rejected rows.
  2. **Event-level dedup.** Collapse correlated news clusters (cluster_id exists) to one trade each, so a single cluster can't dominate the PnL in either direction.
  3. **Longer horizon / resolution PnL.** 20-min exits may be too short; test holding to resolution where `end_date` is near.

## Reproduce

```bash
set -a; . irene/.env; set +a
# rebuild the 6 sets (now larger)
python3 -m irene.scripts.sandbox.make_set --name tradeable_all --all-explorations
python3 -m irene.scripts.sandbox.make_set --name midband        --filter "e.market_mid_t0 BETWEEN 0.2 AND 0.8"
python3 -m irene.scripts.sandbox.make_set --name liquid         --filter "m.volume_usdc > 50000"
python3 -m irene.scripts.sandbox.make_set --name midband_liquid --filter "e.market_mid_t0 BETWEEN 0.2 AND 0.8 AND m.volume_usdc > 50000"
python3 -m irene.scripts.sandbox.make_set --name tight_spread   --filter "e.market_spread_t0 IS NOT NULL AND e.market_spread_t0 < 0.04"
python3 -m irene.scripts.sandbox.make_set --name short_dated    --filter "e.hours_to_expiry < 24"
# /tmp/sweep.sh runs all 9 strategies × 6 sets (tradeable_all first per strategy to warm the cache)
python3 -m irene.scripts.sandbox.leaderboard --min-trades 5
python3 -m irene.scripts.sandbox.leaderboard --min-trades 5 --by strategy
python3 -m irene.scripts.sandbox.leaderboard --min-trades 5 --by set
# combined per-trade PDF + TXT over all 54 runs
python3 -m irene.scripts.sandbox.plot_trades --run <all run ids> --out irene/data/sandbox/runs/trades_combined.pdf
```

Prior small-set runs archived under `irene/data/sandbox/runs/_archive_20260527/`.
