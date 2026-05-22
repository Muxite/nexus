# A3.3 — Experimental 1-D variant grid + backtest

**Agent**: A3.3 (Phase 3). **Date**: 2026-05-22. **Window**: 2026-05-21 → 2026-05-23 (price-tape coverage starts 2026-05-21).

## TL;DR

- Grid: `seulgi/configs/experimental_1d_grid.yaml` — baseline `balanced/conf40_tp025_sl045` + 7 strict 1-D mutants + 2 scouts.
- **All 8 balanced/* variants produced statistically identical results** in the 24h and 2-day windows: n=15 trades, sum_pnl ≈ −$9.74, CI_95 [−11.78, −7.84]. The 1-D mutations cannot differentiate under the current signal regime (118/122 directional decisions all at conf=0.9, mid 0.65–0.85, edge ≈ 0.15) and the current 5-minute exit-price horizon (TP/SL never fire).
- **Go/no-go on grid promotion: NO-GO for live cycle.** The harness can't separate variants today. Defer grid promotion until Phase 4A.1 (continuous tape ≥ 30min post-entry) ships.
- The only variants that DO differentiate are `scout/all_signals` (+$47.64, 60% win, n=15) and `super_scout/all_signals` (−$0.14, 0% win, n=15) — both keep their existing role for attribution data.

## 1. Grid design

Baseline: `balanced/conf40_tp025_sl045` (effective settings):

| knob | value |
|---|---|
| min_confidence | 0.40 |
| take_profit | 0.025 |
| stop_loss | 0.045 |
| trailing_stop | 0.012 |
| min_edge | 0.03 |
| min_hold_seconds | 300 |
| mid_yes_min | 0.0 |
| mid_yes_max | 1.0 |
| max_position_usdc | 100.0 |

Each mutant changes exactly ONE knob. The plan's `1d/sim040 (min_top_similarity)` was swapped for `1d/min_edge_005`: `min_top_similarity` is the Stage-2 relevance gate, which the replay harness does not re-run (it only re-runs `_evaluate_pure` + `_close_pure`). The 002/005 pair brackets baseline 0.03.

| variant role | knob changed | new value | historical engine_name carrier |
|---|---|---|---|
| BASELINE | (anchor) | — | `balanced/conf40_tp025_sl045` |
| 1d/conf35 | min_confidence | 0.35 | `balanced/loose_confidence_035` |
| 1d/conf50 | min_confidence | 0.50 | `balanced/conf50_tp025_sl045` |
| 1d/tp020 | take_profit | 0.020 | `balanced/conf40_tp025_sl045_yes_only` |
| 1d/sl030 | stop_loss | 0.030 | `balanced/conf40_tp025_sl045_evolving` |
| 1d/mid060 | mid_yes_min | 0.60 | `balanced/loose_mid_band_058` |
| 1d/min_edge_002 | min_edge | 0.02 | `balanced/conf40_tp025_sl045_mid075` |
| 1d/min_edge_005 | min_edge | 0.05 | `balanced/conf40_tp020_sl030_lowvol_resurrected` |

### Harness constraint

The replay harness walks `engine_results` keyed by `engine_name`; a brand-new variant has zero historical rows so it would never be walked. Empirically verified: every `balanced/*` variant has IDENTICAL direction/confidence/p_hat/mid/similarity per `headline_id` (they share `classify_batch`), so repurposing existing variant names as carriers is equivalent to running fresh 1-D mutants on the same decision stream. Prod-only filters (direction_filter, freshness_filter, max_market_vol_30m, mid_yes_min/max via mid075) are explicitly NOT carried over in the experimental config.

## 2. Backtest results

### 24h window (2026-05-22 → 2026-05-23) — 8,076 decisions, 253 headlines, 150 trades simulated

| variant | knob | value | n | win% | avg_pnl | sum_pnl | std | CI_95_low | CI_95_hi |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| BASELINE | (anchor) | — | 15 | 0.0 | −0.6496 | −9.7442 | 0.2570 | −11.7774 | −7.8362 |
| 1d/conf35 | min_confidence | 0.35 | 15 | 0.0 | −0.6496 | −9.7442 | 0.2570 | −11.7774 | −7.8362 |
| 1d/conf50 | min_confidence | 0.50 | 15 | 0.0 | −0.6496 | −9.7442 | 0.2570 | −11.7774 | −7.8362 |
| 1d/tp020 | take_profit | 0.020 | 15 | 0.0 | −0.6496 | −9.7442 | 0.2570 | −11.7774 | −7.8362 |
| 1d/sl030 | stop_loss | 0.030 | 15 | 0.0 | −0.6496 | −9.7442 | 0.2570 | −11.7774 | −7.8362 |
| 1d/mid060 | mid_yes_min | 0.60 | 15 | 0.0 | −0.6496 | −9.7442 | 0.2570 | −11.7774 | −7.8362 |
| 1d/min_edge_002 | min_edge | 0.02 | 15 | 0.0 | −0.6496 | −9.7442 | 0.2570 | −11.7774 | −7.8362 |
| 1d/min_edge_005 | min_edge | 0.05 | 15 | 0.0 | −0.6496 | −9.7442 | 0.2570 | −11.7774 | −7.8362 |
| scout/all_signals | (scout) | — | 15 | 60.0 | +3.1757 | +47.6358 | 3.2703 | +23.3645 | +72.6796 |
| super_scout/all_signals | (super_scout) | — | 15 | 0.0 | −0.0093 | −0.1388 | 0.0025 | −0.1574 | −0.1203 |

Median hold time = 300s (5min) for every variant — the only exit horizon with price-tape coverage.

### 2-day window (2026-05-21 → 2026-05-23) — 8,119 decisions, 257 headlines, 150 trades

Identical numbers to the 24h table (the 2026-05-21 fragment contributes only 43 directional decisions in the early UTC hour and 0 additional trades). Omitted to save space; raw output at `/tmp/a3-3-1d-grid-2d/runs/`.

### 4-day window (2026-05-18 → 2026-05-22) — 741 decisions, 65 headlines, **0 trades**

The price-tape capture pipeline only writes `market_price_snaps` from 2026-05-21 onward (verified: 47,684 snaps on 2026-05-21 + 144,239 on 2026-05-22; zero before). All 741 directional decisions in the requested window land in the harness pre-2026-05-21 where exit-price lookup returns None, so every row is skipped with `no_price_for_exit`. This is the same gap A3.1 hit (cf. `/tmp/a3-1-baseline/runs/.../manifest.json`). The 24h + 2-day windows are the only meaningful runs at this time.

## 3. Per-knob sensitivity

Every 1-D mutation produced **zero PnL delta** vs baseline (Δ sum_pnl = 0.0000 across all 7 mutants). Four compounding reasons:

- AI signal collapses to one regime — 118/122 directional decisions at confidence=0.9; only 4 at 0.4/0.6. Conf 0.35/0.50 straddles no rows.
- mid_yes always ∈ [0.65, 0.85] (66 at 0.7, 56 at 0.8). `mid_yes_min=0.60` passes every entry.
- TP/SL never fire within 5min — zero `close_reason_inferred` set across all 120 balanced trades; PnL is MTM at 5min minus spread.
- edge ≈ 0.15 for the conf=0.9 cohort; the 0.02/0.03/0.05 thresholds all sit far below.

Per-knob sensitivity is therefore uniformly null (0.0000 Δ on every knob × direction).

## 4. Comparison to A3.1 production-config baseline

A3.1 (`/tmp/a3-1-baseline/runs/20260522T234136Z_10fd8b/`) used **production** `config.yaml` over the same 4-day window and produced 0 trades for every engine: 36,527 decisions walked, 20,387 killed by `no_price_for_exit`. A3.3's 4-day window also returned 0 (consistent), while the 24h tape-covered window returned 120 balanced trades (identical) + 15 scout + 15 super_scout. The grid harness is healthy; the SIGNAL is the bottleneck.

## 5. Top 3 by sum_pnl

With 8 of 10 variants tied at the same sum_pnl, there is no meaningful 3-way ranking among balanced/* mutants. The strictly-ranked top 3 across the full grid:

1. **scout/all_signals**: sum_pnl = +$47.64, win_rate 60.0%, n=15, CI_95 [+23.36, +72.68] — bound below at +$23.36, statistically positive.
2. **super_scout/all_signals**: sum_pnl = −$0.14, win_rate 0.0%, n=15, CI_95 [−0.16, −0.12] — statistically negative (but stake is $0.05/trade; effect size negligible).
3. **balanced/* tied 8-way**: sum_pnl = −$9.74, win_rate 0.0%, n=15, CI_95 [−11.78, −7.84] — statistically negative.

Only **scout/all_signals** has a CI_95 that excludes zero on the positive side. None of the 7 1-D production mutants meet the "CI_95 doesn't include zero" + positive bar; the bound is at −$7.84 and the CI excludes zero on the NEGATIVE side. The 1-D grid as-tested would promote NONE of its mutants.

Note: the scout's +$47.64 is on $1 stakes (max_position_usdc=1.0); scaled to balanced's $100 stake it implies a much larger raw move per trade. But scout admits low-confidence and negative-edge signals on purpose — its trades are a different population from the conf=0.9 cohort the balanced mutants see, so this is NOT directly transferable to a production engine.

## 6. Recommendation

**Go/no-go on grid promotion: NO-GO** for the next live cycle. The grid is clean by construction but the historical signal does not separate the mutants — promoting live would burn 8 × $100 engine pots to learn that under conf=0.9 / mid 0.7 / edge 0.15 / 5min hold, the trader-level knobs don't matter.

**Unblockers (in priority order):**
1. Continuous price tape ≥ 30min (Phase 4A.1 / A0.11). TP/SL need 15-60min exit prices to differentiate.
2. Broader confidence distribution (Phase 4E.4 prompt rewrite + A0.8 NEUTRAL fix), or use the SCOUT signal pool as the oracle source.
3. Loosen pre-trade gates: 5,830/8,076 decisions die at `size_too_small` (simulate-mode Kelly bottoming below $5) and 852 at `hard_rail` before any 1-D filter fires. Fix Kelly returning < $5 and the trade-count denominator should 5-10x.

**Promotion bar (post-tape):** for any 1-D mutant to ship:
- n ≥ 100 trades, median hold ≥ 30min,
- CI_95 of sum_pnl excludes zero on the positive side AND its lower bound exceeds baseline's upper bound,
- paired-trade test against baseline p ≤ 0.05.

Until then, production stays on `balanced/conf40_tp025_sl045` + scout + super_scout; next cycle focuses on the price-tape extension and ai.py NEUTRAL fix, not further variant sweeps.

**Reproduction**: outputs at `/tmp/a3-3-1d-grid-24h/runs/`, `/tmp/a3-3-1d-grid-2d/runs/`, `/tmp/a3-3-1d-grid/runs/`. Commands at the top of `seulgi/configs/experimental_1d_grid.yaml`.
