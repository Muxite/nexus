# SEULGI historical postmortem — 2026-05-19

Forensic pass over every strategy-relevant closed trade in the DB.
Use this to inform the next round of strategy/variant changes.

## Overview

* Closed trades scored: **192** (excludes cycle_reset, engine_retired).
* Wins / losses / zero: 60 / 132 / 0 (hit% 31.2%)
* Net realized PnL: **$-4.40** (avg per trade $-0.02)
* ROI on staked capital: **-1.50%** (lifetime, all engines)
## Conf calibration

If a row says `hit% < confidence midpoint`, the LLM is
over-confident in that bucket; an isotonic-regression
layer could re-shape the output.

### Conf vs realized hit-rate

| bucket | n | hit% | sum_pnl | avg_pnl |
|---|---:|---:|---:|---:|
| `<0.40` | 0 | — | — | — |
| `0.40-0.50` | 0 | — | — | — |
| `0.50-0.60` | 1 | 0.0% | $-0.01 | $-0.01 |
| `0.60-0.70` | 39 | 5.1% | $-1.66 | $-0.04 |
| `0.70-0.75` | 152 | 38.2% | $-2.73 | $-0.02 |
| `0.75-0.85` | 0 | — | — | — |
| `>=0.85` | 0 | — | — | — |

## Direction + freshness

### By direction

| direction | n | hit% | sum_pnl |
|---|---:|---:|---:|
| NO | 121 | 19.8% | $-4.62 |
| YES | 71 | 50.7% | $+0.22 |

### By freshness label

| freshness | n | hit% | sum_pnl | avg_pnl |
|---|---:|---:|---:|---:|
| BREAKING | 88 | 13.6% | $-2.82 | $-0.03 |
| EVOLVING | 104 | 46.2% | $-1.58 | $-0.02 |

## Close-reason analysis

TP/SL/TRAIL asymmetry tells you whether the win/loss
magnitudes match the strategy assumptions (e.g. TP 2% vs SL 3%).

| close_reason | n | sum_pnl | avg_pnl | avg_hold_min |
|---|---:|---:|---:|---:|
| `TP` | 100 | $+0.58 | $+0.01 | 53.2 |
| `TRAIL` | 59 | $-1.92 | $-0.03 | 79.8 |
| `SL` | 33 | $-3.07 | $-0.09 | 47.0 |

## Per-variant lifetime ROI

ROI-on-pot uses the $100/engine bankroll (cycle 9 + cycle 2
combined). ROI-on-stake is sum_pnl / sum_size_usdc.

| variant | n | hit% | sum_pnl | ROI on $100 pot | ROI on staked |
|---|---:|---:|---:|---:|---:|
| `balanced/conf40_tp020_sl030_slpct10` | 5 | 60.0% | $+0.05 | 0.05% | 0.51% |
| `balanced/conf40_tp020_sl030_lowvol` | 5 | 60.0% | $+0.05 | 0.05% | 0.51% |
| `inverted_highconf/conf40_tp020_sl030_inv75_lowvol` | 5 | 60.0% | $+0.05 | 0.05% | 0.51% |
| `balanced/conf40_tp030_sl050` | 6 | 33.3% | $-0.02 | -0.02% | -0.14% |
| `inverted_highconf/conf40_tp030_sl050_inv75` | 6 | 33.3% | $-0.02 | -0.02% | -0.14% |
| `inverted_highconf/conf40_tp020_sl030_inv75_hi65` | 9 | 55.6% | $-0.07 | -0.07% | -0.51% |
| `balanced/conf40_tp010_sl020` | 10 | 50.0% | $-0.08 | -0.08% | -0.51% |
| `inverted_highconf/conf40_tp010_sl020_inv75` | 10 | 50.0% | $-0.08 | -0.08% | -0.51% |
| `aggressive/conf25_tp010_sl020` | 14 | 42.9% | $-0.15 | -0.15% | -0.70% |
| `balanced/conf40_tp020_sl030_norail` | 14 | 57.1% | $-0.16 | -0.16% | -0.87% |
| `inverted_highconf/conf40_tp020_sl030_inv75_hi75` | 6 | 33.3% | $-0.20 | -0.20% | -1.98% |
| `balanced/conf40_tp020_sl030_inv65` | 6 | 16.7% | $-0.27 | -0.27% | -3.16% |
| `inverted_highconf/conf40_tp020_sl030_inv65` | 6 | 16.7% | $-0.27 | -0.27% | -3.16% |
| `aggressive/conf25_tp005_sl015_slpct10` | 18 | 22.2% | $-0.47 | -0.47% | -1.77% |
| `aggressive/conf25_tp005_sl015_lowvol` | 24 | 16.7% | $-0.74 | -0.74% | -2.09% |
| `aggressive/conf25_tp005_sl015` | 22 | 13.6% | $-0.86 | -0.86% | -2.61% |
| `aggressive/conf25_tp005_sl015_norail` | 26 | 11.5% | $-1.17 | -1.17% | -3.22% |

## Top losing markets (≥2 trades)

Markets that we kept re-entering and consistently lost on.
If the same question_id dominates losses, the right fix is at
the market-index level (block it) or the LLM prompt level (it's
miscalling this market type).

| pnl | n_trades | question |
|---:|---:|---|
| $-0.76 | 7 | US-Iran nuclear deal by June 30? |
| $-0.57 | 13 | OpenAI IPO before 2027? |
| $-0.48 | 9 | US-Iran nuclear deal before 2027? |
| $-0.47 | 5 | Will the DFM Real Estate Index dip to 10,000 in 2026? |
| $-0.39 | 2 | Will the Democratic Party win the VA-07 House seat? |
| $-0.37 | 7 | Will Trump's approval rating hit 35% in 2026? |
| $-0.33 | 5 | Will Israel launch a major ground offensive in Gaza by June 30? |
| $-0.31 | 2 | Will Japan GDP growth in Q1 2026 be between 0.3% and 0.5%? |
| $-0.30 | 7 | Ukraine signs peace deal with Russia before 2027? |
| $-0.28 | 4 | Will no Fed rate cuts happen in 2026? |
| $-0.20 | 5 | Will Crude Oil (CL) hit (LOW) $80 by end of June? |
| $-0.20 | 5 | US strike on Cuba by December 31? |
| $-0.18 | 2 | Will S&P 500 (SPX) hit $6,200 (LOW) in December? |
| $-0.15 | 4 | Will Israel launch a major ground offensive in Gaza by December 31? |
| $-0.15 | 5 | Venezuela election scheduled by December 31, 2026? |

### Top winning markets (≥2 trades)

| pnl | n_trades | question |
|---:|---:|---|
| $+0.66 | 14 | US federally charges ex-Cuba leader Raul Castro? |
| $+0.63 | 16 | Luigi Mangione tried before 2027? |
| $+0.22 | 7 | US x Cuba economic deal by June 30, 2026? |
| $+0.19 | 2 | Will Crude Oil (CL) settle over $90 on the final trading day of June 2026? |
| $+0.11 | 2 | Will the U.S. 30-year Fixed-Rate Mortgage hit 6.50% (HIGH) by December 31, 2026? |
| $+0.03 | 7 | Sam Altman out as OpenAI CEO before 2027? |
| $+0.01 | 2 | Will the San Antonio Spurs win the NBA Western Conference Finals? |
| $-0.00 | 2 | Will Crude Oil (CL) hit (LOW) $70 by end of June? |
| $-0.00 | 11 | Will Trump's approval rating hit 45% in 2026? |
| $-0.01 | 2 | Will NVIDIA be the largest company in the world by market cap on June 30? |
| $-0.01 | 3 | AI wins IMO gold medal in 2026? |
| $-0.02 | 3 | Iran leadership change by June 30? |
| $-0.03 | 2 | Will India’s 2026 Annual Inflation be at least 4.50%? |
| $-0.03 | 3 | Will Arsenal finish in 2nd place in the 2025-26 English Premier League? |
| $-0.03 | 2 | CDC issues Level 4 warning by December 31? |

## Per-source ROI (≥3 trades)

Source-host aggregation. The watcher feeds 15-ish sites; if any of
them has consistently negative ROI we can deprioritize it at the
watcher level (low-pri queue) or kill it.

Sorted worst first.

| host | n | ROI | sum_pnl | hit% |
|---|---:|---:|---:|---:|
| `www.washingtonpost.com` | 5 | -6.10% | $-0.42 | 20.0% |
| `www.theguardian.com` | 3 | -4.80% | $-0.16 | 0.0% |
| `www.cnbc.com` | 15 | -4.56% | $-0.93 | 6.7% |
| `www.cbsnews.com` | 5 | -3.63% | $-0.20 | 0.0% |
| `www.straitstimes.com` | 7 | -3.45% | $-0.34 | 0.0% |
| `www.politico.eu` | 3 | -3.42% | $-0.13 | 0.0% |
| `apnews.com` | 15 | -2.81% | $-0.55 | 26.7% |
| `www.nbcnews.com` | 3 | -2.73% | $-0.11 | 0.0% |
| `www.france24.com` | 5 | -2.37% | $-0.16 | 0.0% |
| `www.nytimes.com` | 40 | -2.22% | $-1.56 | 17.5% |
| `www.bloomberg.com` | 10 | -2.20% | $-0.30 | 30.0% |
| `www.bbc.com` | 11 | -1.47% | $-0.19 | 18.2% |
| `www.theverge.com` | 8 | 0.40% | $+0.04 | 75.0% |
| `www.usatoday.com` | 43 | 0.85% | $+0.68 | 62.8% |
| `www.aljazeera.com` | 12 | 0.87% | $+0.14 | 66.7% |

## Hour-of-day heatmap (UTC)

Phase E.11 currently pauses trading at hours 23/00/01 UTC.
Verify whether the actual data still supports that window.

| hour (UTC) | n | hit% | sum_pnl | avg_pnl |
|---:|---:|---:|---:|---:|
| 00 | 19 | 0.0% | $-1.41 | $-0.07 |
| 01 | 7 | 28.6% | $-0.16 | $-0.02 |
| 02 | 0 | — | — | — |
| 03 | 0 | — | — | — |
| 04 | 0 | — | — | — |
| 05 | 0 | — | — | — |
| 06 | 0 | — | — | — |
| 07 | 0 | — | — | — |
| 08 | 0 | — | — | — |
| 09 | 0 | — | — | — |
| 10 | 0 | — | — | — |
| 11 | 0 | — | — | — |
| 12 | 0 | — | — | — |
| 13 | 0 | — | — | — |
| 14 | 0 | — | — | — |
| 15 | 0 | — | — | — |
| 16 | 0 | — | — | — |
| 17 | 0 | — | — | — |
| 18 | 0 | — | — | — |
| 19 | 0 | — | — | — |
| 20 | 124 | 37.9% | $-2.21 | $-0.02 |
| 21 | 21 | 19.0% | $-0.39 | $-0.02 |
| 22 | 12 | 58.3% | $+0.03 | $+0.00 |
| 23 | 9 | 0.0% | $-0.26 | $-0.03 |
