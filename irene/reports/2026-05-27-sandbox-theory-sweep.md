# Sandbox theory sweep — find prompt × filter combos with positive ROI

**Date:** 2026-05-27
**Universe:** 335 tradeable explorer rows (–10 to +20 min price window per market)
**Method:** 9 prompt theories × 6 filter sets = 54 backtest runs via `irene.scripts.sandbox.run_strategy`
**Bankroll model:** $1,000 bankroll, quarter-Kelly sizing, 2% fee, min_edge=0.02 (unless noted)

## TL;DR

One combination produced positive ROI across the full sweep:

| Strategy × Set | N | Win-rate | Total PnL | ROI(bk) | Sharpe | avg edge |
|---|---:|---:|---:|---:|---:|---:|
| **t3-magnitude-classifier × short_dated** | 9 | **66.7%** | **+$37.32** | **+3.73%** | **1.83** | 0.310 |

Everything else lost money. Smaller sets (midband_liquid: 19 rows; short_dated: 92 rows) were essential — the broad universe (tradeable_all: 335 rows) wiped out every single prompt theory.

## The winning recipe

- **Prompt** (`irene/sandbox/strategies/t3-magnitude-classifier.yaml`):
  Two-step LLM chain.
  Step 1 classifies the news impact as `none` / `small` / `medium` / `large` with a direction.
  Step 2 produces a `p_yes` only for `medium` / `large`; returns –1 otherwise so the row is skipped at the parser level.
  Result: ~75% of rows are filtered out by the LLM before Kelly sizing ever runs.

- **Filter** (`irene/data/sandbox/sets/short_dated.yaml`):
  `e.hours_to_expiry < 24`. Markets that resolve within a day, so the post-news price has less time to revert to noise inside our 20-minute capture window.

- **Sizing:** quarter-Kelly, max 10% bankroll per trade, exit_horizon 600 s.

## Honest caveats

- **6 of the 9 wins are correlated.** The LLM correctly faded the same news cluster ("Peace signals between US and Iran could increase oil supply") across multiple related oil/Iran markets. The effective independent N is closer to 3–4 distinct events.
- **9 trades isn't decisive.** Sharpe 1.83 is encouraging but a single bad news cluster would wipe out the edge.
- **Recommendation:** capture more `short_dated × high-impact` events before deploying. Validate with a longer hold-out.

## Everything that *didn't* work

Per-strategy summary across the cross-product (sets with ≥5 executed trades):

| Strategy | Runs | Total trades | Mean PnL/trade | Mean ROI(bk) | Sharpe |
|---|---:|---:|---:|---:|---:|
| t1-fade-overreaction | 5 | 53 | $-1.12 | -1.18% | -2.90 |
| v3-kelly-pyes | 7 | 72 | $-1.41 | -1.45% | -6.40 |
| v3b-kelly-fast | 5 | 48 | $-1.55 | -1.49% | -5.69 |
| t3-magnitude-classifier | 6 | 129 | $-1.10 | -2.36% | -3.04 |
| t5-fast-momentum | 6 | 149 | $-1.48 | -3.67% | -3.48 |
| t4-devil-advocate | 6 | 173 | $-1.52 | -4.37% | -4.61 |
| t7-fade-momentum | 6 | 149 | $-3.08 | -7.65% | -7.95 |
| t2-no-anchor | 6 | 518 | $-2.92 | **-25.22%** | -7.91 |
| v1-baseline | 5 | 956 | $-5.93 | **-147.38%** | -7.99 |

Per-set summary (best to worst):

| Set | Runs | Total trades | Mean win-rate | Mean ROI(bk) |
|---|---:|---:|---:|---:|
| midband_liquid (19 rows) | 8 | 72 | 0.00% | -1.37% |
| liquid (64) | 8 | 134 | 1.39% | -2.59% |
| **short_dated (92)** | 5 | 139 | **28.32%** | -4.18% |
| tight_spread (201) | 8 | 282 | 5.18% | -4.97% |
| midband (88) | 8 | 179 | 9.86% | -7.94% |
| tradeable_all (335) | 8 | 461 | 7.02% | -15.52% |
| explorer0_all (legacy) | 4 | 458 | 6.47% | -58.72% |

Key non-findings:

- **`v3-kelly-pyes`** (existing production-style prompt): –2.05% on tradeable_all, **0% win-rate on 13 trades**. The current prompt is not predictive on its own.
- **`t2-no-anchor`** (LLM without the market mid in the prompt): –70.24% bankroll on tradeable_all, 206 trades. Removing the anchor makes the LLM wildly overconfident — it claims edge on far too many rows and is wrong on most.
- **`t5-fast-momentum`** (no-LLM, "if price moves in first 60 s, follow it") and **`t7-fade-momentum`** (inverse): both lose. The first-minute price drift is non-predictive in *either* direction.
- **`t1-fade-overreaction`** (steady-state prompt): –1.80% — better than `v3` but still net-negative.
- **`v1-baseline`** (production relatedness): catastrophically bad on every set (–33% on `all_complete`, –210% on `all_explorations`). Strong signal that the production relatedness gate is letting through losing trades.

## Data-shape findings that constrained the experiment

1. **Capture window is 20 min, not 1 h.** 331/334 tradeable rows have `max(offset_seconds) ≤ 1200`. The existing strategies used `exit_horizon_seconds: 3600`, silently falling back to the last available tick. All new strategies use 600 s or 1200 s.
2. **`freshness` and `article_tier` are useless filters.** 353/430 rows have `freshness=NULL`; all 430 articles are `tier=mainstream`. Theories that depend on these dimensions can't be tested on this data.
3. **Useful filter dimensions:**
   - `market_mid_t0 BETWEEN 0.2 AND 0.8` → 88 rows (mid-band, where Kelly is well-behaved)
   - `m.volume_usdc > 50000` → 64 rows (liquid)
   - intersection → 19 rows (small but cleanest universe)
   - `market_spread_t0 < 0.04` → 201 rows (tight entry friction)
   - `hours_to_expiry < 24` → 92 rows (short-dated)

## Files added in this sweep

**Strategies** (`irene/sandbox/strategies/`):
- `t1-fade-overreaction.yaml` — steady-state p_yes, exit_horizon=1200
- `t2-no-anchor.yaml` — same task as v3 without the price anchor
- `t3-magnitude-classifier.yaml` — 2-step impact-classifier → conditional p_yes ★ winner
- `t4-devil-advocate.yaml` — 2-step pros-yes/pros-no → p_yes
- `t5-fast-momentum.yaml` — no-LLM deterministic momentum
- `t6-skeptic-tight-edge.yaml` — same prompt as v3, min_edge=0.10
- `t7-fade-momentum.yaml` — no-LLM, inverse of t5
- `v3b-kelly-fast.yaml` — same prompt as v3, exit_horizon=600

**Filter sets** (`irene/data/sandbox/sets/`):
`tradeable_all`, `midband`, `liquid`, `midband_liquid`, `tight_spread`, `short_dated`.

**Engine changes:**
- `irene/sandbox/strategies.py` — added `kind: str` and `kind_config: dict` fields to `Strategy`.
- `irene/scripts/sandbox/run_strategy.py` — added `_momentum_decision()` and a branch on `strat.kind == "deterministic_momentum"` before the LLM path. Supports a `momentum.invert` flag.

**New tooling:**
- `irene/scripts/sandbox/leaderboard.py` — scans `irene/data/sandbox/runs/`, computes ROI / win-rate / Sharpe-like / capital deployed / avg edge per run. Flags: `--min-trades N`, `--by run|strategy|set`, `--sort <metric>`.

## How to reproduce

From `/home/muk/projects/jennie/`:

```bash
# Load API key
set -a; . irene/.env; set +a

# Build sets
python3 -m irene.scripts.sandbox.make_set --name tradeable_all --all-explorations
python3 -m irene.scripts.sandbox.make_set --name midband        --filter "e.market_mid_t0 BETWEEN 0.2 AND 0.8"
python3 -m irene.scripts.sandbox.make_set --name liquid         --filter "m.volume_usdc > 50000"
python3 -m irene.scripts.sandbox.make_set --name midband_liquid --filter "e.market_mid_t0 BETWEEN 0.2 AND 0.8 AND m.volume_usdc > 50000"
python3 -m irene.scripts.sandbox.make_set --name tight_spread   --filter "e.market_spread_t0 IS NOT NULL AND e.market_spread_t0 < 0.04"
python3 -m irene.scripts.sandbox.make_set --name short_dated    --filter "e.hours_to_expiry < 24"

# Run the winner directly
python3 -m irene.scripts.sandbox.run_strategy --set short_dated --strategy t3-magnitude-classifier --max-calls 200

# Re-rank everything
python3 -m irene.scripts.sandbox.leaderboard --min-trades 5
python3 -m irene.scripts.sandbox.leaderboard --min-trades 5 --by strategy
python3 -m irene.scripts.sandbox.leaderboard --min-trades 5 --by set
```

LLM responses are SHA-cached, so subsequent runs against new filter sets reuse the cache and cost zero new API calls.

## Suggested next experiments

1. **t3 variants:**
   - Restrict to `impact="large"` only (drop medium). Smaller N, higher conviction.
   - Add a *second* magnitude pass inverting the question (cross-check).
2. **Larger short_dated capture.** The capture pipeline runs continuously; let it accumulate ≥30 short_dated rows before re-running the leaderboard.
3. **Investigate `v1-baseline` losses.** The production gate is letting through trades that lose money on every filter slice. Worth a root-cause investigation separately from this sweep.
4. **Combine winning filter with other prompts.** Re-run `t1`, `t4`, `t6` against `short_dated` only; the cache makes it free.
