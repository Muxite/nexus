# Sandbox sweep — t3b-magnitude-filtered on the full dataset (2026-05-29)

**TL;DR.** The new second-stage pretrade gate + smarter (realistic bid/ask + TP/SL/trailing)
exits do **not** recover a tradeable edge. On the full current dataset the strategy loses
**−$124.88 (−12.49% on a $1,000 bankroll, 7.0% win-rate)**. The decomposition is the headline:
the directional edge is ≈ 0 (−$0.66 of drift across 43 trades) and **99% of the loss is friction
(spread + fees)**. Even a perfect-foresight exit (oracle) only gets to −$77.03, so no exit policy
makes this universe profitable. This corroborates and sharpens the 2026-05-28 conclusion.

## Run

- **run_id:** `20260529T051205Z_t3b-magnitude-filtered_tradeable_all_b99dfb`
- **set:** `tradeable_all` — rebuilt today, **1,465 tradeable rows** (was 1,135 on 05-28;
  the always-on explorer added ~330). By cycle: cycle-23 1,248, explorer-0 217. Resolution:
  all 1,465 resolved (YES 285 / NO 1,180), 0 pending.
- **strategy:** `t3b-magnitude-filtered (v1)` — two-step magnitude classifier + 5-predicate
  pretrade gate (require_llm_related, min_volume, mid_band, min_pre_stdev, max_entry_spread) +
  exit policy (take_profit 0.04 / stop_loss 0.04 / trailing 0.03).
- **cost:** 72 new LLM calls (56 cache hits), **$0.029** of OpenRouter credit. Cap was 500 calls;
  the pretrade gate filtered most rows before the LLM, so the cap never bound.
- reproduce: `python3 -m irene.scripts.sandbox.run_strategy --set tradeable_all --strategy t3b-magnitude-filtered --max-calls 500`

## Headline numbers

| metric | value |
|---|---|
| trades executed | 43 (of 1,465; 1,422 skipped) |
| total PnL | **−$124.88** |
| win-rate | 7.0% (3 W / 40 L) |
| mean PnL/trade | −$2.90 |
| capital deployed | $1,305.97 |
| ROI on deployed | −9.56% |
| ROI on bankroll ($1k) | −12.49% |
| avg edge (executed) | +0.348 |

### Skip-reason histogram (the gate is doing its job — it's the *trades that pass* that lose)
`pretrade:low_volume` 761 · `pretrade:not_related` 347 · `pretrade:extreme_price` 220 ·
`pretrade:flat_history` 65 · `missing_p_yes` 22 · `pretrade:wide_spread` 7.

## Why it loses — PnL decomposition (the important part)

| component | total | mean/trade | note |
|---|---:|---:|---|
| directional drift (move) | **−$0.66** | −$0.02 | what we'd net with zero friction |
| − entry spread cost | −$23.60 | −$0.55 | half-spread crossed on entry |
| − exit spread cost | −$27.51 | −$0.64 | half-spread crossed on exit |
| − fees | **−$73.11** | −$1.70 | flat fee_pct × notional |
| **= total PnL** | **−$124.88** | **−$2.90** | |

Friction (spread + fees) = **−$124.22 ≈ 99% of the loss**. The predictions are not, on net,
directionally wrong — they're break-even before friction. **Fees alone (−$73) are 59% of the
loss.** Trading at ~$85 avg notional, the round-trip spread + fee cost swamps the ~zero
post-news directional edge.

### Edge bucket — confidence doesn't help (it hurts)
The highest-edge bucket is the worst: edge ≥0.30 → n=25, **−$86.00, 4.0% win-rate**. The model is
confidently wrong exactly where it sizes up. (Most of the worst trades are the same correlated
Iran/oil/ceasefire news cluster that flattered the 05-27 sample — independence is still low.)

### Exit ceiling — smarter exits can't save it
| N | realized PnL | oracle (perfect-exit) PnL | harvestable gap |
|---:|---:|---:|---:|
| 43 | −$124.88 | **−$77.03** | +$47.85 ($1.11/trade) |

Even with perfect-foresight exit timing the strategy still loses $77. The exit policy can recover
at most ~$48; it cannot cross zero. **The friction floor, not exit timing, is the binding
constraint.**

## Conclusions / what would actually move profit

1. **There is no directional alpha to harvest here at this horizon** (drift ≈ −$0.02/trade). The
   20-minute–1-hour post-news price action remains non-predictive at scale, matching 05-28.
2. **Friction is the whole story.** To have any chance the universe must be restricted to
   *near-zero-fee, ultra-tight-spread* markets — but `min_volume`/`max_entry_spread` filters
   already cut to 43 trades and friction still dominates. The fee term especially (−$73) means a
   flat-fee structure is incompatible with a sub-1¢ directional edge.
3. **Smarter exits are not the lever** — oracle exit is still −$77.
4. **Recommendation:** do not deploy any current prompt × filter × exit combo. The explorer's
   value remains the *labeled dataset*, not PnL. Next experiments worth running (cheap, cache-warm)
   would target the friction floor directly — e.g. a zero/low-fee market subset, or a much larger
   per-trade edge requirement combined with maker-style (resting, spread-earning) entry rather than
   crossing — rather than another prompt variant.

## Test / codebase note

This run was done after fixing the 30 preexisting test failures and finishing the cycle-26 gate
migration (full suite: **826 passed, 0 failed**). See the accompanying commit.
