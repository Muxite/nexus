# Calibration seeds (📊)

> Statistical claims from external research that should NEVER be treated as
> ground truth. Each seed is the RAG value we have today; the bot's own
> measurement is the source of truth. When measurement lands, update the
> seed entry with the measured value + a pointer to the producing report.
>
> Convention (per the research-distill pipeline plan):
> - `# Seed:` — the RAG number we started with
> - `# Source:` — batch + raw quote
> - `# Measured:` — our own value (when collected)
> - `# Producing:` — report path that contains the measurement
> - `# TODO:` — open work to replace the seed

---

## A2 — Per-market `taker_base_fee` distribution

```
# Seed: category-mapped rates (crypto 7.2%, finance/politics/tech 4%, sports 3%, etc.)
# Source: unstructured answers.md 2026-05-25-unstructured-1 lines 7-14
#         + 2026-05-25 cycle-10 1000-market survey returning {0: 999, 200: 1}
# Measured (2026-05-25, cycle 15 top-500 by volume24hr survey via /markets/<cid>):
#   fee_rate distribution (n=500):
#     0.0000 (0% taker):   89 / 500  = 17.8%
#     0.1000 (10% taker): 411 / 500  = 82.2%
#   ALL 500 sampled markets had minimum_order_size = 5 shares (uniform).
#   USDC floor (= 5 × yes_price): median $0.10, p90 $3.52, max $5.00.
#   Markets with per-market USDC floor > our $25: 0 / 500 = 0%.
# Producing: irene/reports/2026-05-25-cycle15-market-floor-audit.md
# Sample comparison: the cycle-10 survey saw {0: 999, 200: 1} — most likely
# hit random/low-volume markets via default Gamma pagination order. The
# cycle-15 explicit `order=volume24hr&descending=true` survey of the TOP
# 500 shows a fundamentally different picture: the high-volume tail is
# DOMINATED by 10% taker, not 0% or category-tier rates.
# Action items (in order):
#   1. Cycles 10+13 already fetch fee_rate per-market on first-seen, so
#      production trades use the real 10% (not the 5% category default)
#      automatically. THIS IS A RELIEF — the bug we'd otherwise have is
#      already plugged.
#   2. The category-fallback table in config.polymarket_fee_rates (default
#      0.05) materially understates the true mean fee on the trading tail.
#      Recommend raising default_fee_rate from 0.05 -> 0.10, and noting in
#      the docstring that the table is a fallback for unfetched markets
#      only (per-market fetch is the source of truth post-cycle-10).
#   3. min_order_size is uniform at 5 shares; the cycle-13 per-market gate
#      is well-designed but the worst-case USDC floor ($5) is well under
#      our config floor ($25), so the per-market check basically never
#      binds in practice. Keep the gate for defense-in-depth.
```

## C1 — Bid-ask spread distribution by volume tier

```
# Seed: $0.01-0.02 tight / $0.03-0.05 medium / $0.05+ wide
# Source: unstructured answers.md 2026-05-25-unstructured-1 line 203
#
# Measured #1 — high-volume (2026-05-25 cycle-10 soak, top-30 by
# volume24hr, n=812 samples over 30 min):
#   ≥$1M volume:      median 0.0010, p90 0.0100, max 0.0100
#   $500k-$1M volume: median 0.0010, p90 0.0100, max 0.0200
#   $100k-$500k:      median 0.0025, p90 0.0040, max 0.0040
#   All samples:      p50 0.0010, p90 0.0100, p99 0.0200, max 0.0200
#   spread > $0.10:   0 / 812 = 0.0% (wide_spread gate would never fire)
# Producing: irene/reports/2026-05-25-cycle12-data-collection.md
#
# Measured #2 — low-volume (2026-05-25 cycle-14 soak v2, $10k-$50k band,
# top-30 by volume24hr, n=790 samples / 37 unique markets over 28 min;
# only sampled markets all landed in $30k-$50k subband, didn't reach
# below $30k — TOP_N=30 was filled before walking that far):
#   $30k-$50k:        n=37, median 0.0010, p75 0.0100, p90 0.0100, max 0.0450
#   All samples:      p50 0.0010, p75 0.0100, p90 0.0100, p99 0.0300, max 0.0450
#   spread > $0.05:   0 / 790 = 0.0%
#   spread > $0.10:   0 / 790 = 0.0%
# Producing: irene/reports/2026-05-25-cycle16-low-vol-spread-analysis.md
#
# Combined interpretation:
# - Spreads at $30k-$50k volume look IDENTICAL to spreads at $500k+
#   volume (same medians, slightly wider tail at $0.045 vs $0.020 max).
# - The RAG "$0.05+ wide" claim is invalidated for markets above $30k.
# - Markets below $30k still unsampled — TRUE niche markets (<$1k) may
#   yet show the RAG-claimed wider spreads.
# - cfg.trade.wide_spread_threshold = 0.10 is now even MORE conservative
#   than cycle 12 thought: across 1602 samples spanning $30k - $5M+
#   volume, ZERO would have tripped. Could tighten to 0.05 with high
#   confidence (still leaves 0/1602 = 0% rejection rate on this dataset).
# - Defer tightening: collect a <$30k sample first to confirm the
#   thin_market + wide_spread pair is well-tuned even at the bottom of
#   the active universe.
```

## C4 / E2 — CLOB round-trip latency from our host

```
# Seed: matching engine 70-85ms + 50-100ms network from London / 180-420ms residential
# Source: unstructured answers.md 2026-05-25-unstructured-1 lines 60-62, 223-224
# Measured (2026-05-25, /midpoint GET, n=300 sequential samples over 5 min):
#   min  143.23ms    p50 149.74ms
#   p90  157.26ms    p95 160.84ms
#   p99  193.43ms    max 308.88ms
#   mean 152.01ms    stdev 13.38ms
#   failures: 0 / 300
# Producing: irene/reports/2026-05-25-cycle12-data-collection.md
# Host: current dev box (not co-located with eu-west-2).
# Action: ~150ms p50 from our host vs RAG's sub-100ms pro floor — we're
#         ~50ms over the floor; closing the gap needs co-located infra.
#         Well under the 2s philosophy.md budget (13x margin). The 200ms
#         claim in philosophy.md is currently tight to our reality on a
#         single CLOB call; multi-call chains (mid + book + fee_rate)
#         would blow it. Replan the budget once we have a multi-call
#         per-signal latency measurement.
```

## E1 — News repricing half-life

```
# Seed: 80% reversion in hours, complete recovery 24-48h; top 1% capture ~84%
#       of gains via latency arb; sub-100ms execution required
# Source: unstructured answers.md 2026-05-25-unstructured-1 lines 55-57
# Measured: (not yet — needs the cycle-11 long-tail impact buckets
#           +7200/14400/28800/86400 to accumulate signal data over a soak)
# TODO: 30+ day soak of impact_tracker post-cycle-11 schema; bin by signal
#       freshness label (BREAKING / EVOLVING / KNOWN) and compute median
#       half-life per bin. Replace this seed with that median.
```

## F1 — Kelly fraction for binary prediction markets

```
# Seed: 0.25 (quarter Kelly) — literature consistent
# Source: unstructured answers.md 2026-05-25-unstructured-1 line 71
# Current config: cfg.trade.kelly_fraction = 0.25 — matches seed
# Measured: literature-consistent; our own measured optimum awaits 30+ day
#           soak with stable execution
# TODO: revisit only after 30-day soak data + the W2 p_hat-cap work
#       (cfg.trade.p_hat_cap) lands in a future cycle
```

## F2 — Minimum edge threshold

```
# Seed: 3-5% required; 4% optimal for compounding (Kalshalyst reference)
# Source: unstructured answers.md 2026-05-25-unstructured-1 line 74
# Current config: cfg.trade.min_edge = 0.04 — matches seed
# TODO: after the cycle-10 explicit-spread-term lands data, re-validate
#       that 0.04 is still the right floor given the new (smaller)
#       edge-after-costs values
```

## F4 — Win rate + variance

```
# Seed: ~85% win rate for grinding strategies; 1-3% per-trade return;
#       heavy-tailed losses on adverse selection
# Source: unstructured answers.md 2026-05-25-unstructured-1 line 80
# Measured: not yet (needs paper-trade soak with cycle-11 schema)
# TODO: track realized win-rate per cycle; replace seed when we have
#       statistically meaningful n (≥100 closed trades)
```

## D3 — UMA dispute frequency

```
# Seed: ~1.96% disputed; 98.11% undisputed; >80% disputes from "too early"
# Source: unstructured answers.md 2026-05-25-unstructured-1 line 188
#         (UMA stats as of August 2024)
# Measured: not yet (cycle-11 D1 state machine records uma_proposed_at /
#           uma_finalized_at on transition; cycle-12 orchestration poll
#           feeds it on closed markets)
# TODO: after 100+ resolved markets in our own data, compute our observed
#       dispute rate per market_category and compare against the 1.96%
#       baseline. Replace seed if it drifts materially.
```

---

When you ADD a seed, also update the entry in `research_answers.md` to point
to the relevant `📊` block here. When you REPLACE a seed with measured data,
update the entry above with `# Measured:` and `# Producing:` lines pointing
at the report that contains the measurement.
