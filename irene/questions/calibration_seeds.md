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

## C1 — Bid-ask spread distribution by volume tier

```
# Seed: $0.01-0.02 tight / $0.03-0.05 medium / $0.05+ wide
# Source: unstructured answers.md 2026-05-25-unstructured-1 line 203
# Measured (2026-05-25, top-30 markets by volume24hr, n=812 samples over 30 min):
#   ≥$1M volume:      median 0.0010, p90 0.0100, max 0.0100
#   $500k-$1M volume: median 0.0010, p90 0.0100, max 0.0200
#   $100k-$500k:      median 0.0025, p90 0.0040, max 0.0040
#   All samples:      p50 0.0010, p90 0.0100, p99 0.0200, max 0.0200
#   spread > $0.10:   0 / 812 = 0.0% (wide_spread gate would never fire on this sample)
# Producing: irene/reports/2026-05-25-cycle12-data-collection.md
# Sample bias: top-30 by volume; thin / niche markets not in this dataset.
# Action: high-volume markets are 5-50x tighter than the RAG "tight" prior.
#         Consider tightening cfg.trade.wide_spread_threshold from 0.10
#         after a second window of data, AND collect a separate
#         thin-market sample to confirm the RAG "10¢+ on thin" prior
#         (would inform whether thin_market gate is enough or we need a
#         volume-conditional spread threshold).
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
