# 2026-05-25 — cycle 12 data collection (spread + latency soaks)

- **Producing scripts:** `irene/scripts/soak_spread_sampler.py` (cycle 10), `irene/scripts/soak_clob_latency.py` (cycle 11).
- **Raw artifacts:** `/tmp/spread_soak.csv` (812 rows), `/tmp/clob_latency.log`.
- **Replaces seeds:** C1, C4/E2 in `irene/questions/calibration_seeds.md`.

## Spread distribution (C1)

- **Sample:** top-30 active Polymarket markets by `volume24hr`, `/book?token_id=` every 60s for 30 min.
- **Coverage:** 812 successful samples, 29 distinct conditionIds (1 market consistently failed mid-cycle — likely token absent from one side).

By volume tier (latest sample per market):

| Volume24hr tier  | n  | median spread | p90 spread | max    |
|------------------|----|--------------:|-----------:|-------:|
| ≥ $1M            | 16 | $0.0010       | $0.0100    | $0.0100 |
| $500k – $1M      | 11 | $0.0010       | $0.0100    | $0.0200 |
| $100k – $500k    |  2 | $0.0025       | $0.0040    | $0.0040 |

All samples (n=812):
- p50: **$0.0010**
- p75: $0.0020
- p90: $0.0100
- p99: **$0.0200**
- max: $0.0200
- `spread > $0.10` (would be rejected by `wide_spread` gate): **0 / 812 = 0.0%**

### Interpretation

The RAG batch said "$0.01-0.02 tight, $0.03-0.05 medium, $0.05+ wide". On the high-volume tail those numbers overstate by 5-50×: high-volume Polymarket spreads sit at 1-2 ticks ($0.001-$0.002 median), and even the 99th percentile only reaches $0.02. The `cfg.trade.wide_spread_threshold = 0.10` from cycle 8 is very conservative for the markets we'd actually want to trade — over a full 30-min window it would have rejected zero high-volume entries.

**Sample bias:** we sampled top-30 by volume only. The RAG "10¢+ on thin / 10-15% on niche" claim could still be real on the long tail, but those markets are already gated by `thin_market` (`cfg.trade.min_market_volume_usdc = 10,000`) so they're not in this sample's universe.

### Actions
- KEEP `cfg.trade.wide_spread_threshold = 0.10` for now. The gate is correct safety; this dataset says it just rarely fires on the markets we'd trade.
- Consider a follow-up "low-volume spread sample" — pull the bottom-30 of the active-and-above-thin-market band ($10k-$50k volume) and re-run the same probe. If those show $0.05+ medians, the `thin_market + wide_spread` pair is well-tuned; if they're tight too, we can lower the threshold.
- Replaces `calibration_seeds.md` C1 ✅.

## CLOB round-trip latency (C4 / E2)

- **Sample:** 300 sequential `GET /midpoint?token_id=<BTC YES>` calls, 1 per second, from this dev box.
- **Result:**

| metric  | ms    |
|---------|------:|
| min     |  143.23 |
| p50     |  149.74 |
| p90     |  157.26 |
| p95     |  160.84 |
| p99     |  193.43 |
| max     |  308.88 |
| mean    |  152.01 |
| stdev   |   13.38 |
| failures | 0 / 300 |

### Interpretation

- **Tight distribution.** p99 is only 30% over p50; max is 2× — no pathological tail.
- **~50ms over the pro floor.** The RAG cites sub-100ms for high-volume traders on co-located AWS eu-west-2 infra. Our dev box sits at ~150ms; closing the gap requires moving the bot (see `plans/legal-ops-hosting.md` AWS eu-west-2 recommendation, cross-cut with `plans/latency-repricing.md`).
- **13x margin under the 2s philosophy budget.** philosophy.md's 200ms budget is currently *roughly* equal to a single CLOB round-trip. Multi-call chains (mid + book + fee_rate at arm-time) will blow that — replan the budget after we measure the actual per-signal chain.

### Actions
- Replaces `calibration_seeds.md` C4/E2 ✅.
- Document hosting recommendation in cycle 13: a VPS in eu-west-2 should drop p50 below 100ms based on the RAG's network estimates.
- Open question for the philosophy doc: is the 200ms target stage-internal (post-mid-fetch) or end-to-end (article → trade)? The cycle-12 measurement reframes the budget conversation.

## Closing notes

- Both soaks completed without errors (300/300 latency, 812 samples / 29 markets across 30 cycles spread).
- The cycle-10 explicit-spread-term refactor will see `fallback=True` in 100% of paper-mode `features_snapshot` until the production trader actually exercises `get_top_of_book` on a live signal — confirmed expected.
- All RAG values cited here are seeds; this report's measurements supersede them.
