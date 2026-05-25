# 2026-05-25 — cycle 16 low-volume spread analysis

- **Producing script:** `irene/scripts/soak_spread_low_volume.py` (cycle-14 v2, ran 28 cycles / ~28 min).
- **Raw artifact:** `/tmp/spread_soak_low_vol.csv` (790 rows, 37 unique markets).
- **Pairs with:** `irene/reports/2026-05-25-cycle12-data-collection.md` (high-volume sample).
- **Updates:** `irene/questions/calibration_seeds.md` C1 entry.

## What we sampled

- Top-N=30 active markets in the `$10k-$50k volume24hr` band, refreshed every 60s, polled `/book?token_id=` for the YES side.
- Sampling order: `volume24hr` descending + post-filter on the band. Because the descending walk fills `TOP_N=30` before going much below $30k, the dataset is effectively `$30k-$50k volume`, NOT the full target band. Pre-band markets (>$50k) were skipped; sub-band markets (<$30k) were never reached.

## Results

| Volume bucket  | n unique markets | median spread | p75    | p90    | max    |
|----------------|------------------|--------------:|-------:|-------:|-------:|
| $30k-$50k      | 37               | $0.0010       | $0.0100 | $0.0100 | $0.0450 |

All 790 samples combined:
- p50 = $0.0010
- p75 = $0.0100
- p90 = $0.0100
- p99 = $0.0300
- max = $0.0450
- spread > $0.05: **0 / 790 (0.0%)**
- spread > $0.10: **0 / 790 (0.0%)** — would not trip the `wide_spread` gate

## Cross-reference vs cycle 12 (high-volume sample)

| Metric | Cycle 12 (top-30 by vol, $100k-$5M+) | Cycle 16 ($30k-$50k) |
|--------|---------------------------------------|----------------------|
| n samples | 812 | 790 |
| median spread | $0.0010 | $0.0010 |
| p90 spread | $0.0100 | $0.0100 |
| p99 spread | $0.0200 | $0.0300 |
| max spread | $0.0200 | $0.0450 |
| % > $0.10 | 0.0% | 0.0% |

**Spreads at $30k-$50k volume look essentially identical to spreads at $500k+ volume.** The tail is mildly heavier (p99 0.03 vs 0.02; max 0.045 vs 0.02), but the medians and p90s are the same.

## Interpretation

1. **The RAG's "$0.05+ wide" claim is invalidated** for all market volumes above $30k. The transition (if it exists) is somewhere below.

2. **`cfg.trade.wide_spread_threshold = 0.10` is very conservative.** Across 1602 combined samples spanning the full $30k–$5M+ active band, zero would have tripped. Tightening to 0.05 would still produce 0% rejections on this dataset.

3. **Don't tighten yet** — sub-$30k markets are unsampled. True niche markets (<$1k) may yet show the RAG-claimed wider spreads. A cycle 17+ candidate is a `min_market_volume_usdc`-band sampler ($1k–$10k) or a one-shot random-walk through the bottom of the active list.

4. **The `thin_market + wide_spread` pair is well-tuned for the trading band we'd actually use.** `min_market_volume_usdc = $10,000` filters out the band we haven't sampled; `wide_spread_threshold = $0.10` is a safety net that essentially never fires.

## Action items

- Drop the "$0.05+ wide on thin markets" assumption from any plan that cites it without the "below $30k volume" qualifier.
- Defer `wide_spread_threshold` tightening until we have a sub-$30k sample.
- Consider an opportunistic low-volume sampler that walks from the bottom of the active list upward (cycle 17 candidate).

## Closing notes

- Soak completed cleanly in 28 cycles (28-29 markets / cycle); no rate-limit issues.
- The v1 sampler (cycle 13) failed because `ascending=true` is dominated by zero-volume active markets — that filter shape was the bug, not the band itself.
- 1 of every 30 attempted markets was unsamplable (likely token side absent or `/book` empty); 28-29/30 success rate is consistent with the cycle-10 / cycle-12 / cycle-14 spread soaks.
