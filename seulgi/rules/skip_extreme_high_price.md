---
name: extreme_high_price
stage: pre_ai
action: skip
mid_min: 0.90
engines_exclude: scout, super_scout
---

# Skip markets priced ≥ 0.90

## Evidence
From the 2026-05-14 post-mortem `bucket_stats.md`:

| p0 range | n | no_signal rate | captured |
|---|---|---|---|
| [0.90, 0.95) | 76 | 89.5% | 2.6% |
| [0.95, 1.01) | 327 | **98.8%** | 0.6% |

The soft rail at 0.95 catches the worst slice, but the 0.90–0.95 band is
also dominated by no_signal — the market is already pinned and the headline
can't move it. The captured share is so low that the AI-token cost of
evaluating these markets isn't justified.

## Caveats
Same shape as the low-price counterpart: a confirmed flip-trigger headline
on a high-probability market could legitimately move it back toward 0.5.
Future work: AI-gated exception for "headline materially undermines the
already-priced outcome."
