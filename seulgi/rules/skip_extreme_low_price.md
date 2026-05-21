---
name: extreme_low_price
stage: pre_ai
action: skip
mid_max: 0.10
engines_exclude: scout
---

# Skip markets priced ≤ 0.10

## Evidence
From the 2026-05-14 post-mortem `bucket_stats.md`:

| p0 range | n | no_signal rate |
|---|---|---|
| [0.00, 0.05) | 44 | **100.0%** |
| [0.05, 0.10) | 20 | **90.0%** |

The hard rail at 0.02 and the soft rail at 0.05 already cover the worst of
this band; this rule extends the floor to 0.10. The dollar cost of skipping
is near-zero (we're sacrificing 0.0% captured trades) and we stop burning AI
tokens evaluating these markets.

## Caveats
The 0.05–0.10 band can occasionally host a flip-trigger (e.g. a very
unlikely event headline that confirms the unlikely outcome). When we
eventually wire the AI-gated exception path, this rule is a candidate for
`has_exceptions: true` with the override "headline confirms a low-probability
event has actually occurred."
