---
name: mid_below_065
stage: pre_ai
action: skip
mid_max: 0.65
engines_exclude: scout
---

# Skip markets with mid < 0.65

## Evidence
2026-05-19 forensic on 190 organic closes:

| mid bucket | n | hit | sum_pnl |
|---|---:|---:|---:|
| 0.30–0.50 | 22 | 4.5% | −$1.17 |
| 0.50–0.70 | 67 | 22.4% | −$2.17 |
| **0.70–0.80** | **61** | **57.4%** | **+$0.06** |
| 0.80–0.90 | 40 | 22.5% | −$1.03 |

Below 0.65 the hit rate collapses. The 0.50–0.70 cell with 67 trades and
22% hit accounts for half of total loss after the BREAKING+NO bucket.

## Caveats
Combined with `skip_mid_above_085`, entry is gated to [0.65, 0.85] —
where the only profitable cell in the data lives. Outside this range we
have no measured edge, so denying entry is conservative.

If a variant explicitly wants to play outside this range (e.g. a "long
tail flip-trigger" variant), it would need to override via
`mid_range_override` (not yet implemented; revisit when a use case lands).
