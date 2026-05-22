---
name: mid_above_085
stage: pre_ai
action: skip
mid_min: 0.85
engines_exclude: scout, super_scout
---

# Skip markets with mid > 0.85

## Evidence
2026-05-19 forensic on 190 organic closes:

| mid bucket | n | hit | sum_pnl |
|---|---:|---:|---:|
| 0.70–0.80 | 61 | 57.4% | +$0.06 |
| 0.80–0.90 | 40 | 22.5% | −$1.03 |

Above 0.85 the asymmetric payoff (small TP target vs large SL exposure
when the market drifts toward the 1.0 rail) destroys expectancy. The
existing `extreme_high_price` rule fires at 0.95+; this extends the
ceiling down to 0.85 where the empirical degradation begins.

## Caveats
Same as `mid_below_065` — combined gate keeps entries in [0.65, 0.85].
