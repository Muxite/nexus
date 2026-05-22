---
name: breaking_no_anti_edge
stage: post_ai
action: skip
direction: NO
freshness_label: BREAKING
engines_exclude: scout, super_scout
---

# Skip NO trades on BREAKING headlines

## Evidence
From the 2026-05-19 forensic analysis of the first 190 organic
post-simulate-mode closes (`seulgi/data/seulgi.db`,
`closed_at >= '2026-05-18' AND close_reason != 'engine_retired'`):

| freshness | direction | n | hit | sum_pnl |
|---|---|---:|---:|---:|
| **BREAKING** | **NO** | 69 | **8.7%** | **−$2.78** |
| BREAKING | YES | 17 | 35.3% | +$0.05 |
| EVOLVING | NO | 50 | 36.0% | −$1.74 |
| EVOLVING | YES | 54 | 55.6% | +$0.16 |

The "BREAKING + NO" cell alone produced 64% of total dollar losses over
the window. Hit rate 8.7% on n=69 is statistically anti-edge (Wilson
upper bound ≈ 17%) — fading fresh news is exactly when fresh news moves
markets *in* the announced direction.

## Caveats
This is a post_ai gate: the AI still classifies the headline; we just
refuse to act on a NO call. Cost is one classification per BREAKING
headline that would have hit this rule — small.

If a future regime shift makes BREAKING+NO net-positive (e.g. crowd
overreacts and reverses), this rule should be revisited from a fresh
postmortem.
