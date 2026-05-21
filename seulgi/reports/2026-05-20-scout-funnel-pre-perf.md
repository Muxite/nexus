# SCOUT funnel report — scout engine
_cutoff: since 12.0h ago (2026-05-20T07:30:11.270397+00:00)_

**Total decisions**: 85,783
**Executed**: 15 (0.02%)
**Closed**: 11 (wins=3, losses=8, hit=27.3%, realized_pnl=$-0.76)

## Where decisions die (first failing step)

| step | count | pct |
|---|---:|---:|
| pre_ai_rule | 82,924 | 96.7% |
| relevance | 2,719 | 3.2% |
| post_ai_rule | 11 | 0.0% |
| duplicate_position | 3 | 0.0% |

## Top skip_reason values

| skip_reason | count |
|---|---:|
| `RULE_extreme_low_price_SKIP` | 42,072 |
| `RULE_mid_below_065_SKIP` | 35,305 |
| `RULE_extreme_high_price_SKIP` | 2,741 |
| `not_relevant:NO` | 1,982 |
| `RULE_iran_topic_anti_edge_SKIP` | 1,602 |
| `RULE_mid_above_085_SKIP` | 849 |
| `not_relevant:WEAK` | 737 |
| `RULE_us_election_topic_low_edge_SKIP` | 355 |
| `neutral` | 98 |
| `EXECUTED` | 15 |
| `RULE_breaking_no_anti_edge_SKIP` | 10 |
| `cluster_size_too_small:0.58` | 5 |
| `cluster_size_too_small:0.71` | 4 |
| `cluster_size_too_small:0.50` | 4 |
| `duplicate_position` | 3 |

## AI Stage direction mix (when reached)

- NEUTRAL: 98
- YES: 26
- NO: 16

## p_alpha distribution (when AI reached)

| p_alpha bucket | count |
|---:|---:|
| 0.8 | 1 |
| 0.7 | 1 |
| 0.6 | 6 |
| 0.5 | 12 |
| 0.4 | 22 |
| 0.3 | 35 |
| 0.2 | 21 |
| 0.1 | 38 |
| 0.0 | 4 |

## signed_edge distribution

| edge bucket | count |
|---:|---:|
| +0.00 | 85,774 |
| +0.01 | 3 |
| +0.02 | 1 |
| +0.03 | 4 |
| +0.04 | 1 |

## mid_yes distribution

| mid_yes bucket | count |
|---:|---:|
| 0.0 | 30,406 |
| 0.1 | 19,724 |
| 0.2 | 9,466 |
| 0.3 | 6,261 |
| 0.4 | 4,497 |
| 0.5 | 6,571 |
| 0.6 | 1,908 |
| 0.7 | 1,808 |
| 0.8 | 1,659 |
| 0.9 | 2,408 |
| 1.0 | 1,075 |

## Counterfactuals (rows that died on each gate)

- If `min_edge` were lowered to -1.0: would admit ~0 more rows.
- If `min_confidence` gate were removed: would admit ~0 more rows.
- If `pre_ai_rule` filters were dropped: would admit ~82,924 more rows.

> NB: these are upper-bound counts — each row could still fail on a downstream gate after the earlier fix.
