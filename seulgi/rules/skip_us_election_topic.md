---
name: us_election_topic_low_edge
stage: pre_ai
action: skip
question_rx: (?i)\b(election|democratic\s+party|republican\s+party|house\s+seat|senate\s+seat)\b
---

# Skip US-election micro-races

## Evidence
2026-05-19 forensic: us_election bucket n=10, hit 10%, sum_pnl −$0.58.
Per-race state polling, candidate-specific news, and local political
dynamics are not in our news pipeline; LLM is guessing from generic
political coverage.

## Caveats
This bans specific-race markets ("VA-07 House seat?", "Will the
Democratic Party win X?"). It does NOT ban general macro-political
markets (presidential approval, government shutdown odds — those still
pass via the `(?i)\\btrump\\b`-style coverage in other gates if added).
Revisit when per-race polling integration ships.
