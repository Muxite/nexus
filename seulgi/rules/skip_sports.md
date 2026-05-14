---
name: sports
stage: pre_ai
action: skip
market_type: ^sports$
---

# Skip sports markets

## Evidence
The user-curated thesis: people watching the actual sports event have far
more real-time information than a headline-driven bot ever will. The
markets re-price on play-by-play data we don't ingest, so a news headline
arriving minutes-to-hours after the action is structurally late.

Sports is also a large slice (~27% of the market universe — see
`scripts/postmortem/tag_market_types.py` distribution), so skipping it
removes a significant token spend with negligible expected captured share.

## Caveats — future exception
The user explicitly flagged the carve-out we want: a news-driven sports
trade only makes sense when the headline names a specific player AND
contains a high-impact event (ACL tear, sudden retirement, mid-match
ejection). When we wire the AI-gated `has_exceptions: true` path, this is
the first rule to upgrade.

Until then: hard skip.
