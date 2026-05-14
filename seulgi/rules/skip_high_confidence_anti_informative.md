---
name: high_confidence_anti_informative
stage: post_ai
action: skip
confidence_min: 0.75
---

# Skip when the AI is highly confident

## Evidence
From the 2026-05-14 post-mortem `bucket_stats.md`, confidence bucketed:

| confidence | n | captured | no_signal |
|---|---|---|---|
| [0.75, 0.85) | 22 | **0** | 100.0% |
| [0.85, 0.95) | 29 | **0** | 96.6% |
| [0.95, 1.01) | 39 | **0** | 100.0% |

The global IC is -0.007. Within these three high-confidence buckets it is
solidly negative — the AI is most wrong when it is most sure. The
`inverted_highconf` engine was designed to test the same hypothesis at
threshold 0.75 and is currently down ~$9 from its prior cycle (per
`delta_vs_previous.md`), suggesting that flipping direction is also not the
answer. Simplest move: do not trade at all when the AI is highly confident.

## Caveats
This rule fires *post_ai* so the AI tokens are already spent. Once the
behavior is validated, the next iteration can move the cutoff into the
classifier (skip even calling the AI for matches we'd skip anyway based on
prior confidence distributions — but that requires confidence proxying,
which is harder).

## Interaction with `inverted_highconf`
The shadow engine inverts direction at conf ≥ 0.75 in non-rail markets.
This rule fires post-AI for ALL engines including `inverted_highconf` — it
will therefore cancel the inversion shadow. That is intentional for this
cycle: if inversion were working we'd see captured>0 in these buckets, and
we don't.
