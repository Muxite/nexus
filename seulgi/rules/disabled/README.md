# Disabled rules

The `rules/` loader uses `glob("*.md")` (non-recursive, `app/rules.py:313`),
so any `*.md` files moved into this subdir are not loaded at runtime.

The markdown files here retain their evidence + caveats sections so the
historical rationale isn't lost when the rule is re-evaluated. To
re-enable a rule, `git mv` it back to the parent `rules/` directory.

## Currently disabled

- `skip_mid_below_065.md` — retired 2026-05-24 in the F1 retry
  escalation. The original 190-row cycle-9 evidence base was flagged
  stale by A0.10. Pre-AI rule was killing 35% of decisions in the
  2026-05-24 smoke without measurable PnL benefit; re-evaluate after
  ≥7d of post-retirement closed trades land.
- `skip_extreme_low_price.md` — retired 2026-05-24 in the same
  escalation. Cycle-9 evidence base for the 0.05–0.10 band. Hard rail
  (0.02) and soft rail (0.05) still cover the worst of this band; the
  extension to 0.10 was the marginal cut and the 44% skip share
  outweighs the conservative-skip rationale.
