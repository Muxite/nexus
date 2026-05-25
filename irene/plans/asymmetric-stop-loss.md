# Plan — asymmetric stop-loss + wrong-hypothesis pattern mining

> Status: STUB (planned during cycle 20 setup, 2026-05-25). Real design + implementation TBD.

## Why this exists

User request 2026-05-25:
> "what if we had a hard stop loss, so if our hypothesis is wrong and the news didnt swing the needle -> lose a little money and fees. if the hypothesis is right -> make a big amount during the swing. then by writing to our db appropriately, we can find patterns in wrong hypothesis cases and learn from our mistakes"

This is **asymmetric risk management**: a tight downside cap + let winners run, with structured logging so we can later mine wrong-hypothesis cases for systematic LLM / gate / source failure patterns.

## Current state

- `cfg.closer.stop_loss_pct` exists (`app/config.py:CloserSection`) but defaults to **0.0 (disabled)** per the existing closer rules (cycle 2: edge_decay → take_profit → stop_loss → stagnation → timeout).
- `take_profit_pct` similarly disabled.
- The primary close path today is `edge_decay` (target_mid hit) — the model's implied "edge collapses here" price, written by `trade.py` at execute time. Cycle 7+ math.
- When a trade exits via any rule, `trade_closures` records `close_reason`, `entry_fee_usdc`, `exit_fee_usdc`, `realized_pnl_usdc`, `exit_mid_price`. The `trades.features_snapshot` (cycle 7) carries the full decision provenance (prompt, LLM call, gate values, p_hat, mid, edge, kelly_full, kelly_capped, size, balance) — so the "what did we believe?" data is already there.

## Design sketch (pre-PR, not implemented)

### Phase A — turn on a tight stop_loss
- Set `cfg.closer.stop_loss_pct` default to e.g. **0.05** (5% of size_usdc). Configurable.
- Validate against open-position simulation: replay the cycle-12 / cycle-15 trades through the new rule; what % of trades would have stopped out vs. closed via other paths? Does the average expected $ outcome shift?
- The cycle-7 `target` close_reason already exists; this proposal just turns it on.

### Phase B — make "winners run" actually run
- Today `take_profit_pct` is also disabled. If we enable stop_loss WITHOUT take_profit, the upside cap is `edge_decay` (which fires when mid reaches target_mid — a model-implied profit ceiling).
- The user's framing implies they want the UPSIDE UNCAPPED beyond the model's target. Two options:
  1. **Disable `edge_decay`** and only rely on stagnation / timeout / stop_loss on the upside. Risk: holding past target_mid means assuming the market overshoots OR the model under-predicted. Today's `target_mid` math is conservative; we'd be trading edge gain for variance.
  2. **Move `edge_decay` to a TRAILING stop after target**: once target_mid is hit, set a trailing stop at e.g. `mid - trail_pct` so we capture overshoots without giving everything back.
- Option 2 is the asymmetric-let-winners-run shape.

### Phase C — structured wrong-hypothesis logging
- Today `trade_closures.close_reason` plus `trades.features_snapshot` is enough to JOIN and find "all trades that hit `stop_loss` and inspect the LLM's call". This is more about an ANALYSIS surface than new schema.
- Add a `scripts/analysis/wrong_hypothesis.py` subcommand: bucket stop-loss trades by direction, freshness, source, similarity tier, and the LLM's confidence. Surface the worst-performing buckets so we can iterate on the gate / prompt / source.
- A `wrong_hypothesis_signature TEXT` column on `trade_closures` would be a useful future addition (a fingerprint of the failure mode: e.g. `"BREAKING|source=reuters|confidence=0.9|direction=YES|outcome=stopped@-5pct"`). Defer until the analysis CLI shows what fingerprint structure is actually useful.

### Phase D — Bayesian update to the source-quality plan
- Stop-loss exits are direct evidence that a source's article didn't move the market our way.
- The `source-quality-weighting.md` plan's pre_news_drift signal + post-trade impact_tracker buckets are the right substrate.
- Cross-link: `wrong_hypothesis` mining feeds the source-quality view.

## Hard constraints

- `philosophy.md` says irene is an "interpreter-and-runner", not a forecaster. The stop-loss is a RISK MANAGEMENT layer, not a prediction-revision layer. Don't add anything that re-interprets the LLM mid-trade.
- Asymmetric risk-management requires real data: the cycle-12 / 14 / 15 / 16 soaks gave us spread / latency / fee data, but not enough closed-trade outcomes to set the right stop_loss threshold. Need ≥100 closed trades' worth of data first.
- W2 (`p_hat=1.0 → ruin` weakspot from the philosophy critique batch) ALSO applies: a tight stop-loss is one of the W2 mitigations.

## When to act

- Earliest after the cycle-20 24h soak: gives us 24h of REAL trade data with the new gates + instrumentation. If we see ≥20 closed trades, Phase A can ship with confidence.
- Cycle 21+: Phase A (turn on tight stop_loss with empirical threshold).
- Cycle 22+: Phase B (decide on disable-edge_decay vs trailing-stop).
- Phase C (analysis CLI) can run in parallel.

## Cross-cuts

- `irene/plans/source-quality-weighting.md` (cycle 20 stub) — Phase D explicitly feeds source-quality from stop-loss outcomes.
- `irene/questions/research_answers.md` W2 — asymmetric stop-loss IS the W2 mitigation.
- `irene/plans/latency-repricing.md` W6 (pre_news_drift) — wrong-hypothesis cases with positive pre_news_drift are the "already priced in" signal materializing.
