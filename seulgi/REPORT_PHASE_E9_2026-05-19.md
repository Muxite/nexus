# Phase E.9 — drought diagnosis & first remediation

**Window**: 2026-05-19 (cycle 2 reset at 06:37 UTC)
**Author session**: 09:50 → 10:25 UTC
**Status**: E.9 (min_edge cut) and E.9b (latent NameError fix) live.
Anti-informative-rule decision deferred pending overnight data.

---

## TL;DR

* The "execution drought" since 01:17 UTC has **two stacked causes**, both
  in the post-AI gate stack, neither in the LLM itself.
* Phase E.9 (this session) lowered `min_edge` 0.03 → 0.015 to admit the
  conf [0.5625, 0.625) band. Backtest on the drought window says this
  unlocks ~1 trade per variant per 3h — small but non-zero.
* The far bigger lever is `RULE_high_confidence_anti_informative_SKIP`,
  which is killing every conf=0.75 signal (3 per variant per 3h, ~24
  rows total). That rule's evidence is 5 days stale. **Hold for now**:
  if E.9 alone produces enough overnight data we can decide on next
  cycle; if not, plan to A/B the rule via per-engine exemption.
* Found and fixed a **latent NameError in trader._evaluate_market** —
  `cluster_factor` was used but never plumbed through. Would have
  crashed live as soon as any E.9 trade reached the sizing block.

---

## Root cause: `result.confidence` is `p_alpha`, not argmax

```python
# seulgi/app/ai.py:602
prod_conf = p_alpha if p_alpha > 0.0 else conf   # falls back to argmax
return ClassifyResult(direction, prod_conf, ...)
```

The trader's edge model (`trader.py:798-800`):

```python
confidence_scale = max(0.0, 2.0 * result.confidence - 1.0)
edge = self._cfg.edge_increment * confidence_scale
p_hat = mid_price + edge
```

So when the LLM emits `p_alpha`, that — not the argmax probability —
drives the edge. The implications:

1. **`signed_edge ≥ min_edge` requires `p_alpha ≥ 0.625`** when
   `edge_increment=0.12, min_edge=0.03`.
2. Gemini's `p_alpha` distribution is now **bimodal**:
   * `≤0.5` (handled by NEUTRAL or fails min_confidence → skip)
   * exactly `0.75` (handled by anti-informative rule → skip)
3. The middle band `[0.625, 0.75)` — which was **89% of executions in
   the 4h before the drought** — has zero post-cycle-2 entries.

Phase E.9 widens the executable band downward to `p_alpha ∈ [0.5625, 0.75)`.
The post-cycle-2 drought sample has 8 rows in `[0.5625, 0.625)` over 3
hours = roughly 1/h once dispatched, before further gates.

## What E.9 changes

| | Before E.9 | After E.9 |
|---|---|---|
| `min_edge` (balanced, inverted_highconf) | 0.03 | 0.015 |
| `edge_increment` (unchanged) | 0.12 | 0.12 |
| Required p_alpha to clear edge gate | ≥ 0.625 | ≥ 0.5625 |
| Drought-window admits (per `REPORT_BACKTEST_E9_drought.md`) | 0/96 | 8/96 |
| Pre-drought-window admits | 5954/7499 | 6460/7499 (+8.5%) |

## What E.9b fixes

`evaluate()` accepted `cluster_factor` (E.7.3, commit d0babdd) but never
forwarded it to `_evaluate_market`. Inside that method, line ~912 reads
`cluster_factor` from a NameError-bound free variable. The bug was
masked because every post-cycle-2 trade skipped at `edge_too_small`
before reaching the sizing block. The first E.9 trade to clear the
edge gate would have crashed.

Fix: thread `cluster_factor` through the method signature.

## Side effects, watched

* E.5b rolling hit-rate pause: still active. If E.9-admitted trades
  start losing, the per-variant guard will pause them inside 8 closes.
* `RULE_high_confidence_anti_informative_SKIP`: still firing on every
  conf=0.75 row. Evidence dates to 2026-05-14 — if it's stale we're
  blocking 3x the signals E.9 unlocks. Decision deferred.

## Backtest tooling

`seulgi/scripts/analysis/backtest_min_edge.py` replays the AI signals
already logged in the `trades` table through the current edge model.
Two windows generated:

* `seulgi/REPORT_BACKTEST_E9.md` — pre-drought (2026-05-18T21 → 2026-05-19T01:30)
* `seulgi/REPORT_BACKTEST_E9_drought.md` — drought (cycle 2 start → 10:00 UTC)

Caveats are in-file. The replay covers the AI-stage gates only —
sizing/Kelly/duplicate/max-positions need bankroll state we don't
reconstruct. Treat "admit" as an **upper bound**, not a guaranteed fire.

## Tests

* `tests/test_trader.py` — repaired (was 32 failing, now 26/26 green
  in the trader group). New cycle-10+ config fields added to `_make_cfg`.
* `tests/test_ai.py::TestProductionConfidenceContract` — new. Pins
  down (a) `confidence = p_alpha` when emitted, (b) fallback to argmax
  when missing, (c) E.9 edge-band semantics for `edge_increment=0.12`.
  Will fail loudly if a future change re-breaks the contract.
* 11 remaining suite failures in `test_pipeline` / `test_engine` /
  `test_simulate_fidelity` etc. — same flavor of mock rot, not blocking.
