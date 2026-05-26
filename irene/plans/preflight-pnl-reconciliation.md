# Plan — preflight gross-vs-net PnL reconciliation

> Source: cycle 22 surprise finding (`cycles/cycle-22.md` §"Surprise
> finding"). Filed as a plan because the fix is non-trivial and the
> formulas live in two places that must stay in lockstep.

## Context

Cycle 22 added `scripts/replay/preflight.py` — a thin CLI wrapper over
`scripts/replay/accuracy.py:run_inproc()` that runs as
`ExecStartPre=-` on each `irene-soak.service` timer fire. The intent
is the P1 ship-gate invariant: every closed trade's stored
`realized_pnl_usdc` must reconstruct within `±0.01` of the closer's
formula.

First live invocation against the production DB exited 1:

```
PREFLIGHT DRIFT: 35/36 within ±0.01; max drift $17.3204
  trade 19: stored=$-1.6687 recon=$15.6518 diff=$17.3204
    (YES size=$158.26 entry=0.4550 exit=0.5000)
```

Diagnosis: `accuracy.reconstruct_pnl()` (lines 57–73 of
`scripts/replay/accuracy.py`) implements the **gross** YES/NO PnL
formula:

```
YES:  size_usdc * (exit_mid - entry_mid) / entry_mid
NO:   size_usdc * (entry_mid - exit_mid) / (1.0 - entry_mid)
```

`app/stages/closer.py` stores `realized_pnl_usdc` **net of fees** —
entry + exit Polymarket taker fees are deducted per cycle-7's parabolic
model (`shares * fee_rate * p * (1-p)`). On a $158 YES position at 10%
taker, round-trip fees ≈ $17.3 — exactly the drift size.

The accuracy.py docstring says the formula is "identical to
`app/stages/closer.py`'s. If that file changes, this file changes in
the same PR." That invariant broke during cycles 7+10+15+16 (fee math
landed in `closer.py`; `accuracy.py` was never updated).

Until reconciled, every preflight tick on a healthy soak DB exits 1.
The kill switch (`-` prefix on `ExecStartPre`) keeps it non-blocking,
but the noise floor masks any genuine drift the gate is meant to catch.

## Recommended approach

Option B: **make `accuracy.py` call the closer's fee helper directly.**

- Extract the per-leg fee computation from `app/stages/closer.py` into a
  pure helper (likely `app/stages/closer.py:_polymarket_taker_fee` or
  similar — verify with `grep -n "def _polymarket_taker_fee" closer.py`).
  If not already pure, refactor it to be pure (no `self`, no DB access).
- In `accuracy.py:reconstruct_pnl()`, after computing gross PnL, subtract
  `_polymarket_taker_fee(size, fee_rate, p_entry)` and
  `_polymarket_taker_fee(size, fee_rate, p_exit)`.
- `accuracy.py` needs the `fee_rate` for each trade. Pull it from the
  trade row's `features_snapshot.market_at_trade.fee_rate` (pinned at
  trade time per cycle-21 PR-B). Fall back to `cfg.trade.default_fee_rate`
  when NULL, matching `closer.py:_resolve_fee_rate`'s logic.

Rejected alternatives:
- **Option A: split into `reconstruct_gross_pnl()` + `reconstruct_net_pnl()`.**
  Doubles the surface area; the gate that matters is net (matches stored
  data). Gross is only useful for diagnostics.
- **Option C: widen tolerance.** Hides the entire failure class the gate
  was built to catch; a $17 drift would be invisible if tolerance were
  $20, and we'd lose all signal value.

Reuse the existing `accuracy.run_inproc(db_path, tolerance) → ReplayResult`
return shape — no API change. `preflight.py` keeps working unmodified.

## Verification

1. `pytest scripts/replay/tests/test_accuracy.py` (write it if missing —
   currently only `test_preflight.py` exists) covers: (a) zero-fee
   market reconstructs exactly to stored net; (b) 10% fee market
   reconstructs to stored net within ±$0.01; (c) NULL `fee_rate` falls
   back to `cfg.trade.default_fee_rate`.
2. Re-run `python3 -m scripts.replay.preflight` against the live DB.
   Expect: `PREFLIGHT OK: 36/36 within ±$0.01`. Trade 19 specifically
   should reconstruct to ≈ $-1.67 (within 1¢ of the stored $-1.6687).
3. Add a CI-runnable regression: a closed trade with known
   `entry_mid=0.45, exit_mid=0.50, size=$158.26, fee_rate=0.10` must
   reconstruct to $-1.6687 ± $0.01 in the test.
4. Update `scripts/replay/accuracy.py`'s docstring "Invariants" section
   to note both formulas now mirror `closer.py`'s net path (not just
   gross), and re-affirm the "any closer.py change ships in the same PR"
   rule.
