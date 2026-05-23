# A4B.4 — threshold re-tune after calibration + EV bundle

Date: 2026-05-23
Agent: Phase 4B.4
Master plan section: 4B.4 — restore execution volume + PnL after the
A4B.1 calibration + A4B.2 true-EV gate bundle.

## TL;DR

After sweeping ~80 (min_ev_per_dollar, min_confidence, calibration_method,
use_true_ev_formula) points against the longest available historical
window (5 days, 2026-05-18..23, 46,155 directional decisions, 270 baseline
trades), **no single threshold triple satisfies all three master-plan
exit criteria simultaneously**:

  1. volume within ±25 % of the legacy 270-trade baseline,
  2. PnL ≥ legacy (sum_pnl=−$12.64),
  3. engine rank order stable (Kendall τ ≥ 0.8).

The bundle, as designed, is *mathematically* incompatible with the
current YES/NO calibration curves on this dataset. Calibration deflates
the median p_alpha from ~0.65 to ~0.33–0.34 (Brier −36 % YES / −54 % NO),
which is statistically correct but pushes nearly every entry below the
true-EV gate AND below Kelly's positive-edge requirement
(p_calibrated < p_market). Result under `use_true_ev_formula=True` +
calibration=isotonic: **0 trades across the entire grid, including a
sweep with `min_ev_per_dollar` ranging from −0.50 to +0.08**.

**Recommendation: phased rollout.** Production engines (`balanced`,
`inverted_highconf`, `loose_relevance_yes`) override
`use_true_ev_formula: false` per-engine — keeping the legacy edge gate
while calibration curves accumulate samples and the LLM model improves.
Research engines (`scout`, `super_scout`) inherit the global default
`use_true_ev_formula: true` so they continue to exercise the new path
on micro-stakes for diagnostic data. `min_ev_per_dollar` and
`min_confidence` are left at their existing values.

This restores the pre-bundle 270-trade volume and −$12.64 PnL
**exactly** in backtest (verified with `--prod-calibration isotonic`
ON; per-engine overrides make the calibrated path a no-op for prod
engines).

## Baseline

Window: `2026-05-18 00:00Z .. 2026-05-23 00:00Z` (longest viable post
cycle-2; 4 days of meaningful traffic + 1h of cycle-3 day-0).

Oracle: `engine_results` (decision_inputs not yet wired — A2.1 pending).
46,155 directional decisions across 681 distinct headlines; 100 %
oracle coverage. Variant config:
`/tmp/a4b4/configs/baseline_legacy.yaml` (= production `config.yaml`
plus `trading.use_true_ev_formula: false` injection — the explicit
legacy gate). Backtest run id: `20260523T003647Z_ea04df`.

| metric                                  | legacy baseline |
| --------------------------------------- | --------------: |
| n_trades_simulated                      | **270**         |
| sum_pnl (longest horizon w/ coverage)   | **−12.6358**    |
| n_positive_engines (sum_pnl > 0)        | 0 / 35          |
| engines that traded                     | 18              |
| n_skipped total                         | 45,885          |
| top skip reason                         | no_price_for_exit (20,387) |

The 18 engines that traded:

| parent              | trades | sum_pnl   |
| ------------------- | -----: | --------: |
| balanced/*          |   195  | −9.293    |
| inverted_highconf/* |    60  | −2.643    |
| loose_relevance_yes |    15  | −0.700    |
| aggressive          |     0  | +0.000 (retired) |
| canary_temp_preflight|    0  | +0.000 (no decisions) |
| scout / super_scout |     0  | +0.000 (size_too_small under backtest's Kelly path) |

Note: scout + super_scout's $0.05–$1 micro-stakes do not pass
`min_trade_usdc` in the backtest's evaluate path, so the backtest
*cannot* observe their volume. In live production they bypass via the
simulate-mode micro-stake logic. The volume question for them is
answered by production logs, not this sweep.

## Sweep results (full grid)

The driver `seulgi/scripts/replay/threshold_sweep.py` runs each grid
point as a separate `backtest.py` subprocess so a `set_default_calibrator()`
install in one point does not leak into the next. Each backtest run
takes ~5 s; total sweep wall time ~5 min for 80+ points.

Calibration curves were fitted offline by
`scripts.analysis.calibration_prototype --since 2026-05-15
--out /tmp/a4b4/calibration.json` against 504 closed trades
(YES n=229, NO n=279). The same curve file feeds the backtest via
`--prod-calibration {platt,isotonic} --calibration-curve …`, which
installs the curves into `app.calibration.set_default_calibrator()`
so trader.py's `calibrate(p_alpha, direction)` picks them up exactly
as production does (only `p_alpha → p_calibrated` is rewritten;
`result.confidence` stays raw, matching the production semantic).

### Coarse 3×3×2×2 (use_true_ev × calibration × min_ev × min_conf)

| use_true_ev | calibration | min_ev | min_conf | n_trades | sum_pnl   |
|:-----------:|:-----------:|:------:|:--------:|---------:|----------:|
| **False**   | identity    | 0.00   | 0.35     | **270**  | **−12.64** ← legacy baseline |
| False       | identity    | 0.00   | 0.20     | 270      | −12.64    |
| False       | identity    | 0.00   | 0.50     | 270      | −12.64    |
| False       | identity    | 0.04   | 0.35     | 270      | −12.64    |
| False       | identity    | 0.08   | 0.35     | 270      | −12.64    |
| False       | isotonic    | 0.00   | 0.35     | 270      | −12.64    |
| False       | isotonic    | 0.04   | 0.35     | 270      | −12.64    |
| False       | isotonic    | 0.08   | 0.35     | 270      | −12.64    |
| True        | identity    | 0.00   | 0.35     | 270      | −26.79    |
| True        | identity    | 0.04   | 0.35     | 262      | −23.84    |
| True        | identity    | 0.08   | 0.35     | 255      | −19.86    |
| True        | isotonic    | 0.00   | 0.35     | **0**    | +0.00     |
| True        | isotonic    | 0.04   | 0.35     | **0**    | +0.00     |
| True        | isotonic    | 0.08   | 0.35     | **0**    | +0.00     |
| (… 36 rows total — see /tmp/a4b4-sweep/sweep_index_coarse.json …) |

Observations:

  - **`use_true_ev_formula=False`** is volume- and PnL-identical
    across every (min_ev, min_conf, calibration) point. This is by
    design: under the legacy gate, p_calibrated has no role (Kelly
    uses `p_hat`, no EV check fires), so calibration is a no-op.
    `min_ev_per_dollar` is also a no-op under legacy.
  - **`use_true_ev_formula=True` + identity** keeps volume close to
    baseline (270 → 255 across `min_ev=[0,0.08]`) but PnL is
    *worse* (−$26.79 → −$19.86), confirming the true-EV gate at
    `min_ev=0 + 2·default_rate=0.10` is gentler than legacy
    `min_edge=0.04` on the same trades but admits a different mix.
  - **`use_true_ev_formula=True` + isotonic** is uniformly **0 trades**
    across all min_ev / min_conf settings, because (a) the EV gate
    fails for nearly every entry (p_calibrated ≈ 0.33 ⟹ ev_yes ≈
    p·(1−ask) − (1−p)·ask ≈ −0.34 for ask ≈ 0.50) and (b) Kelly
    sizing rejects with `kelly_negative_edge` whenever
    p_calibrated < p_market — independent of min_ev. The 18,664
    `kelly_negative_edge` skips dwarf the 235–96 `edge_too_small`
    skips at each individual EV threshold.

### Refinement 5×5 around (min_ev=0.04, min_conf=0.35), isotonic

All 25 points → 0 trades. Sweep file: `/tmp/a4b4-sweep/sweep_index_refine.json`.

### Platt check at (min_ev=0.00, min_conf=0.20)

`{use_true_ev=True, calibration=platt, min_ev=0.00, min_conf=0.20}`
→ 0 trades. Platt and isotonic both deflate p_alpha enough to kill the
EV + Kelly gates. Sweep file: `/tmp/a4b4-sweep/sweep_index_platt.json`.

### Negative-`min_ev` rescue attempt

To pin the failure on the EV gate vs Kelly, ran
`min_ev_per_dollar ∈ {−0.50, −0.30, −0.20, −0.15, −0.10, −0.05, 0.00}`
with `use_true_ev=True` + isotonic. All 7 → 0 trades. Sweep file:
`/tmp/a4b4-sweep/negative_ev/results.json`. Skip-reason breakdown for
the `min_ev=−0.50` run:

  - `kelly_negative_edge`: 18,664   ← Kelly itself, downstream of any EV gate
  - `no_price`: 8,290
  - `no_price_for_exit`: 7,013
  - `size_too_small`: 5,787
  - `edge_too_small (ev=−0.5… < −0.40)`: ~2,500 (cumulative)

Conclusion: even disabling the EV gate (`min_ev=−10`), Kelly
recovers the same collapse because `kelly_p = p_calibrated if
use_true_ev else p_hat` and `fractional_kelly` returns
`skip_reason="kelly_negative_edge"` when `p_true ≤ p_market`. To
restore volume under `use_true_ev=True` + calibration, A4B.3's Kelly
sizing would also need a fallback to `p_hat` — out of scope for A4B.4.

## Composite-score evaluation

Composite score = `sum_pnl - 0.5 * abs(n_trades - 270)`. (PnL is
denominated in dollars over 5 days; volume penalty calibrated so a
±27-trade deviation costs about half what an extra dollar of PnL is
worth.) Top 5 from the full sweep:

| rank | use_true_ev | calibration | min_ev | min_conf | n_trades | sum_pnl  | score    |
| ---: | :---------: | :---------: | :----: | :------: | -------: | -------: | -------: |
|   1  | False       | identity/iso| 0.00   | any      | 270      | −12.64   | **−12.64** |
|   2  | False       | identity/iso| 0.04   | any      | 270      | −12.64   | −12.64   |
|   3  | True        | identity    | 0.08   | any      | 255      | −19.86   | −27.36   |
|   4  | True        | identity    | 0.04   | any      | 262      | −23.84   | −27.84   |
|   5  | True        | identity    | 0.00   | any      | 270      | −26.79   | −26.79   |

Top-3 rank-stability (Kendall τ on per-variant sum_pnl rank vs
baseline):

  - **Rank #1 (legacy baseline duplicate)**: τ = **1.000** (18/18 engines
    identical PnL, identical ordering).
  - Rank #3 (true-EV identity, min_ev=0.08): trades 255 vs baseline 270
    (−5.6 %, within ±25 %), but sum_pnl is $7.22 worse and Kendall τ
    not computed because PnL distribution shifts on a different mix
    of 255 trades. By inspection: scout-grade variants drop out at the
    `min_ev=0.08 + fee_round_trip` threshold first.

The clear winner on the composite is rank #1: **identical to the
legacy baseline**. There is no triple that *improves* PnL because
calibration's correct signal is "you don't have an edge"; the model
just needs more data + a model upgrade.

## Chosen configuration (phased rollout)

Applied to `seulgi/config.yaml`:

```yaml
# global (unchanged from A4B.2)
trading:
  use_true_ev_formula: true          # implicit code default
  min_ev_per_dollar: 0.0             # implicit code default

# per-engine override — production engines opt out of the bundle for
# the migration window. Research engines (scout, super_scout) keep the
# global default to gather diagnostic data on the new path.
engines:
  - name: "balanced"
    use_true_ev_formula: false        # NEW (A4B.4)
  - name: "inverted_highconf"
    use_true_ev_formula: false        # NEW (A4B.4)
  - name: "loose_relevance_yes"
    use_true_ev_formula: false        # NEW (A4B.4)
  # scout, super_scout — inherit global true
```

`min_confidence` (per-engine 0.40 / 0.45 / 0.50) and `min_edge`
(per-engine 0.03 / -10.0) are **not changed** — they're already
hand-tuned and the sweep confirms moving them within ±0.15 has no
volume effect (the legacy gate dominates).

`ai.calibration_method` is **not added to config**. Production's
`main.py:392` hard-codes `Calibrator(db, method="isotonic")`, which
is correct per A3.2 (isotonic has slightly better test-set Brier than
Platt: 0.219 vs 0.220 YES, 0.174 vs 0.177 NO). The calibrator keeps
fitting + refreshing every 4 h regardless of whether any engine uses
the EV path — its diagnostics still flow to the dashboard.

### Per-engine impact estimate

  - **balanced/* (13 variants)**: identical to legacy. 195 trades / 5d
    in this backtest window; expect ~40 trades/day post-rollout.
    PnL signal unchanged. Calibration sees these trades' p_alpha →
    realized_pnl pairs and continues to fit, but doesn't gate them.
  - **inverted_highconf/* (3 variants)**: identical to legacy.
    60 trades / 5d (~12 / day). Same rationale.
  - **loose_relevance_yes/default**: identical to legacy. 15 trades / 5d
    (~3 / day).
  - **scout/all_signals**: in production, micro-stake ($1) decisions
    continue. Under the bundle, true-EV + Kelly will reject ~100 % of
    them with `kelly_negative_edge`. **Expected scout trade volume
    drops to ~0 / day.** This is acceptable for the migration window
    — scout's job is diagnostic data, and `engine_results` still
    captures every directional verdict regardless of whether scout
    trades. If scout volume must stay positive for funnel attribution,
    add `use_true_ev_formula: false` to scout too (single-line patch).
  - **super_scout/all_signals**: same as scout, $0.05 micro-stake.
    Likely ~0 trades / day under the bundle.

### Verification backtest

Re-ran `python3 -m scripts.replay.backtest --since 2026-05-18
--until 2026-05-23 --variant-config config.yaml --prod-calibration
isotonic --calibration-curve /tmp/a4b4/calibration.json --out-dir
/tmp/a4b4/final` after the config edit. Result:

```
n_trades=270  sum_pnl=−12.6358  pos_engines=0
  balanced              n=195 pnl=−9.293
  inverted_highconf     n= 60 pnl=−2.643
  loose_relevance_yes   n= 15 pnl=−0.700
  scout                 n=  0 pnl=+0.000   ← backtest-only artifact
  super_scout           n=  0 pnl=+0.000   ← backtest-only artifact
```

**Exact match with the legacy baseline (270 trades, −$12.6358).**
Kendall τ between legacy and post-edit per-engine PnL ranks: **1.000**
(18 / 18 engines preserved).

## Risks + monitoring

  1. **Scout volume collapse in production.** If the rollout pushes
     scout to ~0 trades/day, the `scout_funnel.py` filter attribution
     loses its denominator. Mitigation: monitor
     `data/dashboard/scout_funnel.txt` for n_trades; if 0 for >24 h,
     add `use_true_ev_formula: false` to the scout engine block too
     (the master plan's exit criterion is rank-stability, not "scout
     must use the bundle").
  2. **Calibration model drift.** The current YES/NO Brier
     improvements (−36 % / −54 %) are on n=229 / n=279 closed trades.
     As the cycle accumulates more closed trades the curves will
     refit every 4h and may move. The dashboard's `calibration`
     diagnostics tile (`Calibrator.diagnostics()`) shows
     `ic_before / ic_after / brier_before / brier_after` per
     direction — watch for IC_after dropping below 0 (signals
     calibration has overfit).
  3. **Forward compatibility.** When the production engines are ready
     to flip back to the bundle (after, e.g., a model swap restores
     prediction calibration), the change is a one-line YAML edit per
     engine: delete the `use_true_ev_formula: false` line and
     re-deploy. No code changes needed — `_OVERRIDABLE_TRADING_FIELDS`
     already includes the knob.
  4. **EV gate threshold staying at 0 + 2·fee_rate.** When the bundle
     is re-enabled for production engines, `min_ev_per_dollar` will
     likely need a non-zero floor (~0.04 from this sweep's coarse
     identity-mode points showed 7 fewer trades for $7 better PnL).
     Re-run `threshold_sweep.py --grid coarse` once calibration is
     less aggressive to pick the precise value.

### Dashboard tile to watch

`seulgi/data/dashboard/engine_comparison.txt` — tracks per-engine
n_trades + sum_pnl. After deploy, expect:

  - `balanced/*`, `inverted_highconf/*`, `loose_relevance_yes/*`
    trade counts within ±25 % of the prior 24h baseline
    (~40 / 12 / 3 trades/day respectively).
  - `scout/all_signals` and `super_scout/all_signals` trade counts
    may drop sharply (toward 0) — this is *expected*, not an alarm
    condition. The `scout_funnel.txt` funnel report will show
    `kelly_negative_edge` as the new dominant cut.

## Artifacts

  - Driver: `seulgi/scripts/replay/threshold_sweep.py` (new — A4B.4).
  - Backtest patch: `seulgi/scripts/replay/backtest.py` adds
    `--prod-calibration {none,platt,isotonic}` (installs the curve
    via `set_default_calibrator()` instead of pre-calibrating
    `result.confidence`, matching production semantics).  Also adds
    `use_true_ev_formula`/`min_ev_per_dollar` to the per-engine
    override list in `_build_engine_cfg` (otherwise the phased
    rollout overrides have no effect under replay).
  - Calibration curve: `/tmp/a4b4/calibration.json` (n=508 closed
    trades; YES isotonic 6 knots, NO isotonic 8 knots).
  - Coarse sweep:  `/tmp/a4b4-sweep/sweep_index_coarse.json` (36 pts).
  - Refine sweep:  `/tmp/a4b4-sweep/sweep_index_refine.json` (25 pts).
  - Platt check:   `/tmp/a4b4-sweep/sweep_index_platt.json` (1 pt).
  - Negative-EV sweep: `/tmp/a4b4-sweep/negative_ev/results.json` (7 pts).
  - Phased rollout test: `/tmp/a4b4-sweep/phased/runs/*/manifest.json`.
  - Verification run:  `/tmp/a4b4/final/runs/*/manifest.json`.

## Test impact

`python3 -m pytest app/tests/test_calibration.py app/tests/test_backtest.py
app/tests/test_trader.py app/tests/test_simulate_fidelity.py` → 131 / 131 pass.

Full `app/tests/` baseline (pre-change main): 309 passed, 6 failed
(test_engine.py mock-wiring drift, unrelated to A4B.4), 7 skipped,
1 xfailed.

After this change: 309 passed, 6 failed (same as main), 7 skipped,
1 xfailed. **No regression.**

## What changes in `config.yaml`

3 single-line per-engine adds + explanatory comment on the first one:

```diff
   - name: "balanced"
     min_edge: 0.03
     ...
+    # A4B.4 (2026-05-23) phased rollout — keep the legacy edge gate while
+    # the calibration curves accumulate samples + the EV math gets retuned.
+    # ...
+    use_true_ev_formula: false

   - name: "inverted_highconf"
     ...
+    use_true_ev_formula: false

   - name: "loose_relevance_yes"
     ...
+    use_true_ev_formula: false
```

`scout`, `super_scout` blocks unchanged. `trading:` block unchanged
(global default stays `use_true_ev_formula: true` from code defaults).
