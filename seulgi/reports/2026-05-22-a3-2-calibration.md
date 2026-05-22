# A3.2 — Calibration prototype + backtest comparison

**Date**: 2026-05-22
**Agent**: Phase 3 A3.2
**Scope**: Offline-only prototype. Does fitting Platt scaling / isotonic
regression on closed trades change the variant ranking? Verdict feeds
the go/no-go on Phase 4B.1 (`seulgi/app/calibration.py`).

## TL;DR

- Built `scripts/analysis/calibration_prototype.py` (stdlib + numpy
  only; no scipy/sklearn).
- Fit per-direction Platt + isotonic on 501 closed trades (YES=229,
  NO=272) from `decided_at >= 2026-05-18`.
- **Calibration massively improves Brier score** (raw 0.35 → calibrated
  0.17–0.22) and **flips IC from −0.19 to +0.12 / +0.16** (raw
  confidence is *anti-informative* on realized PnL; calibration
  restores positive signal). This is a strong win for Phase 4B.1.
- However, **applying the calibrated probability into the legacy edge
  formula (`edge = increment × (2·c − 1)`) collapses every trade
  through the `min_edge` gate** — because calibrated YES≈0.33 produces
  a *negative* raw edge, clamped to zero. **Rank order is undefined
  in the calibrated runs (all engines at zero trades / zero PnL).**
- **Go for 4B.1**, but it must ship *together* with 4B.2 (true EV
  formula) and a recalibrated `min_edge` threshold, not in isolation.
  Calibration alone breaks the existing trader.

## Deliverables

| Path | Description |
|------|-------------|
| `seulgi/scripts/analysis/calibration_prototype.py` | Prototype calibration fitter (Platt + isotonic) |
| `/tmp/a3-2/calibration.json` | Fitted curve JSON consumed by backtest |
| `/tmp/a3-2-nocal/runs/20260522T234635Z_c5b900/` | Baseline backtest (no calibration), exact user window |
| `/tmp/a3-2-platt/runs/20260522T234641Z_24eacc/` | Platt-calibrated backtest, exact user window |
| `/tmp/a3-2-isotonic/runs/20260522T234646Z_4f920b/` | Isotonic-calibrated backtest, exact user window |
| `/tmp/a3-2-nocal-eff/runs/20260522T234658Z_67d218/` | Baseline, *effective* window 2026-05-21→2026-05-23 (where market snaps exist) |
| `/tmp/a3-2-platt-eff/runs/20260522T234659Z_68f4c9/` | Platt-calibrated, effective window |
| `/tmp/a3-2-isotonic-eff/runs/20260522T234701Z_abdcc1/` | Isotonic-calibrated, effective window |

## Data window

User-requested window: `--since 2026-05-18 --until 2026-05-22`.

`market_price_snaps` is only populated from **2026-05-21** onward
(`COUNT(*) = 191,916` total, all on 5-21/5-22). The user-window
backtest therefore returns **zero trades for all three runs** —
every decision flunks `no_price_for_exit`.

To get meaningful trade counts I also ran an **effective window**
`--since 2026-05-21 --until 2026-05-23` (same data, just trimmed to
where snaps exist). That window yields **270 trades for nocal**, **0
trades for platt**, **0 trades for isotonic** (per
`manifest.n_trades_simulated`).

## Calibrator: prototype script

`scripts/analysis/calibration_prototype.py`

- **Platt scaling**: hand-rolled Newton-Raphson on the logistic NLL
  with Platt's Bayesian target smoothing (`t_+ = (n_+ + 1)/(n_+ + 2)`,
  `t_- = 1/(n_- + 2)`). Converges in 4–5 iterations on this dataset.
- **Isotonic regression**: Pool-Adjacent-Violators (PAV) over the
  sorted `(c_raw, hit)` sequence. Linear interpolation between knots.
- Per-direction split (YES/NO). Falls back to pooled fit if either
  direction has < 25 samples.
- Brier score before/after per direction.
- CLI: `python3 -m scripts.analysis.calibration_prototype --since 30d
  --out /tmp/a3-2/calibration.json`.
- Exposes `load_calibrator(path, method) -> callable` for use by
  `scripts/replay/backtest.py`.

## Fitted coefficients (per direction)

### YES (n=229, hit_rate=0.328)

```
Platt:    alpha = -1.7132   beta = +0.4311   (converged, 4 iters)
Isotonic: 6 knots
    x=0.333  y=0.000
    x=0.350  y=0.000
    x=0.367  y=0.250
    x=0.600  y=0.250
    x=0.625  y=0.335
    x=0.700  y=0.335
```

Note the **negative alpha** — for YES, higher raw confidence *predicts
fewer wins*. The LLM's directional confidence on YES is structurally
anti-informative in this sample.

### NO (n=272, hit_rate=0.224)

```
Platt:    alpha = +1.0816   beta = -1.9744   (converged, 5 iters)
Isotonic: 7 knots
    x=0.250  y=0.000
    x=0.267  y=0.000
    x=0.317  y=0.000
    x=0.417  y=0.138
    x=0.650  y=0.138
    x=0.700  y=0.265
    x=0.900  y=0.265
```

NO has the "expected" positive alpha — higher conf → higher hit — but
the *level* is still well below the raw confidence (c=0.7 → p≈0.23).

### Brier score deltas

| direction | raw    | platt  | isotonic | Δplatt    | Δisotonic |
|-----------|-------:|-------:|---------:|----------:|----------:|
| YES (229) | 0.3465 | 0.2197 | 0.2189   | **-0.127** | **-0.128** |
| NO  (272) | 0.3818 | 0.1739 | 0.1701   | **-0.208** | **-0.212** |

Both methods reduce Brier by ~30–55% — large effect. Isotonic is
fractionally better than Platt but the gap is within noise on this
sample size.

### Information Coefficient

Pearson correlation of `confidence` against the realized outcome
(across all 501 closed trades, pooled):

| measure | raw | platt | isotonic |
|---------|----:|------:|---------:|
| IC(conf, hit) (Pearson)        | **−0.017** | +0.123 | +0.165 |
| IC(conf, realized_pnl) | **−0.194** | +0.123 | +0.164 |

**Key finding**: raw confidence has *negative* IC against realized
PnL — confirming the "high-conf is anti-informative" pattern observed
in `phase-e9` reports. Calibration not only normalises levels but
**restores positive signal**. Isotonic's +0.164 IC clears the +0.05
"useful signal" threshold; Platt's +0.12 is in the same ballpark.

## Backtest comparison

### Exact user window (2026-05-18 → 2026-05-22)

All three runs produce **0 trades** because `market_price_snaps` is
empty before 2026-05-21. Trade simulation cannot start without exit
prices. Top skip reasons (nocal):

| reason | n |
|--------|--:|
| no_price_for_exit | 20,387 |
| no_price | 8,288 |
| edge_too_small | 6,376 |
| low_confidence | 1,312 |

No rank ordering can be drawn from these runs.

### Effective window (2026-05-21 → 2026-05-23)

Same code paths, restricted to where snap data exists:

| run | n_trades | sum_pnl (longest horizon w/ coverage) | top skip reason |
|-----|---------:|--------------------------------------:|-----------------|
| nocal    | 270 | distributed across 18 engines | size_too_small |
| platt    | **0** | 0 | size_too_small / low_confidence |
| isotonic | **0** | 0 | size_too_small / low_confidence |

**Rank-stability verdict on the simulator**: **collapsed**. With
calibration, no engine produces any trades. The "rank" is degenerate
— every engine ties at zero.

#### Why the collapse

When calibration is applied to `result.confidence` before
`_evaluate_pure` runs, the calibrated YES≈0.33 / NO≈0.23 values:

1. Flunk the `min_confidence ≥ 0.40` gate on most engines (only
   `scout/all_signals` and `super_scout/all_signals` have
   `min_confidence=0.0`).
2. Even when they pass `min_confidence` (scout engines), the legacy
   `edge = edge_increment × (2c − 1)` formula goes negative
   (clamped to 0) — failing `min_edge`.
3. The few that pass both then fail `size_too_small` because Kelly
   sizing on `p_true=0.23` vs `mid=0.5` is also negative.

This is consistent and reproducible across both Platt and isotonic.

### External rank-stability check (realized PnL, no simulator)

Since the simulator window is too small to draw rank conclusions, I
also re-decided each of the **501 actually-executed historical
trades** under the calibrated edge formula `edge = increment × (2·c_cal
− 1) ≥ min_edge` (per-engine `edge_increment` + `min_edge` honoured):

| scenario | kept (of 501) | sum realized_pnl | top engine |
|----------|--------------:|-----------------:|-----------|
| nocal (raw c) | 457 | -4.50 USDC | balanced/conf40_tp030_sl050 (+0.38) |
| platt    | **16** | -0.90 USDC | scout/all_signals only |
| isotonic | **16** | -0.90 USDC | scout/all_signals only |

The only engine that keeps any trade under calibration is
`scout/all_signals` (because its `min_confidence=0` AND its `min_edge`
is the lowest in the config). Top-10 overlap between nocal and either
calibrated scenario is **∅ (empty set)**. Rank order is **not stable**.

## Discussion

This is *not* a bug in the prototype — it is the intended behaviour
of "calibration in isolation" applied to a config that was tuned
against *uncalibrated* confidence. The legacy edge formula treats
`c=0.7` as "70% probability of being right", which it isn't (the LLM
hits ~33% at c=0.7 on YES, ~23% on NO). Calibration tells the truth
about the underlying probability; the rest of the trader hasn't yet
been adapted.

In other words: the existing `min_confidence` + legacy `edge` formula
work *only because* the LLM is over-confident in a fairly stable way.
Calibration unmasks that, but the EV/sizing path needs to be replaced
in lockstep (Phase 4B.2 + 4B.3).

## Go / no-go: Phase 4B.1 production calibration

**Go.** The offline prototype demonstrates:

1. Calibration is **statistically very strong**: Brier drops 35–55%
   and IC flips from negative to positive. This is exactly the
   diagnostic Phase 4B.1 was designed to deliver.
2. Both Platt and isotonic converge cleanly on 500-trade samples.
   Isotonic edges out Platt on Brier and IC; recommend
   shipping **both** and picking nightly via cross-validated Brier.
3. The per-direction fit is **necessary** — YES and NO have opposite
   alpha signs.

**But: 4B.1 cannot ship in isolation.** Production must ship with:

- 4B.2 EV formula (`EV_yes = p · (1 − P_ask) − (1 − p) · P_ask`)
  replacing `edge = increment × (2c − 1)`. The legacy formula is
  incompatible with calibrated p.
- 4B.3 fractional-Kelly cap so calibrated p (which is often < mid) +
  small magnitudes don't produce zero-size positions.
- Re-tuned `min_edge` / `min_confidence` thresholds — old values are
  calibrated implicitly against raw conf and will dump all trades.

A staged rollout where 4B.1 lands first behind a feature flag (so we
can shadow-compute `p_true` and audit it in `trades` rows without
touching the gate) is the safest path.

## Caveats

- Sample size 501 is **small** for production calibration. Phase 4B.1
  must refresh nightly and ideally pool ≥ 1k trades before going live.
- The fits are stable across both methods, but YES has only 5 samples
  with `c < 0.4` — the low-confidence tail of the YES curve is
  extrapolation.
- The "rank stability collapsed" finding is robust across both the
  simulator and the external recompute. There is no scenario in this
  dataset where calibration *preserves* the engine ranking.
- Tests pass: `pytest app/tests/test_backtest.py` → 7 passed.

## Reproducibility

```bash
# 1. Fit the calibration curve
python3 -m scripts.analysis.calibration_prototype \
    --since 2026-05-18 \
    --out /tmp/a3-2/calibration.json

# 2. Backtests (user-requested window)
python3 -m scripts.replay.backtest \
    --since 2026-05-18 --until 2026-05-22 \
    --out-dir /tmp/a3-2-nocal
python3 -m scripts.replay.backtest \
    --since 2026-05-18 --until 2026-05-22 \
    --calibration platt \
    --calibration-curve /tmp/a3-2/calibration.json \
    --out-dir /tmp/a3-2-platt
python3 -m scripts.replay.backtest \
    --since 2026-05-18 --until 2026-05-22 \
    --calibration isotonic \
    --calibration-curve /tmp/a3-2/calibration.json \
    --out-dir /tmp/a3-2-isotonic
```
