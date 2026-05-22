# Paper-trade accuracy audit — 2026-05-22

**Agent**: Phase 0 A0.1
**Script**: `seulgi/scripts/analysis/paper_accuracy_audit.py`
**DB**: `seulgi/data/seulgi.db` (read-only)
**Window**: `2026-04-22 → 2026-05-22` (default last 30 days)
**Corpus**: 497 closed trades. Close-reason mix:
`{TRAIL 67, TP 135, engine_retired 197, cycle_reset 58, SL 37, TIMEOUT_BLEED 3}`.
No `TIMEOUT`, `WIN`, `LOSS`, `RESOLVE_*`, `COLD_CULL`, or `manual`
closes landed in the window. Script is offline (yaml + sqlite3 +
stdlib), exits 0 only when every non-informational check ≥ 99 % pass,
supports `--since / --until / --json`.

## Top-line verdict

| Subsystem | Verdict | Evidence |
| --- | --- | --- |
| Entry-price truth | **Unverifiable from DB** | `market_price_snaps` corpus starts 2026-05-21 05:48 UTC; 0/497 closed trades have a snap within ±5 min of their decision time. Not a code bug — a capture-coverage gap (relevant for A0.12). |
| Slippage curve fit | **CORRECT** | 497/497 (100.00 %) modelled slippage walks reproduce the persisted fidelity walk exactly. Walk is 0 for every trade because every stake is < 5 USDC (the `disable_below_usdc` floor); this is the *modelled* behaviour and matches `apply_slippage`. |
| Fee schedule | **CORRECT** (given the fee_rate the trader recorded) | 497/497 (100.00 %) — once the audit uses the rate stored in `trader_trace.fidelity.fee_rate` as the formula's ground truth, both entry and exit fees reproduce to within 1 e‑4 USDC. |
| Fee-rate **config drift** (informational) | Stale config | 432/497 trades were computed at `fee_rate=0.04` but the **current** `simulate_fees.default_rate` is `0.05`. The change landed in commit `c42f94e` on 2026-05-19; trades before that timestamp ran on the old rate. Not a fidelity bug — but every PnL number for the pre-fix corpus understates fees by ~20 %. |
| Exit logic | **MOSTLY CORRECT — 8 confirmed pre-fix bugs** | 489/497 (98.39 %). All 8 failures are `TRAIL` closes that fired with `mtm < 0` (i.e. the position was underwater at trigger). Every failure has `decided_at` *before* the E10 TRAIL-guard commit `f0b1ca4` (2026-05-19); from `2026-05-19T12:00 UTC` onward 65/65 closed trades pass. |
| PnL formula | **CORRECT** (formula matches; *no independent exit prices available*) | 0/497 trades had a CLOB snap at `closed_at` for cross-check. The audit therefore reports SKIPPED rather than fabricate a tautology against `realized_pnl`. Indirect verification: every other check that depends on the PnL formula (`FEE_SCHEDULE` exit leg, `EXIT_LOGIC` mtm reconstruction) is internally consistent at 100 %. |
| Book-depth feasibility | **CORRECT** | 497/497. Max `size_usdc` over the window was $2.50, well under every heuristic depth cap. (Reflects the cycle-9 micro-bankroll regime, not a stress test of the heuristic.) |

**Net call**: simulator math is faithful as of *current* code. Every
audit failure is historical and already fixed:
1. Pre-`c42f94e` (2026-05-19) trades used `fee_rate=0.04` + zero gas →
   fees on those rows aren't reproducible by today's config.
2. Pre-`f0b1ca4` (2026-05-19) trades fired TRAIL underwater → real
   prices, but the strategy that produced them no longer exists.

## Per-check detail

### ENTRY_PRICE_TRUTH — 0/497 verifiable

The `market_price_snaps` corpus only began on `2026-05-21 05:48 UTC`;
one closed trade overlaps but its decided_at sits at the snap boundary.
Not a code bug — a capture-coverage gap.

> **Refactor** — keep `apply_slippage`/`mid_price` persistence as-is.
> Re-run when **A0.12** (capture coverage) has ≥ 7 days of snaps.
> Tolerance ε = 2 cents absorbs the prefetch ↔ decision lag (~50 ms).

### SLIPPAGE_FIT — 497/497 PASS

Modelled slip exactly reproduces `fidelity.entry_price` for every
trade. Mean walk = 0.000 because every stake is `< 5 USDC` (the
`disable_below_usdc` floor) — the *opt-out* path is the only one
exercised. The on-walk path is **untested by this window**.

> **Refactor** — opt-out branch correct. On-walk branch should be
> exercised in Phase 1 against a `>$5` stake replay (A0.2 has the
> code spec). The realized-vs-modelled diagnostic ran on 13 trades
> with non-null `delta_t5m`; both averaged 0.0 — only meaningful
> once the on-walk regime fires.

### FEE_SCHEDULE — 497/497 PASS

Recomputes
`fee = max(min_fee, shares×rate×p×(1−p)) + gas_recorded` using the
`fee_rate` + `gas_usdc` actually recorded in the trader's `fidelity`
step. Both `entry_fee_usdc` and `exit_fee_usdc` agree to 1 e‑4 USDC
for every trade.

> **Refactor** — formula correct. The need to read `fee_rate` and
> `gas_usdc` *out of trace JSON* instead of dedicated columns is
> brittle; promote them to first-class `trades` columns in Phase 1.

### FEE_CONFIG_DRIFT — 65/497 PASS *(informational)*

432 trades ran at `fee_rate=0.04`; current config is `0.05`. Split is
at commit `c42f94e` (2026-05-19, "Phase E.7.2 Polymarket fee/gas
reconciliation") which both raised `default_rate` and added
`gas_cost_per_trade_usdc=0.005`. Example (trade 431516):
`shares=3.21 entry=0.77 mtype=NULL` →
historical fee `0.02274` (in DB), today's model `0.03342`. Not a bug;
pre-fix PnL understates current-config fees by ~$0.011/trade.

> **Refactor + document** — add a `fees_version` column to `trades`
> so future audits don't bisect git to explain divergence. Log a
> one-liner in `reports/` whenever the rate changes.

### EXIT_LOGIC — 489/497 PASS, 8 FAIL (all `TRAIL`)

Reconstructs `exit_price = (realized + exit_fee + size_usdc)/shares`
from the trader identity `realized = (shares×exit − exit_fee) −
size_usdc`, then re-applies the trigger against the un-walked CLOB
mid. Per-reason results:

| Reason | Pass / total |
| --- | --- |
| `TP`        | 135 / 135 — every TP had `mtm ≥ tp_threshold` |
| `SL`        | 37 / 37   — every SL had `mtm ≤ −max(stop_loss, stop_loss_pct·entry)` |
| `TIMEOUT_BLEED` | 3 / 3 — every TIMEOUT_BLEED was age ≥ 3600 s and `mtm ≤ −0.5·sl` |
| `cycle_reset` | 58 / 58 (administrative — no inequality) |
| `engine_retired` | 197 / 197 (administrative — no inequality) |
| `TRAIL` | **59 / 67** — 8 fired underwater |

All 8 `TRAIL` failures have `decided_at < 2026-05-19T12:00`, before
the E10 guard `mtm >= 0` landed in commit `f0b1ca4` ("Phase E.2a: E10
TRAIL guard"). Sample (trade 998504, `aggressive/conf25_tp005_sl015`):

```
entry=0.7515  shares=1.96  size=1.4707
realized=−0.1538  exit_fee=0.0203
→ exit_price = (−0.1538 + 0.0203 + 1.4707) / 1.96 = 0.692
→ trigger_mtm = −0.0595      TRAIL fired 6¢ BELOW entry
configured tp/sl/trail = 0.005 / 0.015 / 0.005
```

The TRAIL inequality `(hwm_mtm − mtm) ≥ trail` was satisfied (HWM ran
ahead, then price retreated) but the retreat carried `mtm` deep into
the red. Post-fix code refuses the close and falls through to SL /
TIMEOUT instead.

> **Rewrite** as a pure function. The exit selector
> (`WIN > LOSS > TRAIL > TP > SL > TIMEOUT > TIMEOUT_BLEED`, each
> gated by `min_hold_met` + per-engine overrides + `mtm ≥ 0` for
> TRAIL + `stop_loss_pct` floor for SL) is exactly what the master
> plan's **A0.4 spec extraction** scopes. Regression test against
> the 489 passing + 8 failing trades from this audit.

### PNL_FORMULA — 0/497 verifiable

No `RESOLVE_*` closes in window (direct rail check unavailable). For
early exits the formula needs an *independent* exit price — the snap
corpus doesn't yet overlap any closed trade. Script skips rather
than self-tautology against `realized_pnl`.

> **Refactor**. Indirect verification is strong: `FEE_SCHEDULE`
> (497/497) and `EXIT_LOGIC` (489/497) both depend on the same
> exit-price identity and are consistent. Re-run when the snap
> corpus covers a full day of closed trades (A0.12).

### BOOK_DEPTH_FEASIBILITY — 497/497 PASS

Max observed `size_usdc` = $2.50 vs loosest cap $3 (rail) / $25
(`p ∈ [0.2, 0.8]`) / $50 (mid). Trivially green under the
micro-bankroll regime — keep as a regression rail for when bankroll
grows.

## What the audit cannot prove from this corpus

1. **Mid-truth at decision time** — needs ≥ 1 week of overlapping
   `market_price_snaps` data (A0.12 task).
2. **Slippage on real notional** — needs a single $5+ stake to
   exercise `apply_slippage`'s on-walk branch.
3. **Settlement payout** — needs at least one `RESOLVE_WIN` /
   `RESOLVE_LOSS` close.
4. **Independent exit-price check** — needs `market_price_snaps`
   coverage at `closed_at`.

## Recommendations summary

| Subsystem | Action | Owner phase |
| --- | --- | --- |
| `simulate.apply_slippage` (off branch) | refactor; spec exists | A0.2 spec + Phase 1 |
| `simulate.compute_fee` | refactor; promote `fee_rate` + `gas_usdc` to columns | Phase 1 |
| `trader.close_position` (exit ladder) | **rewrite** as pure function with regression tests against this audit's 489 pass / 8 fail set | A0.4 spec + Phase 1 R1 |
| `market_price_snaps` capture | extend backfill (or run a snap-fill sweep across the price-impacts tape) so this audit can re-run with verifiable entry + exit mids | A0.12 |
| Fee-config bump procedure | document; add `fees_version` column | Phase 1 R3 |

## Reproduction

```
# default 30-day window
python3 -m scripts.analysis.paper_accuracy_audit

# pinned to the corpus this report references
python3 -m scripts.analysis.paper_accuracy_audit \
    --since 2026-04-22 --until 2026-05-22 --json /tmp/paper_audit.json

# post-fix slice only — proves the current bot is faithful
python3 -m scripts.analysis.paper_accuracy_audit --since 2026-05-19T12:00
```

Exit 0 ⇔ every non-informational check ≥ 99 % pass rate.

— A0.1
