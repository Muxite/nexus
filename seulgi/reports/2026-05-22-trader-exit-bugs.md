# Trader exit-path bug list — 2026-05-22 (Agent A0.4)

Scope: `seulgi/app/trader.py::monitor_positions` (and the COLD_CULL pre-pass).
Companion: A0.3 wrote the module-level spec; A0.1 audits realized PnL in parallel.
DB sampled: `/home/muk/projects/jennie/seulgi/data/seulgi.db` (live, offline).

## TL;DR (highest impact first)

1. **`close_reason='TP'` and `'TRAIL'` routinely book NET LOSSES.** 53 TP rows and 58 TRAIL rows have `realized_pnl < 0`. Mean TP-loss = −$0.028 (NO TPs avg net = −$0.008). Cause: thresholds compare *gross* MTM against `tp`/`ts`, ignoring entry_fee + exit_fee + slippage. A 2 ¢ TP on a 0.695-mid trade nets ≈ +$0.04 gross which is less than the round-trip ≈ $0.05 in fees + gas.
2. **No `exit_price` / `exit_mid` column on `trades`.** `close_trade()` writes only `realized_pnl`, `closed_at`, `close_reason`, `exit_fee_usdc`. Once `close_position` deletes the `positions` row, the actual exit price is **gone forever** — A0.1's audit / postmortem reconciler cannot recompute PnL from the DB alone.
3. **TIMEOUT and WIN/LOSS resolution branches have never fired in production.** 0 rows with `close_reason ∈ {TIMEOUT, WIN, LOSS, RESOLVE_WIN, RESOLVE_LOSS, COLD_CULL}` (all-time). `position_timeout_hours=24`, but admin closes (`cycle_reset`, `engine_retired`) intercept after ≤ 10 h. The timeout/rail/cull paths are unverified live code.
4. **60 s monitor cadence systematically blows past SL.** Sampled NO/SL trades take losses 3–6× the `stop_loss` knob (e.g. id=998514: aggressive `sl=0.015`, observed pnl −$0.21 on $1.07 stake). Tight engines bleed because the next monitor tick sees a price already far below the SL line.

---

## 1. Close-reason routing — partial coverage, taxonomy collision

`close_position()` is just the DB primitive that deletes the `positions` row; routing happens in `monitor_positions` (post-A0.3: lines ~1386–1414). Vocabulary emitted: `WIN / LOSS / TRAIL / TP / SL / TIMEOUT / TIMEOUT_BLEED / COLD_CULL`, with simulate-mode rewrites `WIN → RESOLVE_WIN`, `LOSS → RESOLVE_LOSS`.

All-time distribution: engine_retired 197, TP 135, TRAIL 67, cycle_reset 58, SL 37, TIMEOUT_BLEED 3. **TIMEOUT, WIN, LOSS, RESOLVE_*, COLD_CULL: 0 rows.**

**Bug 1.1**: `engine_retired` / `cycle_reset` are written **outside** `monitor_positions` (cycle-reset script) and aren't in any enum — free-text strings can collide with TP/SL.

**Bug 1.2**: Task-spec vocabulary `{tp, sl, timeout, manual, expired, rail_zero_liquidity}` does not match the actual uppercase set. No `manual`, no `expired`, no close-time `rail_zero_liquidity` (it's an entry-skip reason only). If exit slippage clamps at PRICE_MAX the code just returns proceeds at the clamped price — no distinct reason emitted.

**Bug 1.3**: The if/elif at lines ~1386–1414 enforces a hidden priority **WIN/LOSS > TRAIL > TP > SL > TIMEOUT > TIMEOUT_BLEED**. Correct, but undocumented.

## 2. TP / SL math — *gross* MTM in probability points, fees absorb the edge

```python
mtm = current_price - entry_price
... if min_hold_met and tp > 0 and mtm >= tp:  close_reason = "TP"
... if min_hold_met and sl > 0 and mtm <= -sl: close_reason = "SL"
```

These are *relative-price-delta* in **probability points**, not percent-of-entry. `take_profit: 0.020` ≡ "2 ¢ above entry mid". The `_pct` knob `stop_loss_pct` adds a price-relative SL **floor**: `sl = max(stop_loss, stop_loss_pct * entry_price)`.

**Bug 2.1 (highest impact)**: `mtm >= tp` ignores fees. $1.4 stake at 0.695 mid: entry fee ≈ $0.026, exit fee ≈ $0.025 → round-trip ≈ $0.051. Shares ≈ 1.97. Gross at TP=0.020: $0.039 — **less than fee cost**. 53 TP rows closed net-negative (mean −$0.028, worst −$0.055, id=545658). Same for TRAIL (`ts=0.010` < fee cost). Fix (Phase 1): gate on net `(mtm × shares) − est_exit_fee ≥ tp_target`.

**Bug 2.2**: `mtm` sign correctness on NO trades depends on `positions.token_id` being the *directional* token (NO side for NO). Verified in code — `token_yes`/`token_no` selected at entry per direction. But this invariant is unwritten; a future patch substituting `mid_yes` would silently sign-flip NO MTM.

**Bug 2.3**: TRAIL gate (`mtm >= 0`) only blocks *gross underwater*. 58/67 TRAIL rows still net-negative because exit slippage + fees push past zero after the trigger fires. E10 comment claims this was fixed; empirically only gross case is.

**Bug 2.4**: SL trigger compares `current_price` (mid) to threshold; actual exit price is `mid − walk`. Realized loss = `sl + slip.walk` per share + exit_fee. Undocumented.

## 3. SL realized-loss vastly exceeds the `stop_loss` knob

Sample of 20 NO/SL closes: aggressive (`sl=0.015`) routinely books 5–7× the knob (id 998514: pnl −$0.215 vs predicted −$0.036). Causes: 60 s cadence + exit slippage + round-trip fees.

**Bug 3.1**: SL knob is a *trigger*, not a *cap*. Spec should distinguish. Mitigations (Phase 1, not this report): shorter cadence on high-vol markets (already have `mkt_vol_30m` on trades), `sl + safety_margin` closes earlier under known volatility.

## 4. Timeout / max_hold — never reached

Code: `timeout_secs = self._cfg.position_timeout_hours * 3600`; `if age_secs >= timeout_secs: close_reason = "TIMEOUT"`. Default 24 h. Spec name is `position_timeout_hours` (not `max_hold_minutes` as task assumed).

**Bug 4.1**: 0 TIMEOUT rows ever. Longest-held trade = 9.92 h, all admin-closed. The 24 h timeout path is dead code in production.

**Bug 4.2**: `TIMEOUT_BLEED` (>1 h underwater & `mtm <= -0.5 * sl`) fired 3× — fee-blind same as Bug 2.1.

**Bug 4.3**: `Config.position_timeout_hours: float = 24.0` is the default; per-engine override is `Optional[float]`. The merge logic looks safe but no unit test guarantees no `None` reaches `monitor_positions`.

## 5. Realized PnL formula — auditable in simulate, lossy in paper

Simulate: `proceeds = shares * exit_price - exit_fee`; `realized_pnl = proceeds - size_usdc` (where `size_usdc` already includes entry_fee + gas per A0.3 entry-side note). Nets out correctly.

Paper: `gross_pnl = (current_price - entry) * shares; realized_pnl = gross - size_usdc * spread_cost_bps/10000`. No slippage applied, single flat-bps proxy for round-trip spread, no real fees.

**Bug 5.1 (major postmortem hole)**: `trades` persists `realized_pnl`, `closed_at`, `close_reason`, `exit_fee_usdc` but **no `exit_price` and no `entry_price`** (entry_price lives only on `positions`, which is deleted on close). A0.1 cannot reconstruct PnL from `trades` alone for closed rows.

**Bug 5.2**: Spot-checked 20 NO SL trades; magnitudes consistent with `shares × (exit_mid − entry) − exit_fee − entry_fee` (within slippage uncertainty). No sign-flip bug on NO.

## 6. Exit fee accounting

`exit_fee` computed at close time with slipped exit price + category fee + per-leg gas. Persisted to `trades.exit_fee_usdc` (simulate only).

**Bug 6.1**: `RESOLVE_WIN`/`RESOLVE_LOSS` branches set `exit_fee = 0` AND skip the gas_per_leg add. Inconsistent with entry where gas is unconditional. Settlement still triggers a Polygon tx; ≤ $0.01/trade.

**Bug 6.2**: Both COLD_CULL (line 1209) and early-exit (line 1357) pass `market_type=None` to `apply_slippage` / `compute_fee`, bypassing per-category disables and default fee rates. Comment "no need to refetch" is misleading — we still use category-aware rates at entry; at exit we silently fall back to default rate.

## 7. Rail-snap edge case

`_WIN_PRICE=0.95` / `_LOSS_PRICE=0.05` are hard-coded module constants.

**Bug 7.1**: `super_scout` has `entry_rail_skip_threshold=0.0` so it admits entries at 0.96–0.98. First monitor tick triggers `WIN` (current ≥ 0.95) and settles at $1/share — but if price drifts to 0.94 in the ≤ 60 s window, the WIN-rail closes silently and falls through to TP/SL.

**Bug 7.2**: PRICE_MAX (0.99) clamp inside `apply_slippage` swallows the "we hit the wall" case at exit — the trade closes at $0.99 implied not $1.00, plus exit_fee, booking a tiny loss instead of a clean WIN. Cannot quantify (0 WIN closes ever).

**Bug 7.3**: `_WIN_PRICE` / `_LOSS_PRICE` aren't per-engine even though entry-rail knobs are.

## 8. Trailing stop — HWM gating + net-loss bleed

`ts = self._cfg.trailing_stop`. Trigger: `ts > 0 and hwm_mtm >= ts and (hwm_mtm - mtm) >= ts and mtm >= 0`.

**Bug 8.1**: HWM lives in `positions.high_water_price`, advanced via SQL `MAX(COALESCE(hwm, ?), ?)`. Monotonic. Local copy at line 1284 uses `max(current, prev_hwm or current)` — fine under single-task monitor loop. No race.

**Bug 8.2**: The E10 `mtm >= 0` gate prevents *gross-underwater* trails but not *net-of-fee* trails. 58/67 TRAIL rows closed net-negative. Fix: gate on `(mtm × shares) − est_exit_fee ≥ 0`.

**Bug 8.3**: Trail can fire seconds after entry on a fast-bleed price (id=998504, age 84 s, pnl −$0.154 on $1.49 stake). Confirms `min_hold_seconds` is not honored on the *trail* path for the retired aggressive engine (had 60 s min_hold; trade fired at 84 s with a deep loss). Worth a regression test against the surviving engines.

## 9. Unrealized PnL (MTM) — dashboard reads stale value

Tick updates `positions.last_price` and `positions.unrealized_pnl = (current - entry) * shares` — **gross** (no entry_fee).

**Bug 9.1**: Currently-open id=6465561 has `last_price = entry = 0.935`, `unrealized_pnl = 0.0` 8 h after open. Either monitor hasn't run or `_fetch_mid_price` returns None → `continue` at line 1254 silently skips the update. Dashboard "Unrealized PnL: $0.00" masks an API outage.

**Bug 9.2**: `unrealized_pnl` doesn't subtract paid entry_fee or estimated exit_fee. On a $1 stake at 0.5 mid that's ~4 % invisible drag until close.

## 10. Position-monitor cadence (`main.py:89`)

`interval_secs=60.0`, first tick 60 s after start. `runner.monitor_all_positions` uses `asyncio.gather(..., return_exceptions=True)`.

**Bug 10.1**: `return_exceptions=True` swallows per-engine failures — results are returned and immediately discarded; the outer loop catch never sees them.

**Bug 10.2**: 60 s is too coarse for tight SL/TP (Section 3). 10–15 s would catch SL closer to threshold; cost is ~6× CLOB calls, well below rate limit.

**Bug 10.3**: All engines monitor in lockstep behind one `write_lock` serializing `close_position` + `close_trade`. Under load the tick window stretches and SL drift worsens.

---

## Recommended spec additions (for A0.3 / Phase 1)

Append to the module-level Invariants:

1. **token_id discipline**: `positions.token_id` is the *directional* token; all exit math relies on this.
2. **Close-reason vocabulary**: `monitor_positions` writes one of `{WIN, LOSS, RESOLVE_WIN, RESOLVE_LOSS, TP, SL, TRAIL, TIMEOUT, TIMEOUT_BLEED, COLD_CULL}`. Admin paths (`cycle_reset`, `engine_retired`) write outside this function and are not part of the enum.
3. **Close-reason priority**: WIN/LOSS > TRAIL > TP > SL > TIMEOUT > TIMEOUT_BLEED. WIN/LOSS bypass `min_hold_met`.
4. **MTM is gross probability-point delta**, in the directional token's mid. TP/SL/TRAIL gates do **not** net fees or exit slippage — a `TP` close can book a net loss (Bug 2.1 / 8.2).
5. **HWM is monotonic-up** via SQL `MAX(COALESCE(hwm,?),?)`; never reset.
6. **size_usdc** at open is the gross dollar cost including entry_fee + gas, so `realized_pnl = proceeds − size_usdc` nets all in/out fees in simulate.
7. **exit_price is not persisted on trades** (only `exit_fee_usdc`). Once `positions` is deleted, PnL is not auditable from `trades` alone.
8. **Settlement WIN/LOSS** skip both exit_fee and gas (Bug 6.1); all other closes pay both.
9. **Monitor cadence**: 60 s (`main._position_monitor_loop.interval_secs`). Realized loss can be 3–7× the `stop_loss` knob (Section 3).
10. **Silent skip on price None**: `monitor_positions` continues silently when `_fetch_mid_price` returns None — masks API outages (Bug 9.1).

---

## Followups / not-this-report

- A0.1: Bug 5.1 means no `exit_price` exists to recompute PnL for closed trades.
- A0.2: Bug 2.4 / 3.1 amplified by slippage `walk` on tight-SL engines.
- Phase 1 fix candidates: (a) add `exit_price` + `entry_price` cols to `trades`; (b) make TP/SL/TRAIL fee-aware; (c) shorten cadence to 10–15 s; (d) widen enum (`MANUAL`, `RAIL_DEAD_END`); (e) metric/log on price-None and per-engine monitor exceptions.
