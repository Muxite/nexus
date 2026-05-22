# Trader entry-path bugs — Phase 0 A0.3 (2026-05-22)

Scope: `seulgi/app/trader.py` `evaluate()` + `_evaluate_market()`. Sampling
done via `sqlite3` against `data/seulgi.db` (6,611,748 skip rows, 498
executed trades). Cross-ref A0.1 (empirical), A0.2 (simulate.py), A0.4 (exit).

---

## 1. Edge formula — confidence is mis-routed; `agreement` is dropped

`trader.py:807-814`:
```python
confidence_scale = max(0.0, 2.0 * result.confidence - 1.0)
edge = self._cfg.edge_increment * confidence_scale
p_hat = mid_price + edge
```

`result.confidence` comes from `ai.py:_aggregate_ensemble`. The docstring at
`ai.py:663` advertises `confidence = mean_p_winner × agreement_fraction`, but
the implementation at `ai.py:719` is `confidence = p_alpha if p_alpha > 0 else
mean_p_winner` — agreement is silently dropped. A 2/3-split at p=0.62 reports
0.62 where the spec promises 0.41.

Math vs true EV: current formula is `edge = k·(2c−1)` (mid-relative, ±k band
centred on zero, ignores rail headroom). True EV for a YES purchase is
`EV = p − P_ask`. The two coincide only when c is a calibrated estimator of
true p; the production "edge" is therefore a confidence-strength heuristic,
not an EV. Phase 4B replaces this with `p_hat = c_calibrated(...)` then
`edge = p_hat − mid` (and symmetric on NO).

**Severity: HIGH.**

---

## 2. confidence vs p_alpha — semantic mismatch

`evaluate()` gates twice (`min_confidence`, `min_agreement`) but `min_confidence`
runs against `result.confidence`, which is **`p_alpha` when emitted, else
`mean_p_winner`**. Empirical sample of 498 executed rows:

* `mean(confidence_raw − p_alpha) = +0.58`.
* `confidence_raw > p_alpha + 0.05`: 422 rows. Reverse: 0 rows.
* Trade `id=6465561`: `confidence_raw=0.617, p_alpha=0.300, mid=0.935, edge=0.028`.
  AI literally signalled "30% likely +EV" and the trader sized at 0.617.

`p_alpha` is persisted but never gates execution. Need explicit `min_p_alpha`
(e.g. 0.5) and a decision on whether `confidence` = p_alpha (the cycle-7
contract) or mean_p_winner (the fallback). Right now it's ambiguous per-row.

**Severity: HIGH.**

---

## 3. Edge formula sanity per engine

200-row recompute of `0.12·max(0, 2c−1)` against `signed_edge`:
* balanced + inverted_highconf: 129 / 129 match.
* aggressive (retired): match at `edge_increment=0.20`.
* scout: match at 0.08 for 15 rows, 0.12 for 2 — scout's config_yaml says
  0.12; the 15 rows at 0.08 predate a config change. No active bug.

Edge increment is read per-engine from `_cfg.edge_increment` — correct.

---

## 4. Fee accounting at entry

`trader.py:1045-1060`: balance debit = `size_usdc` (gross). `positions.size_usdc`
= gross. `shares = floor((size − fee_est)/effective_price/0.01)*0.01`
(`simulate.py:132-145`). `entry_fee_usdc` is **written on the `trades` row**
(`db.py:1196-1200`); `positions` has no fee column. On close: `realized_pnl
= proceeds − size_usdc` where `proceeds = shares × exit_price − exit_fee`.
Math works out (entry fee was implicitly subtracted from share count) but
**`positions.size_usdc` is "gross dollars debited", not "shares × entry_price"**
— easy to mis-read. Spec block at top of trader.py now documents this.

**Severity: MEDIUM** — accounting correct; documentation/naming risk.

---

## 5. Sizing — per-engine method

Three branches (trader.py:881-921):
1. **Fractional Kelly** — simulate mode, not scout, method=="fractional_kelly".
2. **infinite_balance** — `size = max_position_usdc` unconditional.
3. **fixed/normal** — `min(max_position_usdc, balance)`; skip < `min_trade_usdc`.

Config (`config.yaml`):

| Engine        | max_position | min_trade  | method actually taken |
|---------------|-------------:|-----------:|------------------------|
| balanced      | $100         | $5         | fractional Kelly        |
| scout         | $1           | $5 global  | **fixed** (name prefix) |
| super_scout   | $0.05        | $5 global  | **Kelly** (prefix fails)|

**Bug 5a**: `is_scout = self._engine_name.startswith("scout")` matches `scout`
but **not `super_scout`**. super_scout falls into Kelly with
`max_position_usdc=0.05` and `SimulateSizingConfig.min_stake_usdc=1.0`
(`config.py:271`) — Kelly returns `kelly_below_min` on every candidate.
super_scout almost certainly trading zero rows; defeats its purpose as the
loosest research engine. A0.1 should confirm.

**Bug 5b**: `trading.simulate_min_trade_usdc: 1.0` is set in config.yaml line
536 but the trader reads `self._cfg.min_trade_usdc` (global $5). The simulate
override is **never wired**. Either delete the unused key or plumb it through.

**Severity: HIGH (5a)**, **MEDIUM (5b)**.

---

## 6. Pre-AI rules — two code paths

`process()` (legacy single-trader) applies rules in-trader (line 285-297);
EngineRunner splits into "universal" rules in the shared pass (engine.py:381)
+ "engine-scoped" rules per-engine (line 769). Tests use `process()`; prod
uses the runner. Rule with `engines_exclude` can therefore behave differently
in tests vs prod. No active bug observed; flagging for the rules-engine review.

**Severity: LOW.**

---

## 7. Gate summary — thresholds & dead branches

| Gate                | Source                              | Notes |
|---------------------|-------------------------------------|-------|
| NEUTRAL skip        | `result.direction`                  | hard |
| min_confidence      | `ai_cfg.min_confidence` (0.35; per-engine override) | scout/super_scout = 0 |
| min_agreement       | `ai_cfg.min_agreement` (0.66)       | `agree=0` bypasses (fail-open) |
| min_entry_yes_mid   | `_cfg.min_entry_yes_mid`            | 0 in all 3 engines → **dead** |
| direction_filter    | variant-only                        | unconfigured → **dead** |
| freshness_filter    | variant-only                        | unconfigured → **dead** |
| variant_mid_band    | `mid_yes_min/max`                   | only super_scout sets (0,1) |
| max_market_vol_30m  | `_cfg.max_market_vol_30m`           | 0 in all engines → **dead** |
| post_ai_rule        | rule engine                         | active |
| invert_high_conf    | `_cfg.invert_high_conf`             | false in all current engines |
| no_token/no_price   | match.token_* / refetch             | active |
| hard_rail           | `entry_rail_skip_threshold` (0.02)  | super_scout opts out (0) |
| soft rail_skip      | `rail_skip_threshold` (0.05)        | super_scout opts out (0) |
| min_edge            | per-engine                          | scout/super_scout = −10 (admit all) |
| max_open_positions  | per-engine                          | active |
| duplicate_position  | DB check                            | active |
| max_unique_markets  | 15 global                           | super_scout inherits 15 (tight) |

Four dead gates (`min_entry_yes_mid`, `direction_filter`, `freshness_filter`,
`max_market_vol_30m`) still execute on every match — wasted code path.
Either expose in config or delete.

---

## 8. Skip-row contract (cycle-2) — NOT in force

Empirical: `trades.skip_reason IS NOT NULL` = 6,611,748 vs `IS NULL` = 498.
Every skip path in trader.py calls `_record → db.insert_trade` (line 1141) and
EngineRunner *also* writes to `engine_results` (engine.py:815). Dual-write.

Top trades skip reasons:
* `RULE_extreme_low_price_SKIP`  1.81M
* `RULE_mid_below_065_SKIP`      1.14M
* `not_relevant:NO`              0.31M
* `not_relevant:WEAK`            0.20M
* `RULE_extreme_high_price_SKIP` 0.12M

Either stop `_record`-ing skip paths (let engine_results carry them) or
officially document the dual-write and drop one consumer. Currently `trades`
is 13,000× the executed-row count.

**Severity: MEDIUM-HIGH** — disk/IO bloat; the WAL pressure visible in
recent commits is consistent with this.

---

## 9. Mid drift between AI and trade execution

`trader.py:767-779` reads `match.mid_yes/no` populated by
`EngineRunner._prefetch_prices` (engine.py:319), *before* the stage-2
relevance LLM and stage-3 prediction LLM. Total elapsed before execution:
~200ms (S2) + 1–3s (S3) + per-engine eval. Drift on hot headlines:
empirically 200–500bps (see `decision_inputs` corpus, A0.1 will quantify).

**Bug**: trader never refetches mid before `insert_trade`. The trade sizes
on the AI-time mid and stamps `entry_price = mid_price` (paper) or
`effective_entry_price` post-slippage (simulate). On a fast-moving market the
true edge has already been consumed; the trader is buying after the news has
been priced in. Slippage model walks against you but cannot compensate for
drift.

Recommendation: single CLOB refetch after all guards pass; skip if drift ≥
`max_mid_drift_pct` (suggest 0.02). Phase 4B candidate. Triangulate with
A0.2's slippage curve.

**Severity: HIGH** — plausible systemic alpha leak.

---

## 10. Capital-pause check vs infinite_balance

`engine.py:_ai_should_pause` (line 199): `if e.trader.balance >=
cfg.min_trade_usdc`. Under `infinite_balance=true`, `balance` is *cumulative
realized PnL* — can go negative. A losing infinite engine reports "no
capital" and globally pauses the LLM. `infinite_balance` is currently
`false`; bug is latent.

**Severity: LOW-LATENT.**

---

## 11. Pricer rail check (PRICE_MIN/MAX)

`simulate.py:27-37` defines `PRICE_MIN=0.01, PRICE_MAX=0.99`. trader.py:
hard_rail + soft rail filter pre-slippage; then `is_at_rail(effective_entry_price)`
post-slippage. But `clamp_price` silently clamps in `apply_slippage` and
`compute_fee` — a true mid of 0.005 becomes 0.01 going in, and the slippage
walk is computed on the clamped value. Under super_scout's
`entry_rail_skip_threshold=0` opt-out, micro-rail entries reach this
silent-clamp regime. Active for super_scout only.

**Severity: MEDIUM** — super_scout-only.

---

## Top entry-path bugs

1. **§1/§2** Edge formula uses `result.confidence` (sometimes p_alpha,
   sometimes mean_p_winner); `agreement` dropped; `p_alpha` persisted but
   never gated. Mean conf-vs-p_alpha gap = +0.58 across 498 executed rows.
2. **§9** Mid drift between AI prefetch and execute — no refetch before
   `insert_trade`; likely systemic alpha leak on hot headlines.
3. **§5a** `super_scout` falls into Kelly via name-prefix bug
   (`startswith("scout")` matches `scout`, not `super_scout`); Kelly's
   `min_stake_usdc=$1` rejects every $0.05 candidate.
4. **§8** Skip rows dual-written to `trades` and `engine_results` (6.6M:498);
   cycle-2 contract not enforced.

Cross-cues for sibling agents:
* §9 ↔ A0.2 (slippage curve assumes pre-slip mid is the AI-time mid).
* §5a ↔ A0.2 (Kelly `min_stake_usdc` vs `max_position_usdc` mismatch).
* §8 ↔ A0.1 (empirical 6.6M:498 ratio).

Files (absolute):
* `/home/muk/projects/jennie/seulgi/app/trader.py` (600–1108)
* `/home/muk/projects/jennie/seulgi/app/ai.py` (650–742)
* `/home/muk/projects/jennie/seulgi/app/engine.py` (199–250, 769–805)
* `/home/muk/projects/jennie/seulgi/app/db.py` (991–1205, 1461–1525)
* `/home/muk/projects/jennie/seulgi/app/simulate.py` (27–37, 65–189)
* `/home/muk/projects/jennie/seulgi/app/config.py` (260–271)
* `/home/muk/projects/jennie/seulgi/config.yaml` (488–660)
