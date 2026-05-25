# Plan — closer.py fee math

> Source: `irene/questions/unstructured answers.md` lines 1-22, batch
> `2026-05-25-unstructured-1`. Cross-refs: `research_answers.md` §A1–A4.

## Context

Cycle 7 (commit `11f8819`) wired the Polymarket per-leg parabolic taker fee
into `app/stages/closer.py`. The math lives in `_compute_round_trip_fees`
(`closer.py:181-213`) and `_polymarket_taker_fee` (`closer.py:168-178`),
with the category→rate table in `app/config.py:TradeSection.polymarket_fee_rates`
(`config.py:274-280`). The new batch of external research re-confirms the
formula and re-publishes the category→rate table — but it disagrees with
the earlier batch on one number (crypto: **0.07** vs the **0.072** we
currently ship), and it spotlights one structural bug we haven't fixed:
`closer.py` uses `entry_mid` / `exit_mid` as `p` in the fee formula
instead of the executed fill price (see A1 plan stub).

This plan is the focused close-out for **closer.py fee math**. Per-market
fee fetching via `getClobMarketInfo(conditionID)` is noted as a
cross-cutting hand-off to the `markets-gamma.md` plan.

## Findings

### Protocol facts (✅ act on these)

- **Formula** `fee = C × feeRate × p × (1 − p)` where `C` = shares, `p` =
  **taker's fill price, not displayed mid** (line 4). Our current code
  passes `entry_mid` / `exit_mid` for `p` (`closer.py:200-205`).
  Re-confirms `research_answers.md` A1.
- **Per-leg, per match, taker only** — entry and exit each pay if they
  cross; resting (maker) legs pay 0 (lines 4, 15). Already implemented
  symmetrically in `_compute_round_trip_fees`.
- **No fee at resolution** — winning shares pay exactly $1 (line 4).
  Confirms that the cycle-7 `market_resolved` close path must **not**
  charge `entry_fee + exit_fee` for the exit leg when the market actually
  resolved (it currently does — see "Proposed code changes" §3).
- **Maker fee = 0** across all categories (lines 10-14, 15). Matches our
  current model (we never add a maker fee).
- **Maker rebate is a daily pool, not a per-trade negative fee** (line 16).
  No code change for closer.py — rebate accounting belongs to the future
  real-money execution layer (already captured in `research_answers.md`
  A3 plan stub).
- **Share count, USDC settlement** (lines 18-19). For *buy* legs the
  protocol collects the fee in **shares**; for *sell* legs in **USDC**.
  Our PnL math reports USDC throughout — fine for paper mode, but a
  side-note for the live executor (see "Cross-cutting notes").

### Contradictions vs existing distilled answers

- **Crypto fee rate: 0.07 (this batch, line 10) vs 0.072 (A2, the
  number we ship in `config.py:275`).** Two possibilities:
  1. The earlier RAG run was wrong and the rate is 0.07.
  2. Polymarket re-published a rounded number and the *actual on-chain
     param* is still 0.072 (or vice versa).
  This is the only fee-rate cell that disagrees; the rest of the table
  matches exactly. **Resolution path:** treat the in-chain value as
  authoritative — hit `getClobMarketInfo(conditionID)` against one live
  crypto market and use the response to settle 0.07 vs 0.072. Until that
  happens, leave `0.072` (conservative — over-estimates fees by ~2.8%).
- **"Rates are per-market, not universally per-category"** (A2). The
  unstructured batch doesn't re-mention this caveat — it just publishes
  the category table as-is. The right interpretation is still A2's:
  the table is the **published schedule**, but the on-chain
  `fee_rate_bps` field can differ per market. Closer.py's lookup should
  prefer a stored per-market value when present and only fall back to
  the category table.

### Stale / partial (⚠️)

- **`p = entry_mid` instead of `p = fill_price`** in
  `_compute_round_trip_fees` (`closer.py:200-205`). In paper mode we
  don't have a real fill price — `entry_mid` is the best proxy we have.
  But once the real-money executor lands (cycle 9+), the closure write
  needs to take the executed `fill_price` from the trade row, not the
  decision-time mid. Adds <10 lines to `write_trade_closure` and a
  nullable `fill_price` column on `trades`. **Out of scope** for this
  plan — flagged for the executor cycle. For now, document the
  approximation in the docstring.

## Proposed code changes

All changes are in `app/stages/closer.py` and `app/config.py`. Each
change cites the raw line(s) driving it.

### 1. Document the `p = mid` approximation (lines 4, 18-19)

`closer.py:_compute_round_trip_fees` (lines 181-213) — extend the
docstring to make the paper-mode approximation explicit:

```text
PAPER MODE NOTE: Polymarket charges fees against the *taker fill price*,
not the mid. We don't have a fill price in paper mode, so we use mid as
the best available proxy. The error is bounded by half the spread per
leg and is symmetric (mid over-estimates fee on one side and
under-estimates on the other for the same fill). Once the real-money
executor lands, replace `entry_mid`/`exit_mid` with the executed
`fill_price` from the trade row.
```

No behavior change; just makes the assumption auditable.

### 2. Reconcile the crypto rate (line 10)

`config.py:274-280` — the comment block above the dict (`config.py:266`)
already says `0.072`. Add one inline note:

```python
"crypto": 0.072,  # 2026-05-25: unstructured RAG batch reports 0.07.
                  # Keep 0.072 (conservative) until getClobMarketInfo
                  # confirms — see plans/markets-gamma.md.
```

No numeric change yet. The authoritative resolution happens when
`markets-gamma.md` lands the per-market fee fetcher.

### 3. Wire `close_reason='market_resolved'` to skip the exit fee (line 4)

Raw input is explicit: **no trading fees at market resolution**. The
cycle-7 `MarketsStage` auto-close path currently routes
`market_resolved` closures through `write_trade_closure`, which
unconditionally charges `entry_fee + exit_fee`. The entry leg fee is
correct (we did pay it at decision time). The **exit leg fee is wrong**
on this code path — there is no taker fill, the shares just redeem at
$1/$0 from the oracle.

`closer.py:write_trade_closure` (lines 223-296) — pass `exit_fee = 0`
when `close_reason == 'market_resolved'`:

```python
entry_fee, exit_fee = _compute_round_trip_fees(...)
if close_reason == "market_resolved":
    # Polymarket charges no trading fee at resolution — winning shares
    # redeem at $1, losing shares at $0. The entry-leg taker fee still
    # stands (we paid it on entry); zero out the exit leg only.
    # Source: unstructured answers line 4.
    exit_fee = 0.0
net_pnl = gross_pnl - entry_fee - exit_fee
```

This shifts realized PnL for every prior `market_resolved` closure by
+`exit_fee_usdc`. Replay (`scripts/replay/accuracy.py`) will pick it up;
verify the cycle-7 audit query (`research_questions.md` i-A1) re-runs
clean after backfill.

### 4. Surface the gas knob in `config.yaml` (no raw-line driver, cleanup)

`gas_cost_usdc` is defined in `config.py:284` (default 0) but not
exposed in `config.yaml` (`trade:` block is just `balance_usdc: 1000.0`,
`config.yaml:188-189`). Add an explicit `gas_cost_usdc: 0.0` line so
the knob is discoverable. Pure config hygiene; no code change.

## Open questions

- **Q1.** Crypto fee rate: 0.07 or 0.072? Resolved by one `getClobMarketInfo`
  call against any active crypto-tagged market (e.g. a BTC price market).
  Owner: `markets-gamma.md`.
- **Q2.** Does `market_resolved` ever fire with a partial-fill or
  pre-resolution forced close that *does* require an exit fee? Read the
  MarketsStage auto-close path (`app/stages/markets.py`, cycle-7 wiring)
  to confirm `market_resolved` is only set when the market is genuinely
  finalized — never as a synonym for "we panicked and crossed the
  spread to flatten before resolution".
- **Q3.** Is there a "Default" category distinct from "Economics /
  Culture / Weather"? The table groups them all at 0.05 (line 11), but
  our dict (`config.py:276`) lists them as separate keys — fine, they
  all map to 0.05. No action needed.

## Verification

- **V1.** Re-run the cycle-7 audit query (`research_questions.md` i-A1)
  after the change-3 patch + replay. Expect `total_fees` to drop by the
  sum of `exit_fee_usdc` over rows where `close_reason='market_resolved'`,
  and `net_pnl` to rise by the same amount.
- **V2.** Unit test for change 3: extend `app/tests/stages/test_closer.py`
  with a `test_market_resolved_charges_no_exit_fee` that asserts the
  saved `exit_fee_usdc` is 0 for `close_reason='market_resolved'` and
  matches `_polymarket_taker_fee(...)` for any other `close_reason`.
- **V3.** Spot-check `_compute_round_trip_fees` against the protocol
  example in `unstructured answers.md` line 4: pick a $100 entry on a
  Politics market at mid=0.40, fee_rate=0.04, expected entry fee =
  `(100/0.40) × 0.04 × 0.40 × 0.60 = 2.40` USDC. Cross-check against
  the existing `test_pnl_yes_with_default_fee_rate` (line 411) numbers.

## Cross-cutting notes

- **Per-market `fee_rate` from `getClobMarketInfo(conditionID)`**
  (A2 plan stub; B3 parser policy) belongs to `markets-gamma.md`. That
  plan should add a `fee_rate_bps` column to `markets`, a fetcher in
  `app/stages/markets.py`, and a closer.py change that prefers
  `trade['fee_rate']` (joined from `markets`) over the category lookup
  when present. Once that lands, `_resolve_fee_rate` (`closer.py:150-165`)
  becomes pure fallback.
- **`p = fill_price` instead of `p = mid`** belongs to the real-money
  executor cycle (cycle 9+). Tracked in this plan's "Stale / partial"
  section so it's not lost.
- **Maker rebate accounting** (A3) is an execution-layer concern, not a
  closer concern — `write_trade_closure` never sees maker vs taker; the
  executor decides post-vs-cross. Out of scope here.
- **Buy-leg-fee-in-shares vs sell-leg-fee-in-USDC** (line 19) is an
  on-chain settlement detail. Paper mode reports USDC everywhere and
  the bookkeeping identity (`identity OK` in the dashboard) holds. The
  live executor will need to track the in-kind fee for wallet
  reconciliation — note for that cycle, no action here.
