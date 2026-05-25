# Plan — order types + spread term + wide-spread bug (C1-C4)

> Source: `irene/questions/unstructured answers.md` lines 195-228, batch
> `2026-05-25-unstructured-1`. Reconciles and extends the prior C-batch
> in `research_answers.md` §C1-C4. Cross-cuts `closer-fees.md`,
> `markets-gamma.md`, `latency-repricing.md`.

## Context

The most important PROTOCOL finding in this slice is **mechanical, not
statistical**: per Polymarket's own docs, the "displayed price" they
return as midpoint is the bid-ask mid **only when the spread is
$0.10 or narrower**. When the spread is wider than $0.10, the same
field falls back to the **last traded price** (line 201). The last
trade can be hours stale and arbitrarily far from the live mid.

Our hot-path mid comes from `app/clob.py:get_mid()` via
`GET /midpoint?token_id=<id>` (`clob.py:111-139`, returns
`{"mid":"0.545"}`). The docstring at the top of `clob.py:1-31` treats
that value as "the midpoint" with no caveat. Every consumer —
`stages/trade.py` (edge gate, `_resolve_mid` at line 723; edge math at
lines 450/452), `stages/closer.py` (exit-mid for PnL at line 41;
`current_mid` at lines 364-380), and `dashboard/pages/main.py`
(mark-to-market) — treats the returned scalar as a true mid. If
Polymarket's `/midpoint` endpoint honours the same UI fallback as their
docs describe, **every wide-spread market we touch is mis-priced at
exactly the moments fees and slippage matter most**. This plan leads
with the audit and the fix.

The second-tier PROTOCOL facts confirm and extend the prior C-batch
(`research_answers.md` §C3, §C2): order types are limit-only, with
GTC / GTD / FOK / FAK + post-only; "market buy" is sugar for an
aggressive limit; per-market `min_order_size` is the real floor.
Statistical numbers (spread tiers, 250ms taker delay, sub-50ms book
updates, 180-420ms residential RTT) stay seeds — our `i-C1` / `i-D1`
measurements rule. The 250ms taker-delay figure in particular almost
certainly pre-dates Polymarket's removal of the 500ms artificial
penalty (`latency-repricing.md` line 62) and is flagged below.

## Findings

### Protocol facts (act on these) — ranked by impact

- **CRITICAL — wide-spread mid fallback (line 201).** The displayed
  price is bid-ask mid **only if spread ≤ $0.10**; on wider spreads it
  becomes the last traded price. Today every `clob.get_mid()` consumer
  assumes mid; we have no second source to disprove it on a per-call
  basis. This is the single highest-value fix in this batch.
- **All orders are limit (line 215).** No native market-buy primitive;
  what we informally call "market buy" must be implemented as a
  marketable limit priced relative to the executable side.
- **Order types: GTC, GTD, FOK, FAK + post-only (line 217).** FAK =
  "fill what's available, cancel the rest" — semantically IOC. FOK is
  all-or-nothing. Post-only is rejected if it would cross.
- **Partial-fill behaviour (line 219).** GTC/GTD rest the unfilled
  remainder on the book; FOK either completes or cancels in full;
  FAK takes the slice and discards the rest. Implication: any
  execution path that submits GTC/GTD must own a residual-cleanup
  loop or it will accumulate stale exposure.
- **No platform-wide trading-size floor (line 208).** Per-market
  `min_order_size` (typically ~$1 or 5 shares) is the only hard
  minimum — fetch at arm-time from market metadata.
- **Bridge minimum ~$2 (line 212).** Funding-side, not order-side;
  noted here only so we don't confuse the two.

### Seeds only — measure, do NOT bake into config

- **Spread tier heuristic (line 203):** $0.01-0.02 tight,
  $0.05+ wide. Useful as plot-axis buckets, not as gate thresholds.
  Our `i-C1` (live `/book` snapshots) is the authority once collected;
  prior batch already adopted this in `research_answers.md` §C1.
- **Fill latency anecdata (lines 223-224):** 250ms taker-side delay;
  sub-50ms book updates; 180-420ms residential RTT. Direction-
  setting only; `i-D1` is the reference.

### Contradictions / stale — flag, don't auto-resolve

- **⚠️ "250ms taker delay" (line 223) vs penalty-removal.**
  `latency-repricing.md` line 62 records that Polymarket removed the
  500ms artificial taker penalty. A 250ms figure post-removal would be
  pure matching + network and is plausible only on bad paths; the
  cited datapoint almost certainly pre-dates the change. Treat as a
  direction, not a floor. Reconciliation lives in
  `latency-repricing.md`, not here.
- **Per-market min reconciliation.** Prior batch
  (`research_answers.md` §C2) says `min_order_size` is per-market with
  a ~$1 outside reference; this batch confirms ~$1 or 5 shares "per-
  market metadata" (line 208). No conflict — fold both into the §C2
  primary-info bullet and move on.

## Proposed changes — by impact (highest first)

### 1. Wide-spread mid-fallback audit & fix — CRITICAL

The bug surface is `clob.get_mid()` and every call site. The fix is to
stop trusting a single-scalar mid on any market whose spread we don't
know.

- **Audit.** Enumerate every consumer of `clob.get_mid()`:
  - `app/stages/trade.py:723` — `_resolve_mid()` for the edge gate.
  - `app/stages/closer.py:368` — `current_mid` for take-profit /
    stop-loss / stagnation; also feeds `exit_mid` for PnL.
  - `app/dashboard/pages/main.py` — mark-to-market on open positions.
  - `app/stages/markets.py:723` — closer's entry-mid fallback when
    CLOB returns `None`.
- **Add the live-book read.** Extend `clob.py` with
  `get_book(token_id) -> {best_bid, best_ask, bid_size, ask_size} |
  None` and (separately) `get_top_of_book(token_id) -> (bid, ask) |
  None`. The historical note in `clob.py:14-16` says `/book/<id>`
  returned 404 on 2026-05-24 — re-probe the current Polymarket
  endpoint surface (`/book`, `/orderbook`, `/prices`,
  `/midpoint?include_book=true`) and document what works. If no live
  book endpoint exists, escalate before writing code.
- **Compute a true mid + spread.** Replace the consumer-side
  `mid = await get_mid(...)` pattern with:
  ```python
  bid, ask = await clob.get_top_of_book(token_id)
  if bid is None or ask is None:
      mid = await clob.get_mid(token_id)  # legacy fallback
      spread = None
  else:
      mid = 0.5 * (bid + ask)
      spread = ask - bid
  ```
  Surface `spread` and the per-side prices to the caller; do not throw
  the structured book away.
- **Gate on spread width.** In `trade.py` edge gate, when `spread` is
  None or `spread > cfg.trade.wide_spread_threshold` (default `0.10`,
  matching the docs), either (a) reject with a new
  `wide_spread_no_book` gate reason, or (b) downgrade the edge with an
  explicit penalty. Default to rejection — it's safer and we have no
  empirical reason to accept yet.
- **Closer-side correction.** `closer.py` PnL math is mid-to-mid
  (`_compute_gross_pnl`, lines 135-147). If exit-mid is silently a
  stale last-trade on a wide-spread market, realised PnL is wrong by
  the spread distance. At minimum, persist `exit_spread` alongside
  `exit_mid_price` in `trade_closures`; ideally close at the
  executable side (best_ask if exiting a YES long, best_bid if
  exiting a NO long).
- **No edits this cycle to `markets-gamma.md` or `closer-fees.md`** —
  this plan only documents the audit; the cross-cutting work belongs
  to those plans.

### 2. Edge gate — explicit executable-side term

`trade.py:450-452` currently computes:

```python
edge = p_hat - mid - self._cfg.fee_buffer   # YES
edge = mid - p_hat - self._cfg.fee_buffer   # NO
```

`fee_buffer` (default `0.02`, `config.py:240`) absorbs both fee and
spread in a single constant. With the book in hand we can split them:

- **From:** `edge = p_hat − mid − fee_buffer`
- **To:** `edge_after_costs = p_hat − executable_price − fee −
  0.5·spread − depth_slippage`
  where `executable_price = best_ask` for YES-buys, `best_bid` for
  YES-sells (symmetric on NO).

`fee` keeps using the per-market rate from `closer.py:_compute_round_
trip_fee` (already correct per `research_answers.md` §A1). `0.5·spread`
is the half-spread cost of crossing once. `depth_slippage` starts at
`0` with a TODO to fill from `i-C2`'s sized-order simulation; a `$25`
order against a $1k top-of-book lifts the price by ~0, but the term is
load-bearing once we scale.

Keep `fee_buffer` as a backwards-compat constant for paper-mode
runs that have no book — when `spread is None`, fall back to the
single-constant path so paper-mode tests don't break. Document the
fallback as "paper-only; live execution must take the spread branch."

### 3. Execution rewrite — FAK-first with max-slippage cap

Today the codebase has no live executor; the cycle-7+ plan stub in
`research_answers.md` §C3 calls for replacing the implicit "IOC-at-mid"
assumption when one is built. This plan locks in the shape:

- **Default = FAK** (immediate fill of available depth, cancel the
  rest). Matches our "decide-and-fire" thesis: we never want a
  resting order accidentally surviving a re-pricing.
- **Limit price = executable_side ± max_slippage.** For a YES buy:
  `limit = best_ask × (1 + cfg.execution.max_slippage)`. Symmetric
  for sells / NO. `max_slippage` starts as an open question (see
  Open Questions below); seed at ~1¢ for liquid books, ~3¢ for thin,
  configured per market category once we have data.
- **FOK only for "all or nothing" semantics** — e.g. a position-size
  exit where partial unwind leaves a hedged stub. Default behaviour
  stays FAK; FOK is opt-in per call.
- **GTC / GTD only with a cleanup loop.** If we ever post a GTC/GTD
  (cycle-9+ maker-side, per `research_answers.md` §A3 maker-rebate
  follow-up), the executor must own a "cancel stale residuals" task.
  Until that loop exists, **GTC/GTD MUST NOT be used** — call this
  out explicitly in the executor docstring.
- **Post-only for maker path.** Reject-on-cross is the right
  behaviour for the rebate-capture path; surface the rejection to the
  caller so it can fall back to FAK if the spread closed.

### 4. Per-market `min_order_size` at arm-time

- `config.py:290` sets `min_position_usdc = 1.0` (default; not
  currently overridden in `config.yaml`). The cycle-7 handoff
  aspirationally talks about `$25`; this plan doesn't move the
  default, but flags the gap so the cycle-9 sizing work can resolve
  it intentionally rather than by accident.
- Live executor: read the per-market `min_order_size` from market
  metadata at arm-time (the field exists on the order book per
  `research_answers.md` §C2). Reject sub-floor orders with a new
  `below_market_floor` gate reason before submission.
- Note the pUSD migration risk (`research_answers.md` §C2) in a
  comment near the collateral-handling code — don't act on it yet.

### 5. Pre-trade quote-freshness check

- Before submitting an order, re-fetch top-of-book. If the cached
  bid/ask is older than `cfg.execution.quote_max_age_ms` (seed at
  `500`), abort with `stale_quote` rather than fire on stale prices.
- The 500ms seed is a placeholder, not a measurement — `i-D1` is the
  reference. Cross-link to `latency-repricing.md` for the latency-
  budget breakdown.

## Verification

- **Wide-spread audit:** produce a `scripts/inspect/midpoint_vs_book.py`
  that, for a sample of ~100 live markets, hits `/midpoint` and the
  live book endpoint in parallel and emits rows of
  `(market, mid_endpoint, book_mid, best_bid, best_ask, spread,
  last_trade, divergence)`. Any row with `spread > 0.10` and
  `|mid_endpoint - book_mid| > 1¢` confirms the bug. Save the sample
  in `irene/data/` alongside the resulting bucketed counts.
- **FAK execution (when built):** shadow-trade for a week on the
  cycle-3 paper environment using mocked fills derived from observed
  top-of-book; compare expected vs simulated fill prices. Real-money
  smoke on $25 orders only, in liquid markets, after the audit closes.
- **Latency:** end-to-end submit-to-ack on $25-$200 orders; report
  median and p95 vs the RAG's 250ms anecdote (stale, useful as
  direction). Tracker lives in `latency-repricing.md`.
- **Edge-gate regression:** the paper-mode fallback (no book → single
  `fee_buffer`) must reproduce existing cycle-7 trade counts on a
  replay against the snapshot DB. Any drift means we silently changed
  paper behaviour and must roll back the fallback.

## Open questions

- **Does `/midpoint` actually exhibit the docs' "spread > $0.10 →
  last-trade" fallback?** The audit script answers this. If it does
  not — i.e. `/midpoint` always returns a true mid — the wide-spread
  bug downgrades from CRITICAL to "spread term still missing from
  edge gate", which is fix #2 alone.
- **What is the right live-book endpoint?** `clob.py:14-16` says
  `/book/<id>` 404s as of 2026-05-24. Re-probe required.
- **Right `max_slippage` per category?** Statistical — measure first.
  Seeds: ~1¢ crypto, ~3¢ thin, but treat as placeholders.
- **Is there a partial-fill cleanup loop today?** No — there is no
  live executor at all. GTC/GTD must remain unused until one exists.
- **Should we move `min_position_usdc` to $25 intentionally?** Out of
  scope here; flagged for cycle-9 sizing work.

## Cross-cutting notes

- The wide-spread mid bug touches **`closer-fees.md`** (PnL math is
  mid-to-mid and inherits the same wrong "mid") and **`markets-
  gamma.md`** (Gamma's `lastTradePrice` / `outcomePrices` are the
  discovery-side mirror of the same fallback). This plan owns the
  `clob.py` consumer fix; the closer-side persistence of
  `exit_spread` and the Gamma-side parser hardening should be filed
  as follow-ups in those plans.
- The FAK-first execution shape overlaps `latency-repricing.md` (the
  WebSocket / co-location agenda) — the executor's freshness check is
  cheap on HTTP polling, near-free on a live WebSocket subscription.
- The min-position floor overlaps `closer-fees.md` (cash-clip-then-
  gate). No action here beyond flagging.
- This plan deliberately does not edit `philosophy.md`, `config.yaml`,
  or any source file. It is the audit + design document; the diffs
  belong to the cycle that takes ownership.
