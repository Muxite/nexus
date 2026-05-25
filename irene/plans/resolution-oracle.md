# Plan — resolution / oracle close path (D1-D3)

> Source: `irene/questions/unstructured answers.md` lines 174-194, batch
> `2026-05-25-unstructured-1`. Cross-refs: `research_answers.md` D1-D3
> (batch 5, 2026-05-25), `research_questions.md` O-D1/O-D2/O-D3, handoff
> `2026-05-25.md` follow-up #2.

## Context

Two code changes converge here, plus one audit-column add:

1. **`closer.py` + `markets.py::_close_orphaned_trades_for`** — close
   path currently books at `entry_mid` (`net_pnl=−fees`) the moment
   Gamma drops a market from the open list. Research says `closed=true`
   is NOT finality — settlement runs through UMA's Optimistic Oracle
   with a 2h challenge window (undisputed) or ~4-6 days (disputed). Need
   a **three-state machine**: `market_closed → proposed_resolution →
   finalized_resolution`.

2. **`trade.py::_days_to` (line 647) + `_check_market_news_imbalance`
   (line 663)** — expiry parsing currently defaults to UTC and reads
   only Gamma `end_date`. Some Polymarket markets use ET (DST-related
   incident confirms it, line 184). Fix: prefer market-rules timestamp,
   default-zone ET, fall back to title-string only when rules are silent.

3. **Dispute-exposure log** — public seeds exist (1.96% disputed,
   98.11% undisputed, >80% from "too-early") but those are priors only.
   We need our own dispute-exposure log to replace the seed after soak.

This plan **owns handoff follow-up #2** ("auto-close at correct $1/$0").
It consumes the new field reads (`acceptingOrders`, `umaResolutionStatus`,
`resolvedBy`, `outcomePrices`) added by `markets-gamma.md`.

## Findings

### Protocol facts (✅ act on these)

- **UMA Optimistic Oracle** settles Polymarket International (lines
  178-179). Polymarket US uses "Markets Team". We don't currently
  distinguish — see Q2 below.
- **2-hour uncontested challenge window** after a proposal is posted;
  effectively final after that window if no dispute (lines 180-181).
- **Disputed path: ~4-6 days** = ~24-48h debate + ~48h token-weighted
  vote (lines 180-181).
- **`closed=true` ≠ finality** (lines 180, 192). Trading has stopped;
  the oracle has not necessarily spoken. Stop opening, enter wait — do
  not settle the book.
- **Some markets use ET** for the operative clock; market rules page
  is the source of truth for end timestamp + timezone (lines 183-185).
- **Disputes do flip outcomes** — public March 2025 reversal exists
  (lines 189-190). Rare but non-zero.
- **Finalization is immutable** on Polymarket once UMA finalizes
  (research_answers.md D3). The state machine only needs forward
  transitions.

### Seeds only (📊 priors, never config defaults)

- ~**1.96% dispute rate** at UMA-wide scale, Aug 2024 (217 / 11,093,
  line 187). Useful prior for tail-risk sizing; track our own rate.
- ~**98.11% undisputed proposals** (line 188).
- ~**>80% of disputes** came from proposals submitted too early
  (line 188). Implies most disputes resolve to "Too Early" and the
  retried proposal produces the originally-intended outcome.

Compounds **W2** (`p_hat=1.0 → Full Kelly → ruin`): even a 2% dispute
rate is non-trivial tail risk on a finalization-instant assumption.

### Stale / ambiguous (⚠️)

- Aug 2024 dispute numbers are **directionally correct, may drift**.
- **No published median** from `closed=true` to final `outcomePrices`
  (line 175). The 2h / 4-6 day numbers describe the oracle process
  once a proposal is posted — not the gap between close and proposal.
  See Q3 below.

## Proposed changes

### 1. `closer.py` + `markets.py` — three-state close machine (PRIMARY)

Today (`markets.py:_close_orphaned_trades_for:666-734`): when a
`condition_id` drops from Gamma's open list we call `write_trade_closure`
with `exit_mid=trade["mid_price"]`. PnL = `−fees`.

New flow, keyed off the three Gamma fields the parser plan adds plus
`outcomePrices`:

**State 1: `market_closed`** (`closed=true` OR `acceptingOrders=false`
OR dropped from `/markets?active=true&closed=false`)
- STOP opening new positions on this `condition_id` (already enforced
  by `active=0`; make the intent explicit).
- Do NOT auto-settle. The existing trade row stays open; no
  `trade_closures` row written yet.

**State 2: `proposed_resolution`** (`umaResolutionStatus="proposed"`
OR `resolvedBy` set AND `outcomePrices` missing/empty)
- Enter a post-close wait of `cfg.closer.uma_undisputed_wait_hours`
  (default 6h = 2h window + 4h propagation buffer).
- Do NOT mark-to-market against the (likely stale / purged) CLOB mid
  during the wait — once Gamma drops the listing the orderbook is
  gone (see `closer.py:60-69`).
- Stamp `markets.uma_proposed_at = now` so a restart resumes correctly.

**State 3: `finalized_resolution`** (`outcomePrices` populated AND
`umaResolutionStatus` indicates final)
- Translate `outcomePrices` (e.g. `["1","0"]`) into the trade's payout:
  - YES trade: `exit_mid = float(outcomePrices[0])`
  - NO trade:  `exit_mid = 1.0 - float(outcomePrices[0])`
- Write `trade_closures` with `close_reason="market_resolved"` and the
  proper $1/$0 exit. Fee math is unchanged
  (`closer.py:_compute_round_trip_fees:181`).
- Stamp `markets.uma_finalized_at = now`.

**Fallback: human-review** — after `cfg.closer.uma_dispute_wait_hours`
(default 168h = 7 days) with no `finalized_resolution`, write a
placeholder closure with `close_reason="manual"` and
`exit_mid=trade["mid_price"]` (the cycle-7 conservative path), AND emit
`WARN closer.dispute_overrun trade_id=X market=Y waited=Zh` for human
audit. The `close_reason` CHECK constraint already includes `'manual'`
(`closer.py:97`); no DDL change.

**State persistence** — add three nullable columns to `markets`:
- `uma_proposed_at` (TEXT, ISO)
- `uma_finalized_at` (TEXT, ISO)
- `last_resolution_status` (TEXT)

These let the post-close wait survive process restarts without
re-querying Gamma history.

### 2. `trade.py` — deadline parsing (ET + market-rules first)

Current (`trade.py:_days_to:647-661`):
```python
end = datetime.fromisoformat(end_date_iso.replace("Z", "+00:00"))
if end.tzinfo is None:
    end = end.replace(tzinfo=timezone.utc)
```

Two issues: defaults to UTC when `tzinfo` is missing (should be ET);
reads only `end_date`, ignores any explicit time in the rules text.

Changes:
- Flip the default-fallback zone to ET via
  `zoneinfo.ZoneInfo("America/New_York")` (handles DST natively).
  Explicit-offset strings continue to parse correctly.
- New helper `_parse_market_deadline(market) -> datetime|None`
  preferring (in order): (1) explicit timestamp regex-extracted from
  `description` (`"by <date>[, <time>] (ET|UTC|GMT)"`); (2) Gamma
  `end_date` with ET default; (3) None — and the existing `days is
  None` defensives (lines 693-699) skip the time-bound sub-rules.
- Unit test: `"by June 30"` → `2026-06-30 23:59:59 ET`
  = `2026-07-01 03:59:59 UTC`, matching ET end-of-day not UTC midnight.

Affects `cfg.trade.min_days_to_expiry`, `cfg.trade.far_future_days`,
and the cycle-7 `near_expiry` sub-rule (config.py:251-252).

### 3. Dispute-exposure logging (📊 → empirical)

Add two nullable columns to `trades` (not `trade_closures` — disputes
can be observed between entry and close):
- `dispute_observed` (INTEGER, 0/1, default 0) — set to 1 when the
  closer first sees `umaResolutionStatus="disputed"` (or any contested
  marker) on a market with an open trade.
- `dispute_resolved_at` (TEXT, ISO, nullable) — set when
  `dispute_observed=1` AND `outcomePrices` becomes populated.

After ~1 month of soak, compute our own dispute rate stratified by
`category` and `volume_usdc` bucket. If it matches the ~2% Aug-2024
seed, leave wait timeouts unchanged. If higher (we're concentrated in
dispute-prone categories), tighten post-close exposure.

## Open questions

- **Q1.** Does Gamma expose `umaResolutionStatus="proposed"` distinctly
  from `"resolved"`, or do we need to query UMA directly? The
  unstructured-1 batch only enumerates the `"resolved"` payload. Capture
  a mid-window market via UMA's optimistic-oracle dashboard.
- **Q2.** Polymarket-US vs International detection from the Gamma
  payload — possible? If not, do we assume all our markets are
  International? Matters because the state machine is UMA-shaped; US
  markets would not have `umaResolutionStatus` at all.
- **Q3.** Right value for `cfg.closer.uma_undisputed_wait_hours`?
  Conservative 6h vs aggressive 3h. Default 6h until we have own data
  on propagation lag.
- **Q4.** Can we distinguish "closed but oracle not yet asked" from
  "closed and no oracle activity for hours"? If Q1 shows we can't see
  the proposed/finalized transition, we need an explicit timer in
  State 1 to escalate to human-review earlier.
- **Q5.** `outcomePrices` shape for N>2-outcome markets. We only trade
  YES/NO; confirm the new code rejects N>2 gracefully rather than
  silently picking `outcome[0]`.

## Verification

1. **Lifecycle capture.** Three recently-resolved markets (election,
   sports finished, one contested if findable). Pull Gamma at pre-close
   → closed-pre-proposal → proposal-posted → finalized. Confirm the
   field transitions match the three-state machine.
2. **Shadow-mode soak.** Run the three-state machine alongside the
   current `_close_orphaned_trades_for` for 1-2 weeks. Log
   would-be-PnL delta vs current `entry_mid` close. Promote to live
   if shadow PnL is systematically higher (it should be — $1/$0 vs
   eating fees).
3. **ET deadline unit test** on ≥5 historical markets where the rules
   text is known; hand-verify against Polymarket's page.
4. **Dispute-exposure backfill.** Once audit columns ship, one-shot
   backfill from cached Gamma payloads (likely empty on day 1; soak
   builds the data).
5. **Closure idempotency regression.** Confirm `INSERT OR IGNORE` on
   `trade_closures.trade_id UNIQUE` (closer.py:95) prevents a
   proposed→finalized re-entry from double-writing.

## Cross-cutting notes

- **Field-read ownership.** This plan *consumes* the three new fields
  (`accepting_orders`, `uma_resolution_status`, `resolved_by`) that
  `markets-gamma.md` adds to `_parse_gamma_row`. **Also need
  `outcome_prices`** — the parser plan doesn't currently list it; add
  it there in the parser PR review. Do NOT duplicate reads in
  `closer.py`. Ship parser additions first, then the state-machine
  read.
- **Supersedes prior D-section in `research_answers.md`.** The plan
  stubs at lines 238, 251, 263 of that file pointed forward to "this
  plan" — flag for the patcher to mark them superseded. Mark the
  Aug-2024 dispute-rate snapshot ⚠️ "directionally correct, may drift".
- **Cycle-7 conservative close is retained** as the human-review
  fallback (State 3 fallback), preserving the invariant that no trade
  is left open forever.
- **W2 interaction.** The 📊 dispute seed compounds the unbounded
  `p_hat=1.0 → Full Kelly → ruin` weakspot; enforcing W2's `p_hat_cap`
  becomes more important once we hold positions through the post-close
  wait.
- **Closes handoff #2.** When this plan ships, replace handoff
  follow-up #2 in `handoffs/2026-05-25.md` with the resolving commit
  hash.
