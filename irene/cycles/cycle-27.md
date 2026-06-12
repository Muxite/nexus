# Cycle 27 — wallet honesty (spread fills, real fees, position space)

**Date:** 2026-06-12
**Trigger:** `reports/2026-06-12-irene-big-retrospective.md` §8 item 2:
"fix the wallet so it can't lie again." First of four post-retro cycles
(27 wallet honesty → 28 cluster accounting → 29 days-horizon capture →
30 H8 capture-only lane).

## What changed

### 1. Position-space convention (the bug nobody ordered)

While implementing spread fills we found the wallet's price spaces were
inconsistent for NO trades, live since P1:

- `_resolve_mid` (entry) and the closer's poll both read the **traded
  token's** mid — so `trades.mid_price` for NO trades is the NO token's
  own price. Verified against live rows: for NO trades
  `features_snapshot.edge_components.executable_price` (YES space) sits
  at ≈ `1 - mid_price` (16/17 book-backed NO trades).
- But every formula assumed **YES-space** mids: the NO gross-PnL branch
  (`(entry-exit)/(1-entry)`), Kelly's `mid_no = 1 - mid`, the edge
  fallback (`mid - p_hat`), `target_mid` (`p_hat + fee`), the UMA
  resolution exit (always the YES payout), and the dashboard's
  mark-to-market.

Net effect (fee_audit.py §d): **308 of 594 NO closures had sign-flipped
gross PnL**; stored NO gross was +$2,549 where the position-space truth
is +$475. NO sizing/edges/exits were also computed in the wrong space.

**Fix: position space is now the canonical convention** — every stored
price (`mid_price`, fills, `target_mid`, `exit_mid_price`, resolution
payouts) is the traded token's own price. This matches what the data
feed always recorded, so legacy YES rows are untouched and legacy NO
rows become *correctly interpreted* rather than migrated:

- `closer._compute_gross_pnl` — one direction-independent formula.
- `_compute_kelly_and_size` / `_compute_edge` fallback / dkg_trade's
  inline copies — position-space inputs.
- `target_mid` stored position-space; the closer mirrors legacy NO
  targets via `1 - target` when `entry_fill_price IS NULL`.
- `markets._state3_exit_mid` returns the payout of the token the trade
  holds (was: always YES payout).
- `wallet.position_value` — direction-independent; dashboard passes the
  traded token's book mid as before.

### 2. Spread-aware fills + empty-book hard reject

- Entry (`trade.py`): ONE `get_top_of_book` read of the traded token
  feeds fill (=ask), spread, and mid (=(bid+ask)/2). `/midpoint` is
  gone from the entry path (it degrades to last-trade on wide books).
  New first-class columns: `trades.entry_best_bid/entry_best_ask/
  entry_fill_price` (+ `features_snapshot.entry_book`).
- New gate `_gate_book_unavailable` (in `_gates.py`, before
  wide_spread): missing/empty book → `book_unavailable`;
  `markets.volume_usdc` NULL/0 → `zero_volume_market`. Replaces the
  cycle-10 graceful degrade that minted the retro's mid-price ghosts
  (179 zero-volume trades, +$1,461 of untradeable "profit").
- Exit (`closer.py`): `get_top_of_book` on the traded token; rules
  evaluate on the book mid; the exit fills at the **bid**. New columns:
  `trade_closures.exit_best_bid/exit_best_ask/exit_fill_price`. No book
  → hold (`skip_on_missing_mid` semantics). Resolution/dead-market
  closes are redemptions (no book fill, no exit fee) as before.
- Result: the retro's decomposition residual (exactly $0.00 = "no
  spread modeled") becomes the spread actually paid, by construction.
- Legacy open rows (`entry_fill_price IS NULL`) close mid-to-mid via
  the same formula — deterministic, no migration.

### 3. Fee model

- Formula: `fee = shares * fee_rate * min(p, 1-p)` per leg (Polymarket's
  documented schedule), replacing the cycle-7 parabola `p*(1-p)`.
- Rate sourcing: per-market `markets.fee_rate` else
  `cfg.trade.default_fee_rate` = **0.0** (was 0.10). The category→rate
  table is deleted from the fee path (config field kept parse-compat).
- NEW: gamma's poll payload carries the operative fee
  (`feesEnabled` + `takerBaseFee` bps) — parsed every poll into
  `markets.fee_rate` (COALESCE; the cycle-10/15 CLOB fetcher stays as
  fallback). Live probe 2026-06-12: fee-disabled markets are
  feesEnabled=false (CLOB agrees: taker_base_fee=0); fee-enabled
  sports_fees_v2 markets are 1000 bps.
- Audit (`scripts/analysis/fee_audit.py`, read-only): stored DB rates
  are ~90% at a stale 0.10 (cycle-15-era fetch — that survey read the
  schedule ceiling as an operative flat rate); fees-as-charged were
  6.02% of size. The gamma-sourced rates correct this within one poll
  for the active universe.

### 4. `markets.category` (0 rows → populated)

Gamma stopped emitting per-market `category` (probe 2026-06-12: None on
both endpoints) — that's why the column was empty. Now derived from the
parent event's tag slugs via `_TAG_CATEGORY_MAP` (first match wins;
tagged-but-unmatched → 'general'; no event meta → NULL). Upsert
COALESCEs so `/markets`-endpoint rows never clobber a tag-derived
value. Analytics-only (fees no longer read category).

## Files

`app/stages/trade.py` (book gate, fills, kelly/edge, target_mid, DDL),
`app/stages/closer.py` (book exits, fills PnL, fee model, DDL),
`app/stages/_gates.py` (`_gate_book_unavailable`),
`app/stages/dkg_trade.py` (inline NO-space fixes),
`app/stages/markets.py` (position-space payouts, category-from-tags,
gamma fee_rate), `app/wallet.py`, `app/dashboard/pages/main.py`,
`app/config.py`, `scripts/analysis/fee_audit.py` (new), tests.

## Out of scope / notes

- `scripts/replay/predict.py` keeps its own pre-cycle-27 fee mirror —
  it reconstructs historical rows under the rules they were recorded
  with. Needs a version flag before replaying cycle-27+ rows.
- DKG path entries stay mid-priced (no book read there); its trades
  close via the legacy mid path. Fine while DKG stays shadow.
- The `mid_unavailable` gate is now unreachable live (mid derives from
  the book) but kept for replay symmetry.

## Expected live effects (watch after deploy)

- Trade volume DROPS: thin/empty/zero-volume books now hard-reject
  (`book_unavailable` / `zero_volume_market` in the funnel).
- Fees collapse toward 0 on fee-disabled markets as gamma rates land.
- NO-trade PnL becomes sign-correct; per-trade numbers will look
  different from the historical record — that is the point.
- Open trades at deploy close via the legacy mid path; positions on
  dead books are held until resolution/dead-market paths catch them.

## Verification

836 tests green (incl. new: spread-fill round trip asserting
residual == spread paid, position-space NO PnL, book gate matrix,
legacy/new target_mid mirroring, fee model, category/fee parse).
Post-deploy: re-run retro §10 decomposition (residual ≠ 0) +
`fee_audit.py` — goes in the dated cycle-27 report after 24–48h soak.
