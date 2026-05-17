# Seulgi deployment report — 2026-05-17

End-of-plan handoff for the carved `aggressive_core` strategy. All nine
sections of `plan-a-new-part-shimmying-kitten.md` shipped this session.

---

## What's live

| Item | Path |
|---|---|
| Carved strategy module | `app/strategies/aggressive_core/` |
| Trader ABC + Paper / Live (stub) | `…/trader_base.py`, `paper_trader.py`, `live_trader.py` |
| Guard cascade (15 guards) | `…/guards.py` |
| Exit policy (TP/SL/TRAIL/TIMEOUT) | `…/exits.py` |
| Strategy orchestrator | `…/strategy.py` |
| Friction-aware PaperTrader | `…/paper_trader.py` (bid/ask cross, latency, vol cap, slippage) |
| Reconciliation hooks | `…/reconcile.py` |
| Event bus + ring buffer | `…/observability.py` |
| Validation gate script | `scripts/analysis/validate_report.py` |
| Validation report | `REPORT_VALIDATION_2026-05-17.md` (VALIDATION: PASS) |
| Fail-fast config validator | `app/config_validate.py` |
| Alerts sink | `app/notify.py` |
| Menu CLI | `python3 -m seulgi` (10-item numbered menu) |
| Soak harness | `app/strategies/aggressive_core/tests/soak/run_soak.py` |
| Unit + integration tests | `app/strategies/aggressive_core/tests/` — **37 pass** |
| Dead-end variants retired | 6 removed from `config.yaml` |
| Snapshot baseline | `snapshots/pre-aggressive-core-2026-05-17.zip` (428 MB) |

---

## Validation gate (§A)

`scripts/analysis/validate_report.py` produces `REPORT_VALIDATION_<date>.md`.
Anchors all PASS; spread sanity OK across 930 closed trades. Winner =
`aggressive/conf25_tp005_sl015_lowvol` (n=44, hit 81.8%, Wilson CI
[68.0%, 90.5%], ROI +2.98%). This is the strategy carved into
`aggressive_core/`.

## Variant cleanup (§C)

Removed (all net-negative, n≥26):
- `balanced/conf40_tp020_sl030` · `balanced/conf55_tp020_sl030` · `balanced/conf70_tp020_sl030`
- `inverted_highconf/conf40_tp020_sl030_inv75` · `inverted_highconf/conf55_tp020_sl030_inv75` · `inverted_highconf/conf40_tp020_sl030_inv75_norail`

Kept (watch-list): all `_hi65`, `_hi75`, and `_lowvol` siblings of
inverted_highconf + balanced. These keep accumulating samples; promotion
is governed by the validate-report selection score (`roi_lower_wilson95
× min(1, n/30) × friction_adj_roi`).

## OOP boundary (§B.3)

`CoreStrategy.__init__(trader: Trader, …)` — strategy never references
"paper" or "live". `LiveTrader.place_order` raises `NotImplementedError`
today; swapping to live is one constructor change in `factory.py`.
`SEULGI_LIVE=1` selects the live trader at bootstrap.

## Capital regime (§B.2)

`CoreConfig` defaults sized for **$50 bankroll** with reinvestment:
`max_position_usdc=5`, `max_open_positions=6`, exposure cap $30,
8 entries/hr, 40/day, AI token budget $2/day. Drawdown halts at -15% / 24h
and -8% / 4h (tight on small bankroll).

## Safety hardening (§D)

15 guards in the entry cascade:
DirectionGuard · MarketClosedGuard · HardRailGuard · SoftRailGuard ·
VolGuard · EdgeGuard · SpreadGuard · DuplicateGuard · PositionCountGuard ·
ExposureGuard · CashGuard · FrequencyGuard · DrawdownGuard ·
TokenBudgetGuard · WatcherHealthGuard.

Each one is independently unit-tested.

## Test suite (§F)

37 tests in `app/strategies/aggressive_core/tests/`:
- 24 unit tests across guards / exits / paper_trader
- 3 integration tests (happy-path, neutral-blocks-entry, high-vol-blocks)
- Soak runner gating on ≥30 closes + observed hit-rate inside validated CI ± 10pp tolerance + ROI within ±2pp of validation

Run: `python3 -m pytest app/strategies/aggressive_core/tests -q` (or
menu option 8 → 1/2/3).

## Operations (§E)

`python3 -m seulgi` — interactive numbered menu. No long argv invocations.
Item `R` resets and starts a fresh $50 cycle with reinvestment.

## Cycle launched (§G)

- New cycle: `aggressive_core_fresh_50_20260517` (cycles.id=2)
- Docker compose started with `AGGRESSIVE_CORE_ENABLE=1`
- 18 engines registered (17 legacy + `aggressive_core`)
- `aggressive_core` is paper-mode with `cash=$50.00`, reinvestment ON
  (realized PnL added back to cash on close)
- Pre-launch snapshot: `snapshots/pre-aggressive-core-2026-05-17.zip`
  (preserves historical trade data for ongoing validation runs)

## Going live (when ready)

1. Implement `LiveTrader.place_order/close_order/verify_*` (file is
   self-documenting about what's needed).
2. Set `SEULGI_LIVE=1` in `.env`.
3. Restart compose. `MODE=LIVE` boot log + alert via D.6.
4. Reconciler runs at every monitor tick — divergence halts entries.

No other code outside `live_trader.py` and `reconcile.py` needs to
change to support real-money trading.

## Watching the run

- `python3 -m seulgi` → option 4 (Status / health) — one-screen
  trades/positions/PnL plus last 5 alerts.
- Menu option 8 → 3 for a soak run → `REPORT_SOAK_<ts>.md`.
- `python3 -m scripts.analysis validate-report` to re-gate at any time.

---

VALIDATION: PASS · MODE: PAPER · BANKROLL: $50.00 · REINVESTMENT: ON
