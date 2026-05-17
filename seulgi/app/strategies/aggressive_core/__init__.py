"""aggressive_core — carved-out high-ROI strategy module.

Selected by scripts/analysis/validate_report.py on 2026-05-17:
    aggressive/conf25_tp005_sl015_lowvol — n=44, hit 81.8%
    Wilson 95% CI [68.0%, 90.5%], ROI +2.98% on $6600 staked.

Module layout (see plan-a-new-part-shimmying-kitten.md §B.3):
    config.py        — CoreConfig dataclass
    trader_base.py   — Trader ABC: place_order / close_order / verify_balance
    paper_trader.py  — PaperTrader (the default Trader)
    live_trader.py   — LiveTrader stub
    guards.py        — Guard protocol + concrete entry guards
    exits.py         — ExitPolicy
    strategy.py      — CoreStrategy — orchestrates the cascade
    reconcile.py     — on-chain reconciliation (paper no-op today)
    observability.py — structured event bus
"""

from .config import CoreConfig

__all__ = ["CoreConfig"]
