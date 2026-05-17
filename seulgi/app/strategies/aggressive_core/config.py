"""CoreConfig — capital, frequency, token, and strategy-parameter constants.

Two layers in one dataclass:

  STRATEGY parameters — the validated edge from REPORT_VALIDATION:
    aggressive/conf25_tp005_sl015_lowvol. These were the inputs to the
    legacy variant whose closed trades survived Wilson-CI screening.
    Changing them invalidates the validated edge; treat as load-bearing.

  CAPITAL / FREQUENCY / TOKEN constants — new in the carved module.
    Tuned for a ~$50 real bankroll. Strategy code never reads these —
    the Trader and the guards do. The strategy logic is therefore
    capital-regime-agnostic: paper-trade with $50 here, or in theory
    with $5 or $500 later, without touching the strategy.

  SAFETY constants — drawdown and starvation thresholds. Per plan §D.2.

Inheriting from the legacy TradingConfig defaults is deliberately avoided.
Listing every value explicitly makes the validated-edge contract auditable
and prevents silent drift if a global default ever changes.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class CoreConfig:
    # ── Identity ────────────────────────────────────────────────────────────
    # Logical engine_name written to the trades table. Analysis tooling
    # (variants.py) uses COALESCE(variant_name, engine_name, 'default') so
    # filling both with the same value gives the carved strategy a single
    # row in every leaderboard.
    engine_name: str = "aggressive_core"
    # Source variant that earned the carve. Recorded in the runtime_mode
    # table on boot so a future validation re-score can detect drift.
    source_variant: str = "aggressive/conf25_tp005_sl015_lowvol"

    # ── Strategy parameters (validated edge — DO NOT TUNE casually) ─────────
    min_confidence: float = 0.25
    min_agreement: float = 0.66           # AI ensemble agreement floor
    min_edge: float = 0.005
    edge_increment: float = 0.20
    take_profit: float = 0.005            # +0.5¢ MTM exit
    stop_loss: float = 0.015              # -1.5¢ MTM exit
    trailing_stop: float = 0.003          # 0.3¢ trail from high-water once in profit
    min_hold_seconds: float = 60.0
    position_timeout_hours: float = 24.0
    cold_position_hours: float = 12.0
    entry_rail_skip_threshold: float = 0.02
    rail_skip_threshold: float = 0.0
    max_market_vol_30m: float = 0.02      # _lowvol filter baked in
    # Validated edge does NOT include these — left at neutral so a future
    # config can opt in without changing the dataclass shape.
    stop_loss_pct: float = 0.0
    min_entry_yes_mid: float = 0.0
    invert_high_conf: bool = False
    invert_conf_threshold: float = 0.85

    # ── Capital (tuned for ~$50 bankroll) ───────────────────────────────────
    bankroll_usdc: float = 50.0
    max_position_usdc: float = 5.0
    min_trade_usdc: float = 1.0
    max_open_positions: int = 6
    max_unique_markets: int = 6
    max_exposure_pct: float = 0.60        # halt new entries above this fraction of bankroll
    max_size_per_market_per_hour_usdc: float = 5.0

    # ── Frequency & token discipline ────────────────────────────────────────
    min_seconds_between_entries: float = 90.0
    max_entries_per_hour: int = 8
    max_entries_per_day: int = 40
    daily_ai_token_budget_usd: float = 2.00
    relevance_gate_required: bool = True
    min_top_similarity: float = 0.55      # raised from legacy 0.45 — cuts Stage-3 calls
    ensemble_samples_normal: int = 1      # legacy default was 3
    ensemble_samples_premium: int = 3     # premium fallback keeps the ensemble

    # ── Activity floor (observability, not a halt) ─────────────────────────
    min_entries_per_day_target: int = 4
    starvation_alert_hours: float = 6.0

    # ── Safety thresholds (§D.2) ────────────────────────────────────────────
    max_drawdown_pct: float = 0.15        # halt at -15% of bankroll over window
    drawdown_window_hours: float = 24.0
    session_drawdown_pct: float = 0.08    # second-tier halt at -8% over 4h
    session_drawdown_window_hours: float = 4.0

    # ── Friction model (§B.1 — used by PaperTrader at fill time) ───────────
    spread_cost_bps: float = 20.0         # legacy round-trip default; preserved
    fee_bps: float = 0.0                  # Polymarket today; reserve for future
    entry_delay_ms: int = 250             # simulated order-placement latency
    max_entry_spread_bps: float = 80.0    # refuse if bid-ask wider than this
    slippage_tolerance_bps: float = 50.0  # abort if price drifted between quote & place
    book_depth_levels: int = 3            # top-N levels considered for fillability
    book_depth_factor: float = 0.9        # fillable = min(req, sum(top-N) * factor)

    # ── Mode ────────────────────────────────────────────────────────────────
    # Single source of truth for paper-vs-live. Read once at bootstrap to
    # pick PaperTrader vs LiveTrader. NO if-branches on this value live
    # inside strategy / guards / exits.
    mode: str = "paper"                   # "paper" | "live"

    # ── Optional per-environment overrides ──────────────────────────────────
    db_path: Optional[str] = None         # default = legacy app DB path
    notes: list[str] = field(default_factory=list)

    # ── Convenience ─────────────────────────────────────────────────────────
    @property
    def is_live(self) -> bool:
        return self.mode == "live"

    def derived(self) -> dict:
        """Compact dict for boot-log / observability events."""
        return {
            "engine_name": self.engine_name,
            "source_variant": self.source_variant,
            "mode": self.mode,
            "bankroll_usdc": self.bankroll_usdc,
            "max_position_usdc": self.max_position_usdc,
            "max_open_positions": self.max_open_positions,
            "min_confidence": self.min_confidence,
            "take_profit": self.take_profit,
            "stop_loss": self.stop_loss,
            "trailing_stop": self.trailing_stop,
            "max_market_vol_30m": self.max_market_vol_30m,
            "max_drawdown_pct": self.max_drawdown_pct,
            "daily_ai_token_budget_usd": self.daily_ai_token_budget_usd,
        }
