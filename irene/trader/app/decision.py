"""
Trade decision engine.

Given a MappedSignal and the current orderbook, decides whether to trade
and at what size. Returns a TradeDecision with a skip_reason if no trade.

All thresholds are read from TradingSection config on each call so hot-reload
works without restart.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from irene_shared.config import TradingSection
from irene_shared.models import Direction, Orderbook, MappedSignal, TradeDecision

logger = logging.getLogger(__name__)

# Fee estimate used in the edge check: Polymarket taker fee + conservative slippage
_DEFAULT_FEE_BUFFER = 0.02


class DecisionEngine:
    """
    Stateless decision engine. Instantiate once; pass config on each call.
    """

    def evaluate(
        self,
        signal: MappedSignal,
        orderbook: Orderbook,
        balance_usdc: float,
        open_positions: int,
        cfg: TradingSection,
    ) -> TradeDecision:
        """
        Evaluate whether to trade a mapped signal.

        Returns a TradeDecision. If any check fails, `executed` is False
        and `skip_reason` explains why.

        Args:
            signal:         The mapped signal to act on.
            orderbook:      Current CLOB orderbook for the relevant token.
            balance_usdc:   Current available balance in USDC.
            open_positions: Number of currently open positions.
            cfg:            Current TradingSection config.
        """
        direction = signal.direction
        now = datetime.now(timezone.utc)

        # Determine which token to buy based on direction
        token_id = signal.token_yes if direction == Direction.YES else signal.token_no

        # --- Step 1: Derive p_hat ---
        p_hat = self._estimate_probability(signal, orderbook, cfg)

        # --- Step 2: Edge check ---
        mid = orderbook.mid_price
        edge = abs(p_hat - mid)
        fee_cost = orderbook.spread / 2 + cfg.fee_buffer
        net_edge = edge - fee_cost

        if net_edge <= 0:
            return self._skip(
                signal, token_id, p_hat, mid, edge, cfg,
                f"edge too small: net_edge={net_edge:.4f} (edge={edge:.4f}, fee_cost={fee_cost:.4f})",
            )

        if net_edge < cfg.min_edge:
            return self._skip(
                signal, token_id, p_hat, mid, edge, cfg,
                f"net_edge={net_edge:.4f} < min_edge={cfg.min_edge}",
            )

        # --- Step 3: Exposure check ---
        if open_positions >= cfg.max_open_positions:
            return self._skip(
                signal, token_id, p_hat, mid, edge, cfg,
                f"at position cap: {open_positions}/{cfg.max_open_positions}",
            )

        # --- Step 4: Kelly sizing ---
        # Half-Kelly: size = kelly_fraction * (edge / price) * balance
        # For a binary outcome betting on p at price q: edge/odds = (p-q)/(1-q)
        price_to_buy = orderbook.best_ask if direction == Direction.YES else (1.0 - orderbook.best_bid)
        price_to_buy = max(price_to_buy, 0.01)  # guard against near-zero
        kelly_raw = net_edge / (1.0 - price_to_buy)
        kelly_size = cfg.kelly_fraction * kelly_raw * balance_usdc
        kelly_size = min(kelly_size, cfg.max_position_fraction * balance_usdc)
        kelly_size = max(kelly_size, 0.0)

        if kelly_size < 1.0:
            return self._skip(
                signal, token_id, p_hat, mid, edge, cfg,
                f"kelly_size=${kelly_size:.2f} too small (< $1.00)",
            )

        # --- Step 5: Liquidity check ---
        available_depth = (
            orderbook.ask_depth_usdc if direction == Direction.YES
            else orderbook.bid_depth_usdc
        )
        max_from_liquidity = cfg.max_liquidity_fraction * available_depth
        if kelly_size > max_from_liquidity and max_from_liquidity >= 1.0:
            kelly_size = max_from_liquidity
            logger.debug("Size capped by liquidity to $%.2f", kelly_size)
        elif max_from_liquidity < 1.0:
            return self._skip(
                signal, token_id, p_hat, mid, edge, cfg,
                f"insufficient depth: available=${available_depth:.2f}, "
                f"max_tradeable=${max_from_liquidity:.2f}",
            )

        return TradeDecision(
            mapped_signal_id=signal.signal_id,
            condition_id=signal.condition_id,
            token_id=token_id,
            direction=direction,
            p_hat=round(p_hat, 6),
            mid_price=round(mid, 6),
            edge=round(net_edge, 6),
            kelly_size_usdc=round(kelly_size, 4),
            executed=False,  # executor sets this to True after submission
            order_type=cfg.order_type,
            skip_reason=None,
            decided_at=now,
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _estimate_probability(
        self,
        signal: MappedSignal,
        orderbook: Orderbook,
        cfg: TradingSection,
    ) -> float:
        """
        Derive p_hat from signal direction and mid-price.

        YES: p_hat = mid + edge_increment_yes
        NO:  p_hat = mid - edge_increment_no  (i.e. we think NO token rises)

        Clamped to (0.01, 0.99) to avoid degenerate Kelly sizing.
        """
        mid = orderbook.mid_price
        if signal.direction == Direction.YES:
            p_hat = mid + cfg.edge_increment_yes
        else:
            p_hat = mid - cfg.edge_increment_no

        return max(0.01, min(0.99, p_hat))

    @staticmethod
    def _skip(
        signal: MappedSignal,
        token_id: str,
        p_hat: float,
        mid: float,
        edge: float,
        cfg: TradingSection,
        reason: str,
    ) -> TradeDecision:
        logger.debug("Skip %s: %s", signal.condition_id, reason)
        return TradeDecision(
            mapped_signal_id=signal.signal_id,
            condition_id=signal.condition_id,
            token_id=token_id,
            direction=signal.direction,
            p_hat=round(p_hat, 6),
            mid_price=round(mid, 6),
            edge=round(edge, 6),
            kelly_size_usdc=0.0,
            executed=False,
            order_type=None,
            skip_reason=reason,
            decided_at=datetime.now(timezone.utc),
        )
