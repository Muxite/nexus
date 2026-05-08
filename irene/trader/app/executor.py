"""
Order executor.

In paper mode:  records the TradeDecision to the DB, returns None.
In live mode:   signs and submits a FOK/FAK order via py-clob-client,
                records the fill result to the DB.

Live mode requires POLYMARKET_PRIVATE_KEY, API_KEY, API_SECRET, API_PASSPHRASE
set in config. The py-clob-client handles EIP-712 signing and Polygon submission.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from irene_shared.config import IreneConfig
from irene_shared.db import IreneDB
from irene_shared.models import FillResult, TradeDecision, TradeMode

logger = logging.getLogger(__name__)


class Executor:
    """
    Submits orders or logs paper decisions.

    Stateless beyond the DB reference; config is passed on each call.
    """

    def __init__(self, db: IreneDB) -> None:
        self._db = db

    async def execute(
        self,
        decision: TradeDecision,
        cfg: IreneConfig,
    ) -> Optional[FillResult]:
        """
        Execute or paper-log a trade decision.

        Returns FillResult if a live order filled, None otherwise.
        Always writes to the trades table.
        """
        trade_id = await self._db.insert_trade(
            mapped_signal_id=decision.mapped_signal_id,
            condition_id=decision.condition_id,
            token_id=decision.token_id,
            direction=decision.direction.value,
            p_hat=decision.p_hat,
            mid_price=decision.mid_price,
            edge=decision.edge,
            kelly_size_usdc=decision.kelly_size_usdc,
            executed=(cfg.irene.mode == TradeMode.LIVE),
            order_type=decision.order_type,
            skip_reason=decision.skip_reason,
        )

        if cfg.irene.mode != TradeMode.LIVE:
            logger.info(
                "[PAPER] %s %s cond=%s size=$%.2f p_hat=%.3f edge=%.4f",
                decision.direction.value,
                decision.token_id[:8],
                decision.condition_id[:8],
                decision.kelly_size_usdc,
                decision.p_hat,
                decision.edge,
            )
            return None

        # --- Live mode ---
        fill = await self._submit_live_order(decision, cfg)

        if fill:
            await self._db.record_fill(
                trade_id=trade_id,
                order_id=fill.order_id,
                fill_price=fill.fill_price,
                fill_size_usdc=fill.fill_size_usdc,
                fill_at=fill.fill_at,
            )
            await self._db.upsert_position(
                trade_id=trade_id,
                condition_id=decision.condition_id,
                token_id=decision.token_id,
                direction=decision.direction.value,
                size_usdc=fill.fill_size_usdc,
                entry_price=fill.fill_price,
                opened_at=fill.fill_at,
            )
            logger.info(
                "[LIVE] FILLED %s cond=%s size=$%.2f @ %.4f order=%s",
                decision.direction.value,
                decision.condition_id[:8],
                fill.fill_size_usdc,
                fill.fill_price,
                fill.order_id[:8],
            )
        else:
            logger.warning(
                "[LIVE] NO FILL %s cond=%s",
                decision.direction.value,
                decision.condition_id[:8],
            )

        return fill

    # ------------------------------------------------------------------
    # Live order submission
    # ------------------------------------------------------------------

    async def _submit_live_order(
        self,
        decision: TradeDecision,
        cfg: IreneConfig,
    ) -> Optional[FillResult]:
        """
        Submit a signed order to the CLOB via py-clob-client.

        Returns FillResult on success, None if the order did not fill
        (FOK miss, API error, etc.).

        py-clob-client is a sync library; we run it in an executor.
        """
        import asyncio

        loop = asyncio.get_event_loop()
        try:
            fill = await loop.run_in_executor(
                None,
                self._submit_sync,
                decision,
                cfg,
            )
            return fill
        except Exception as exc:
            logger.error("Live order submission failed: %s", exc)
            return None

    @staticmethod
    def _submit_sync(
        decision: TradeDecision,
        cfg: IreneConfig,
    ) -> Optional[FillResult]:
        """
        Synchronous py-clob-client call. Runs in a thread executor.

        Guarded by try/except so a missing py-clob-client install (dev env)
        or a CLOB error never crashes the trader loop.
        """
        try:
            from py_clob_client.client import ClobClient  # type: ignore
            from py_clob_client.clob_types import ApiCreds, OrderArgs, OrderType  # type: ignore
        except ImportError:
            logger.error(
                "py-clob-client not installed — cannot submit live order. "
                "Set irene.mode=paper or install py-clob-client."
            )
            return None

        poly_cfg = cfg.polymarket
        creds = ApiCreds(
            api_key=poly_cfg.api_key,
            api_secret=poly_cfg.api_secret,
            api_passphrase=poly_cfg.api_passphrase,
        )
        client = ClobClient(
            host=poly_cfg.clob_url,
            chain_id=poly_cfg.chain_id,
            key=poly_cfg.private_key,
            creds=creds,
        )

        order_type = (
            OrderType.FOK if cfg.trading.order_type == "FOK" else OrderType.FAK
        )

        # Buy YES or NO token: price is how much USDC per share
        price = decision.mid_price + decision.edge * 0.5  # limit slightly inside spread
        price = round(max(0.01, min(0.99, price)), 4)
        size = decision.kelly_size_usdc / price  # convert USDC to shares

        order_args = OrderArgs(
            token_id=decision.token_id,
            price=price,
            size=round(size, 2),
            side="BUY",
        )

        try:
            resp = client.create_and_post_order(order_args, order_type=order_type)
        except Exception as exc:
            logger.error("py-clob-client order error: %s", exc)
            return None

        # Extract fill info from response
        order_id = resp.get("orderID") or resp.get("order_id") or ""
        fill_price = float(resp.get("price") or price)
        fill_size = float(resp.get("sizeMatched") or resp.get("size_matched") or 0)

        if fill_size < 0.01:
            logger.warning("Order %s did not fill (sizeMatched=%.4f)", order_id, fill_size)
            return None

        return FillResult(
            order_id=order_id,
            token_id=decision.token_id,
            direction=decision.direction,
            fill_price=fill_price,
            fill_size_usdc=fill_size * fill_price,
            fill_at=datetime.now(timezone.utc),
        )
