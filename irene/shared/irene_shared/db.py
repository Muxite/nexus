"""
Async PostgreSQL wrapper for Irene using asyncpg.

Usage:
    db = IreneDB(database_url)
    await db.connect()
    # ... use db helpers ...
    await db.close()

Schema is applied (idempotently) on connect.
"""

from __future__ import annotations

import logging
import os
import pickle
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import asyncpg
import numpy as np

logger = logging.getLogger(__name__)

_MIGRATION_PATH = Path(__file__).parent / "migrations" / "001_initial.sql"


def _now() -> datetime:
    return datetime.now(timezone.utc)


class IreneDB:
    """Thin asyncpg wrapper with schema management and typed helpers."""

    def __init__(self, url: str, pool_size: int = 5) -> None:
        self._url = url
        self._pool_size = pool_size
        self._pool: Optional[asyncpg.Pool] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(
            self._url,
            min_size=1,
            max_size=self._pool_size,
        )
        await self._apply_migrations()
        logger.info("IreneDB connected (pool_size=%d)", self._pool_size)

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def _apply_migrations(self) -> None:
        sql = _MIGRATION_PATH.read_text()
        async with self._pool.acquire() as conn:
            await conn.execute(sql)
        logger.debug("Migrations applied")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _pool_required(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("IreneDB.connect() has not been called")
        return self._pool

    # ------------------------------------------------------------------
    # Markets
    # ------------------------------------------------------------------

    async def upsert_market(
        self,
        condition_id: str,
        slug: Optional[str],
        question: str,
        description: Optional[str],
        category: Optional[str],
        end_date: datetime,
        volume_usdc: Optional[float],
        token_yes: Optional[str],
        token_no: Optional[str],
        embedding: Optional[np.ndarray] = None,
    ) -> None:
        pool = self._pool_required()
        now = _now()
        embedding_bytes = pickle.dumps(embedding.astype(np.float32)) if embedding is not None else None
        embedding_updated_at = now if embedding is not None else None

        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO markets
                    (condition_id, slug, question, description, category,
                     end_date, volume_usdc, token_yes, token_no,
                     embedding, embedding_updated_at,
                     active, first_seen_at, updated_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,true,$12,$12)
                ON CONFLICT (condition_id) DO UPDATE SET
                    slug                  = EXCLUDED.slug,
                    question              = EXCLUDED.question,
                    description           = EXCLUDED.description,
                    category              = EXCLUDED.category,
                    end_date              = EXCLUDED.end_date,
                    volume_usdc           = EXCLUDED.volume_usdc,
                    token_yes             = EXCLUDED.token_yes,
                    token_no              = EXCLUDED.token_no,
                    embedding             = COALESCE(EXCLUDED.embedding, markets.embedding),
                    embedding_updated_at  = COALESCE(EXCLUDED.embedding_updated_at, markets.embedding_updated_at),
                    active                = true,
                    updated_at            = EXCLUDED.updated_at
                """,
                condition_id, slug, question, description, category,
                end_date, volume_usdc, token_yes, token_no,
                embedding_bytes, embedding_updated_at, now,
            )

    async def deactivate_expired_markets(self, before: datetime) -> int:
        pool = self._pool_required()
        async with pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE markets SET active=false WHERE end_date < $1 AND active=true",
                before,
            )
        return int(result.split()[-1])

    async def get_active_markets_with_embeddings(self) -> list[dict[str, Any]]:
        """Return all active markets that have embeddings, with embedding as np.ndarray."""
        pool = self._pool_required()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT condition_id, question, token_yes, token_no,
                       end_date, category, embedding
                FROM markets
                WHERE active=true AND embedding IS NOT NULL
                """
            )
        result = []
        for row in rows:
            d = dict(row)
            if d["embedding"]:
                d["embedding"] = pickle.loads(d["embedding"])
            result.append(d)
        return result

    async def get_markets_needing_embeddings(
        self, stale_before: datetime
    ) -> list[dict[str, Any]]:
        """Return active markets with no embedding or stale embedding."""
        pool = self._pool_required()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT condition_id, question, description, token_yes, token_no
                FROM markets
                WHERE active=true
                  AND (embedding IS NULL OR embedding_updated_at < $1)
                """,
                stale_before,
            )
        return [dict(row) for row in rows]

    async def update_market_embedding(
        self, condition_id: str, embedding: np.ndarray
    ) -> None:
        pool = self._pool_required()
        embedding_bytes = pickle.dumps(embedding.astype(np.float32))
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE markets
                SET embedding=$1, embedding_updated_at=$2, updated_at=$2
                WHERE condition_id=$3
                """,
                embedding_bytes, _now(), condition_id,
            )

    # ------------------------------------------------------------------
    # Signals
    # ------------------------------------------------------------------

    async def insert_signal(
        self,
        source: str,
        source_detail: str,
        url: str,
        title: str,
        description: Optional[str],
        published_at: Optional[datetime],
        embedding: Optional[np.ndarray] = None,
    ) -> Optional[int]:
        """Insert a raw signal. Returns signal id, or None if URL already exists."""
        pool = self._pool_required()
        embedding_bytes = pickle.dumps(embedding.astype(np.float32)) if embedding is not None else None
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO signals
                    (source, source_detail, url, title, description,
                     published_at, ingested_at, embedding, mapped)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,false)
                ON CONFLICT (url) DO NOTHING
                RETURNING id
                """,
                source, source_detail, url, title, description,
                published_at, _now(), embedding_bytes,
            )
        return row["id"] if row else None

    async def get_unmapped_signals_with_embeddings(
        self, limit: int = 500
    ) -> list[dict[str, Any]]:
        """
        Return signals that have an embedding stored but have never been
        successfully mapped to a market (mapped=false).

        Used by the feeder backfill loop to re-run the mapper over historical
        signals after the market embedding index has been populated.
        """
        pool = self._pool_required()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, source, source_detail, url, title, description,
                       published_at, ingested_at, embedding
                FROM signals
                WHERE mapped = false AND embedding IS NOT NULL
                ORDER BY ingested_at DESC
                LIMIT $1
                """,
                limit,
            )
        result = []
        for row in rows:
            d = dict(row)
            if d["embedding"]:
                d["embedding"] = pickle.loads(d["embedding"])
            result.append(d)
        return result

    async def mark_signal_mapped(self, signal_id: int) -> None:
        pool = self._pool_required()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE signals SET mapped=true WHERE id=$1", signal_id
            )

    async def update_signal_embedding(
        self, signal_id: int, embedding: np.ndarray
    ) -> None:
        pool = self._pool_required()
        embedding_bytes = pickle.dumps(embedding.astype(np.float32))
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE signals SET embedding=$1 WHERE id=$2",
                embedding_bytes, signal_id,
            )

    # ------------------------------------------------------------------
    # Mapped signals
    # ------------------------------------------------------------------

    async def insert_mapped_signal(
        self,
        signal_id: int,
        condition_id: str,
        similarity_score: float,
        direction: str,
        direction_confidence: float,
    ) -> int:
        pool = self._pool_required()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO mapped_signals
                    (signal_id, condition_id, similarity_score,
                     direction, direction_confidence, created_at, dispatched)
                VALUES ($1,$2,$3,$4,$5,$6,false)
                RETURNING id
                """,
                signal_id, condition_id, similarity_score,
                direction, direction_confidence, _now(),
            )
        return row["id"]

    async def mark_mapped_signal_dispatched(self, mapped_signal_id: int) -> None:
        pool = self._pool_required()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE mapped_signals SET dispatched=true WHERE id=$1",
                mapped_signal_id,
            )

    # ------------------------------------------------------------------
    # Trades
    # ------------------------------------------------------------------

    async def insert_trade(
        self,
        mapped_signal_id: Optional[int],
        condition_id: str,
        token_id: str,
        direction: str,
        p_hat: float,
        mid_price: float,
        edge: float,
        kelly_size_usdc: float,
        executed: bool,
        order_type: Optional[str] = None,
        skip_reason: Optional[str] = None,
    ) -> int:
        pool = self._pool_required()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO trades
                    (mapped_signal_id, condition_id, token_id, direction,
                     p_hat, mid_price, edge, kelly_size_usdc,
                     executed, order_type, skip_reason, decided_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                RETURNING id
                """,
                mapped_signal_id, condition_id, token_id, direction,
                p_hat, mid_price, edge, kelly_size_usdc,
                executed, order_type, skip_reason, _now(),
            )
        return row["id"]

    async def record_fill(
        self,
        trade_id: int,
        order_id: str,
        fill_price: float,
        fill_size_usdc: float,
        fill_at: datetime,
    ) -> None:
        pool = self._pool_required()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE trades
                SET order_id=$1, fill_price=$2, fill_size_usdc=$3, fill_at=$4
                WHERE id=$5
                """,
                order_id, fill_price, fill_size_usdc, fill_at, trade_id,
            )

    async def close_trade(
        self,
        trade_id: int,
        exit_price: float,
        exit_reason: str,
        realized_pnl: float,
        outcome: str,
    ) -> None:
        pool = self._pool_required()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE trades
                SET closed_at=$1, exit_price=$2, exit_reason=$3,
                    realized_pnl=$4, outcome=$5
                WHERE id=$6
                """,
                _now(), exit_price, exit_reason, realized_pnl, outcome, trade_id,
            )

    async def get_open_trades(self) -> list[dict[str, Any]]:
        pool = self._pool_required()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, condition_id, token_id, direction,
                       fill_size_usdc, fill_price, fill_at, decided_at
                FROM trades
                WHERE executed=true AND closed_at IS NULL AND fill_at IS NOT NULL
                """
            )
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # Positions
    # ------------------------------------------------------------------

    async def upsert_position(
        self,
        trade_id: int,
        condition_id: str,
        token_id: str,
        direction: str,
        size_usdc: float,
        entry_price: float,
        opened_at: datetime,
        last_price: Optional[float] = None,
        unrealized_pnl: Optional[float] = None,
    ) -> None:
        pool = self._pool_required()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO positions
                    (trade_id, condition_id, token_id, direction,
                     size_usdc, entry_price, opened_at, last_price, unrealized_pnl)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                ON CONFLICT (trade_id) DO UPDATE SET
                    last_price     = EXCLUDED.last_price,
                    unrealized_pnl = EXCLUDED.unrealized_pnl
                """,
                trade_id, condition_id, token_id, direction,
                size_usdc, entry_price, opened_at, last_price, unrealized_pnl,
            )

    async def delete_position(self, trade_id: int) -> None:
        pool = self._pool_required()
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM positions WHERE trade_id=$1", trade_id)

    async def get_open_positions(self) -> list[dict[str, Any]]:
        pool = self._pool_required()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM positions ORDER BY opened_at DESC")
        return [dict(row) for row in rows]

    async def count_open_positions(self) -> int:
        pool = self._pool_required()
        async with pool.acquire() as conn:
            return await conn.fetchval("SELECT COUNT(*) FROM positions")

    # ------------------------------------------------------------------
    # Market events
    # ------------------------------------------------------------------

    async def log_event(
        self,
        condition_id: str,
        event_type: str,
        payload: dict,
    ) -> None:
        pool = self._pool_required()
        import json
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO market_events (condition_id, event_type, payload, created_at)
                VALUES ($1,$2,$3,$4)
                """,
                condition_id, event_type, json.dumps(payload), _now(),
            )

    async def get_market_events(
        self,
        condition_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        pool = self._pool_required()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, event_type, payload, created_at
                FROM market_events
                WHERE condition_id=$1
                ORDER BY created_at DESC
                LIMIT $2 OFFSET $3
                """,
                condition_id, limit, offset,
            )
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # Brier scores
    # ------------------------------------------------------------------

    async def insert_brier_score(
        self,
        trade_id: int,
        signal_source: str,
        p_hat: float,
        actual_outcome: float,
    ) -> None:
        brier_component = (p_hat - actual_outcome) ** 2
        pool = self._pool_required()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO brier_scores
                    (trade_id, signal_source, p_hat, actual_outcome,
                     brier_component, recorded_at)
                VALUES ($1,$2,$3,$4,$5,$6)
                """,
                trade_id, signal_source, p_hat, actual_outcome,
                brier_component, _now(),
            )

    async def get_brier_summary(self) -> list[dict[str, Any]]:
        """Return average Brier score per signal source."""
        pool = self._pool_required()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT signal_source,
                       COUNT(*) AS sample_count,
                       AVG(brier_component) AS mean_brier,
                       MIN(recorded_at) AS first_at,
                       MAX(recorded_at) AS last_at
                FROM brier_scores
                GROUP BY signal_source
                ORDER BY mean_brier ASC
                """
            )
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # Monitor read helpers
    # ------------------------------------------------------------------

    async def get_dashboard_stats(self) -> dict[str, Any]:
        """
        Aggregate counts and P&L for the text dashboard (24h windows in UTC).

        :returns: Counts, P&L aggregates, timestamps, and ``generated_at``.
        """
        pool = self._pool_required()
        now = _now()
        since_24h = now - timedelta(hours=24)
        async with pool.acquire() as conn:
            active_markets = await conn.fetchval(
                "SELECT COUNT(*) FROM markets WHERE active = true"
            )
            open_positions = await conn.fetchval("SELECT COUNT(*) FROM positions")
            signals_total = await conn.fetchval("SELECT COUNT(*) FROM signals")
            signals_mapped = await conn.fetchval(
                "SELECT COUNT(*) FROM signals WHERE mapped = true"
            )
            signals_24h = await conn.fetchval(
                "SELECT COUNT(*) FROM signals WHERE ingested_at >= $1",
                since_24h,
            )
            mapped_total = await conn.fetchval("SELECT COUNT(*) FROM mapped_signals")
            mapped_24h = await conn.fetchval(
                "SELECT COUNT(*) FROM mapped_signals WHERE created_at >= $1",
                since_24h,
            )
            mapped_dispatched = await conn.fetchval(
                "SELECT COUNT(*) FROM mapped_signals WHERE dispatched = true"
            )
            trades_closed_24h = await conn.fetchval(
                """
                SELECT COUNT(*) FROM trades
                WHERE closed_at IS NOT NULL AND closed_at >= $1
                """,
                since_24h,
            )
            trades_open = await conn.fetchval(
                """
                SELECT COUNT(*) FROM trades
                WHERE closed_at IS NULL AND executed = true
                """
            )
            row = await conn.fetchrow(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN closed_at IS NOT NULL
                         AND realized_pnl IS NOT NULL THEN realized_pnl END), 0)
                        AS realized_pnl_total,
                    COALESCE(SUM(CASE WHEN closed_at IS NOT NULL
                         AND closed_at >= $1 AND realized_pnl IS NOT NULL
                         THEN realized_pnl END), 0)
                        AS realized_pnl_24h,
                    COUNT(*) FILTER (
                        WHERE closed_at IS NOT NULL AND realized_pnl > 0) AS wins,
                    COUNT(*) FILTER (
                        WHERE closed_at IS NOT NULL AND realized_pnl < 0) AS losses,
                    COUNT(*) FILTER (
                        WHERE closed_at IS NOT NULL AND realized_pnl IS NOT NULL
                         AND realized_pnl = 0) AS breakeven,
                    COUNT(*) FILTER (WHERE closed_at IS NOT NULL) AS trades_closed_total,
                    COUNT(*) FILTER (WHERE executed = true) AS trades_executed_total,
                    COUNT(*) FILTER (WHERE executed = false) AS trades_skipped_total
                FROM trades
                """,
                since_24h,
            )
            pos_row = await conn.fetchrow(
                """
                SELECT
                    COALESCE(SUM(unrealized_pnl), 0) AS unrealized_pnl_total,
                    COALESCE(SUM(size_usdc), 0) AS open_exposure_usdc
                FROM positions
                """
            )
            first_signal = await conn.fetchval(
                "SELECT MIN(ingested_at) FROM signals"
            )
            first_trade = await conn.fetchval(
                "SELECT MIN(decided_at) FROM trades WHERE executed = true"
            )
            last_close = await conn.fetchval(
                "SELECT MAX(closed_at) FROM trades WHERE closed_at IS NOT NULL"
            )
        r = dict(row) if row else {}
        pr = dict(pos_row) if pos_row else {}
        return {
            "active_markets": int(active_markets or 0),
            "open_positions": int(open_positions or 0),
            "signals_total": int(signals_total or 0),
            "signals_mapped_total": int(signals_mapped or 0),
            "signals_ingested_24h": int(signals_24h or 0),
            "mapped_signals_total": int(mapped_total or 0),
            "mapped_signals_24h": int(mapped_24h or 0),
            "mapped_signals_dispatched": int(mapped_dispatched or 0),
            "trades_closed_24h": int(trades_closed_24h or 0),
            "trades_open_executed": int(trades_open or 0),
            "trades_closed_total": int(r.get("trades_closed_total") or 0),
            "trades_executed_total": int(r.get("trades_executed_total") or 0),
            "trades_skipped_total": int(r.get("trades_skipped_total") or 0),
            "realized_pnl_total": float(r.get("realized_pnl_total") or 0),
            "realized_pnl_24h": float(r.get("realized_pnl_24h") or 0),
            "unrealized_pnl_total": float(pr.get("unrealized_pnl_total") or 0),
            "open_exposure_usdc": float(pr.get("open_exposure_usdc") or 0),
            "closed_wins": int(r.get("wins") or 0),
            "closed_losses": int(r.get("losses") or 0),
            "closed_breakeven": int(r.get("breakeven") or 0),
            "first_signal_ingested_at": first_signal,
            "first_trade_decided_at": first_trade,
            "last_trade_closed_at": last_close,
            "generated_at": now,
        }

    async def get_active_markets(self, limit: int = 200) -> list[dict[str, Any]]:
        """Return active markets with aggregated trade counts and P&L."""
        pool = self._pool_required()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT m.condition_id, m.slug, m.question, m.category,
                       m.end_date, m.volume_usdc, m.token_yes, m.token_no,
                       m.first_seen_at, m.updated_at,
                       COUNT(t.id)               AS trade_count,
                       COALESCE(SUM(t.realized_pnl), 0) AS realized_pnl
                FROM markets m
                LEFT JOIN trades t
                       ON t.condition_id = m.condition_id
                      AND t.executed = true
                      AND t.closed_at IS NOT NULL
                WHERE m.active = true
                GROUP BY m.condition_id
                ORDER BY m.end_date ASC
                LIMIT $1
                """,
                limit,
            )
        return [dict(row) for row in rows]

    async def get_recent_trades(
        self,
        limit: int = 100,
        offset: int = 0,
        condition_id: Optional[str] = None,
        outcome: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Return trades ordered by decided_at desc, with optional filters."""
        pool = self._pool_required()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, condition_id, token_id, direction, p_hat,
                       mid_price, edge, kelly_size_usdc, executed,
                       order_type, skip_reason, fill_price, fill_size_usdc,
                       fill_at, decided_at, closed_at, exit_price,
                       exit_reason, realized_pnl, outcome
                FROM trades
                WHERE ($1::TEXT IS NULL OR condition_id = $1)
                  AND ($2::TEXT IS NULL OR outcome = $2)
                ORDER BY decided_at DESC
                LIMIT $3 OFFSET $4
                """,
                condition_id, outcome, limit, offset,
            )
        return [dict(row) for row in rows]

    async def get_recent_signals(
        self, limit: int = 100, offset: int = 0
    ) -> list[dict[str, Any]]:
        """Return recent signals with mapped-market match counts."""
        pool = self._pool_required()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT s.id, s.source, s.source_detail, s.url, s.title,
                       s.description, s.published_at, s.ingested_at, s.mapped,
                       COUNT(ms.id) AS match_count
                FROM signals s
                LEFT JOIN mapped_signals ms ON ms.signal_id = s.id
                GROUP BY s.id
                ORDER BY s.ingested_at DESC
                LIMIT $1 OFFSET $2
                """,
                limit, offset,
            )
        return [dict(row) for row in rows]
