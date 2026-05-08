"""
Seulgi database layer — thin wrapper around SQLite via aiosqlite.

Schema:
  markets   - Polymarket markets with embeddings stored as raw bytes
  signals   - Detected headline changes from watched sites
  trades    - Every trade decision (executed or skipped), including close results
  positions - Currently open paper positions (deleted on close)

Embeddings stored as raw float32 bytes (numpy tobytes/frombuffer).

Schema migrations are applied at connect() time. New columns are added
idempotently — each ALTER TABLE is attempted and the error silently ignored
if the column already exists (SQLite's only safe migration pattern).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import aiosqlite
import numpy as np

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS engine_results (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    engine_name     TEXT NOT NULL,
    condition_id    TEXT NOT NULL,
    question        TEXT NOT NULL DEFAULT '',
    direction       TEXT NOT NULL,
    confidence      REAL NOT NULL,
    similarity      REAL NOT NULL,
    p_hat           REAL NOT NULL,
    mid_price       REAL,
    edge            REAL NOT NULL DEFAULT 0.0,
    size_usdc       REAL NOT NULL,
    executed        INTEGER NOT NULL DEFAULT 0,  -- 1=executed, 0=skipped
    skip_reason     TEXT,
    decided_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS markets (
    condition_id    TEXT PRIMARY KEY,
    question        TEXT NOT NULL,
    token_yes       TEXT,
    token_no        TEXT,
    end_date        TEXT,
    volume_usdc     REAL,
    embedding       BLOB,          -- float32 bytes, shape (384,)
    fetched_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS signals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    site_url        TEXT NOT NULL,
    headline        TEXT NOT NULL,
    detected_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS trades (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_id       INTEGER NOT NULL REFERENCES signals(id),
    condition_id    TEXT NOT NULL,
    question        TEXT NOT NULL DEFAULT '',
    token_id        TEXT,
    direction       TEXT NOT NULL,
    similarity      REAL NOT NULL,
    p_hat           REAL NOT NULL,
    mid_price       REAL,
    size_usdc       REAL NOT NULL,
    skip_reason     TEXT,           -- NULL = executed
    decided_at      TEXT NOT NULL,
    realized_pnl    REAL,           -- filled on close; NULL if still open / skipped
    closed_at       TEXT            -- ISO timestamp when position closed
);

CREATE TABLE IF NOT EXISTS positions (
    trade_id        INTEGER PRIMARY KEY REFERENCES trades(id),
    condition_id    TEXT NOT NULL,
    question        TEXT NOT NULL DEFAULT '',
    token_id        TEXT NOT NULL,
    direction       TEXT NOT NULL,
    size_usdc       REAL NOT NULL,
    entry_price     REAL NOT NULL,
    opened_at       TEXT NOT NULL,
    last_price      REAL,
    unrealized_pnl  REAL DEFAULT 0.0,
    p_hat           REAL
);
"""

# Idempotent migrations for columns added after initial schema
_PRICE_IMPACT_SCHEMA = """
CREATE TABLE IF NOT EXISTS price_impacts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    headline_id     TEXT NOT NULL,
    headline        TEXT NOT NULL,
    site_url        TEXT NOT NULL,
    condition_id    TEXT NOT NULL,
    question        TEXT NOT NULL,
    token_id        TEXT NOT NULL,
    direction       TEXT NOT NULL,      -- YES or NO (which token we expect to move)
    ai_confidence   REAL NOT NULL,
    similarity      REAL NOT NULL,
    price_t0        REAL,               -- price at headline detection time
    price_t1m       REAL,               -- price 1 minute later
    price_t5m       REAL,               -- price 5 minutes later
    price_t15m      REAL,               -- 15 minutes later
    price_t30m      REAL,               -- 30 minutes later
    price_t60m      REAL,               -- 60 minutes later
    delta_t1m       REAL,               -- price_t1m  - price_t0
    delta_t5m       REAL,               -- price_t5m  - price_t0
    delta_t15m      REAL,
    delta_t30m      REAL,
    delta_t60m      REAL,
    detected_at     TEXT NOT NULL,      -- ISO timestamp of headline
    source          TEXT DEFAULT 'news' -- 'news' | 'twitter' | 'truth_social'
);

CREATE INDEX IF NOT EXISTS idx_pi_headline_id ON price_impacts(headline_id);
CREATE INDEX IF NOT EXISTS idx_pi_condition_id ON price_impacts(condition_id);
CREATE INDEX IF NOT EXISTS idx_pi_detected_at ON price_impacts(detected_at);
"""

_MIGRATIONS = [
    "ALTER TABLE trades    ADD COLUMN question     TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE trades    ADD COLUMN realized_pnl REAL",
    "ALTER TABLE trades    ADD COLUMN closed_at    TEXT",
    "ALTER TABLE positions ADD COLUMN question     TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE positions ADD COLUMN unrealized_pnl REAL DEFAULT 0.0",
    "ALTER TABLE signals        ADD COLUMN headline_id TEXT",
    "ALTER TABLE trades         ADD COLUMN headline_id TEXT",
    "ALTER TABLE engine_results ADD COLUMN headline_id TEXT",
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class SeulgiDB:
    """
    Async SQLite wrapper for Seulgi.

    All operations go through a single aiosqlite connection; SQLite's
    serialised write model means no extra locking is needed here.
    """

    def __init__(self, path: str) -> None:
        self._path = path
        self._conn: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        """Open the DB connection, create schema, and apply migrations."""
        self._conn = await aiosqlite.connect(self._path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.executescript(_SCHEMA)
        await self._conn.executescript(_PRICE_IMPACT_SCHEMA)
        await self._conn.commit()
        await self._migrate()
        logger.info("SeulgiDB connected: %s", self._path)

    async def _migrate(self) -> None:
        """Apply column additions idempotently — ignores 'duplicate column' errors."""
        for sql in _MIGRATIONS:
            try:
                await self._conn.execute(sql)
                await self._conn.commit()
            except Exception:
                pass  # column already exists

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    # ------------------------------------------------------------------
    # Markets
    # ------------------------------------------------------------------

    async def upsert_market(
        self,
        condition_id: str,
        question: str,
        token_yes: Optional[str],
        token_no: Optional[str],
        end_date: Optional[str],
        volume_usdc: Optional[float],
        embedding: Optional[np.ndarray],
    ) -> None:
        emb_bytes = embedding.tobytes() if embedding is not None else None
        await self._conn.execute(
            """
            INSERT OR REPLACE INTO markets
              (condition_id, question, token_yes, token_no, end_date, volume_usdc, embedding, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (condition_id, question, token_yes, token_no, end_date,
             volume_usdc, emb_bytes, _now_iso()),
        )
        await self._conn.commit()

    async def get_markets_with_embeddings(self) -> list[dict]:
        async with self._conn.execute(
            "SELECT condition_id, question, token_yes, token_no, embedding "
            "FROM markets WHERE embedding IS NOT NULL"
        ) as cur:
            rows = await cur.fetchall()
        result = []
        for row in rows:
            emb = np.frombuffer(row["embedding"], dtype=np.float32).copy()
            result.append({
                "condition_id": row["condition_id"],
                "question": row["question"],
                "token_yes": row["token_yes"],
                "token_no": row["token_no"],
                "embedding": emb,
            })
        return result

    async def count_markets(self) -> int:
        async with self._conn.execute("SELECT COUNT(*) FROM markets") as cur:
            row = await cur.fetchone()
            return row[0] if row else 0

    # ------------------------------------------------------------------
    # Signals
    # ------------------------------------------------------------------

    async def insert_signal(
        self, site_url: str, headline: str, headline_id: Optional[str] = None
    ) -> int:
        cur = await self._conn.execute(
            "INSERT INTO signals (site_url, headline, detected_at, headline_id) VALUES (?, ?, ?, ?)",
            (site_url, headline, _now_iso(), headline_id),
        )
        await self._conn.commit()
        return cur.lastrowid

    # ------------------------------------------------------------------
    # Trades
    # ------------------------------------------------------------------

    async def insert_trade(
        self,
        signal_id: int,
        condition_id: str,
        question: str,
        token_id: Optional[str],
        direction: str,
        similarity: float,
        p_hat: float,
        mid_price: Optional[float],
        size_usdc: float,
        skip_reason: Optional[str],
        headline_id: Optional[str] = None,
    ) -> int:
        cur = await self._conn.execute(
            """
            INSERT INTO trades
              (signal_id, condition_id, question, token_id, direction, similarity,
               p_hat, mid_price, size_usdc, skip_reason, decided_at, headline_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (signal_id, condition_id, question, token_id, direction, similarity,
             p_hat, mid_price, size_usdc, skip_reason, _now_iso(), headline_id),
        )
        await self._conn.commit()
        return cur.lastrowid

    async def close_trade(self, trade_id: int, realized_pnl: float) -> None:
        """Record the outcome of a closed position on the trade row."""
        await self._conn.execute(
            "UPDATE trades SET realized_pnl = ?, closed_at = ? WHERE id = ?",
            (realized_pnl, _now_iso(), trade_id),
        )
        await self._conn.commit()

    async def recent_trades(self, limit: int = 20) -> list[dict]:
        async with self._conn.execute(
            """
            SELECT t.id, t.condition_id, t.question, t.direction, t.p_hat,
                   t.mid_price, t.size_usdc, t.skip_reason, t.decided_at,
                   t.realized_pnl, t.closed_at, s.headline, s.site_url
            FROM trades t JOIN signals s ON t.signal_id = s.id
            ORDER BY t.decided_at DESC LIMIT ?
            """,
            (limit,),
        ) as cur:
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # Positions
    # ------------------------------------------------------------------

    async def open_position(
        self,
        trade_id: int,
        condition_id: str,
        question: str,
        token_id: str,
        direction: str,
        size_usdc: float,
        entry_price: float,
        p_hat: float,
    ) -> None:
        await self._conn.execute(
            """
            INSERT OR REPLACE INTO positions
              (trade_id, condition_id, question, token_id, direction,
               size_usdc, entry_price, opened_at, last_price, unrealized_pnl, p_hat)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0.0, ?)
            """,
            (trade_id, condition_id, question, token_id, direction,
             size_usdc, entry_price, _now_iso(), entry_price, p_hat),
        )
        await self._conn.commit()

    async def get_open_positions(self) -> list[dict]:
        async with self._conn.execute("SELECT * FROM positions") as cur:
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    async def has_open_position(self, condition_id: str) -> bool:
        async with self._conn.execute(
            "SELECT 1 FROM positions WHERE condition_id = ? LIMIT 1",
            (condition_id,),
        ) as cur:
            return await cur.fetchone() is not None

    async def count_open_positions(self) -> int:
        async with self._conn.execute("SELECT COUNT(*) FROM positions") as cur:
            row = await cur.fetchone()
            return row[0] if row else 0

    async def get_committed_usdc(self) -> float:
        """Sum of size_usdc across all open positions — used to restore balance."""
        async with self._conn.execute(
            "SELECT COALESCE(SUM(size_usdc), 0.0) FROM positions"
        ) as cur:
            row = await cur.fetchone()
            return float(row[0]) if row else 0.0

    async def update_position_price(
        self, trade_id: int, last_price: float, unrealized_pnl: float
    ) -> None:
        await self._conn.execute(
            "UPDATE positions SET last_price = ?, unrealized_pnl = ? WHERE trade_id = ?",
            (last_price, unrealized_pnl, trade_id),
        )
        await self._conn.commit()

    async def close_position(self, trade_id: int) -> None:
        await self._conn.execute(
            "DELETE FROM positions WHERE trade_id = ?", (trade_id,)
        )
        await self._conn.commit()

    # ------------------------------------------------------------------
    # Balance state restoration
    # ------------------------------------------------------------------

    async def get_realized_pnl_total(self) -> float:
        """Sum of all realized_pnl from closed trades."""
        async with self._conn.execute(
            "SELECT COALESCE(SUM(realized_pnl), 0.0) FROM trades WHERE realized_pnl IS NOT NULL"
        ) as cur:
            row = await cur.fetchone()
            return float(row[0]) if row else 0.0

    # ------------------------------------------------------------------
    # Engine results (multi-engine comparison)
    # ------------------------------------------------------------------

    async def record_engine_decisions(
        self, engine_name: str, decisions: list, headline_id: Optional[str] = None
    ) -> None:
        """Bulk-insert all TradeDecision objects from one engine run."""
        now = _now_iso()
        rows = [
            (
                engine_name,
                d.condition_id,
                d.question,
                d.direction.value,
                d.confidence,
                d.similarity,
                d.p_hat,
                d.mid_price,
                d.edge,
                d.size_usdc,
                1 if d.executed else 0,
                d.skip_reason,
                now,
                headline_id,
            )
            for d in decisions
        ]
        await self._conn.executemany(
            """
            INSERT INTO engine_results
              (engine_name, condition_id, question, direction, confidence,
               similarity, p_hat, mid_price, edge, size_usdc, executed,
               skip_reason, decided_at, headline_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        await self._conn.commit()

    async def get_engine_summary(self) -> list[dict]:
        """
        Per-engine summary: trades executed, total staked, skip breakdown.
        Used by the dashboard engine_comparison.txt file.
        """
        async with self._conn.execute(
            """
            SELECT
                engine_name,
                COUNT(*)                                        AS total_decisions,
                SUM(executed)                                   AS executed_count,
                COUNT(*) - SUM(executed)                        AS skipped_count,
                COALESCE(SUM(CASE WHEN executed=1 THEN size_usdc END), 0) AS total_staked,
                COALESCE(AVG(CASE WHEN executed=1 THEN edge END), 0)      AS avg_edge,
                COALESCE(AVG(CASE WHEN executed=1 THEN confidence END), 0) AS avg_confidence,
                MIN(decided_at)                                 AS first_trade,
                MAX(decided_at)                                 AS last_trade
            FROM engine_results
            GROUP BY engine_name
            ORDER BY executed_count DESC
            """
        ) as cur:
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    async def get_engine_skip_reasons(self) -> list[dict]:
        """Per-engine skip reason breakdown."""
        async with self._conn.execute(
            """
            SELECT engine_name, skip_reason, COUNT(*) AS count
            FROM engine_results
            WHERE executed = 0
            GROUP BY engine_name, skip_reason
            ORDER BY engine_name, count DESC
            """
        ) as cur:
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # Price impact tracking
    # ------------------------------------------------------------------

    async def insert_price_impact(
        self,
        headline_id: str,
        headline: str,
        site_url: str,
        condition_id: str,
        question: str,
        token_id: str,
        direction: str,
        ai_confidence: float,
        similarity: float,
        price_t0: Optional[float],
        detected_at: str,
        source: str = "news",
    ) -> int:
        """Create a new price impact row. Returns the row id."""
        async with self._conn.execute(
            """
            INSERT INTO price_impacts
              (headline_id, headline, site_url, condition_id, question,
               token_id, direction, ai_confidence, similarity,
               price_t0, detected_at, source)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (headline_id, headline, site_url, condition_id, question,
             token_id, direction, ai_confidence, similarity,
             price_t0, detected_at, source),
        ) as cur:
            row_id = cur.lastrowid
        await self._conn.commit()
        return row_id

    async def update_price_impact_snapshot(
        self, row_id: int, bucket: str, price: float
    ) -> None:
        """
        Update one time-bucket column (t1m, t5m, t15m, t30m, t60m) and its delta.

        bucket must be one of: "t1m", "t5m", "t15m", "t30m", "t60m"
        """
        assert bucket in ("t1m", "t5m", "t15m", "t30m", "t60m")
        await self._conn.execute(
            f"""
            UPDATE price_impacts
            SET price_{bucket} = ?,
                delta_{bucket} = ? - price_t0
            WHERE id = ?
            """,
            (price, price, row_id),
        )
        await self._conn.commit()

    async def get_pending_price_impacts(self) -> list[dict]:
        """Return rows that still have at least one bucket unfilled (up to 2h old)."""
        async with self._conn.execute(
            """
            SELECT * FROM price_impacts
            WHERE (price_t60m IS NULL)
              AND detected_at > datetime('now', '-2 hours')
            ORDER BY detected_at DESC
            LIMIT 500
            """
        ) as cur:
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    async def get_price_impact_stats(self, limit: int = 500) -> list[dict]:
        """Return completed rows (price_t60m is not null) for analysis."""
        async with self._conn.execute(
            """
            SELECT * FROM price_impacts
            WHERE price_t60m IS NOT NULL
            ORDER BY detected_at DESC LIMIT ?
            """,
            (limit,),
        ) as cur:
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    async def get_engine_recent_trades(
        self, engine_name: str, limit: int = 20
    ) -> list[dict]:
        async with self._conn.execute(
            """
            SELECT * FROM engine_results
            WHERE engine_name = ? AND executed = 1
            ORDER BY decided_at DESC LIMIT ?
            """,
            (engine_name, limit),
        ) as cur:
            rows = await cur.fetchall()
        return [dict(row) for row in rows]
