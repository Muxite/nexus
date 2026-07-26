"""
## Specification

**Purpose.** Provide the SQLite connection, the atomic `batch()` context
manager that capture-writes piggyback on, and the schema-bootstrap
mechanism that stage modules plug into. **No domain DDL lives in this
file** (per `docs/modularization.md` §4 "Table ownership"). The only
exception is the housekeeping `schema_migrations` table that this module
owns itself.

**Public interface.**
- `DB` — async wrapper around `aiosqlite`.
  - `DB(path: str)` — construct; calling `connect()` is required before use.
  - `await db.connect()` — opens the connection, applies pragmas, creates the
    `schema_migrations` table, and runs every DDL statement registered via
    `register_schema()`.
  - `await db.close()` — closes.
  - `async with db.batch() as cur:` — opens a single transaction; yields an
    `aiosqlite.Cursor`. Capture writes (`decision_inputs`, `llm_calls`) MUST
    share this cursor with the live write so the two cannot diverge.
  - `db.register_schema(owner: str, statements: list[str])` — call at module
    import time. `owner` is the file the statements belong to (e.g.
    `"app/stages/ingest.py"`); used by the table-ownership lint.
- `DB.ephemeral()` — convenience factory for in-memory tests.
- `IreneDBPathError` — raised by the seulgi-path assertion.

**Invariants.**
- The seulgi-path assertion fires at `connect()` time. It rejects:
  - any path whose basename is `seulgi.db`,
  - any path containing a directory named `seulgi`,
  - the empty string.
  `:memory:` is allowed for tests.
- `batch()` either commits in full or rolls back in full. No partial writes.
- `batch()` is **serialised by an `asyncio.Lock`** — concurrent callers
  queue rather than collide. SQLite is single-writer at the database
  level, and `aiosqlite`'s shared connection raises "cannot start a
  transaction within a transaction" if two `BEGIN`s overlap on the same
  connection. Serialising at the Python level avoids that and matches
  the SQLite write semantics exactly.
- `register_schema()` is idempotent — re-registering the same `(owner, ddl)`
  is a no-op.

**Side effects.** Opens an `aiosqlite` connection; sets WAL + NORMAL pragmas;
creates the `schema_migrations` table on first connect.

**Failure modes.**
- `IreneDBPathError` on a seulgi-shaped path.
- `aiosqlite.OperationalError` propagates if a registered DDL is malformed.

**Non-goals.** Holding domain CREATE statements (except `schema_migrations`
and `decision_inputs`; see below). Holding query helpers — stages own
their queries.

**One curated exception: `decision_inputs`.** Per `docs/p1_contracts.md`
§7 the capture row for each decision MUST be written inside the same
`db.batch()` as the live write. The natural home for that helper is the
`DB` class itself (where the batch lives), and the natural home for the
DDL follows the helper. `db.capture(cur, ...)` is the public surface.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Optional

import aiosqlite

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Path assertion
# ---------------------------------------------------------------------------


class IreneDBPathError(RuntimeError):
    """Raised when DB.connect() is given a path that overlaps seulgi."""


def _assert_not_seulgi(path: str) -> None:
    if path == ":memory:":
        return
    if not path:
        raise IreneDBPathError("DB path is empty")
    p = Path(path)
    if p.name == "seulgi.db":
        raise IreneDBPathError(
            f"refusing to open {p!r} — basename 'seulgi.db' belongs to the "
            "live seulgi pipeline. See project_dual_container memo."
        )
    if "seulgi" in p.parts:
        raise IreneDBPathError(
            f"refusing to open {p!r} — path contains a 'seulgi' directory "
            "component. Irene must keep its own DB tree (default /data/irene.db)."
        )


# ---------------------------------------------------------------------------
# Schema registry
# ---------------------------------------------------------------------------


@dataclass
class _Migration:
    owner: str
    statements: tuple[str, ...]


_REGISTRY: list[_Migration] = []


def register_schema(owner: str, statements: list[str]) -> None:
    """Stage modules call this at import time to declare their DDL.

    `owner` is the dotted module path or file path that owns the listed
    tables; used by the table-ownership CI lint.
    """
    norm = _Migration(owner=owner, statements=tuple(statements))
    for m in _REGISTRY:
        if m.owner == norm.owner and m.statements == norm.statements:
            return  # idempotent re-registration
    _REGISTRY.append(norm)


def _registered_owners() -> list[str]:
    """For tests / introspection."""
    return [m.owner for m in _REGISTRY]


# Housekeeping — db.py owns the migration log itself.
_SCHEMA_MIGRATIONS_DDL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    owner           TEXT NOT NULL,
    statement_sha   TEXT NOT NULL,
    applied_at      TEXT NOT NULL,
    UNIQUE(owner, statement_sha)
)
"""

# Capture rows — written via `DB.capture(cur, ...)` inside the live batch
# of whichever stage produced the decision input. Per-signal in P1 (match
# is the only producer); the schema is flexible enough to accommodate
# other kinds of capture in later phases by stuffing them under a
# different `kind` value.
_DECISION_INPUTS_DDL = [
    """
    CREATE TABLE IF NOT EXISTS decision_inputs (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        kind            TEXT NOT NULL,
        ref_id          INTEGER NOT NULL,
        payload_json    TEXT NOT NULL,
        snapshot_json   TEXT NOT NULL,
        cycle_id        INTEGER NOT NULL,
        is_tracer       INTEGER NOT NULL DEFAULT 0,
        tracer_id       TEXT,
        captured_at     TEXT NOT NULL,
        UNIQUE(kind, ref_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_decision_inputs_ref ON decision_inputs(kind, ref_id)",
    "CREATE INDEX IF NOT EXISTS idx_decision_inputs_cycle_id ON decision_inputs(cycle_id)",
    "CREATE INDEX IF NOT EXISTS idx_decision_inputs_tracer_id ON decision_inputs(tracer_id)",
]


def _sha1(text: str) -> str:
    import hashlib
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------


@dataclass
class DB:
    path: str
    journal_mode: str = "WAL"
    synchronous: str = "NORMAL"
    _conn: Optional[aiosqlite.Connection] = field(default=None, init=False, repr=False)
    # Created lazily in connect() — asyncio.Lock pins itself to the running
    # event loop on first use, so we delay construction until the loop exists.
    _batch_lock: Optional["asyncio.Lock"] = field(default=None, init=False, repr=False)

    @classmethod
    def ephemeral(cls) -> "DB":
        return cls(path=":memory:", journal_mode="MEMORY", synchronous="OFF")

    async def connect(self) -> None:
        _assert_not_seulgi(self.path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute(f"PRAGMA journal_mode={self.journal_mode}")
        await self._conn.execute(f"PRAGMA synchronous={self.synchronous}")
        await self._conn.execute("PRAGMA foreign_keys=ON")
        # Cycle 26 followup: dual-container (irene + irene-explorer) write
        # contention on the same WAL surfaces as `database is locked` when
        # SQLite's default busy_timeout=0 makes the second writer fail
        # immediately. 30s covers the long-held write window during the
        # cycle-26 dual-read upsert loop (~69k markets via /events +
        # /markets union per poll). If we wait past 30s, something is
        # genuinely stuck and surfacing the error is correct.
        await self._conn.execute("PRAGMA busy_timeout=30000")
        self._batch_lock = asyncio.Lock()
        await self._apply_schema()
        logger.info("irene DB connected at %s", self.path)

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def _apply_schema(self) -> None:
        assert self._conn is not None
        await self._conn.execute(_SCHEMA_MIGRATIONS_DDL)
        for ddl in _DECISION_INPUTS_DDL:
            await self._conn.execute(ddl)
        await self._conn.commit()
        now = datetime.now(timezone.utc).isoformat()
        for mig in _REGISTRY:
            for stmt in mig.statements:
                sha = _sha1(stmt)
                cur = await self._conn.execute(
                    "SELECT 1 FROM schema_migrations WHERE owner=? AND statement_sha=?",
                    (mig.owner, sha),
                )
                already = await cur.fetchone()
                await cur.close()
                if already:
                    continue
                await self._conn.execute(stmt)
                await self._conn.execute(
                    "INSERT INTO schema_migrations (owner, statement_sha, applied_at) "
                    "VALUES (?, ?, ?)",
                    (mig.owner, sha, now),
                )
        await self._conn.commit()

    @asynccontextmanager
    async def batch(self) -> AsyncIterator[aiosqlite.Cursor]:
        """Open a transaction. Yields a cursor; commits on success, rolls
        back on exception. Capture writes share this cursor with the live
        write (modularization rule + master-plan §"Capture").

        Serialised by `self._batch_lock`. Concurrent subscriber tasks
        queueing for the writer is correct SQLite behaviour anyway — the
        DB file accepts one writer at a time.
        """
        if self._conn is None or self._batch_lock is None:
            raise RuntimeError("DB.connect() must be called before batch()")
        async with self._batch_lock:
            cur = await self._conn.cursor()
            try:
                await cur.execute("BEGIN")
                yield cur
                await self._conn.commit()
            except Exception:
                await self._conn.rollback()
                raise
            finally:
                await cur.close()

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("DB.connect() must be called before .conn access")
        return self._conn

    # ---- helpers ---------------------------------------------------------

    async def capture(
        self,
        cur: aiosqlite.Cursor,
        *,
        kind: str,
        ref_id: int,
        payload: dict[str, Any],
        snapshot: dict[str, Any],
        is_tracer: bool = False,
        tracer_id: Optional[str] = None,
    ) -> None:
        """Write a `decision_inputs` row using an already-open cursor.

        MUST be called inside an existing `db.batch()` — the cursor's
        transaction is the live write's transaction, and the capture row
        commits or rolls back atomically with the live row
        (`docs/p1_contracts.md` §7).

        Idempotent on `(kind, ref_id)` — INSERT OR IGNORE. The first
        capture for a given (kind, ref_id) wins; replays do not overwrite.
        """
        # cycle_id read from the contextvar at write time — calling capture
        # outside a configured run is a wiring bug worth raising on.
        from app.cycle import current_cycle_id  # local import avoids a top-level cycle
        cycle_id = current_cycle_id()
        await cur.execute(
            """
            INSERT OR IGNORE INTO decision_inputs
                (kind, ref_id, payload_json, snapshot_json, cycle_id,
                 is_tracer, tracer_id, captured_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                kind,
                ref_id,
                json.dumps(payload, sort_keys=True, default=str),
                json.dumps(snapshot, sort_keys=True, default=str),
                cycle_id,
                1 if is_tracer else 0,
                tracer_id,
                datetime.now(timezone.utc).isoformat(),
            ),
        )

    async def ensure_current_cycle(self, name: str) -> int:
        """Insert-or-fetch the cycle row, return cycle_id.

        Owned by main.py per `docs/modularization.md` §4; main.py is the only
        caller in P1. Kept on DB for ergonomic reasons (single boot helper).
        The CREATE TABLE for `cycles` is registered by main.py at import time.
        """
        async with self.batch() as cur:
            await cur.execute(
                "INSERT OR IGNORE INTO cycles (name, opened_at) VALUES (?, ?)",
                (name, datetime.now(timezone.utc).isoformat()),
            )
            await cur.execute("SELECT id FROM cycles WHERE name=?", (name,))
            row = await cur.fetchone()
        if row is None:
            raise RuntimeError(f"cycle row not found after upsert: {name!r}")
        return int(row[0])
