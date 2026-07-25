"""Smoke tests for app/db.py.

Covers:
- DB connects on a fresh path + writes the housekeeping `schema_migrations` row.
- Registered DDL is applied; re-running connect() is idempotent.
- `db.batch()` commits on success and rolls back on exception.
- `_assert_not_seulgi()` fires for every shape of seulgi-flavoured path.
- `ensure_current_cycle()` upserts and returns the same id on re-call.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from app import db as db_module
from app.db import DB, IreneDBPathError, _assert_not_seulgi


# ---------------------------------------------------------------------------
# Path assertion (synchronous, no DB needed)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad_path",
    [
        "/data/seulgi.db",
        "/home/muk/projects/nexus/seulgi/data/seulgi.db",
        "seulgi.db",
        "/var/lib/seulgi/whatever.db",
        "",
    ],
)
def test_path_assertion_rejects_seulgi_shapes(bad_path):
    with pytest.raises(IreneDBPathError):
        _assert_not_seulgi(bad_path)


@pytest.mark.parametrize(
    "ok_path",
    [
        "/data/irene.db",
        "/home/muk/projects/nexus/irene/data/irene.db",
        "irene.db",
        ":memory:",
    ],
)
def test_path_assertion_allows_irene_shapes(ok_path):
    _assert_not_seulgi(ok_path)  # does not raise


# ---------------------------------------------------------------------------
# Connection + schema
# ---------------------------------------------------------------------------


@pytest.fixture
def main_module_registers_cycles_schema():
    """Importing `app.main` registers the cycles DDL with db_module — the
    test relies on that side effect to exercise the registry."""
    # noqa: F401 — imported for the registration side effect
    import app.main  # noqa: F401
    return True


async def test_connect_creates_schema_migrations_table(main_module_registers_cycles_schema, tmp_path):
    db = DB(path=":memory:", journal_mode="MEMORY", synchronous="OFF")
    await db.connect()
    try:
        cur = await db.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = [row["name"] for row in await cur.fetchall()]
        await cur.close()
        assert "schema_migrations" in tables
        # cycles table comes from main.py's registration
        assert "cycles" in tables
    finally:
        await db.close()


async def test_connect_is_idempotent(main_module_registers_cycles_schema):
    db = DB.ephemeral()
    await db.connect()
    try:
        cur = await db.conn.execute("SELECT COUNT(*) FROM schema_migrations")
        before = (await cur.fetchone())[0]
        await cur.close()
        await db._apply_schema()  # re-apply; should be a no-op
        cur = await db.conn.execute("SELECT COUNT(*) FROM schema_migrations")
        after = (await cur.fetchone())[0]
        await cur.close()
        assert before == after, "re-applying schema should not duplicate rows"
    finally:
        await db.close()


async def test_connect_rejects_seulgi_path(tmp_path):
    bad = tmp_path / "seulgi" / "seulgi.db"
    db = DB(path=str(bad))
    with pytest.raises(IreneDBPathError):
        await db.connect()


# ---------------------------------------------------------------------------
# batch() semantics
# ---------------------------------------------------------------------------


async def test_batch_commits_on_success(main_module_registers_cycles_schema):
    db = DB.ephemeral()
    await db.connect()
    try:
        async with db.batch() as cur:
            await cur.execute(
                "INSERT INTO cycles (name, opened_at) VALUES (?, ?)",
                ("batch-commit-test", "2026-05-23T00:00:00+00:00"),
            )
        cur = await db.conn.execute("SELECT COUNT(*) FROM cycles WHERE name=?", ("batch-commit-test",))
        assert (await cur.fetchone())[0] == 1
        await cur.close()
    finally:
        await db.close()


async def test_concurrent_batches_serialise_not_collide(main_module_registers_cycles_schema):
    """Multiple subscriber tasks all calling db.batch() must queue on an
    asyncio.Lock — without the lock aiosqlite raises "cannot start a
    transaction within a transaction" on the second concurrent BEGIN."""
    db = DB.ephemeral()
    await db.connect()
    try:
        async with db.batch() as cur:
            await cur.execute("CREATE TABLE counters (i INTEGER)")

        async def writer(i: int) -> None:
            async with db.batch() as cur:
                await cur.execute("INSERT INTO counters (i) VALUES (?)", (i,))
                # Hold the transaction open across an await to expose any
                # missing serialisation — without the lock, a concurrent
                # writer would attempt BEGIN before this one commits.
                await asyncio.sleep(0.005)

        await asyncio.gather(*[writer(i) for i in range(20)])
        cur = await db.conn.execute("SELECT COUNT(*) FROM counters")
        assert (await cur.fetchone())[0] == 20
        await cur.close()
    finally:
        await db.close()


async def test_batch_rolls_back_on_exception(main_module_registers_cycles_schema):
    db = DB.ephemeral()
    await db.connect()
    try:
        with pytest.raises(RuntimeError, match="boom"):
            async with db.batch() as cur:
                await cur.execute(
                    "INSERT INTO cycles (name, opened_at) VALUES (?, ?)",
                    ("batch-rollback-test", "2026-05-23T00:00:00+00:00"),
                )
                raise RuntimeError("boom")
        cur = await db.conn.execute("SELECT COUNT(*) FROM cycles WHERE name=?", ("batch-rollback-test",))
        assert (await cur.fetchone())[0] == 0, "row must not survive a rolled-back batch"
        await cur.close()
    finally:
        await db.close()


# ---------------------------------------------------------------------------
# ensure_current_cycle
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# db.capture (p1_contracts §7)
# ---------------------------------------------------------------------------


async def test_decision_inputs_table_exists_after_connect(main_module_registers_cycles_schema):
    db = DB.ephemeral()
    await db.connect()
    try:
        cur = await db.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='decision_inputs'"
        )
        assert (await cur.fetchone()) is not None
        await cur.close()
    finally:
        await db.close()


async def test_capture_writes_inside_caller_batch(main_module_registers_cycles_schema):
    from app.cycle import cycle_scope
    db = DB.ephemeral()
    await db.connect()
    try:
        with cycle_scope(1):
            async with db.batch() as cur:
                await db.capture(
                    cur,
                    kind="signal",
                    ref_id=42,
                    payload={"a": 1},
                    snapshot={"b": 2},
                    is_tracer=False,
                    tracer_id=None,
                )
        cur = await db.conn.execute(
            "SELECT kind, ref_id FROM decision_inputs WHERE ref_id=42"
        )
        row = await cur.fetchone()
        assert row is not None and row["kind"] == "signal"
        await cur.close()
    finally:
        await db.close()


async def test_capture_rolled_back_with_caller_batch(main_module_registers_cycles_schema):
    """If the caller's batch raises after capture(), the capture row must
    NOT be visible — it shares the live write's transaction."""
    from app.cycle import cycle_scope
    db = DB.ephemeral()
    await db.connect()
    try:
        with cycle_scope(1):
            with pytest.raises(RuntimeError, match="boom"):
                async with db.batch() as cur:
                    await db.capture(
                        cur, kind="signal", ref_id=99,
                        payload={}, snapshot={},
                    )
                    raise RuntimeError("boom")
        cur = await db.conn.execute(
            "SELECT COUNT(*) FROM decision_inputs WHERE ref_id=99"
        )
        assert (await cur.fetchone())[0] == 0
        await cur.close()
    finally:
        await db.close()


async def test_capture_idempotent_on_kind_ref(main_module_registers_cycles_schema):
    from app.cycle import cycle_scope
    db = DB.ephemeral()
    await db.connect()
    try:
        with cycle_scope(1):
            async with db.batch() as cur:
                await db.capture(cur, kind="signal", ref_id=1, payload={"v": 1}, snapshot={})
            async with db.batch() as cur:
                await db.capture(cur, kind="signal", ref_id=1, payload={"v": 2}, snapshot={})
        cur = await db.conn.execute(
            "SELECT COUNT(*), MAX(payload_json) FROM decision_inputs WHERE ref_id=1"
        )
        row = await cur.fetchone()
        assert row[0] == 1, "INSERT OR IGNORE should keep the first capture"
        # first call wrote v=1; the second should not have overwritten
        import json as _json
        assert _json.loads(row[1])["v"] == 1
        await cur.close()
    finally:
        await db.close()


async def test_capture_outside_cycle_scope_raises(main_module_registers_cycles_schema):
    db = DB.ephemeral()
    await db.connect()
    try:
        with pytest.raises(LookupError):
            async with db.batch() as cur:
                await db.capture(cur, kind="signal", ref_id=1, payload={}, snapshot={})
    finally:
        await db.close()


async def test_ensure_current_cycle_is_idempotent(main_module_registers_cycles_schema):
    db = DB.ephemeral()
    await db.connect()
    try:
        first = await db.ensure_current_cycle("cycle-test")
        second = await db.ensure_current_cycle("cycle-test")
        assert first == second
        third = await db.ensure_current_cycle("cycle-different")
        assert third != first
    finally:
        await db.close()
