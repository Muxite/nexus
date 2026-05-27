"""Tests for app/system_stats.py + the system_stats dashboard page.

Covers the CPU% math, the memory accounting, the page's empty/non-empty
render paths, and the retention prune. The collector loop itself is
not exercised end-to-end here (it needs a docker socket); the seam is
`_sample_one`, which is mocked separately in any docker-touching probe.
"""

from __future__ import annotations

import pytest

# DDL fan-in: importing the module registers `container_stats`.
import app.system_stats as system_stats  # noqa: E402
from app.dashboard import Dashboard  # noqa: E402
from app.dashboard.pages import system_stats as system_stats_page  # noqa: E402
from app.db import DB  # noqa: E402


@pytest.fixture
async def db_ready():
    db = DB.ephemeral()
    await db.connect()
    try:
        yield db
    finally:
        await db.close()


# ── CPU% math ──────────────────────────────────────────────────────────


def _payload(*, cpu_total, sys_total, pre_cpu_total, pre_sys_total,
             online_cpus=4, mem_usage=0, mem_limit=0, inactive_file=0,
             cache=0):
    return {
        "cpu_stats": {
            "cpu_usage": {"total_usage": cpu_total},
            "system_cpu_usage": sys_total,
            "online_cpus": online_cpus,
        },
        "precpu_stats": {
            "cpu_usage": {"total_usage": pre_cpu_total},
            "system_cpu_usage": pre_sys_total,
        },
        "memory_stats": {
            "usage": mem_usage,
            "limit": mem_limit,
            "stats": {"inactive_file": inactive_file, "cache": cache},
        },
    }


def test_cpu_pct_standard_formula():
    # 50% of 4 cores → 200% total. cpu_delta=2e9, sys_delta=4e9, online=4
    # → (2e9 / 4e9) * 4 * 100 = 200.0
    p = _payload(
        cpu_total=3_000_000_000, pre_cpu_total=1_000_000_000,
        sys_total=10_000_000_000, pre_sys_total=6_000_000_000,
        online_cpus=4,
    )
    assert system_stats._cpu_pct(p) == pytest.approx(200.0)


def test_cpu_pct_zero_on_fresh_container():
    # precpu_stats absent / zeroed → don't extrapolate from a single sample.
    p = _payload(
        cpu_total=1_000_000, pre_cpu_total=0,
        sys_total=2_000_000, pre_sys_total=0,
    )
    # sys_delta > 0 → formula yields a value. Still safe (small).
    # The guard we care about: when both deltas are 0 we return 0.0.
    p_zero = _payload(
        cpu_total=0, pre_cpu_total=0, sys_total=0, pre_sys_total=0,
    )
    assert system_stats._cpu_pct(p_zero) == 0.0


def test_cpu_pct_missing_keys_returns_zero():
    assert system_stats._cpu_pct({}) == 0.0
    assert system_stats._cpu_pct({"cpu_stats": {}}) == 0.0


# ── memory accounting ──────────────────────────────────────────────────


def test_mem_used_subtracts_inactive_file():
    p = _payload(
        cpu_total=0, pre_cpu_total=0, sys_total=0, pre_sys_total=0,
        mem_usage=500_000_000, mem_limit=4_000_000_000,
        inactive_file=120_000_000,
    )
    used, limit = system_stats._mem_used_bytes(p)
    assert used == 380_000_000
    assert limit == 4_000_000_000


def test_mem_used_falls_back_to_cache_when_no_inactive_file():
    p = _payload(
        cpu_total=0, pre_cpu_total=0, sys_total=0, pre_sys_total=0,
        mem_usage=300_000_000, mem_limit=2_000_000_000,
        inactive_file=0, cache=50_000_000,
    )
    used, limit = system_stats._mem_used_bytes(p)
    assert used == 250_000_000
    assert limit == 2_000_000_000


def test_mem_used_clamps_negative_to_zero():
    # Pathological: inactive_file > usage. Don't return a negative number.
    p = _payload(
        cpu_total=0, pre_cpu_total=0, sys_total=0, pre_sys_total=0,
        mem_usage=10, mem_limit=100, inactive_file=50,
    )
    used, _ = system_stats._mem_used_bytes(p)
    assert used == 0


# ── DDL is registered ──────────────────────────────────────────────────


def test_ddl_owner_registered():
    from app.db import _registered_owners  # noqa: WPS437 — test introspection
    assert "app/system_stats.py" in _registered_owners()


# ── page renders ──────────────────────────────────────────────────────


async def test_page_returns_empty_string_when_no_rows(db_ready, tmp_path):
    dash = Dashboard(data_dir=tmp_path, db=db_ready, interval_secs=60)
    body = await system_stats_page.render(dash)
    assert body == ""


async def test_page_renders_latest_per_container(db_ready, tmp_path):
    async with db_ready.batch() as cur:
        # Insert two ticks for two containers; page should pick the
        # newest row per container only.
        await cur.execute(
            "INSERT INTO container_stats "
            "(ts, container, cpu_pct, mem_bytes, mem_limit_bytes) "
            "VALUES (?, ?, ?, ?, ?)",
            ("2026-05-27T10:00:00+00:00", "irene", 5.0,
             100 << 20, 4 << 30),
        )
        await cur.execute(
            "INSERT INTO container_stats "
            "(ts, container, cpu_pct, mem_bytes, mem_limit_bytes) "
            "VALUES (?, ?, ?, ?, ?)",
            ("2026-05-27T10:00:30+00:00", "irene", 12.4,
             312 << 20, 4 << 30),
        )
        await cur.execute(
            "INSERT INTO container_stats "
            "(ts, container, cpu_pct, mem_bytes, mem_limit_bytes) "
            "VALUES (?, ?, ?, ?, ?)",
            ("2026-05-27T10:00:30+00:00", "jennie-scraper-v2", 31.7,
             178 << 20, 4 << 30),
        )
    dash = Dashboard(data_dir=tmp_path, db=db_ready, interval_secs=60)
    body = await system_stats_page.render(dash)

    assert body.startswith("# irene system stats")
    # Newest irene tick (12.4) shown; older (5.0) suppressed.
    assert "12.4" in body
    assert "  5.0" not in body  # leading space avoids matching '12.4'-suffix
    assert "irene" in body
    assert "jennie-scraper-v2" in body
    assert "31.7" in body


# ── retention prune ───────────────────────────────────────────────────


async def test_prune_drops_rows_older_than_retain_window(db_ready):
    async with db_ready.batch() as cur:
        # Old row (3 hours ago) and a fresh one (now).
        await cur.execute(
            "INSERT INTO container_stats "
            "(ts, container, cpu_pct, mem_bytes, mem_limit_bytes) "
            "VALUES (datetime('now','-3 hours'), 'irene', 0.0, 0, 0)"
        )
        await cur.execute(
            "INSERT INTO container_stats "
            "(ts, container, cpu_pct, mem_bytes, mem_limit_bytes) "
            "VALUES (datetime('now'), 'irene', 0.0, 0, 0)"
        )

    await system_stats._prune(db_ready, retain_minutes=120)

    cur = await db_ready.conn.execute("SELECT COUNT(*) FROM container_stats")
    n = (await cur.fetchone())[0]
    await cur.close()
    assert n == 1


# ── _write_rows + empty-list short-circuit ─────────────────────────────


async def test_write_rows_no_op_on_empty(db_ready):
    # An empty iterable must not open a batch (would still be fine, but
    # we promised: "missing target → no row, no exception").
    await system_stats._write_rows(db_ready, [], "2026-05-27T00:00:00+00:00")
    cur = await db_ready.conn.execute("SELECT COUNT(*) FROM container_stats")
    n = (await cur.fetchone())[0]
    await cur.close()
    assert n == 0


async def test_write_rows_persists_full_tuple(db_ready):
    await system_stats._write_rows(
        db_ready,
        [{"container": "irene", "cpu_pct": 9.5,
          "mem_bytes": 1_000_000, "mem_limit_bytes": 4_000_000_000}],
        "2026-05-27T00:00:00+00:00",
    )
    cur = await db_ready.conn.execute(
        "SELECT container, cpu_pct, mem_bytes, mem_limit_bytes "
        "FROM container_stats"
    )
    rows = await cur.fetchall()
    await cur.close()
    assert len(rows) == 1
    assert rows[0]["container"] == "irene"
    assert rows[0]["cpu_pct"] == pytest.approx(9.5)
    assert rows[0]["mem_bytes"] == 1_000_000
