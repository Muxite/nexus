"""Tests for the cycle-24 tracer-stress-harness extension.

Covers:
  * All 12 cycle-24 templates parse and produce >= 50 distinct articles.
  * `fire.py` accepts `--tracer-id-prefix`, `--exercise-wallet`,
    `--budget-cap-usd`, and `--force` flags without crashing the existing
    embedded-mock path.
  * `audit_burst.funnel()` aggregates an in-memory burst correctly.
  * `cleanup.plan_deletes` matches what's there and
    `cleanup.execute_deletes` removes every tracer row while leaving
    non-tracer rows intact.

We deliberately do NOT exercise `--live-externals` end-to-end here —
that would shell out to OpenRouter. The CI mock path through
`run_inproc` is the cheap, deterministic way to assert structure.
"""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import pytest

# Register every DDL before any DB.connect()/sqlite3.connect.
import app.ai  # noqa: F401
import app.main  # noqa: F401
import app.stages.dedupe  # noqa: F401
import app.stages.extract  # noqa: F401
import app.stages.ingest  # noqa: F401
import app.stages.markets  # noqa: F401
import app.stages.match  # noqa: F401
import app.stages.trade  # noqa: F401

from scripts.tracer import audit_burst, cleanup, fire


TEMPLATES_DIR = Path(fire.TEMPLATES_DIR)

NEW_TEMPLATES = (
    "iran_strike",
    "iran_peace_deal",
    "hezbollah_disarm",
    "market_crash",
    "central_bank_surprise",
    "election_upset",
    "celebrity_event",
    "tech_breakthrough",
    "sports_upset",
    "natural_disaster",
    "crypto_event",
    "absurd_events",
)


# ---------------------------------------------------------------------------
# Step 1 — templates
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", NEW_TEMPLATES)
def test_template_parses(name):
    path = TEMPLATES_DIR / f"{name}.yaml"
    assert path.exists(), f"missing template file: {path}"
    tmpl = fire.load_template(path)
    assert tmpl.name == name
    # title + summary must be non-empty after rendering.
    rendered = tmpl.render(0)
    assert rendered["title"], "rendered title was empty"
    assert rendered["summary"], "rendered summary was empty"
    assert rendered["url"], "rendered url was empty"
    assert rendered["source"] == "tracer"


@pytest.mark.parametrize("name", NEW_TEMPLATES)
def test_template_supports_50_distinct(name):
    path = TEMPLATES_DIR / f"{name}.yaml"
    tmpl = fire.load_template(path)
    # Conservative estimate: distinct ≈ variants × prod(var lengths).
    assert tmpl.estimate_distinct() >= 50, (
        f"template {name} can produce only "
        f"{tmpl.estimate_distinct()} distinct articles (need >= 50)"
    )
    # Empirical: render 50 and assert at least 30 unique titles.
    titles = {tmpl.render(i)["title"] for i in range(50)}
    assert len(titles) >= 10, (
        f"template {name} produced only {len(titles)} unique titles "
        f"across 50 renders"
    )


def test_load_all_templates_includes_new():
    names = {t.name for t in fire.load_all_templates()}
    for n in NEW_TEMPLATES:
        assert n in names, f"load_all_templates() missing {n!r}"
    # Legacy templates must still load.
    for legacy in ("economic_indicator", "merger", "regulatory_decision"):
        assert legacy in names


def test_high_quality_overrides_optional():
    """The high_quality_overrides field should be optional; templates
    without it must still load. The flag should not crash render()."""
    tmpl = fire.load_template(TEMPLATES_DIR / "iran_strike.yaml")
    out = tmpl.render(0, prefer_high_quality=True)
    assert out["title"]
    assert out["summary"]


# ---------------------------------------------------------------------------
# Step 2 / 4.5 — fire.py flags
# ---------------------------------------------------------------------------


def test_fire_accepts_tracer_id_prefix():
    """--tracer-id-prefix flows into the rendered tracer_id column."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        result = fire.run_inproc(
            count=5,
            concurrency=2,
            db_path=tmp.name,
            tracer_id_prefix="unit-prefix-A",
            template="iran_strike",
        )
        assert result.counts.articles == 5
        assert result.drops == 0
        # Re-open the file and confirm tracer_id rows carry our prefix.
        conn = sqlite3.connect(tmp.name)
        cur = conn.execute(
            "SELECT COUNT(*) FROM articles "
            "WHERE tracer_id LIKE 'unit-prefix-A%'"
        )
        n = cur.fetchone()[0]
        cur.close()
        conn.close()
        assert n == 5, f"expected 5 articles with prefix, got {n}"


def test_fire_exercise_wallet_smoke():
    """`prefer_high_quality=True` (the underlying flag) must not crash
    the mock path even if some templates lack high_quality_overrides.
    """
    result = fire.run_inproc(
        count=5,
        concurrency=2,
        tracer_id_prefix="wallet-smoke",
        prefer_high_quality=True,
    )
    assert result.drops == 0
    assert result.counts.articles == 5


def test_fire_default_path_unchanged():
    """Sanity: with no new flags, behaviour matches the cycle-23 contract
    (drops == 0, signals == articles × stub markets, etc)."""
    result = fire.run_inproc(count=10, concurrency=5)
    assert result.drops == 0
    assert result.counts.articles == 10
    assert result.counts.signals == 10 * fire.STUB_MARKET_COUNT


# ---------------------------------------------------------------------------
# Step 3 — audit_burst
# ---------------------------------------------------------------------------


def _seed_burst_and_get_db_path(prefix: str, count: int = 5) -> str:
    """Run a tracer burst to a file-backed DB and return its path."""
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    fire.run_inproc(
        count=count,
        concurrency=2,
        db_path=tmp.name,
        tracer_id_prefix=prefix,
        template="iran_strike",
    )
    return tmp.name


def test_audit_funnel_aggregates_counts():
    db_path = _seed_burst_and_get_db_path("audit-test", count=5)
    try:
        conn = sqlite3.connect(db_path)
        f = audit_burst.funnel(conn, "audit-test")
        assert f.prefix == "audit-test"
        assert f.articles == 5
        assert f.signals == 5 * fire.STUB_MARKET_COUNT
        assert f.decisions == f.signals
        assert f.window_start is not None
        # Stub AI always returns YES — so the verdict map should have YES.
        assert "YES" in f.llm_label_counts
        # All YES verdicts at confidence 0.7.
        assert f.llm_label_avg_conf.get("YES") == pytest.approx(0.7, abs=1e-3)
        conn.close()
    finally:
        Path(db_path).unlink(missing_ok=True)


def test_audit_csv_dump():
    db_path = _seed_burst_and_get_db_path("csv-test", count=3)
    csv_path = Path(tempfile.gettempdir()) / "tracer_burst_csv_test.csv"
    csv_path.unlink(missing_ok=True)
    try:
        conn = sqlite3.connect(db_path)
        n = audit_burst.write_csv(conn, "csv-test", csv_path)
        conn.close()
        assert n == 3 * fire.STUB_MARKET_COUNT
        text = csv_path.read_text()
        # Header sanity.
        assert "tracer_id" in text.splitlines()[0]
        assert "csv-test-" in text
    finally:
        Path(db_path).unlink(missing_ok=True)
        csv_path.unlink(missing_ok=True)


def test_audit_unknown_prefix_returns_zeros():
    db_path = _seed_burst_and_get_db_path("real-prefix", count=2)
    try:
        conn = sqlite3.connect(db_path)
        f = audit_burst.funnel(conn, "no-such-prefix")
        assert f.articles == 0
        assert f.signals == 0
        assert f.decisions == 0
        assert f.trades == 0
        conn.close()
    finally:
        Path(db_path).unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Step 4 — cleanup
# ---------------------------------------------------------------------------


def test_cleanup_requires_prefix_via_main():
    """The CLI must REQUIRE --tracer-id-prefix; running without it
    is a parser error (SystemExit)."""
    with pytest.raises(SystemExit):
        cleanup.main([])


def test_cleanup_dry_run_reports_counts_only():
    db_path = _seed_burst_and_get_db_path("dry-run-test", count=4)
    try:
        conn = sqlite3.connect(db_path)
        plan = cleanup.plan_deletes(conn, "dry-run-test")
        # dry-run never mutates.
        assert plan["articles"] == 4
        assert plan["signals"] == 4 * fire.STUB_MARKET_COUNT
        # Articles still there.
        cur = conn.execute("SELECT COUNT(*) FROM articles WHERE is_tracer=1")
        assert cur.fetchone()[0] == 4
        cur.close()
        conn.close()
    finally:
        Path(db_path).unlink(missing_ok=True)


def test_cleanup_executes_and_is_idempotent():
    db_path = _seed_burst_and_get_db_path("delete-test", count=5)
    try:
        conn = sqlite3.connect(db_path)
        # First execute removes rows.
        counts = cleanup.execute_deletes(conn, "delete-test")
        assert counts["articles"] == 5
        assert counts["signals"] == 5 * fire.STUB_MARKET_COUNT
        # All tracer rows gone.
        for tbl in ("articles", "signals", "decisions"):
            cur = conn.execute(
                f"SELECT COUNT(*) FROM {tbl} WHERE is_tracer = 1"
            )
            assert cur.fetchone()[0] == 0, f"{tbl} still has tracer rows"
            cur.close()
        # Idempotency: second run deletes nothing.
        again = cleanup.execute_deletes(conn, "delete-test")
        total_again = sum(again.values())
        assert total_again == 0, f"second run still deleted: {again}"
        conn.close()
    finally:
        Path(db_path).unlink(missing_ok=True)


def test_cleanup_leaves_other_prefixes_alone():
    """A delete scoped to prefix A must not touch prefix B's rows."""
    db_path = _seed_burst_and_get_db_path("keep-me", count=3)
    try:
        # Layer a second burst with a different prefix on top.
        fire.run_inproc(
            count=2,
            concurrency=1,
            db_path=db_path,
            tracer_id_prefix="wipe-me",
            template="iran_strike",
        )
        conn = sqlite3.connect(db_path)
        # Sanity: both prefixes are present.
        cur = conn.execute(
            "SELECT COUNT(*) FROM articles WHERE tracer_id LIKE 'keep-me%'"
        )
        assert cur.fetchone()[0] == 3
        cur.close()
        cur = conn.execute(
            "SELECT COUNT(*) FROM articles WHERE tracer_id LIKE 'wipe-me%'"
        )
        assert cur.fetchone()[0] == 2
        cur.close()
        # Wipe just one prefix.
        cleanup.execute_deletes(conn, "wipe-me")
        cur = conn.execute(
            "SELECT COUNT(*) FROM articles WHERE tracer_id LIKE 'keep-me%'"
        )
        assert cur.fetchone()[0] == 3, "keep-me prefix was wrongly deleted"
        cur.close()
        cur = conn.execute(
            "SELECT COUNT(*) FROM articles WHERE tracer_id LIKE 'wipe-me%'"
        )
        assert cur.fetchone()[0] == 0
        cur.close()
        conn.close()
    finally:
        Path(db_path).unlink(missing_ok=True)


def test_cleanup_empty_prefix_rejected():
    """Cleanup CLI refuses to act on an empty prefix string."""
    rc = cleanup.main(["--tracer-id-prefix", ""])
    assert rc == 2


# ---------------------------------------------------------------------------
# Step 5 — live-pipeline safety pre-flight (mock plumbing only)
# ---------------------------------------------------------------------------


def test_live_externals_preflight_blocks_concurrent_prefix():
    """If a *different* tracer prefix already wrote into articles in the
    last hour, the pre-flight must raise unless --force is set."""
    import asyncio

    from app.db import DB

    async def _go():
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        try:
            db = DB(path=tmp.name, journal_mode="MEMORY", synchronous="OFF")
            await db.connect()
            await db.ensure_current_cycle("tracer-test")
            # Drop a tracer row under a *different* prefix.
            now = fire._now_iso()
            async with db.batch() as cur:
                await cur.execute(
                    "INSERT INTO articles (source, source_detail, url, "
                    "title, summary, published_at, ingested_at, cycle_id, "
                    "is_tracer, tracer_id) "
                    "VALUES ('tracer','x','http://x/1','t','s',?,?,1,1,?)",
                    (now, now, "other-prefix-1"),
                )
            # Pre-flight with a DIFFERENT prefix should refuse without
            # --force.
            with pytest.raises(RuntimeError):
                await fire._live_externals_preflight(
                    db, tracer_id_prefix="my-prefix", force=False
                )
            # With force=True it succeeds.
            await fire._live_externals_preflight(
                db, tracer_id_prefix="my-prefix", force=True
            )
            await db.close()
        finally:
            Path(tmp.name).unlink(missing_ok=True)

    asyncio.run(_go())


# ---------------------------------------------------------------------------
# Step 7 — centralized campaign mode (cycle 24 follow-up)
# ---------------------------------------------------------------------------


from datetime import datetime, timezone  # noqa: E402

from scripts.tracer import _campaign  # noqa: E402


def test_campaign_parse_template_count_arg():
    b = _campaign.parse_template_count_arg("iran_strike:50")
    assert b.template == "iran_strike"
    assert b.count == 50
    with pytest.raises(ValueError):
        _campaign.parse_template_count_arg("missing-colon")
    with pytest.raises(ValueError):
        _campaign.parse_template_count_arg("iran-strike:50")  # hyphen banned
    with pytest.raises(ValueError):
        _campaign.parse_template_count_arg("iran_strike:0")  # gt=0


def test_campaign_compose_and_parse_tracer_id_roundtrip():
    tid = _campaign.compose_tracer_id("cycle24-root", "iran_strike", 42)
    assert tid == "cycle24-root-iran_strike-42"
    assert _campaign.parse_tracer_id_template(tid, "cycle24-root") == "iran_strike"
    # Legacy form (no template segment) returns None, not error.
    legacy = "cycle24-root-99"
    assert _campaign.parse_tracer_id_template(legacy, "cycle24-root") is None
    # Wrong root returns None.
    assert _campaign.parse_tracer_id_template(tid, "other-root") is None


def test_campaign_resolve_prefix_root_substitutes_ts():
    out = _campaign.resolve_prefix_root(
        "cycle-{ts}",
        now=datetime(2026, 5, 26, 19, 8, tzinfo=timezone.utc),
    )
    assert out == "cycle-20260526T1908"
    # No placeholder → identity.
    assert _campaign.resolve_prefix_root("plain-root") == "plain-root"


def test_campaign_from_args_multi_template():
    spec = _campaign.campaign_from_args(
        template_args=["iran_strike:50", "market_crash:30"],
        count_fallback=5,
        prefix_root="test-root",
    )
    assert spec.total_count() == 80
    assert [b.template for b in spec.bursts] == ["iran_strike", "market_crash"]
    # Bare name (no colon) uses count_fallback.
    spec2 = _campaign.campaign_from_args(
        template_args=["iran_strike"],
        count_fallback=7,
        prefix_root="r",
    )
    assert spec2.bursts[0].count == 7


def test_campaign_load_campaign_yaml(tmp_path):
    p = tmp_path / "campaign.yaml"
    p.write_text(
        "name: testc\n"
        "prefix_root: testc-root\n"
        "shared: {concurrency: 2}\n"
        "bursts:\n"
        "  - template: iran_strike\n"
        "    count: 3\n"
        "  - template: market_crash\n"
        "    count: 2\n"
    )
    spec = _campaign.load_campaign(p)
    assert spec.name == "testc"
    assert spec.shared["concurrency"] == 2
    assert spec.total_count() == 5
    assert spec.bursts[1].template == "market_crash"


def test_run_campaign_inproc_two_templates():
    spec = _campaign.CampaignSpec(
        name="t",
        prefix_root="run-campaign-test",
        bursts=[
            _campaign.BurstSpec(template="iran_strike", count=3),
            _campaign.BurstSpec(template="market_crash", count=2),
        ],
    )
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        result = fire.run_campaign_inproc(
            spec,
            concurrency=2,
            db_path=tmp.name,
            timeout_seconds=15.0,
        )
        assert result.run.drops == 0
        assert result.run.counts.articles == 5
        assert result.per_burst_requested == {"iran_strike": 3, "market_crash": 2}
        assert result.per_burst_published == {"iran_strike": 3, "market_crash": 2}
        # Tracer_ids carry the template middle segment.
        conn = sqlite3.connect(tmp.name)
        rows = list(
            conn.execute(
                "SELECT tracer_id FROM articles "
                "WHERE is_tracer=1 AND tracer_id LIKE 'run-campaign-test%' "
                "ORDER BY id"
            )
        )
        conn.close()
        templates_seen = set()
        for (tid,) in rows:
            t = _campaign.parse_tracer_id_template(tid, "run-campaign-test")
            assert t in {"iran_strike", "market_crash"}
            templates_seen.add(t)
        assert templates_seen == {"iran_strike", "market_crash"}


def test_audit_funnel_by_template_groups_correctly():
    spec = _campaign.CampaignSpec(
        name="t",
        prefix_root="audit-grouped-test",
        bursts=[
            _campaign.BurstSpec(template="iran_strike", count=4),
            _campaign.BurstSpec(template="market_crash", count=2),
        ],
    )
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    try:
        fire.run_campaign_inproc(
            spec, concurrency=2, db_path=tmp.name, timeout_seconds=15.0
        )
        conn = sqlite3.connect(tmp.name)
        grouped = audit_burst.funnel_by_template(conn, "audit-grouped-test")
        conn.close()
        assert set(grouped) == {"iran_strike", "market_crash"}
        assert grouped["iran_strike"].articles == 4
        assert grouped["market_crash"].articles == 2
        # Each template's per-funnel signals = articles × stub_market_count.
        assert (
            grouped["iran_strike"].signals
            == 4 * fire.STUB_MARKET_COUNT
        )
        # Discover-templates utility returns the same set.
        conn2 = sqlite3.connect(tmp.name)
        discovered = audit_burst.discover_templates(conn2, "audit-grouped-test")
        conn2.close()
        assert sorted(discovered) == ["iran_strike", "market_crash"]
    finally:
        Path(tmp.name).unlink(missing_ok=True)


def test_cli_multi_template_flag_triggers_campaign_mode(tmp_path):
    """End-to-end CLI smoke: `--template a:N --template b:M` lowers into
    campaign mode and produces the expected tracer_id middle segments.
    """
    db = tmp_path / "smoke.db"
    rc = fire.main(
        [
            "--template", "iran_strike:3",
            "--template", "market_crash:2",
            "--tracer-id-prefix", "cli-smoke",
            "--db", str(db),
            "--concurrency", "2",
            "--timeout-seconds", "15",
        ]
    )
    assert rc == 0
    conn = sqlite3.connect(db)
    rows = list(
        conn.execute(
            "SELECT tracer_id FROM articles WHERE tracer_id LIKE 'cli-smoke%'"
        )
    )
    conn.close()
    assert len(rows) == 5
    tmpls = {
        _campaign.parse_tracer_id_template(tid, "cli-smoke") for (tid,) in rows
    }
    assert tmpls == {"iran_strike", "market_crash"}


def test_cli_campaign_yaml_path(tmp_path):
    """`--campaign <yaml>` is honored by main()."""
    spec_path = tmp_path / "c.yaml"
    spec_path.write_text(
        "name: cli-yaml\n"
        "prefix_root: cli-yaml-root\n"
        "shared: {concurrency: 2}\n"
        "bursts:\n"
        "  - template: iran_strike\n"
        "    count: 2\n"
    )
    db = tmp_path / "y.db"
    rc = fire.main(
        [
            "--campaign", str(spec_path),
            "--db", str(db),
            "--concurrency", "2",
            "--timeout-seconds", "15",
        ]
    )
    assert rc == 0
    conn = sqlite3.connect(db)
    n = conn.execute(
        "SELECT COUNT(*) FROM articles WHERE tracer_id LIKE 'cli-yaml-root%'"
    ).fetchone()[0]
    conn.close()
    assert n == 2
