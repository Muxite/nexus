"""ResolveStage tests — the deterministic rule chain exercised against
a real in-memory DB with hand-seeded entities/facts. Extraction layer
is bypassed (these tests validate `resolve.py`, not `extract.py`)."""

from __future__ import annotations

import json
from dataclasses import dataclass

import pytest

# Import DDL-bearing modules so register_schema() fires before connect().
import app.main  # noqa: F401 — register cycles DDL
import app.stages.dedupe  # noqa: F401
import app.stages.extract  # noqa: F401 — owns entities/facts/aliases tables
import app.stages.ingest  # noqa: F401
import app.stages.resolve  # noqa: F401 — owns entity_resolutions
from app.db import DB
from app.deps import StageDeps
from app.stages.resolve import ResolveStage
from app.topics import TOPIC_RESOLUTION_COMPLETED


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeBus:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict]] = []

    async def publish(self, topic: str, payload: dict) -> None:
        json.loads(json.dumps(payload))  # enforce JSON-only payload (rule 3)
        self.published.append((topic, payload))

    def subscribe(self, topic: str, handler) -> None:  # noqa: ARG002
        pass

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


@dataclass
class _Config:
    """ResolveStage doesn't read config; an empty shim satisfies StageDeps."""


# ---------------------------------------------------------------------------
# Seeding helpers — raw SQL so the test isn't entangled with extract.py
# ---------------------------------------------------------------------------


async def _insert_entity(
    db: DB, *, entity_type: str, canonical_name: str
) -> int:
    async with db.batch() as cur:
        await cur.execute(
            """
            INSERT INTO entities
                (entity_type, canonical_name, first_seen_at, last_seen_at)
            VALUES (?, ?, '2026-05-23T00:00:00+00:00', '2026-05-23T00:00:00+00:00')
            """,
            (entity_type, canonical_name),
        )
        await cur.execute("SELECT last_insert_rowid()")
        row = await cur.fetchone()
    return int(row[0])


async def _insert_alias(db: DB, *, entity_id: int, alias: str) -> None:
    async with db.batch() as cur:
        await cur.execute(
            """
            INSERT INTO entity_aliases (entity_id, alias, alias_kind)
            VALUES (?, ?, 'synonym')
            """,
            (entity_id, alias),
        )


async def _insert_article(db: DB, *, title: str = "t") -> int:
    async with db.batch() as cur:
        await cur.execute(
            """
            INSERT INTO articles
                (source, source_detail, url, title, summary, published_at,
                 ingested_at, cycle_id, is_tracer, tracer_id)
            VALUES ('rss','t',?,?,?, '2026-05-23T00:00:00+00:00',
                    '2026-05-23T00:00:00+00:00', 1, 0, NULL)
            """,
            (f"https://x/{title}", title, ""),
        )
        await cur.execute("SELECT last_insert_rowid()")
        row = await cur.fetchone()
    return int(row[0])


async def _insert_extraction_run(db: DB, *, article_id: int) -> int:
    async with db.batch() as cur:
        await cur.execute(
            """
            INSERT INTO extraction_runs
                (article_id, model, prompt_version, ontology_version, status,
                 fact_count, entity_count, dropped_fact_count, cycle_id,
                 is_tracer, tracer_id, created_at)
            VALUES (?, 'm', 'v1', 'v1', 'ok', 0, 0, 0, 1, 0, NULL,
                    '2026-05-23T00:00:00+00:00')
            """,
            (article_id,),
        )
        await cur.execute("SELECT last_insert_rowid()")
        row = await cur.fetchone()
    return int(row[0])


async def _insert_fact(
    db: DB,
    *,
    run_id: int,
    article_id: int,
    subj_id: int,
    obj_id: int | None = None,
    relation: str = "announces",
) -> int:
    async with db.batch() as cur:
        await cur.execute(
            """
            INSERT INTO facts
                (extraction_run_id, article_id, subj_entity_id, relation,
                 obj_entity_id, obj_value, confidence, observed_at,
                 evidence_span, cycle_id, is_tracer, tracer_id)
            VALUES (?, ?, ?, ?, ?, NULL, 0.9, '2026-05-23T00:00:00+00:00',
                    'evidence', 1, 0, NULL)
            """,
            (run_id, article_id, subj_id, relation, obj_id),
        )
        await cur.execute("SELECT last_insert_rowid()")
        row = await cur.fetchone()
    return int(row[0])


def _payload(*, run_id: int, article_id: int, status: str = "ok",
             is_tracer: bool = False, tracer_id: str | None = None) -> dict:
    return {
        "extraction_run_id": run_id,
        "article_id": article_id,
        "status": status,
        "fact_count": 1,
        "entity_count": 1,
        "is_tracer": is_tracer,
        "tracer_id": tracer_id,
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def db_with_extract_state():
    """In-memory DB with the full P2 schema applied. Tests seed
    entities + facts directly; ResolveStage operates on that state."""
    db = DB.ephemeral()
    await db.connect()
    try:
        yield db
    finally:
        await db.close()


async def _make_stage(db: DB, bus: _FakeBus) -> ResolveStage:
    deps = StageDeps(db=db, bus=bus, config=_Config())
    stage = ResolveStage(deps)
    await stage.start()
    return stage


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_ddl_creates_entity_resolutions_table(db_with_extract_state):
    cur = await db_with_extract_state.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name='entity_resolutions'"
    )
    row = await cur.fetchone()
    await cur.close()
    assert row is not None and row["name"] == "entity_resolutions"


async def test_handle_skips_when_status_not_ok(db_with_extract_state):
    db = db_with_extract_state
    bus = _FakeBus()
    stage = await _make_stage(db, bus)
    article_id = await _insert_article(db)
    run_id = await _insert_extraction_run(db, article_id=article_id)

    await stage.handle(_payload(run_id=run_id, article_id=article_id,
                                status="parse_error"))

    cur = await db.conn.execute("SELECT COUNT(*) AS c FROM entity_resolutions")
    assert (await cur.fetchone())["c"] == 0
    assert bus.published == []


async def test_self_resolution_when_no_match(db_with_extract_state):
    db = db_with_extract_state
    bus = _FakeBus()
    stage = await _make_stage(db, bus)
    article_id = await _insert_article(db)
    run_id = await _insert_extraction_run(db, article_id=article_id)
    eid = await _insert_entity(db, entity_type="COMPANY",
                               canonical_name="Lonelycorp")
    await _insert_fact(db, run_id=run_id, article_id=article_id, subj_id=eid)

    await stage.handle(_payload(run_id=run_id, article_id=article_id))

    cur = await db.conn.execute(
        "SELECT entity_id, resolved_to_id, resolution_kind, confidence "
        "FROM entity_resolutions"
    )
    rows = await cur.fetchall()
    assert len(rows) == 1
    r = rows[0]
    assert r["entity_id"] == eid
    assert r["resolved_to_id"] == eid
    assert r["resolution_kind"] == "self"
    assert r["confidence"] == 1.0


async def test_acronym_merge_both_directions(db_with_extract_state):
    db = db_with_extract_state
    bus = _FakeBus()
    stage = await _make_stage(db, bus)
    article_id = await _insert_article(db)
    run_id = await _insert_extraction_run(db, article_id=article_id)
    # Both forms exist; both are referenced by facts of this article.
    fda_id = await _insert_entity(db, entity_type="ORG", canonical_name="FDA")
    long_id = await _insert_entity(
        db, entity_type="ORG",
        canonical_name="U.S. Food and Drug Administration",
    )
    await _insert_fact(db, run_id=run_id, article_id=article_id, subj_id=fda_id)
    await _insert_fact(db, run_id=run_id, article_id=article_id, subj_id=long_id)

    await stage.handle(_payload(run_id=run_id, article_id=article_id))

    cur = await db.conn.execute(
        "SELECT entity_id, resolved_to_id, resolution_kind "
        "FROM entity_resolutions ORDER BY entity_id"
    )
    rows = await cur.fetchall()
    by_eid = {r["entity_id"]: dict(r) for r in rows}
    # Lower id wins for determinism → both rows point to fda_id (inserted first).
    assert by_eid[fda_id]["resolved_to_id"] == fda_id
    assert by_eid[fda_id]["resolution_kind"] in {"self", "acronym"}
    assert by_eid[long_id]["resolved_to_id"] == fda_id
    assert by_eid[long_id]["resolution_kind"] == "acronym"


async def test_acronym_self_when_only_one_form_present(db_with_extract_state):
    db = db_with_extract_state
    bus = _FakeBus()
    stage = await _make_stage(db, bus)
    article_id = await _insert_article(db)
    run_id = await _insert_extraction_run(db, article_id=article_id)
    fda_id = await _insert_entity(db, entity_type="ORG", canonical_name="FDA")
    await _insert_fact(db, run_id=run_id, article_id=article_id, subj_id=fda_id)

    await stage.handle(_payload(run_id=run_id, article_id=article_id))

    cur = await db.conn.execute(
        "SELECT resolution_kind, resolved_to_id FROM entity_resolutions"
    )
    rows = await cur.fetchall()
    assert len(rows) == 1
    assert rows[0]["resolution_kind"] == "self"
    assert rows[0]["resolved_to_id"] == fda_id


async def test_acronym_match_case_insensitive(db_with_extract_state):
    db = db_with_extract_state
    bus = _FakeBus()
    stage = await _make_stage(db, bus)
    article_id = await _insert_article(db)
    run_id = await _insert_extraction_run(db, article_id=article_id)
    low_id = await _insert_entity(db, entity_type="ORG", canonical_name="fda")
    long_id = await _insert_entity(
        db, entity_type="ORG",
        canonical_name="U.S. Food and Drug Administration",
    )
    await _insert_fact(db, run_id=run_id, article_id=article_id, subj_id=low_id)
    await _insert_fact(db, run_id=run_id, article_id=article_id, subj_id=long_id)

    await stage.handle(_payload(run_id=run_id, article_id=article_id))

    cur = await db.conn.execute(
        "SELECT entity_id, resolved_to_id, resolution_kind "
        "FROM entity_resolutions ORDER BY entity_id"
    )
    rows = await cur.fetchall()
    by_eid = {r["entity_id"]: dict(r) for r in rows}
    assert by_eid[long_id]["resolved_to_id"] == low_id
    assert by_eid[long_id]["resolution_kind"] == "acronym"


async def test_acronym_never_crosses_entity_type(db_with_extract_state):
    db = db_with_extract_state
    bus = _FakeBus()
    stage = await _make_stage(db, bus)
    article_id = await _insert_article(db)
    run_id = await _insert_extraction_run(db, article_id=article_id)
    org_fda = await _insert_entity(db, entity_type="ORG", canonical_name="FDA")
    # A COMPANY happening to share the surface form must NOT collide.
    co_fda = await _insert_entity(
        db, entity_type="COMPANY", canonical_name="FDA Pharma",
    )
    await _insert_fact(db, run_id=run_id, article_id=article_id, subj_id=org_fda)
    await _insert_fact(db, run_id=run_id, article_id=article_id, subj_id=co_fda)

    await stage.handle(_payload(run_id=run_id, article_id=article_id))

    cur = await db.conn.execute(
        "SELECT entity_id, resolved_to_id, resolution_kind "
        "FROM entity_resolutions ORDER BY entity_id"
    )
    rows = {r["entity_id"]: dict(r) for r in await cur.fetchall()}
    # ORG FDA self-resolves (the long form is not present).
    assert rows[org_fda]["resolution_kind"] == "self"
    assert rows[org_fda]["resolved_to_id"] == org_fda
    # COMPANY FDA Pharma self-resolves (acronym table entry is ORG-typed).
    assert rows[co_fda]["resolution_kind"] == "self"
    assert rows[co_fda]["resolved_to_id"] == co_fda


async def test_alias_merge(db_with_extract_state):
    db = db_with_extract_state
    bus = _FakeBus()
    stage = await _make_stage(db, bus)
    article_id = await _insert_article(db)
    run_id = await _insert_extraction_run(db, article_id=article_id)
    a_id = await _insert_entity(db, entity_type="COMPANY", canonical_name="A Holdings")
    b_id = await _insert_entity(db, entity_type="COMPANY", canonical_name="FOO")
    await _insert_alias(db, entity_id=a_id, alias="FOO")
    await _insert_fact(db, run_id=run_id, article_id=article_id, subj_id=a_id)
    await _insert_fact(db, run_id=run_id, article_id=article_id, subj_id=b_id)

    await stage.handle(_payload(run_id=run_id, article_id=article_id))

    cur = await db.conn.execute(
        "SELECT entity_id, resolved_to_id, resolution_kind "
        "FROM entity_resolutions WHERE resolution_kind = 'alias'"
    )
    rows = await cur.fetchall()
    # At least one of A↔B resolves via 'alias'. Both directions are valid:
    # B's canonical equals A's alias.
    assert len(rows) >= 1
    pair = {a_id, b_id}
    for r in rows:
        assert r["entity_id"] in pair
        assert r["resolved_to_id"] in pair
        assert r["resolved_to_id"] != r["entity_id"]


async def test_alias_skipped_on_ambiguity(db_with_extract_state):
    db = db_with_extract_state
    bus = _FakeBus()
    stage = await _make_stage(db, bus)
    article_id = await _insert_article(db)
    run_id = await _insert_extraction_run(db, article_id=article_id)
    target = await _insert_entity(
        db, entity_type="ORG", canonical_name="Ambig",
    )
    # Three OTHER entities all carry the same alias "AMBIG" — when the
    # resolver looks at `target`, it sees three sibling candidates and
    # must fall through to self.
    sib1 = await _insert_entity(db, entity_type="ORG", canonical_name="Sib1")
    sib2 = await _insert_entity(db, entity_type="ORG", canonical_name="Sib2")
    sib3 = await _insert_entity(db, entity_type="ORG", canonical_name="Sib3")
    for sid in (sib1, sib2, sib3):
        await _insert_alias(db, entity_id=sid, alias="AMBIG")
    await _insert_fact(db, run_id=run_id, article_id=article_id, subj_id=target)

    await stage.handle(_payload(run_id=run_id, article_id=article_id))

    cur = await db.conn.execute(
        "SELECT resolution_kind, resolved_to_id "
        "FROM entity_resolutions WHERE entity_id = ?",
        (target,),
    )
    row = await cur.fetchone()
    assert row["resolution_kind"] == "self"
    assert row["resolved_to_id"] == target


async def test_suffix_merge_company(db_with_extract_state):
    db = db_with_extract_state
    bus = _FakeBus()
    stage = await _make_stage(db, bus)
    article_id = await _insert_article(db)
    run_id = await _insert_extraction_run(db, article_id=article_id)
    short = await _insert_entity(db, entity_type="COMPANY", canonical_name="Acme")
    long_ = await _insert_entity(
        db, entity_type="COMPANY", canonical_name="Acme Holdings",
    )
    await _insert_fact(db, run_id=run_id, article_id=article_id, subj_id=long_)
    await _insert_fact(db, run_id=run_id, article_id=article_id, subj_id=short)

    await stage.handle(_payload(run_id=run_id, article_id=article_id))

    cur = await db.conn.execute(
        "SELECT entity_id, resolved_to_id, resolution_kind "
        "FROM entity_resolutions WHERE entity_id = ?",
        (long_,),
    )
    row = await cur.fetchone()
    assert row["resolved_to_id"] == short
    assert row["resolution_kind"] == "suffix"


async def test_suffix_never_crosses_entity_type(db_with_extract_state):
    db = db_with_extract_state
    bus = _FakeBus()
    stage = await _make_stage(db, bus)
    article_id = await _insert_article(db)
    run_id = await _insert_extraction_run(db, article_id=article_id)
    # ORG (not COMPANY) with a "Group" suffix must NOT trigger suffix-strip,
    # because the rule is COMPANY-only.
    grouped = await _insert_entity(
        db, entity_type="ORG", canonical_name="Stuff Group",
    )
    bare = await _insert_entity(db, entity_type="ORG", canonical_name="Stuff")
    await _insert_fact(db, run_id=run_id, article_id=article_id, subj_id=grouped)
    await _insert_fact(db, run_id=run_id, article_id=article_id, subj_id=bare)

    await stage.handle(_payload(run_id=run_id, article_id=article_id))

    cur = await db.conn.execute(
        "SELECT entity_id, resolution_kind FROM entity_resolutions"
    )
    rows = {r["entity_id"]: r["resolution_kind"] for r in await cur.fetchall()}
    assert rows[grouped] == "self"
    assert rows[bare] == "self"


async def test_idempotent_on_repeat_handle(db_with_extract_state):
    db = db_with_extract_state
    bus = _FakeBus()
    stage = await _make_stage(db, bus)
    article_id = await _insert_article(db)
    run_id = await _insert_extraction_run(db, article_id=article_id)
    eid = await _insert_entity(db, entity_type="ORG", canonical_name="Singleton")
    await _insert_fact(db, run_id=run_id, article_id=article_id, subj_id=eid)

    payload = _payload(run_id=run_id, article_id=article_id)
    await stage.handle(payload)
    await stage.handle(payload)
    await stage.handle(payload)

    cur = await db.conn.execute(
        "SELECT COUNT(*) AS c FROM entity_resolutions WHERE entity_id = ?",
        (eid,),
    )
    assert (await cur.fetchone())["c"] == 1


async def test_unique_constraint_enforces_idempotency(db_with_extract_state):
    db = db_with_extract_state
    bus = _FakeBus()
    stage = await _make_stage(db, bus)
    article_id = await _insert_article(db)
    run_id = await _insert_extraction_run(db, article_id=article_id)
    eid = await _insert_entity(db, entity_type="ORG", canonical_name="OnlyOne")
    await _insert_fact(db, run_id=run_id, article_id=article_id, subj_id=eid)
    await stage.handle(_payload(run_id=run_id, article_id=article_id))

    # Hand-crafted second insert should be silently ignored by INSERT OR
    # IGNORE on the UNIQUE(entity_id). A bare INSERT would raise; this
    # check confirms the trigger is UNIQUE, not absence-checking.
    async with db.batch() as cur:
        await cur.execute(
            """
            INSERT OR IGNORE INTO entity_resolutions
                (entity_id, resolved_to_id, resolution_kind, confidence,
                 resolver_version, created_at)
            VALUES (?, ?, 'manual', 1.0, 'v999', '2099-01-01')
            """,
            (eid, eid),
        )
    cur = await db.conn.execute(
        "SELECT COUNT(*) AS c FROM entity_resolutions WHERE entity_id = ?",
        (eid,),
    )
    assert (await cur.fetchone())["c"] == 1


async def test_bus_publish_on_success(db_with_extract_state):
    db = db_with_extract_state
    bus = _FakeBus()
    stage = await _make_stage(db, bus)
    article_id = await _insert_article(db)
    run_id = await _insert_extraction_run(db, article_id=article_id)
    fda = await _insert_entity(db, entity_type="ORG", canonical_name="FDA")
    long_ = await _insert_entity(
        db, entity_type="ORG",
        canonical_name="U.S. Food and Drug Administration",
    )
    await _insert_fact(db, run_id=run_id, article_id=article_id, subj_id=fda)
    await _insert_fact(db, run_id=run_id, article_id=article_id, subj_id=long_)

    await stage.handle(_payload(run_id=run_id, article_id=article_id,
                                is_tracer=True, tracer_id="trace-1"))

    assert len(bus.published) == 1
    topic, payload = bus.published[0]
    assert topic == TOPIC_RESOLUTION_COMPLETED
    assert payload["extraction_run_id"] == run_id
    assert payload["article_id"] == article_id
    assert payload["entities_resolved"] == 2
    assert payload["merges"] == 1
    assert payload["is_tracer"] is True
    assert payload["tracer_id"] == "trace-1"


async def test_no_writes_outside_entity_resolutions(db_with_extract_state):
    db = db_with_extract_state
    bus = _FakeBus()
    stage = await _make_stage(db, bus)
    article_id = await _insert_article(db)
    run_id = await _insert_extraction_run(db, article_id=article_id)
    fda = await _insert_entity(db, entity_type="ORG", canonical_name="FDA")
    long_ = await _insert_entity(
        db, entity_type="ORG",
        canonical_name="U.S. Food and Drug Administration",
    )
    await _insert_alias(db, entity_id=fda, alias="food and drug")
    await _insert_fact(db, run_id=run_id, article_id=article_id, subj_id=fda)
    await _insert_fact(db, run_id=run_id, article_id=article_id, subj_id=long_)

    async def _counts() -> dict[str, int]:
        out = {}
        for tbl in ("entities", "entity_aliases", "facts", "extraction_runs"):
            cur = await db.conn.execute(f"SELECT COUNT(*) AS c FROM {tbl}")
            out[tbl] = (await cur.fetchone())["c"]
            await cur.close()
        return out

    before = await _counts()
    await stage.handle(_payload(run_id=run_id, article_id=article_id))
    after = await _counts()
    assert before == after


async def test_confidence_always_one(db_with_extract_state):
    db = db_with_extract_state
    bus = _FakeBus()
    stage = await _make_stage(db, bus)
    article_id = await _insert_article(db)
    run_id = await _insert_extraction_run(db, article_id=article_id)
    # Mix of all rule kinds in one batch.
    self_id = await _insert_entity(db, entity_type="EVENT",
                                   canonical_name="Loner")
    fda = await _insert_entity(db, entity_type="ORG", canonical_name="FDA")
    long_ = await _insert_entity(
        db, entity_type="ORG",
        canonical_name="U.S. Food and Drug Administration",
    )
    short = await _insert_entity(db, entity_type="COMPANY", canonical_name="Acme")
    longco = await _insert_entity(
        db, entity_type="COMPANY", canonical_name="Acme Holdings",
    )
    for eid in (self_id, fda, long_, short, longco):
        await _insert_fact(db, run_id=run_id, article_id=article_id, subj_id=eid)

    await stage.handle(_payload(run_id=run_id, article_id=article_id))

    cur = await db.conn.execute(
        "SELECT DISTINCT confidence FROM entity_resolutions"
    )
    rows = await cur.fetchall()
    assert [r["confidence"] for r in rows] == [1.0]


async def test_resolver_version_stamped(db_with_extract_state):
    db = db_with_extract_state
    bus = _FakeBus()
    stage = await _make_stage(db, bus)
    article_id = await _insert_article(db)
    run_id = await _insert_extraction_run(db, article_id=article_id)
    eid = await _insert_entity(db, entity_type="ORG", canonical_name="Solo")
    await _insert_fact(db, run_id=run_id, article_id=article_id, subj_id=eid)

    await stage.handle(_payload(run_id=run_id, article_id=article_id))

    cur = await db.conn.execute(
        "SELECT resolver_version FROM entity_resolutions WHERE entity_id = ?",
        (eid,),
    )
    row = await cur.fetchone()
    assert row["resolver_version"] == ResolveStage.RESOLVER_VERSION
