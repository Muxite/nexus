"""ExtractStage tests — every branch of `handle()` exercised against an
in-memory DB. The OpenRouter HTTP layer is mocked by patching
`_call_openrouter` on the instance (same idiom as test_ai.py); a
FakeBudget mirrors test_ai.py's; a FakeBus collects publishes."""

from __future__ import annotations

import json
from dataclasses import dataclass, field

import pytest

# Import DDL-bearing modules so register_schema() fires before db.connect().
import app.ai  # noqa: F401
import app.main  # noqa: F401 — register cycles DDL
import app.stages.dedupe  # noqa: F401
import app.stages.ingest  # noqa: F401
import app.stages.extract  # noqa: F401
from app.cycle import cycle_scope
from app.db import DB
from app.deps import StageDeps
from app.stages.extract import ExtractStage
from app.topics import TOPIC_EXTRACTION_COMPLETED


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


@dataclass
class _FakeBudget:
    paused: bool = False
    ok_count: int = 0
    f402_count: int = 0

    def is_paused(self) -> bool:
        return self.paused

    def note_ok(self) -> None:
        self.ok_count += 1

    def note_402(self) -> None:
        self.f402_count += 1


class _FakeBus:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict]] = []

    async def publish(self, topic: str, payload: dict) -> None:
        # Round-trip the payload to enforce JSON-only contract (rule 3).
        json.loads(json.dumps(payload))
        self.published.append((topic, payload))

    def subscribe(self, topic: str, handler) -> None:  # noqa: ARG002
        pass

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


@dataclass
class _Config:
    """Minimal Config-shaped object. ExtractStage only reads .extract and
    .openrouter — the other sections are left undefined intentionally so
    a future refactor that reaches into them trips this fixture."""

    class _Extract:
        model = "test-model"
        timeout_seconds = 5.0
        max_tokens = 256
        max_article_chars = 4000

    class _OR:
        api_key = "test-key"

    extract: _Extract = field(default_factory=_Extract)
    openrouter: _OR = field(default_factory=_OR)


# ---------------------------------------------------------------------------
# Helpers — shape an OpenRouter response with embedded extraction JSON
# ---------------------------------------------------------------------------


def _ok_response(inner: dict) -> dict:
    """Wrap an extraction-shaped dict in an OpenRouter chat-completion body."""
    inner_json = json.dumps(inner)
    body = json.dumps(
        {
            "choices": [{"message": {"content": inner_json}}],
            "usage": {"prompt_tokens": 30, "completion_tokens": 12},
        }
    )
    return {
        "status_code": 200,
        "body": body,
        "latency_ms": 11.1,
        "tokens_in": 30,
        "tokens_out": 12,
        "error": None,
    }


def _bad_outer_response() -> dict:
    """A 200 whose content isn't JSON at all."""
    body = json.dumps({"choices": [{"message": {"content": "totally not json"}}]})
    return {
        "status_code": 200, "body": body, "latency_ms": 1.0,
        "tokens_in": 1, "tokens_out": 1, "error": None,
    }


def _http_status_response(code: int) -> dict:
    return {
        "status_code": code, "body": f'{{"error":"status {code}"}}',
        "latency_ms": 1.0, "tokens_in": None, "tokens_out": None, "error": None,
    }


def _network_error_response() -> dict:
    return {
        "status_code": 0, "body": "", "latency_ms": 1.0,
        "tokens_in": None, "tokens_out": None,
        "error": "http_error: ConnectError",
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def db_ready():
    db = DB.ephemeral()
    await db.connect()
    try:
        yield db
    finally:
        await db.close()


async def _insert_article(db: DB, *, title: str, summary: str = "") -> int:
    async with db.batch() as cur:
        await cur.execute(
            """
            INSERT INTO articles
                (source, source_detail, url, title, summary, published_at,
                 ingested_at, cycle_id, is_tracer, tracer_id)
            VALUES ('rss','test',?,?,?,?,?,1,0,NULL)
            """,
            (
                f"https://x/{title}",
                title,
                summary,
                "2026-05-23T00:00:00+00:00",
                "2026-05-23T00:00:00+00:00",
            ),
        )
        await cur.execute("SELECT last_insert_rowid()")
        row = await cur.fetchone()
    return int(row[0])


def _make_stage(db, bus, budget=None) -> ExtractStage:
    budget = budget or _FakeBudget()
    deps = StageDeps(
        db=db,
        bus=bus,
        config=_Config(),
        budget=budget,
    )
    return ExtractStage(deps)


def _attach_mock(stage: ExtractStage, response: dict, monkeypatch) -> None:
    async def stub(prompt: str) -> dict:  # noqa: ARG001
        return response

    monkeypatch.setattr(stage, "_call_openrouter", stub)


def _attach_raising_mock(stage, monkeypatch) -> None:
    async def stub(prompt: str) -> dict:  # noqa: ARG001
        raise AssertionError("HTTP path must not be called in this test")

    monkeypatch.setattr(stage, "_call_openrouter", stub)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_ddl_creates_all_four_tables(db_ready):
    cur = await db_ready.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name IN ('entities','entity_aliases','extraction_runs','facts')"
    )
    names = {r["name"] for r in await cur.fetchall()}
    await cur.close()
    assert names == {"entities", "entity_aliases", "extraction_runs", "facts"}


async def test_init_requires_budget(db_ready):
    bus = _FakeBus()
    with pytest.raises(RuntimeError, match="budget"):
        ExtractStage(
            StageDeps(db=db_ready, bus=bus, config=_Config(), budget=None)
        )


async def test_handle_happy_path_writes_entity_and_fact(db_ready, monkeypatch):
    bus = _FakeBus()
    budget = _FakeBudget()
    stage = _make_stage(db_ready, bus, budget)
    article_id = await _insert_article(db_ready, title="FDA decides on Pfizer")
    inner = {
        "entities": [
            {"name": "FDA", "type": "ORG", "aliases": []},
            {"name": "Pfizer", "type": "COMPANY", "aliases": ["PFE"]},
        ],
        "facts": [
            {
                "subj": "FDA", "relation": "decides", "obj": "Pfizer",
                "obj_value": None, "confidence": 0.85,
                "evidence_span": "the FDA today rejected Pfizer's application",
            }
        ],
    }
    _attach_mock(stage, _ok_response(inner), monkeypatch)

    with cycle_scope(1):
        await stage.handle({"article_id": article_id, "cluster_id": 1,
                            "freshness": "BREAKING",
                            "is_tracer": False, "tracer_id": None})

    cur = await db_ready.conn.execute(
        "SELECT status, fact_count, entity_count, dropped_fact_count "
        "FROM extraction_runs WHERE article_id=?", (article_id,))
    row = await cur.fetchone()
    assert row["status"] == "ok"
    assert row["fact_count"] == 1
    assert row["entity_count"] == 2
    assert row["dropped_fact_count"] == 0

    cur = await db_ready.conn.execute("SELECT COUNT(*) FROM entities")
    assert (await cur.fetchone())[0] == 2
    cur = await db_ready.conn.execute("SELECT COUNT(*) FROM facts")
    assert (await cur.fetchone())[0] == 1
    cur = await db_ready.conn.execute(
        "SELECT alias FROM entity_aliases WHERE alias='PFE'"
    )
    assert (await cur.fetchone())["alias"] == "PFE"
    assert budget.ok_count == 1
    assert any(t == TOPIC_EXTRACTION_COMPLETED for t, _ in bus.published)
    payload = bus.published[0][1]
    assert payload["status"] == "ok"
    assert payload["fact_count"] == 1
    assert payload["entity_count"] == 2


async def test_entity_dedupe_across_articles(db_ready, monkeypatch):
    bus = _FakeBus()
    stage = _make_stage(db_ready, bus)
    a1 = await _insert_article(db_ready, title="FDA acts 1")
    a2 = await _insert_article(db_ready, title="FDA acts 2")
    inner = {
        "entities": [
            {"name": "FDA", "type": "ORG", "aliases": []},
            {"name": "Pfizer", "type": "COMPANY", "aliases": []},
        ],
        "facts": [
            {"subj": "FDA", "relation": "decides", "obj": "Pfizer",
             "obj_value": None, "confidence": 0.7,
             "evidence_span": "FDA decided on Pfizer"},
        ],
    }
    _attach_mock(stage, _ok_response(inner), monkeypatch)

    with cycle_scope(1):
        await stage.handle({"article_id": a1})
        await stage.handle({"article_id": a2})

    cur = await db_ready.conn.execute(
        "SELECT entity_type, canonical_name FROM entities ORDER BY id"
    )
    rows = await cur.fetchall()
    assert [(r["entity_type"], r["canonical_name"]) for r in rows] == [
        ("ORG", "FDA"), ("COMPANY", "Pfizer"),
    ]
    cur = await db_ready.conn.execute("SELECT COUNT(*) FROM facts")
    assert (await cur.fetchone())[0] == 2


async def test_idempotent_rerun_on_same_article(db_ready, monkeypatch):
    bus = _FakeBus()
    stage = _make_stage(db_ready, bus)
    article_id = await _insert_article(db_ready, title="Same again")
    inner = {
        "entities": [{"name": "FDA", "type": "ORG", "aliases": []}],
        "facts": [
            {"subj": "FDA", "relation": "announces", "obj": None,
             "obj_value": None, "confidence": 0.5,
             "evidence_span": "FDA announces something"},
        ],
    }
    _attach_mock(stage, _ok_response(inner), monkeypatch)

    with cycle_scope(1):
        await stage.handle({"article_id": article_id})
        # Second call returns at the idempotency guard (no HTTP, no inserts).
        await stage.handle({"article_id": article_id})

    cur = await db_ready.conn.execute(
        "SELECT COUNT(*) FROM extraction_runs WHERE article_id=?", (article_id,))
    assert (await cur.fetchone())[0] == 1
    cur = await db_ready.conn.execute("SELECT COUNT(*) FROM facts")
    assert (await cur.fetchone())[0] == 1
    # Only the first handle's completion was published.
    assert len(bus.published) == 1


async def test_budget_paused_short_circuits(db_ready, monkeypatch):
    bus = _FakeBus()
    budget = _FakeBudget(paused=True)
    stage = _make_stage(db_ready, bus, budget)
    article_id = await _insert_article(db_ready, title="Paused")
    _attach_raising_mock(stage, monkeypatch)

    with cycle_scope(1):
        await stage.handle({"article_id": article_id})

    cur = await db_ready.conn.execute(
        "SELECT status FROM extraction_runs WHERE article_id=?", (article_id,))
    assert (await cur.fetchone())["status"] == "budget_paused"
    cur = await db_ready.conn.execute("SELECT COUNT(*) FROM facts")
    assert (await cur.fetchone())[0] == 0
    assert budget.ok_count == 0 and budget.f402_count == 0
    assert bus.published[0][1]["status"] == "budget_paused"


async def test_network_error_yields_http_error_status(db_ready, monkeypatch):
    bus = _FakeBus()
    stage = _make_stage(db_ready, bus)
    article_id = await _insert_article(db_ready, title="Net dies")
    _attach_mock(stage, _network_error_response(), monkeypatch)

    with cycle_scope(1):
        await stage.handle({"article_id": article_id})

    cur = await db_ready.conn.execute(
        "SELECT status FROM extraction_runs WHERE article_id=?", (article_id,))
    assert (await cur.fetchone())["status"] == "http_error"
    cur = await db_ready.conn.execute("SELECT COUNT(*) FROM facts")
    assert (await cur.fetchone())[0] == 0


async def test_500_yields_http_error_status(db_ready, monkeypatch):
    bus = _FakeBus()
    budget = _FakeBudget()
    stage = _make_stage(db_ready, bus, budget)
    article_id = await _insert_article(db_ready, title="500")
    _attach_mock(stage, _http_status_response(500), monkeypatch)

    with cycle_scope(1):
        await stage.handle({"article_id": article_id})

    cur = await db_ready.conn.execute(
        "SELECT status FROM extraction_runs WHERE article_id=?", (article_id,))
    assert (await cur.fetchone())["status"] == "http_error"
    assert budget.f402_count == 0


async def test_402_calls_budget_note_402(db_ready, monkeypatch):
    bus = _FakeBus()
    budget = _FakeBudget()
    stage = _make_stage(db_ready, bus, budget)
    article_id = await _insert_article(db_ready, title="402")
    _attach_mock(stage, _http_status_response(402), monkeypatch)

    with cycle_scope(1):
        await stage.handle({"article_id": article_id})

    cur = await db_ready.conn.execute(
        "SELECT status FROM extraction_runs WHERE article_id=?", (article_id,))
    assert (await cur.fetchone())["status"] == "http_error"
    assert budget.f402_count == 1
    assert budget.ok_count == 0


async def test_malformed_outer_json_yields_parse_error(db_ready, monkeypatch):
    bus = _FakeBus()
    stage = _make_stage(db_ready, bus)
    article_id = await _insert_article(db_ready, title="Garble")
    _attach_mock(stage, _bad_outer_response(), monkeypatch)

    with cycle_scope(1):
        await stage.handle({"article_id": article_id})

    cur = await db_ready.conn.execute(
        "SELECT status FROM extraction_runs WHERE article_id=?", (article_id,))
    assert (await cur.fetchone())["status"] == "parse_error"
    cur = await db_ready.conn.execute("SELECT COUNT(*) FROM facts")
    assert (await cur.fetchone())[0] == 0


async def test_relation_not_in_ontology_is_dropped(db_ready, monkeypatch):
    bus = _FakeBus()
    stage = _make_stage(db_ready, bus)
    article_id = await _insert_article(db_ready, title="Bad relation")
    inner = {
        "entities": [
            {"name": "FDA", "type": "ORG", "aliases": []},
            {"name": "Pfizer", "type": "COMPANY", "aliases": []},
        ],
        "facts": [
            {"subj": "FDA", "relation": "frobnicates", "obj": "Pfizer",
             "obj_value": None, "confidence": 0.9,
             "evidence_span": "FDA frobnicates Pfizer"},
            {"subj": "FDA", "relation": "decides", "obj": "Pfizer",
             "obj_value": None, "confidence": 0.9,
             "evidence_span": "FDA decides on Pfizer"},
        ],
    }
    _attach_mock(stage, _ok_response(inner), monkeypatch)

    with cycle_scope(1):
        await stage.handle({"article_id": article_id})

    cur = await db_ready.conn.execute(
        "SELECT status, fact_count, dropped_fact_count FROM extraction_runs "
        "WHERE article_id=?", (article_id,))
    row = await cur.fetchone()
    assert row["status"] == "ok"
    assert row["fact_count"] == 1
    assert row["dropped_fact_count"] == 1


async def test_subj_not_in_entities_is_dropped(db_ready, monkeypatch):
    bus = _FakeBus()
    stage = _make_stage(db_ready, bus)
    article_id = await _insert_article(db_ready, title="Ghost subj")
    inner = {
        "entities": [
            {"name": "Pfizer", "type": "COMPANY", "aliases": []},
        ],
        "facts": [
            {"subj": "MysteryActor", "relation": "decides", "obj": "Pfizer",
             "obj_value": None, "confidence": 0.9,
             "evidence_span": "someone decided"},
        ],
    }
    _attach_mock(stage, _ok_response(inner), monkeypatch)

    with cycle_scope(1):
        await stage.handle({"article_id": article_id})

    cur = await db_ready.conn.execute(
        "SELECT status, fact_count, dropped_fact_count FROM extraction_runs "
        "WHERE article_id=?", (article_id,))
    row = await cur.fetchone()
    # No facts survive -> status flips to 'empty'.
    assert row["status"] == "empty"
    assert row["fact_count"] == 0
    assert row["dropped_fact_count"] == 1
    cur = await db_ready.conn.execute("SELECT COUNT(*) FROM facts")
    assert (await cur.fetchone())[0] == 0


async def test_confidence_clamped_and_nan_dropped(db_ready, monkeypatch):
    bus = _FakeBus()
    stage = _make_stage(db_ready, bus)
    article_id = await _insert_article(db_ready, title="Confidence")
    inner = {
        "entities": [
            {"name": "FDA", "type": "ORG", "aliases": []},
        ],
        "facts": [
            {"subj": "FDA", "relation": "announces", "obj": None,
             "obj_value": None, "confidence": 1.5,
             "evidence_span": "FDA announces too high"},
            {"subj": "FDA", "relation": "announces", "obj": None,
             "obj_value": None, "confidence": "not a number",
             "evidence_span": "FDA announces non-numeric"},
        ],
    }
    _attach_mock(stage, _ok_response(inner), monkeypatch)

    with cycle_scope(1):
        await stage.handle({"article_id": article_id})

    cur = await db_ready.conn.execute(
        "SELECT confidence FROM facts WHERE article_id=?", (article_id,))
    rows = await cur.fetchall()
    assert len(rows) == 1
    assert rows[0]["confidence"] == 1.0  # clamped from 1.5
    cur = await db_ready.conn.execute(
        "SELECT dropped_fact_count FROM extraction_runs WHERE article_id=?",
        (article_id,))
    assert (await cur.fetchone())["dropped_fact_count"] == 1


async def test_empty_evidence_span_drops_fact(db_ready, monkeypatch):
    bus = _FakeBus()
    stage = _make_stage(db_ready, bus)
    article_id = await _insert_article(db_ready, title="Empty span")
    inner = {
        "entities": [
            {"name": "FDA", "type": "ORG", "aliases": []},
        ],
        "facts": [
            {"subj": "FDA", "relation": "announces", "obj": None,
             "obj_value": None, "confidence": 0.5,
             "evidence_span": ""},
        ],
    }
    _attach_mock(stage, _ok_response(inner), monkeypatch)

    with cycle_scope(1):
        await stage.handle({"article_id": article_id})

    cur = await db_ready.conn.execute(
        "SELECT status, dropped_fact_count FROM extraction_runs WHERE article_id=?",
        (article_id,))
    row = await cur.fetchone()
    assert row["status"] == "empty"
    assert row["dropped_fact_count"] == 1


async def test_tracer_flag_threaded_to_owned_rows(db_ready, monkeypatch):
    bus = _FakeBus()
    stage = _make_stage(db_ready, bus)
    article_id = await _insert_article(db_ready, title="Tracer")
    inner = {
        "entities": [{"name": "FDA", "type": "ORG", "aliases": []}],
        "facts": [
            {"subj": "FDA", "relation": "announces", "obj": None,
             "obj_value": None, "confidence": 0.6,
             "evidence_span": "FDA announces"},
        ],
    }
    _attach_mock(stage, _ok_response(inner), monkeypatch)

    with cycle_scope(1):
        await stage.handle({"article_id": article_id, "is_tracer": True,
                            "tracer_id": "t-extract"})

    cur = await db_ready.conn.execute(
        "SELECT is_tracer, tracer_id FROM extraction_runs WHERE article_id=?",
        (article_id,))
    row = await cur.fetchone()
    assert row["is_tracer"] == 1
    assert row["tracer_id"] == "t-extract"
    cur = await db_ready.conn.execute(
        "SELECT is_tracer, tracer_id FROM facts WHERE article_id=?",
        (article_id,))
    row = await cur.fetchone()
    assert row["is_tracer"] == 1
    assert row["tracer_id"] == "t-extract"
    # Entities are global — they do NOT carry tracer flags (no column).
    assert bus.published[0][1]["is_tracer"] is True
    assert bus.published[0][1]["tracer_id"] == "t-extract"


async def test_missing_article_warns_and_skips(db_ready, monkeypatch):
    bus = _FakeBus()
    stage = _make_stage(db_ready, bus)
    _attach_raising_mock(stage, monkeypatch)

    with cycle_scope(1):
        await stage.handle({"article_id": 99999})

    cur = await db_ready.conn.execute("SELECT COUNT(*) FROM extraction_runs")
    assert (await cur.fetchone())[0] == 0
    assert bus.published == []


async def test_unrecognised_entity_type_is_dropped(db_ready, monkeypatch):
    """Entity with type='ALIEN' is dropped; any fact referencing it goes too."""
    bus = _FakeBus()
    stage = _make_stage(db_ready, bus)
    article_id = await _insert_article(db_ready, title="Bad type")
    inner = {
        "entities": [
            {"name": "Pfizer", "type": "COMPANY", "aliases": []},
            {"name": "Galactus", "type": "ALIEN", "aliases": []},
        ],
        "facts": [
            {"subj": "Galactus", "relation": "acquires", "obj": "Pfizer",
             "obj_value": None, "confidence": 0.8,
             "evidence_span": "Galactus acquires Pfizer"},
        ],
    }
    _attach_mock(stage, _ok_response(inner), monkeypatch)

    with cycle_scope(1):
        await stage.handle({"article_id": article_id})

    cur = await db_ready.conn.execute("SELECT canonical_name FROM entities")
    names = {r["canonical_name"] for r in await cur.fetchall()}
    assert "Galactus" not in names
    cur = await db_ready.conn.execute("SELECT COUNT(*) FROM facts")
    assert (await cur.fetchone())[0] == 0
    cur = await db_ready.conn.execute(
        "SELECT status, dropped_fact_count FROM extraction_runs WHERE article_id=?",
        (article_id,))
    row = await cur.fetchone()
    assert row["status"] == "empty"
    assert row["dropped_fact_count"] == 1


async def test_obj_value_dict_persisted_as_json(db_ready, monkeypatch):
    """The `changes` relation puts a typed dict in obj_value."""
    bus = _FakeBus()
    stage = _make_stage(db_ready, bus)
    article_id = await _insert_article(db_ready, title="Fed cuts")
    inner = {
        "entities": [
            {"name": "Federal Reserve", "type": "ORG", "aliases": []},
            {"name": "fed funds rate", "type": "ECON_INDICATOR", "aliases": []},
        ],
        "facts": [
            {"subj": "Federal Reserve", "relation": "changes",
             "obj": "fed funds rate",
             "obj_value": {"kind": "percentage", "value": -0.25,
                           "unit": "pp", "raw": "25 basis points cut"},
             "confidence": 0.95,
             "evidence_span": "the Fed cut rates 25 basis points"},
        ],
    }
    _attach_mock(stage, _ok_response(inner), monkeypatch)

    with cycle_scope(1):
        await stage.handle({"article_id": article_id})

    cur = await db_ready.conn.execute(
        "SELECT obj_value FROM facts WHERE article_id=?", (article_id,))
    row = await cur.fetchone()
    stored = json.loads(row["obj_value"])
    assert stored["kind"] == "percentage"
    assert stored["value"] == -0.25
