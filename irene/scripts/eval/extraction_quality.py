"""
## Specification

**Purpose.** Run `ExtractStage` against a held-out gold set of news
articles + expected entities/facts, then report precision/recall vs the
gold expectations. This is the evidence source for the end-of-P2 ship
gate (`docs/p2_contracts.md` §6 "Precision sample" / "Recall sample").

**The two modes.**
- **Mock mode (default).** For each gold article, the OpenRouter HTTP
  layer is monkeypatched on the stage instance to return a synthetic
  chat-completion body whose `content` is built FROM that article's
  expected entities + facts. With a bug-free `extract.py` parse layer,
  precision and recall both come out 1.0. This mode is the regression
  test for the parse layer: any drop here means the parser regressed.
- **Real mode (`--real-llm`).** Issues actual OpenRouter requests. The
  `OPENROUTER_API_KEY` env var must be set; we abort with a clear error
  if it isn't (no silent fallback to mock). This mode answers the
  P2-gate question: how well does the LIVE model extract against held-
  out human labels?

**Public interface.**
- CLI: `python -m scripts.eval.extraction_quality --gold <path>
  [--real-llm] [--db <path>] [--json]`
- `run_inproc(*, gold_path, db_path=":memory:", real_llm=False) ->
  QualityResult`

**Matching rules** (deterministic, no fuzziness):
- **Entity** match: case-insensitive equality on
  `(entity_type, canonical_name.strip())`. NO substring matching.
- **Fact** match: subject name CI-equality AND `relation` exact
  AND (object-name CI-equality OR both objects null).
- **Precision** = matched / extracted; **Recall** = matched / expected.
  Both default to 1.0 when their denominator is 0 (vacuous truth — no
  data to be wrong about).

**Per-relation breakdown.** Each of the 10 relations gets
`{n_expected, n_extracted, n_matched, precision, recall}` so the report
narrative can call out "we missed every `changes`" or "we hallucinate
`acquires`."

**Per-article details.** For each gold article, capture which expected
facts matched, which got dropped, and which extras the extractor
introduced — drives the report's article-by-article narrative.

**Anti-patterns** (also enforced in code review):
- Don't fall back to mock mode silently if `--real-llm` is set without
  a key — fail loudly.
- Don't add new tables. The harness reads `entities` and `facts` that
  extract.py already wrote.
- Don't import from other stages — extract is the only stage exercised.

**Non-goals.**
- Calibration of LLM confidences (separate concern).
- Cross-article entity linkage (P3 territory).
- Cost reporting (the P2 report itself rolls cost from `extraction_runs`
  tokens — this harness is precision/recall only).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional

import yaml

# Import DDL-bearing modules so register_schema() fires before db.connect().
import app.ai  # noqa: F401
import app.main  # noqa: F401 — registers cycles DDL
import app.stages.dedupe  # noqa: F401
import app.stages.extract  # noqa: F401 — registers extract DDL
import app.stages.ingest  # noqa: F401
import app.stages.resolve  # noqa: F401 — registers entity_resolutions DDL
from app.cycle import cycle_scope
from app.db import DB
from app.deps import StageDeps
from app.stages.extract import ExtractStage
from app.stages.resolve import ResolveStage

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result shape
# ---------------------------------------------------------------------------


ONTOLOGY_RELATIONS: tuple[str, ...] = (
    "announces", "decides", "elects", "acquires", "launches",
    "accuses", "signs", "changes", "resigns", "nominates",
)


@dataclass
class QualityResult:
    n_articles: int = 0
    n_expected_entities: int = 0
    n_expected_facts: int = 0
    n_extracted_entities: int = 0
    n_extracted_facts: int = 0
    entity_precision: float = 1.0
    entity_recall: float = 1.0
    fact_precision: float = 1.0
    fact_recall: float = 1.0
    per_relation: dict[str, dict[str, float]] = field(default_factory=dict)
    per_article: list[dict] = field(default_factory=list)
    # Alias-aware (P3) numbers — populated only when the runner was
    # called with `alias_aware=True`. None otherwise. The alias-aware
    # result is itself a `QualityResult` (with its own .alias_aware =
    # None) so the printer can iterate over both shapes uniformly.
    alias_aware: Optional["QualityResult"] = None


# ---------------------------------------------------------------------------
# Fakes — minimal wiring (mirrors app/tests/stages/test_extract.py)
# ---------------------------------------------------------------------------


class _FakeBudget:
    """OK-forever budget. The extract path always needs a budget; for the
    eval harness we never want budget_paused to short-circuit a run."""

    def is_paused(self) -> bool:
        return False

    def note_ok(self) -> None:
        return

    def note_402(self) -> None:
        return


class _FakeBus:
    """Collects publishes; the eval harness doesn't subscribe anyone, but
    extract.py publishes TOPIC_EXTRACTION_COMPLETED unconditionally."""

    def __init__(self) -> None:
        self.published: list[tuple[str, dict]] = []

    async def publish(self, topic: str, payload: dict) -> None:
        # JSON round-trip — same invariant as the bus tests.
        json.loads(json.dumps(payload))
        self.published.append((topic, payload))

    def subscribe(self, topic: str, handler) -> None:  # noqa: ARG002
        pass

    async def start(self) -> None:
        return

    async def stop(self) -> None:
        return


@dataclass
class _ExtractCfg:
    # Default model is the mock value so the mock-mode tests stay
    # provider-agnostic; --real-llm overrides this with the configured
    # production extract model (cfg.extract.model).
    model: str = "eval-mock"
    timeout_seconds: float = 30.0
    max_tokens: int = 1024
    max_article_chars: int = 4000


@dataclass
class _ORCfg:
    api_key: str = "eval-mock-key"


@dataclass
class _Config:
    extract: _ExtractCfg = field(default_factory=_ExtractCfg)
    openrouter: _ORCfg = field(default_factory=_ORCfg)


def _build_real_config() -> "_Config":
    """Build a config tied to the real OpenRouter credential.

    Uses the production default extract model + a real api_key from env.
    Called only when `--real-llm` is set; mock mode never goes here.
    """
    from app.config import ExtractSection  # production-default model
    cfg = _Config()
    cfg.extract.model = ExtractSection().model
    cfg.openrouter.api_key = os.environ["OPENROUTER_API_KEY"]
    return cfg


# ---------------------------------------------------------------------------
# Gold-set loading
# ---------------------------------------------------------------------------


def _load_gold(gold_path: str) -> list[dict]:
    """Parse the gold YAML, assert ontology_version, return article list."""
    p = Path(gold_path)
    if not p.exists():
        raise FileNotFoundError(f"gold set not found: {gold_path}")
    with p.open() as fh:
        doc = yaml.safe_load(fh)
    if not isinstance(doc, dict):
        raise ValueError(f"gold set malformed (top level not a mapping): {gold_path}")
    onto = doc.get("ontology_version")
    if onto != "v1":
        raise ValueError(
            f"ontology_version mismatch: gold says {onto!r}, expected 'v1'"
        )
    arts = doc.get("articles")
    if not isinstance(arts, list) or not arts:
        raise ValueError("gold set has no articles")
    return arts


# ---------------------------------------------------------------------------
# Mock-mode synthesis: turn a gold article into an OpenRouter response
# ---------------------------------------------------------------------------


def _synthesize_response(article: dict) -> dict:
    """Wrap the article's expected entities + facts in an OpenRouter
    chat-completion body shape that extract.py's parse layer will accept."""
    inner = {
        "entities": [
            {"name": e["name"], "type": e["type"], "aliases": []}
            for e in article["expected_entities"]
        ],
        "facts": [
            {
                "subj": f["subj"],
                "relation": f["relation"],
                "obj": f.get("obj"),
                "obj_value": None,
                "confidence": 0.9,
                "evidence_span": f"[gold-{article['id']}] {f['subj']} "
                                 f"{f['relation']} {f.get('obj') or ''}".strip(),
            }
            for f in article["expected_facts"]
        ],
    }
    inner_json = json.dumps(inner)
    body = json.dumps({
        "choices": [{"message": {"content": inner_json}}],
        "usage": {"prompt_tokens": 50, "completion_tokens": 25},
    })
    return {
        "status_code": 200,
        "body": body,
        "latency_ms": 5.0,
        "tokens_in": 50,
        "tokens_out": 25,
        "error": None,
    }


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------


def _norm(s: Any) -> Optional[str]:
    if s is None:
        return None
    if not isinstance(s, str):
        return None
    return s.strip().lower()


def _entity_key(etype: str, name: str) -> tuple[str, str]:
    return (etype.upper(), name.strip().lower())


def _fact_key(subj: str, relation: str, obj: Optional[str]) -> tuple:
    return (_norm(subj), relation, _norm(obj))


def _safe_div(num: int, den: int) -> float:
    if den == 0:
        # Vacuous: no extracted items → precision is trivially 1.0;
        # no expected items → recall is trivially 1.0. Both branches are
        # called explicitly with the right denominator by callers.
        return 1.0
    return num / den


# ---------------------------------------------------------------------------
# Article seeding
# ---------------------------------------------------------------------------


async def _seed_article(db: DB, *, article_id: int, title: str, summary: str) -> None:
    """Insert one row into `articles`. extract.py's handle() reads it back.
    We pre-allocate the id so the gold-article id matches the DB row id."""
    async with db.batch() as cur:
        await cur.execute(
            """
            INSERT INTO articles
                (id, source, source_detail, url, title, summary, published_at,
                 ingested_at, cycle_id, is_tracer, tracer_id)
            VALUES (?, 'eval', 'gold', ?, ?, ?, ?, ?, 1, 0, NULL)
            """,
            (
                article_id,
                f"https://eval.local/{article_id}",
                title,
                summary,
                "2026-05-23T00:00:00+00:00",
                "2026-05-23T00:00:00+00:00",
            ),
        )


# ---------------------------------------------------------------------------
# DB readback — what extract.py wrote
# ---------------------------------------------------------------------------


async def _read_entities_for(db: DB, article_id: int) -> list[tuple[str, str]]:
    """Read entities REFERENCED by any fact for `article_id`.

    extract.py only writes entities that are referenced by at least one
    surviving fact (see `_persist_run_with_data`), so joining facts→entities
    gives us the article's entity set. We dedupe within the article."""
    cur = await db.conn.execute(
        """
        SELECT DISTINCT e.entity_type, e.canonical_name
        FROM facts f
        JOIN entities e
          ON e.id = f.subj_entity_id
        WHERE f.article_id = ?
        UNION
        SELECT DISTINCT e.entity_type, e.canonical_name
        FROM facts f
        JOIN entities e
          ON e.id = f.obj_entity_id
        WHERE f.article_id = ? AND f.obj_entity_id IS NOT NULL
        """,
        (article_id, article_id),
    )
    rows = await cur.fetchall()
    await cur.close()
    return [(r["entity_type"], r["canonical_name"]) for r in rows]


async def _read_facts_for(db: DB, article_id: int) -> list[tuple[str, str, Optional[str]]]:
    """Return (subj_name, relation, obj_name|None) for each fact on the article."""
    cur = await db.conn.execute(
        """
        SELECT s.canonical_name AS subj,
               f.relation        AS relation,
               o.canonical_name  AS obj
        FROM facts f
        JOIN entities s ON s.id = f.subj_entity_id
        LEFT JOIN entities o ON o.id = f.obj_entity_id
        WHERE f.article_id = ?
        """,
        (article_id,),
    )
    rows = await cur.fetchall()
    await cur.close()
    return [(r["subj"], r["relation"], r["obj"]) for r in rows]


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def _attach_mock(stage: ExtractStage, response: dict) -> None:
    async def stub(prompt: str) -> dict:  # noqa: ARG001
        return response
    stage._call_openrouter = stub  # type: ignore[method-assign]


async def _run_async(
    *,
    gold_path: str,
    db_path: str,
    real_llm: bool,
    mock_factory=None,
    alias_aware: bool = False,
) -> QualityResult:
    """Async core. `mock_factory(article) -> response_dict` lets tests
    inject degraded mocks (drop / hallucinate). Default = exact-gold mock.

    `alias_aware=True` also runs `ResolveStage` after each extract pass
    (so `entity_resolutions` is populated) and scores the gold a second
    time using an expansion-based matcher (canonical ∪ aliases ∪
    resolution chain ∪ acronym pairs). Both strict and alias-aware
    numbers come back on the result."""
    articles = _load_gold(gold_path)

    if real_llm:
        if not os.environ.get("OPENROUTER_API_KEY"):
            raise RuntimeError(
                "real_llm=True but OPENROUTER_API_KEY is not set. "
                "Refusing to silently fall back to mock mode."
            )

    if db_path == ":memory:":
        db = DB.ephemeral()
    else:
        db = DB(path=db_path)
    await db.connect()
    try:
        bus = _FakeBus()
        cfg = _build_real_config() if real_llm else _Config()
        deps = StageDeps(
            db=db,
            bus=bus,
            config=cfg,
            budget=_FakeBudget(),
        )
        stage = ExtractStage(deps)
        await stage.start()

        resolver: Optional[ResolveStage] = None
        if alias_aware:
            resolver = ResolveStage(deps)
            await resolver.start()

        # Seed all articles first so each handle() finds its row.
        for idx, art in enumerate(articles, start=1):
            await _seed_article(
                db,
                article_id=idx,
                title=art["title"],
                summary=art["summary"],
            )

        # Run extract per article, with the appropriate mock attached.
        with cycle_scope(1):
            for idx, art in enumerate(articles, start=1):
                if not real_llm:
                    factory = mock_factory or _synthesize_response
                    _attach_mock(stage, factory(art))
                await stage.handle({
                    "article_id": idx,
                    "cluster_id": None,
                    "freshness": "BREAKING",
                    "is_tracer": False,
                    "tracer_id": None,
                })
                if resolver is not None:
                    # Look up the extraction_run we just wrote so the
                    # resolver can target it. The eval drives the bus by
                    # hand here (instead of going through bus.publish) so
                    # the loop stays deterministic — resolve fires
                    # synchronously after extract.
                    cur = await db.conn.execute(
                        "SELECT id, status FROM extraction_runs "
                        "WHERE article_id = ? ORDER BY id DESC LIMIT 1",
                        (idx,),
                    )
                    row = await cur.fetchone()
                    await cur.close()
                    if row is not None:
                        await resolver.handle({
                            "extraction_run_id": row["id"],
                            "article_id": idx,
                            "status": row["status"],
                            "fact_count": 0,    # unused by resolver
                            "entity_count": 0,  # unused by resolver
                            "is_tracer": False,
                            "tracer_id": None,
                        })

        if resolver is not None:
            await resolver.stop()
        await stage.stop()

        # Strict scoring (the P2 metric).
        result = await _score(db, articles)
        # Alias-aware second pass, when asked. Doesn't re-read extract —
        # uses the SAME extracted rows but a richer matcher.
        if alias_aware:
            expansions = await _build_expansions(db)
            result.alias_aware = await _score(
                db, articles, expansions=expansions,
            )
    finally:
        await db.close()
    return result


async def _build_expansions(db: DB) -> dict[tuple[str, str], set[str]]:
    """Build per-(entity_type, lowercased_canonical_or_alias) → expansion set.

    The expansion set for a name N of type T is every other surface form
    that should be considered equivalent under alias-aware matching:
      - N's own canonical (always)
      - every alias attached to the canonical entity (and reverse: every
        canonical reachable from an alias)
      - the resolved-to canonical, if N's entity has an entity_resolutions
        row (and reverse: every entity that resolves TO N's canonical)
      - acronym pairs from the loaded acronym table (both directions)

    All keys + values are case-folded. The matcher tests pairwise
    set-overlap.
    """
    out: dict[tuple[str, str], set[str]] = {}

    def _add(etype: str, key: str, val: str) -> None:
        ke = (etype.upper(), key.strip().lower())
        ve = val.strip().lower()
        if ke[1] == ve:
            return
        out.setdefault(ke, set()).add(ve)

    # entity_aliases: alias <-> canonical, both ways.
    cur = await db.conn.execute(
        """
        SELECT e.entity_type AS et, e.canonical_name AS canon, a.alias AS alias
        FROM entity_aliases a
        JOIN entities e ON e.id = a.entity_id
        """
    )
    for row in await cur.fetchall():
        _add(row["et"], row["canon"], row["alias"])
        _add(row["et"], row["alias"], row["canon"])
    await cur.close()

    # entity_resolutions: any A resolved-to B → B is an expansion of A's
    # canonical (and vice versa).
    cur = await db.conn.execute(
        """
        SELECT
            e1.entity_type   AS et,
            e1.canonical_name AS src,
            e2.canonical_name AS dst
        FROM entity_resolutions r
        JOIN entities e1 ON e1.id = r.entity_id
        JOIN entities e2 ON e2.id = r.resolved_to_id
        WHERE r.resolution_kind != 'self'
        """
    )
    for row in await cur.fetchall():
        _add(row["et"], row["src"], row["dst"])
        _add(row["et"], row["dst"], row["src"])
    await cur.close()

    # Acronym table (loaded straight from disk to avoid coupling to the
    # resolver's internal state). Bidirectional.
    acronyms = _load_acronym_table()
    for entry in acronyms:
        etype, short, long_form = entry["type"], entry["short"], entry["long"]
        _add(etype, short, long_form)
        _add(etype, long_form, short)
    return out


def _load_acronym_table() -> list[dict]:
    """Read the same YAML the resolver reads. Cheap; called once per
    `_build_expansions`. Returns the raw `acronyms:` list."""
    path = Path(__file__).resolve().parents[2] / "app" / "stages" / "data" / "acronyms.yaml"
    if not path.exists():
        return []
    doc = yaml.safe_load(path.read_text()) or {}
    return doc.get("acronyms") or []


def _expand(name: str, etype: str, expansions: dict[tuple[str, str], set[str]]) -> set[str]:
    """Return {lowered name} ∪ its known equivalences. Always at least
    contains the input itself, so a name with no aliases / resolutions /
    acronym mappings still matches itself."""
    base = name.strip().lower()
    out = {base}
    out.update(expansions.get((etype.upper(), base), ()))
    return out


def _entities_overlap(
    etype: str,
    a: str,
    b: str,
    expansions: Optional[dict[tuple[str, str], set[str]]],
) -> bool:
    """True iff the expansion sets of (etype, a) and (etype, b) intersect.
    When `expansions is None`, falls back to strict CI equality."""
    if expansions is None:
        return a.strip().lower() == b.strip().lower()
    return bool(_expand(a, etype, expansions) & _expand(b, etype, expansions))


def _alias_aware_entity_match(
    expected: set[tuple[str, str]],
    extracted: set[tuple[str, str]],
    expansions: dict[tuple[str, str], set[str]],
) -> set[tuple[str, str]]:
    """For each expected (type, name_lower), count it as matched if ANY
    extracted entity of the same type has an overlapping expansion set.

    Returns a set of expected keys that were matched, so the caller can
    take its size as the alias-aware "matched_ents" count. Same shape as
    the strict matcher's `expected_ents & extracted_ents`.
    """
    matched: set[tuple[str, str]] = set()
    for (etype, ename) in expected:
        ent_expansion = _expand(ename, etype, expansions)
        for (etype_x, ename_x) in extracted:
            if etype_x != etype:
                continue
            ext_expansion = _expand(ename_x, etype_x, expansions)
            if ent_expansion & ext_expansion:
                matched.add((etype, ename))
                break
    return matched


def _alias_aware_fact_match(
    expected: set[tuple],
    extracted: set[tuple],
    *,
    type_by_name: dict[str, str],
    expansions: dict[tuple[str, str], set[str]],
) -> set[tuple]:
    """Match facts when subj / obj overlap on expansion sets and relation matches.

    Each fact is `(subj_lower, relation, obj_lower_or_None)`. obj_None
    is its own expansion set ({None}); it matches only obj_None on the
    other side."""
    matched: set[tuple] = set()
    for exp_fact in expected:
        exp_subj, exp_rel, exp_obj = exp_fact
        if exp_subj is None:
            continue
        exp_subj_type = type_by_name.get(exp_subj)
        exp_obj_type = type_by_name.get(exp_obj) if exp_obj else None
        exp_subj_set = (
            _expand(exp_subj, exp_subj_type, expansions)
            if exp_subj_type else {exp_subj}
        )
        exp_obj_set = (
            _expand(exp_obj, exp_obj_type, expansions)
            if (exp_obj and exp_obj_type) else ({exp_obj} if exp_obj else None)
        )
        for ext_fact in extracted:
            ext_subj, ext_rel, ext_obj = ext_fact
            if ext_rel != exp_rel:
                continue
            if ext_subj is None:
                continue
            ext_subj_type = type_by_name.get(ext_subj, exp_subj_type)
            ext_subj_set = (
                _expand(ext_subj, ext_subj_type, expansions)
                if ext_subj_type else {ext_subj}
            )
            if not (exp_subj_set & ext_subj_set):
                continue
            # Object handling: both None → match. Both not None → require
            # overlap. One None and one not → no match (object polarity
            # is meaningful — a `signs (X, NULL)` is a different
            # assertion from `signs (X, Y)`).
            if exp_obj is None and ext_obj is None:
                matched.add(exp_fact)
                break
            if exp_obj is None or ext_obj is None:
                continue
            ext_obj_type = type_by_name.get(ext_obj, exp_obj_type)
            ext_obj_set = (
                _expand(ext_obj, ext_obj_type, expansions)
                if ext_obj_type else {ext_obj}
            )
            if exp_obj_set & ext_obj_set:
                matched.add(exp_fact)
                break
    return matched


async def _score(
    db: DB,
    articles: list[dict],
    *,
    expansions: Optional[dict[tuple[str, str], set[str]]] = None,
) -> QualityResult:
    """Walk the gold articles, compare against the DB, build QualityResult.

    When `expansions` is provided, matching uses alias-aware expansion
    overlap instead of strict CI equality."""
    result = QualityResult(n_articles=len(articles))

    # Per-relation accumulators. Initialise every ontology relation so the
    # report shape is stable even when some relations have zero data.
    per_rel: dict[str, dict[str, int]] = {
        r: {"n_expected": 0, "n_extracted": 0, "n_matched": 0}
        for r in ONTOLOGY_RELATIONS
    }

    total_exp_ents = 0
    total_exp_facts = 0
    total_ext_ents = 0
    total_ext_facts = 0
    matched_ents = 0
    matched_facts = 0

    for idx, art in enumerate(articles, start=1):
        expected_ents = {
            _entity_key(e["type"], e["name"])
            for e in art["expected_entities"]
        }
        expected_facts_set = {
            _fact_key(f["subj"], f["relation"], f.get("obj"))
            for f in art["expected_facts"]
        }

        extracted_ents_rows = await _read_entities_for(db, idx)
        extracted_facts_rows = await _read_facts_for(db, idx)
        extracted_ents = {_entity_key(t, n) for (t, n) in extracted_ents_rows}
        extracted_facts_set = {
            _fact_key(subj, rel, obj) for (subj, rel, obj) in extracted_facts_rows
        }

        # Type lookup for the alias-aware matcher: fact rows arrive with
        # subj/obj as bare names, so we need to know which entity_type to
        # expand them against. Build a name → type map from the article's
        # extracted entities.
        type_by_name = {n.strip().lower(): t for (t, n) in extracted_ents_rows}
        # Add the expected-side types too so unmatched expectations can
        # be expanded.
        for e in art["expected_entities"]:
            type_by_name.setdefault(e["name"].strip().lower(), e["type"])

        if expansions is None:
            ent_match = expected_ents & extracted_ents
            fact_match = expected_facts_set & extracted_facts_set
        else:
            ent_match = _alias_aware_entity_match(
                expected_ents, extracted_ents, expansions,
            )
            fact_match = _alias_aware_fact_match(
                expected_facts_set, extracted_facts_set,
                type_by_name=type_by_name,
                expansions=expansions,
            )

        # Per-relation tally — count expectations + extractions + matches
        # keyed by the relation itself.
        for f in art["expected_facts"]:
            r = f["relation"]
            if r in per_rel:
                per_rel[r]["n_expected"] += 1
        for (_subj, r, _obj) in extracted_facts_rows:
            if r in per_rel:
                per_rel[r]["n_extracted"] += 1
        for (_subj, r, _obj) in fact_match:
            if r in per_rel:
                per_rel[r]["n_matched"] += 1

        total_exp_ents += len(expected_ents)
        total_exp_facts += len(expected_facts_set)
        total_ext_ents += len(extracted_ents)
        total_ext_facts += len(extracted_facts_set)
        matched_ents += len(ent_match)
        matched_facts += len(fact_match)

        result.per_article.append({
            "id": art["id"],
            "article_id": idx,
            "n_expected_entities": len(expected_ents),
            "n_extracted_entities": len(extracted_ents),
            "n_matched_entities": len(ent_match),
            "n_expected_facts": len(expected_facts_set),
            "n_extracted_facts": len(extracted_facts_set),
            "n_matched_facts": len(fact_match),
            "missing_facts": [
                {"subj": s, "relation": r, "obj": o}
                for (s, r, o) in (expected_facts_set - extracted_facts_set)
            ],
            "extra_facts": [
                {"subj": s, "relation": r, "obj": o}
                for (s, r, o) in (extracted_facts_set - expected_facts_set)
            ],
        })

    # Roll up.
    result.n_expected_entities = total_exp_ents
    result.n_expected_facts = total_exp_facts
    result.n_extracted_entities = total_ext_ents
    result.n_extracted_facts = total_ext_facts
    result.entity_precision = _safe_div(matched_ents, total_ext_ents)
    result.entity_recall = _safe_div(matched_ents, total_exp_ents)
    result.fact_precision = _safe_div(matched_facts, total_ext_facts)
    result.fact_recall = _safe_div(matched_facts, total_exp_facts)

    # Per-relation precision/recall (with safe divisions).
    rel_out: dict[str, dict[str, float]] = {}
    for r, agg in per_rel.items():
        rel_out[r] = {
            "n_expected": float(agg["n_expected"]),
            "n_extracted": float(agg["n_extracted"]),
            "n_matched": float(agg["n_matched"]),
            "precision": _safe_div(agg["n_matched"], agg["n_extracted"]),
            "recall": _safe_div(agg["n_matched"], agg["n_expected"]),
        }
    result.per_relation = rel_out
    return result


def run_inproc(
    *,
    gold_path: str,
    db_path: str = ":memory:",
    real_llm: bool = False,
    mock_factory=None,
    alias_aware: bool = False,
) -> QualityResult:
    """Synchronous wrapper around the async runner — mirrors accuracy.py."""
    return asyncio.run(
        _run_async(
            gold_path=gold_path,
            db_path=db_path,
            real_llm=real_llm,
            mock_factory=mock_factory,
            alias_aware=alias_aware,
        )
    )


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def _print_report(result: QualityResult) -> None:
    print("extraction quality report")
    print(f"  articles processed:      {result.n_articles}")
    print(f"  expected entities:       {result.n_expected_entities}")
    print(f"  extracted entities:      {result.n_extracted_entities}")
    print(f"  expected facts:          {result.n_expected_facts}")
    print(f"  extracted facts:         {result.n_extracted_facts}")
    print(f"  entity precision/recall: "
          f"{result.entity_precision:.3f} / {result.entity_recall:.3f}")
    print(f"  fact precision/recall:   "
          f"{result.fact_precision:.3f} / {result.fact_recall:.3f}")
    if result.alias_aware is not None:
        a = result.alias_aware
        print("  alias-aware (resolver + acronyms + aliases):")
        print(f"    entity P/R:            {a.entity_precision:.3f} / {a.entity_recall:.3f}")
        print(f"    fact P/R:              {a.fact_precision:.3f} / {a.fact_recall:.3f}")
    print("  per-relation:")
    for r in ONTOLOGY_RELATIONS:
        row = result.per_relation.get(r, {})
        print(
            f"    {r:<12} expected={int(row.get('n_expected', 0)):>3} "
            f"extracted={int(row.get('n_extracted', 0)):>3} "
            f"matched={int(row.get('n_matched', 0)):>3} "
            f"P={row.get('precision', 1.0):.3f} R={row.get('recall', 1.0):.3f}"
        )
    # Surface the worst articles (largest gap) for the human reader.
    worst = sorted(
        result.per_article,
        key=lambda a: (a["n_expected_facts"] - a["n_matched_facts"]
                       + len(a["extra_facts"])),
        reverse=True,
    )[:5]
    if any((a["missing_facts"] or a["extra_facts"]) for a in worst):
        print("  worst articles (gap = missing + extra):")
        for a in worst:
            gap = (a["n_expected_facts"] - a["n_matched_facts"]) + len(a["extra_facts"])
            if gap == 0:
                continue
            print(
                f"    {a['id']}: missed={len(a['missing_facts'])} "
                f"extra={len(a['extra_facts'])}"
            )


def _result_to_jsonable(result: QualityResult) -> dict:
    out = {
        "n_articles": result.n_articles,
        "n_expected_entities": result.n_expected_entities,
        "n_expected_facts": result.n_expected_facts,
        "n_extracted_entities": result.n_extracted_entities,
        "n_extracted_facts": result.n_extracted_facts,
        "entity_precision": result.entity_precision,
        "entity_recall": result.entity_recall,
        "fact_precision": result.fact_precision,
        "fact_recall": result.fact_recall,
        "per_relation": result.per_relation,
        "per_article": result.per_article,
    }
    if result.alias_aware is not None:
        out["alias_aware"] = _result_to_jsonable(result.alias_aware)
    return out


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Extraction-quality harness (precision/recall against a gold set)."
    )
    parser.add_argument("--gold", required=True,
                        help="Path to the gold-set YAML (e.g. scripts/eval/gold_set/v1.yaml).")
    parser.add_argument("--db", default=":memory:",
                        help="DB path (default ':memory:'; use a file for persistence).")
    parser.add_argument("--real-llm", action="store_true",
                        help="Use real OpenRouter (requires OPENROUTER_API_KEY).")
    parser.add_argument("--alias-aware", action="store_true",
                        help="Also run the resolver and report alias-aware "
                             "P/R alongside the strict numbers (P3+).")
    parser.add_argument("--json", dest="as_json", action="store_true",
                        help="Emit machine-readable JSON instead of the human report.")
    args = parser.parse_args(argv)

    try:
        result = run_inproc(
            gold_path=args.gold,
            db_path=args.db,
            real_llm=args.real_llm,
            alias_aware=args.alias_aware,
        )
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        print(f"extraction quality: ERROR {exc}", flush=True)
        return 2

    if args.as_json:
        print(json.dumps(_result_to_jsonable(result), indent=2, sort_keys=True))
    else:
        _print_report(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
