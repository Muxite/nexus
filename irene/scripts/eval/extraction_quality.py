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
from app.cycle import cycle_scope
from app.db import DB
from app.deps import StageDeps
from app.stages.extract import ExtractStage

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
) -> QualityResult:
    """Async core. `mock_factory(article) -> response_dict` lets tests
    inject degraded mocks (drop / hallucinate). Default = exact-gold mock."""
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

        await stage.stop()

        result = await _score(db, articles)
    finally:
        await db.close()
    return result


async def _score(db: DB, articles: list[dict]) -> QualityResult:
    """Walk the gold articles, compare against the DB, build QualityResult."""
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

        ent_match = expected_ents & extracted_ents
        fact_match = expected_facts_set & extracted_facts_set

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
) -> QualityResult:
    """Synchronous wrapper around the async runner — mirrors accuracy.py."""
    return asyncio.run(
        _run_async(
            gold_path=gold_path,
            db_path=db_path,
            real_llm=real_llm,
            mock_factory=mock_factory,
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
    return {
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
    parser.add_argument("--json", dest="as_json", action="store_true",
                        help="Emit machine-readable JSON instead of the human report.")
    args = parser.parse_args(argv)

    try:
        result = run_inproc(
            gold_path=args.gold,
            db_path=args.db,
            real_llm=args.real_llm,
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
