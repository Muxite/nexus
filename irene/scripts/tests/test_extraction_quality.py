"""Tests for scripts.eval.extraction_quality.

The harness has two correctness axes:
  1. The gold YAML covers all 10 ontology relations and has 16-20 articles.
  2. Against a faithful mock LLM (returns exactly the gold), precision and
     recall are 1.0; degraded mocks degrade exactly the metric they should.

We do NOT exercise --real-llm here — that path requires OPENROUTER_API_KEY
and costs money. The runtime error when real_llm=True with no key is
tested via a unit-level call.
"""

from __future__ import annotations

import copy
import json
import os
from pathlib import Path

import pytest
import yaml

import app.ai  # noqa: F401
import app.main  # noqa: F401
import app.stages.dedupe  # noqa: F401
import app.stages.extract  # noqa: F401
import app.stages.ingest  # noqa: F401

from scripts.eval import extraction_quality as eq


GOLD_PATH = str(
    Path(__file__).resolve().parents[1] / "eval" / "gold_set" / "v1.yaml"
)


# ---------------------------------------------------------------------------
# Gold-set static checks
# ---------------------------------------------------------------------------


def _load() -> dict:
    with open(GOLD_PATH) as fh:
        return yaml.safe_load(fh)


def test_gold_yaml_parses_and_has_expected_size():
    doc = _load()
    assert doc["version"] == "v1"
    assert doc["ontology_version"] == "v1"
    articles = doc["articles"]
    # Spec: 16-20 articles. Under-provisioning hurts coverage, over-
    # provisioning hurts review time.
    assert 16 <= len(articles) <= 20, f"expected 16-20 articles, got {len(articles)}"


def test_gold_covers_all_ten_relations():
    doc = _load()
    rels = {f["relation"] for a in doc["articles"] for f in a["expected_facts"]}
    missing = set(eq.ONTOLOGY_RELATIONS) - rels
    assert not missing, f"gold set missing relations: {missing}"


def test_gold_covers_at_least_five_entity_types():
    doc = _load()
    types = {e["type"] for a in doc["articles"] for e in a["expected_entities"]}
    assert len(types) >= 5, f"only {len(types)} entity types covered: {types}"


def test_gold_entity_types_all_in_ontology():
    """Every expected entity type must be one of the 7 ontology types,
    otherwise extract.py's parse layer would drop it and our 'should match'
    assumption breaks."""
    valid = {"COMPANY", "PERSON", "ORG", "COUNTRY",
             "ECON_INDICATOR", "ASSET", "EVENT"}
    doc = _load()
    for a in doc["articles"]:
        for e in a["expected_entities"]:
            assert e["type"] in valid, (
                f"article {a['id']} entity {e['name']!r} has invalid type {e['type']!r}"
            )


def test_gold_facts_reference_listed_entities():
    """Every expected fact's subj/obj must appear in expected_entities —
    otherwise the synthesized mock response would emit a fact whose subj
    extract.py drops at parse time, and our 'mock recall = 1.0' assumption
    would silently fail."""
    doc = _load()
    for a in doc["articles"]:
        names = {e["name"] for e in a["expected_entities"]}
        for f in a["expected_facts"]:
            assert f["subj"] in names, (
                f"article {a['id']}: fact subj {f['subj']!r} not in entities"
            )
            if f.get("obj") is not None:
                assert f["obj"] in names, (
                    f"article {a['id']}: fact obj {f['obj']!r} not in entities"
                )


# ---------------------------------------------------------------------------
# run_inproc — mock-mode happy path
# ---------------------------------------------------------------------------


def test_mock_mode_yields_perfect_precision_and_recall():
    result = eq.run_inproc(gold_path=GOLD_PATH)
    # Faithful mock returns exactly the gold → both metrics 1.0.
    assert result.entity_precision == 1.0
    assert result.entity_recall == 1.0
    assert result.fact_precision == 1.0
    assert result.fact_recall == 1.0
    # Sanity: we processed every article and matched every fact.
    doc = _load()
    assert result.n_articles == len(doc["articles"])
    expected_fact_count = sum(len(a["expected_facts"]) for a in doc["articles"])
    assert result.n_expected_facts == expected_fact_count
    assert result.n_extracted_facts == expected_fact_count


def test_per_relation_report_has_every_ontology_relation():
    result = eq.run_inproc(gold_path=GOLD_PATH)
    for r in eq.ONTOLOGY_RELATIONS:
        assert r in result.per_relation, f"missing relation key: {r}"
    # And every relation present in the gold has a non-zero match count.
    doc = _load()
    gold_rels = {f["relation"] for a in doc["articles"] for f in a["expected_facts"]}
    for r in gold_rels:
        row = result.per_relation[r]
        assert row["n_expected"] > 0
        assert row["n_extracted"] > 0
        assert row["n_matched"] > 0
        assert row["precision"] == 1.0
        assert row["recall"] == 1.0


# ---------------------------------------------------------------------------
# Degraded-mock fixtures — drop / hallucinate
# ---------------------------------------------------------------------------


def _drop_one_fact_factory(article: dict) -> dict:
    """Mock that returns the gold MINUS the first expected fact.
    → recall drops; precision stays 1.0 (no false positives)."""
    trimmed = copy.deepcopy(article)
    if trimmed["expected_facts"]:
        trimmed["expected_facts"] = trimmed["expected_facts"][1:]
    return eq._synthesize_response(trimmed)


def _hallucinate_one_fact_factory(article: dict) -> dict:
    """Mock that returns the gold PLUS one bogus fact between two real
    entities. → precision drops; recall stays 1.0 (no real facts missed)."""
    inflated = copy.deepcopy(article)
    ents = inflated["expected_entities"]
    if len(ents) >= 2:
        subj = ents[0]["name"]
        obj = ents[1]["name"]
    else:
        # Single-entity articles → subject-only `resigns` hallucination is
        # within-ontology, so extract.py's parse layer accepts it.
        subj = ents[0]["name"]
        obj = None
    # Pick a relation that isn't already on this article so the bogus fact
    # never accidentally matches an expected one.
    used = {f["relation"] for f in inflated["expected_facts"]}
    bogus_rel = next(
        (r for r in eq.ONTOLOGY_RELATIONS if r not in used),
        "announces",
    )
    inflated["expected_facts"] = list(inflated["expected_facts"]) + [
        {"subj": subj, "relation": bogus_rel, "obj": obj}
    ]
    return eq._synthesize_response(inflated)


def test_degraded_mock_drop_one_drops_recall_not_precision():
    """Each article loses its first expected fact at the mock layer."""
    result = eq.run_inproc(gold_path=GOLD_PATH, mock_factory=_drop_one_fact_factory)
    assert result.fact_precision == 1.0, (
        f"precision should stay 1.0 (no extras) but got {result.fact_precision}"
    )
    assert result.fact_recall < 1.0, (
        f"recall should drop below 1.0 (we dropped a fact per article), got {result.fact_recall}"
    )
    # Every article had at least one expected fact → every article loses one
    # → recall is exactly (expected - n_articles) / expected.
    doc = _load()
    expected_facts = sum(len(a["expected_facts"]) for a in doc["articles"])
    expected_after_drop = expected_facts - len(doc["articles"])
    assert result.fact_recall == pytest.approx(expected_after_drop / expected_facts)


def test_degraded_mock_hallucinate_drops_precision_not_recall():
    """Each article gains one bogus fact at the mock layer."""
    result = eq.run_inproc(
        gold_path=GOLD_PATH, mock_factory=_hallucinate_one_fact_factory,
    )
    assert result.fact_recall == 1.0, (
        f"recall should stay 1.0 (no expected facts missed), got {result.fact_recall}"
    )
    assert result.fact_precision < 1.0, (
        f"precision should drop below 1.0 (extras added), got {result.fact_precision}"
    )


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


def test_real_llm_without_api_key_raises_runtime_error(monkeypatch):
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="OPENROUTER_API_KEY"):
        eq.run_inproc(gold_path=GOLD_PATH, real_llm=True)


def test_missing_gold_file_raises_file_not_found():
    with pytest.raises(FileNotFoundError):
        eq.run_inproc(gold_path="/tmp/this/does/not/exist.yaml")


def test_wrong_ontology_version_rejected(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text(
        "version: v1\n"
        "ontology_version: v999\n"
        "articles: [{id: g, title: t, summary: s, "
        "expected_entities: [], expected_facts: []}]\n"
    )
    with pytest.raises(ValueError, match="ontology_version"):
        eq.run_inproc(gold_path=str(bad))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def test_cli_main_exits_zero_in_mock_mode(capsys):
    rc = eq.main(["--gold", GOLD_PATH])
    assert rc == 0
    out = capsys.readouterr().out
    assert "extraction quality report" in out
    # Both perfect-score lines should appear in the human report.
    assert "1.000 / 1.000" in out


def test_cli_main_json_output_parses(capsys):
    rc = eq.main(["--gold", GOLD_PATH, "--json"])
    assert rc == 0
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert payload["fact_precision"] == 1.0
    assert payload["fact_recall"] == 1.0
    assert "per_relation" in payload
    for r in eq.ONTOLOGY_RELATIONS:
        assert r in payload["per_relation"]


def test_cli_main_missing_gold_returns_two(capsys):
    rc = eq.main(["--gold", "/tmp/does/not/exist.yaml"])
    assert rc == 2
    err = capsys.readouterr().out
    assert "ERROR" in err
