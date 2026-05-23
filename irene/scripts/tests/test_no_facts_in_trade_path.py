"""Tests for the P2 trade-path lint."""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.lint import no_facts_in_trade_path as lint


def test_clean_scan_against_live_source():
    """Run the lint against the live irene/ tree — current code MUST be
    clean. If this fails, somebody added a SELECT against an extraction
    table from the trade path."""
    rc = lint.main([])
    assert rc == 0


def test_violation_detected(tmp_path):
    # Mimic the trade.py path layout under tmp.
    src_dir = tmp_path / "app" / "stages"
    src_dir.mkdir(parents=True)
    bad = src_dir / "trade.py"
    bad.write_text("async def x():\n    cur = await db.execute('SELECT * FROM facts')\n")
    # Need the other three files to exist too — the lint errors out
    # on missing files rather than scan a partial set.
    for other in (
        tmp_path / "app" / "ai.py",
        tmp_path / "app" / "stages" / "match.py",
        tmp_path / "app" / "stages" / "closer.py",
    ):
        other.parent.mkdir(parents=True, exist_ok=True)
        other.write_text("# innocent\n")
    rc = lint.main(["--root", str(tmp_path)])
    assert rc == 1


def test_allow_marker_skips(tmp_path):
    src_dir = tmp_path / "app" / "stages"
    src_dir.mkdir(parents=True)
    permitted = src_dir / "trade.py"
    permitted.write_text(
        "# in P2 the trade path never reads `FROM facts` table  "
        "# lint: allow-extraction-table-ref\n"
    )
    for other in (
        tmp_path / "app" / "ai.py",
        tmp_path / "app" / "stages" / "match.py",
        tmp_path / "app" / "stages" / "closer.py",
    ):
        other.parent.mkdir(parents=True, exist_ok=True)
        other.write_text("# innocent\n")
    rc = lint.main(["--root", str(tmp_path)])
    assert rc == 0


def test_word_boundary_avoids_false_positives(tmp_path):
    """`factsheet` should NOT trigger; only the exact table token."""
    src_dir = tmp_path / "app" / "stages"
    src_dir.mkdir(parents=True)
    f = src_dir / "trade.py"
    f.write_text("notes = 'see factsheet'\nother = 'JOIN entitiestable'\n")
    for other in (
        tmp_path / "app" / "ai.py",
        tmp_path / "app" / "stages" / "match.py",
        tmp_path / "app" / "stages" / "closer.py",
    ):
        other.parent.mkdir(parents=True, exist_ok=True)
        other.write_text("# innocent\n")
    rc = lint.main(["--root", str(tmp_path)])
    assert rc == 0


def test_missing_files_returns_2(tmp_path):
    rc = lint.main(["--root", str(tmp_path)])
    # No app/ai.py etc. → exit 2 (config error, not a violation).
    assert rc == 2


@pytest.mark.parametrize(
    "snippet",
    [
        "SELECT * FROM facts",
        "select * from FACTS",  # case-insensitive
        "JOIN entities e ON e.id = x",
        "UPDATE entity_aliases SET ...",
        "DELETE FROM extraction_runs WHERE id = 1",
    ],
)
def test_each_forbidden_pattern_caught(tmp_path, snippet):
    src_dir = tmp_path / "app" / "stages"
    src_dir.mkdir(parents=True)
    (src_dir / "trade.py").write_text(f"q = '''{snippet}'''\n")
    for other in (
        tmp_path / "app" / "ai.py",
        tmp_path / "app" / "stages" / "match.py",
        tmp_path / "app" / "stages" / "closer.py",
    ):
        other.parent.mkdir(parents=True, exist_ok=True)
        other.write_text("# innocent\n")
    rc = lint.main(["--root", str(tmp_path)])
    assert rc == 1
