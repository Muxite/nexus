"""Regression guard: the page registry must not reintroduce any of the
files culled in Phase 1 A1.5 (2026-05-22) — see
seulgi/reports/2026-05-22-dashboard-cull-plan.md.
"""

from __future__ import annotations

from dashboard.registry import A1_5_CULLED_FILES, PAGES


def test_no_culled_files_in_registry():
    registered = {p.FILENAME for p in PAGES}
    leaks = registered & A1_5_CULLED_FILES
    assert not leaks, (
        f"page registry reintroduces A1.5-culled files: {sorted(leaks)}. "
        "If you intend to undo the cull, remove the file from "
        "A1_5_CULLED_FILES in dashboard/registry.py and update the cull report."
    )


def test_no_duplicate_filenames():
    registered = [p.FILENAME for p in PAGES]
    assert len(registered) == len(set(registered)), (
        f"page registry has duplicate FILENAMEs: {sorted(registered)}"
    )


def test_pages_have_required_attrs():
    for page in PAGES:
        assert hasattr(page, "FILENAME"), f"{page.__name__} missing FILENAME"
        assert hasattr(page, "render"), f"{page.__name__} missing render()"
        assert isinstance(page.FILENAME, str) and page.FILENAME.endswith(".txt"), (
            f"{page.__name__} FILENAME must be a .txt string"
        )
