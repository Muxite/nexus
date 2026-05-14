"""Shared helpers for analysis scripts."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
DB_PATH = REPO / "data" / "seulgi.db"
ANALYSIS_DIR = REPO / "data" / "analysis"

sys.path.insert(0, str(REPO / "app"))
from impact_tracker import classify_missed_alpha, MISSED_ALPHA_THRESH  # noqa: E402


def open_db() -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def all_classified(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """All price_impacts rows with both delta_t5m and delta_pre_60s present."""
    return conn.execute(
        """
        SELECT * FROM price_impacts
        WHERE delta_t5m IS NOT NULL AND delta_pre_60s IS NOT NULL
        """
    ).fetchall()


__all__ = [
    "DB_PATH", "ANALYSIS_DIR", "open_db", "all_classified",
    "classify_missed_alpha", "MISSED_ALPHA_THRESH",
]
