"""Shared helpers for `python -m scripts.analysis <subcommand>`.

Pattern lifted from seulgi/scripts/analysis/_common.py. Every analysis
subcommand opens a read-only SQLite connection, accepts a `--since`
window, and returns a `Section` (see `_format.py`) that the top-level
dispatcher renders to text / md / json.
"""

from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional


def add_common_args(p: argparse.ArgumentParser) -> None:
    """Standard flags every subcommand inherits."""
    p.add_argument(
        "--db",
        default="/data/irene.db",
        help="Path to irene.db (default: /data/irene.db).",
    )
    p.add_argument(
        "--since",
        default="1h",
        help="Window to scan. Accepts ISO-8601 absolute (2026-05-23T00:00:00Z) "
             "or a relative shorthand like 1h / 30m / 24h / 7d. Default: 1h.",
    )
    p.add_argument(
        "--format",
        choices=("text", "md", "json"),
        default="text",
    )


def parse_since(s: str) -> Optional[str]:
    """Convert `--since` to an ISO-8601 cutoff. None means no window."""
    if not s:
        return None
    s = s.strip()
    if s.endswith("h") and s[:-1].isdigit():
        return (datetime.now(timezone.utc) - timedelta(hours=int(s[:-1]))).isoformat()
    if s.endswith("m") and s[:-1].isdigit():
        return (datetime.now(timezone.utc) - timedelta(minutes=int(s[:-1]))).isoformat()
    if s.endswith("d") and s[:-1].isdigit():
        return (datetime.now(timezone.utc) - timedelta(days=int(s[:-1]))).isoformat()
    # Treat anything else as an absolute ISO timestamp.
    return s


def open_db(path: str) -> sqlite3.Connection:
    """Open irene.db read-only. Raises if the file doesn't exist — analysis
    against a fresh empty DB is silly; let the analyst fix the path."""
    if not Path(path).exists():
        raise FileNotFoundError(f"DB not found: {path}")
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn
