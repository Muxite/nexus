"""Shared helpers for irene dashboard pages + plots.

`_atomic_write` (text) and `atomic_write_png` (binary) write via tempfile
+ rename so operators tailing the files never see a partial state.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _percentiles(samples: list[float]) -> dict[str, float]:
    """Small-N percentiles by sort. Returns p50/p95/p99 and count."""
    if not samples:
        return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "count": 0}
    s = sorted(samples)

    def _q(p: float) -> float:
        if not s:
            return 0.0
        idx = max(0, min(len(s) - 1, int(round((len(s) - 1) * p))))
        return s[idx]

    return {
        "p50": _q(0.50),
        "p95": _q(0.95),
        "p99": _q(0.99),
        "count": len(s),
    }


def _atomic_write(path: Path, body: str) -> None:
    """Atomic text write via rename. Pattern from seulgi/app/dashboard/io.py."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp.write_text(body)
        os.replace(tmp, path)
    except Exception as exc:
        logger.warning("dashboard: failed to write %s: %s", path, exc)
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass


def atomic_write_png(path: Path, data: bytes) -> None:
    """Atomic binary write via rename — for PNG plots."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp.write_bytes(data)
        os.replace(tmp, path)
    except Exception as exc:
        logger.warning("dashboard: failed to write %s: %s", path, exc)
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
