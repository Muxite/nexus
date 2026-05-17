"""§D.6 — minimal alert sink with two outputs:
  1. SQLite `alerts` table (durable, queryable from analysis)
  2. append-only data/dashboard/alerts.txt (tail-friendly)

The notifier never holds locks across IO so a wedged file system can't
halt trading. An optional relay process (scripts/notify/relay.py) tails
the file and forwards lines to a webhook / Telegram bot — kept out-of-
process so a relay failure can never cascade into the trading loop.
"""
from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class Notifier:
    def __init__(
        self,
        *,
        db_path: Optional[Path] = None,
        log_path: Optional[Path] = None,
    ) -> None:
        self._db_path = Path(db_path) if db_path else None
        self._log_path = Path(log_path) if log_path else None
        self._lock = asyncio.Lock()
        self._initialised = False

    def _ensure_table(self) -> None:
        if not self._db_path or self._initialised:
            return
        try:
            conn = sqlite3.connect(str(self._db_path), timeout=2.0)
            conn.execute(
                """CREATE TABLE IF NOT EXISTS alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    level TEXT NOT NULL,
                    source TEXT NOT NULL,
                    msg TEXT NOT NULL
                )"""
            )
            conn.commit()
            conn.close()
            self._initialised = True
        except Exception as exc:  # noqa: BLE001
            logger.warning("notifier table init failed: %s", exc)

    async def alert(self, level: str, source: str, msg: str) -> None:
        ts = datetime.now(timezone.utc).isoformat()
        line = f"[{ts}] {level.upper()} {source}: {msg}"
        # Always log to stdout
        if level.lower() in ("error", "critical"):
            logger.error("ALERT: %s", line)
        else:
            logger.warning("ALERT: %s", line)
        async with self._lock:
            # File sink
            if self._log_path is not None:
                try:
                    self._log_path.parent.mkdir(parents=True, exist_ok=True)
                    with self._log_path.open("a") as f:
                        f.write(line + "\n")
                except Exception as exc:  # noqa: BLE001
                    logger.warning("alert file sink failed: %s", exc)
            # DB sink (best-effort)
            if self._db_path is not None:
                self._ensure_table()
                try:
                    conn = sqlite3.connect(str(self._db_path), timeout=2.0)
                    conn.execute(
                        "INSERT INTO alerts (ts, level, source, msg) VALUES (?,?,?,?)",
                        (ts, level, source, msg),
                    )
                    conn.commit()
                    conn.close()
                except Exception as exc:  # noqa: BLE001
                    logger.warning("alert DB sink failed: %s", exc)


# Module-level singleton used by aggressive_core observability hook
_default: Optional[Notifier] = None


def get_default_notifier(
    *, db_path: Optional[Path] = None, log_path: Optional[Path] = None,
) -> Notifier:
    global _default
    if _default is None:
        _default = Notifier(db_path=db_path, log_path=log_path)
    return _default
