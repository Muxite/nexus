"""
## Specification

**Purpose.** One-shot cleanup for the cycle-12/18 active-flicker bug
(handoff D — `irene/handoffs/2026-05-26-D-active-flicker.md`). Reconciles
the contradictory rows where `markets.active=1 AND markets.closed_at IS
NOT NULL` simultaneously by forcing `active=0`.

As of 2026-05-26 the live DB had ~1,022 such rows (≈10 percent of the
active universe). The structural fix that prevents new flickers ships in
`app/stages/markets.py:_upsert_market` (cycle-24 SQL `CASE` in the
ON CONFLICT block). This script reconciles the existing backlog so the
new gate logic in handoff C (`_gate_market_state`) sees a consistent
world the first time it runs.

**Public interface.**
- CLI: ``python -m scripts.migrate.2026_05_26_active_flicker
        --irene-db <path> [--dry-run]``
- ``run(db_path: str, *, dry_run: bool = False) -> int`` for
  programmatic use (returns the number of rows updated).

**Invariants.**
- Idempotent. Re-runs after the first apply update zero rows because the
  SQL `CASE` upsert prevents any new flickers from being written.
- Pure UPDATE — no inserts, no deletes, no schema change. Safe to run
  while the bot is up (single statement, single transaction).
- Does NOT touch `closed_at`, `last_outcome_prices`, or any other
  column — only flips `active` from 1 → 0 where `closed_at IS NOT NULL`.

**Failure modes.**
- Refuses to run against a seulgi-shaped path
  (`app.db._assert_not_seulgi`).
- Bubbles `sqlite3.OperationalError` if the file is unreadable / locked.

**Non-goals.**
- Does NOT bootstrap the schema. Run after `app.db.DB.connect()` (or
  any normal irene boot) has been at the path at least once.
- Does NOT log to the `schema_migrations` table — this is data
  reconciliation, not a DDL migration. Re-running is intentionally cheap
  and a no-op.
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys

from app.db import _assert_not_seulgi

logger = logging.getLogger("scripts.migrate.active_flicker")


_RECONCILE_SQL = """
UPDATE markets
   SET active = 0
 WHERE active = 1
   AND closed_at IS NOT NULL
"""


def run(db_path: str, *, dry_run: bool = False) -> int:
    """Reconcile flickered rows. Returns the number of rows that
    were (or would have been, when ``dry_run=True``) updated."""
    _assert_not_seulgi(db_path)
    conn = sqlite3.connect(db_path)
    try:
        # Count first so we can report the impact regardless of mode.
        cur = conn.execute(
            "SELECT COUNT(*) FROM markets "
            "WHERE active = 1 AND closed_at IS NOT NULL"
        )
        (n_flickered,) = cur.fetchone()
        cur.close()
        logger.info(
            "active-flicker: %d row(s) currently have "
            "active=1 AND closed_at IS NOT NULL",
            n_flickered,
        )
        if dry_run:
            logger.info("active-flicker: --dry-run, no UPDATE issued")
            return int(n_flickered)
        cur = conn.execute(_RECONCILE_SQL)
        updated = cur.rowcount
        cur.close()
        conn.commit()
        logger.info(
            "active-flicker: UPDATE complete — %d row(s) flipped to active=0",
            updated,
        )
        return int(updated)
    finally:
        conn.close()


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Cycle-24 cleanup: reconcile markets where active=1 AND "
            "closed_at IS NOT NULL by forcing active=0. Idempotent."
        ),
    )
    p.add_argument(
        "--irene-db",
        required=True,
        help="Path to the irene SQLite DB (e.g. /data/irene.db).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Report the count without issuing the UPDATE.",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = _build_parser().parse_args(argv)
    run(args.irene_db, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
