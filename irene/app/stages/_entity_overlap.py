"""Shared helper — does an article's resolved entities overlap a market's
question text? Cycle-1 stage-0 hard filter for the trade path; also the
substring rule that `link.py` uses for the DKG entity-overlap bonus.

Pure, testable, no DB writes. Reads from `facts`, `entities`, and
`entity_resolutions` (which are owned by `extract.py` and `resolve.py`).
Trade-path callers must add the `# lint: allow-extraction-table-ref`
marker on the SQL line that references these tables — the lint at
`scripts/lint/no_facts_in_trade_path.py` otherwise forbids it.
"""

from __future__ import annotations

from typing import Iterable

from app.db import DB

# Names shorter than this are too noisy to gate on (e.g. "US", "Iran"
# would match "USA" / "Iranian" in unrelated market questions). Same
# minimum length the DKG link stage uses for the entity-overlap bonus.
_MIN_NAME_LEN = 4


async def fetch_article_entity_names(db: DB, article_id: int) -> set[str]:
    """Canonical names of every resolved entity referenced by the article's
    facts. Returns an empty set when extraction hasn't completed yet —
    callers should treat empty as "no signal", not as "no match".
    """
    cur = await db.conn.execute(
        # lint: allow-extraction-table-ref
        """
        SELECT DISTINCT e.canonical_name
        FROM facts f
        JOIN entity_resolutions er
             ON er.entity_id IN (f.subj_entity_id, f.obj_entity_id)
        JOIN entities e ON e.id = er.resolved_to_id
        WHERE f.article_id = ?
        """,
        (article_id,),
    )
    rows = await cur.fetchall()
    await cur.close()
    names: set[str] = set()
    for r in rows:
        n = (r[0] or "").strip()
        if n:
            names.add(n)
    # Fallback: if resolutions haven't landed for this article, fall back
    # to the raw entity names off facts directly. extract.py finishes
    # before resolve.py — both run in parallel with trade.py, so the
    # race is real.
    if not names:
        cur = await db.conn.execute(
            # lint: allow-extraction-table-ref
            """
            SELECT DISTINCT e.canonical_name
            FROM facts f
            JOIN entities e ON e.id IN (f.subj_entity_id, f.obj_entity_id)
            WHERE f.article_id = ?
            """,
            (article_id,),
        )
        rows = await cur.fetchall()
        await cur.close()
        for r in rows:
            n = (r[0] or "").strip()
            if n:
                names.add(n)
    return names


def names_overlap_question(names: Iterable[str], market_question: str) -> bool:
    """True iff any name (length >= _MIN_NAME_LEN) appears as a substring
    of the market question, case-insensitive. Same rule used by the DKG
    linker's `_market_question_mentions` for consistency.
    """
    if not market_question:
        return False
    q = market_question.lower()
    for name in names:
        if not name or len(name) < _MIN_NAME_LEN:
            continue
        if name.lower() in q:
            return True
    return False


async def article_entities_overlap_market(
    db: DB, article_id: int, market_question: str
) -> tuple[bool, int]:
    """Returns (overlaps, n_entities).

    - `overlaps=True` only when at least one resolved entity name is
      present in the question. Pass this to the gate.
    - `overlaps=False, n_entities=0` is the "extraction hasn't run yet"
      case — callers should be permissive (don't reject on no signal).
    - `overlaps=False, n_entities>0` is a real entity-mismatch — reject.
    """
    names = await fetch_article_entity_names(db, article_id)
    if not names:
        return False, 0
    return names_overlap_question(names, market_question), len(names)
