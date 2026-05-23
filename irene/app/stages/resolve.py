"""
## Specification

**Purpose.** Subscriber on `TOPIC_EXTRACTION_COMPLETED` that maps each
entity surfaced by extract.py onto a canonical sibling via a
deterministic rule chain. Writes one `entity_resolutions` row per
entity touched by facts of the just-extracted article. The rule chain
runs in **EXACTLY** this order and stops at the first hit:

  1. `acronym` — case-insensitive lookup of the entity's canonical name
     against the locked acronym table (`app/stages/data/acronyms.yaml`).
     If matched, find the OTHER form's entity row (same `entity_type`,
     CI canonical match) and resolve to it.
  2. `alias`   — CI equality on `entity_aliases.alias` against other
     entities of the same `entity_type`. Single-hit only; ambiguous
     matches fall through.
  3. `suffix`  — COMPANY-only safety net: stripping a known legal
     suffix yields another COMPANY's canonical_name. (Mostly redundant
     with extract.py's write-time strip; here for legacy rows.)
  4. `self`    — no merge target. The resolution row points to the
     entity itself; confidence 1.0.

**The P2/P3 hard invariant (inherited from `docs/p2_contracts.md` §1 +
`docs/p3_contracts.md` §1).** The trade path **NEVER** reads `facts`,
`entities`, `entity_aliases`, `extraction_runs`, **nor**
`entity_resolutions`. This stage produces an overlay table queried only
by the alias-aware metric and (in P3 chunk 2) the eventize stage.

**Overlay-not-mutation model.** `facts`, `entities`, and
`entity_aliases` stay immutable (extract.py owns them, append-only).
The `entity_resolutions` table is the truth for "what's the canonical
of this entity right now?" — re-running the resolver with a new
`resolver_version` adds rows; it does not overwrite extract's writes.

**Public interface.**
- `ResolveStage` — standard stage surface:
  - `ResolveStage(deps: StageDeps)` — needs only `db` + `bus` from deps.
  - `await start()` — loads the acronym table from disk once.
  - `await stop()` — idempotent; no background tasks to cancel.
  - `await handle(payload: dict)` — subscriber on
    `TOPIC_EXTRACTION_COMPLETED`. Payload shape per
    `docs/p3_contracts.md` §3.
- `RESOLVER_VERSION` / `ACRONYM_TABLE_PATH` — class constants. The
  version bumps when the rule chain itself changes; a bump invalidates
  prior decisions on `ORDER BY resolver_version DESC`.

**Owned tables** (`docs/modularization.md` §4): `entity_resolutions`.
DDL is registered with `db.register_schema()` at import time per the
schema in `docs/p3_contracts.md` §5 (verbatim).

**Invariants.**
- `entity_resolutions.entity_id` is UNIQUE — re-firing the same payload
  is a no-op via `INSERT OR IGNORE`.
- Resolution NEVER crosses `entity_type` boundaries (ORG↔ORG only,
  COMPANY↔COMPANY only, etc.). The acronym table entries already carry
  the type; the alias and suffix rules filter by it explicitly.
- No `cycle_id` column. Entities are global; resolutions are global.
  The audit timestamp + `resolver_version` are the only diagnostics
  we need (matches `docs/p3_contracts.md` §5 schema verbatim).
- `is_tracer` / `tracer_id` are NOT carried onto resolution rows. The
  bus payload still carries the flag for downstream subscribers.
- All writes for one article share **one** `db.batch()`.
- Confidence is always 1.0 in P3 chunk 1. A future fuzzy rule would
  emit < 1.0.
- Read-only on `entities`, `entity_aliases`, `facts`. NEVER writes to a
  table owned by another stage.

**Side effects.** SQLite writes to `entity_resolutions`. Publishes
`TOPIC_RESOLUTION_COMPLETED` (one per `handle()` call where status was
`ok`; non-ok extraction payloads no-op without publishing).

**Failure modes.**
- Acronym YAML missing / malformed at `start()` → raises. A pipeline
  that can't load the rule table is a wiring bug worth crashing on.
- Extraction payload `status != 'ok'` → no entities to resolve; no
  rows written, no publish. Other terminal statuses (parse_error,
  budget_paused, http_error, empty) produced zero facts → there is
  literally nothing to resolve.

**Non-goals.**
- Fuzzy / Levenshtein matching. Deterministic-only by design.
- Cross-`entity_type` resolution.
- LLM calls. The resolver is rule-based; no model in the loop.
- Mutating `entities.status='merged'`. The overlay is the source of
  truth; `entities` rows stay active.
- Cycle stamping / tracer-flagging the resolution rows themselves.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import yaml

from app import db as db_module
from app.deps import StageDeps
from app.topics import TOPIC_EXTRACTION_COMPLETED, TOPIC_RESOLUTION_COMPLETED

logger = logging.getLogger(__name__)


_RESOLVE_DDL = [
    """
    CREATE TABLE IF NOT EXISTS entity_resolutions (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        entity_id       INTEGER NOT NULL UNIQUE REFERENCES entities(id),
        resolved_to_id  INTEGER NOT NULL REFERENCES entities(id),
        resolution_kind TEXT NOT NULL CHECK(resolution_kind IN
                            ('self','acronym','alias','suffix','manual')),
        confidence      REAL NOT NULL CHECK(confidence >= 0.0 AND confidence <= 1.0),
        resolver_version TEXT NOT NULL,
        created_at      TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_entity_resolutions_resolved_to ON entity_resolutions(resolved_to_id)",
    "CREATE INDEX IF NOT EXISTS idx_entity_resolutions_kind ON entity_resolutions(resolution_kind)",
]
db_module.register_schema("app/stages/resolve.py", _RESOLVE_DDL)


# Duplicated from extract.py rather than imported — modularization rule 2
# forbids cross-stage imports. The suffix list is a contract value; both
# copies drift together by intent, and the resolver's role is the safety
# net for entities written before extract.py acquired the strip (legacy
# rows + any future provider that bypasses extract).
_LEGAL_SUFFIXES = (
    ", Incorporated", " Incorporated",
    ", Inc.", " Inc.", ", Inc", " Inc",
    ", Corporation", " Corporation",
    ", Corp.", " Corp.", ", Corp", " Corp",
    ", Company", " Company", ", Co.", " Co.", " & Co.",
    ", Limited", " Limited", ", Ltd.", " Ltd.", " Ltd",
    ", LLC", " LLC",
    ", plc", " plc", ", PLC", " PLC",
    ", AG", " AG",
    ", S.A.", " S.A.",
    ", N.V.", " N.V.",
    " GmbH",
    ", Holdings", " Holdings",
    ", Group", " Group",
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _strip_suffix(name: str) -> Optional[str]:
    """Return the name with one known legal suffix stripped, or None if
    no suffix matched. Longer suffixes first so ", Inc." wins over " Inc."."""
    n = name.strip()
    for sfx in _LEGAL_SUFFIXES:
        if n.endswith(sfx):
            stripped = n[: -len(sfx)].rstrip(", .").strip()
            if stripped and stripped != n:
                return stripped
    return None


def _load_acronyms(path: Path) -> dict[tuple[str, str], list[str]]:
    """Parse `acronyms.yaml` into a `(entity_type, ci_name) -> [other_forms]`
    lookup. Both directions are indexed so the matcher can ask the
    question with either the short or the long form in hand.
    """
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise RuntimeError(f"acronym table at {path} is not a YAML mapping")
    rows = raw.get("acronyms") or []
    if not isinstance(rows, list):
        raise RuntimeError(f"acronym table at {path} missing 'acronyms' list")
    lookup: dict[tuple[str, str], list[str]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        etype = row.get("type")
        short = row.get("short")
        long_ = row.get("long")
        if not (isinstance(etype, str) and isinstance(short, str) and isinstance(long_, str)):
            continue
        short = short.strip()
        long_ = long_.strip()
        if not short or not long_:
            continue
        lookup.setdefault((etype, short.lower()), []).append(long_)
        lookup.setdefault((etype, long_.lower()), []).append(short)
    return lookup


class ResolveStage:
    RESOLVER_VERSION: str = "v1"
    ACRONYM_TABLE_PATH: str = "app/stages/data/acronyms.yaml"

    def __init__(self, deps: StageDeps) -> None:
        self._db = deps.db
        self._bus = deps.bus
        self._acronyms: dict[tuple[str, str], list[str]] = {}
        self._stopped = False

    # ---- lifecycle -----------------------------------------------------

    async def start(self) -> None:
        # Resolve relative to the repo root (the cwd irene runs under,
        # matching how extract.py / closer.py expect to be launched).
        path = Path(self.ACRONYM_TABLE_PATH)
        if not path.is_absolute():
            # Walk up from this file to the repo root (irene/), the parent
            # of `app/`. That makes the stage runnable regardless of the
            # caller's cwd while keeping ACRONYM_TABLE_PATH cwd-relative
            # for the docs.
            repo_root = Path(__file__).resolve().parent.parent.parent
            path = repo_root / self.ACRONYM_TABLE_PATH
        self._acronyms = _load_acronyms(path)

    async def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True

    # ---- main entry ----------------------------------------------------

    async def handle(self, payload: dict) -> None:
        status = payload.get("status")
        if status != "ok":
            # Non-ok extractions wrote zero facts → no entities to resolve.
            return
        article_id_raw = payload.get("article_id")
        if article_id_raw is None:
            logger.warning("resolve: payload missing article_id: %r", payload)
            return
        article_id = int(article_id_raw)
        extraction_run_id = payload.get("extraction_run_id")
        is_tracer = bool(payload.get("is_tracer", False))
        tracer_id = payload.get("tracer_id")

        entities = await self._fetch_unresolved_entities(article_id)
        if not entities:
            # All entities for this article already have a resolution row
            # (e.g. a re-fired payload, or another article first surfaced
            # the same entities). Publish completion so downstream still
            # gets the keep-alive.
            await self._publish_completion(
                extraction_run_id=extraction_run_id,
                article_id=article_id,
                entities_resolved=0,
                merges=0,
                is_tracer=is_tracer,
                tracer_id=tracer_id,
            )
            return

        decisions: list[tuple[int, int, str]] = []
        merges = 0
        for ent in entities:
            kind, target_id = await self._resolve_one(ent)
            decisions.append((ent["id"], target_id, kind))
            if kind != "self":
                merges += 1

        now = _now_iso()
        async with self._db.batch() as cur:
            for entity_id, resolved_to_id, kind in decisions:
                await cur.execute(
                    """
                    INSERT OR IGNORE INTO entity_resolutions
                        (entity_id, resolved_to_id, resolution_kind,
                         confidence, resolver_version, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (entity_id, resolved_to_id, kind, 1.0,
                     self.RESOLVER_VERSION, now),
                )

        await self._publish_completion(
            extraction_run_id=extraction_run_id,
            article_id=article_id,
            entities_resolved=len(decisions),
            merges=merges,
            is_tracer=is_tracer,
            tracer_id=tracer_id,
        )

    # ---- DB lookups ----------------------------------------------------

    async def _fetch_unresolved_entities(self, article_id: int) -> list[dict]:
        cur = await self._db.conn.execute(
            """
            SELECT DISTINCT e.id, e.entity_type, e.canonical_name
            FROM entities e
            JOIN facts f
              ON f.subj_entity_id = e.id OR f.obj_entity_id = e.id
            LEFT JOIN entity_resolutions r ON r.entity_id = e.id
            WHERE f.article_id = ? AND r.id IS NULL
            ORDER BY e.id
            """,
            (article_id,),
        )
        rows = await cur.fetchall()
        await cur.close()
        return [
            {
                "id": int(r["id"]),
                "entity_type": r["entity_type"],
                "canonical_name": r["canonical_name"],
            }
            for r in rows
        ]

    # ---- rule chain ----------------------------------------------------

    async def _resolve_one(self, ent: dict) -> tuple[str, int]:
        """Walk the rule chain. Returns `(resolution_kind, resolved_to_id)`."""
        target = await self._try_acronym(ent)
        if target is not None:
            return "acronym", target

        target = await self._try_alias(ent)
        if target is not None:
            return "alias", target

        target = await self._try_suffix(ent)
        if target is not None:
            return "suffix", target

        return "self", ent["id"]

    async def _try_acronym(self, ent: dict) -> Optional[int]:
        etype = ent["entity_type"]
        name = ent["canonical_name"]
        candidates = self._acronyms.get((etype, name.lower()))
        if not candidates:
            return None
        # Lowercased "other forms" — there may be more than one (the country
        # table has both `US` and `USA` pairing with `United States`).
        other_lcs = [c.lower() for c in candidates]
        # Only resolve to an entity with a LOWER id. This breaks the
        # symmetric tie when both forms exist (`FDA` and the long form
        # both find each other in the acronym table) — the lower-id row
        # self-resolves, the higher-id row points to it. Without this
        # the two would form a cycle (A→B, B→A) and `resolved_to_id`
        # wouldn't transitively yield a canonical.
        placeholders = ",".join("?" for _ in other_lcs)
        sql = (
            "SELECT id FROM entities "
            f"WHERE entity_type = ? AND id < ? "
            f"AND LOWER(canonical_name) IN ({placeholders}) "
            "ORDER BY id LIMIT 1"
        )
        cur = await self._db.conn.execute(sql, (etype, ent["id"], *other_lcs))
        row = await cur.fetchone()
        await cur.close()
        return int(row["id"]) if row else None

    async def _try_alias(self, ent: dict) -> Optional[int]:
        etype = ent["entity_type"]
        name_lc = ent["canonical_name"].lower()
        # Two ways an alias merge fires:
        #   (a) some OTHER entity has an alias whose CI value equals THIS
        #       entity's canonical_name, OR
        #   (b) THIS entity has an alias whose CI value equals SOME OTHER
        #       entity's canonical_name.
        # Either way the "share an alias" condition is satisfied by §4
        # of p3_contracts.md. Singletons only — ambiguity falls through.
        cur = await self._db.conn.execute(
            """
            SELECT DISTINCT e2.id
            FROM entities e2
            WHERE e2.entity_type = ?
              AND e2.id != ?
              AND (
                EXISTS (
                  SELECT 1 FROM entity_aliases a2
                  WHERE a2.entity_id = e2.id AND LOWER(a2.alias) = ?
                )
                OR EXISTS (
                  SELECT 1 FROM entity_aliases a1
                  WHERE a1.entity_id = ? AND LOWER(a1.alias) = LOWER(e2.canonical_name)
                )
              )
            """,
            (etype, ent["id"], name_lc, ent["id"]),
        )
        rows = await cur.fetchall()
        await cur.close()
        if len(rows) != 1:
            return None
        other_id = int(rows[0]["id"])
        # Lower-id-wins symmetry break, same reason as `_try_acronym`:
        # otherwise A↔B would each merge into the other.
        if other_id >= ent["id"]:
            return None
        return other_id

    async def _try_suffix(self, ent: dict) -> Optional[int]:
        if ent["entity_type"] != "COMPANY":
            return None
        stripped = _strip_suffix(ent["canonical_name"])
        if stripped is None:
            return None
        cur = await self._db.conn.execute(
            """
            SELECT id FROM entities
            WHERE entity_type = 'COMPANY'
              AND id != ?
              AND LOWER(canonical_name) = ?
            ORDER BY id LIMIT 1
            """,
            (ent["id"], stripped.lower()),
        )
        row = await cur.fetchone()
        await cur.close()
        return int(row["id"]) if row else None

    # ---- publish -------------------------------------------------------

    async def _publish_completion(
        self,
        *,
        extraction_run_id: Optional[int],
        article_id: int,
        entities_resolved: int,
        merges: int,
        is_tracer: bool,
        tracer_id: Optional[str],
    ) -> None:
        await self._bus.publish(
            TOPIC_RESOLUTION_COMPLETED,
            {
                "extraction_run_id": extraction_run_id,
                "article_id": article_id,
                "entities_resolved": entities_resolved,
                "merges": merges,
                "is_tracer": is_tracer,
                "tracer_id": tracer_id,
            },
        )


if __name__ == "__main__":
    # Per modularization rule 8: every stage runs standalone for dev poking.
    import sys

    print(
        "ResolveStage instantiates with only db + bus from deps; "
        "wire via app.main.",
        file=sys.stderr,
    )
    _ = TOPIC_EXTRACTION_COMPLETED  # silence import-unused under the lint
