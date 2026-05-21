"""
Connection / schema audit.

Read-only inspection of the seulgi.db schema, focused on the constructs
that can cause `insert_trade` / `open_position` to fail silently:

- Foreign-key relationships (so you can see what `open_position` could
  fail on if the trade row isn't yet visible)
- UNIQUE / PRIMARY KEY constraints (so you can see what an INSERT could
  conflict on)
- Indexes covering `decided_at` (verify the perf indexes added in
  the 2026-05-20 work are still in place)
- All triggers (silent rollback can be triggered by RAISE in a trigger)
- Current PRAGMA values

Output: Markdown — pipe to a file under `seulgi/reports/`.

Usage
─────
    python3 seulgi/scripts/analysis/reports/connection_audit.py
    python3 seulgi/scripts/analysis/reports/connection_audit.py --out seulgi/reports/connection_audit.md
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from datetime import datetime, timezone


PRAGMAS_TO_REPORT = (
    "journal_mode", "synchronous", "busy_timeout", "wal_autocheckpoint",
    "foreign_keys", "page_size", "cache_size", "mmap_size", "temp_store",
)


def _connect_ro(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def section(title: str) -> str:
    return f"\n## {title}\n\n"


def table_list(conn: sqlite3.Connection) -> list[str]:
    return [r["name"] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name NOT LIKE 'sqlite_%' ORDER BY name"
    )]


def fk_for(conn: sqlite3.Connection, table: str) -> list[dict]:
    return [dict(r) for r in conn.execute(f"PRAGMA foreign_key_list({table})")]


def index_for(conn: sqlite3.Connection, table: str) -> list[dict]:
    idxs: list[dict] = []
    for r in conn.execute(f"PRAGMA index_list({table})"):
        keys = r.keys()
        cols = [c["name"] for c in conn.execute(f"PRAGMA index_info({r['name']})")]
        idxs.append({
            "name": r["name"],
            "unique": r["unique"],
            "columns": cols,
            "origin": r["origin"] if "origin" in keys else None,
        })
    return idxs


def column_info(conn: sqlite3.Connection, table: str) -> list[dict]:
    return [dict(r) for r in conn.execute(f"PRAGMA table_info({table})")]


def triggers_for(conn: sqlite3.Connection, table: str) -> list[dict]:
    return [dict(r) for r in conn.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='trigger' AND tbl_name=?",
        (table,),
    )]


def render(conn: sqlite3.Connection) -> str:
    lines: list[str] = [
        f"# Connection / schema audit — {datetime.now(timezone.utc).isoformat()}",
        "",
    ]
    tables = table_list(conn)
    lines.append(f"Tables: {len(tables)}")
    lines.append("")

    lines.append(section("PRAGMAs").strip())
    for p in PRAGMAS_TO_REPORT:
        row = conn.execute(f"PRAGMA {p}").fetchone()
        if row is not None:
            lines.append(f"- {p} = {row[0]}")

    lines.append(section("Foreign keys").strip())
    found_any_fk = False
    for tbl in tables:
        fks = fk_for(conn, tbl)
        if not fks:
            continue
        found_any_fk = True
        lines.append(f"### {tbl}")
        for fk in fks:
            lines.append(
                f"- {fk.get('from')} → {fk.get('table')}.{fk.get('to')}  "
                f"on_update={fk.get('on_update')} on_delete={fk.get('on_delete')}"
            )
    if not found_any_fk:
        lines.append("(no foreign keys declared)")

    lines.append(section("Indexes (only non-implicit)").strip())
    for tbl in tables:
        idxs = [i for i in index_for(conn, tbl) if i.get("origin") != "pk"]
        if not idxs:
            continue
        lines.append(f"### {tbl}")
        for i in idxs:
            uniq = "UNIQUE " if i["unique"] else ""
            cols = ", ".join(i["columns"])
            lines.append(f"- {uniq}{i['name']} ({cols})")

    lines.append(section("Columns referencing `decided_at` / `_at`").strip())
    for tbl in tables:
        cols = column_info(conn, tbl)
        ts_cols = [c["name"] for c in cols if c["name"].endswith("_at") or c["name"] == "ts"]
        if not ts_cols:
            continue
        lines.append(f"- {tbl}: {', '.join(ts_cols)}")

    lines.append(section("Triggers").strip())
    found_any = False
    for tbl in tables:
        trigs = triggers_for(conn, tbl)
        for t in trigs:
            found_any = True
            lines.append(f"### {tbl}.{t['name']}")
            lines.append("```sql")
            lines.append(t["sql"] or "(no sql captured)")
            lines.append("```")
    if not found_any:
        lines.append("(no triggers defined)")

    lines.append(section("Row counts (cheap)").strip())
    for tbl in tables:
        try:
            n = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            lines.append(f"- {tbl}: {n:,}")
        except sqlite3.OperationalError as exc:
            lines.append(f"- {tbl}: error — {exc}")

    return "\n".join(lines) + "\n"


def main() -> int:
    p = argparse.ArgumentParser(description="Connection/schema audit")
    p.add_argument("--db", default="seulgi/data/seulgi.db")
    p.add_argument("--out", default=None)
    args = p.parse_args()

    if not os.path.exists(args.db):
        print(f"ERROR: db not found at {args.db}", file=sys.stderr)
        return 2

    conn = _connect_ro(args.db)
    try:
        text = render(conn)
    finally:
        conn.close()
    if args.out:
        with open(args.out, "w") as f:
            f.write(text)
    sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
