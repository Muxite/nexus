"""
Per-cycle snapshot / restore for the seulgi data folder.

Why this exists
───────────────
Every cycle iterates the bot's behavior (new rules, new models, new prompts).
We want to be able to:

1. **Checkpoint everything before a risky change** so we can roll back if a
   new model or rule set proves worse than the baseline.
2. **A/B compare across model configurations** without trading state from
   one config polluting another. Each model run gets a clean trades /
   positions / engine_results slate, but inherits the slowly-built market
   index, news dedup memory, and headline history.
3. **Reproduce a past run end-to-end** — the snapshot keeps the full
   dashboard / analysis / postmortem outputs alongside the DB, so a
   restored cycle looks bit-for-bit identical to when it was saved.

Snapshot layout
───────────────
    seulgi/
      data/
        seulgi.db
        news_chroma/
        dashboard/
        analysis/
        postmortems/
      snapshots/
        <cycle-name>/
          manifest.json
          seulgi.db
          news_chroma/
          dashboard/         (only if --full)
          analysis/          (only if --full)
          postmortems/       (only if --full)

`snapshots/` is a SIBLING of `data/`, not inside it, so save operations
don't recurse.

CLI
───
    python3 -m scripts.snapshot.snapshot save <name>           # default: full
    python3 -m scripts.snapshot.snapshot save <name> --minimal # db + news_chroma only
    python3 -m scripts.snapshot.snapshot list
    python3 -m scripts.snapshot.snapshot restore <name>        # full restore
    python3 -m scripts.snapshot.snapshot restore <name> --reset-trading
        # Restore everything BUT then truncate trading state (trades,
        # positions, engine_results) so a new model starts fresh while
        # keeping the market index + news memory + headline history.
    python3 -m scripts.snapshot.snapshot diff <name>           # what would change

Safety
──────
- `restore` refuses to run if `data/seulgi.db` is currently held by another
  writer (best-effort: tries to take an exclusive lock and aborts on
  failure). Stop seulgi first: `docker compose stop seulgi`.
- File operations preserve permissions; when the DB is root-owned (typical
  in docker-bind-mount setups) you'll need sudo.
- `save` is always safe to run against a live bot — SQLite supports
  concurrent reads. The snapshot may capture a slightly-inconsistent
  point-in-time view; we use SQLite's online backup API for the DB to
  guarantee a consistent snapshot.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional


REPO = Path(__file__).resolve().parents[2]
DATA_DIR = REPO / "data"
SNAPSHOT_ROOT = REPO / "snapshots"


# Items in data/ that are always part of a snapshot (irreplaceable state).
_CORE_ITEMS = ("seulgi.db", "news_chroma")

# Items copied only in --full mode (regenerable but useful for reproducibility).
_FULL_ITEMS = ("dashboard", "analysis", "postmortems")

# Tables truncated by --reset-trading. Order matters only for FK cascades; we
# have none, so any order works.
_TRADING_TABLES = ("trades", "positions", "engine_results", "signals",
                   "price_impacts", "price_timeseries", "headline_articles",
                   "usage_events")


# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------

def _sha256_file(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            buf = f.read(chunk)
            if not buf:
                break
            h.update(buf)
    return h.hexdigest()


def _db_table_counts(db_path: Path) -> dict[str, int]:
    """Quick row-count summary for the manifest. Best-effort; skips missing tables."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        names = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()]
        counts: dict[str, int] = {}
        for n in names:
            try:
                counts[n] = conn.execute(f"SELECT COUNT(*) FROM {n}").fetchone()[0]
            except sqlite3.OperationalError:
                pass
        return counts
    finally:
        conn.close()


def _sqlite_backup(src: Path, dst: Path) -> None:
    """
    Online backup of an SQLite file. Uses the SQLite backup API so the result
    is consistent even if the source DB has a live writer.
    """
    src_conn = sqlite3.connect(f"file:{src}?mode=ro", uri=True)
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        dst.unlink()
    dst_conn = sqlite3.connect(str(dst))
    try:
        with dst_conn:
            src_conn.backup(dst_conn, pages=0)   # pages=0 → all in one call
    finally:
        dst_conn.close()
        src_conn.close()


def _zip_dir(src: Path, dst_zip: Path, *, keep_dir: bool = False) -> Path:
    """Zip `src` into `dst_zip` (uses DEFLATE). Removes `src` afterward unless keep_dir."""
    import zipfile
    dst_zip.parent.mkdir(parents=True, exist_ok=True)
    if dst_zip.exists():
        dst_zip.unlink()
    with zipfile.ZipFile(dst_zip, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for path in src.rglob("*"):
            if path.is_file():
                zf.write(path, path.relative_to(src.parent))
    if not keep_dir:
        shutil.rmtree(src)
    return dst_zip


def save(name: str, *, full: bool = True, overwrite: bool = False,
         zip: bool = False, keep_dir: bool = False) -> Path:
    """Take a snapshot of the current data folder."""
    target = SNAPSHOT_ROOT / name
    zip_path = SNAPSHOT_ROOT / f"{name}.zip"
    if target.exists() or zip_path.exists():
        if not overwrite:
            existing = zip_path if zip_path.exists() else target
            raise SystemExit(
                f"snapshot {name!r} already exists at {existing} — pass --overwrite to replace"
            )
        if target.exists():
            shutil.rmtree(target)
        if zip_path.exists():
            zip_path.unlink()
    target.mkdir(parents=True)

    # 1) DB via online backup
    db_src = DATA_DIR / "seulgi.db"
    db_dst = target / "seulgi.db"
    if not db_src.exists():
        raise SystemExit(f"no DB at {db_src} — refusing to save an empty snapshot")
    _sqlite_backup(db_src, db_dst)

    # 2) news_chroma directory (best-effort tree copy)
    chroma_src = DATA_DIR / "news_chroma"
    if chroma_src.exists():
        shutil.copytree(chroma_src, target / "news_chroma",
                        dirs_exist_ok=True, copy_function=shutil.copy2)

    # 3) Full mode: dashboard / analysis / postmortems (regenerable but
    # included so a restored snapshot is identical to the moment of save)
    if full:
        for item in _FULL_ITEMS:
            src = DATA_DIR / item
            if src.exists():
                shutil.copytree(src, target / item,
                                dirs_exist_ok=True, copy_function=shutil.copy2)

    # 4) Manifest
    manifest = {
        "name": name,
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "full": bool(full),
        "db_sha256": _sha256_file(db_dst),
        "db_size_bytes": db_dst.stat().st_size,
        "row_counts": _db_table_counts(db_dst),
        "items": sorted(p.name for p in target.iterdir()),
    }
    (target / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print(f"[snapshot] saved → {target}")
    print(f"[snapshot]   db: {manifest['db_size_bytes']/1e6:.1f} MB, "
          f"sha256={manifest['db_sha256'][:12]}…")
    print(f"[snapshot]   row counts: " + ", ".join(
        f"{k}={v}" for k, v in sorted(manifest["row_counts"].items())
        if v > 0
    ))
    if zip:
        archive = _zip_dir(target, zip_path, keep_dir=keep_dir)
        print(f"[snapshot] zipped → {archive} ({archive.stat().st_size/1e6:.1f} MB)"
              + ("" if keep_dir else "  (source dir removed)"))
        return archive
    return target


# ---------------------------------------------------------------------------
# Restore
# ---------------------------------------------------------------------------

def _ensure_db_unlocked(path: Path) -> None:
    """Try to acquire EXCLUSIVE lock on the live DB; abort if held by a writer."""
    if not path.exists():
        return    # nothing to overwrite
    try:
        conn = sqlite3.connect(str(path), timeout=2.0)
        with conn:
            conn.execute("BEGIN EXCLUSIVE")
            conn.execute("ROLLBACK")
        conn.close()
    except sqlite3.OperationalError as exc:
        raise SystemExit(
            f"refusing to restore: {path} is locked by another process ({exc}). "
            f"Stop the bot first: `docker compose stop seulgi`"
        )


def _replace_tree(src: Path, dst: Path) -> None:
    """Replace `dst` with a copy of `src` (file or directory)."""
    if dst.exists():
        if dst.is_dir():
            shutil.rmtree(dst)
        else:
            dst.unlink()
    if src.is_dir():
        shutil.copytree(src, dst, copy_function=shutil.copy2)
    else:
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)


def restore(name: str, *, reset_trading: bool = False) -> None:
    """Restore a snapshot into data/. Optionally reset model-specific state."""
    source = SNAPSHOT_ROOT / name
    if not source.is_dir():
        raise SystemExit(f"no snapshot at {source}")
    manifest_path = source / "manifest.json"
    if not manifest_path.exists():
        raise SystemExit(f"{source} missing manifest.json — refusing to restore")
    manifest = json.loads(manifest_path.read_text())

    _ensure_db_unlocked(DATA_DIR / "seulgi.db")

    # 1) DB
    src_db = source / "seulgi.db"
    dst_db = DATA_DIR / "seulgi.db"
    # Verify checksum before clobbering
    actual = _sha256_file(src_db)
    expected = manifest.get("db_sha256")
    if expected and actual != expected:
        raise SystemExit(
            f"db_sha256 mismatch in snapshot {name!r}: "
            f"manifest={expected[:12]}…, file={actual[:12]}…  (refusing)"
        )
    _replace_tree(src_db, dst_db)

    # 2) news_chroma (only if present in snapshot)
    chroma_src = source / "news_chroma"
    if chroma_src.exists():
        _replace_tree(chroma_src, DATA_DIR / "news_chroma")

    # 3) Optional full items
    for item in _FULL_ITEMS:
        src = source / item
        if src.exists():
            _replace_tree(src, DATA_DIR / item)

    print(f"[restore] data/ replaced from snapshot {name!r} "
          f"(saved {manifest.get('saved_at', '?')})")

    if reset_trading:
        cleared = _reset_trading_state(dst_db)
        print(f"[restore] --reset-trading: cleared {sum(cleared.values())} rows "
              "across " + ", ".join(f"{k}={v}" for k, v in cleared.items() if v))


def _reset_trading_state(db_path: Path) -> dict[str, int]:
    """
    Wipe per-model trading state from the restored DB so a new model starts
    fresh. Keeps: markets (universe), market embeddings, the news_chroma
    deduper. Clears: trades, positions, engine_results, signals,
    price_impacts, price_timeseries, headline_articles, usage_events.

    Rationale: trades/positions/engine_results are obvious per-model state.
    Signals + price_impacts + price_timeseries reference specific historical
    headline → market matches that were classified with the *old* model;
    keeping them would let the old model's decisions contaminate the new
    model's measurements. Same for headline_articles (captured for the old
    model's analysis) and usage_events (token accounting).
    """
    conn = sqlite3.connect(str(db_path))
    cleared: dict[str, int] = {}
    try:
        for tbl in _TRADING_TABLES:
            row = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (tbl,),
            ).fetchone()
            if not row:
                continue
            n = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            conn.execute(f"DELETE FROM {tbl}")
            cleared[tbl] = n
        conn.commit()
        # VACUUM after a big delete to actually shrink the file
        conn.execute("VACUUM")
    finally:
        conn.close()
    return cleared


# ---------------------------------------------------------------------------
# List / diff
# ---------------------------------------------------------------------------

def list_snapshots() -> list[dict]:
    if not SNAPSHOT_ROOT.exists():
        return []
    out = []
    for d in sorted(SNAPSHOT_ROOT.iterdir()):
        if not d.is_dir():
            continue
        m = d / "manifest.json"
        if not m.exists():
            out.append({"name": d.name, "error": "no manifest.json"})
            continue
        manifest = json.loads(m.read_text())
        out.append({
            "name": manifest["name"],
            "saved_at": manifest["saved_at"],
            "full": manifest.get("full", False),
            "db_size_mb": manifest["db_size_bytes"] / 1e6,
            "rows": manifest.get("row_counts", {}),
        })
    return out


def _print_list() -> None:
    rows = list_snapshots()
    if not rows:
        print("(no snapshots in", SNAPSHOT_ROOT, ")")
        return
    print(f"{'name':<28}  {'saved_at':<25}  {'full':<5}  {'db (MB)':>8}  "
          f"trades  positions  markets")
    for r in rows:
        if "error" in r:
            print(f"{r['name']:<28}  ERROR: {r['error']}")
            continue
        c = r["rows"]
        print(
            f"{r['name']:<28}  {r['saved_at']:<25}  "
            f"{'yes' if r['full'] else 'no':<5}  "
            f"{r['db_size_mb']:>8.1f}  "
            f"{c.get('trades', 0):>6}  {c.get('positions', 0):>9}  {c.get('markets', 0):>7}"
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(prog="snapshot",
                                description=__doc__.strip().splitlines()[0])
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("save", help="snapshot current data/ to snapshots/<name>/")
    sp.add_argument("name")
    sp.add_argument("--minimal", action="store_true",
                    help="DB + news_chroma only (skip dashboard/analysis/postmortems)")
    sp.add_argument("--overwrite", action="store_true",
                    help="overwrite an existing snapshot with the same name")
    sp.add_argument("--zip", action="store_true",
                    help="compress snapshot to snapshots/<name>.zip after saving")
    sp.add_argument("--keep-dir", action="store_true",
                    help="with --zip, also keep the uncompressed directory")

    sub.add_parser("list", help="list available snapshots")

    sp = sub.add_parser("restore", help="restore a snapshot into data/")
    sp.add_argument("name")
    sp.add_argument("--reset-trading", action="store_true",
                    help="restore everything but truncate per-model trading state")

    args = p.parse_args(argv)

    if args.cmd == "save":
        save(args.name, full=not args.minimal, overwrite=args.overwrite,
             zip=args.zip, keep_dir=args.keep_dir)
    elif args.cmd == "list":
        _print_list()
    elif args.cmd == "restore":
        restore(args.name, reset_trading=args.reset_trading)
    return 0


if __name__ == "__main__":
    sys.exit(main())
