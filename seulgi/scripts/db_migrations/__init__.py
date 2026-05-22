"""One-shot DB migration scripts runnable against the live seulgi.db.

Each module exposes a `migrate(db_path) -> dict` function that is
idempotent (safe to re-run) and a `main()` CLI entrypoint. Schema-level
migrations also live in `app/db.py::_MIGRATIONS` for fresh DBs (tests,
replay) — the scripts here are the explicit "apply now without
restarting the writer" path.
"""
