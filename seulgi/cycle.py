"""
Seulgi cycle orchestrator — one script for the whole develop / save / iterate
workflow.

Common commands
───────────────
  python3 cycle.py status                   # quick health snapshot of the live bot
  python3 cycle.py save <name>              # snapshot current data/ under that name
  python3 cycle.py list                     # show every saved snapshot
  python3 cycle.py cycle <name>             # snapshot+restart: save current state under <name>,
                                            # reset per-model trading state but keep the market
                                            # universe and news memory, then restart the bot.
                                            # This is the killer command for iterating models/rules.
  python3 cycle.py cycle <name> --fresh-markets
                                            # ... but also forget the market universe (use when the
                                            # market-finding code itself changed).
  python3 cycle.py postmortem [--no-ai]     # generate a fresh post-mortem
  python3 cycle.py rebuild                  # docker compose build seulgi (after app/* edits)
  python3 cycle.py restore <name>           # full byte-identical rollback to a snapshot
  python3 cycle.py restart                  # stop + start the bot

Notes
─────
- Snapshot save/restore use the SQLite online backup API, so save is safe
  against a live bot. restore stops the bot for you.
- Auto-sources seulgi/.env so OPENROUTER_API_KEY reaches the post-mortem
  recommender without manual `set -a; source .env`.
- Auto-invokes sudo for the ops that touch the root-owned DB (snapshot
  save/restore, tag_market_types). You'll get a sudo prompt the first time
  in a session.
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path


REPO = Path(__file__).resolve().parent           # the seulgi/ directory
PROJECT_ROOT = REPO.parent                       # /home/muk/projects/jennie
ENV_FILE = REPO / ".env"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_env() -> None:
    """Source seulgi/.env into our process so subprocesses inherit it."""
    if not ENV_FILE.exists():
        return
    for line in ENV_FILE.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def _run(cmd: list[str], *, cwd: Path = REPO, check: bool = True,
         capture: bool = False) -> subprocess.CompletedProcess:
    """Run a subprocess with sensible defaults; print the command first."""
    pretty = " ".join(c if " " not in c else f'"{c}"' for c in cmd)
    print(f"  $ {pretty}")
    return subprocess.run(
        cmd, cwd=cwd, check=check,
        capture_output=capture, text=capture,
    )


def _docker(*args: str, cwd: Path = PROJECT_ROOT, check: bool = True) -> subprocess.CompletedProcess:
    """docker compose <args>, always run from the project root."""
    return _run(["docker", "compose", *args], cwd=cwd, check=check)


def _sudo_py(module: str, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    """Invoke a snapshot/tagger module via sudo so it can read/write the root-owned DB."""
    return _run(["sudo", "-E", sys.executable, "-u", "-m", module, *args], check=check)


def _py(module: str, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    return _run([sys.executable, "-u", "-m", module, *args], check=check)


def _bot_is_running() -> bool:
    out = subprocess.run(
        ["docker", "ps", "--format", "{{.Names}}"],
        cwd=PROJECT_ROOT, capture_output=True, text=True,
    )
    return "jennie-seulgi" in out.stdout


def _wait_for_bot_ready(timeout: float = 90.0) -> bool:
    """Poll docker logs until the bot writes the rules-loaded line, or timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        out = subprocess.run(
            ["docker", "compose", "logs", "--tail", "200", "seulgi"],
            cwd=PROJECT_ROOT, capture_output=True, text=True,
        )
        if "rules] loaded" in out.stdout or "Seulgi starting up" in out.stdout:
            # Give the index refresh a moment to finish so first-tick noise is past.
            time.sleep(3)
            return True
        time.sleep(2)
    return False


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_status(args: argparse.Namespace) -> int:
    print("── docker ──")
    _docker("ps", "--format", "table {{.Names}}\t{{.Status}}", check=False)
    print()
    # system-health exits non-zero when something looks unhealthy (scraper
    # unreachable from the *host* is a known false negative — Docker DNS
    # only resolves inside the container). Don't propagate that to our caller.
    print("── system-health (informational; non-zero exit is OK) ──")
    _sudo_py("app.debug", "system-health", check=False)
    print()
    print("── recent-skips (last 200) ──")
    _sudo_py("app.debug", "recent-skips", "-n", "200", check=False)
    return 0


def cmd_save(args: argparse.Namespace) -> int:
    extra = []
    if args.minimal:
        extra.append("--minimal")
    if args.overwrite:
        extra.append("--overwrite")
    _sudo_py("scripts.snapshot.snapshot", "save", args.name, *extra)
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    _sudo_py("scripts.snapshot.snapshot", "list")
    return 0


def cmd_restore(args: argparse.Namespace) -> int:
    if _bot_is_running():
        print("[cycle] stopping seulgi for full restore…")
        _docker("stop", "seulgi")
    extra = ["--reset-trading"] if args.reset_trading else []
    _sudo_py("scripts.snapshot.snapshot", "restore", args.name, *extra)
    print("[cycle] starting seulgi…")
    _docker("start", "seulgi")
    return 0


def cmd_cycle(args: argparse.Namespace) -> int:
    """
    Snapshot current state, open a new cycle row, restart the bot.

    Cycle-8 change: this no longer DELETES trading data. Instead it inserts a
    new row into the `cycles` table whose `started_at` becomes the live
    dashboard's window — all PnL/staked counters narrow to "since this cycle
    started". Historical trades stay in the DB so you can SQL-trace any
    prior cycle directly without rehydrating a snapshot.

    `--fresh-markets` still wipes the market index + news_chroma (for cycles
    that re-discover the market universe).
    """
    # 1. Snapshot the state at cycle boundary so the byte-identical copy is
    #    always available even if someone manually mutates the live DB later.
    print(f"[cycle] 1/4 snapshotting state as {args.name!r}…")
    save_args = [args.name]
    if args.minimal:
        save_args.append("--minimal")
    save_args.append("--overwrite")
    _sudo_py("scripts.snapshot.snapshot", "save", *save_args)

    # 2. Stop the bot so the cycle row update happens against a quiet DB.
    if _bot_is_running():
        print("[cycle] 2/4 stopping seulgi…")
        _docker("stop", "seulgi")
    else:
        print("[cycle] 2/4 seulgi already stopped")

    # 3. Close the current cycle row and open a new one. Inline SQL keeps the
    #    cycle.py script standalone — no need to import the SeulgiDB module.
    db_path = REPO / "data" / "seulgi.db"
    print(f"[cycle] 3/4 opening cycle row {args.name!r} (no data deleted)…")
    _run([
        "sudo", sys.executable, "-c",
        "import sqlite3, datetime as dt; "
        f"c=sqlite3.connect(r{str(db_path)!r}); "
        "now=dt.datetime.now(dt.timezone.utc).isoformat(); "
        "c.execute(\"CREATE TABLE IF NOT EXISTS cycles ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  name TEXT NOT NULL, started_at TEXT NOT NULL,"
        "  ended_at TEXT, notes TEXT)\"); "
        "c.execute('UPDATE cycles SET ended_at = ? WHERE ended_at IS NULL', (now,)); "
        f"c.execute('INSERT INTO cycles (name, started_at) VALUES (?, ?)', ({args.name!r}, now)); "
        "c.commit(); "
        "row=c.execute('SELECT id, name, started_at FROM cycles WHERE ended_at IS NULL').fetchone(); "
        "print(f'  new cycle id={row[0]} name={row[1]} started_at={row[2]}'); "
        "c.close()",
    ])

    if args.fresh_markets:
        chroma_path = REPO / "data" / "news_chroma"
        print("[cycle]   --fresh-markets: clearing markets table + news_chroma…")
        _run(["sudo", "rm", "-rf", str(chroma_path)])
        _run([
            "sudo", sys.executable, "-c",
            "import sqlite3; "
            f"c=sqlite3.connect(r{str(db_path)!r}); "
            "c.execute('DELETE FROM markets'); c.commit(); c.close(); "
            "print('  markets table cleared')",
        ])

    # 4. Restart and wait until it's healthy
    print("[cycle] 4/4 starting seulgi…")
    _docker("start", "seulgi")
    if _wait_for_bot_ready():
        print("[cycle] bot is up.")
    else:
        print("[cycle] WARN: bot did not log ready line within timeout — "
              "check `docker compose logs seulgi`")

    print()
    print(f"[cycle] done. snapshot {args.name!r} saved; cycle row inserted. "
          f"Dashboard now shows 'since {args.name}' totals. "
          "Past trades remain in DB for SQL trace.")
    print("[cycle] suggested next steps:")
    print("[cycle]   python3 cycle.py status         # confirm rules + scraper")
    print("[cycle]   python3 cycle.py postmortem     # after ~30 min of new data")
    return 0


def cmd_postmortem(args: argparse.Namespace) -> int:
    extra = []
    if args.no_ai:
        extra.append("--no-ai")
    if args.recommender_model:
        extra += ["--recommender-model", args.recommender_model]
    _py("scripts.postmortem.run_postmortem", *extra)
    return 0


def cmd_rebuild(args: argparse.Namespace) -> int:
    _docker("build", "seulgi")
    print("[cycle] image rebuilt. Run `python3 cycle.py restart` to apply.")
    return 0


def cmd_restart(args: argparse.Namespace) -> int:
    if _bot_is_running():
        _docker("stop", "seulgi")
    _docker("start", "seulgi")
    if _wait_for_bot_ready():
        print("[cycle] bot is up.")
    return 0


def cmd_tag_markets(args: argparse.Namespace) -> int:
    """Re-run the market_type tagger. Usually only needed after editing the regex."""
    _sudo_py("scripts.postmortem.tag_market_types")
    return 0


# ---------------------------------------------------------------------------
# argparse
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="cycle",
                                description=__doc__.strip().splitlines()[0])
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("status", help="quick health snapshot of the live bot")
    sp.set_defaults(func=cmd_status)

    sp = sub.add_parser("save", help="snapshot current data/ under a name")
    sp.add_argument("name")
    sp.add_argument("--minimal", action="store_true",
                    help="DB + news_chroma only (skip dashboard/analysis/postmortems)")
    sp.add_argument("--overwrite", action="store_true")
    sp.set_defaults(func=cmd_save)

    sp = sub.add_parser("list", help="list saved snapshots")
    sp.set_defaults(func=cmd_list)

    sp = sub.add_parser("cycle",
                        help="save current state, reset trading, restart bot")
    sp.add_argument("name", help="name to save the current state under")
    sp.add_argument("--fresh-markets", action="store_true",
                    help="also clear the market universe + news memory")
    sp.add_argument("--minimal", action="store_true",
                    help="minimal snapshot (DB + news_chroma only)")
    sp.set_defaults(func=cmd_cycle)

    sp = sub.add_parser("restore", help="full rollback to a saved snapshot")
    sp.add_argument("name")
    sp.add_argument("--reset-trading", action="store_true",
                    help="restore but truncate per-model trading state")
    sp.set_defaults(func=cmd_restore)

    sp = sub.add_parser("postmortem", help="generate a fresh post-mortem")
    sp.add_argument("--no-ai", action="store_true",
                    help="skip the LLM recommender call")
    sp.add_argument("--recommender-model", default=None,
                    help="override the OpenRouter model used for synthesis")
    sp.set_defaults(func=cmd_postmortem)

    sp = sub.add_parser("rebuild", help="rebuild the seulgi docker image")
    sp.set_defaults(func=cmd_rebuild)

    sp = sub.add_parser("restart", help="stop + start the seulgi container")
    sp.set_defaults(func=cmd_restart)

    sp = sub.add_parser("tag-markets",
                        help="re-tag markets.market_type via the regex tagger")
    sp.set_defaults(func=cmd_tag_markets)

    return p


def main(argv=None) -> int:
    _load_env()
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except subprocess.CalledProcessError as exc:
        print(f"[cycle] command failed (exit {exc.returncode}): "
              f"{' '.join(exc.cmd) if exc.cmd else '?'}", file=sys.stderr)
        return exc.returncode or 1
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
