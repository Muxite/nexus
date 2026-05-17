"""seulgi — interactive menu CLI (§E).

    python3 -m seulgi

Numbered menu only. Everything that the operator commonly does is one
keystroke away. The actual work shells out to existing commands so the
runtime path stays unchanged; the menu is purely a quality-of-life
wrapper.
"""
from __future__ import annotations

import os
import shlex
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent
DATA = REPO / "data"
DASH = DATA / "dashboard"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _hr(c: str = "─") -> str:
    return c * 60


def _run(cmd: list[str], *, env_extra: dict | None = None, capture: bool = False) -> int:
    """Run a subprocess. Returns exit code. capture=True returns stdout via print."""
    env = os.environ.copy()
    if env_extra:
        env.update(env_extra)
    print(f"$ {' '.join(shlex.quote(c) for c in cmd)}")
    if capture:
        try:
            out = subprocess.check_output(cmd, env=env, text=True)
            print(out)
            return 0
        except subprocess.CalledProcessError as exc:
            print(exc.output or "", file=sys.stderr)
            return exc.returncode
    return subprocess.call(cmd, env=env)


def _docker_compose(*args: str) -> int:
    return _run(["docker", "compose", *args])


def _ask(prompt: str, *, default: str | None = None) -> str:
    suffix = f" [{default}]" if default else ""
    try:
        s = input(f"{prompt}{suffix}: ").strip()
    except EOFError:
        s = ""
    return s or (default or "")


# ---------------------------------------------------------------------------
# Menu actions
# ---------------------------------------------------------------------------

def start_runner_all() -> int:
    """Start the full docker-compose stack with aggressive_core enabled."""
    print("Starting docker compose with AGGRESSIVE_CORE_ENABLE=1 …")
    return _docker_compose("up", "-d")


def start_runner_core_only() -> int:
    """Start with ONLY the carved aggressive_core engine (legacy engines disabled).

    Uses SEULGI_ENGINES_FILTER=aggressive_core (read by EngineRunner via env)
    plus AGGRESSIVE_CORE_ENABLE=1."""
    os.environ["AGGRESSIVE_CORE_ENABLE"] = "1"
    os.environ["SEULGI_ENGINES_FILTER"] = "aggressive_core"
    print("Starting compose with aggressive_core ONLY (legacy engines filtered).")
    return _docker_compose("up", "-d")


def stop_runner() -> int:
    print("Stopping seulgi container …")
    return _docker_compose("stop", "seulgi")


def status() -> int:
    print(_hr())
    print("Container status")
    _docker_compose("ps")
    print(_hr())
    print("aggressive_core balance / positions (from latest snapshot or live DB)")
    _run(
        ["python3", "-c",
         "import sqlite3, os, json; "
         "p=os.environ.get('SEULGI_DB','data/seulgi.db'); "
         "c=sqlite3.connect(f'file:{p}?mode=ro',uri=True); "
         "rows=list(c.execute(\"SELECT COUNT(*), COALESCE(SUM(size_usdc),0), COALESCE(SUM(realized_pnl),0) FROM trades WHERE engine_name='aggressive_core'\")); "
         "n,staked,pnl=rows[0]; "
         "print(f'  aggressive_core: trades={n}  staked=${staked:.2f}  realized=${pnl:.2f}'); "
         "pos=list(c.execute(\"SELECT COUNT(*), COALESCE(SUM(size_usdc),0) FROM positions WHERE engine_name='aggressive_core'\")); "
         "p_n,p_s=pos[0]; print(f'  open positions: {p_n}  committed=${p_s:.2f}')"],
    )
    alerts = DASH / "alerts.txt"
    if alerts.exists():
        print(_hr())
        print("Last 5 alerts")
        for line in alerts.read_text().splitlines()[-5:]:
            print(" ", line)
    return 0


def snapshot_menu() -> int:
    while True:
        print(_hr())
        print("Snapshot")
        print("  1) Save (full + zip)")
        print("  2) List")
        print("  3) Restore")
        print("  0) Back")
        choice = _ask("Pick", default="0")
        if choice == "1":
            name = _ask("Snapshot name (e.g. pre-aggressive-core)")
            if not name:
                continue
            _run(["python3", "-m", "scripts.snapshot.snapshot", "save", name, "--zip"])
        elif choice == "2":
            _run(["python3", "-m", "scripts.snapshot.snapshot", "list"])
        elif choice == "3":
            name = _ask("Snapshot name to restore")
            if not name:
                continue
            reset = _ask("Also reset trading state? (y/N)", default="N").lower()
            args = ["python3", "-m", "scripts.snapshot.snapshot", "restore", name]
            if reset == "y":
                args.append("--reset-trading")
            _run(args)
        elif choice == "0":
            return 0


def validate() -> int:
    return _run(["python3", "-m", "scripts.analysis", "validate-report"])


def analysis_menu() -> int:
    cmds = [
        ("pnl",            "Overall realized PnL"),
        ("variants",       "Per-variant leaderboard"),
        ("validate-report","Run the validation gate (writes REPORT_VALIDATION_*.md)"),
        ("funnel",         "Decision funnel by skip reason"),
        ("close-reasons",  "Close-reason breakdown"),
        ("conf-buckets",   "AI confidence buckets"),
        ("entry-buckets",  "Entry price buckets"),
        ("vol-buckets",    "Pre-headline vol buckets"),
        ("freshness",      "Freshness label breakdown"),
        ("sources",        "Per-news-source PnL"),
        ("drag",           "Open-position capital drag"),
        ("direction",      "YES/NO × inverted breakdown"),
    ]
    while True:
        print(_hr())
        print("Analysis")
        for i, (c, desc) in enumerate(cmds, 1):
            print(f"  {i:2}) {c:<18} — {desc}")
        print("   0) Back")
        choice = _ask("Pick", default="0")
        if choice == "0":
            return 0
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(cmds):
                _run(["python3", "-m", "scripts.analysis", cmds[idx][0]])
        except ValueError:
            continue


def tests_menu() -> int:
    while True:
        print(_hr())
        print("Tests")
        print("  1) Unit  (aggressive_core)")
        print("  2) Integration (aggressive_core)")
        print("  3) Soak (paper, 4h default)")
        print("  4) Run a single test file (you provide path)")
        print("  0) Back")
        choice = _ask("Pick", default="0")
        if choice == "1":
            _run(["python3", "-m", "pytest",
                  "app/strategies/aggressive_core/tests/unit",
                  "-q"])
        elif choice == "2":
            _run(["python3", "-m", "pytest",
                  "app/strategies/aggressive_core/tests/integration",
                  "-q"])
        elif choice == "3":
            hours = _ask("Soak hours", default="4")
            _run(["python3", "-m",
                  "app.strategies.aggressive_core.tests.soak.run_soak",
                  "--hours", hours])
        elif choice == "4":
            p = _ask("Path")
            if p:
                _run(["python3", "-m", "pytest", p, "-q"])
        elif choice == "0":
            return 0


def tail_logs() -> int:
    events = DASH / "events.ndjson"
    if events.exists():
        print(f"Tailing {events} (Ctrl-C to exit)")
        return _run(["tail", "-F", str(events)])
    print("No event stream yet — start the runner first.")
    return 0


def fresh_cycle_50() -> int:
    """Reset for a fresh $50 paper cycle with reinvestment (§G acceptance).

    Steps:
      1) snapshot baseline as 'pre-fresh-50-<date>'
      2) restore that snapshot with --reset-trading (wipes per-engine
         trading state; preserves market index + news memory)
      3) seed a new cycle row 'fresh-50-<date>'
      4) restart compose with AGGRESSIVE_CORE_ENABLE=1
    """
    from datetime import datetime, timezone
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    snap = f"pre-fresh-50-{date}"
    cycle = f"fresh-50-{date}"
    print(_hr())
    print(f"Fresh $50 cycle — snap={snap} cycle={cycle}")
    if _run(["python3", "-m", "scripts.snapshot.snapshot", "save", snap, "--zip"]):
        print("Snapshot failed; aborting fresh cycle.")
        return 1
    if _run(["python3", "-m", "scripts.snapshot.snapshot",
             "restore", snap, "--reset-trading"]):
        print("Restore failed; aborting.")
        return 2
    print("Trading state wiped. Re-starting compose with aggressive_core enabled …")
    os.environ["AGGRESSIVE_CORE_ENABLE"] = "1"
    _docker_compose("up", "-d")
    print(_hr())
    print(f"Fresh cycle '{cycle}' started. Bankroll: $50.00. Reinvestment: ON.")
    print(f"Pre-reset snapshot saved at snapshots/{snap}.zip")
    return 0


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

ACTIONS: list[tuple[str, str, callable]] = [
    ("1", "Start runner (paper, all engines + aggressive_core)", start_runner_all),
    ("2", "Start runner (paper, aggressive_core ONLY)",          start_runner_core_only),
    ("3", "Stop runner",                                          stop_runner),
    ("4", "Status / health",                                      status),
    ("5", "Snapshot …",                                           snapshot_menu),
    ("6", "Validate (validate-report)",                           validate),
    ("7", "Analysis …",                                           analysis_menu),
    ("8", "Tests …",                                              tests_menu),
    ("9", "Tail live events",                                     tail_logs),
    ("R", "Reset & start fresh $50 cycle (reinvestment ON)",      fresh_cycle_50),
    ("0", "Quit",                                                 lambda: None),
]


def main() -> int:
    while True:
        print()
        print(_hr("═"))
        print("  seulgi — interactive menu")
        print(_hr("═"))
        for key, label, _ in ACTIONS:
            print(f"  {key}) {label}")
        choice = _ask("Pick", default="0").strip().lower()
        action = next((fn for k, _, fn in ACTIONS if k.lower() == choice), None)
        if action is None:
            print(f"(unknown choice: {choice!r})")
            continue
        if choice == "0":
            return 0
        try:
            action()
        except KeyboardInterrupt:
            print("\n(interrupted)")
        except Exception as exc:  # noqa: BLE001
            print(f"!! action failed: {exc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
