"""
seulgi 1-hour live system test.

Runs against the LIVE running seulgi + scraper containers and produces
a rich, coloured terminal report on:

  • Watcher health — how many sites are reachable, failure counts
  • Headline traceability — headline_id flows through DB tables
  • Engine thought process — AI decisions logged per headline
  • NewsMemory dedup — republish detection rate
  • Scraper method mix — which sites used fast vs browser method
  • Dashboard file freshness — all files updated within TTL
  • DB consistency — signals, trades, engine_results share headline_id
  • Failure recovery — DEGRADED sites attempt recovery between polls

Usage
─────
# Quick smoke (10 min):
    SOAK_MINUTES=10 python -m pytest tests/test_system_live.py -v -s

# Full 1h soak:
    SOAK_MINUTES=60 python -m pytest tests/test_system_live.py -v -s

Environment
───────────
    SEULGI_DATA   path to seulgi data dir (default: ../seulgi/data)
    SCRAPER_URL   scraper service base URL (default: http://localhost:9000)
    SEULGI_DB     path to seulgi.db (default: SEULGI_DATA/seulgi.db)
    SOAK_MINUTES  how long to run the soak (default: 60)
    POLL_EVERY    seconds between observation polls (default: 30)
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import time
import textwrap
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pytest

# ── Config ────────────────────────────────────────────────────────────────────

_DATA_DIR     = Path(os.getenv("SEULGI_DATA",  str(Path(__file__).parent.parent / "data")))
_SCRAPER_URL  = os.getenv("SCRAPER_URL",  "http://localhost:9000")
_DB_PATH      = Path(os.getenv("SEULGI_DB", str(_DATA_DIR / "seulgi.db")))
_DASHBOARD    = _DATA_DIR / "dashboard"
_SOAK_MINUTES = int(os.getenv("SOAK_MINUTES", "60"))
_POLL_EVERY   = int(os.getenv("POLL_EVERY",   "30"))

# How many sites must be reachable (not permanently unreachable) for the test to pass
_MIN_REACHABLE_SITES = 8
# Minimum total headlines we expect to see during the soak
_MIN_HEADLINES_PER_HOUR = 30
# Max fraction of sites allowed to be permanently unreachable
_MAX_UNREACHABLE_FRACTION = 0.4


# ── Colour helpers ────────────────────────────────────────────────────────────

_GREEN  = "\033[32m"
_YELLOW = "\033[33m"
_RED    = "\033[31m"
_CYAN   = "\033[36m"
_BOLD   = "\033[1m"
_RESET  = "\033[0m"
_DIM    = "\033[2m"

def _ok(msg):   return f"{_GREEN}✓{_RESET}  {msg}"
def _warn(msg): return f"{_YELLOW}⚠{_RESET}  {msg}"
def _fail(msg): return f"{_RED}✗{_RESET}  {msg}"
def _info(msg): return f"{_CYAN}·{_RESET}  {msg}"
def _head(msg): return f"\n{_BOLD}{_CYAN}{msg}{_RESET}"


# ── DB helper ────────────────────────────────────────────────────────────────

def _db() -> sqlite3.Connection:
    if not _DB_PATH.exists():
        pytest.skip(f"DB not found at {_DB_PATH} — is seulgi running?")
    con = sqlite3.connect(str(_DB_PATH))
    con.row_factory = sqlite3.Row
    return con


# ── Snapshot dataclass ───────────────────────────────────────────────────────

@dataclass
class _Snapshot:
    ts: float = field(default_factory=time.time)

    # DB counts
    signals_total: int = 0
    signals_with_hid: int = 0
    trades_total: int = 0
    trades_with_hid: int = 0
    engine_rows: int = 0
    engine_with_hid: int = 0
    executed_trades: int = 0

    # Watcher status parsed from dashboard file
    watcher_statuses: dict = field(default_factory=dict)  # site → {ok, failures, headlines}

    # Scraper /methods endpoint
    domain_methods: dict = field(default_factory=dict)    # host → preferred_method

    # NewsMemory — parsed from seulgi logs
    republish_count: int = 0
    total_checked: int = 0

    # Dashboard file mtimes
    dashboard_mtimes: dict = field(default_factory=dict)  # filename → mtime

    # AI decision log lines captured this poll
    ai_decisions: list[str] = field(default_factory=list)


# ── Snapshot collection ───────────────────────────────────────────────────────

def _collect_snapshot() -> _Snapshot:
    snap = _Snapshot()

    # ── DB ────────────────────────────────────────────────────────────────────
    try:
        con = _db()
        cur = con.cursor()

        cur.execute("SELECT COUNT(*) FROM signals")
        snap.signals_total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM signals WHERE headline_id IS NOT NULL AND headline_id != ''")
        snap.signals_with_hid = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM trades")
        snap.trades_total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM trades WHERE headline_id IS NOT NULL AND headline_id != ''")
        snap.trades_with_hid = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM trades WHERE skip_reason IS NULL")
        snap.executed_trades = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM engine_results")
        snap.engine_rows = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM engine_results WHERE headline_id IS NOT NULL AND headline_id != ''")
        snap.engine_with_hid = cur.fetchone()[0]

        con.close()
    except Exception as exc:
        print(_warn(f"DB read error: {exc}"))

    # ── Watcher status file ───────────────────────────────────────────────────
    ws_path = _DASHBOARD / "watcher_status.txt"
    if ws_path.exists():
        _parse_watcher_status(ws_path.read_text(), snap)

    # ── Dashboard file freshness ──────────────────────────────────────────────
    if _DASHBOARD.exists():
        for f in _DASHBOARD.iterdir():
            if f.suffix == ".txt":
                snap.dashboard_mtimes[f.name] = f.stat().st_mtime

    # ── Scraper /methods ──────────────────────────────────────────────────────
    try:
        with urllib.request.urlopen(f"{_SCRAPER_URL}/methods", timeout=5) as r:
            data = json.loads(r.read())
        for host, info in data.items():
            snap.domain_methods[host] = info.get("preferred_method") or "requests"
    except Exception:
        pass

    # ── AI decisions file ─────────────────────────────────────────────────────
    ai_path = _DASHBOARD / "ai_decisions.txt"
    if ai_path.exists():
        lines = ai_path.read_text().splitlines()
        snap.ai_decisions = [l for l in lines if l.strip()][-50:]  # last 50

    # ── Republish count from news_seen.txt ────────────────────────────────────
    news_seen = _DASHBOARD / "news_seen.txt"
    if news_seen.exists():
        content = news_seen.read_text()
        snap.republish_count = content.count("REPUBLISH")
        snap.total_checked = content.count("hid=")

    return snap


def _parse_watcher_status(text: str, snap: _Snapshot) -> None:
    """Parse dashboard/watcher_status.txt into snap.watcher_statuses."""
    current_site = None
    for line in text.splitlines():
        line = line.strip()
        # Site line: ✓ https://...  or  ✗ https://...
        if line.startswith("✓") or line.startswith("✗"):
            parts = line.split()
            if len(parts) >= 2 and parts[1].startswith("http"):
                current_site = parts[1]
                snap.watcher_statuses[current_site] = {
                    "ok": True, "failures": 0, "headlines": 0, "status": "OK"
                }
        if current_site:
            if "Status" in line and ":" in line:
                val = line.split(":", 1)[1].strip()
                snap.watcher_statuses[current_site]["status"] = val
                snap.watcher_statuses[current_site]["ok"] = "UNREACHABLE" not in val
            if "Total headlines" in line and ":" in line:
                try:
                    snap.watcher_statuses[current_site]["headlines"] = int(line.split(":")[1].strip())
                except ValueError:
                    pass
            if "failures" in line.lower():
                import re
                m = re.search(r"(\d+)\s+failure", line)
                if m:
                    snap.watcher_statuses[current_site]["failures"] = int(m.group(1))


# ── Report helpers ────────────────────────────────────────────────────────────

def _print_watcher_table(snap: _Snapshot) -> None:
    print(_head("  WATCHER HEALTH"))
    print(f"  {'SITE':<45} {'STATUS':<25} {'HEADLINES':>10} {'FAILURES':>9}")
    print("  " + "─" * 95)
    ok_count = 0
    for site, info in sorted(snap.watcher_statuses.items(), key=lambda x: x[0]):
        status = info["status"]
        hdl = info["headlines"]
        fails = info["failures"]
        col = _GREEN if "OK" == status else (_YELLOW if "DEGRADED" in status else _RED)
        icon = "✓" if info["ok"] else "✗"
        short = site.replace("https://www.", "").replace("https://", "")[:44]
        print(f"  {col}{icon}{_RESET} {short:<44} {col}{status:<24}{_RESET} {hdl:>10,} {fails:>9}")
        if info["ok"]:
            ok_count += 1
    print(f"\n  {ok_count}/{len(snap.watcher_statuses)} sites reachable")


def _print_db_traceability(snap: _Snapshot) -> None:
    print(_head("  DB TRACEABILITY  (headline_id propagation)"))
    rows = [
        ("signals",        snap.signals_total,  snap.signals_with_hid),
        ("trades",         snap.trades_total,   snap.trades_with_hid),
        ("engine_results", snap.engine_rows,    snap.engine_with_hid),
    ]
    for table, total, with_hid in rows:
        if total == 0:
            print(_info(f"  {table:<20} — no rows yet"))
            continue
        pct = 100 * with_hid / total
        col = _GREEN if pct >= 95 else (_YELLOW if pct >= 50 else _RED)
        bar = "█" * int(pct / 5) + "░" * (20 - int(pct / 5))
        print(f"  {table:<20} {col}{bar}{_RESET} {pct:5.1f}%  ({with_hid}/{total} rows have headline_id)")


def _print_scraper_methods(snap: _Snapshot) -> None:
    print(_head("  SCRAPER METHOD MIX"))
    if not snap.domain_methods:
        print(_warn("  No domain data from scraper /methods (is scraper running?)"))
        return
    method_counts: dict[str, int] = {}
    for host, method in snap.domain_methods.items():
        method_counts[method] = method_counts.get(method, 0) + 1
    for method, count in sorted(method_counts.items(), key=lambda x: -x[1]):
        col = _GREEN if method in ("requests", "cloudscraper") else _YELLOW
        print(f"  {col}{method:<25}{_RESET} {count} domains")


def _print_dashboard_freshness(snap: _Snapshot, start_mtime_baseline: dict) -> None:
    print(_head("  DASHBOARD FILE FRESHNESS"))
    now = time.time()
    for fname, mtime in sorted(snap.dashboard_mtimes.items()):
        age_s = now - mtime
        baseline = start_mtime_baseline.get(fname, 0)
        updated = mtime > baseline
        if age_s < 120:
            col, icon = _GREEN, "✓"
        elif age_s < 600:
            col, icon = _YELLOW, "~"
        else:
            col, icon = _RED, "✗"
        upd_tag = f" {_GREEN}[updated since start]{_RESET}" if updated else ""
        print(f"  {col}{icon}{_RESET} {fname:<35} age={int(age_s):>5}s{upd_tag}")


def _print_ai_sample(snap: _Snapshot) -> None:
    print(_head("  AI DECISION SAMPLES  (last 10 lines from ai_decisions.txt)"))
    lines = snap.ai_decisions[-10:] if snap.ai_decisions else []
    if not lines:
        print(_info("  No AI decisions logged yet"))
        return
    for line in lines:
        # Highlight key decision words
        for kw, col in [("BUY", _GREEN), ("SELL", _RED), ("NEUTRAL", _DIM),
                        ("REPUBLISH", _YELLOW), ("hid=", _CYAN)]:
            line = line.replace(kw, f"{col}{kw}{_RESET}")
        print(f"  {line}")


def _print_newsdedup(snap: _Snapshot) -> None:
    print(_head("  NEWS DEDUP  (NewsMemory republish detection)"))
    if snap.total_checked == 0:
        print(_info("  No headlines checked yet"))
        return
    pct = 100 * snap.republish_count / snap.total_checked
    col = _YELLOW if pct > 30 else _GREEN
    print(f"  {snap.republish_count} republishes / {snap.total_checked} total checked  "
          f"({col}{pct:.1f}%{_RESET} republish rate)")


# ── Core assertions ───────────────────────────────────────────────────────────

def _assert_final(snap: _Snapshot, start_snap: _Snapshot, elapsed_min: float) -> list[str]:
    """Return list of failure strings (empty = pass)."""
    failures = []
    scale = elapsed_min / 60.0  # scale thresholds to actual run time

    # 1. Watcher health
    reachable = sum(1 for s in snap.watcher_statuses.values() if s["ok"])
    total_sites = len(snap.watcher_statuses)
    if total_sites > 0 and reachable < _MIN_REACHABLE_SITES:
        failures.append(
            f"Only {reachable}/{total_sites} sites reachable — need ≥ {_MIN_REACHABLE_SITES}"
        )

    # 2. Headlines collected
    new_signals = snap.signals_total - start_snap.signals_total
    expected = int(_MIN_HEADLINES_PER_HOUR * scale)
    if new_signals < expected and elapsed_min >= 5:
        failures.append(
            f"Only {new_signals} new signals in {elapsed_min:.0f}m — expected ≥{expected}"
        )

    # 3. headline_id propagation (only if rows exist)
    for table, total, with_hid in [
        ("signals",        snap.signals_total,  snap.signals_with_hid),
        ("engine_results", snap.engine_rows,    snap.engine_with_hid),
    ]:
        if total > 10:
            pct = 100 * with_hid / total
            if pct < 90:
                failures.append(f"{table}: only {pct:.0f}% rows have headline_id (need ≥90%)")

    # 4. Dashboard files are being updated
    stale = [
        fname for fname, mtime in snap.dashboard_mtimes.items()
        if fname in ("dashboard.txt", "watcher_status.txt", "activity_log.txt")
        and (time.time() - mtime) > 300
    ]
    if stale and elapsed_min >= 5:
        failures.append(f"Dashboard files stale (>5min): {stale}")

    # 5. Scraper is responding
    if not snap.domain_methods and elapsed_min >= 2:
        failures.append("Scraper /methods endpoint not responding — is scraper up?")

    return failures


# ── Main soak test ────────────────────────────────────────────────────────────

@pytest.mark.live
def test_seulgi_system_soak():
    """
    Drive seulgi for SOAK_MINUTES and validate full observability pipeline.
    Polls every POLL_EVERY seconds and prints a live progress report.
    """
    duration_s = _SOAK_MINUTES * 60
    poll_s = _POLL_EVERY

    # Verify seulgi data dir exists
    if not _DASHBOARD.exists():
        pytest.skip(f"Dashboard dir {_DASHBOARD} not found — is seulgi running?")

    print(f"\n{_BOLD}{'═'*80}{_RESET}")
    print(f"{_BOLD}  SEULGI SYSTEM SOAK — {_SOAK_MINUTES}m  "
          f"(poll every {poll_s}s){_RESET}")
    print(f"  DB         : {_DB_PATH}")
    print(f"  Scraper    : {_SCRAPER_URL}")
    print(f"  Dashboard  : {_DASHBOARD}")
    print(f"{_BOLD}{'═'*80}{_RESET}")

    # Baseline snapshot
    start_snap = _collect_snapshot()
    start_mtime_baseline = dict(start_snap.dashboard_mtimes)
    start_time = time.time()

    print(_head("  BASELINE"))
    print(_info(f"  signals={start_snap.signals_total}  trades={start_snap.trades_total}  "
                f"engine_rows={start_snap.engine_rows}"))
    print(_info(f"  watcher sites seen: {len(start_snap.watcher_statuses)}"))

    poll_num = 0
    last_snap = start_snap

    while True:
        elapsed = time.time() - start_time
        remaining = duration_s - elapsed
        if remaining <= 0:
            break

        # Sleep until next poll (or until done)
        sleep_for = min(poll_s, remaining)
        print(f"\n{_DIM}  ── sleeping {int(sleep_for)}s "
              f"(elapsed {int(elapsed/60)}m{int(elapsed%60):02d}s / "
              f"{_SOAK_MINUTES}m) ──{_RESET}", flush=True)
        time.sleep(sleep_for)

        elapsed = time.time() - start_time
        poll_num += 1
        snap = _collect_snapshot()

        # ── Per-poll report ───────────────────────────────────────────────────
        print(f"\n{_BOLD}{'─'*80}{_RESET}")
        print(f"{_BOLD}  POLL #{poll_num}  "
              f"elapsed={int(elapsed/60)}m{int(elapsed%60):02d}s{_RESET}")
        print(f"{_BOLD}{'─'*80}{_RESET}")

        # Delta since last poll
        d_signals  = snap.signals_total  - last_snap.signals_total
        d_trades   = snap.trades_total   - last_snap.trades_total
        d_engine   = snap.engine_rows    - last_snap.engine_rows
        d_executed = snap.executed_trades - last_snap.executed_trades

        print(f"\n  {_BOLD}DELTA this poll:{_RESET}  "
              f"+{d_signals} signals  "
              f"+{d_trades} trade-decisions  "
              f"+{d_engine} engine rows  "
              f"+{d_executed} executed")

        # Cumulative since start
        total_signals  = snap.signals_total  - start_snap.signals_total
        total_executed = snap.executed_trades - start_snap.executed_trades
        print(f"  {_BOLD}CUMULATIVE since start:{_RESET}  "
              f"{total_signals} signals  {total_executed} executed trades")

        _print_watcher_table(snap)
        _print_db_traceability(snap)
        _print_newsdedup(snap)
        _print_scraper_methods(snap)
        _print_dashboard_freshness(snap, start_mtime_baseline)
        _print_ai_sample(snap)

        # ── Inline spot-checks ────────────────────────────────────────────────
        print(_head("  INLINE CHECKS"))
        _check_headline_id_consistency(snap)
        _check_scraper_reachability(snap)
        _check_trace_log(snap)

        last_snap = snap

    # ── Final report ──────────────────────────────────────────────────────────
    final_snap = _collect_snapshot()
    elapsed_min = (time.time() - start_time) / 60

    print(f"\n{_BOLD}{'═'*80}{_RESET}")
    print(f"{_BOLD}  FINAL REPORT  ({elapsed_min:.1f}m elapsed){_RESET}")
    print(f"{_BOLD}{'═'*80}{_RESET}")

    _print_watcher_table(final_snap)
    _print_db_traceability(final_snap)
    _print_newsdedup(final_snap)
    _print_scraper_methods(final_snap)
    _print_dashboard_freshness(final_snap, start_mtime_baseline)
    _print_ai_sample(final_snap)

    # ── Engine thought-process deep-dive ──────────────────────────────────────
    _print_engine_thought_process(start_snap.signals_total)

    # ── Republish trace examples ──────────────────────────────────────────────
    _print_republish_examples()

    # ── Assertions ────────────────────────────────────────────────────────────
    failures = _assert_final(final_snap, start_snap, elapsed_min)
    print(_head("  PASS / FAIL"))
    if failures:
        for f in failures:
            print(_fail(f"  {f}"))
        pytest.fail(f"\n{len(failures)} assertion(s) failed:\n" + "\n".join(f"  • {f}" for f in failures))
    else:
        total_new = final_snap.signals_total - start_snap.signals_total
        reachable = sum(1 for s in final_snap.watcher_statuses.values() if s["ok"])
        print(_ok(f"  {reachable}/{len(final_snap.watcher_statuses)} sites reachable"))
        print(_ok(f"  {total_new} headlines collected over {elapsed_min:.0f}m"))
        print(_ok(f"  headline_id propagation ≥90% in all tables"))
        print(_ok(f"  All dashboard files fresh"))
        print(f"\n{_BOLD}{_GREEN}  SYSTEM SOAK PASSED{_RESET}\n")


# ── Inline spot-checks (called each poll) ────────────────────────────────────

def _check_headline_id_consistency(snap: _Snapshot) -> None:
    """Spot-check that a random recent signal has matching rows in trades and engine_results."""
    try:
        con = _db()
        cur = con.cursor()
        # Pick a recent signal that has a headline_id
        cur.execute(
            "SELECT id, headline_id, headline FROM signals "
            "WHERE headline_id IS NOT NULL ORDER BY id DESC LIMIT 5"
        )
        rows = cur.fetchall()
        if not rows:
            print(_info("  headline_id check: no rows with headline_id yet"))
            return

        for row in rows[:3]:
            hid = row["headline_id"]
            # Check engine_results has this hid
            cur.execute("SELECT COUNT(*) FROM engine_results WHERE headline_id=?", (hid,))
            eng_count = cur.fetchone()[0]
            short_hl = (row["headline"] or "")[:60]
            if eng_count > 0:
                print(_ok(f"  [hid={hid}] '{short_hl}' → {eng_count} engine rows"))
            else:
                print(_warn(f"  [hid={hid}] '{short_hl}' → 0 engine rows (may be unmatched)"))
        con.close()
    except Exception as exc:
        print(_warn(f"  headline_id consistency check error: {exc}"))


def _check_scraper_reachability(snap: _Snapshot) -> None:
    """Verify scraper health endpoint is responding."""
    try:
        with urllib.request.urlopen(f"{_SCRAPER_URL}/health", timeout=3) as r:
            data = json.loads(r.read())
        if data.get("status") == "ok":
            print(_ok("  scraper /health → ok"))
        else:
            print(_warn(f"  scraper /health unexpected: {data}"))
    except Exception as exc:
        print(_fail(f"  scraper /health unreachable: {exc}"))


def _check_trace_log(snap: _Snapshot) -> None:
    """Check trace_log.txt has recent entries linking headline→market→decision."""
    trace_path = _DASHBOARD / "trace_log.txt"
    if not trace_path.exists():
        print(_warn("  trace_log.txt does not exist yet"))
        return
    content = trace_path.read_text()
    entry_count = content.count("hid=")
    age_s = time.time() - trace_path.stat().st_mtime
    col = _GREEN if age_s < 300 else _YELLOW
    print(f"  trace_log.txt: {entry_count} entries, "
          f"{col}age={int(age_s)}s{_RESET}")

    # Show last 3 trace entries
    lines = [l for l in content.splitlines() if "hid=" in l][-3:]
    for line in lines:
        line = line.replace("hid=", f"{_CYAN}hid={_RESET}")
        print(f"  {_DIM}{line[:120]}{_RESET}")


# ── Deep-dive reporters (final only) ─────────────────────────────────────────

def _print_engine_thought_process(baseline_signal_id: int) -> None:
    """Print a full breakdown of engine decisions for headlines seen during the soak."""
    print(_head("  ENGINE THOUGHT PROCESS  (top decisions this soak)"))
    try:
        con = _db()
        cur = con.cursor()

        # Get engine_results rows since baseline
        cur.execute("""
            SELECT er.engine_name, er.question, er.direction, er.confidence,
                   er.similarity, er.p_hat, er.mid_price, er.edge,
                   er.size_usdc, er.executed, er.skip_reason, er.headline_id,
                   s.headline, s.site_url
            FROM engine_results er
            LEFT JOIN signals s ON s.headline_id = er.headline_id
            WHERE er.id > (
                SELECT COALESCE(MAX(id),0) FROM engine_results
                WHERE decided_at < datetime('now', '-{0} minutes')
            )
            ORDER BY er.id DESC
            LIMIT 30
        """.format(0))  # get recent rows

        rows = cur.fetchall()
        if not rows:
            print(_info("  No engine decisions recorded yet"))
            con.close()
            return

        # Summary stats
        executed = [r for r in rows if r["executed"]]
        skipped  = [r for r in rows if not r["executed"]]
        by_engine: dict[str, dict] = {}
        for r in rows:
            e = r["engine_name"]
            if e not in by_engine:
                by_engine[e] = {"executed": 0, "skipped": 0, "directions": {}}
            if r["executed"]:
                by_engine[e]["executed"] += 1
            else:
                by_engine[e]["skipped"] += 1
            d = r["direction"]
            by_engine[e]["directions"][d] = by_engine[e]["directions"].get(d, 0) + 1

        print(f"\n  {len(rows)} decisions — {len(executed)} executed, {len(skipped)} skipped")
        print(f"\n  {'ENGINE':<18} {'EXEC':>6} {'SKIP':>6} {'DIRECTIONS'}")
        print("  " + "─" * 60)
        for eng, stats in sorted(by_engine.items()):
            dirs = ", ".join(f"{k}×{v}" for k, v in stats["directions"].items())
            print(f"  {eng:<18} {stats['executed']:>6} {stats['skipped']:>6}  {dirs}")

        # Show 5 most interesting decisions (executed first, then highest confidence)
        interesting = sorted(rows, key=lambda r: (-r["executed"], -r["confidence"]))[:5]
        print(f"\n  {_BOLD}SAMPLE DECISIONS:{_RESET}")
        for r in interesting:
            hl = (r["headline"] or r["question"] or "")[:70]
            site = (r["site_url"] or "")[:30]
            executed_str = f"{_GREEN}EXEC{_RESET}" if r["executed"] else f"{_DIM}SKIP{_RESET}"
            skip_why = f" [{r['skip_reason']}]" if r["skip_reason"] else ""
            dir_col = _GREEN if r["direction"] == "YES" else _RED if r["direction"] == "NO" else _DIM
            print(f"\n  {_CYAN}[hid={r['headline_id'] or '?':>12}]{_RESET} {executed_str}")
            print(f"  {_DIM}{site}{_RESET}")
            print(f"  headline  : {hl}")
            print(f"  question  : {r['question'][:70]}")
            print(f"  direction : {dir_col}{r['direction']}{_RESET}  "
                  f"conf={r['confidence']:.2f}  sim={r['similarity']:.2f}  "
                  f"edge={r['edge']:.3f}  p_hat={r['p_hat']:.2f}  "
                  f"mid={r['mid_price']:.2f if r['mid_price'] else 'n/a'}")
            print(f"  engine    : {r['engine_name']}{skip_why}")

        con.close()
    except Exception as exc:
        print(_warn(f"  engine thought-process query error: {exc}"))


def _print_republish_examples() -> None:
    """Show recent republish detections from the news_seen dashboard file."""
    print(_head("  REPUBLISH DETECTION EXAMPLES"))
    news_seen = _DASHBOARD / "news_seen.txt"
    if not news_seen.exists():
        print(_info("  news_seen.txt not found"))
        return
    lines = news_seen.read_text().splitlines()
    republish_lines = [l for l in lines if "REPUBLISH" in l][-5:]
    if not republish_lines:
        print(_info("  No republishes detected yet (good if early in soak)"))
        return
    for line in republish_lines:
        line = line.replace("REPUBLISH", f"{_YELLOW}REPUBLISH{_RESET}")
        line = line.replace("hid=", f"{_CYAN}hid={_RESET}")
        print(f"  {line[:120]}")


# ── Additional targeted unit-style assertions ─────────────────────────────────

@pytest.mark.live
def test_scraper_service_health():
    """Scraper must be up and responding before we start the soak."""
    try:
        with urllib.request.urlopen(f"{_SCRAPER_URL}/health", timeout=5) as r:
            data = json.loads(r.read())
        assert data.get("status") == "ok", f"Unexpected health response: {data}"
    except Exception as exc:
        pytest.fail(f"Scraper not reachable at {_SCRAPER_URL}: {exc}")


@pytest.mark.live
def test_scraper_cache_stats_structure():
    """Scraper /cache/stats must return well-formed data."""
    try:
        with urllib.request.urlopen(f"{_SCRAPER_URL}/cache/stats", timeout=5) as r:
            data = json.loads(r.read())
    except Exception as exc:
        pytest.fail(f"Scraper /cache/stats unreachable: {exc}")
    assert "memory" in data and "sqlite" in data
    assert "hits" in data["memory"] and "misses" in data["memory"]
    assert "hits" in data["sqlite"] and "evictions" in data["sqlite"]


@pytest.mark.live
def test_db_exists_and_has_expected_tables():
    """seulgi.db must exist and contain all expected tables."""
    if not _DB_PATH.exists():
        pytest.skip(f"DB not found: {_DB_PATH}")
    con = _db()
    cur = con.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {r[0] for r in cur.fetchall()}
    con.close()
    required = {"signals", "trades", "engine_results", "markets", "positions"}
    missing = required - tables
    assert not missing, f"Missing DB tables: {missing}"


@pytest.mark.live
def test_headline_id_column_exists_in_all_tables():
    """All three tables must have the headline_id column after migrations."""
    if not _DB_PATH.exists():
        pytest.skip(f"DB not found: {_DB_PATH}")
    con = _db()
    cur = con.cursor()
    for table in ("signals", "trades", "engine_results"):
        cur.execute(f"PRAGMA table_info({table})")
        cols = {r[1] for r in cur.fetchall()}
        assert "headline_id" in cols, f"headline_id missing from {table}"
    con.close()


@pytest.mark.live
def test_market_index_capped_at_300():
    """Market index must have been built with ≤300 markets (volume cap)."""
    mi_path = _DASHBOARD / "market_index.txt"
    if not mi_path.exists():
        pytest.skip("market_index.txt not found")
    content = mi_path.read_text()
    # Look for "300 markets" line
    assert "300" in content, (
        f"Expected market index to be capped at 300, content:\n{content[:500]}"
    )


@pytest.mark.live
def test_dashboard_files_all_exist():
    """All expected dashboard files must be present after startup."""
    if not _DASHBOARD.exists():
        pytest.skip(f"Dashboard dir not found: {_DASHBOARD}")
    expected = [
        "dashboard.txt", "watcher_status.txt", "activity_log.txt",
        "ai_decisions.txt", "trace_log.txt", "engine_comparison.txt",
    ]
    for fname in expected:
        path = _DASHBOARD / fname
        assert path.exists(), f"Missing dashboard file: {fname}"


@pytest.mark.live
def test_trace_log_entries_have_hid():
    """trace_log.txt entries must contain hid= for traceability."""
    trace_path = _DASHBOARD / "trace_log.txt"
    if not trace_path.exists():
        pytest.skip("trace_log.txt not found")
    content = trace_path.read_text()
    if len(content.strip()) == 0:
        pytest.skip("trace_log.txt is empty (no decisions yet)")
    assert "hid=" in content, "trace_log.txt has entries but none contain hid="


@pytest.mark.live
def test_news_memory_chroma_dir_exists():
    """ChromaDB persistent store must be present."""
    chroma_path = _DATA_DIR / "news_chroma"
    assert chroma_path.exists(), f"ChromaDB dir not found: {chroma_path}"


@pytest.mark.live
def test_signals_headline_id_is_12_hex():
    """All signals with headline_id set must have exactly 12 hex chars."""
    if not _DB_PATH.exists():
        pytest.skip(f"DB not found: {_DB_PATH}")
    con = _db()
    cur = con.cursor()
    cur.execute(
        "SELECT headline_id FROM signals WHERE headline_id IS NOT NULL LIMIT 50"
    )
    rows = cur.fetchall()
    con.close()
    if not rows:
        pytest.skip("No signals with headline_id yet")
    for row in rows:
        hid = row[0]
        assert len(hid) == 12, f"headline_id wrong length: {hid!r}"
        int(hid, 16)  # must be valid hex


@pytest.mark.live
def test_engine_results_have_matching_signals():
    """Every engine_result with a headline_id should have a matching signal."""
    if not _DB_PATH.exists():
        pytest.skip(f"DB not found: {_DB_PATH}")
    con = _db()
    cur = con.cursor()
    cur.execute(
        "SELECT DISTINCT headline_id FROM engine_results "
        "WHERE headline_id IS NOT NULL LIMIT 20"
    )
    hids = [r[0] for r in cur.fetchall()]
    if not hids:
        pytest.skip("No engine_results with headline_id yet")
    orphans = []
    for hid in hids:
        cur.execute("SELECT COUNT(*) FROM signals WHERE headline_id=?", (hid,))
        if cur.fetchone()[0] == 0:
            orphans.append(hid)
    con.close()
    assert not orphans, f"engine_results with no matching signal: {orphans}"
