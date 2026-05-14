"""
Seulgi debug CLI — `python -m app.debug <subcommand> [args]`.

Read-only inspection tools for an already-running seulgi deployment (or a
stopped one with a populated DB). Every subcommand opens the DB read-only
and either prints a summary or fires a one-shot HTTP probe. Nothing here
writes to the DB or talks to the trade-execution path.

Subcommands
───────────
  probe-scraper          Hit the scraper /health endpoint. Exits non-zero
                         on failure — useful from CI/shell scripts.
  system-health          Print the same fields the SYSTEM HEALTH dashboard
                         block renders (scraper, AI key, AI neutral rate,
                         DB last-write).
  inspect-engine NAME    Print the merged TradingConfig for one engine plus
                         its live balance, total_staked, and open positions.
  recent-skips [-n 20]   Tally the most recent `engine_results.skip_reason`
                         buckets (optionally filtered to one --engine).
  replay HEADLINE_ID     Pull the stored headline, run embed → match →
                         classify in-process, print every step. NEVER hits
                         the DB writers — pure dry-run.

Usage
─────
  python -m app.debug probe-scraper
  python -m app.debug system-health
  python -m app.debug inspect-engine aggressive
  python -m app.debug recent-skips --engine balanced -n 30
  python -m app.debug replay abc123def456 --engine aggressive
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sqlite3
import sys
from pathlib import Path
from typing import Optional


def _repo_root() -> Path:
    """Path to the seulgi/ repo root regardless of CWD."""
    return Path(__file__).resolve().parents[1]


def _add_app_to_path() -> None:
    sys.path.insert(0, str(_repo_root() / "app"))


# ---------------------------------------------------------------------------
# probe-scraper
# ---------------------------------------------------------------------------

async def _cmd_probe_scraper(args: argparse.Namespace) -> int:
    _add_app_to_path()
    from watcher import ScraperHealthPoller, _SCRAPER_URL
    url = args.scraper_url or _SCRAPER_URL
    poller = ScraperHealthPoller(scraper_url=url)
    status = await poller.probe_once()
    await poller.close()
    glyph = "✓" if status["reachable"] else "✗"
    print(f"{glyph} {url}/health")
    if status["reachable"]:
        print(f"  latency : {status['latency_ms']:.0f}ms")
        print(f"  last_ok : {status['last_ok_at']}")
        return 0
    print(f"  error   : {status['last_error']}")
    return 1


# ---------------------------------------------------------------------------
# system-health
# ---------------------------------------------------------------------------

async def _cmd_system_health(args: argparse.Namespace) -> int:
    _add_app_to_path()
    from config import load_config
    from db import SeulgiDB
    from watcher import ScraperHealthPoller, _SCRAPER_URL

    cfg = load_config(args.config or os.getenv("SEULGI_CONFIG", "config.yaml"))
    db_path = args.db or cfg.db.path
    if not Path(db_path).exists():
        # Fall back to data/seulgi.db relative to repo root
        alt = _repo_root() / "data" / "seulgi.db"
        if alt.exists():
            db_path = str(alt)
    db = SeulgiDB(db_path)
    await db.connect_ro()

    poller = ScraperHealthPoller(scraper_url=args.scraper_url or _SCRAPER_URL)
    scraper = await poller.probe_once()
    await poller.close()

    neutral_rate, n_decisions = await db.get_recent_neutral_rate(hours=1.0)
    last_insert = await db.get_last_signal_at()
    api_key = bool(cfg.ai.api_key or os.getenv("OPENROUTER_API_KEY") or os.getenv("GEMINI_API_KEY"))

    glyph = "✓" if scraper["reachable"] else "✗"
    print(f"{glyph} scraper      : {scraper['scraper_url']}")
    if scraper["reachable"]:
        print(f"    latency    : {scraper['latency_ms']:.0f}ms")
        print(f"    last_ok    : {scraper['last_ok_at']}")
    else:
        print(f"    last_error : {scraper['last_error']}")
    print(f"  AI key       : {'✓ present' if api_key else '✗ MISSING'}")
    if neutral_rate is not None:
        warn = "  ⚠ signal starved" if neutral_rate >= 0.85 else ""
        print(f"  AI 1h        : {neutral_rate*100:.1f}% NEUTRAL  (n={n_decisions}){warn}")
    else:
        print(f"  AI 1h        : (no decisions in last hour)")
    print(f"  DB last write: {last_insert}")
    await db.close()
    return 0 if (scraper["reachable"] and api_key) else 2


# ---------------------------------------------------------------------------
# inspect-engine
# ---------------------------------------------------------------------------

async def _cmd_inspect_engine(args: argparse.Namespace) -> int:
    _add_app_to_path()
    from config import load_config
    from engine import _merge_trading_cfg
    from db import SeulgiDB

    cfg = load_config(args.config or os.getenv("SEULGI_CONFIG", "config.yaml"))
    engine_cfg = next((e for e in cfg.engines if e.name == args.engine), None)
    if engine_cfg is None:
        print(f"No engine named {args.engine!r}. Known engines:")
        for e in cfg.engines:
            print(f"  - {e.name}")
        return 1

    merged = _merge_trading_cfg(cfg.trading, engine_cfg)
    print(f"── Engine: {args.engine} ──────────────────────────────")
    for attr in sorted(vars(merged)):
        print(f"  {attr:<28} {getattr(merged, attr)!r}")

    db_path = args.db or cfg.db.path
    if not Path(db_path).exists():
        alt = _repo_root() / "data" / "seulgi.db"
        if alt.exists():
            db_path = str(alt)
    db = SeulgiDB(db_path)
    await db.connect_ro()

    gain, staked = await db.get_engine_gain_and_staked(args.engine)
    open_pos = await db.get_open_positions(engine_name=args.engine)
    print()
    print(f"── Live state ─────────────────────────────────────────")
    print(f"  realized gain : {gain:+.2f}")
    print(f"  total staked  : {staked:.2f}")
    print(f"  open positions: {len(open_pos)}")
    for p in open_pos[:20]:
        print(
            f"    {p['direction']:>3}  entry={p['entry_price']:.4f}  "
            f"size={p['size_usdc']:.2f}  age_h="
            f"{p.get('unrealized_pnl') or 0:+.2f}  q={(p.get('question') or '')[:60]}"
        )
    if len(open_pos) > 20:
        print(f"    … {len(open_pos)-20} more not shown")
    await db.close()
    return 0


# ---------------------------------------------------------------------------
# recent-skips
# ---------------------------------------------------------------------------

async def _cmd_recent_skips(args: argparse.Namespace) -> int:
    """
    Tally skip_reason buckets over the last N rows of engine_results,
    optionally filtered to a single engine. Uses sqlite3 directly to avoid
    opening a writeable connection.
    """
    _add_app_to_path()
    from config import load_config
    cfg = load_config(args.config or os.getenv("SEULGI_CONFIG", "config.yaml"))
    db_path = args.db or cfg.db.path
    if not Path(db_path).exists():
        alt = _repo_root() / "data" / "seulgi.db"
        if alt.exists():
            db_path = str(alt)

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    if args.engine:
        rows = conn.execute(
            "SELECT skip_reason, executed FROM engine_results "
            "WHERE engine_name=? ORDER BY id DESC LIMIT ?",
            (args.engine, args.n * 10),  # over-sample so we get N non-executed
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT skip_reason, executed FROM engine_results ORDER BY id DESC LIMIT ?",
            (args.n * 10,),
        ).fetchall()
    counts: dict[str, int] = {}
    examined = 0
    for r in rows:
        examined += 1
        if r["executed"]:
            counts["__executed__"] = counts.get("__executed__", 0) + 1
            continue
        reason = r["skip_reason"] or "(no reason)"
        # Bucket numeric tails: "edge_too_small:0.0480" → "edge_too_small:*"
        key = reason.split(":")[0] if ":" in reason else reason
        counts[key] = counts.get(key, 0) + 1
        if examined >= args.n:
            break

    label = f" engine={args.engine}" if args.engine else " (all engines)"
    print(f"── Last {examined} decisions{label} ──")
    for k in sorted(counts, key=lambda x: -counts[x]):
        print(f"  {k:<28} {counts[k]:>5}")
    return 0


# ---------------------------------------------------------------------------
# replay
# ---------------------------------------------------------------------------

async def _cmd_replay(args: argparse.Namespace) -> int:
    """
    Re-run a stored headline through embed → match → classify against ONE
    engine's config. Prints every step. No DB writes, no trade decisions
    persisted — pure dry run.
    """
    _add_app_to_path()
    from config import load_config
    from db import SeulgiDB
    from markets import Embedder, MarketIndex
    from ai import DirectionClassifier

    cfg = load_config(args.config or os.getenv("SEULGI_CONFIG", "config.yaml"))
    db_path = args.db or cfg.db.path
    if not Path(db_path).exists():
        alt = _repo_root() / "data" / "seulgi.db"
        if alt.exists():
            db_path = str(alt)

    db = SeulgiDB(db_path)
    await db.connect_ro()

    # Pull headline text from signals (which has headline_id since rev N).
    row = await db.fetchone(
        "SELECT headline, site_url, detected_at FROM signals "
        "WHERE headline_id=? ORDER BY id DESC LIMIT 1",
        (args.headline_id,),
    ) if hasattr(db, "fetchone") else None
    if row is None:
        # Fall back to raw sqlite read
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        r = conn.execute(
            "SELECT headline, site_url, detected_at FROM signals "
            "WHERE headline_id=? ORDER BY id DESC LIMIT 1",
            (args.headline_id,),
        ).fetchone()
        if r is None:
            print(f"No signals row for headline_id={args.headline_id}")
            await db.close()
            return 1
        headline = r["headline"]
        site_url = r["site_url"]
        detected_at = r["detected_at"]
    else:
        headline = row["headline"]
        site_url = row["site_url"]
        detected_at = row["detected_at"]

    print(f"── Headline ──")
    print(f"  hid    : {args.headline_id}")
    print(f"  site   : {site_url}")
    print(f"  when   : {detected_at}")
    print(f"  text   : {headline}")

    embedder = Embedder()
    embedding = embedder.embed(headline)
    print(f"\n── Embedded ──  dim={len(embedding)}")

    index = MarketIndex(db=db, embedder=embedder, cfg=cfg)
    cached = await index.load_from_db()
    if cached == 0:
        print("  ⚠ market index empty in DB — run the bot once to populate")
        await db.close()
        return 1
    print(f"  market index : {cached} markets loaded")

    matches = index.find_matches(embedding)
    print(f"\n── Matches (top {len(matches)}) ──")
    for m in matches:
        print(f"  sim={m.similarity:.3f}  {m.question[:78]}")

    if not matches:
        print("  (no matches above similarity_threshold)")
        await db.close()
        return 0

    api_key = cfg.ai.api_key or os.getenv("OPENROUTER_API_KEY") or os.getenv("GEMINI_API_KEY", "")
    if not api_key:
        print("\n  ⚠ no OPENROUTER_API_KEY — skipping AI classification")
        await db.close()
        return 0

    clf = DirectionClassifier(api_key=api_key, model=cfg.ai.model, api_base=cfg.ai.api_base)
    results = await clf.classify_batch(headline=headline, matches=matches)
    await clf.close()

    print(f"\n── AI directions ──")
    for m, r in zip(matches, results):
        print(
            f"  {r.direction.value:>7}  conf={r.confidence:.2f}  "
            f"sim={m.similarity:.2f}  {m.question[:60]}  ↳ {r.reason[:60]}"
        )

    await db.close()
    return 0


# ---------------------------------------------------------------------------
# argparse
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m app.debug", description=__doc__.strip().splitlines()[0])
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("probe-scraper", help="Probe scraper /health (one-shot)")
    sp.add_argument("--scraper-url", default=None)

    sp = sub.add_parser("system-health", help="Print SYSTEM HEALTH block fields")
    sp.add_argument("--scraper-url", default=None)
    sp.add_argument("--db", default=None)
    sp.add_argument("--config", default=None)

    sp = sub.add_parser("inspect-engine", help="Show merged config + live state for one engine")
    sp.add_argument("engine")
    sp.add_argument("--db", default=None)
    sp.add_argument("--config", default=None)

    sp = sub.add_parser("recent-skips", help="Tally skip_reason buckets over recent decisions")
    sp.add_argument("--engine", default=None)
    sp.add_argument("-n", type=int, default=200)
    sp.add_argument("--db", default=None)
    sp.add_argument("--config", default=None)

    sp = sub.add_parser("replay", help="Replay a stored headline through the pipeline (dry-run)")
    sp.add_argument("headline_id")
    sp.add_argument("--engine", default=None, help="Currently informational — uses global cfg")
    sp.add_argument("--db", default=None)
    sp.add_argument("--config", default=None)

    return p


_HANDLERS = {
    "probe-scraper":  _cmd_probe_scraper,
    "system-health":  _cmd_system_health,
    "inspect-engine": _cmd_inspect_engine,
    "recent-skips":   _cmd_recent_skips,
    "replay":         _cmd_replay,
}


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    handler = _HANDLERS[args.cmd]
    try:
        return asyncio.run(handler(args))
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        # Print the exception in a CLI-friendly way and exit non-zero rather
        # than letting Python's default tracebacks tangle with pending
        # aiosqlite worker threads on the way out.
        import traceback
        traceback.print_exc()
        print(f"\nerror: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    # os._exit bypasses lingering non-daemon threads (aiosqlite/embedder/etc.)
    # that can keep the interpreter alive even after our coroutines finish.
    # Acceptable for a one-shot CLI; tests invoke us as a subprocess so they
    # get the same fast-exit behaviour.
    os._exit(main())
