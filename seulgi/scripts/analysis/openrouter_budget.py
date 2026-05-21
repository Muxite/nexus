"""Tiny budget audit for the OpenRouter key.

Why this exists
───────────────
On 2026-05-21 the bot silently 403'd for ~21 hours after the OpenRouter
monthly key cap was reached. We had no visibility — no dashboard tile,
no alert. This script gives a one-liner snapshot any time the user wants
to know how close the key is to its monthly cap, broken down by:

  • OpenRouter's own /key endpoint (authoritative cap state)
  • The DB's `usage_events` table (tokens by provider + estimated cost)

Pairs with Part 12 of the plan ("LLM cost reduction").

Usage
─────
  python3 -m scripts.analysis.openrouter_budget
  python3 -m scripts.analysis.openrouter_budget --json
  python3 -m scripts.analysis.openrouter_budget --since-hours 24

The OpenRouter API key is read from $OPENROUTER_API_KEY (same as the bot).
Cost estimates use a small inline price table — keep in sync with the
real OpenRouter pricing if the bot adopts new models.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Optional


# Approximate per-1M-token prices (USD) as of 2026-05-21. Update when the
# bot switches models. Sourced from /v1/models on OpenRouter. The token
# counter in `usage_events` lumps prompt + completion together, so we use
# a 30/70 blended rate — close enough for budget-level monitoring.
_PRICE_USD_PER_1M = {
    "google/gemini-2.0-flash-lite-001":      (0.075, 0.300),
    "google/gemini-2.5-flash-lite":          (0.100, 0.400),
    "google/gemini-3.1-flash-lite-preview":  (0.250, 1.500),
    "google/gemini-3.1-flash-lite":          (0.250, 1.500),
    "google/gemini-2.5-flash":               (0.300, 2.500),
    # Anthropic Haiku 4.5 is the premium fallback; rough numbers — refresh
    # against /v1/models if it becomes a meaningful share of spend.
    "anthropic/claude-haiku-4-5":            (1.000, 5.000),
}
_DEFAULT_PRICE = (0.200, 1.000)


def _blended_cost(model: str, tokens: float) -> float:
    inp, out = _PRICE_USD_PER_1M.get(model, _DEFAULT_PRICE)
    blended = 0.30 * inp + 0.70 * out
    return (tokens / 1_000_000.0) * blended


def fetch_key_state(api_key: str) -> Optional[dict]:
    req = urllib.request.Request(
        "https://openrouter.ai/api/v1/key",
        headers={"Authorization": f"Bearer {api_key}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))["data"]
    except (urllib.error.URLError, KeyError, ValueError) as exc:
        print(f"[budget] /key fetch failed: {exc}", file=sys.stderr)
        return None


def db_path_default() -> Path:
    # Same default the bot uses (host-side path; works when running on host).
    return Path(__file__).resolve().parents[2] / "data" / "seulgi.db"


def db_usage_by_provider(db: Path, since_hours: Optional[int]) -> list[tuple[str, int, float, float]]:
    """Return [(provider, n_calls, tokens, est_usd), ...] sorted by est_usd desc."""
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        query = """
            SELECT provider, COUNT(*) n, SUM(count) tokens
            FROM usage_events
            WHERE kind = 'ai_tokens'
            {window}
            GROUP BY provider
        """
        args: tuple = ()
        if since_hours is not None:
            window = "AND ts >= datetime('now', ?)"
            args = (f"-{since_hours} hours",)
        else:
            window = ""
        rows = conn.execute(query.format(window=window), args).fetchall()
    finally:
        conn.close()
    out = []
    for provider, n, tokens in rows:
        if not tokens:
            continue
        cost = _blended_cost(provider or "", float(tokens))
        out.append((provider or "(unknown)", int(n), float(tokens), cost))
    out.sort(key=lambda r: r[3], reverse=True)
    return out


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--json", action="store_true",
                        help="emit machine-readable JSON instead of a text report")
    parser.add_argument("--since-hours", type=int, default=None,
                        help="restrict usage_events to the last N hours (default: all-time)")
    parser.add_argument("--db", default=str(db_path_default()),
                        help="path to seulgi.db (default: auto-detected from script location)")
    parser.add_argument("--warn-pct", type=float, default=0.80,
                        help="warn when monthly usage exceeds this fraction of the cap")
    args = parser.parse_args(argv)

    api_key = os.environ.get("OPENROUTER_API_KEY", "")
    key_state = fetch_key_state(api_key) if api_key else None

    db = Path(args.db)
    if not db.exists():
        print(f"[budget] DB not found at {db}", file=sys.stderr)
        return 1
    rows = db_usage_by_provider(db, since_hours=args.since_hours)
    total_tokens = sum(r[2] for r in rows)
    total_cost = sum(r[3] for r in rows)

    if args.json:
        payload = {
            "key": key_state,
            "usage_events": [
                {"provider": p, "n_calls": n, "tokens": t, "est_usd": c}
                for p, n, t, c in rows
            ],
            "total_tokens": total_tokens,
            "total_est_usd": total_cost,
            "since_hours": args.since_hours,
        }
        print(json.dumps(payload, indent=2))
        return 0

    window = f"last {args.since_hours}h" if args.since_hours else "all-time"

    print(f"# OpenRouter budget ({window})\n")
    if key_state:
        limit = key_state.get("limit") or 0
        used_m = key_state.get("usage_monthly") or 0
        remaining = key_state.get("limit_remaining") or 0
        pct = (used_m / limit) if limit else 0
        bar_full = 30
        filled = int(round(pct * bar_full))
        bar = "█" * filled + "░" * (bar_full - filled)
        flag = ""
        if limit and pct >= args.warn_pct:
            flag = "  ⚠  CAP THRESHOLD"
        print(f"  Monthly cap   : ${limit:.2f}")
        print(f"  Used this mo  : ${used_m:.2f}  ({pct*100:.1f}%)")
        print(f"  Remaining     : ${remaining:.2f}")
        print(f"  [{bar}]{flag}")
        print(f"  Used today    : ${key_state.get('usage_daily') or 0:.2f}")
        print(f"  Used this wk  : ${key_state.get('usage_weekly') or 0:.2f}")
    else:
        print("  (could not fetch /key — set OPENROUTER_API_KEY)")
    print()

    print(f"# DB usage_events breakdown ({window})\n")
    print(f"  {'provider':<48} {'n':>6}  {'tokens':>14}  {'est $':>8}")
    print(f"  {'-'*48} {'-'*6}  {'-'*14}  {'-'*8}")
    for provider, n, tokens, cost in rows:
        print(f"  {provider:<48} {n:>6}  {int(tokens):>14,}  ${cost:>7.3f}")
    print(f"  {'-'*48} {'-'*6}  {'-'*14}  {'-'*8}")
    print(f"  {'TOTAL':<48} {'':>6}  {int(total_tokens):>14,}  ${total_cost:>7.3f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
