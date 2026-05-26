"""Audit current LLM prompt sizes — handoff H (cycle 24).

Pulls the last N prompts from `llm_calls`, computes character count + a
rough token estimate (chars/4), and prints percentiles (p10/p50/p90/p99)
plus min/max. Establishes the baseline so the post-deploy run can quantify
the prompt-richness delta from enriching `compose_direction_prompt`.

Read-only. SQL-only — no Gamma calls, no OpenRouter, no embedder.

Usage:  python -m scripts.audit.llm_prompt_size_audit [--db PATH] [--limit N]
"""
from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime, timezone


DEFAULT_DB = "/home/muk/projects/jennie/irene/data/irene.db"
DEFAULT_LIMIT = 100


SQL = """
SELECT id, prompt, created_at
FROM llm_calls
ORDER BY id DESC
LIMIT ?
"""


def _percentile(sorted_values: list[float], pct: float) -> float:
    """Linear-interpolation percentile. `pct` in [0, 100]. Empty → 0."""
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    k = (len(sorted_values) - 1) * (pct / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = k - lo
    return float(sorted_values[lo]) * (1.0 - frac) + float(sorted_values[hi]) * frac


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", default=DEFAULT_DB,
                   help=f"SQLite path (default: {DEFAULT_DB}).")
    p.add_argument("--limit", type=int, default=DEFAULT_LIMIT,
                   help=f"How many recent llm_calls rows to sample "
                        f"(default: {DEFAULT_LIMIT}).")
    args = p.parse_args(argv)

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(SQL, (args.limit,)).fetchall()
    conn.close()

    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"llm_prompt_size_audit (handoff H baseline)")
    print(f"  db:       {args.db}")
    print(f"  ran_at:   {now_iso}")
    print(f"  sampled:  last {args.limit} llm_calls rows")
    print()

    if not rows:
        print("No llm_calls rows — nothing to audit.")
        return 0

    char_counts = sorted(len(r["prompt"] or "") for r in rows)
    # Rough estimate — OpenAI/Anthropic both average ~4 chars/token on
    # English prose; gemini is similar enough for a back-of-envelope.
    token_estimates = sorted(c // 4 for c in char_counts)

    n = len(char_counts)
    oldest = rows[-1]["created_at"]
    newest = rows[0]["created_at"]
    print(f"actual sample size: {n}")
    print(f"oldest call:        {oldest}")
    print(f"newest call:        {newest}")
    print()

    print("prompt size (characters)")
    print(f"  min:  {char_counts[0]:>7d}")
    print(f"  p10:  {_percentile(char_counts, 10):>7.0f}")
    print(f"  p50:  {_percentile(char_counts, 50):>7.0f}")
    print(f"  p90:  {_percentile(char_counts, 90):>7.0f}")
    print(f"  p99:  {_percentile(char_counts, 99):>7.0f}")
    print(f"  max:  {char_counts[-1]:>7d}")
    print()
    print("estimated tokens (chars / 4)")
    print(f"  min:  {token_estimates[0]:>7d}")
    print(f"  p10:  {_percentile(token_estimates, 10):>7.0f}")
    print(f"  p50:  {_percentile(token_estimates, 50):>7.0f}")
    print(f"  p90:  {_percentile(token_estimates, 90):>7.0f}")
    print(f"  p99:  {_percentile(token_estimates, 99):>7.0f}")
    print(f"  max:  {token_estimates[-1]:>7d}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
