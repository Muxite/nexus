"""Direct-LLM sampler for Stage-3 NEUTRAL-rate diagnosis.

Pulls recent (headline, market) pairs from `decision_inputs` (Phase 5
schema, captured by the runner), feeds them to the SAME `_BATCH_PROMPT_TEMPLATE`
the bot uses, optionally toggles the lessons block, and prints the model's
raw output side-by-side. Lets us see whether the LLM is genuinely uncertain
or whether the lessons block (seeded 2026-05-18, "treat as NEUTRAL unless
headline is a hard fact") is driving the 100% NEUTRAL rate.

Usage:
    cd seulgi
    OPENROUTER_API_KEY=sk-or-... \\
        python3 -m scripts.analysis.sample_classify --n 10

    # Override the model:
    OPENROUTER_API_KEY=... python3 -m scripts.analysis.sample_classify \\
        --n 5 --model anthropic/claude-haiku-4-5

    # Only run with lessons enabled (skip the no-lessons run):
    OPENROUTER_API_KEY=... python3 -m scripts.analysis.sample_classify \\
        --n 5 --skip-no-lessons

The script writes a per-sample report and a summary at the end (per-direction
count + average p_neutral / p_yes / p_no across all (sample × market) pairs).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


# Need the app package on sys.path so we can import ai's prompt template
# without duplicating it. The bot lives at /app inside the container, but
# the host-side checkout is `/home/muk/projects/nexus/seulgi/app`.
_APP_DIR = Path(__file__).resolve().parents[2] / "app"
if str(_APP_DIR) not in sys.path:
    sys.path.insert(0, str(_APP_DIR))

import aiohttp  # noqa: E402

import ai  # noqa: E402 — re-use _BATCH_SYSTEM_MESSAGE + _BATCH_PROMPT_TEMPLATE


_DEFAULT_API_BASE = "https://openrouter.ai/api/v1"
_CHAT_PATH = "/chat/completions"


@dataclass
class Sample:
    signal_id: int
    headline_id: Optional[str]
    headline: str
    matches: list[dict]      # [{cond_id, question, similarity, mid_yes, mid_no}, ...]
    freshness_label: Optional[str]
    source_host: Optional[str]


def db_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "seulgi.db"


def fetch_samples(db: Path, n: int) -> list[Sample]:
    """Pull the N most recent decision_inputs rows joined to their signal.

    We require at least 1 market in `matches_json` that passed the sim-gate
    (rel_score=SIM_PASS), since SIM_FAIL markets never reach Stage 3 anyway
    so testing them tells us nothing about Stage 3's NEUTRAL bias.
    """
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            """
            SELECT di.signal_id, di.headline_id, di.matches_json,
                   di.freshness_label, di.source_host, s.headline
            FROM decision_inputs di
            JOIN signals s ON s.id = di.signal_id
            ORDER BY di.signal_id DESC
            LIMIT ?
            """,
            (n * 5,),  # over-fetch — filter to ones with SIM_PASS matches
        ).fetchall()
    finally:
        conn.close()

    out: list[Sample] = []
    for sid, hid, mj, fl, sh, hl in rows:
        try:
            matches = json.loads(mj)
        except Exception:
            continue
        kept = [m for m in matches if m.get("rel_score") == "SIM_PASS"]
        if not kept:
            continue
        out.append(Sample(
            signal_id=sid, headline_id=hid, headline=hl,
            matches=kept[:5],  # cap markets per sample for budget
            freshness_label=fl, source_host=sh,
        ))
        if len(out) >= n:
            break
    return out


def fetch_lesson_notes(db: Path) -> list[str]:
    """The same lesson notes the runner would inject — `note` column of
    active rows, in insertion order (matches what `lessons.load_active_notes`
    does at runtime).
    """
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            "SELECT note FROM lessons WHERE active=1 AND note IS NOT NULL "
            "ORDER BY updated_at DESC LIMIT 8"
        ).fetchall()
    finally:
        conn.close()
    return [r[0] for r in rows]


def build_prompt(sample: Sample, lesson_notes: list[str]) -> str:
    """Re-create the exact prompt the bot would send for this sample."""
    lessons_block = ""
    if lesson_notes:
        lines = "\n".join(f"  - {n}" for n in lesson_notes[:8])
        lessons_block = f"Recent lessons (apply to confidence + direction):\n{lines}\n"

    def _q_line(i: int, m: dict) -> str:
        mid = m.get("mid_yes")
        mid_s = f"{mid:.3f}" if mid is not None else "unknown"
        sim = m.get("similarity", 0.0) or 0.0
        return f"{i}. [YES mid={mid_s} | sim={sim:.2f}] {m.get('question', '')}"

    questions_block = "\n".join(
        _q_line(i, m) for i, m in enumerate(sample.matches)
    )

    source_block = f"Source: {sample.source_host}\n" if sample.source_host else ""
    freshness_block = (
        f"FRESHNESS: {sample.freshness_label}\n"
        if sample.freshness_label else ""
    )

    return ai._BATCH_PROMPT_TEMPLATE.format(
        headline=sample.headline,
        source_block=source_block,
        lessons_block=lessons_block,
        republish_block=freshness_block,
        mkt_ctx_block="",
        questions_block=questions_block,
        n=len(sample.matches),
    )


async def call_llm(
    session: aiohttp.ClientSession,
    api_key: str,
    api_base: str,
    model: str,
    prompt: str,
    temperature: float,
) -> Optional[dict]:
    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": ai._BATCH_SYSTEM_MESSAGE},
            {"role": "user", "content": prompt},
        ],
        "max_tokens": 4000,
        "temperature": temperature,
        "response_format": {"type": "json_object"},
    }
    url = api_base.rstrip("/") + _CHAT_PATH
    async with session.post(
        url,
        json=body,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/seulgi-sampler",
            "X-Title": "Seulgi-Sampler",
        },
        timeout=aiohttp.ClientTimeout(total=60),
    ) as resp:
        if resp.status != 200:
            text = await resp.text()
            print(f"  [HTTP {resp.status}] {text[:300]}", file=sys.stderr)
            return None
        return await resp.json()


def parse_response(data: dict, n: int) -> list[dict]:
    """Return [{idx, p_yes, p_no, p_neutral, p_alpha, risk, reason}, ...]."""
    try:
        text = data["choices"][0]["message"]["content"].strip()
    except Exception:
        return []
    # Strip code fences if any
    if text.startswith("```"):
        text = text.lstrip("`").lstrip("json").strip()
        text = text.rstrip("`").strip()
    try:
        parsed = json.loads(text)
    except Exception as e:
        print(f"  [parse-fail] {e} | raw: {text[:200]}", file=sys.stderr)
        return []
    rows = parsed.get("results") if isinstance(parsed, dict) else parsed
    if not isinstance(rows, list):
        return []
    return rows[:n]


def winning_direction(row: dict) -> str:
    """The argmax bucket."""
    probs = {
        "YES": float(row.get("p_yes", 0.0)),
        "NO": float(row.get("p_no", 0.0)),
        "NEUTRAL": float(row.get("p_neutral", 0.0)),
    }
    return max(probs, key=probs.get)


async def main_async(args) -> int:
    api_key = os.environ.get("OPENROUTER_API_KEY", "")
    if not api_key:
        print("ERROR: set OPENROUTER_API_KEY", file=sys.stderr)
        return 2

    db = db_path()
    if not db.exists():
        print(f"ERROR: db not found at {db}", file=sys.stderr)
        return 2

    samples = fetch_samples(db, args.n)
    if not samples:
        print("No decision_inputs rows with SIM_PASS markets — "
              "make sure the bot has been running with the new schema.",
              file=sys.stderr)
        return 1

    lesson_notes = fetch_lesson_notes(db)
    print(f"# Stage-3 classifier sampling")
    print(f"  samples       : {len(samples)}")
    print(f"  model         : {args.model}")
    print(f"  temperature   : {args.temperature}")
    print(f"  lesson_notes  : {len(lesson_notes)} loaded from DB")
    if lesson_notes:
        print(f"  (sample lesson): {lesson_notes[0][:120]}")
    print()

    # Run twice: with and without lessons. Skip the second run if asked.
    runs: list[tuple[str, list[str]]] = [("WITH_LESSONS", lesson_notes)]
    if not args.skip_no_lessons:
        runs.append(("NO_LESSONS", []))

    summaries: dict[str, dict] = {}
    async with aiohttp.ClientSession() as session:
        for label, notes in runs:
            print(f"────────── {label} ──────────")
            agg = Counter()
            sum_p = defaultdict(float)
            n_rows = 0
            for s in samples:
                prompt = build_prompt(s, notes)
                data = await call_llm(
                    session, api_key, args.api_base, args.model,
                    prompt, args.temperature,
                )
                if data is None:
                    continue
                rows = parse_response(data, len(s.matches))
                if not rows:
                    print(f"[sig={s.signal_id}] PARSE-FAIL")
                    continue
                # Per-sample print
                print(f"[sig={s.signal_id}] {s.headline[:90]}")
                for i, row in enumerate(rows):
                    m = s.matches[i] if i < len(s.matches) else {}
                    dir_ = winning_direction(row)
                    py = float(row.get("p_yes", 0.0))
                    pn = float(row.get("p_no", 0.0))
                    pne = float(row.get("p_neutral", 0.0))
                    pa = float(row.get("p_alpha", 0.0))
                    rsk = float(row.get("risk", 0.0))
                    rsn = (row.get("reason") or "")[:90]
                    print(
                        f"  → {dir_:7s}  p=(Y={py:.2f} N={pn:.2f} U={pne:.2f}) "
                        f"α={pa:.2f} r={rsk:.2f}  "
                        f"sim={m.get('similarity', 0.0):.2f}  "
                        f"q='{(m.get('question') or '')[:60]}'"
                    )
                    if rsn:
                        print(f"     reason: {rsn}")
                    agg[dir_] += 1
                    sum_p["p_yes"] += py
                    sum_p["p_no"] += pn
                    sum_p["p_neutral"] += pne
                    sum_p["p_alpha"] += pa
                    n_rows += 1
                print()

            if n_rows:
                summaries[label] = {
                    "n_rows": n_rows,
                    "directions": dict(agg),
                    "avg_p_yes": sum_p["p_yes"] / n_rows,
                    "avg_p_no": sum_p["p_no"] / n_rows,
                    "avg_p_neutral": sum_p["p_neutral"] / n_rows,
                    "avg_p_alpha": sum_p["p_alpha"] / n_rows,
                    "neutral_rate": agg.get("NEUTRAL", 0) / n_rows,
                }
            print()

    print("══════════ SUMMARY ══════════")
    for label, s in summaries.items():
        print(f"{label}:")
        print(f"  n classifications      : {s['n_rows']}")
        print(f"  directions             : {s['directions']}")
        print(f"  NEUTRAL rate           : {s['neutral_rate']:.1%}")
        print(f"  avg p_yes / p_no / p_n : "
              f"{s['avg_p_yes']:.3f} / {s['avg_p_no']:.3f} / {s['avg_p_neutral']:.3f}")
        print(f"  avg p_alpha            : {s['avg_p_alpha']:.3f}")
        print()
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--n", type=int, default=5, help="number of headline samples")
    p.add_argument("--model", default="google/gemini-2.5-flash",
                   help="OpenRouter model id")
    p.add_argument("--temperature", type=float, default=0.5,
                   help="ensemble-style temperature")
    p.add_argument("--api-base", default=_DEFAULT_API_BASE)
    p.add_argument("--skip-no-lessons", action="store_true",
                   help="skip the comparison run with lessons disabled")
    args = p.parse_args(argv)
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    sys.exit(main())
