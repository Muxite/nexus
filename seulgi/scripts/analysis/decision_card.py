"""decision-card — one paste-ready text card per picked trade.

Emits a self-contained markdown blob carrying every input the pipeline
saw (headline, source, market, freshness, prior coverage, relevance
verdict, AI prediction, pre-headline market context, trader trace)
plus the eventual outcome (executed / skipped, realized PnL, close
reason). Designed for pasting verbatim into a free chat UI (ChatGPT /
Claude / Gemini web) to ask "what would you have done?" — no API
cost, manual benchmark against frontier models.

Pick modes (mutually exclusive):
  --trade-id N        exact row
  --bucket B --pick N N random picks from bucket B (seed for repro)
  --every N           stride across the filtered set, pick every Nth

Buckets: executed, closed_win, closed_loss, sl_loss, tp_win,
timeout, wrong_dir, no_signal, captured, too_slow,
relevance_dropped, freshness_rehash.

Options:
  --with-prompt    prepend a paste-able prompt preamble
  --hide-outcome   omit the outcome section (so the smarter AI can't peek)
  --limit N        max cards (safety cap, default 10)
"""
from __future__ import annotations

import json
import random
import sqlite3
from datetime import datetime, timezone
from typing import Optional

from scripts.analysis._common import (
    add_common_args, fetch_pipeline_inputs, parse_since, trade_outcome_for,
)
from scripts.analysis._format import Section


_BUCKETS = (
    "executed", "closed_win", "closed_loss",
    "sl_loss", "tp_win", "timeout",
    "wrong_dir", "no_signal", "captured", "too_slow",
    "relevance_dropped", "freshness_rehash",
)


# ---------------------------------------------------------------------------
# Picker
# ---------------------------------------------------------------------------


def _bucket_where(bucket: Optional[str]) -> tuple[str, list]:
    """Return (where_fragment, join_fragment) needed to filter trades to a bucket."""
    if not bucket:
        return "size_usdc > 0 AND skip_reason IS NULL", []
    if bucket == "executed":
        return "size_usdc > 0 AND skip_reason IS NULL", []
    if bucket == "closed_win":
        return "size_usdc > 0 AND skip_reason IS NULL AND realized_pnl > 0", []
    if bucket == "closed_loss":
        return ("size_usdc > 0 AND skip_reason IS NULL "
                "AND realized_pnl IS NOT NULL AND realized_pnl <= 0"), []
    if bucket == "sl_loss":
        return "size_usdc > 0 AND skip_reason IS NULL AND close_reason = 'SL'", []
    if bucket == "tp_win":
        return "size_usdc > 0 AND skip_reason IS NULL AND close_reason = 'TP'", []
    if bucket == "timeout":
        return "size_usdc > 0 AND skip_reason IS NULL AND close_reason = 'TIMEOUT'", []
    if bucket == "relevance_dropped":
        return "skip_reason LIKE 'not_relevant%'", []
    if bucket == "freshness_rehash":
        return "skip_reason LIKE 'freshness:rehash%'", []
    # The four impact_tracker buckets need to be filtered post-fetch (we
    # don't store the classification in trades). Return a permissive
    # filter; caller applies classify_missed_alpha.
    return "size_usdc > 0 AND skip_reason IS NULL", []


def _select_trade_ids(conn, args) -> list[int]:
    if args.trade_id:
        return [args.trade_id]

    where, _ = _bucket_where(args.bucket)
    params: list = []
    if args.since:
        cutoff = parse_since(args.since)
        where += " AND decided_at >= ?"
        params.append(cutoff)
    sql = f"SELECT id, direction FROM trades WHERE {where} ORDER BY id DESC"
    rows = conn.execute(sql, params).fetchall()

    # Post-filter for impact_tracker buckets that need a join.
    if args.bucket in ("wrong_dir", "no_signal", "captured", "too_slow"):
        from scripts.analysis._common import classify_missed_alpha
        keep = []
        for r in rows:
            imp = conn.execute(
                """SELECT delta_pre_60s, delta_t5m FROM price_impacts
                       WHERE condition_id IN
                             (SELECT condition_id FROM trades WHERE id = ?)
                         AND headline_id IN
                             (SELECT headline_id FROM trades WHERE id = ?)
                       LIMIT 1""",
                (r["id"], r["id"]),
            ).fetchone()
            if imp is None:
                continue
            b = classify_missed_alpha(r["direction"], imp["delta_pre_60s"], imp["delta_t5m"])
            if b == args.bucket:
                keep.append(r["id"])
        ids = keep
    else:
        ids = [r["id"] for r in rows]

    if not ids:
        return []

    limit = args.limit or 10
    if args.every:
        return ids[::args.every][:limit]
    if args.pick:
        rng = random.Random(args.seed)
        n = min(args.pick, len(ids))
        return rng.sample(ids, n)
    # Default: most-recent N (capped by --limit, default 10)
    return ids[:limit]


# ---------------------------------------------------------------------------
# Renderer
# ---------------------------------------------------------------------------


_PROMPT_PREAMBLE = """\
You are a quantitative analyst evaluating a prediction-market trade.
The card below shows every input the pipeline used to score a candidate
market against a news headline. Read everything through "What the
trader did" — but DO NOT read any "Outcome (revealed)" section first.

Tell me, BEFORE you peek at the outcome:
  1. Would you take this trade? YES / NO / NEUTRAL.
  2. If yes, which direction (YES or NO) and how much
     (as a fraction of the $100 max stake)?
  3. One-sentence reasoning.
  4. After committing your answer, scroll down to the revealed outcome
     and compare. Where did the pipeline call differ from yours, and why?

"""


def _humanise_age(detected_iso: str) -> str:
    try:
        dt = datetime.fromisoformat(detected_iso)
    except Exception:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - dt
    secs = int(delta.total_seconds())
    if secs < 3600:
        return f"{secs // 60}m ago"
    if secs < 86400:
        return f"{secs // 3600}h ago"
    return f"{secs // 86400}d ago"


def _compact_trace(trace: list) -> str:
    """Render trader_trace JSON as one short line."""
    if not trace:
        return "(no trace recorded)"
    parts: list[str] = []
    for step in trace:
        name = step.get("step", "?")
        if name == "ai":
            parts.append(f"ai:{step.get('direction','?')}@{step.get('confidence', 0):.2f}")
        elif name == "freshness":
            parts.append(f"freshness:{step.get('label','?')}")
        elif name == "relevance":
            parts.append(f"relevance:{step.get('score','?')}{'!' if step.get('pass') is False else ''}")
        elif name in ("min_confidence", "agreement", "post_ai_rule",
                      "min_entry", "hard_rail", "rail_skip", "edge",
                      "size", "mkt_vol"):
            tag = "PASS" if step.get("pass") else "FAIL"
            parts.append(f"{name}:{tag}")
        elif name == "invert":
            parts.append("invert:" + ("YES" if step.get("fired") else "no"))
        elif name in ("max_positions", "duplicate_position", "max_unique_markets"):
            parts.append(f"{name}:FAIL")
        elif name == "executed":
            parts.append("executed")
        elif name == "skip":
            parts.append(f"skip:{step.get('reason','?')}")
        else:
            parts.append(name)
    return " → ".join(parts)


def _render_card(conn, trade_id: int, hide_outcome: bool) -> str:
    bundle = fetch_pipeline_inputs(conn, trade_id)
    if bundle is None:
        return f"## Decision Card — trade_id={trade_id} (not found)\n"
    t = bundle["trade"]
    s = bundle["signal"]
    art = bundle["article"]
    imp = bundle["impact"]
    pmeta = bundle["prompt_meta"]
    trace = bundle["trader_trace"]

    variant = t.get("variant_name") or t.get("engine_name") or "default"
    detected = s.get("detected_at") or t.get("decided_at") or ""
    age = _humanise_age(detected)
    headline = s.get("headline") or "(no headline)"
    src = s.get("site_url") or "(unknown)"
    art_url = (art or {}).get("article_url")

    freshness = t.get("freshness_label") or s.get("freshness_label")
    nearest_age = s.get("nearest_age_secs")
    nearest_age_s = (
        f"{int(nearest_age // 3600)}h" if nearest_age and nearest_age >= 3600
        else f"{int(nearest_age // 60)}m" if nearest_age
        else "no prior"
    )

    # Stage-2 relevance verdict
    relevance_score = t.get("relevance_score")
    relevance_reason = t.get("relevance_reason")

    # Stage-3 prediction
    model = pmeta.get("model") or "?"
    n_samples = pmeta.get("n_samples") or "?"
    direction = t.get("direction")
    conf = t.get("confidence_raw") or 0.0
    agreement = t.get("agreement") or 0.0
    p_alpha = t.get("p_alpha") or 0.0
    risk = t.get("risk_score") or 0.0
    ai_reason = (t.get("ai_reason") or "").replace("\n", " ").strip()[:300]

    # Market context (cycle-11)
    mkt_vol = t.get("mkt_vol_30m")
    mkt_mean = t.get("mkt_mean_30m")
    mkt_lo = t.get("mkt_min_30m")
    mkt_hi = t.get("mkt_max_30m")
    mkt_drift = t.get("mkt_drift_30m")
    mkt_n = t.get("mkt_n_samples")

    # Outcome
    executed = (t.get("size_usdc") or 0) > 0 and not t.get("skip_reason")
    realized = t.get("realized_pnl")
    close_reason = t.get("close_reason")

    # Aggregate across all variants
    outcome = trade_outcome_for(conn, t.get("headline_id"), t.get("condition_id"))

    L: list[str] = []
    L.append(f"## Decision Card — trade_id={trade_id}  variant={variant}")
    L.append("")
    L.append("### Inputs the pipeline saw")
    L.append(f"- **Detected**: {detected} ({age})")
    L.append(f"- **Source**: {src}")
    L.append(f"- **Headline**: \"{headline}\"")
    if art_url:
        L.append(f"- **URL**: {art_url}")
    if freshness:
        nd = s.get("nearest_age_secs")
        nd_s = (
            f", nearest prior {nearest_age_s} ago" if nd else ", no prior coverage"
        )
        L.append(f"- **Freshness**: {freshness}{nd_s}")

    # Prior coverage (best-effort: surface from prompt_meta's recent_context
    # if we ever start logging it; not currently persisted, so skip silently).

    L.append("")
    L.append("### Candidate market")
    L.append(f"- **Question**: \"{t.get('question', '')}\"")
    cid = t.get("condition_id") or ""
    L.append(f"- **condition_id**: `{cid[:18]}…`")
    my, mn = t.get("mid_yes"), t.get("mid_no")
    if my is not None or mn is not None:
        my_s = f"{my:.3f}" if my is not None else "—"
        mn_s = f"{mn:.3f}" if mn is not None else "—"
        L.append(f"- **Mid prices at decision**: YES={my_s}  NO={mn_s}")

    if relevance_score:
        L.append("")
        L.append("### Stage 2 — relevance gate")
        L.append(f"- **Score**: {relevance_score}")
        if relevance_reason:
            L.append(f"- **Reason**: \"{relevance_reason[:280]}\"")

    if direction:
        L.append("")
        L.append(f"### Stage 3 — prediction ({model}, {n_samples}-sample ensemble)")
        L.append(
            f"- **Direction**: {direction}  conf_raw={conf:.2f}  agreement={agreement:.2f}"
        )
        L.append(f"- **p_alpha**: {p_alpha:.2f}  risk={risk:.2f}")
        if ai_reason:
            L.append(f"- **Reason**: \"{ai_reason}\"")

    if mkt_vol is not None or mkt_n is not None:
        L.append("")
        L.append("### Pre-headline market behaviour (30-min)")
        if mkt_vol is not None:
            cur = t.get("mid_yes")
            parts = [
                f"vol={mkt_vol:.4f}" if mkt_vol is not None else "vol=—",
                f"mean={mkt_mean:.3f}" if mkt_mean is not None else "mean=—",
                f"min={mkt_lo:.3f}" if mkt_lo is not None else "min=—",
                f"max={mkt_hi:.3f}" if mkt_hi is not None else "max=—",
                f"cur={cur:.3f}" if cur is not None else "cur=—",
                f"drift={mkt_drift:+.3f}" if mkt_drift is not None else "drift=—",
                f"n={mkt_n}" if mkt_n is not None else "n=—",
            ]
            L.append("- " + "  ".join(parts))

    L.append("")
    L.append("### What the trader did")
    if executed:
        L.append(
            f"- **Decision**: EXECUTED  size=${t.get('size_usdc', 0):.0f}  "
            f"entry_mid={t.get('mid_price', 0) or 0:.3f}"
        )
    else:
        L.append(f"- **Decision**: SKIPPED  reason=`{t.get('skip_reason')}`")
    L.append(f"- **Guard trace**: {_compact_trace(trace)}")

    if not hide_outcome:
        L.append("")
        L.append("### Outcome (revealed)")
        if executed:
            closed_at = t.get("closed_at") or ""
            L.append(
                f"- **Closed**: {closed_at}  close_reason={close_reason or 'open'}"
            )
            if realized is not None:
                L.append(f"- **Realized PnL**: {('+' if realized >= 0 else '')}{realized:.2f}")
        elif t.get("skip_reason"):
            L.append("- (trade was skipped — no outcome on this row)")
        if imp:
            d5 = imp.get("delta_t5m")
            d60 = imp.get("delta_pre_60s")
            if d5 is not None or d60 is not None:
                signed_post = (
                    None if d5 is None else (d5 if direction == "YES" else -d5)
                )
                L.append(
                    f"- **Market move pre60s/+5m**: "
                    f"pre={d60 if d60 is None else f'{d60:+.4f}'}  "
                    f"post={d5 if d5 is None else f'{d5:+.4f}'}"
                    + (f"  (signed in our direction: {signed_post:+.4f})"
                       if signed_post is not None else "")
                )
        if outcome and outcome.get("n", 0) > 1:
            L.append(
                f"- **All variants on this market**: {outcome['n']}  "
                f"staked=${outcome['staked']:.0f}  "
                f"realized={('+' if outcome['realized'] >= 0 else '')}"
                f"{outcome['realized']:.2f}"
            )

    return "\n".join(L)


# ---------------------------------------------------------------------------
# Subcommand contract
# ---------------------------------------------------------------------------


def add_parser(p) -> None:
    add_common_args(p)
    p.add_argument("--trade-id", type=int, default=None,
                   help="render this exact trade row")
    p.add_argument("--bucket", choices=_BUCKETS, default=None,
                   help="filter to a category (see module docstring)")
    p.add_argument("--pick", type=int, default=None,
                   help="random sample N trades from the filtered set")
    p.add_argument("--every", type=int, default=None,
                   help="stride: emit every Nth trade in the filtered set")
    p.add_argument("--seed", type=int, default=42,
                   help="random seed for --pick mode")
    p.add_argument("--with-prompt", action="store_true",
                   help="prepend a paste-ready prompt preamble")
    p.add_argument("--hide-outcome", action="store_true",
                   help="omit the outcome section (for blind A/B vs another AI)")


def run(args, conn):
    ids = _select_trade_ids(conn, args)
    cards: list[str] = []
    if args.with_prompt and ids:
        cards.append(_PROMPT_PREAMBLE.rstrip())
    for tid in ids:
        cards.append(_render_card(conn, tid, hide_outcome=args.hide_outcome))
    body = ("\n\n---\n\n").join(cards) if cards else "(no matching trades)"

    # Print the body directly so the operator can pipe it into a paste
    # buffer or a file. The Section we return is only a summary line.
    print(body)
    return Section(
        title="decision-card — written to stdout",
        headers=["count", "bucket", "mode"],
        rows=[[
            len(ids),
            args.bucket or "all",
            ("trade-id" if args.trade_id else
             "every" if args.every else
             "pick" if args.pick else "recent"),
        ]],
        notes=[f"{len(ids)} card(s) above; paste into a chat UI to compare."],
    )
