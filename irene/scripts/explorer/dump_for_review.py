"""Dump explorer decisions for manual review.

Joins `explorations` to `articles` and `markets` and writes a markdown
report (one block per row, plus a header summary) and a sibling CSV for
spreadsheet sorting. Read-only — never writes to the irene DB.

    python -m irene.scripts.explorer.dump_for_review
    python -m irene.scripts.explorer.dump_for_review --cycle explorer-0 --only-complete
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import statistics
from collections import Counter

from irene.scripts.explorer._common import connect_readonly


DEFAULT_DB = "/home/muk/projects/jennie/irene/data/irene.db"
DEFAULT_OUT_DIR = "/home/muk/projects/jennie/irene/data/explorer"

COLS = [
    "id", "t0_iso", "capture_status",
    "llm_confidence", "llm_reason",
    "cosine_similarity", "freshness", "article_tier",
    "market_volume_24h", "hours_to_expiry", "market_mid_t0",
    "article_title", "article_source", "article_url",
    "market_question", "market_slug", "market_end_date",
]

QUERY = """
SELECT
  e.id, e.t0_iso, e.capture_status,
  e.llm_confidence, e.llm_reason,
  e.cosine_similarity, e.freshness, e.article_tier,
  e.market_volume_24h, e.hours_to_expiry, e.market_mid_t0,
  a.title       AS article_title,
  a.source      AS article_source,
  a.url         AS article_url,
  m.question    AS market_question,
  m.slug        AS market_slug,
  m.end_date    AS market_end_date
FROM explorations e
LEFT JOIN articles a ON a.id           = e.article_id
LEFT JOIN markets  m ON m.condition_id = e.market_condition_id
WHERE e.cycle_id = (SELECT id FROM cycles WHERE name = ?)
  AND e.llm_related = 1
  {extra_where}
ORDER BY e.t0_iso ASC
"""


def _fmt_float(v, fmt="{:.3f}"):
    return fmt.format(v) if isinstance(v, (int, float)) else "?"


def _fmt_money(v):
    return f"${v:,.0f}" if isinstance(v, (int, float)) else "?"


def _summary(rows):
    by_status = Counter(r["capture_status"] for r in rows)
    by_tier = Counter((r["article_tier"] or "unknown") for r in rows)
    by_source = Counter((r["article_source"] or "unknown") for r in rows)
    confs = [r["llm_confidence"] for r in rows if r["llm_confidence"] is not None]
    summary = {
        "total": len(rows),
        "by_status": dict(by_status),
        "by_tier": dict(by_tier),
        "by_source": dict(by_source),
        "llm_confidence_min": min(confs) if confs else None,
        "llm_confidence_median": statistics.median(confs) if confs else None,
        "llm_confidence_max": max(confs) if confs else None,
        "llm_confidence_ge_0_7": sum(1 for c in confs if c >= 0.7),
        "llm_confidence_ge_0_9": sum(1 for c in confs if c >= 0.9),
    }
    return summary


def _render_summary_md(cycle, only_complete, summary):
    def kvline(d):
        return ", ".join(f"{k}: {v}" for k, v in sorted(d.items(), key=lambda x: -x[1]))
    lines = [
        f"# Explorer review — cycle `{cycle}`",
        f"_generated {dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')}Z; only_complete={only_complete}_",
        "",
        f"- **total rows (llm_related=1):** {summary['total']}",
        f"- **by capture_status:** {kvline(summary['by_status'])}",
        f"- **by article_tier:** {kvline(summary['by_tier'])}",
        f"- **by source:** {kvline(summary['by_source'])}",
        (
            f"- **llm_confidence:** min {_fmt_float(summary['llm_confidence_min'])} · "
            f"median {_fmt_float(summary['llm_confidence_median'])} · "
            f"max {_fmt_float(summary['llm_confidence_max'])} · "
            f"≥0.7: {summary['llm_confidence_ge_0_7']} · "
            f"≥0.9: {summary['llm_confidence_ge_0_9']}"
        ),
        "",
        "---",
        "",
    ]
    return "\n".join(lines)


def _render_row_md(n, r):
    art = (r["article_title"] or "<missing article>").strip()
    src = r["article_source"] or "?"
    url = r["article_url"] or ""
    q = (r["market_question"] or "<missing market>").strip()
    slug = r["market_slug"] or "?"
    end = r["market_end_date"] or "?"
    mid = _fmt_float(r["market_mid_t0"])
    vol = _fmt_money(r["market_volume_24h"])
    hte = _fmt_float(r["hours_to_expiry"], "{:.1f}h")
    reason = (r["llm_reason"] or "").strip() or "<no reason>"
    return (
        f"### [{n}] {r['t0_iso']}  "
        f"(conf {_fmt_float(r['llm_confidence'])}, tier {r['article_tier'] or '?'}, "
        f"status {r['capture_status']})\n"
        f"- **Article:** \"{art}\" — *{src}*  \n  {url}\n"
        f"- **Market:** \"{q}\" — `{slug}` · ends {end} · mid {mid} · vol {vol} · {hte} to expiry\n"
        f"- **Reason:** {reason}\n"
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--cycle", default="explorer-0")
    ap.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    ap.add_argument("--only-complete", action="store_true")
    args = ap.parse_args()

    extra_where = "AND e.capture_status = 'complete'" if args.only_complete else ""
    con = connect_readonly(args.db)
    rows = [dict(r) for r in con.execute(
        QUERY.format(extra_where=extra_where), (args.cycle,)
    )]

    summary = _summary(rows)
    summary_md = _render_summary_md(args.cycle, args.only_complete, summary)

    os.makedirs(args.out_dir, exist_ok=True)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    base = f"review_{args.cycle}_{stamp}"
    md_path = os.path.join(args.out_dir, base + ".md")
    csv_path = os.path.join(args.out_dir, base + ".csv")

    with open(md_path, "w") as f:
        f.write(summary_md)
        for i, r in enumerate(rows, 1):
            f.write(_render_row_md(i, r))
            f.write("\n")

    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLS, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(summary_md)
    print(f"wrote {md_path}")
    print(f"wrote {csv_path}")


if __name__ == "__main__":
    main()
