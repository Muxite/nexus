"""Emit a verdict-stub markdown file for human grading.

    python -m irene.scripts.sandbox.verdict_template --set smoke10 --out verdicts/smoke10.md

The user opens the file, replaces `verdict: ` with `verdict: GOOD|MARG|BAD|SKIP`
per row, optionally fills in `notes:`, then runs aggregate_verdicts.py.
"""

from __future__ import annotations

import argparse
import os

from irene.sandbox import loader, sets
from irene.sandbox.paths import VERDICTS_DIR, ensure_data_dirs
from irene.sandbox.report import fmt_float, fmt_money


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--set", required=True, dest="set_name")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    s = sets.load(args.set_name)
    con = loader.connect_readonly()
    rows = loader.load_rows(con, s.exploration_ids)

    ensure_data_dirs()
    out = args.out or os.path.join(VERDICTS_DIR, f"{s.name}.md")

    with open(out, "w") as f:
        f.write(f"# Verdicts for set `{s.name}` ({len(rows)} rows)\n")
        f.write(f"_{s.description}_\n\n")
        f.write("Set `verdict:` to GOOD, MARG, BAD, or SKIP. "
                "Optional free-text after `notes:`. Blocks separated by `---`.\n\n")
        for i, row in enumerate(rows, 1):
            f.write(f"## [{i}] exploration_id={row.exploration_id}\n")
            f.write("verdict: \n")
            f.write("notes: \n")
            f.write(f"- t0: {row.t0_iso}  ·  orig_conf {fmt_float(row.orig_llm_confidence)}  "
                    f"·  freshness {row.orig_freshness or '?'}\n")
            f.write(f"- **Article:** \"{(row.article_title or '').strip()}\" "
                    f"— *{row.article_source or '?'}*\n")
            f.write(f"- **Market:** \"{row.market_question.strip()}\"  "
                    f"· mid {fmt_float(row.market_mid_t0)} · vol {fmt_money(row.market_volume_24h)}\n")
            f.write(f"- **Orig reason:** {(row.orig_llm_reason or '').strip() or '<none>'}\n")
            f.write("---\n\n")

    print(f"wrote {out}")
    print(f"  rows = {len(rows)}")


if __name__ == "__main__":
    main()
