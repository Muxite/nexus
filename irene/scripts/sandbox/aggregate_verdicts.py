"""Parse a filled verdict file and report aggregated counts.

    python -m irene.scripts.sandbox.aggregate_verdicts --in verdicts/smoke10.md
"""

from __future__ import annotations

import argparse
from collections import Counter

from irene.sandbox import loader, verdicts
from irene.sandbox.report import fmt_float


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", required=True, dest="path")
    args = ap.parse_args()

    with open(args.path) as f:
        parsed = verdicts.parse(f.read())

    if not parsed:
        print("no verdict blocks found")
        return

    con = loader.connect_readonly()
    rows = loader.load_rows(con, [v.exploration_id for v in parsed])
    by_id = {r.exploration_id: r for r in rows}

    overall = Counter(v.verdict or "(blank)" for v in parsed)
    by_freshness = Counter()
    by_conf_bucket = Counter()
    by_source = Counter()
    for v in parsed:
        r = by_id.get(v.exploration_id)
        if r is None:
            continue
        verdict = v.verdict or "(blank)"
        by_freshness[(r.orig_freshness or "unknown", verdict)] += 1
        by_conf_bucket[(_bucket(r.orig_llm_confidence), verdict)] += 1
        by_source[(r.article_source or "?", verdict)] += 1

    print(f"## Verdict summary — {len(parsed)} blocks")
    print()
    print("### overall")
    for k, n in overall.most_common():
        print(f"- {k}: {n}")

    print("\n### by confidence bucket")
    _print_matrix(by_conf_bucket)

    print("\n### by freshness")
    _print_matrix(by_freshness)

    print("\n### by source")
    _print_matrix(by_source)

    notes = [v for v in parsed if v.notes]
    if notes:
        print(f"\n### notes ({len(notes)})")
        for v in notes[:20]:
            print(f"- [{v.n}] id={v.exploration_id} ({v.verdict or '(blank)'}): {v.notes}")


def _bucket(c):
    if c is None:
        return "unknown"
    if c < 0.5:
        return "<0.5"
    if c < 0.7:
        return "0.5-0.7"
    if c < 0.9:
        return "0.7-0.9"
    return ">=0.9"


def _print_matrix(counter: Counter):
    rows = sorted({k[0] for k in counter})
    cols = sorted({k[1] for k in counter})
    header = ["bucket"] + cols + ["total"]
    print("| " + " | ".join(header) + " |")
    print("|" + "|".join(["---"] * len(header)) + "|")
    for r in rows:
        total = sum(counter[(r, c)] for c in cols)
        cells = [str(counter.get((r, c), 0) or "·") for c in cols]
        print(f"| {r} | " + " | ".join(cells) + f" | {total} |")


if __name__ == "__main__":
    main()
