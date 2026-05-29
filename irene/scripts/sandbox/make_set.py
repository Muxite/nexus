"""Build a sandbox set from a SQL WHERE filter or by composing existing sets.

    # From a fresh filter against the joined view (tradeable-only by default)
    python -m irene.scripts.sandbox.make_set \
        --name highconf --filter "e.llm_confidence >= 0.9 AND e.freshness != 'REHASH'"

    # From other sets
    python -m irene.scripts.sandbox.make_set --name overlap \
        --from-set highconf evolving --op intersect

    # Subsample an existing set
    python -m irene.scripts.sandbox.make_set --name smoke10 \
        --from-set highconf --sample 10 --seed 1

    # Include rows we can't actually backtest (no post-news best_ask)
    python -m irene.scripts.sandbox.make_set \
        --name audit_all --all-explorations --include-untradeable

Default: rows without a non-NULL post-news `best_ask` are filtered out
because they can't be priced for entry. Set --include-untradeable to
keep them (useful when auditing rejections or coverage gaps).
"""

from __future__ import annotations

import argparse

from irene.sandbox import features, loader, sets


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True)
    ap.add_argument("--description", default="")
    ap.add_argument("--filter", default="",
                    help="SQL WHERE fragment against the joined explorations⨝articles⨝markets view")
    ap.add_argument("--all-explorations", action="store_true",
                    help="Shortcut for --filter '1=1' — every exploration across every cycle")
    ap.add_argument("--from-set", nargs="+", default=[],
                    help="One or more existing set names to compose")
    ap.add_argument("--op", choices=("union", "intersect", "difference"),
                    default="union")
    ap.add_argument("--sample", type=int, default=0,
                    help="If >0, take a random subsample of this size after composition")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--include-untradeable", action="store_true",
                    help="Keep rows without a non-NULL post-news best_ask "
                         "(they can't be backtested but may be useful for audit)")
    # Second-stage feature filters — applied after the SQL/compose stage.
    # min-pre-stdev walks each row's odds history, so it is the expensive one.
    ap.add_argument("--require-related", action="store_true",
                    help="Drop rows with production llm_related=0 (spurious DKG matches).")
    ap.add_argument("--min-volume", type=float, default=None,
                    help="Drop rows with market volume below this.")
    ap.add_argument("--mid-band", type=float, nargs=2, metavar=("LO", "HI"),
                    default=None, help="Keep only rows with market_mid_t0 in [LO,HI].")
    ap.add_argument("--min-pre-stdev", type=float, default=None,
                    help="Drop rows whose pre-news mid stdev < this (flat-history filter).")
    args = ap.parse_args()

    if args.all_explorations and args.filter:
        ap.error("Use either --all-explorations or --filter, not both")
    if args.all_explorations:
        args.filter = "1=1"
    if not args.filter and not args.from_set:
        ap.error("Provide --filter, --all-explorations, or --from-set")

    ids: set[int] = set()
    parents_used: list[str] = []
    filter_summary = ""

    if args.from_set:
        parents = [sets.load(n) for n in args.from_set]
        composed = sets.compose(parents, args.op)
        ids = set(composed)
        parents_used = [p.name for p in parents]

    if args.filter:
        sets.validate_filter(args.filter, loader.FILTER_COLUMNS)
        con = loader.connect_readonly()
        rows = loader.query_rows(
            con, args.filter,
            tradeable_only=not args.include_untradeable,
        )
        filter_ids = {r.exploration_id for r in rows}
        if args.from_set:
            ids = ids.intersection(filter_ids) if args.op != "union" else ids.union(filter_ids)
        else:
            ids = filter_ids
        filter_summary = args.filter
        if not args.include_untradeable:
            filter_summary += "  [+ tradeable_only: has non-NULL post-news best_ask]"
    elif args.from_set and not args.include_untradeable:
        # Composing from existing sets — re-validate that surviving ids
        # are still tradeable (rows whose odds_observations have no
        # best_ask, possibly because capture is in-flight, get dropped).
        con = loader.connect_readonly()
        tradeable_rows = loader.query_rows(
            con, "1=1", tradeable_only=True,
        )
        tradeable_ids = {r.exploration_id for r in tradeable_rows}
        before = len(ids)
        ids = ids.intersection(tradeable_ids)
        dropped = before - len(ids)
        if dropped:
            filter_summary = f"composed from {args.from_set} via {args.op}  [+ tradeable_only filter dropped {dropped}]"

    # ----- second-stage feature filter (post SQL/compose, pre sample) -----
    feat_active = (args.require_related or args.min_volume is not None
                   or args.mid_band is not None or args.min_pre_stdev is not None)
    if feat_active and ids:
        fcon = loader.connect_readonly()
        kept: set[int] = set()
        for r in loader.load_rows(fcon, sorted(ids)):
            if args.require_related and not r.orig_llm_related:
                continue
            if args.min_volume is not None and (
                    r.market_volume_24h is None or r.market_volume_24h < args.min_volume):
                continue
            if args.mid_band is not None and (
                    r.market_mid_t0 is None
                    or not (args.mid_band[0] <= r.market_mid_t0 <= args.mid_band[1])):
                continue
            if args.min_pre_stdev is not None:
                f = features.compute(fcon, r.exploration_id)
                if f.pre_stdev is None or f.pre_stdev < args.min_pre_stdev:
                    continue
            kept.add(r.exploration_id)
        dropped = len(ids) - len(kept)
        ids = kept
        parts = []
        if args.require_related:
            parts.append("llm_related=1")
        if args.min_volume is not None:
            parts.append(f"vol>={args.min_volume:g}")
        if args.mid_band is not None:
            parts.append(f"mid in [{args.mid_band[0]:g},{args.mid_band[1]:g}]")
        if args.min_pre_stdev is not None:
            parts.append(f"pre_stdev>={args.min_pre_stdev:g}")
        feat_desc = f"[feature-filter: {', '.join(parts)}; dropped {dropped}]"
        filter_summary = f"{filter_summary}  {feat_desc}".strip()

    final_ids = sorted(ids)
    if args.sample and args.sample < len(final_ids):
        final_ids = sets.sample(final_ids, args.sample, args.seed)

    out = sets.SetFile(
        name=args.name,
        description=args.description or (
            filter_summary or f"composed from {args.from_set} via {args.op}"
        ),
        exploration_ids=final_ids,
        filter_summary=filter_summary,
        parents=parents_used,
    )
    p = sets.save(out)
    print(f"wrote {p}")
    print(f"  n = {len(final_ids)} ids")
    if parents_used:
        print(f"  parents = {parents_used}  op = {args.op}")
    if filter_summary:
        print(f"  filter = {filter_summary}")
    if args.sample:
        print(f"  sampled {args.sample} with seed {args.seed}")


if __name__ == "__main__":
    main()
