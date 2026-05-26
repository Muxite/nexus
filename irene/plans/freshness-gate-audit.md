# Plan — freshness-gate audit (biggest market-finding loss)

> Source: cycle 22 audit (`cycles/cycle-22.md`, decision funnel SQL).
> Filed as a plan because it's the single biggest unlock if recoverable
> — but the upstream stamping logic is non-trivial and worth a focused
> investigation rather than a config tweak.

## Context

Decision-funnel SQL from cycle 22's live DB (last 24h):

```
reject reason=freshness_stale    n=3,248   (83% of rejects)
reject reason=direction_neutral  n=  624
reject reason=insufficient_cash  n=   29
execute reason=ok                n=   15
```

`freshness_stale` is the cycle-7 reject reason emitted by
`app/stages/trade.py:_gate_freshness` when `freshness NOT IN
cfg.trade.accept_freshness` (default `["BREAKING", "EVOLVING"]`).
`freshness` is assigned upstream in `app/stages/dedupe.py` based on
distance-and-age boundaries (chroma cosine distance to the nearest
known cluster + published_at age).

So 83% of all signals — the matched (article, market) pairs that already
cleared the cosine-similarity floor — die here. That's a much larger
cap on volume than the LLM direction call, the edge gate, or the
market-news imbalance gate combined.

Three possible explanations, each with different fixes:

1. **The stamper is correctly identifying stale news.** 83% of news IS
   genuinely stale (rehash, repost, follow-up coverage of yesterday's
   events). Then this is "expected, just visible now" — no action.
2. **The stamper's age threshold is too aggressive.** If "stale"
   triggers at, say, `published_at > 3h old`, lots of legitimately
   trade-able news gets cut. Loosening the threshold (or adding a
   `LATE_BREAKING` category to `accept_freshness`) recovers signals.
3. **The stamper's cluster-distance threshold is too aggressive.** A
   slightly-reworded headline gets matched to an existing cluster and
   stamped STALE even though it's new information. Then the fix is
   raising the cluster-distance threshold or adding a recency-bias to
   the matcher.

We don't know which without sampling. The cost of a wrong fix is high
(false-positive trades on truly stale news, or — worse —
missing-deadline issues if we relax age too far).

## Recommended approach

Audit before changing anything. Three steps, each independently useful:

1. **Sampling script `scripts/audit/freshness_sampler.py`.** Pull 100
   recent `freshness_stale` decisions. For each: the article (title,
   summary, published_at), the matched market (question, end_date), and
   the `freshness` label + the dedupe trace
   (cluster_id + distance to nearest). Output to
   `reports/2026-05-NN-freshness-audit.md`.
2. **Human read of the 100 samples.** Tag each as "correctly stale" /
   "false stale, late-breaking" / "false stale, reworded fresh".
   Compute proportions. Report should include the proportions + 5
   representative examples per tag.
3. **Decide:**
   - >90% correctly stale → no action; close this plan as `wontfix`.
   - >20% false-stale-late-breaking → introduce a `LATE_BREAKING`
     freshness label with a more permissive age threshold; add to
     `cfg.trade.accept_freshness`.
   - >20% false-stale-reworded → raise the cluster-distance threshold
     in `dedupe.py` (likely a single config knob change in
     `cfg.dedupe.cluster_distance_threshold` or similar).
   - Mixed → ship both adjustments with a paired A/B.

The audit script can reuse pattern from
`scripts/audit/resolution_timeout_audit.py` (cycle 23). The dedupe
trace likely needs a join against the `article_clusters` table — verify
with `grep -n "cluster_id\|article_clusters" app/stages/dedupe.py`.

## Verification

1. `scripts/audit/freshness_sampler.py` runs against the live DB,
   produces `reports/2026-05-NN-freshness-audit.md` with 100 sampled
   decisions, each annotated.
2. After the human read and the chosen fix lands: re-run `soak_check.py`
   24h later. The `freshness_stale` share of total rejects should drop
   (target: from 83% → <70% if any of the false-stale categories were
   >10%).
3. **Counterfactual safety:** simultaneously confirm `execute` count
   does NOT explode (e.g., 15/day → 150/day with no PnL improvement
   would mean we just admitted noise). Compare 24h-after-fix `execute`
   count and 24h closed-trade PnL distribution against a 24h-before
   baseline. Roll back if execute volume jumps >3× without
   proportional PnL.
