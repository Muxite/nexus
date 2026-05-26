# Handoff B — Dedupe / freshness fix (CRITICAL — blocks LLM exposure)

**Date:** 2026-05-26
**Reference:** `irene/handoffs/2026-05-26-A-iran-funnel-audit.md` for the empirical case.
**Dependency:** none. Can start immediately.
**Blocks:** F (near-miss audit is sparse until this lands), G (tracer harness has nothing meaningful to verify until this lands).

## The problem

Per handoff A, **87% of all signals** in the last 24h were labeled `STALE_RETURN` or `REHASH` and rejected by `_gate_freshness`. The 556 Iran-strike articles produced 1,656 signals; 1,654 died on freshness. The single most damning case: an article titled *"US says it launched 'self-defense strikes' in Iran"* matched correctly to *"Will the Iranian regime survive U.S. military strikes?"* at sim 0.59 — but was labeled `STALE_RETURN` and never reached the LLM.

The dedupe stage is collapsing every new Iran-war article into a single days-old "Iran war" cluster and stamping them stale. This is structural: any sustained-news regime (war, financial crisis, election, natural disaster) hits the same wall.

## How the labeling works today

File: `irene/app/stages/dedupe.py`.

```python
# dedupe.py:91-101
_BREAKING_DIST_DEFAULT = 0.55       # cosine distance above this = new cluster
_EVOLVING_AGE_SECS = 6 * 3600       # 6 hours
_REHASH_AGE_SECS = 24 * 3600        # 24 hours (implied via _label)

# dedupe.py:150 — pure decision fn
def _label(nearest_dist: float, age_secs: float, breaking_dist: float) -> str:
    if nearest_dist > breaking_dist:                  # > 0.55 → new cluster
        return "BREAKING"
    if age_secs < _EVOLVING_AGE_SECS:                 # < 6h → still developing
        return "EVOLVING"
    if age_secs < _REHASH_AGE_SECS:                   # 6-24h → rehash
        return "REHASH"
    return "STALE_RETURN"                             # 24h+ → stale
```

`age_secs` = time since the **first** article in the matched cluster was ingested. So:
- A new Iran article that lands on a cluster whose first article was >24h ago → `STALE_RETURN` regardless of how fresh the new content actually is.
- The Iran war started days/weeks before the soak window, so the cluster is permanent → every new Iran article = STALE.

The `accept_freshness` config (default `["BREAKING","EVOLVING"]` at `app/config.py:352`) rejects REHASH and STALE_RETURN downstream in `_gate_freshness` (`app/stages/trade.py:355`).

## The plan — ship all three fixes together for maximum STALE reduction

The user directive (post-audit): "**stale return's aggression should be drastically reduced.**" Don't ship one fix and wait — ship the full triad and tighten the defaults aggressively. The current behavior treats a multi-day news cycle as one stale cluster forever; the new behavior should treat any *actively-fed* cluster as EVOLVING and only fall to STALE_RETURN when nothing has touched the cluster for a long time.

### Fix B1 — recency-bias the label (foundational)

Change `_label` so the age driving REHASH/STALE_RETURN is **time since the cluster's most recent article**, not cluster age. A cluster being fed every few minutes = EVOLVING, regardless of when it opened.

```python
def _label(nearest_dist, latest_article_age_secs, breaking_dist):
    if nearest_dist > breaking_dist:
        return "BREAKING"
    # Cycle 25+: "time since cluster last received an article" is the right
    # staleness signal. A burst of activity = EVOLVING regardless of when
    # the cluster opened (Iran war is "evolving" on day 30 just like day 1).
    if latest_article_age_secs < _EVOLVING_AGE_SECS:
        return "EVOLVING"
    if latest_article_age_secs < _REHASH_AGE_SECS:
        return "REHASH"
    return "STALE_RETURN"
```

Add a `latest_article_at` column to `article_clusters`; stamp it on every `_attach` call.

### Fix B2 — burst-detection promotion (overrides above)

If a cluster receives ≥N articles in the last T minutes, force `EVOLVING` regardless of any other signal. Defaults: **N=3, T=30**. Captures "10 outlets reporting on the same strike within an hour."

Implementation: a rolling `recent_article_count` on the cluster row, or a `SELECT COUNT(*) FROM articles WHERE cluster_id=? AND ingested_at >= now-T` query at attach time.

### Fix B3 — widen EVOLVING window aggressively + expose as config

Today: `_EVOLVING_AGE_SECS = 6 * 3600` (6h). **Bump to 24h.** And `_REHASH_AGE_SECS = 24 * 3600` (implicit). **Bump to 72h.** Anything below those is EVOLVING/REHASH (acceptable to the trade gate when we also widen `accept_freshness`).

Expose all three as config knobs in `app/config.py` `DedupeSection`:

```python
breaking_distance: float = Field(default=0.55, ge=0.0, le=2.0)
evolving_age_hours: float = Field(default=24.0, ge=0.0)   # was 6
rehash_age_hours: float = Field(default=72.0, ge=0.0)     # was 24
burst_min_articles: int = Field(default=3, ge=1)
burst_window_minutes: int = Field(default=30, ge=1)
```

### Fix B4 — accept REHASH in the trade gate

Today: `cfg.trade.accept_freshness = ["BREAKING", "EVOLVING"]` rejects REHASH. **Add REHASH** to the default. REHASH now means "story still warm, but no new info in the last 24h" — that's not "stale," that's "developing slower." Still worth a trade gate look.

```python
accept_freshness: list[str] = Field(default_factory=lambda: ["BREAKING", "EVOLVING", "REHASH"])
```

Only `STALE_RETURN` (nothing for 72h+) remains rejected — the genuinely dead clusters.

### Combined effect (cumulative)

Under today's behavior, an Iran article landing on a 5-day-old cluster with a new article 10 minutes ago: STALE_RETURN → reject.

Under the post-fix behavior:
- B2 (burst): the cluster has had 20 new articles in the last 30 minutes → EVOLVING → accept ✓
- Even without burst: B1 (recency-bias) + B3 (24h EVOLVING window): the cluster's latest article was 10 minutes ago → EVOLVING → accept ✓
- Without recency-bias either: B4 accepts REHASH → still accept ✓

Triple redundancy. The bot stops being blind during sustained news cycles.

### Why not just accept all four freshness labels and skip the dedupe work?

Because then truly stale clusters (a 30-day-old story with one new fluff piece) pass through and burn LLM tokens. The dedupe fix preserves the upstream filter for genuinely-dead news while admitting the actually-evolving stories the bot needs.

## Files

| Change | File | Notes |
|---|---|---|
| Add column | `app/stages/dedupe.py` (DDL block around line 113) | `latest_article_at TEXT` on `article_clusters` |
| Update label fn | `app/stages/dedupe.py:150` (`_label`) | take `latest_article_age_secs`; check burst-detection first |
| Burst detection | `app/stages/dedupe.py` (`_resolve_cluster`) | `recent_article_count` query at attach time |
| Stamp latest_article_at | `app/stages/dedupe.py` (`_attach`) | `latest_article_at = now` on each attach |
| Config knobs | `app/config.py` (DedupeSection) | `evolving_age_hours=24`, `rehash_age_hours=72`, `burst_min_articles=3`, `burst_window_minutes=30` |
| Accept REHASH | `app/config.py` (TradeSection) | `accept_freshness = ["BREAKING", "EVOLVING", "REHASH"]` |
| Migration | `scripts/migrate/2026_05_26_cluster_latest.py` (new) | `ALTER TABLE article_clusters ADD COLUMN latest_article_at TEXT`; backfill `= first_seen_at` |
| Tests | `app/tests/stages/test_dedupe.py` | `_label` cases — cluster opened 48h ago, last article 2h ago → EVOLVING; cluster opened 48h ago, last article 80h ago → STALE_RETURN; burst with 5 articles in 10 min → EVOLVING even when latest_article_age is 23h |

## Verification

1. **Unit:** `pytest app/tests/stages/test_dedupe.py` → existing tests pass + new recency + burst + REHASH-accept cases pass.
2. **Replay vs DB:** write a one-off `scripts/audit/dedupe_replay.py` that re-runs the existing 24h of articles through the new `_label` (using the live DB) and reports the new label distribution. Target: BREAKING+EVOLVING+REHASH share rises from 12.9% to ≥80%.
3. **Live re-audit:** restart container with the fix. After 1h of soak, re-run handoff A's Q4 query. Target: STALE_RETURN drops from 58% → <20% of signals.
4. **Iran-specific:** re-run handoff A's Q5 on the latest "US strikes" article. The label should now be `EVOLVING` (burst-detection or recency-bias hits), and a decision/prediction should appear.
5. **Trade pipeline:** within 2h post-restart, expect the LLM call rate (`dashboard.txt` "AI subsystem labels" line) to rise from ~5/hr to ≥50/hr. Funnel `direction_neutral`/`direction_lowconf` will replace `freshness_stale` as the dominant reject reason — that's the win.

## Pair this with…

- Handoff G (tracer stress harness) — once B lands, fire 100 Iran-strike tracers and confirm ≥70 land BREAKING/EVOLVING. This is the fast feedback loop.
- Handoff F (near-miss audit panel) — only meaningful once B unblocks the LLM exposure.

## Subagent prompt suggestion

> Read `irene/handoffs/2026-05-26-B-dedupe-freshness.md` end-to-end. Implement Fix B1 (recency-bias label). Add the migration. Add the test cases. Run `pytest app/tests/stages/test_dedupe.py` to confirm. Rebuild + restart the irene container. After 1h, re-run the Q4 query from handoff A and report the new freshness distribution. Do NOT touch any other handoff scope.
