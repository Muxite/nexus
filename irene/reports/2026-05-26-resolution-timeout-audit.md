# RESOLUTION_TIMEOUT audit (cycle-21 Fix E, executed cycle 23)

**Date:** 2026-05-26
**Script:** `irene/scripts/audit/resolution_timeout_audit.py`
**Trigger:** Cycle-21 handoff listed the 11 markets stuck past the 7-day UMA cutoff as a low-priority audit. By cycle-23 execution time the count had aged into **13** (the cycle-22 audit predicted this aging-in pattern at 1–2 new markets per day).

## Headline

- **13 markets** in `RESOLUTION_TIMEOUT` state (`closed_at IS NOT NULL AND uma_finalized_at IS NULL AND age > 168h`).
- **0 markets with open trades NOW.**
- **0 markets we ever held a historical position on.**
- **Conclusion:** No action required. The entire backlog is passive legacy markets the bot never positioned on. Each is closed on Polymarket but Gamma never populated `umaResolutionStatus` / `outcomePrices` for them. This is a Polymarket-side data gap, not our bug.

## Why this is safe to ignore

The cycle-12 `_resolution_poll_loop` only operates on markets where we hold an OPEN trade (markets.py:842–854). Since none of these 13 have ever had a trade against them, there is no PnL exposure, no stuck position to close, no work the loop should be doing. The metric they trip in `soak_check.py` ("RESOLUTION_TIMEOUT") is informational on this slice; the cycle-22 split (actionable vs `legacy_uma_backlog`) already handles the actionable-vs-passive distinction for the cycle-12 work surface.

If a market in this backlog later sees us trade against it, the cycle-12 poll will pick it up automatically as part of its open-trade JOIN.

## Pattern observations

- 11 of 13 markets closed on **2026-05-18**, 2 on **2026-05-19** — a clustered aging-in window matching the cycle-22 audit's "natural aging into the bucket as time advances past the 7d cutoff" prediction.
- Question themes are heavy on niche sports (Czech-Liga football outcomes, LCK/CD esports, NBA basketball) and long-tail prediction markets (Elon Musk tweet counts, Itzy songs, "Will the 10-year Treasury yield hit 4.6% before 2027?").
- All 13 have `last_resolution_status=NULL` and `last_outcome_prices=NULL`. Gamma's `/markets?condition_ids=<cid>` endpoint is not populating these fields for them. This matches the cycle-22 finding that **`close_reason='market_resolved'` has fired ZERO times in production trade history** — the State 3 finalization mechanism is architecturally ready but unreachable because Gamma doesn't expose the status field we need for this slice.

## Recommendations

1. **No remediation needed** for the 13 listed markets.
2. **Watch the soak alert** — if `RESOLUTION_TIMEOUT` count keeps growing (now 13, prior 11, prior 3), it's the natural aging-in continuing, not a new bug.
3. **Cycle-24 follow-up:** verify whether Gamma's `umaResolutionStatus` is reachable via a different endpoint (e.g. the CLOB `/markets?condition_id=<cid>` endpoint that cycle 23 found is paginated and not filtered as a query). If yes, augment `_fetch_single_market` to source it from there.
4. **Cycle-24 follow-up:** if we ever start trading on markets in this aging-in pattern, add a stronger filter at trade-entry time (`_gate_market_state` already covers `near_expiry`, but not "Polymarket likely to leave you stranded with no UMA finalization" — that needs a separate heuristic).

## Full audit output

```
RESOLUTION_TIMEOUT count: 13
  with open trades NOW:    0
  with any historical pos: 0
  ran at:                  2026-05-26T01:09:35+00:00

- [no-position] closed=2026-05-18T06:25:02 age=7.8d Did a crypto hedge fund blow up?
- [no-position] closed=2026-05-18T06:25:02 age=7.8d Will Oberá Tennis Club win Liga Nacional de Básquetbol?
- [no-position] closed=2026-05-18T10:25:46 age=7.6d Will Rennes qualify for the League Phase of the 2026-27 UEFA Champions League?
- [no-position] closed=2026-05-18T20:39:34 age=7.2d Will Itzy release a song in 2026?
- [no-position] closed=2026-05-18T20:39:34 age=7.2d Will Elon Musk post 400-419 tweets in May 2026?
- [no-position] closed=2026-05-18T20:39:34 age=7.2d Will Elon Musk post 420-439 tweets in May 2026?
- [no-position] closed=2026-05-18T20:39:34 age=7.2d Will Slavia Prague win Czechia Fortuna Liga?
- [no-position] closed=2026-05-18T20:39:34 age=7.2d Will Sparta Prague win Czechia Fortuna Liga?
- [no-position] closed=2026-05-18T20:39:34 age=7.2d Will Viktoria Plzeň win Czechia Fortuna Liga?
- [no-position] closed=2026-05-18T20:39:34 age=7.2d Will Jablonec win Czechia Fortuna Liga?
- [no-position] closed=2026-05-18T20:39:34 age=7.2d Will 7REX win CD 2026 Split 1?
- [no-position] closed=2026-05-19T00:40:48 age=7.0d Will the 10-year Treasury yield hit 4.6% before 2027?
- [no-position] closed=2026-05-19T00:40:48 age=7.0d CDC issues Level 3 warning by December 31?
```

Re-run any time with `python3 -m scripts.audit.resolution_timeout_audit`.
