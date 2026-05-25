# Cycle 9 — ET deadlines, similarity-floor docs, scraper audit + Axios pin

- **Commit:** `98677e6` (2026-05-25)
- **Theme:** First round of plan execution — three parallel-agent outputs from `plans/*`. No production-default changes; all behavioural shifts backwards-compatible.

## What landed

### ET-based deadline parsing (`plans/resolution-oracle.md` D2 slice)
- `app/stages/trade.py` — new `_parse_market_deadline`, `_parse_end_date_string`, `_parse_title_deadline`. `_days_to` refactored to accept a dict (preferred) or legacy string.
- Naïve ISO end_dates → `America/New_York`; explicit UTC suffix preserved; explicit offset respected.
- Title fallback `"by June 30"` → `2026-06-30 23:59:59 ET = 2026-07-01 03:59:59 UTC`; fires only when `end_date` is absent.
- `zoneinfo` stdlib only — no new deps.
- +13 tests in `app/tests/test_deadline_parsing.py` (EST/EDT, legacy-string compat, dict-priority over title, None defensives).
- D1 (three-state close machine) and D3 (dispute columns) intentionally out of scope.

### Similarity-floor docs clarification (`plans/sizing-edge-similarity.md`)
- `docs/similarity_thresholds.md` — 3 floors documented with units, file:line consumers, cosine-similarity-vs-distance conversion.
- `questions/research_questions.md` F3 — appended unit-clarification note linking new doc.
- Plan-error caught during agent verification: `cfg.match.min_similarity` does not exist; the 0.35 floor lives on `TradeSection` and is effectively redundant given `match.similarity_floor=0.50` gates upstream. Documented, not removed.
- NO config or code changes.

### Scraper audit + Axios RSS pin (`plans/scraper-news-sources.md`)
- `reports/2026-05-26-scraper-source-audit.md` — 97 sources classified (FREE_RSS_RECOMMENDED / TIER_1_PROHIBITED / SITEMAP_NEWS / PUBLISHER_RSS / OTHER) with per-source risk + recommended action.
- `services/scraper-v2/config/per_host.yaml` — single additive edit: `rss_url` pin on `axios.com`. Strategy stays `auto`; HTML fallback unchanged.
- 3 primary high-risk sources flagged for human review (`bloomberg.com` obscura, `ft.com` obscura, `nytimes.com` combined-HTML). NO removals executed.

## Verified
- `pytest app/tests/` → **419 / 419 passed** (+13 from deadline-parsing tests).
- scraper-v2 suite: 89/89 pass.

## Not yet (carried forward)
- Three-state close machine in `closer.py` (D1).
- Dispute-exposure columns (`dispute_observed`, `dispute_resolved_at`).
- Decisions on the 3 flagged-for-review high-risk scraper sources.
- Per-market `fee_rate` fetcher.
- Explicit spread term in trade.py edge math.
- Background data collection to replace 📊 spread/latency seeds.
