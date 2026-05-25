# Plan — scraper-v2 news sources (G1–G4)

> Source: `irene/questions/unstructured answers.md` lines 83–107, batch
> `2026-05-25-unstructured-1`. Cross-refs: `research_questions.md` i-G1,
> i-G2, O-G1, O-G2, O-G3, O-G4 (lines 106–110, 204–215);
> `research_answers.md` section G (currently open).

## Context

The current ingest surface (after cycle 6a, 2026-05-25) is split across
two paths in `irene/config.yaml`:

- `ingest.feeds` (lines 33–123): **~62 standalone RSS URLs** polled
  every 60s via the scraper-v2 `/feed` endpoint. All are first-party
  RSS published by the outlet (BBC, NYT `rss.nytimes.com`, Guardian,
  NPR, WSJ `feeds.a.dj.com`, Politico, The Hill, Sky News, CBS, ABC,
  plus tier-2 opinion / finance / tech / international).
- `ingest.combined_sources` (lines 142–185): **~35 homepage URLs**
  polled every 180s via `/scrape mode=combined`. The host's pinned
  fetch strategy is held in
  `services/scraper-v2/config/per_host.yaml`; some entries pin an
  `rss_url` (combined = RSS + HTML merge), some pin `sitemap_news`,
  and three pin `obscura` (headless render).

The RAG slice (lines 83–107) makes four protocol-level claims that map
directly onto holes / risks in the current setup:

1. **Axios has a direct free RSS at `axios.com/feeds/feed.rss`** that
   we are not currently polling. `per_host.yaml` line 44 has
   `axios.com: strategy: auto` with **no pinned `rss_url`** — so
   combined mode for `https://www.axios.com` (config.yaml line 155)
   silently degenerates to HTML-only. The native RSS is free for the
   taking and we are skipping it.
2. **Bloomberg, Reuters, NYT prohibit automated scraping** in robots.txt
   + ToS and run WAFs that will rate-limit / IP-blacklist. We currently
   render `bloomberg.com` and `ft.com` via **obscura** (per_host.yaml
   lines 48–51) — headless-browser scraping of two of the publishers
   the RAG explicitly names as WAF-protected.
3. **No free push (SSE / WebSocket) news feeds exist** for retail.
   Confirms the polling-only ingest design — no architectural change.
4. **Enterprise wire pricing is prohibitive**: Bloomberg Terminal
   ~$2,665/mo, LSEG Workspace $300–$1,833/mo, AP / Dow Jones custom
   B2B. Off the table for solo-dev budget; document and move on.

The statistical claim "highest signal-per-dollar" is a seed only — the
right per-source ranking comes from joining `articles → signals →
trades → price_impacts` (i-G1 / i-G2), not from RAG opinion. Section G
of `research_answers.md` is currently empty; this batch fills it.

## Findings

### Protocol facts (act on these)

- **Free RSS exists for Axios** (line 88): `axios.com/feeds/feed.rss`.
  Direct-poll candidate.
- **AP publishes free category feeds for non-commercial use**
  (line 88). We currently reach AP only via `sitemap_news` against
  `apnews.com/news-sitemap-content.xml`. Category-level RSS would be
  additive (and lower-fanout than the 761-entry sitemap noted in
  `per_host.yaml` line 33).
- **FT offers myFT custom RSS for registered users** (line 88).
  Requires a free FT account; can replace the obscura render of `ft.com`
  if we register.
- **Bloomberg, Reuters, NYT prohibit automated scraping** (lines 90–91).
  - Bloomberg: we run `obscura` render. **At risk.**
  - Reuters: we use their own `news-sitemap` (their published machine
    feed). Lower risk than HTML scrape but ToS still restricts
    redistribution.
  - NYT: we use their own `rss.nytimes.com` (publisher-served public
    RSS, generally permitted) **and** we HTML-scrape the homepage via
    combined mode. The HTML side is the risky half.
- **WAFs + recent AI-extraction blocks** (line 91): aggressive
  scraping of tier-1 risks IP blacklisting of the host/VPS.
- **No free push feeds** (lines 93–94). Polling-only path stays.
- **Enterprise pricing ballpark** (lines 99–107): Bloomberg ~$2,665/mo
  ($31,980/yr); LSEG Workspace $300–$1,833/mo; AP Newsroom + Dow Jones
  custom B2B (institutional minimums).

### Seeds only (📊 — verify with our own data, don't act on)

- **"Highest signal-per-dollar"** ranking (line 85). True ranking is
  i-G1: per-source net PnL from
  `articles JOIN signals JOIN trades JOIN price_impacts`. RAG's
  Axios/AP nomination is a prior, not a finding.
- **"Polite high-frequency polling"** as the latency floor (line 94).
  Our actual floor is whatever cycle-7 throughput.txt + CLT stats say.

### Stale (⚠️ — ballpark, drifts)

- **Bloomberg / LSEG monthly numbers**. RAG itself notes "Bloomberg
  recently raised its prices for 2026" (line 97). Use as
  order-of-magnitude only.

## Proposed changes

### G1 — Source-list audit (precondition for everything else)

**Do this first; no other change ships without the audit report.**

- Inventory both ingest paths:
  - 62 RSS URLs in `config.yaml ingest.feeds`
  - 35 homepages in `config.yaml ingest.combined_sources` × their
    pinned strategy in `per_host.yaml`
- For each source, tag:
  - `publisher_rss` (publisher's own RSS, e.g. `rss.nytimes.com`,
    `feeds.bbci.co.uk`) — **lowest risk**
  - `publisher_sitemap` (publisher's own news-sitemap, e.g. AP,
    Reuters, WSJ) — **low risk**
  - `html_scrape` (combined mode hitting homepage HTML when no RSS
    pin) — **medium risk**
  - `obscura_render` (headless-browser render) — **high risk on tier-1**
- Cross-reference against the RAG tier-1 prohibited list (Bloomberg,
  Reuters, NYT) plus implied (FT, WSJ, WaPo via "tier-1 publishers").
- Output: `irene/reports/2026-05-26-scraper-source-audit.md` with a
  table — source × access mode × RAG-tier-1-flag × current per-source
  contribution to `price_impacts.realised_pnl_usdc` (from i-G1 query).

### G1 — Add the missing free tier-1 RSS

Once the audit lands and the per-source PnL is in hand:

- **Add Axios native RSS** (line 88). In `per_host.yaml` line 44 set
  `rss_url: https://axios.com/feeds/feed.rss` so combined mode picks
  it up. Verify with `scripts/audit.py` first that the feed returns
  ≥10 entries (cycle 6a probing convention, config.yaml line 31).
- **Add AP category feeds** (line 88). Probe the documented categories
  (Politics, Business, World, US, Tech) and add the survivors to
  `ingest.feeds`. These complement, not replace, the `sitemap_news`
  pull — they are lower-volume and faster to parse.
- **FT myFT** (line 88). Decision point, not an auto-add: requires a
  registered FT account whose ToS may bar bot use. Open question for
  the user, not a unilateral change.

Cite RAG lines 87–89 in the config.yaml comment for each addition.

### G2 — Quarantine the high-risk scrapes (HUMAN REVIEW REQUIRED)

The audit will almost certainly flag at least these:

- **`bloomberg.com` — `obscura` render** (per_host.yaml line 50–51).
  RAG line 90–91 explicitly names Bloomberg as ToS-prohibited and
  WAF-protected. Headless render is exactly the access mode that
  trips IP blacklisting.
- **`ft.com` — `obscura` render** (per_host.yaml line 48–49). FT
  belongs to the same tier; same risk profile.
- **`https://www.nytimes.com` in `combined_sources`** (config.yaml
  line 148). The HTML-homepage half of combined mode is the risky
  half; the publisher RSS (already in `ingest.feeds` line 40 +
  subsections lines 58–63) is fine and would still ship.

**Do not auto-propose a deletion PR.** The audit report must list these
with: (a) RAG citation, (b) per-source PnL contribution from i-G1,
(c) recommendation. Removal is a user decision. Until the user decides,
suggested holding action is to **lower their poll cadence** (cheap,
reversible, reduces IP-blacklist surface).

Reuters and WSJ via `sitemap_news` are a softer call — they are
publisher-served sitemaps, not scraped HTML. Include in the audit but
do not flag as urgent.

### G3 — Push feeds

**No code action.** RAG lines 93–94 confirm no free public push exists.
Polling stays. Note for `irene/docs/`: any "we should add SSE/WebSocket
ingest" idea is dead until we are paying enterprise rates.

### G4 — Commercial wire decision document

**No code action.** Write `irene/docs/wire-pricing.md` (new) with the
RAG table (lines 99–107) as the "do we ever subscribe?" reference,
flagged ⚠️ ballpark per the cycle-2026 price increase note. Decision
criterion: only revisit if the cycle-7+ infra is consistently
PnL-positive AND a single wire's per-source PnL contribution justifies
five figures/year. Far from a current concern.

## Open questions

- **i-G1 result.** What is the current per-source contribution to
  `price_impacts.realised_pnl_usdc`? Drives prioritisation; blocks
  the quarantine decisions in G2.
- **Are we tripping any WAF today?** Audit script should expose
  per-host HTTP error rates from scraper-v2 metrics. If `bloomberg.com`
  or `ft.com` obscura renders are already returning 4xx/5xx at
  elevated rates, the quarantine is moot (we're already broken there).
- **FT myFT** — does the user want to register a personal FT account
  for bot use? Touches the legal-ops question in
  `legal-ops-hosting.md`.
- **AP category feed URLs.** RAG line 88 mentions them but does not
  enumerate. Need a probe pass before adding.

## Verification

- **Pre-change:** run `services/scraper-v2/scripts/audit.py` to
  regenerate `per_host.suggested.yaml` and the per-host fetch-stat
  snapshot in `data/audit/`. Write the audit report under
  `irene/reports/2026-05-26-scraper-source-audit.md`.
- **Post-change** (per addition): confirm scraper-v2 `/feed` returns
  ≥10 items for any newly added feed URL (cycle 6a convention,
  config.yaml line 31). Confirm post-change article volume ≥ baseline
  in `data/throughput.txt`.
- **Post-quarantine** (if a user-approved removal lands): re-run i-G1
  over the next 7 days to confirm removed-source PnL was indeed
  zero / negative (not high-variance positive).

## Cross-cutting notes

- **i-G1 / i-G2 ↔ this plan.** The per-source PnL and per-source
  ingest-latency queries (research_questions.md lines 106–110) are
  the entire quantitative basis for G2's quarantine recommendations.
  They block this plan more than this plan blocks them — get those
  queries scripted first.
- **Per-source value-of-information.** Overlaps with
  `latency-repricing.md` "Findings → Statistical claims" — the
  `price_impacts` table is the shared substrate. Wire the audit's
  per-source PnL column directly off the same join.
- **Legal / jurisdiction.** Tier-1 scraping ToS risk here is
  publisher-side (IP blacklist, takedown notice). User-side
  jurisdiction risk (CFTC, sanctions) is `legal-ops-hosting.md`'s
  problem — they do not collapse into one plan.
- **`scraper:9000` legacy.** master-plan history references the v1
  scraper at `scraper:9000`; cycle 4 cut over to v2 at `9001`
  (config.yaml lines 14–17). This plan only touches v2; if any code
  still references the v1 hostname, that is a separate cleanup, not
  in scope.

## Status update for `research_answers.md` section G

Once this plan ships its audit, append to `research_answers.md`:

- **G1 — Source rankings.** ⚠️ partial — RAG nominates Axios + AP as
  highest signal-per-dollar (line 85, 88). Our own ranking pending
  i-G1 query results.
- **G2 — ToS / robots.txt.** ✅ — Bloomberg, Reuters, NYT prohibit
  scraping; WAFs in place; recent AI-extraction blocks (lines 90–91).
- **G3 — Push feeds.** ✅ — none free; polling-only stays
  (lines 93–94).
- **G4 — Wire pricing.** ⚠️ ballpark — Bloomberg ~$2,665/mo, LSEG
  $300–$1,833/mo, AP / Dow custom B2B (lines 99–107); 2026 prices
  already drifted up per line 97.
