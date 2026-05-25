# Scraper source audit — 2026-05-26

Plan: `irene/plans/scraper-news-sources.md` (G1).
RAG source: `irene/questions/unstructured answers.md` lines 83–107.
Inputs: `irene/config.yaml` (`ingest.feeds`, `ingest.combined_sources`) and
`services/scraper-v2/config/per_host.yaml`.

Scope: classification + recommendations only. The only config change
shipped alongside this report is an additive `rss_url` pin for
`axios.com` in `per_host.yaml` (G1 "add the missing free tier-1 RSS"
sub-step). No source has been removed, quarantined, or cadence-changed
in this pass — those are explicitly user decisions.

Inventory totals:
- `ingest.feeds`: **62 standalone RSS URLs** (config.yaml lines 33–123),
  polled every 60 s via `/feed`.
- `ingest.combined_sources`: **35 homepage URLs** (lines 142–185),
  polled every 180 s via `/scrape mode=combined`. Strategy + RSS pin
  per host in `per_host.yaml`.

Classification key:
- **FREE_RSS_RECOMMENDED** — RAG-blessed free tier (Axios, AP, FT-myFT).
- **TIER_1_PROHIBITED** — RAG-named ToS-prohibited + WAF-protected
  (Bloomberg, Reuters, NYT). High risk if we touch HTML/headless.
- **SITEMAP_NEWS** — publisher-served news sitemap (medium risk; their
  own machine feed, but redistribution ToS still applies).
- **PUBLISHER_RSS** — publisher's own public RSS, polled directly. Low
  risk.
- **OTHER** — tier-2 sources not on the RAG hot list.

Risk levels are about **scraping risk** (IP blacklist, ToS takedown),
not signal value. Per-source PnL ranking (i-G1) is a separate query
and blocks any actual removal decision.

---

## FREE_RSS_RECOMMENDED (RAG tier — low risk, add where missing)

| Source | Current poll mode | Risk | Recommendation |
|---|---|---|---|
| axios.com | combined-mode HTML only (`auto`, no `rss_url`) → falls back to HTML | LOW (after pin) | **PIN RSS** — `rss_url: https://axios.com/feeds/feed.rss` added in this pass. HTML fallback kept. |
| apnews.com | sitemap_news (`apnews.com/news-sitemap-content.xml`, 761 entries/poll) | LOW (sitemap is publisher-served) | KEEP. Recommend also adding AP category RSS feeds to `ingest.feeds` (Politics / Business / World / US / Tech) — lower fanout than the 761-entry sitemap, faster to parse. Requires a probe pass to enumerate exact URLs (RAG line 88 mentions them but does not list them). |
| ft.com | obscura render (headless) | **HIGH** (see TIER_1_PROHIBITED) | RAG says FT offers free myFT RSS for registered users. Decision point, not auto-add — needs the user's call on registering a personal FT account for bot use. Touches `irene/docs/legal-ops-hosting.md`. |

---

## TIER_1_PROHIBITED (RAG explicitly names — HIGH risk if we scrape HTML/headless)

| Source | Current poll mode | Risk | Recommendation |
|---|---|---|---|
| bloomberg.com | combined-mode + **obscura render** (per_host.yaml lines 50–51) | **HIGH** | CONFIRM PERMISSION / QUARANTINE. RAG lines 90–91 explicitly name Bloomberg as ToS-prohibited and WAF-protected. Headless render is the exact access mode that trips IP blacklisting. Holding action suggested: lower poll cadence (cheap, reversible). Removal blocked on i-G1 per-source PnL. |
| reuters.com | sitemap_news (`reuters.com/arc/outboundfeeds/news-sitemap/`, 50 entries/poll) | MED | KEEP w/ note. Publisher-served sitemap is softer than HTML scrape, but Reuters ToS still restricts redistribution. Not urgent. Include in i-G1 PnL query. |
| nytimes.com (combined_sources line 148) | combined-mode HTML + pinned RSS (`rss.nytimes.com/.../HomePage.xml`) | MED–HIGH on the HTML half | The publisher RSS half is fine and is also in `ingest.feeds` (line 40 + subsections 58–63). The HTML-homepage half of combined mode is the risky half. Recommendation: keep the RSS, drop the homepage HTML pull only — but this is a user-decision quarantine, deferred. |

---

## SITEMAP_NEWS (publisher-served news sitemaps — medium risk)

| Source | Sitemap URL | Risk | Recommendation |
|---|---|---|---|
| apnews.com | `apnews.com/news-sitemap-content.xml` (761 entries) | LOW | KEEP. |
| reuters.com | `reuters.com/arc/outboundfeeds/news-sitemap/?outputType=xml` (50 entries) | MED (RAG tier-1) | KEEP w/ tier-1 note. |
| wsj.com | `wsj.com/news-sitemap.xml` (+ `rss_url` `feeds.a.dj.com/rss/RSSWorldNews.xml`) | MED | KEEP. WSJ is tier-1-adjacent — not RAG-named but same publisher class as FT. Watch HTTP error rate per-host. |

---

## PUBLISHER_RSS (publisher-served public RSS — low risk)

These are entries in `ingest.feeds` polled directly via `/feed`, plus
the `rss_url` pins in `per_host.yaml` that combined mode parallel-fetches.

| Publisher | Feeds (rolled up) | Risk | Recommendation |
|---|---|---|---|
| BBC | `feeds.bbci.co.uk` world + 9 subsection feeds (business, politics, tech, sci-env, health, US/CA, Europe, Middle East, Asia) | LOW | KEEP. |
| NYT | `rss.nytimes.com` HomePage + US, World, Business, Politics, Technology, Science | LOW (the RSS half — see TIER_1 entry above for the HTML half) | KEEP RSS. |
| Guardian | `theguardian.com` world + us-news, politics, business, technology, environment | LOW | KEEP. |
| WSJ | `feeds.a.dj.com` WorldNews, WSJD, MarketsMain, Opinion | LOW (RSS still works) | KEEP. |
| Politico | `rss.politico.com` congress, whitehouse, defense + `feedburner.com/politico` | LOW | KEEP. |
| The Hill | `thehill.com/feed/` + senate, house, administration | LOW | KEEP. |
| NPR | `feeds.npr.org` 1001/1006/1014/1004/1019 (world/business/politics/world/tech) | LOW | KEEP. Note: feed `1001` and `1004` both labelled "world" in config.yaml comments — verify they are distinct. |
| Sky News | world, politics, business, technology | LOW | KEEP. |
| CBS | world, politics, moneywatch | LOW | KEEP. |
| ABC | international, US, politics | LOW | KEEP. |
| Al Jazeera | `aljazeera.com/xml/rss/all.xml` | LOW | KEEP. |
| Times of Israel | `timesofisrael.com/feed/` | LOW | KEEP. |
| Times of India | `timesofindia.indiatimes.com/rssfeedstopstories.cms` | LOW | KEEP. |
| The Hindu | `thehindu.com/news/feeder/default.rss` | LOW | KEEP. |
| Indian Express | `indianexpress.com/feed/` | LOW | KEEP. |
| Economist | `economist.com/the-world-this-week/rss.xml` | LOW | KEEP. |
| El País (EN) | `feeds.elpais.com/.../english.elpais.com/portada` | LOW | KEEP. |
| La Repubblica | `repubblica.it/rss/homepage/rss2.0.xml` | LOW | KEEP. |
| Süddeutsche | `rss.sueddeutsche.de/rss/Topthemen` | LOW | KEEP. |
| Politico EU | `politico.eu/feed/` | LOW | KEEP. |
| DW | `rss.dw.com/rdf/rss-en-all` | LOW | KEEP. |
| France 24 | `france24.com/en/rss` | LOW | KEEP. |
| CNA | `channelnewsasia.com/rssfeeds/8395986` | LOW | KEEP. |
| Mother Jones | `motherjones.com/feed/` | LOW | KEEP. |
| ProPublica | `propublica.org/feeds/propublica/main` | LOW | KEEP. |
| The Atlantic | `theatlantic.com/feed/channel/politics/` | LOW | KEEP. |
| Slate | `slate.com/feeds/news-and-politics.rss` | LOW | KEEP. |
| RealClearPolitics | `realclearpolitics.com/index.xml` | LOW | KEEP. |
| MarketWatch | `feeds.marketwatch.com/marketwatch/topstories/` | LOW | KEEP. |
| Yahoo Finance | `finance.yahoo.com/news/rssindex` | LOW | KEEP. |
| TechCrunch | `techcrunch.com/feed/` | LOW | KEEP. |
| Ars Technica | `feeds.arstechnica.com/arstechnica/index` | LOW | KEEP. |
| The Verge | `theverge.com/rss/index.xml` | LOW | KEEP. |
| Wired | `wired.com/feed/rss` | LOW | KEEP. |
| CNN | `rss.cnn.com/rss/edition.rss` (only via combined-mode `rss_url` pin) | LOW | KEEP. CNN not RAG-named as tier-1 prohibited; RSS pin is fine. |

---

## OTHER

| Source | Mode | Notes / Recommendation |
|---|---|---|
| washingtonpost.com (combined_sources line 149) | combined-mode `auto`, no `rss_url` pin → degenerates to HTML-only | RAG does not explicitly name WaPo, but the plan flags it as "implied tier-1." MED–HIGH risk on the HTML-only path. Recommendation: probe for a public RSS endpoint; if none, CONFIRM PERMISSION / consider lowering cadence. Deferred (user decision). |
| cnbc.com (combined_sources line 158) | combined-mode `auto`, no `rss_url` (per per_host.yaml header comment: paywalled their public RSS in last year) | MED. Tier-2 finance, not RAG-named. KEEP for now. |
| ft.com (combined_sources line 152) | obscura render | Already covered in FREE_RSS_RECOMMENDED + TIER_1_PROHIBITED rows — HIGH risk, decision pending on myFT registration. |

---

## Decisions for human review

These three are the items that need an explicit "keep / quarantine /
remove" call from the user before any further config change. All three
are blocked on the i-G1 per-source PnL query — without it we cannot
weigh scraping-risk against signal value.

1. **`bloomberg.com` obscura render** (per_host.yaml lines 50–51) —
   RAG line 90–91 explicitly names Bloomberg as ToS-prohibited +
   WAF-protected. Headless render is exactly the access mode that
   trips IP blacklisting. Holding action if we want to defer the call:
   lower combined-mode cadence for this host only.
2. **`ft.com` obscura render** (per_host.yaml lines 48–49) — same risk
   profile as Bloomberg. Decision tree: (a) remove, (b) lower cadence,
   or (c) register a free FT account and switch to myFT RSS (RAG
   line 88). Option (c) is the only one that preserves signal —
   needs user sign-off on personal-account use for the bot.
3. **`https://www.nytimes.com` in `combined_sources`** (config.yaml
   line 148) — the HTML-homepage half of combined mode is the risky
   half. The publisher RSS (`rss.nytimes.com`) is already in
   `ingest.feeds` and would still ship if we dropped this entry.
   Recommendation pending PnL: drop the homepage HTML pull, keep all
   RSS.

Secondary items (lower urgency, flagged for completeness):

4. **`washingtonpost.com`** — combined-mode HTML only, no RSS pin, no
   RAG tier-1 callout but plan flags as implied tier-1. Probe for
   public RSS; if absent, treat like NYT-HTML.
5. **AP category RSS feeds** — RAG line 88 mentions them but does not
   enumerate. Needs a one-off probe pass against AP's known category
   slugs (Politics / Business / World / US / Tech) before any feed URL
   lands in `ingest.feeds`.
6. **NPR feed numbering** — `feeds.npr.org/1001` and `feeds.npr.org/1004`
   are both labelled "world" in config.yaml comments (lines 41, 87).
   Confirm they are actually distinct, otherwise one is a dupe.

---

## Change shipped alongside this report

- `services/scraper-v2/config/per_host.yaml` — added
  `rss_url: https://axios.com/feeds/feed.rss` to the `axios.com`
  entry. Strategy stays `auto`; HTML fallback unchanged. Combined mode
  will now parallel-fetch the native Axios RSS instead of degenerating
  to HTML-only. RAG citation: `unstructured answers.md` line 88.
  Per the plan's verification step, post-deploy we should confirm
  scraper-v2 `/feed` returns ≥10 items for the new URL (cycle 6a
  convention, config.yaml line 31).
