# Seulgi — Potential Pitfalls

Collected from design review, live testing, and known failure modes. Each
section describes the problem, why it happens, and what to do about it.

---

## 1. Scraper / Watcher

### 1.1  Paywall sites return empty headlines
**Symptom**: bloomberg, ft, wsj, nytimes, washingtonpost show `Total headlines: 0` in watcher_status.txt despite the scraper fetching them.
**Why**: Playwright grabs the page but paywall login walls contain very few `<a>` links with readable title text. The watcher's `_is_junk` filter strips nav links, so nothing passes.
**Fix**: Lower `_is_junk` thresholds for these sites, or switch to their RSS/API feeds instead of scraping the homepage.

### 1.2  DEGRADED status does not mean the site is down
**Symptom**: Every site shows `DEGRADED (2 failures)` in the first few minutes.
**Why**: The watcher uses a backoff sequence (20 s → 40 s → 80 s…). The first poll almost always races with the scraper's cold-start, causing 1-2 failures before it stabilises. After 5 consecutive failures the site is marked `UNREACHABLE`.
**Fix**: Increase `max_failures` in config.yaml if you see legitimate flakiness. The current value of 5 is a good balance.

### 1.3  Selenium / Chrome container OOM
**Symptom**: Container exits with OOM or undetected_chromedriver hangs.
**Why**: `MAX_BROWSERS=3` means up to 3 Chrome processes running simultaneously. Each uses ~200 MB RAM.
**Fix**: Set `MAX_BROWSERS=1` in the scraper's `.env` if the host has limited RAM, or add `--memory` to the docker-compose service.

### 1.4  Cloudflare challenge pages logged as success
**Symptom**: Headline lists contain entries like "Just a moment…" or "DDoS-Guard".
**Why**: `_is_challenge_page()` checks `html.lower()` for known markers, but very large challenge pages (>30 KB) skip the text check to avoid false positives. Some new challenge signatures may not be in `_CHALLENGE_MARKERS`.
**Fix**: Add the new challenge marker string to `_CHALLENGE_MARKERS` in scraper.py and rebuild.

### 1.5  SQLite cache path collision
**Symptom**: Two scraper containers writing to the same `SQLITE_CACHE_PATH`.
**Why**: Default path is `/tmp/scraper_cache.db` which is container-local; no collision unless you share the tmp volume.
**Fix**: Only a problem if you mount the same tmpfs across replicas. Each replica should have its own cache path.

### 1.6  Preflight state never resets for a domain
**Symptom**: A domain that was unreachable during startup never tries requests/cloudscraper again even after the site recovers.
**Why**: `preflight_at` is only reset when the TTL expires (`_PREFLIGHT_CACHE_TTL = 300 s`). If the domain was set to `min_method_idx=2` before the TTL expires, it stays there.
**Fix**: The TTL resets naturally. If you need immediate recovery, restart the scraper container.

---

## 2. AI Classifier

### 2.1  Batch parse failures → silent fallback to per-market calls
**Symptom**: Log line `"batch parse failed — falling back to per-market classify"`.
**Why**: The LLM may return the JSON array wrapped in an unexpected key, or return fewer items than requested. `_parse_batch_response` handles common wrappers but can't cover every model quirk.
**Impact**: Cost and latency revert to N×single-market calls for that headline.
**Fix**: Check which model is producing bad output. Gemini 2.5 Flash is reliable; some open models wrap output differently. Consider adding the new wrapper key to `_parse_batch_response`.

### 2.2  `response_format: json_object` not supported by all models
**Symptom**: OpenRouter returns a 400 or the model ignores the format and returns prose.
**Why**: `response_format` is an OpenAI extension. Models on OpenRouter that are not OpenAI-compatible may ignore it.
**Fix**: Verify your model supports it at openrouter.ai/models. Gemini 2.5 Flash Lite and standard Flash both support it. If using a different model, test with `classify()` first.

### 2.3  Low confidence inflation
**Symptom**: Almost every classification comes back at confidence 0.50-0.55, causing most signals to be skipped (`low_confidence`).
**Why**: Generic headlines (e.g. "Markets open lower") match many markets but the connection is weak. The LLM correctly expresses uncertainty.
**Fix**: Lower `min_confidence` in config.yaml to 0.40, or increase `similarity_threshold` in markets config to only pass through more relevant matches before calling the LLM.

### 2.4  Republish rate too high / all headlines marked NEUTRAL
**Symptom**: `NewsMemory` marks 80%+ of headlines as republishes.
**Why**: `dedup_distance_threshold` is too high (looser = more republishes). Default 0.12 (~88% similarity) is tight; if you increase it (e.g. 0.25), many topically similar but distinct headlines get flagged.
**Fix**: Keep the threshold at 0.12. If you see too many false positives, decrease it (e.g. 0.08 ≈ 92% similarity).

### 2.5  Premium model routing costs spike
**Symptom**: Unexpected large OpenRouter bill when `premium_model` is set.
**Why**: `premium_sim_threshold: 0.70` routes any headline with a match at ≥0.70 cosine similarity to the premium model. Breaking news on a highly active topic (Iran war, Fed meeting) can sustain this for hours.
**Fix**: Raise `premium_sim_threshold` to 0.85, or leave `premium_model` empty (it defaults to disabled).

### 2.6  OpenRouter 429 during burst headlines
**Symptom**: `[ai] giving up after 3 rate-limit retries` in logs; signals written but all classified NEUTRAL.
**Why**: Many watchers fire simultaneously (e.g. on startup) and each triggers classify_batch calls. The default semaphore of 5 limits concurrency but all 5 slots can hit 429 together.
**Fix**: Increase `max_concurrent` in the classifier (passed at construction in main.py), or add a per-minute rate budget. OpenRouter's default limit is 200 RPM for free tier / 1000+ RPM for paid.

---

## 3. Market Index

### 3.1  Market index goes stale and misses new high-volume markets
**Symptom**: Markets created after the last refresh cycle are never matched.
**Why**: `markets.refresh_hours: 4` means the index is rebuilt every 4 hours. A new viral market created in-between won't appear.
**Fix**: Lower `refresh_hours` to 1 or 2 during active trading periods. Each refresh fetches ~50K markets from Gamma; it takes ~30 s.

### 3.2  `max_tracked_markets: 300` misses niche markets
**Symptom**: A headline about a specific election is in the news but the matching Polymarket question is ranked #350 by volume and never matched.
**Why**: Volume-based capping means low-liquidity markets are excluded.
**Fix**: Raise to 500 or 1000. At 300 the index builds in ~15 s; at 1000 it takes ~60 s and uses ~400 MB RAM for embeddings.

### 3.3  Embedding model cold start delays first classification
**Symptom**: First classification after startup takes 15+ seconds.
**Why**: `sentence-transformers/all-MiniLM-L6-v2` (~85 MB) is loaded on first use. The model cache is in a named Docker volume (`seulgi_model_cache`).
**Fix**: Pre-warm by keeping the container running. On fresh deployments the first soak will be slow.

---

## 4. Database

### 4.1  `headline_id` NULL in old rows
**Symptom**: `headline_id` is NULL in signals/trades/engine_results rows from before this migration.
**Why**: The migration adds the column with `DEFAULT NULL`. Rows inserted before the migration obviously don't have a value.
**Fix**: This is expected and safe. The traceability checks in the live test only assert coverage ≥90%, not 100%.

### 4.2  DB grows unbounded
**Symptom**: `seulgi.db` reaches GB size after weeks of operation.
**Why**: Every poll from every watcher inserts a signal. With 15 sites at 20 s intervals, that's ~2700 signals/hour. Multiply by matches and you get thousands of engine_results rows per hour.
**Fix**: Add a periodic DELETE: `DELETE FROM signals WHERE detected_at < datetime('now', '-30 days')`. Consider adding an index on `detected_at` first.

### 4.3  aiosqlite serialized writes become a bottleneck
**Symptom**: `[trader] PAPER ...` log lines cluster in bursts with gaps; high CPU when many headlines arrive simultaneously.
**Why**: aiosqlite serialises all writes through a single connection. Under burst load (e.g. 10 headlines in 1 s), DB writes queue up behind each other.
**Fix**: For paper trading this is fine. For live trading, migrate to PostgreSQL with asyncpg.

---

## 5. ChromaDB / NewsMemory

### 5.1  ChromaDB telemetry errors on startup
**Symptom**: `Failed to send telemetry event ... capture() takes 1 positional argument but 3 were given`
**Why**: ChromaDB 0.6.x has a bug in its posthog telemetry integration. It's cosmetic and doesn't affect functionality.
**Fix**: Nothing required. Will be fixed in a future chromadb release. You can suppress it with `ANONYMIZED_TELEMETRY=False` env var.

### 5.2  ChromaDB collection grows without bound
**Symptom**: `news_memory.size` keeps growing; eventually slows down HNSW queries.
**Why**: Every unique headline is added. With 15 sites × 40 links × 48 polls/day = ~29K entries/day.
**Fix**: ChromaDB HNSW handles hundreds of thousands of vectors efficiently. At >1M entries (~5 weeks) consider pruning old entries or sharding by date.

### 5.3  False positive republish on site's persistent nav links
**Symptom**: Links like "Read our privacy policy" or "Subscribe" appear as republishes every poll.
**Why**: These links have embeddings similar to each other. Once one is stored, subsequent identical links are flagged.
**Impact**: Low — they'll be classified as republish → NEUTRAL, which is correct (the market doesn't care about nav links).
**Fix**: The watcher's `_is_junk` filter should catch most of these before they reach NewsMemory. If not, tighten `_is_junk` regex.

---

## 6. Operational / Deployment

### 6.1  `docker compose up -d` doesn't rebuild on code changes
**Fix**: Always `docker compose build <service>` then `up -d <service>`.

### 6.2  Config changes require container restart
**Why**: Config is read once at startup. Even though config.yaml is mounted read-only, changes won't be picked up until restart.
**Fix**: `docker compose restart seulgi`

### 6.3  Seulgi can't reach scraper if only seulgi is restarted
**Why**: If the scraper container is healthy but seulgi restarts, the `depends_on: scraper: condition: service_healthy` check only runs at start time.
**Fix**: If both containers need restarting, `docker compose restart scraper seulgi` in that order.

### 6.4  Missing `OPENROUTER_API_KEY` produces silent NEUTRAL classifications
**Symptom**: Everything runs, watchers fire, but zero trades are ever executed; all signals are `neutral`.
**Why**: A missing or invalid API key causes every OpenRouter call to return 401, which the classifier catches and returns `ClassifyResult(NEUTRAL, 0.0, "api_error")`.
**Fix**: Check `docker compose logs seulgi | grep "No OPENROUTER"` or `| grep "OpenRouter 401"`.

### 6.5  Live soak test threshold is scaled but may still be too strict on slow news days
**Symptom**: `test_seulgi_system_soak` fails with `Only N new signals in 60m — expected ≥30`
**Why**: `_MIN_HEADLINES_PER_HOUR = 30` assumes active news. Weekends or public holidays produce far fewer headlines.
**Fix**: Set `MIN_HEADLINES_PER_HOUR=10` in the env before running the soak on slow days, or accept the failure as informational.
