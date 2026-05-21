# Seulgi cycle methodology

This doc describes how we iterate the seulgi paper-trader. Each pass is a "cycle": a snapshot of the previous run, a structural change, a fresh trading slate, and a postmortem.

## The four phases of a cycle

### 1. Snapshot

Before any change to code, schema, config, or rules:

```bash
python3 cycle.py save cycleN_end
```

Snapshots are byte-identical SQLite backups (using the SQLite online-backup API so they're safe against a live bot) plus a copy of `news_chroma`, `dashboard/`, `analysis/`, `postmortems/`. They live under `seulgi/snapshots/<name>/` and are the only insurance against bad ideas.

### 2. Implement

Make the changes. Keep the implementation scope tight to one or two structural hypotheses per cycle — multi-front cycles produce uninterpretable postmortems.

**Anatomy of a healthy change set:**
- One or two structural fixes (e.g., "track signed_edge alongside production edge", "add trading-fee model")
- An expansion of the data input (e.g., add news sites in a region we don't cover)
- A docs update describing the hypothesis being tested

**Things to keep stable across cycles for clean comparison:**
- The engine set (aggressive / balanced / inverted_highconf)
- The base trading parameters (TP / SL / trailing / min_hold) unless explicitly testing them
- The market refresh cadence

### 3. Reset and run

Reset the per-engine trading state but keep slow-to-rebuild assets (the market universe + news dedup memory) unless the market-finding code itself changed:

```bash
python3 cycle.py rebuild                  # picks up app/ edits
python3 cycle.py cycle cycleN_end         # snapshots, resets trading, restarts
```

Add `--fresh-markets` when the market-finding code itself changed and you want the bot to re-discover the universe.

After this, you need to **force-recreate the container** so docker picks up the new image:

```bash
docker compose up -d --force-recreate seulgi
```

`docker compose start seulgi` is not enough — it reuses the stopped container's old image.

Let the run go for at least 6 hours before evaluating. AI/scraper warm-up takes 15 minutes; meaningful PnL needs longer.

### 4. Postmortem

```bash
python3 cycle.py postmortem
```

Read it. Compare to the previous postmortem's `delta_vs_previous.md`. The questions to answer:

1. Did `ic_global` move toward zero or away from it? Toward zero with the current model means the AI is becoming less informative as time passes — investigate the prompt or model.
2. Which confidence buckets changed `captured` share? If high-conf calls stay anti-informative, the `inverted_highconf` engine should continue to beat `balanced`.
3. Per-engine PnL ordering — is aggressive still dominating?
4. Skip-reason histogram — is the bot starved, saturated, or healthy?
5. Lessons table — what new rules has the aggregator added since the previous postmortem?

## Continuous reinforcement (the lessons system)

Postmortems are sporadic; the AI sees them at most every few hours and only via the operator copying findings into prompts. To make feedback constant, seulgi runs an in-process lessons aggregator (see `app/lessons.py`).

### How it works

Every 10 minutes (configurable in `lessons.runner` at `main.py`), the aggregator scans `price_impacts` rows that have classified outcomes (`delta_t5m IS NOT NULL`), buckets each by `(source × market_type × confidence)`, and computes a signed IC + capture rate. Buckets with statistical signal (n ≥ 10 and |IC| ≥ 0.05) are upserted into the `lessons` table with one of three action levels:

- **`note`** — informative, lean on this combination
- **`downweight`** — IC mildly negative; reduce confidence before quoting
- **`skip`** — IC strongly negative; treat as NEUTRAL

The trader pulls the top ~8 active lesson notes once per ~60 seconds and injects them as a compact bullet list into the AI batch prompt header. So every classification call sees the most recent empirical findings without any manual operator intervention.

### Bootstrapping

On first startup with an empty `lessons` table, seven seed lessons from the cycle-3 postmortem are inserted (state-media downweights, conf ≥ 0.75 anti-informativeness, political-market caution, etc.). These are flagged `seeded=1` and `active=1` so the prompt is useful from headline #1. Each gets superseded by empirical evidence the moment the aggregator has enough data for that bucket.

### What it does NOT do

- It doesn't change the executed trade. The trader still uses the production edge formula. The lessons are advisory input to the classifier.
- It doesn't fire on tiny samples. Below `MIN_SUPPORT_FOR_EMPIRICAL = 10` the empirical lesson stays inactive; only seeded lessons cover sparse contexts.
- It doesn't get reset by `cycle.py cycle` (the lessons table is not in the truncate list — accumulated wisdom carries forward across cycles).

## What is logged per trade (cycle-4 additions)

The `trades` table now stores four extra columns:

- `signed_edge` — `p_hat - mid_price` (matches the production edge but renamed for clarity)
- `confidence_raw` — AI's claimed P(direction) at the time of the call
- `p_hat_if_raw` — what `p_hat` would be under the alternative "trust the AI's probability directly" model
- `signed_edge_if_raw` — `confidence_raw - mid_price`

Postmortems can now answer: "would the alternative edge model have flipped sign on this trade?" — which is the question that gates whether we promote it from shadow to production in a future cycle.

## Trading costs

Polymarket has no protocol fees but the bid-ask spread is real. As of cycle 4, every close deducts a configurable round-trip cost from realized PnL:

```yaml
trading:
  spread_cost_bps: 20.0    # 20 bps = 0.20% of stake per round trip
```

The default 20 bps is a conservative model of typical Polymarket spreads on 5-minute markets. Per-engine override is supported. Without this, a flat buy + immediate sell yielded zero PnL — which silently overstated profitability.

## Scraper expectations and watcher backoff

`config.yaml::watched_sites` lists ~70 outlets across US/Europe/Asia (incl. Chinese), Middle East, Africa, and Russia state media. Many are paywall- or Cloudflare-protected; the scraper uses a full browser (the `obscura` rust binary) and handles most of them, but some return 401/403. The watcher does exponential backoff per site (30s → 1800s), so a permanently broken URL stops eating budget. Don't prune the list reactively — the per-site `max_failures: 5` already does it for you.

## Cycle-end checklist

Before declaring a cycle done:

- [ ] Postmortem written, compared to previous
- [ ] Lessons aggregator has produced non-seeded rows for at least 3 bucket families
- [ ] Per-engine PnL is consistent with the engine's design (aggressive harvests, balanced harvests less but bigger)
- [ ] `signed_edge` and `signed_edge_if_raw` distributions plotted; alternative-model flip rate computed
- [ ] Any open structural questions written into the next cycle's hypothesis list
