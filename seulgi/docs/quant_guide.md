# Seulgi Quant Guide

## Core Hypothesis

Breaking news moves Polymarket odds before most traders react. We can find the news,
identify affected markets, and buy/sell within seconds. The hypothesis is testable: if
our AI correctly predicts direction, market prices should drift toward our p_hat over
the next 5–60 minutes.

**We are not claiming FPGA speed.** We claim:
1. We find news faster than casual traders (automated watchers, <5s latency typical)
2. We classify it correctly more than 50% of the time (tested via `quant_stats.txt`)
3. Price moves measurably in the predicted direction within 5 minutes (tested via `price_impacts` table)

## Key Files to Show the Quant

| File | What it proves |
|------|---------------|
| `data/dashboard/quant_stats.txt` | Precision and signal latency |
| `data/dashboard/signal_journal.txt` | Every non-NEUTRAL AI call and whether it traded |
| `data/dashboard/engine_comparison.txt` | Three strategy profiles with live PnL comparison |
| `data/dashboard/near_miss.txt` | Signals with real edge that just missed the threshold |
| `data/dashboard/trades_executed.txt` | Executed paper trades with edge and confidence |
| `data/dashboard/activity_log.txt` | Full event stream — news → AI → trade |

## How to Read quant_stats.txt

```
PREDICTION ACCURACY
  DIRECTION      N  PRECISION  AVG Δ5m  AVG|Δ5m|
  YES          142      58.5%  +0.0082    0.0134
  NO            89      54.0%  -0.0051    0.0098
```

- **Precision**: % of YES calls where price moved up in 5m (random baseline = 50%)
- **AVG Δ5m**: signed average price change — positive for YES, negative for NO = good
- **AVG|Δ5m|**: absolute average move — if this is < 0.005, the market didn't really move

**What counts as signal:**
- Precision > 55% with N > 50 = statistically interesting (p < 0.05 at N=50 is ~57%)
- |Δ5m| > 0.01 = 1¢ move on a binary = market is responding to news

**Signal Latency:**
- Target < 5s headline-to-trade-decision
- Even at 30s we beat humans; FPGAs are sub-millisecond
- Latency is embed time + AI call time; AI batching is the bottleneck

## How to Read signal_journal.txt

Every row is a non-NEUTRAL AI decision. Format:
```
[2025-01-15 14:32:11] [aggressive   ] ▲ YES   conf=72%  sim=0.61
  Headline : Fed raises rates by 50bps unexpectedly
  Market   : Will the Fed raise rates above 5% in 2024?
  Outcome  : TRADED $150 @ 0.410  edge=+0.0720
```

- `sim=0.61` — cosine similarity between headline and market question embedding
- `conf=72%` — AI's confidence in the YES direction
- `edge=+0.0720` — how far we think fair value is above current price
- `SKIP low_confidence:0.48` — AI spoke but didn't clear the confidence threshold

If you see many `SKIP neutral` rows = AI thinks news doesn't affect these markets.
If you see many `SKIP edge_too_small` = AI sees signal but it's below threshold (see `near_miss.txt`).
If you see many `SKIP max_unique_markets` = portfolio is full, we're missing opportunities.

## Pre-written SQL Queries

Run from: `sqlite3 /data/seulgi.db`

**Overall precision by direction:**
```sql
SELECT direction,
       COUNT(*) AS n,
       SUM(CASE WHEN (direction='YES' AND delta_t5m > 0)
                     OR (direction='NO' AND delta_t5m < 0) THEN 1.0 ELSE 0.0 END) / COUNT(*) AS precision,
       AVG(delta_t5m) AS avg_delta,
       AVG(ABS(delta_t5m)) AS avg_abs_delta
FROM price_impacts
WHERE delta_t5m IS NOT NULL
GROUP BY direction;
```

**Precision by similarity bucket (does higher sim = better signal?):**
```sql
SELECT ROUND(similarity, 1) AS sim_bucket,
       COUNT(*) AS n,
       AVG(CASE WHEN (direction='YES' AND delta_t5m > 0)
                     OR (direction='NO' AND delta_t5m < 0) THEN 1.0 ELSE 0.0 END) AS precision,
       AVG(ABS(delta_t5m)) AS avg_abs_delta
FROM price_impacts
WHERE delta_t5m IS NOT NULL
GROUP BY sim_bucket
ORDER BY sim_bucket;
```

**Alpha decay — does edge disappear fast?:**
```sql
SELECT direction,
       AVG(delta_t1m)  AS avg_d1m,
       AVG(delta_t5m)  AS avg_d5m,
       AVG(delta_t15m) AS avg_d15m,
       AVG(delta_t30m) AS avg_d30m,
       AVG(delta_t60m) AS avg_d60m
FROM price_impacts
WHERE direction IN ('YES','NO')
  AND delta_t60m IS NOT NULL
GROUP BY direction;
```

**Engine skip reason breakdown:**
```sql
SELECT engine_name, skip_reason, COUNT(*) AS n
FROM engine_results
WHERE skip_reason IS NOT NULL
GROUP BY engine_name, skip_reason
ORDER BY engine_name, n DESC;
```

**Signal latency percentiles:**
```sql
SELECT engine_name,
       COUNT(*) AS n,
       AVG(signal_latency_ms) AS avg_ms,
       MIN(signal_latency_ms) AS min_ms,
       MAX(signal_latency_ms) AS max_ms
FROM engine_results
WHERE signal_latency_ms IS NOT NULL
GROUP BY engine_name;
```

## 6 Questions for the Quant

1. **Is precision > 50% statistically significant at our sample sizes?**
   What N do we need for p < 0.05? (Answer: ~196 for 55% precision, one-tailed binomial)

2. **Is the edge model calibrated?**
   Does conf=0.7 actually produce 70% directional accuracy? Plot precision vs. confidence bucket.
   If miscalibrated, consider Platt scaling on the classifier output.

3. **What is the Information Coefficient between similarity score and |Δ5m|?**
   IC measures how well our ranking (higher sim → bigger expected move) holds empirically.
   IC > 0.05 is typically considered useful in quant finance.

4. **What Kelly fraction does our observed hit rate imply?**
   Kelly = (p × b − q) / b where p=precision, q=1-p, b=expected payoff ratio.
   At 58% precision and ~1.5:1 payoff (binary odds), Kelly ≈ 0.22 = 22% of bankroll.
   Does our current max_position sizing align with this?

5. **Is there selection bias?**
   We only record price impacts for non-NEUTRAL signals. The true distribution includes
   all headlines — many of which may be true negatives. How do we estimate the false-negative rate?

6. **What is the alpha decay curve?**
   Compare Δ1m vs Δ5m vs Δ15m. If alpha disappears after 5m, we need sub-5m execution.
   If alpha persists to 60m, we have a wide entry window — latency matters less.
