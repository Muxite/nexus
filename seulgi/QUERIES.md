# Seulgi SQL Cookbook

Common ad-hoc queries against `/data/seulgi.db` for filtering the rich
per-trade reasoning captured under cycle 8+. Run from the host with:

```bash
sqlite3 seulgi/data/seulgi.db < query.sql
# or interactively
sqlite3 -header -column seulgi/data/seulgi.db
```

The columns added in this cycle (`ai_reason`, `ai_raw_response`,
`ai_prompt_meta`, `trader_trace`, `mid_yes`, `mid_no`, `inverted`,
`variant_name`) are NULL on rows written before the migration. Filter
`WHERE variant_name IS NOT NULL` to scope to fresh data.

## Filter wins vs losses

```sql
-- Big wins, with the AI's stated reason
SELECT id, variant_name, direction, signed_edge, confidence_raw,
       realized_pnl, close_reason, ai_reason
FROM trades
WHERE realized_pnl IS NOT NULL
ORDER BY realized_pnl DESC
LIMIT 30;

-- Worst losses
SELECT id, variant_name, direction, signed_edge, confidence_raw,
       realized_pnl, close_reason, ai_reason
FROM trades
WHERE realized_pnl IS NOT NULL
ORDER BY realized_pnl ASC
LIMIT 30;
```

## Per-variant hit rate

```sql
SELECT
  COALESCE(variant_name, engine_name) AS variant,
  COUNT(*)                              AS trades,
  SUM(realized_pnl > 0)                 AS wins,
  SUM(realized_pnl <= 0)                AS losses,
  ROUND(AVG(realized_pnl), 4)           AS avg_pnl,
  ROUND(SUM(realized_pnl), 2)           AS sum_pnl
FROM trades
WHERE realized_pnl IS NOT NULL AND skip_reason IS NULL
GROUP BY variant
ORDER BY sum_pnl DESC;
```

## High-confidence buckets

Find confidence ranges where the model actually adds value (vs. anti-informs).

```sql
SELECT
  CASE
    WHEN confidence_raw < 0.50 THEN '0.30-0.49'
    WHEN confidence_raw < 0.65 THEN '0.50-0.64'
    WHEN confidence_raw < 0.80 THEN '0.65-0.79'
    ELSE '0.80+'
  END AS conf_bucket,
  COUNT(*) AS n,
  AVG(realized_pnl) AS avg_pnl,
  SUM(realized_pnl) AS sum_pnl,
  AVG(realized_pnl > 0) * 100 AS hit_rate_pct
FROM trades
WHERE realized_pnl IS NOT NULL AND skip_reason IS NULL
GROUP BY conf_bucket
ORDER BY conf_bucket;
```

## By source

```sql
SELECT
  s.site_url,
  COUNT(*) AS trades,
  ROUND(SUM(t.realized_pnl), 2) AS sum_pnl,
  ROUND(AVG(t.realized_pnl), 4) AS avg_pnl,
  ROUND(AVG(t.realized_pnl > 0) * 100, 1) AS hit_pct
FROM trades t
JOIN signals s ON s.id = t.signal_id
WHERE t.realized_pnl IS NOT NULL AND t.skip_reason IS NULL
GROUP BY s.site_url
HAVING trades >= 5
ORDER BY sum_pnl DESC;
```

## Variant A/B on the same headline

```sql
-- For every headline that hit ≥2 variants, line up their PnLs.
SELECT
  t.signal_id, s.headline,
  GROUP_CONCAT(
    COALESCE(t.variant_name, t.engine_name) || '=' || ROUND(COALESCE(t.realized_pnl,0), 3),
    ' | '
  ) AS per_variant
FROM trades t
JOIN signals s ON s.id = t.signal_id
WHERE t.size_usdc > 0 AND t.skip_reason IS NULL
GROUP BY t.signal_id
HAVING COUNT(DISTINCT COALESCE(t.variant_name, t.engine_name)) >= 2
ORDER BY SUM(COALESCE(t.realized_pnl, 0)) DESC
LIMIT 40;
```

## Skip-reason waterfall

```sql
SELECT skip_reason, COUNT(*) AS n
FROM trades
WHERE skip_reason IS NOT NULL
GROUP BY skip_reason
ORDER BY n DESC;
```

## Replay the trader's decision trace

The `trader_trace` column is JSON listing every guard outcome that led to
the executed/skipped result. Use `json_each` to flatten it:

```sql
SELECT t.id, j.value
FROM trades t, json_each(t.trader_trace) j
WHERE t.id = 12345;

-- Or: every step that ever fired a "low_agreement" skip
SELECT t.id, t.skip_reason, j.value AS step
FROM trades t, json_each(t.trader_trace) j
WHERE json_extract(j.value, '$.step') = 'agreement'
  AND json_extract(j.value, '$.pass') = 0
LIMIT 50;
```

## Headline keyword search

```sql
SELECT t.id, t.variant_name, t.direction, t.realized_pnl, s.headline
FROM trades t JOIN signals s ON s.id = t.signal_id
WHERE s.headline LIKE '%inflation%'
ORDER BY t.id DESC LIMIT 50;
```

## Price context around a single headline

```sql
SELECT relative_s, price
FROM price_timeseries
WHERE headline_id = 'YOUR_HID_HERE'
ORDER BY relative_s ASC;
```

## Re-run dashboard rollups manually

```sql
.read seulgi/QUERIES.md
-- or hand-tune the get_variant_stats query inside app/db.py
```

## Inverted-trade audit

```sql
-- How often does inversion help vs hurt?
SELECT inverted,
       COUNT(*) AS n,
       ROUND(SUM(realized_pnl), 2) AS sum_pnl,
       ROUND(AVG(realized_pnl > 0) * 100, 1) AS hit_pct
FROM trades
WHERE realized_pnl IS NOT NULL AND skip_reason IS NULL
GROUP BY inverted;
```

## Time-of-day breakdown

```sql
SELECT
  substr(decided_at, 12, 2) AS hour_utc,
  COUNT(*) AS trades,
  ROUND(SUM(realized_pnl), 2) AS sum_pnl
FROM trades
WHERE realized_pnl IS NOT NULL AND skip_reason IS NULL
GROUP BY hour_utc
ORDER BY hour_utc;
```
