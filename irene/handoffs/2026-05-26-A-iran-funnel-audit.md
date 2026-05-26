# Handoff A — Iran-strikes funnel audit (the empirical reference)

**Date:** 2026-05-26
**Type:** Reference document, write-once. Not an implementation task.
**Cited by:** handoffs B, C, D, E, F, G.

## Why this exists

In a 24h window where the U.S. launched self-defense strikes on Iran and a chemical tank rupture threatened Orange County, California — i.e. prime conditions for a prediction-market bot to express views — irene fired exactly **one** trade, and that trade was on a market that had been closed for 3 days. Every other signal died before reaching the LLM.

The expectation: large geopolitical news → many article-market pairings → many LLM calls → some decisive YES/NO → some trades. The reality: 1,656 Iran-themed signals → 1 LLM call → 0 trades on Iran/Israel/missile markets.

This document captures the SQL evidence behind that observation so future sessions don't have to re-derive it. Every other 2026-05-26 handoff references this one.

## Reproducible queries

Run from anywhere on the host (the DB is bind-mounted at `/home/muk/projects/jennie/irene/data/irene.db`).

### Q1 — Iran-themed article count

```python
import sqlite3
db = sqlite3.connect('/home/muk/projects/jennie/irene/data/irene.db')
db.execute("""SELECT COUNT(*) FROM articles
  WHERE ingested_at >= datetime('now','-24 hours')
  AND (title LIKE '%Iran%' OR title LIKE '%missile%' OR title LIKE '%Tehran%'
       OR title LIKE '%Hezbollah%' OR title LIKE '%ceasefire%' OR title LIKE '%escalat%')""").fetchone()
# Observed 2026-05-26 ~10:30 UTC: 556 articles
```

### Q2 — signals + decisions + LLM-reached for those articles

```python
art_ids = [r[0] for r in db.execute("""SELECT id FROM articles WHERE ...same WHERE as Q1...""")]
ph = ','.join('?' for _ in art_ids)

# signals
db.execute(f"SELECT COUNT(*) FROM signals WHERE article_id IN ({ph})", art_ids).fetchone()
# Observed: 1,656 signals

# decisions by reason
db.execute(f"""SELECT d.outcome, d.reason, COUNT(*) FROM decisions d
   JOIN signals s ON s.id=d.signal_id WHERE s.article_id IN ({ph})
   GROUP BY d.outcome, d.reason ORDER BY 3 DESC""", art_ids).fetchall()
# Observed:
#   reject  freshness_stale     1654
#   reject  direction_neutral    1
#   reject  execution_disabled   1

# LLM predictions on those signals
db.execute(f"""SELECT p.label, p.direction, p.confidence FROM predictions p
   JOIN decisions d ON d.prediction_id=p.id JOIN signals s ON s.id=d.signal_id
   WHERE s.article_id IN ({ph})""", art_ids).fetchall()
# Observed: 1 row — label='U' direction='NEUTRAL' confidence=~0.5
```

### Q3 — sample Iran-article × Iran-market pairs

```python
db.execute(f"""SELECT a.title, m.question, s.similarity, s.freshness
   FROM signals s JOIN articles a ON a.id=s.article_id JOIN markets m ON m.condition_id=s.market_id
   WHERE a.id IN ({ph})
   AND (m.question LIKE '%Iran%' OR m.question LIKE '%Israel%' OR m.question LIKE '%missile%'
        OR m.question LIKE '%Hezbollah%' OR m.question LIKE '%ceasefire%')
   ORDER BY s.similarity DESC LIMIT 15""", art_ids).fetchall()
```

Observed pairs (representative):

| sim | freshness | article | market |
|-----|---|---|---|
| 0.74 | STALE_RETURN | "U.S.-Iran Peace Deal Could Take Days to Nail Down" | US-Iran nuclear deal by June 30? |
| 0.73 | STALE_RETURN | "Iran and Trump talk down hopes of imminent deal" | US-Iran nuclear deal before 2027? |
| 0.73 | REHASH | "What we know about potential US-Iran deal" | US-Iran nuclear deal before 2027? |
| 0.72 | STALE_RETURN | "Trump outlines options for disposal of Iran's enriched [uranium]" | Iran agrees to surrender enriched uranium stockpile by …? |
| 0.69 | STALE_RETURN | "Middle East live: US carries out 'self-defense' strikes" | Will the Iranian regime survive U.S. military strikes? |

These are **textbook trade candidates** — the matcher routed them correctly to semantically aligned markets. They all died upstream of the LLM.

### Q4 — freshness label distribution (all signals, 24h)

```python
db.execute("""SELECT freshness, COUNT(*) FROM signals
   WHERE created_at >= datetime('now','-24 hours')
   GROUP BY freshness ORDER BY 2 DESC""").fetchall()
```

| label | count | trade-stage accepts? |
|---|---:|---|
| STALE_RETURN | 3,162 | ❌ |
| REHASH | 1,532 | ❌ |
| EVOLVING | 385 | ✅ |
| BREAKING | 310 | ✅ |

Only **12.9%** of all signals (695/5,389) carry a freshness label the trade cascade accepts.

### Q5 — the specific "US strikes" article

```python
db.execute("""SELECT a.id, a.title FROM articles a
   WHERE a.title LIKE '%U.S. Conducts%' OR a.title LIKE '%self-defense strikes%'""").fetchall()
# Observed article 4854964: "US says it launched 'self-defense strikes' in Iran..."

db.execute("""SELECT s.id, m.question, s.similarity, s.freshness,
       d.outcome, d.reason, p.direction, p.confidence
   FROM signals s JOIN markets m ON m.condition_id=s.market_id
   LEFT JOIN decisions d ON d.signal_id=s.id LEFT JOIN predictions p ON p.id=d.prediction_id
   WHERE s.article_id=4854964""").fetchall()
```

Result: ONE signal, sim=0.59 → "Will the Iranian regime survive U.S. military strikes?", freshness=STALE_RETURN, decision=reject(freshness_stale), prediction=None. The LLM never saw this article.

### Q6 — active Iran/Israel/missile market universe

```python
db.execute("""SELECT COUNT(*) FROM markets
   WHERE active=1 AND (question LIKE '%Iran%' OR question LIKE '%Israel%'
        OR question LIKE '%missile%' OR question LIKE '%Hezbollah%' OR question LIKE '%ceasefire%')""").fetchone()
# Observed: 25+ active markets
```

Including questions like:
- *Will the U.S. invade Iran before 2027?*
- *US-Iran nuclear deal before 2027?*
- *Iran nuclear test before 2027?*
- *Will the Iranian regime fall before 2027?*
- *Israel and Lebanon normalize relations before 2027?*
- *Iran agrees to surrender enriched uranium stockpile by …?*
- *Will Hezbollah disarm by December 31?*
- *US x Iran permanent peace deal by May 31/June 30, 2026?*

### Q7 — chemical-tank coverage (parallel event, lower volume)

```python
db.execute("""SELECT title, ingested_at FROM articles
   WHERE ingested_at >= datetime('now','-24 hours')
   AND (title LIKE '%chemical tank%' OR title LIKE '%toxic%' OR title LIKE '%Orange County%')
   ORDER BY ingested_at DESC""").fetchall()
```

Found: 3 articles ("Threat of massive chemical tank explosion is 'eliminated', California officials say", "Threat of Toxic Explosion in Orange County, Calif., Eliminated", "Orange County residents react to evacuation"). No active prediction market on "California chemical tank explosion in 2026", so 0 expected signals — confirmed.

### Q8 — Iran-themed articles by hour

The data is in `Q8_iran_by_hour` (see Q3 in the parent plan). Pattern: steady ~10-20 articles/hour, with two notable spikes (161 articles in hour 04 = ingestion burst from scraper restart, then ~15-20/hr afterward). News volume is real, not a momentary blip.

## Conclusion

The funnel is starved upstream of the LLM. The matcher works (good sim scores on real semantic matches). The market universe is rich. The bot's expressed views during this 24h window were `1 NEUTRAL`. The single executed trade (id=40) was on a dead market and went through the DKG path which lacks the cycle-23 safety gates.

**Three observable failure modes:**
1. **Dedupe starvation** (handoff B) — 87% of signals labeled stale/rehash, including fresh strikes.
2. **DKG safety gap** (handoff C) — the one trade that fired was blind.
3. **Market state hygiene** (handoff D + E) — 1,022 flickered markets, wallet shows misleading numbers.

These three handoffs are the next-cycle work. Handoffs F (near-miss audit) and G (tracer stress harness) close the loop on verification.

## Re-running this audit after fixes

Re-execute Q1 → Q5 on the same DB; compare counts. Success criterion: after handoff B, expect Q2's `reject/freshness_stale` count to drop from 1,654/1,656 to **somewhere below 1,200/1,656** (i.e. at least 25% of Iran signals reach the LLM gate). Q5's specific US-strikes article should land BREAKING or EVOLVING, generate a YES/NO prediction, and produce a decision other than `freshness_stale`.
