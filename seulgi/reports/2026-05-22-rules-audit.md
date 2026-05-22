# Rules audit — 2026-05-22 (Phase 0 A0.10)

Audit of every file in `seulgi/rules/*.md` against `seulgi/app/rules.py`
and the cycle-4 engine roster in `seulgi/config.yaml`.

**Active engines in `config.yaml` (cycle-4):** `balanced`, `scout`,
`super_scout`. **Retired** (commented out or removed): `aggressive`,
`inverted_highconf`, `loose_relevance_yes`.

**Fire-rate window:** last 24h of `engine_results` (now =
2026-05-22T20:14Z, range = 2026-05-21T20:14Z → 2026-05-22T17:58Z;
80 739 rows total). Note: the bot is paused on the $0 OpenRouter cap
(handoff §1), so the window also covers a stale tail-end before pause.

## Per-rule audit

| # | File | Stage | Predicate (one line) | Evidence date | Age (d) | engines_exclude | 24h fires | Verdict | Recommendation |
|---|---|---|---|---|---|---|---:|---|---|
| 1 | `skip_breaking_no.md` | post_ai | direction=NO AND freshness=BREAKING | 2026-05-19 (in body) | 3 | `scout, super_scout` | 0 (last fire 2026-05-20) | KEEP | enable as-is; refresh once Stage-3 resumes |
| 2 | `skip_extreme_high_price.md` | pre_ai | mid_price ≥ 0.90 | 2026-05-14 (in body) | 8 | `scout, super_scout` | 798 | KEEP | structural Polymarket near-rail; safe |
| 3 | `skip_extreme_low_price.md` | pre_ai | mid_price ≤ 0.10 | 2026-05-14 (in body) | 8 | `scout, super_scout` | 9 564 | KEEP | structural; the dominant fire-source |
| 4 | `skip_high_confidence_anti_informative.md` | post_ai | AI confidence ≥ 0.75 | 2026-05-14 (in body) | 8 | `inverted_highconf, super_scout` | 1 | STALE + dead-engine ref | **refactor**: drop `inverted_highconf` from `engines_exclude` (engine retired cycle-4); rerun the bucket_stats backtest before deciding KEEP vs RETIRED |
| 5 | `skip_iran_topic.md` | pre_ai | question matches `\biran\b` | 2026-05-19 (in body) | 3 | `scout, super_scout` | 570 | OVERFIT | hold for now (recent evidence), but flag for Phase 4E retirement once stats-scoring replaces topic gates |
| 6 | `skip_mid_above_085.md` | pre_ai | mid_price ≥ 0.85 | 2026-05-19 (in body) | 3 | `scout, super_scout` | 248 | KEEP | recent evidence, fires across all 18 prod engines |
| 7 | `skip_mid_below_065.md` | pre_ai | mid_price ≤ 0.65 | 2026-05-19 (in body) | 3 | `scout, super_scout` | 6 624 | KEEP | recent evidence, second-largest fire-source |
| 8 | `skip_sports.md` | pre_ai | market_type matches `^sports$` | 2026-05-14 (thesis, no quant) | 8 | `super_scout` | 0 (zero ever) | STALE | enable verification: confirm zero `market_type='sports'` ever reached the rule, or refactor to a question_rx instead of relying on `markets.market_type` |
| 9 | `skip_us_election_topic.md` | pre_ai | question matches election/party/seat regex | 2026-05-19 (in body) | 3 | `scout, super_scout` | 36 | OVERFIT | thin evidence (n=10, 1 hit); demote to Phase 4E candidate; do not retire while sample so small |
| 10 | `skip_weather.md` | pre_ai | market_type matches `^weather$` | 2026-05-14 (thesis, no quant) | 8 | `super_scout` | 0 (zero ever) | STALE | same as `skip_sports.md` — verify `market_type='weather'` is actually populated; could be silent no-op |

## Cross-checks called out by the spec

### 3.1 Dead engine references in `engines_exclude`

| Rule | Excluded engines | Live? |
|---|---|---|
| `skip_high_confidence_anti_informative` | `inverted_highconf, super_scout` | **`inverted_highconf` RETIRED in cycle-4** (config.yaml lines 594–598); `super_scout` live |
| every other rule | `scout, super_scout` or just `super_scout` | both live |

Only one rule contains a stale reference. `inverted_highconf` was removed
from `config.yaml` cycle-4 (2026-05-22 a.m.); the exclusion is now inert
— no live engine matches the string, so the rule fires for every engine
that AI-classifies, identical to a rule with only `super_scout` excluded.
Recommendation: edit the frontmatter to `engines_exclude: super_scout`
during Phase 1 rule cleanup so the file stops advertising a dead
contract. **Do not delete the line entirely** — the rule's post-AI
predicate (confidence ≥ 0.75) should still skip super_scout's research
trades.

### 3.2 Was `skip_high_confidence_anti_informative.md` deleted in cycle-3?

No. The git status snapshot at the top of the conversation showed `D`
for this file, but the working tree (and `git status --short` re-run
now) shows it tracked and unmodified. `git log --diff-filter=A` confirms
it was added 2026-05-14 and last touched 2026-05-22 (the M2
`engines_exclude` add). The "deleted in cycle-3" line in the master
plan / git log narrative does NOT match the current repo state — the
file is alive but its exclusion list refers to a retired engine, which
is what made it look "dead" in the audit. The DB shows
2 121 total fires (1 in the last 24h). Treat as STALE pending
revalidation: evidence is 8 days old and was gathered before the
multi-classifier ensemble (`min_agreement` voting, cycle-6) changed
the confidence distribution.

### 3.3 Rules that only exclude `super_scout`

`skip_sports.md` and `skip_weather.md` — both fire for every production
engine plus `scout` (`scout` is NOT excluded). This is intentional per
the cycle-4 history: those two rules are pure-structural ("we have no
data here") and should apply to scout as well as balanced. Documented
here so future cycles don't "fix" the omission. Both rules currently
show **zero lifetime fires** in `engine_results` — see below.

### 3.4 KEEPs from cycle-9 evidence (3 days old)

`skip_breaking_no.md`, `skip_mid_above_085.md`, `skip_mid_below_065.md`
all carry 2026-05-19 forensic evidence (cycle-9 phase E). All three are
KEEP. `skip_breaking_no` fired 75 times historically but **0 in the
last 24h** — symptom of the budget pause (no fresh BREAKING headlines
classified post-2026-05-20). Re-check fire rate ≥ 24h after the
OpenRouter top-up.

### 3.5 OVERFIT topic rules

`skip_iran_topic.md` (n=22, 0% hit) and `skip_us_election_topic.md`
(n=10, 10% hit) are narrow-topic question-regex gates — match the
OVERFIT definition. KEEP-for-now (Iran: 570 fires/day saves tokens;
US-election: 36/day same family). Flag for Phase 4E.1 retirement once
`scoring.py` absorbs the signal as a `(topic_one_hot × confidence)`
feature.

### 3.6 Structural near-rail rules

`skip_extreme_low_price.md` (mid ≤ 0.10) and
`skip_extreme_high_price.md` (mid ≥ 0.90) are KEEP — friction-aware
filters encoding Polymarket bid-ask reality at the rails. Likely to
survive Phase 4E.1 stats-scoring.

### 3.7 Zero-fire structural rules — possible silent no-op

`skip_sports.md` and `skip_weather.md` show zero fires across the full
DB lifetime. Two hypotheses:

  1. `markets.market_type` is never set to `sports` / `weather` (the
     pre-AI universal gate happens before AI classification fills
     market_type from the tag pipeline). If true, both rules are dead
     letters and should be refactored to `question_rx` regexes (e.g.
     `\b(NFL|NBA|MLB|Premier\s+League|...)\b`).
  2. Stage-2 / Stage-3 already skip these via similarity-floor + the
     post-cycle-2 retiring of the relevance LLM (handoff §2), so the
     rule never sees the trade.

A 30-min sample of `markets.market_type` will disambiguate; that's
Phase 1 work, not Phase 0.

## Top findings (3-line summary)

1. `skip_high_confidence_anti_informative.md` carries a stale
   `engines_exclude: inverted_highconf` reference (cycle-4 retired the
   engine). The exclusion is inert today; refactor to
   `engines_exclude: super_scout` and rerun the high-confidence bucket
   backtest in Phase 1.
2. `skip_sports.md` and `skip_weather.md` show **zero fires lifetime**
   — likely silent no-ops because `markets.market_type` is unpopulated
   at pre-AI gate time. Either refactor to question-regex or accept as
   dead letters in Phase 1.
3. `skip_iran_topic.md` and `skip_us_election_topic.md` match the
   OVERFIT pattern (narrow topic, n<30). Keep through cycle-5 for
   token-savings, then plan retirement once Phase 4E.1 stats-based
   scoring replaces topic-level gates.
