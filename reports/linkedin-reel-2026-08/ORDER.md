# Lean slide set — 2026-08-23 (reordered + polish pass, same day)

Six slides, tightened from `make_reel.py`'s 11-slide set. Every number is
live as of 2026-08-23 (see the Task 1 delta table in the session report).
Cover art and layout are new; slides 2-6 reuse `make_reel.py`'s own
panel-drawing functions unmodified in structure (content simplified in
one case, see below) - this folder holds their freshly regenerated output,
not new drawing code.

This order was revised from the original cover -> numbers -> architecture
-> funnel -> calibration -> ending after a design critique: architecture is
dense with implementation-detail subtitles that won't read at feed size, so
it now closes the deck as a simplified appendix-style slide instead of
sitting third.

1. **`01_cover.png`** — sparse DKG background (Task 3) + the three boldest
   verified, non-trading numbers (markets tracked, graph size, total LLM
   spend) + a placeholder hook line. Opens on scale and the graph visual,
   per the brief ("frontpage should basically be highlights + a graph
   visual of the items connecting"). Revised this pass: added a soft-edged
   dark scrim behind the text column so the stats and hook line stay
   legible over the graph's dense node cluster, and brightened the
   sublabel/kicker text colors for contrast.
2. **`02_numbers.png`** (`slide_numbers`) — the full breadth-of-scale grid
   (LOC, tests, commits, LLM calls, odds samples, markets, WS pool). Placed
   second so the cover's three headline numbers immediately cash out into
   the full picture, rather than making the reader wait for it.
3. **`03_funnel.png`** (`slide_funnel`) — the decision funnel, re-derived
   live from `decisions.reason` (see report). Carries both the scale story
   (552,571 gate decisions) and the rigor story (every rejection audited,
   0 silent skips) in one image. Revised this pass: added an inline
   callout directly under the "gate decisions evaluated" bar explaining
   why that count exceeds the article count above it (one article can
   trigger many gate checks), not just in the small footer line.
4. **`04_calibration.png`** (`slide_calibration`) — "the model's confidence
   was a lie." The one methodology beat that isn't otherwise covered by the
   funnel or the numbers grid: measurement discipline applied to the model
   itself, not just the pipeline around it.
5. **`05_ending.png`** (`slide_ending`) — EDGE = 0, landed explicitly as
   "the most rigorous output was knowing when to stop." Closes on the
   engineering-rigor-not-trading-profit framing the whole deck is building
   toward, in the retrospective's own voice. Revised this pass: the
   post-news-repricing-vs-half-spread bar comparison (the actual evidence
   for the title's claim) is now bigger, boxed, and more central so it
   competes with the oversized EDGE=0 banner instead of reading as a minor
   footnote.
6. **`06_architecture.png`** (`slide_arch`) — the one-bus, gated-pipeline
   diagram, moved to the close as an appendix-style "how it actually
   works" beat now that the numbers/funnel/calibration/ending carry the
   main narrative. Revised this pass: cut from 6 boxes with
   implementation-detail subtitles ("strict JSON", "WAL", raw token
   counts) down to 5 plain top-level stages (news + market ingest ->
   dedupe + match -> gate + decide -> audit store -> dashboard) so it
   still reads at small feed size.

## What got cut from the full 11-slide reel, and why

- **Title card / tmux dashboard** — folded into the cover + numbers grid;
  a dedicated title slide was redundant once the cover carries the numbers.
- **Universe bar chart** (21x) — the headline number survives on the cover;
  the dedicated bar chart was the least information-dense slide for its
  screen time.
- **Real dashboard panels (2x2 live/frozen)** — interesting but secondary;
  cut to hit the "don't have too many slides" instruction. Still available
  as `08_real_plots.png` in the parent `linkedin-reel/` folder if wanted.
- **Market-moves (6 repricing charts)** and **dataset-growth chart** — both
  strong assets, but redundant with the funnel + numbers on the core claim
  ("the system captured a lot, audited every decision"). Cut for length;
  both still exist, freshly regenerated, in `linkedin-reel/11_market_moves.png`
  and `linkedin-reel/12_dataset.png` if the owner wants a longer carousel.

## Judgment calls flagged for review

- Cover hook line is placeholder copy, not final LinkedIn wording (per the
  task brief — final copy is a separate pass).
  - Three cover stats were my picks for "boldest, verified, non-trading":
    markets tracked (21x), graph size, and total LLM spend efficiency. The
    universe-growth plateau, dataset-growth curve, and calibration flip are
    equally defensible alternates if a different angle is preferred.
- Chose the DKG **sparse** variant on the **DARK** theme (not INK) for the
  cover, judging a dark cover reads more "hype tech" for LinkedIn and gives
  large light text more contrast headroom. See `make_dkg_sparse_bg.py`'s
  docstring for the full reasoning.
- The dataset-growth chart (`linkedin-reel/12_dataset.png`) now shows a
  **second, 15-day flat gap** in odds-observation capture (2026-08-07 to
  2026-08-21) that was not investigated as part of this pass — flagged,
  not smoothed over. Worth a look before this asset goes out.

## 2026-08-23 polish pass (this revision)

- Global: `make_reel.py`'s `DIM` secondary-text color raised from `#8b949e`
  to `#a8b3c2` (contrast vs the `#0d1117` background raised from ~6.15:1 to
  ~8.9:1) — applies to every chart's sublabels, axis labels, and footer
  lines across all 6 slides.
- Cover-only: added the text scrim (see slide 1 note above) and brightened
  `make_cover.py`'s own `DIM` (`#9aa4b2` -> `#b7c0cc`) and `CYAN`
  (`#5fd3dc` -> `#8fe4ea`) sublabel colors.
- Fixed three pre-existing em dashes in rendered slide text (numbers-slide
  window title, calibration subtitle, ending-slide finding) to keep the
  deck's zero-em-dash rule intact after the reorder pulled all six slides
  back into scope for review.
- Rejected from the critique this pass: reordering to put anything other
  than the cover first; any em dash anywhere; reframing the $128 LLM spend
  as a negative; a full color-semantics redesign; any "the project failed"
  framing on the ending slide (the deliberate narrative stays "engineering
  rigor and honest research process").
