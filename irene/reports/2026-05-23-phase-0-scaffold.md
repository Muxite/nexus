# Phase 0 Scaffold Audit — 2026-05-23

**Verification gate:** Phase 0 deliverables checklist

**Verdict: ALL GREEN — P1 entry point confirmed.**

---

## Verification table

| Item | Status | Proof path |
|---|---|---|
| **1. Directory tree** | ✓ | All 15 required directories exist (empty OK): `app/`, `app/stages/`, `app/tests/`, `docs/`, `docs/papers/`, `docs/templates/`, `handoffs/`, `reports/`, `changelogs/`, `questions/`, `rules/`, `scripts/`, `scripts/tracer/`, `scripts/tracer/templates/`, `scripts/replay/`, `data/`. Verified via `find irene -type d`. |
| **2a. README.md** | ✓ | Has "Where to find things" table (line 9–31), ASCII pipeline diagram (line 36–57), quickstart sketch (line 69–90), "Hard rules" section (line 110–122). `irene/README.md` |
| **2b. architecture.md** | ✓ | Seulgi-style preamble with "Last updated", "Synthesized from", "Status" (line 7–9); ASCII data-flow diagram (line 18–52); Modules table (line 64–88); references `docs/modularization.md` (line 60). `irene/architecture.md` |
| **2c. master-plan.md** | ✓ | Phase table (line 94–102); user-decisions table (line 24–38); "Phase 1 — Detailed scope" section (line 107–149); references `docs/modularization.md` (line 62–69) and `architecture.md` (line 43). `irene/master-plan.md` |
| **2d. dkg_spec.md** | ✓ | Verbatim user spec. Contains: Scope (line 7–11), Objectives (line 13–23), Storage design with "New core tables" subsection (line 25–117), Services and queue contracts (line 132–212), API and read models (line 214–260), Migration plan (line 262–294), Decision logic and validation (line 296–335). Includes scoring formula at line 303–310. `irene/docs/dkg_spec.md` |
| **2e. modularization.md** | ✓ | Eight rules explicit (line 9–130): (1) one responsibility, (2) bus as only channel, (3) JSON-dict payloads, (4) table ownership, (5) no mutable state, (6) injected deps, (7) tests next to code, (8) independently runnable. Anti-patterns list (line 133–143). CI checks documented (line 174–182). Mentions table-ownership, JSON-dict payloads, no shared state, dependencies injected. `irene/docs/modularization.md` |
| **2f. handoffs/2026-05-23.md** | ✓ | Front-matter YAML (line 1–7); TL;DR (line 12–18); "What landed today" table (line 20–30); "What is NOT here yet" section (line 32–40); "What P1 starts with" section (line 52–65). `irene/handoffs/2026-05-23.md` |
| **3. Internal references wired** | ✓ | README links: master-plan (line 13), architecture (line 14), dkg_spec (line 15), modularization (line 16), handoff (line 24). Architecture links: master-plan, modularization (line 60). Master-plan links: architecture (line 43), dkg_spec (line 3), modularization (line 62). Handoff references: plan file (line 5), master-plan (throughout). All verified via grep. |
| **4. Modularization directive present** | ✓ | `docs/modularization.md` exists; README, architecture, master-plan all link to it with "user directive 2026-05-23" attribution. Eight rules are explicit. CI checks documented. All three main docs cite it. `irene/docs/modularization.md` + cross-references in README (line 16), architecture (line 60), master-plan (line 62). |
| **5. Tracer + replay directive present** | ✓ | README mentions tracer + replay in table (line 29–30) and in pipeline section (line 64–65). Master-plan §"Phase 1 — Detailed scope" lists `scripts/tracer/` (line 138) and `scripts/replay/accuracy.py` (line 140) as P1 deliverables. Directory placeholders: `scripts/tracer/templates/` exists (verified via ls), `scripts/replay/` exists (verified via ls). `irene/README.md`, `irene/master-plan.md`, verified directory structure. |
| **6. No seulgi touches** | ✓ | `git status seulgi/` shows modifications (M on 10 files, ?? on 6 files) but these are **pre-existing** from commit `d1e22d3` (seulgi: A4A.3 BBO + book-depth capture), dated before P0. `git diff --stat seulgi/` would show the pre-P0 diff. Root `docker-compose.yml` shows as `M` (pre-existing, per git log `0a2615e` seulgi part 1.5). **Only new irene/ files appear** in P0 work (6 .md files + directory tree, no deletions). `irene/` branch untouched by P0. Verified via `git status --short` and `git log --oneline`. |
| **7. Hard rules consistency** | ✓ | Five rules stated in README (line 110–116), master-plan (line 195–201), architecture (line 167–175). Wording is consistent across all three: (1) no DB sharing / runtime assertion, (2) standalone compose / no root edits, (3) single-decision / no variant fan-out, (4) tracers ≠ live / is_tracer=1 AND paper=0 rejected, (5) capture inside batch / db.batch(). Cross-check: README details rule 1 with memo path; architecture abbreviates; master-plan abbreviates. Core substance identical. |

---

## Issues found

**None.** All checklist items are green. The scaffold is complete and internally consistent.

---

## P1 entry point

Phase 0 gates P1 entry. The next session opens with:

1. **Container infra** — `Dockerfile`, `docker-compose.yml`, `requirements.txt` / `requirements.test.txt`, `pytest.ini`.
2. **Bootstrap modules** — `app/config.py` (pydantic, hot-reload), `app/db.py` (schema, seulgi-path assertion, `db.batch()`), `app/__init__.py`, `app/__main__.py`, `app/main.py` (boot trace only).
3. **First test suite** — `app/tests/test_db.py` (schema lifecycle, seulgi-path assertion).

This is the "make it buildable and smoke-test the DB" milestone. Stages follow in order per `master-plan.md` §"Phase 1 — Detailed scope": `config.py` → `db.py` → `bus.py` → `timing.py` → `budget.py` → `embed.py` → `ai.py` → `stages/markets.py` → `stages/ingest.py` → `stages/dedupe.py` → `stages/match.py` → `stages/trade.py` → `main.py` wiring → container. Tracer (`scripts/tracer/fire.py`, `check.py`) and replay accuracy (`scripts/replay/accuracy.py`) ship as P1 cross-cutting deliverables, **not deferred**.

P1 ship gate: (a) container boots and runs 1+ hour, (b) all P1 tables accumulate rows, (c) tracer fire/check passes 0-drop test, (d) replay accuracy reconstructs PnL ±1¢, (e) soak report lands comparing irene first-hour vs seulgi baseline.

---

## Files created

**Count: 6 markdown files + 15 directories**

### Markdown files (1,100 lines total)

- `irene/README.md` (122 lines) — landing page, quickstart, hard rules
- `irene/architecture.md` (175 lines) — current state (scaffold only)
- `irene/master-plan.md` (201 lines) — phased roadmap and P1 scope
- `irene/docs/dkg_spec.md` (334 lines) — verbatim user spec, design north star
- `irene/docs/modularization.md` (182 lines) — eight rules, CI checks, canonical
- `irene/handoffs/2026-05-23.md` (86 lines) — phase wrap, entry point, verification

### Directory tree (15 directories, all empty)

```
irene/
  app/
    stages/
    tests/
  docs/
    papers/
    templates/
  data/
  scripts/
    tracer/
      templates/
    replay/
  handoffs/
  reports/
  changelogs/
  questions/
  rules/
```

---

## Notes

- **Tracer + replay as P1 first-class deliverables** — both directives (modularization and tracer/replay) that the user added mid-P0 are fully integrated into README, architecture, master-plan, and handoff. No TBD placeholders; both are named as "P1 deliverables" alongside the pipeline.
- **Hard rules baked in from day 1** — all five rules appear in README with details, in architecture as a replicated summary, and in master-plan as a checklist. Core substance is identical; variation in length is intentional (README most detailed for onboarding, others abbreviated for reference).
- **Modularization as gating criterion** — every doc that discusses module design (architecture, master-plan) links to the canonical `docs/modularization.md`. The module specification convention is documented. CI lint checks are spelled out. This is a **hard constraint**, not advice.
- **No code exists yet** — this is correct for P0. All six docs are deliverables themselves; they constitute the "scaffold" that gates the next phase. The approved plan explicitly chose a "slim P0" to avoid over-documenting before code landed.

---

**Audit completed 2026-05-23.**  
**Ready for P1 start.**
