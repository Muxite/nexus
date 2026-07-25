# nexus

Prediction-market research: two paper-trading agents for Polymarket, a shared
scraping service, and the full cycle-by-cycle record of how they were built.

Everything here is one project. The two agents attack the same problem —
turning breaking news into a probability estimate faster and better than the
market — from different directions, and they share a scraper, a data layout,
and a methodology.

## Layout

| Path | What it is |
|------|-----------|
| `seulgi/` | News-driven trading agent. Cosine similarity over a news corpus, cross-encoder reranking, learned-MLP matcher, LLM direction classifier. The older and more heavily cycled of the two. |
| `irene/` | Clean-slate rebuild around a **dynamic knowledge graph**. Extracts entities and events from articles, resolves them into a graph, and matches markets against graph structure instead of raw cosine distance. |
| `services/ujin` | Shared scraping service (submodule → [`Muxite/ujin`](https://github.com/Muxite/ujin)). Fetching, extraction, RSS, sitemaps, social sources, per-host policy. |
| `reports/` | Presentation material — written results, retrospectives, and the generators that render them. |
| `data/` | Convention for backtest data shared between agents. See `data/README.md`. |
| `docs/` | Repo-level notes, including the extraction history. |

Each agent keeps its own `architecture.md`, `master-plan.md`, `config.yaml`,
`cycles/`, `handoffs/`, and `reports/`. Those are the primary documentation —
this README is only the map.

## Why two agents

seulgi's matcher is cosine-similarity over embeddings. That works until it
doesn't: two headlines about different entities can sit close together in
vector space, and the model has no way to know that "Iran" in one and "Iran" in
another refer to different events. irene was started as a parallel rebuild to
attack that specific bottleneck with a knowledge graph, rather than patching
seulgi's ranking in place.

Keeping both is deliberate. seulgi is the working baseline the newer approach
has to beat, and it holds the longer run of live cycle results.

## Shared services

Both agents consume the scraper over HTTP — never as an import — which is why
the implementation could be swapped without touching agent code:

| Agent | Endpoints used |
|-------|----------------|
| irene | `POST /scrape` (`mode=links`, `mode=combined`), `POST /feed` |
| seulgi | `POST /feed`, `GET /health` |

`services/ujin` supersedes the two earlier in-tree scrapers (`scraper` on
:9000, `scraper-v2` on :9001). ujin is derived from them and is a strict
superset — same request models, more routes, per-host policy, plugin-loadable
scorers, and a job control plane.

## Quickstart

```bash
git clone --recurse-submodules git@github.com:Muxite/nexus.git
cd nexus

docker compose up -d ujin                              # shared scraper (:8901)
docker compose -f irene/docker-compose.yml  up -d irene
docker compose -f seulgi/docker-compose.yml up -d seulgi
```

Start `ujin` first: it creates the `nexus_default` network that both agents
attach to as external. The directory must be named `nexus` for that network
name to resolve.

Each agent is started from **its own** compose file, never from the root one.
Running an agent from two compose files at once corrupted a chromadb HNSW
index on 2026-05-21; the root compose deliberately defines only shared
services so that cannot happen again.

### Submodule note

ujin pins its own nested submodule (`obscura`) via an SSH URL. The repository
is public, but anonymous `--recurse-submodules` clones need:

```bash
git config --global url."https://github.com/".insteadOf git@github.com:
```

## Tests

```bash
docker compose -f irene/docker-compose.yml  --profile test run --rm irene-test
docker compose -f seulgi/docker-compose.yml --profile test run --rm seulgi-test
```

Current state — **irene 863 passed / 0 failed**, **seulgi 476 passed / 3 failed**
(11 `live` tests deselected; they need running containers).

The three seulgi failures predate the extraction and reproduce identically
against the pre-migration source:

| Test | Cause |
|---|---|
| `seulgi …test_news_memory.py::test_dissimilar_headline_not_republish` | chromadb behaviour under the installed 0.6.x. |
| `seulgi …test_news_memory.py::test_size_increments_on_new_headline` | Same. |
| `seulgi tests/test_debug_cli.py::test_inspect_engine_runs` | Assertion failure in the debug CLI inspector. |

### Known flake

`irene scripts/tests/test_tracer.py::test_stress_200_count_finishes_fast` runs
200 articles against a hard `timeout_seconds=30.0`. It fails on a loaded
machine and passes on an idle one — wall-clock, not correctness.

### Latent time bomb

The shared market fixture hard-codes `end_date='2026-12-31'` across 13 irene
test files. On **2026-12-30** that falls inside the 6-hour
`min_hours_to_expiry` buffer and `market_near_expiry` will start firing in all
of them. Its meaning has already drifted once — written in May 2026 it was
~222 days out, past the 180-day `far_future` threshold, and it silently
stopped being "far future" on 2026-07-04. The fix is the same one applied to
`test_market_news_imbalance_extreme_mid_evolving_rejects`: compute the date
relative to `datetime.now(timezone.utc)` instead of pinning a literal.

## History

203 commits, all of them mine, spanning both agents from first scaffold to
cycle 30. The tree was extracted from a larger multi-project repository; see
`docs/rename-from-jennie.md` for what changed and what the old names were.

Runtime data — SQLite databases, chroma vector stores, dashboard renders — is
not tracked. It is derived, and it is large (`irene.db` alone is ~6.7 GB).
`data/README.md` describes the layout and how to rebuild it.
