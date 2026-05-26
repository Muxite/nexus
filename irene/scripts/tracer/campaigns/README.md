# Tracer campaigns

A "campaign" is one CLI invocation that fires several tracer templates back-to-back in a single process. The advantages over running `fire.py` once per template:

- **One bring-up cost** — the embedder, AIClient, CLOBClient and Budget snapshot are loaded once, not once per template.
- **One shared budget envelope** — `--budget-cap-usd` floors the OpenRouter remaining across the *whole* campaign, re-checked between bursts.
- **One aggregated audit** — `audit_burst.py --group-by template --tracer-id-prefix <root>` reports per-template subsections + a grand-total funnel in a single invocation.
- **One cleanup call** — `cleanup.py --tracer-id-prefix <root>` matches all sub-bursts via LIKE.
- **Predictable tracer_ids** — every row carries `tracer_id = <root>-<template>-<i>`, with the template name as the parseable middle segment.

## Invocation

Two equivalent ways to define a campaign.

### Inline (`--template name:count`)

Repeat the flag once per template. Best for ad-hoc verification:

```bash
python -m scripts.tracer.fire \
  --template iran_strike:50 \
  --template market_crash:50 \
  --tracer-id-prefix cycle24-verify-$(date -u +%Y%m%dT%H%M) \
  --live-externals --budget-cap-usd 55 \
  --config /config/config.yaml --db /data/irene.db
```

### YAML (`--campaign`)

For canonical, version-controlled recipes. Drop a `.yaml` file in this directory and reference it:

```bash
python -m scripts.tracer.fire --campaign scripts/tracer/campaigns/cycle-24-verify.yaml --live-externals
```

CLI flags override values in the YAML's `shared:` section. The `prefix_root` in the YAML can include `{ts}` which resolves at fire time to UTC `YYYYMMDDTHHmm`.

## Schema

```yaml
name: <human-readable campaign name (logs only)>
prefix_root: <root tracer_id prefix; supports {ts} placeholder>
shared:                              # defaults; CLI flags win
  concurrency: <int>
  log_level: INFO|DEBUG|WARNING
  config: </path/to/config.yaml>
  db: </path/to/irene.db>
  budget_cap_usd: <float>            # OpenRouter-remaining floor
  exercise_wallet: <bool>
bursts:                              # at least one
  - template: <template stem under templates/>
    count: <int>
```

## Post-run

```bash
# Per-template funnel + grand total.
python -m scripts.tracer.audit_burst \
  --tracer-id-prefix <root> \
  --group-by template \
  --db /data/irene.db

# Wipe the whole campaign in one shot.
python -m scripts.tracer.cleanup \
  --tracer-id-prefix <root> \
  --db /data/irene.db --dry-run   # verify first
python -m scripts.tracer.cleanup \
  --tracer-id-prefix <root> \
  --db /data/irene.db
```

## Adding a new campaign

1. Copy `cycle-24-verify.yaml` to a new file in this directory.
2. Pick a `prefix_root` that's recognisable in audit reports (the cycle name + `{ts}` is the convention).
3. List your bursts. Sequential ordering matches sequential firing — the first listed template runs first.
4. Commit the YAML so the recipe survives across sessions.
