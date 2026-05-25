# Plan — legal / ops / hosting (I1–I5)

> Source: `irene/questions/unstructured answers.md` lines 150-172, batch
> `2026-05-25-unstructured-1`. Cross-refs: `research_questions.md`
> O-I1 / O-I2 / O-I3 / O-I4 / O-I5 (section I is currently open in
> `research_answers.md`).

## Context

This plan is entirely about **real-money cutover preconditions**
(cycle 10+, per `architecture.md:230`). Code changes are minimal;
the bulk is decisions, documentation, and a few infra tasks that
must land before any USDC touches a Polygon wallet under irene's
control.

Three of the five sub-questions (I1 legal, I4 tax, I3 alt-venues) are
not engineering work — they are user-facing decisions that need
**professional confirmation before go-live**. The remaining two (I2
USDC bridging, I5 hosting) become small infra tasks *after* the
legal/tax side is confirmed.

This plan is **gated** on three pieces of user input (jurisdiction
confirm, preferred CEX, go-live date) and on resolution of the
**dual-container hazard** from auto-memory (`jennie-seulgi` root
compose vs `seulgi` standalone, both writing the same DB —
`architecture.md:318-319` bakes a "standalone-only" rule for irene,
but VPS migration is exactly when that hazard can re-emerge).

## Findings

### Protocol facts (✅ act on these, with the standard legal caveat)

- **Ontario blocked; BC not on Polymarket's published restricted list**
  (line 152). "Not listed" ≠ "allowed" — OSC settlement is Ontario-
  specific, but BCSC and the CSA have separate authority. Needs user
  confirmation before real money.
- **Non-custodial wallet, no mandatory KYC** (line 152). Wallet
  funded directly from user's wallet; no account-level identity
  binding.
- **VPN traffic violates ToS; risk of access freeze** (line 152).
  Hard constraint: host in a Polymarket-permitted region directly;
  never tunnel through a VPN to spoof.
- **Polygon-native USDC withdrawals bypass Ethereum bridging cost**
  (line 154). Funding path: CEX → Polygon-native USDC withdrawal →
  Polymarket wallet. Never through the official Polygon Bridge.
- **No native Polygon deposit/withdrawal fee from Polymarket** (line
  154). Only cost is the CEX flat withdrawal fee.
- **Kalshi is US-only** (line 165) — not viable from Canada; the
  "regulated alternative" backup path is unavailable to this user.
- **CRA classifies systematic bot trading as business income**
  (line 167) — fully taxable on a per-trade disposition basis.
  ⚠️ **Research note only; confirm with a Canadian accountant before
  filing.**
- **VPS supports persistent WebSockets; edge workers (Vercel /
  Cloudflare Workers) do not** (lines 170-171). Rules out
  serverless-edge as a hosting target. Cross-cuts with
  `latency-repricing.md`.

### Seeds only (📊 confirm before depending on the number)

- **VPS $5–10/mo** (line 170) — Hetzner CX11 (~€4.5/mo) or DO $6
  droplet. Useful for budgeting; the actual choice depends on region
  (see cross-cut with `latency-repricing.md` — CLOB host is AWS
  `eu-west-2 London`, so any host in London / nearby is preferred).
- **Polygon confirmation latency "a few seconds"** (line 154). Use
  as a prior; measure on the first test deposit.
- **Alt-venue fee/liquidity numbers** (line 163 table) — Manifold
  play-money, Smarkets 2% on net winnings, Polymarket 0–1.8%. These
  are headline figures; do not encode in any config without
  re-reading each venue's own docs.
- **Kalshi "0.5%–1%" fee** (line 163) — moot since US-only.

### ⚠️ Confirm-before-live items

- **Jurisdiction.** Per the RAG inference (line 152) and the
  user-memory (the user is on `muk.chunpongtong@gmail.com`; no other
  location signal in the codebase), the implied jurisdiction is
  Canada / BC. **No code or commit message in this repo asserts a
  province.** Must be confirmed by the user before this plan can be
  acted on.
- **Tax treatment.** CRA business-income classification is the RAG's
  claim — accountant must confirm.
- **VPN/ToS interaction.** If the chosen VPS is in a region the user
  cannot also physically visit / browse Polymarket from, that is a
  flag — not because the bot's traffic is illegitimate but because
  the **detection heuristic** Polymarket runs against bot traffic is
  opaque.

## Proposed changes

### Documentation (the bulk of this plan)

Create `irene/docs/real_money_preconditions.md` summarizing the
decisions, with sections: Jurisdiction status (BC not on blocklist;
needs user confirm + BCSC check), KYC / wallet custody, VPN policy
(host in an allowed region; never tunnel), USDC bridging path,
Hosting plan + region rationale (cross-cut with
`latency-repricing.md`), Tax treatment (with accountant caveat),
Alt-venue stance (noted, not pursued), and the BEFORE_REAL_MONEY
checklist below. Each item tagged `confirmed` / `self-asserted` /
`needs professional confirmation`.

### Hosting choice — recommend, do NOT provision in this plan

- **Recommend** Hetzner CX11 or DigitalOcean $6 droplet in London
  (Hetzner has `hel1` / `fsn1` in EU; for London-region matching
  pick a London-region provider — DO LON1, Vultr LHR, OVH UK1, or
  Hetzner if their roadmap adds a UK region by then). Region choice
  matches the CLOB AWS `eu-west-2` host noted in
  `latency-repricing.md` lines 38-40.
- **Do NOT** touch `irene/docker-compose.yml`, `irene/Dockerfile`,
  or any config in this plan. Hosting selection happens; actual
  migration is a separate, gated cycle.
- **Dual-container hazard (HARD GATE).** Per auto-memory and
  `architecture.md:318-319`, the standalone-vs-root-compose split
  is the canonical risk surface for this codebase. Any VPS
  migration plan must, *as its first step*, confirm:
  1. Only one compose file is in use on the target host.
  2. The SQLite DB at `/data` is written by exactly one container.
  3. The `db.py` runtime assertion against seulgi sharing still
     guards correctness.
  Without resolving that, a VPS migration risks reproducing the
  2026-05-21 incident from the auto-memory.

### USDC funding path

- **Ask the user** which CEX they prefer for Polygon-native USDC
  withdrawal (candidates to check: Coinbase, Kraken, Bybit, OKX —
  user verifies; do not assume).
- **Test deposit protocol:** send $25 (matching `min_position_usdc`)
  CEX → Polymarket via Polygon-native USDC. Record end-to-end
  wall-clock latency (withdrawal-sent → Polygon confirm → Polymarket
  credited), exact CEX fee, and the block-explorer transfer record.
  Save under `data/realmoney/test_deposit_<date>.txt` (directory
  created only when the test runs, not in this plan).
- **No bot-side code change.** Funding wallet is user-operated.

### Alternative-venue work

- **Defer.** Polymarket is the primary; Manifold is play-money;
  Smarkets is sports-heavy and UK-bound; Kalshi is US-only.
- Document in `real_money_preconditions.md` as **"noted, not
  pursued."** If Polymarket access ever becomes an issue, the
  decision to revisit is a fresh cycle, not a TODO in this one.

### Legal / tax (BEFORE_REAL_MONEY checklist)

Lives in `real_money_preconditions.md`; each item is a checkbox:

- [ ] Browse `polymarket.com` from the user's home connection (no
  VPN) and confirm the geofence does not block.
- [ ] Confirm with a Canadian accountant: per-trade disposition
  business-income treatment for an automated trading bot.
- [ ] Set up a per-trade-dispositions export from the SQLite DB
  (likely `trade_closures` joined with `trades`) suitable for tax
  filing. *Format and cadence TBD with the accountant.* No code
  change in this plan, just the placeholder.
- [ ] Confirm wallet custody model with user (hardware wallet vs
  hot wallet vs both; if bot signs transactions, key isolation).
- [ ] Confirm CEX choice and complete the $25 test deposit.
- [ ] Confirm hosting region against `latency-repricing.md`
  recommendation.
- [ ] Resolve the dual-container hazard on the chosen host *before*
  any USDC moves.

## Open questions (require user input — this plan does not assume answers)

- **Is the user in BC, elsewhere in Canada, or elsewhere entirely?**
  No codebase signal confirms this. The RAG inference is just an
  inference. Required input before this plan ships.
- **Preferred CEX** for Polygon-native USDC withdrawal?
- **Target real-money go-live date** — informs the urgency of the
  legal/tax confirmations and whether to start the hosting move now
  or after cycle-9 paper-trade results.
- **Real-money bankroll target** — informs whether the $5–10/mo VPS
  tier is appropriate or whether to budget for a larger / lower-
  latency host.

## Verification

- **Legal/tax items.** Get professional confirmation *before* any
  of the corresponding changes ship. No technical verification
  exists; this is a human-loop step.
- **Hosting.** After picking a host, deploy a copy of irene
  (paper-mode, same config) on the new VPS and measure: (a) CLOB
  WebSocket round-trip vs the current host, (b) Gamma poll latency,
  (c) container-boot time. Compare against current numbers in
  `throughput.txt`. Cross-check the latency-repricing plan's
  region recommendation.
- **USDC funding.** $25 test deposit with full timing/fee log. Do
  not proceed to a larger deposit without seeing the test record.
- **Dual-container hazard.** On the target host, run
  `docker ps -a --filter name=irene` and confirm exactly one irene
  container. Confirm the `db.py` runtime assertion still triggers
  on a deliberate violation (smoke test).

## Cross-cutting notes

- **Hosting region recommendation** overlaps directly with
  `plans/latency-repricing.md` lines 38-40 (CLOB AWS `eu-west-2`).
  Anything cheaper that puts the host outside Europe loses the
  hosting-region argument from that plan.
- **Dual-container hazard** from auto-memory MUST be resolved on
  the target host before any VPS migration. This is a *gating*
  cross-cut, not an advisory one.
- **Per-trade fee logging** (cycle 7, `entry_fee_usdc` /
  `exit_fee_usdc` on `trade_closures` — `architecture.md:325`)
  already gives us most of what the tax export will need; the
  schema is in place, only the export script is missing.
- **Real-money path itself** is cycle 10+ per the architecture
  cycle numbering — this plan is *preconditions for*, not the
  cutover itself.

## Status

- **Plan stub** — written 2026-05-25 from `unstructured answers.md`
  lines 150-172.
- **Not auto-executable** — gated on three user decisions
  (jurisdiction confirm, CEX choice, go-live date) and the
  dual-container hazard resolution.
- Next action: route the three open questions to the user; do NOT
  begin documentation, hosting selection, or test deposit until
  they are answered.
