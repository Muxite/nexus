"""
Continuous reinforcement: the lessons system.

Goal
────
Instead of relying on occasional postmortems to surface "this combination of
(source × market_type × confidence) loses money", run an aggregator
continuously inside seulgi. Every ``aggregate_interval_secs`` it scans the
``price_impacts`` table for newly-classified rows, recomputes per-rule
statistics (IC, capture rate, sample size), and upserts a row into the
``lessons`` table.

The trader reads the active lessons (small list, < 30 rows) and injects them
into every AI batch prompt as a compact bullet list — so the classifier sees
"high-confidence calls on aljazeera.com about geopolitical markets have been
0-for-7 in the last 6h; be more cautious" *every time* it labels a headline.

Rule key
────────
``rule_key`` is the canonical identifier for an activation context. We keep
it simple and human-readable:

    src=apnews.com|mt=political|cb=0.60-0.75

Bucketing keeps `lessons` small (a few hundred rows max) and gives enough
samples to estimate IC even with sparse data.

Bootstrapping
─────────────
On startup, if the `lessons` table is empty we seed it with a handful of
hard-coded findings from the cycle-3 postmortem so the AI gets a useful
prompt header from headline #1. The seeded rows are flagged ``seeded=1`` and
get superseded the moment the aggregator has its own empirical evidence.

Activation rules
────────────────
A lesson is `active=1` when it meets *either*:
  * support_n ≥ 10 AND |IC| ≥ 0.05    (statistical signal)
  * seeded=1 AND support_n < 10        (seeded fallback while we collect data)

Once we have empirical data, the seeded version is deactivated.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


# ── Bucket helpers ──────────────────────────────────────────────────────────

_CONF_BUCKETS: list[tuple[float, float, str]] = [
    (0.00, 0.50, "0.00-0.50"),
    (0.50, 0.60, "0.50-0.60"),
    (0.60, 0.75, "0.60-0.75"),
    (0.75, 0.85, "0.75-0.85"),
    (0.85, 1.01, "0.85-1.00"),
]

# Cycle-7: coarse p_alpha and risk buckets for the lessons aggregator.
# Kept coarse so cells fill up fast and we can fit a curve in a 24h run.
_PA_BUCKETS: list[tuple[float, float, str]] = [
    (0.00, 0.20, "0.0-0.2"),
    (0.20, 0.40, "0.2-0.4"),
    (0.40, 0.60, "0.4-0.6"),
    (0.60, 0.80, "0.6-0.8"),
    (0.80, 1.01, "0.8-1.0"),
]
_RISK_BUCKETS: list[tuple[float, float, str]] = [
    (0.00, 0.34, "lo"),
    (0.34, 0.67, "mid"),
    (0.67, 1.01, "hi"),
]


def conf_bucket(conf: float) -> str:
    for lo, hi, label in _CONF_BUCKETS:
        if lo <= conf < hi:
            return label
    return "0.00-0.50"


def p_alpha_bucket(v: Optional[float]) -> str:
    if v is None:
        return "na"
    for lo, hi, label in _PA_BUCKETS:
        if lo <= v < hi:
            return label
    return "0.0-0.2"


def risk_bucket(v: Optional[float]) -> str:
    if v is None:
        return "na"
    for lo, hi, label in _RISK_BUCKETS:
        if lo <= v < hi:
            return label
    return "lo"


def domain_of(site_url: Optional[str]) -> str:
    if not site_url:
        return "unknown"
    try:
        from urllib.parse import urlparse
        return (urlparse(site_url).netloc or "unknown").lower()
    except Exception:  # noqa: BLE001
        return "unknown"


def make_rule_key(
    source: str,
    market_type: Optional[str],
    cb: str,
    pa: str = "*",
    rk: str = "*",
) -> str:
    """Rule key for the lessons table. The `pa` and `rk` slots are cycle-7
    extensions (p_alpha bucket and risk bucket); legacy rules pass '*' for
    both, which means "any value matches"."""
    mt = (market_type or "unknown").lower()
    return f"src={source}|mt={mt}|cb={cb}|pa={pa}|rk={rk}"


# ── Seed lessons from cycle-3 postmortem findings ───────────────────────────
# Each tuple: (source, market_type, conf_bucket, action, note)
# These are intentionally generic (source='*' or market_type='*') because the
# postmortem evidence is aggregated, not per-source. The aggregator will
# specialize them once enough data is available.
_SEED_LESSONS: list[tuple[str, str, str, str, str]] = [
    ("*", "*", "0.75-0.85", "downweight",
     "Conf 0.75-0.85 was 100% no-signal in last cycle (IC<0); downweight before quoting confidence."),
    ("*", "*", "0.85-1.00", "skip",
     "Conf >=0.85 was 100% no-signal in last cycle; treat as NEUTRAL unless headline is a hard fact."),
    ("*", "political", "*", "downweight",
     "Political markets had 1.8% capture rate last cycle; require corroboration to fire YES/NO."),
    ("*", "price_threshold", "*", "downweight",
     "Price-threshold markets had 6% wrong-dir share — don't infer direction from sentiment alone."),
    ("tass.com", "*", "*", "downweight",
     "tass.com is Russia state media; politically charged claims should be discounted unless corroborated."),
    ("xinhuanet.com", "*", "*", "downweight",
     "Xinhua is China state media; downweight politically charged claims."),
    ("cn.people.com.cn", "*", "*", "downweight",
     "People's Daily is China state media; downweight politically charged claims."),
]


# ── Activation thresholds ───────────────────────────────────────────────────

MIN_SUPPORT_FOR_EMPIRICAL = 10
MIN_ABS_IC_FOR_ACTIVE = 0.05
SKIP_IC_THRESHOLD = -0.08          # IC below this with enough support → action=skip
DOWNWEIGHT_IC_THRESHOLD = -0.03    # IC below this → action=downweight


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def seed_if_empty(db) -> int:
    """Insert hard-coded seed lessons if `lessons` is empty. Returns rows inserted."""
    row = await db._conn.execute("SELECT COUNT(*) FROM lessons")
    n = (await row.fetchone())[0]
    if n > 0:
        return 0
    ts = _now_iso()
    inserted = 0
    for source, mt, cb, action, note in _SEED_LESSONS:
        rule_key = make_rule_key(source, mt, cb)
        await db._conn.execute(
            """INSERT OR IGNORE INTO lessons
               (rule_key, source, market_type, conf_bucket, action, note,
                seeded, active, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, 1, 1, ?)""",
            (rule_key, source, mt, cb, action, note, ts),
        )
        inserted += 1
    await db._conn.commit()
    logger.info("[lessons] seeded %d initial lessons", inserted)
    return inserted


# ── Aggregator ──────────────────────────────────────────────────────────────


async def aggregate_once(db) -> dict:
    """Recompute per-rule stats from price_impacts; upsert into lessons.

    Returns a small dict of {"rules_upserted": int, "active": int}.
    """
    # Cycle-7: pull p_alpha and risk_score from the trades table by joining
    # on (condition_id, headline_id). Many price_impacts don't have a
    # corresponding trade (rule-skipped at the pre-AI gate), and many trades
    # are skipped at later gates — that's fine, NULL p_alpha just lands in
    # the "na" bucket. AVG() collapses multiple-engine fan-out per impact.
    sql = """
    SELECT pi.site_url, pi.direction, pi.ai_confidence, pi.delta_t5m, pi.delta_t30m,
           m.market_type,
           AVG(t.p_alpha)    AS p_alpha,
           AVG(t.risk_score) AS risk_score
    FROM price_impacts pi
    LEFT JOIN markets m ON m.condition_id = pi.condition_id
    LEFT JOIN trades  t ON t.condition_id = pi.condition_id
                       AND t.headline_id = pi.headline_id
                       AND t.skip_reason IS NULL
    WHERE pi.delta_t5m IS NOT NULL
    GROUP BY pi.id
    """
    rows = await (await db._conn.execute(sql)).fetchall()
    if not rows:
        return {"rules_upserted": 0, "active": 0}

    # Aggregate: bucket → (n, captured, wrong_dir, no_signal, sum_signed_delta)
    buckets: dict[str, dict] = {}
    for r in rows:
        src = domain_of(r["site_url"])
        mt = (r["market_type"] or "unknown").lower()
        cb = conf_bucket(float(r["ai_confidence"] or 0.0))
        pa = p_alpha_bucket(r["p_alpha"])
        rk = risk_bucket(r["risk_score"])
        # delta is signed; direction=YES means we expected +delta, NO means -delta
        d5 = float(r["delta_t5m"] or 0.0)
        signed = d5 if r["direction"] == "YES" else -d5
        # Classify outcome
        if abs(d5) < 1e-6:
            outcome = "no_signal"
        elif signed > 0:
            outcome = "captured"
        else:
            outcome = "wrong_dir"

        # Generate multiple specialised rule_keys for the same observation —
        # most-specific to most-general — so the aggregator can fit a curve
        # at whatever granularity the data supports. Coarse cells fill fast;
        # fine cells need more samples but get more actionable when they do.
        candidate_keys = [
            make_rule_key(src, mt, cb),                       # legacy
            make_rule_key(src, "*", cb),
            make_rule_key("*", mt, cb),
            make_rule_key(src, mt, "*"),
        ]
        if pa != "na":
            candidate_keys += [
                make_rule_key("*", "*", "*", pa=pa),          # global p_alpha calibration
                make_rule_key(src, "*", "*", pa=pa),          # per-source p_alpha
                make_rule_key("*", mt, "*", pa=pa),           # per-market-type p_alpha
            ]
        if rk != "na":
            candidate_keys += [
                make_rule_key("*", "*", "*", rk=rk),          # global risk calibration
                make_rule_key("*", mt, "*", rk=rk),
            ]
        if pa != "na" and rk != "na":
            candidate_keys.append(make_rule_key("*", "*", "*", pa=pa, rk=rk))

        for key in candidate_keys:
            slot = buckets.setdefault(key, {
                "support_n": 0, "captured": 0, "wrong_dir": 0, "no_signal": 0,
                "sum_signed": 0.0, "source": src, "market_type": mt, "conf_bucket": cb,
            })
            slot["support_n"] += 1
            slot[outcome] += 1
            slot["sum_signed"] += signed

    ts = _now_iso()
    upserted = 0
    active = 0
    for rule_key, slot in buckets.items():
        n = slot["support_n"]
        # IC proxy: average signed delta in predicted direction.
        ic = slot["sum_signed"] / n if n > 0 else 0.0
        capture_rate = slot["captured"] / n if n > 0 else 0.0

        action: Optional[str] = None
        note: Optional[str] = None
        if n >= MIN_SUPPORT_FOR_EMPIRICAL and abs(ic) >= MIN_ABS_IC_FOR_ACTIVE:
            if ic <= SKIP_IC_THRESHOLD:
                action, note = "skip", (
                    f"{rule_key}: IC={ic:+.3f}, capture={capture_rate:.1%}, n={n}. "
                    f"Has been actively wrong; skip unless headline is unambiguous."
                )
            elif ic <= DOWNWEIGHT_IC_THRESHOLD:
                action, note = "downweight", (
                    f"{rule_key}: IC={ic:+.3f}, capture={capture_rate:.1%}, n={n}. "
                    f"Downweight confidence here."
                )
            elif ic >= 0.05:
                action, note = "note", (
                    f"{rule_key}: IC={ic:+.3f}, capture={capture_rate:.1%}, n={n}. "
                    f"This combination has been informative; lean on it."
                )

        active_flag = 1 if action and action != "note" else 0
        if action:
            active += active_flag
            await db._conn.execute(
                """INSERT INTO lessons
                   (rule_key, source, market_type, conf_bucket, support_n,
                    captured_n, no_signal_n, wrong_dir_n, ic, capture_rate,
                    action, note, seeded, active, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
                   ON CONFLICT(rule_key) DO UPDATE SET
                     support_n=excluded.support_n,
                     captured_n=excluded.captured_n,
                     no_signal_n=excluded.no_signal_n,
                     wrong_dir_n=excluded.wrong_dir_n,
                     ic=excluded.ic,
                     capture_rate=excluded.capture_rate,
                     action=excluded.action,
                     note=excluded.note,
                     seeded=0,
                     active=excluded.active,
                     updated_at=excluded.updated_at""",
                (rule_key, slot["source"], slot["market_type"], slot["conf_bucket"],
                 n, slot["captured"], slot["no_signal"], slot["wrong_dir"],
                 ic, capture_rate, action, note, active_flag, ts),
            )
            upserted += 1
    await db._conn.commit()

    # Deactivate seeded lessons that have been overtaken by empirical evidence
    # (any non-seeded active lesson for the same source/mt/cb tuple)
    await db._conn.execute(
        """UPDATE lessons SET active = 0
           WHERE seeded = 1
             AND EXISTS (
               SELECT 1 FROM lessons l2
               WHERE l2.seeded = 0
                 AND l2.active = 1
                 AND (l2.source = lessons.source OR l2.source = '*' OR lessons.source = '*')
                 AND (l2.market_type = lessons.market_type OR l2.market_type = '*' OR lessons.market_type = '*')
                 AND (l2.conf_bucket = lessons.conf_bucket OR l2.conf_bucket = '*' OR lessons.conf_bucket = '*')
             )"""
    )
    await db._conn.commit()

    logger.info("[lessons] aggregated: %d rules upserted, %d active",
                upserted, active)
    return {"rules_upserted": upserted, "active": active}


async def load_active_notes(db, limit: int = 10) -> list[str]:
    """Return a small list of active lesson notes for prompt injection."""
    rows = await (await db._conn.execute(
        """SELECT note FROM lessons WHERE active=1 AND note IS NOT NULL
           ORDER BY (CASE action WHEN 'skip' THEN 0 WHEN 'downweight' THEN 1 ELSE 2 END),
                    support_n DESC
           LIMIT ?""",
        (limit,),
    )).fetchall()
    return [r["note"] for r in rows if r["note"]]


async def lookup_action(db, source: str, market_type: Optional[str], conf: float) -> Optional[str]:
    """Return the most-specific active action ('skip'|'downweight'|None) for a context."""
    cb = conf_bucket(conf)
    mt = (market_type or "unknown").lower()
    candidates = [
        make_rule_key(source, mt, cb),
        make_rule_key(source, mt, "*"),
        make_rule_key(source, "*", cb),
        make_rule_key("*", mt, cb),
        make_rule_key(source, "*", "*"),
        make_rule_key("*", mt, "*"),
        make_rule_key("*", "*", cb),
    ]
    placeholders = ",".join("?" for _ in candidates)
    row = await (await db._conn.execute(
        f"""SELECT action FROM lessons
            WHERE active=1 AND rule_key IN ({placeholders})
            ORDER BY (CASE action WHEN 'skip' THEN 0 WHEN 'downweight' THEN 1 ELSE 2 END)
            LIMIT 1""",
        candidates,
    )).fetchone()
    return row["action"] if row else None


# ── Background runner ──────────────────────────────────────────────────────


async def runner(db, interval_secs: float = 600.0) -> None:
    """Run aggregator forever. Designed to be created as a task in main()."""
    await seed_if_empty(db)
    while True:
        try:
            await aggregate_once(db)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[lessons] aggregator error: %s", exc, exc_info=True)
        await asyncio.sleep(interval_secs)
