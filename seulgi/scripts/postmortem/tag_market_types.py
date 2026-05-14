"""
Tag every row in `markets` with a coarse `market_type` based on the question
text. Deterministic, regex-only — no LLM. Idempotent: re-running re-evaluates
every row, so editing the rules below and re-running the script propagates.

Categories
──────────
  price_threshold  — "Will [asset] hit $X by Y", "settle at $X-$Y", "above N"
                     on equities/commodities/indices/macro indicators (Fed
                     rate, yield, GDP, CPI, …).
  crypto_price     — Same shape but the asset is a cryptocurrency
                     (BTC, ETH, SOL, XRP, …). Split out because crypto
                     volatility is structurally different from macro markets.
  sports           — Any league/team/match/tournament/player markets.
  weather          — Temperature / precipitation / hurricane markets.
  political        — Elections, primaries, nominees, parliamentary seats,
                     "Will [politician] X" markets.
  entertainment    — Music charts, films, video games, awards.
  event            — Geopolitical, diplomatic, ceasefire, peace deal,
                     announcements, AI/tech product news. The signal-rich
                     bucket for headline-driven trading.
  other            — fallback when nothing matches confidently.

The first-match-wins ordering matters: more specific patterns first.

Usage
─────
  python3 -m scripts.postmortem.tag_market_types          # update DB
  python3 -m scripts.postmortem.tag_market_types --dry    # print summary only
"""
from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Optional


# Crypto tickers we care about. Kept narrow on purpose — bare "BTC"-like
# tokens are too easy to false-match against other contexts.
_CRYPTO_NAMES = (
    "bitcoin", "btc",
    "ethereum", "eth",
    "solana", "sol",
    "ripple", "xrp",
    "dogecoin", "doge",
    "cardano", "ada",
    "avalanche", "avax",
    "litecoin",
    "polkadot",
    "chainlink",
    "tether", "usdt", "usdc",
    "tron",
    "shiba",
    "polygon",
    "uniswap",
    "monero",
    "stellar",
)
_CRYPTO_RX = re.compile(r"\b(" + "|".join(_CRYPTO_NAMES) + r")\b", re.I)

# Tradeable assets and macro indicators that drive price_threshold markets.
_PRICE_ASSET_RX = re.compile(
    r"\b("
    r"oil|crude|brent|wti|cl|gold|silver|copper|gas|natural gas"
    r"|s&p|nasdaq|dow|russell|vix"
    r"|fed|treasury|yield|libor|sofr"
    r"|cpi|ppi|gdp|inflation|unemployment|jobless|payrolls?|nonfarm"
    r"|fdv|market cap"
    r"|eur/usd|usd/jpy|gbp/usd|forex"
    r")\b",
    re.I,
)
# "Will X hit $Y" / "above $Y" / "settle at $Y" / "$Y by date"
_PRICE_SHAPE_RX = re.compile(
    r"(\$\s*\d|\bhit\b|\babove\b|\bbelow\b|\bsettle\b|\breach\b|\bexceed\b|\bbasis points?\b|\bbp\b)",
    re.I,
)
_PRICE_UP_DOWN_RX = re.compile(r"\bup\s+or\s+down\b", re.I)

# Weather markets are extremely templated on Polymarket.
_WEATHER_RX = re.compile(
    r"\b(temperature|precipitation|rainfall|snowfall|hurricane|cyclone"
    r"|highest temperature|lowest temperature|°[CF])\b",
    re.I,
)

# Sports — a deliberately wide net since the user explicitly flagged sports
# as a bad default-trade universe. Catches league names, common formats,
# team-vs-team headers, and betting-jargon prefixes.
_SPORTS_RX = re.compile(
    r"\b("
    r"nba|nfl|nhl|mlb|mls|wnba|ufc|mma|boxing|formula 1|f1|nascar"
    r"|premier league|la liga|serie a|bundesliga|ligue 1|champions league|europa"
    r"|world cup|euro\b|copa america|copa libertadores|copa del rey"
    r"|wimbledon|french open|us open|australian open"
    r"|stanley cup|super bowl|world series"
    r"|playoffs?|conference finals?|semifinals?|quarterfinals?"
    r"|t20|odi|ipl|cricket"
    r"|eurovision"
    r"|grand slam"
    r"|cs2|csgo|lol|league of legends|valorant|vct|dota"
    r"|spread|moneyline|over/under|o/u|handicap|toss match"
    r"|game \d|match \d"
    r")\b",
    re.I,
)
# "Team A vs. Team B" / "Team A vs Team B" header style.
_VS_RX = re.compile(r"\bvs\.?\s+", re.I)

# Political markets.
_POLITICAL_RX = re.compile(
    r"\b("
    r"president|presidential|election|primary|primaries|nominee|nomination"
    r"|governor|gubernatorial|senate|senator|congress|house\s+seat"
    r"|parliament|parliamentary|prime minister|chancellor|prefect"
    r"|democratic|republican|gop|labour|tory|liberal democrat"
    r"|cabinet|impeach|impeachment"
    r"|trump|biden|harris|vance|musk|putin|xi jinping|netanyahu|zelensk|macron"
    r"|will .* (say|tweet|post|sign|veto|fire|nominate|confirm|appoint)"
    r")\b",
    re.I,
)

# Entertainment / culture.
_ENT_RX = re.compile(
    r"\b("
    r"billboard|hot 100|#1 (song|paid app|film|movie)"
    r"|grammys?|oscars?|emmys?|tonys?|cannes"
    r"|james bond|spider-?man|marvel|dc comics?"
    r"|half-life|grand theft auto|gta\s*\d"
    r"|netflix|hulu|spotify|apple music"
    r")\b",
    re.I,
)


def classify_question(question: str) -> str:
    """
    Return the market_type label for a single question string. Pure function;
    safe to import from the post-mortem orchestrator or the trader.
    """
    if not question:
        return "other"
    q = question.strip()

    # 1) Crypto first — the asset keyword is the strongest discriminator and
    #    plenty of crypto markets use generic "above $X" phrasing that would
    #    otherwise fall into price_threshold.
    if _CRYPTO_RX.search(q) and (_PRICE_SHAPE_RX.search(q) or _PRICE_UP_DOWN_RX.search(q)):
        return "crypto_price"

    # 2) Weather is templated and unambiguous.
    if _WEATHER_RX.search(q):
        return "weather"

    # 3) Sports markets often have a "vs." header with no question word at
    #    all, so check the vs. shape too — but only when no political/event
    #    signal is present (otherwise "Russia vs Ukraine" hits sports).
    if _SPORTS_RX.search(q):
        return "sports"
    if _VS_RX.search(q) and not _POLITICAL_RX.search(q):
        # Bare "Team A vs Team B" markets — very common on Polymarket.
        return "sports"

    # 4) Price-target macro/equity markets.
    if _PRICE_ASSET_RX.search(q) and (_PRICE_SHAPE_RX.search(q) or _PRICE_UP_DOWN_RX.search(q)):
        return "price_threshold"

    # 5) Political.
    if _POLITICAL_RX.search(q):
        return "political"

    # 6) Entertainment.
    if _ENT_RX.search(q):
        return "entertainment"

    # 7) Default to event — the residual is dominated by geopolitical /
    #    diplomatic / corporate-action markets, which is the headline-rich
    #    bucket the strategy is actually targeting.
    return "event"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _add_app_to_path() -> None:
    sys.path.insert(0, str(_repo_root() / "app"))


async def _main(args: argparse.Namespace) -> int:
    _add_app_to_path()
    from config import load_config
    from db import SeulgiDB

    cfg = load_config(args.config or os.getenv("SEULGI_CONFIG", "config.yaml"))
    db_path = args.db or cfg.db.path
    if not Path(db_path).exists():
        alt = _repo_root() / "data" / "seulgi.db"
        if alt.exists():
            db_path = str(alt)

    db = SeulgiDB(db_path)
    if args.dry:
        await db.connect_ro()
    else:
        # connect() runs the migration that adds the market_type column.
        await db.connect()
        # Live bot holds the writer lock often; wait up to 60s per statement
        # instead of erroring. Affects this connection only.
        await db._conn.execute("PRAGMA busy_timeout = 60000")

    # Load every market once, classify in-process, batch the UPDATEs.
    # In --dry mode the column may not exist yet (no migration was run),
    # so probe pragma_table_info and adjust the SELECT.
    async with db._conn.execute("PRAGMA table_info(markets)") as cur:
        cols = {row["name"] for row in await cur.fetchall()}
    has_col = "market_type" in cols
    select_sql = (
        "SELECT condition_id, question, market_type FROM markets"
        if has_col
        else "SELECT condition_id, question FROM markets"
    )
    async with db._conn.execute(select_sql) as cur:
        rows = await cur.fetchall()

    counts: Counter[str] = Counter()
    changes: list[tuple[str, str]] = []     # (condition_id, new_type)
    for row in rows:
        new_type = classify_question(row["question"])
        counts[new_type] += 1
        prior = row["market_type"] if has_col else None
        if (prior or None) != new_type:
            changes.append((row["condition_id"], new_type))

    print(f"── market_type tagger  ({len(rows)} markets) ──")
    for k in sorted(counts, key=lambda x: -counts[x]):
        print(f"  {k:<16} {counts[k]:>6}  ({100*counts[k]/len(rows):.1f}%)")
    print(f"  {'changes':<16} {len(changes):>6}")

    if args.dry:
        print("(dry run — no DB writes)")
        await db.close()
        return 0

    # Apply in chunks to keep the transaction reasonable on the live DB.
    BATCH = 1000
    for i in range(0, len(changes), BATCH):
        chunk = changes[i:i + BATCH]
        await db._conn.executemany(
            "UPDATE markets SET market_type = ? WHERE condition_id = ?",
            [(t, cid) for cid, t in chunk],
        )
        await db._conn.commit()
    print(f"wrote {len(changes)} updates")
    await db.close()
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    p.add_argument("--db", default=None)
    p.add_argument("--config", default=None)
    p.add_argument("--dry", action="store_true", help="classify but don't write")
    args = p.parse_args()
    try:
        return asyncio.run(_main(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    os._exit(main())
