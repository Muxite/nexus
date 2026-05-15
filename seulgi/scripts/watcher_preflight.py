"""
Watcher preflight — observe each watched_site for 30 minutes and report whether
it exposes feed/cache signals we could subscribe to instead of polling.

For each URL in config.yaml::watched_sites:
  1. One full GET to discover <link rel=alternate> feed URLs and seed the
     headline set.
  2. Probe well-known feed paths (/feed, /rss, /atom.xml, ...).
  3. HEAD every 60s for 30 min; record Last-Modified / ETag transitions.
  4. Full GET every 5 min; diff headline set vs the previous tick.
  5. Write seulgi/data/preflight/<host>.json with the verdict.

Run from seulgi/ as:  python3 -m scripts.watcher_preflight
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import sys
import time
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlparse

import aiohttp
import yaml

REPO = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO / "config.yaml"
OUT_DIR = REPO / "data" / "preflight"

OBSERVE_SECONDS = 30 * 60
HEAD_INTERVAL = 60
GET_INTERVAL = 5 * 60
REQUEST_TIMEOUT = 25

FEED_PATHS = [
    "/feed", "/feed/", "/rss", "/rss/", "/rss.xml", "/atom.xml",
    "/feed.xml", "/feeds/all.atom.xml", "/index.xml",
]

UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/127.0 Safari/537.36 seulgi-preflight/1.0")
HEADERS = {"User-Agent": UA, "Accept": "text/html,application/xhtml+xml,application/xml"}

logger = logging.getLogger("preflight")


class _LinkAndHeadlineParser(HTMLParser):
    """Pulls <link rel=alternate> feeds AND headline-ish anchor text in one pass."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.feed_links: list[str] = []
        self.headlines: list[str] = []
        self._capture_text = False
        self._current = []
        self._tag_stack: list[str] = []

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "link":
            rel = (a.get("rel") or "").lower()
            typ = (a.get("type") or "").lower()
            if "alternate" in rel and ("rss" in typ or "atom" in typ or "xml" in typ):
                href = a.get("href")
                if href:
                    self.feed_links.append(href)
        if tag in ("h1", "h2", "h3", "a"):
            self._tag_stack.append(tag)
            self._capture_text = True

    def handle_endtag(self, tag):
        if tag in ("h1", "h2", "h3", "a") and self._tag_stack and self._tag_stack[-1] == tag:
            self._tag_stack.pop()
            text = "".join(self._current).strip()
            self._current.clear()
            if 20 <= len(text) <= 200:
                self.headlines.append(re.sub(r"\s+", " ", text))
            if not self._tag_stack:
                self._capture_text = False

    def handle_data(self, data):
        if self._capture_text:
            self._current.append(data)


async def _fetch(session, url, method="GET"):
    try:
        async with session.request(method, url, headers=HEADERS,
                                   timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                                   allow_redirects=True) as resp:
            body = await resp.text(errors="replace") if method == "GET" else ""
            return resp.status, dict(resp.headers), body
    except Exception as exc:  # noqa: BLE001
        return None, {"_error": str(exc)}, ""


async def _discover_feeds(session, base_url, html):
    parser = _LinkAndHeadlineParser()
    try:
        parser.feed(html)
    except Exception:  # noqa: BLE001
        pass
    candidates = [urljoin(base_url, h) for h in parser.feed_links]
    parsed = urlparse(base_url)
    root = f"{parsed.scheme}://{parsed.netloc}"
    for p in FEED_PATHS:
        candidates.append(root + p)
    working = []
    seen = set()
    for url in candidates:
        if url in seen:
            continue
        seen.add(url)
        status, headers, body = await _fetch(session, url)
        if status == 200 and ("<rss" in body[:2000].lower() or "<feed" in body[:2000].lower()):
            working.append({"url": url, "content_type": headers.get("Content-Type", "")})
        if len(working) >= 3:
            break
    return working, parser.headlines


async def _observe(session, site_url):
    out = {
        "url": site_url,
        "started_at": time.time(),
        "initial_get": {},
        "feeds_discovered": [],
        "head_samples": [],
        "headline_churn": [],
        "errors": [],
    }

    # ── Initial GET ───────────────────────────────────────────────────────
    status, headers, body = await _fetch(session, site_url, method="GET")
    out["initial_get"] = {
        "status": status,
        "last_modified": headers.get("Last-Modified"),
        "etag": headers.get("ETag"),
        "content_type": headers.get("Content-Type", ""),
        "bytes": len(body),
        "error": headers.get("_error"),
    }
    if status != 200:
        out["errors"].append(f"initial GET status={status}")
        return out

    feeds, headlines = await _discover_feeds(session, site_url, body)
    out["feeds_discovered"] = feeds
    prev_headlines = set(headlines)
    out["headline_churn"].append({"t": 0, "n_total": len(prev_headlines), "n_new": 0})

    last_lm = out["initial_get"]["last_modified"]
    last_etag = out["initial_get"]["etag"]

    # ── Observe loop ──────────────────────────────────────────────────────
    deadline = time.monotonic() + OBSERVE_SECONDS
    last_head_at = time.monotonic()
    last_get_at = time.monotonic()
    while time.monotonic() < deadline:
        await asyncio.sleep(15)
        now = time.monotonic()
        elapsed = int(now - (deadline - OBSERVE_SECONDS))

        if now - last_head_at >= HEAD_INTERVAL:
            last_head_at = now
            status, headers, _ = await _fetch(session, site_url, method="HEAD")
            lm = headers.get("Last-Modified")
            et = headers.get("ETag")
            if status == 200 and (lm != last_lm or et != last_etag):
                out["head_samples"].append({
                    "t": elapsed, "last_modified": lm, "etag": et,
                    "changed_lm": lm != last_lm, "changed_etag": et != last_etag,
                })
                last_lm, last_etag = lm, et

        if now - last_get_at >= GET_INTERVAL:
            last_get_at = now
            status, _, body = await _fetch(session, site_url, method="GET")
            if status == 200:
                parser = _LinkAndHeadlineParser()
                try:
                    parser.feed(body)
                except Exception:  # noqa: BLE001
                    pass
                cur = set(parser.headlines)
                new = cur - prev_headlines
                out["headline_churn"].append({
                    "t": elapsed, "n_total": len(cur), "n_new": len(new),
                })
                prev_headlines = cur

    # ── Verdict ───────────────────────────────────────────────────────────
    lm_changes = sum(1 for s in out["head_samples"] if s["changed_lm"])
    etag_changes = sum(1 for s in out["head_samples"] if s["changed_etag"])
    total_new_headlines = sum(c["n_new"] for c in out["headline_churn"])
    has_feed = bool(out["feeds_discovered"])
    has_push_signal = has_feed or lm_changes > 0 or etag_changes > 0

    avg_new_per_5min = (total_new_headlines / max(1, len(out["headline_churn"]) - 1)) if len(out["headline_churn"]) > 1 else 0

    # Recommendation: fast poll if churn high, slow if low, defer to feed if available
    if has_feed:
        recommended = "subscribe_to_feed"
    elif avg_new_per_5min >= 5:
        recommended_secs = 20
        recommended = f"poll_{recommended_secs}s"
    elif avg_new_per_5min >= 1:
        recommended_secs = 60
        recommended = f"poll_{recommended_secs}s"
    else:
        recommended_secs = 300
        recommended = f"poll_{recommended_secs}s"

    out["verdict"] = {
        "has_feed": has_feed,
        "has_push_signal": has_push_signal,
        "lm_changes_30min": lm_changes,
        "etag_changes_30min": etag_changes,
        "total_new_headlines_30min": total_new_headlines,
        "avg_new_per_5min": avg_new_per_5min,
        "recommended": recommended,
    }
    return out


def _safe_filename(url: str) -> str:
    p = urlparse(url)
    base = p.netloc.replace(":", "_")
    path = re.sub(r"[^A-Za-z0-9._-]+", "_", p.path).strip("_")
    return f"{base}__{path}.json" if path else f"{base}.json"


async def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [preflight] %(message)s")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with CONFIG_PATH.open() as fh:
        cfg = yaml.safe_load(fh)
    sites = [s["url"] for s in cfg.get("watched_sites", [])]
    logger.info("observing %d sites for %d minutes", len(sites), OBSERVE_SECONDS // 60)

    connector = aiohttp.TCPConnector(limit=20, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        async def run_one(url):
            try:
                result = await _observe(session, url)
            except Exception as exc:  # noqa: BLE001
                result = {"url": url, "errors": [f"unhandled: {exc!r}"]}
            path = OUT_DIR / _safe_filename(url)
            path.write_text(json.dumps(result, indent=2, default=str))
            verdict = result.get("verdict", {})
            logger.info("done %s → %s (feed=%s, new/5m=%.1f)",
                        url, path.name,
                        verdict.get("has_feed"),
                        verdict.get("avg_new_per_5min", 0))

        await asyncio.gather(*(run_one(u) for u in sites))

    # Summary
    summary = {"generated_at": time.time(), "sites": []}
    for jf in sorted(OUT_DIR.glob("*.json")):
        if jf.name == "summary.json":
            continue
        try:
            data = json.loads(jf.read_text())
            summary["sites"].append({
                "url": data.get("url"),
                "initial_status": data.get("initial_get", {}).get("status"),
                "verdict": data.get("verdict"),
                "errors": data.get("errors", []),
            })
        except Exception:  # noqa: BLE001
            pass
    (OUT_DIR / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
    logger.info("summary written to %s", OUT_DIR / "summary.json")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
