"""External-spend log: cumulative AI tokens + Brave Search calls."""

from __future__ import annotations

from ..io import _header

FILENAME = "usage.txt"
TITLE = "EXTERNAL USAGE"


async def render(dash) -> str:
    lines = [
        _header("EXTERNAL USAGE  (lifetime tokens + searches)"),
        "  Tracks every AI token consumed and every Brave Search API call",
        "  across all engines and watchers. Use it to spot runaway costs.",
        "",
    ]
    if not dash._usage:
        lines.append("  (no usage data yet)")
        return "\n".join(lines) + "\n"

    ai_total = int(dash._usage.get("ai_total") or 0)
    brave_total = int(dash._usage.get("brave_total") or 0)
    ai_24h = int(dash._usage.get("ai_24h") or 0)
    brave_24h = int(dash._usage.get("brave_24h") or 0)
    ai_by_provider = dash._usage.get("ai_by_provider", [])

    # Rough: $0.10/M input + $0.40/M output ≈ $0.25/M blended (flash-lite).
    est_cost = ai_total / 1_000_000.0 * 0.25
    est_cost_24h = ai_24h / 1_000_000.0 * 0.25

    lines.append("  AI TOKENS (OpenRouter / Gemini)")
    lines.append(f"    Lifetime : {ai_total:>10,}  tokens  (~${est_cost:.4f} blended)")
    lines.append(f"    Last 24h : {ai_24h:>10,}  tokens  (~${est_cost_24h:.4f})")
    if ai_by_provider:
        lines.append("    By model:")
        for row in ai_by_provider:
            p = row.get("provider") or "?"
            n = int(row.get("n") or 0)
            t = int(row.get("tokens") or 0)
            lines.append(f"      {p:<40} calls={n:>6}  tokens={t:>10,}")
    lines.append("")

    lines.append("  BRAVE SEARCH  (Twitter watchers)")
    lines.append(f"    Lifetime : {brave_total:>6}  searches")
    lines.append(f"    Last 24h : {brave_24h:>6}  searches")
    lines.append("    Free tier cap : ~2,000/month  (~67/day)")
    if brave_24h > 67:
        lines.append("    ⚠  24h usage exceeds free-tier daily quota.")
    lines.append("")
    lines.append("  Real-time hourly rate visible in watcher_status.txt.")
    return "\n".join(lines) + "\n"
