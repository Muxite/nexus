"""
Manual-planning rules engine.

A simple, deterministic predicate engine that gates trades based on
user-curated heuristics in `seulgi/rules/*.md`. Each rule file has YAML-ish
frontmatter declaring the conditions under which the rule fires, plus a
markdown body that's read by humans (and by the post-mortem AI when
proposing new rules).

Design notes
────────────
- Rules run in two stages:
    * `pre_ai`  — checked before the AI classifier is called. Cheap filters
                   on market_type / mid_price. Saves OpenRouter tokens.
    * `post_ai` — checked after the AI returns a direction + confidence.
                   Used for rules conditioned on AI output (e.g. "skip when
                   the AI is highly confident — empirically anti-informative").
- A matching rule whose `action == skip` short-circuits trading and emits
  `RULE_<name>_SKIP` as the skip_reason so post-mortems can attribute exactly
  which rule killed which trade.
- `has_exceptions: true` is reserved for the AI-gated hybrid path (defer
  until a real rule needs it). Today such rules still fire as `skip` but
  log a TODO.

Frontmatter schema (all fields optional except `name`)
──────────────────
    name:            (str) unique identifier, used in RULE_<name>_SKIP
    enabled:         (bool, default true)
    stage:           pre_ai | post_ai     (default pre_ai)
    action:          skip                 (only "skip" is implemented)
    market_type:     (str) regex against markets.market_type, default ".*"
    direction:       YES | NO | any       (post_ai only; default "any")
    mid_min:         (float) inclusive lower bound on mid_price
    mid_max:         (float) inclusive upper bound on mid_price
                       Predicate matches when mid_min ≤ mid_price ≤ mid_max.
                       Either bound may be omitted.
    confidence_min:  (float, post_ai) inclusive lower bound on AI confidence
    confidence_max:  (float, post_ai) inclusive upper bound
    has_exceptions:  (bool, default false) — reserved; currently no-op
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Optional


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Rule:
    """One compiled rule. Predicates are checked left-to-right."""
    name: str
    enabled: bool = True
    stage: str = "pre_ai"                 # "pre_ai" | "post_ai"
    action: str = "skip"                  # only "skip" implemented
    market_type_rx: re.Pattern = re.compile(r".*")
    direction: str = "any"                # "YES" | "NO" | "any"
    mid_min: Optional[float] = None
    mid_max: Optional[float] = None
    confidence_min: Optional[float] = None
    confidence_max: Optional[float] = None
    has_exceptions: bool = False
    source_path: str = ""
    # Phase-E extensions (2026-05-19): freshness label + question regex.
    # Both default to None = "any". The freshness check is exact-match on
    # one of BREAKING / EVOLVING / REHASH / STALE_RETURN. The question regex
    # is searched (not matched) against trades.question and is the cheap path
    # for topic-level skip rules.
    freshness_label: Optional[str] = None
    question_rx: Optional[re.Pattern] = None

    def skip_reason(self) -> str:
        return f"RULE_{self.name}_SKIP"


# ---------------------------------------------------------------------------
# Frontmatter parser — deliberately tiny; no YAML dep
# ---------------------------------------------------------------------------

_FRONT_RX = re.compile(r"^---\s*\n(.*?)\n---\s*\n", re.DOTALL)


def _parse_frontmatter(text: str) -> dict:
    """
    Extract `key: value` pairs from a `---\\n…\\n---` block at the top of `text`.
    Quoting is optional. Lines without a colon are ignored. Comments after
    `#` are stripped. Returns an empty dict if no frontmatter is found.
    """
    m = _FRONT_RX.match(text)
    if not m:
        return {}
    out: dict[str, str] = {}
    for line in m.group(1).splitlines():
        line = line.split("#", 1)[0].strip()
        if not line or ":" not in line:
            continue
        key, _, value = line.partition(":")
        out[key.strip()] = value.strip().strip("'\"")
    return out


def _to_bool(s: str, default: bool = False) -> bool:
    if not s:
        return default
    return s.lower() in ("true", "yes", "1", "on")


def _to_float(s: str) -> Optional[float]:
    if s in ("", "null", "none", "~"):
        return None
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def _compile_rule(fm: dict, source_path: Path) -> Optional[Rule]:
    """Build a Rule from a parsed frontmatter dict. Returns None on schema error."""
    name = fm.get("name", "").strip()
    if not name:
        logger.warning("[rules] %s: missing 'name', skipping", source_path)
        return None
    try:
        market_rx = re.compile(fm.get("market_type", ".*"))
    except re.error as exc:
        logger.warning("[rules] %s: bad market_type regex (%s) — skipping", source_path, exc)
        return None

    stage = fm.get("stage", "pre_ai").lower()
    if stage not in ("pre_ai", "post_ai"):
        logger.warning("[rules] %s: unknown stage %r, defaulting to pre_ai",
                       source_path, stage)
        stage = "pre_ai"

    direction = fm.get("direction", "any").upper()
    if direction not in ("YES", "NO", "ANY"):
        direction = "ANY"

    freshness_label = fm.get("freshness_label", "").strip().upper() or None

    question_rx_raw = fm.get("question_rx", "").strip()
    question_rx: Optional[re.Pattern] = None
    if question_rx_raw:
        try:
            question_rx = re.compile(question_rx_raw)
        except re.error as exc:
            logger.warning("[rules] %s: bad question_rx (%s) — skipping", source_path, exc)
            return None

    return Rule(
        name=name,
        enabled=_to_bool(fm.get("enabled", "true"), default=True),
        stage=stage,
        action=fm.get("action", "skip").lower(),
        market_type_rx=market_rx,
        direction=direction.lower() if direction == "ANY" else direction,
        mid_min=_to_float(fm.get("mid_min", "")),
        mid_max=_to_float(fm.get("mid_max", "")),
        confidence_min=_to_float(fm.get("confidence_min", "")),
        confidence_max=_to_float(fm.get("confidence_max", "")),
        has_exceptions=_to_bool(fm.get("has_exceptions", "false")),
        source_path=str(source_path),
        freshness_label=freshness_label,
        question_rx=question_rx,
    )


# ---------------------------------------------------------------------------
# Predicate evaluation
# ---------------------------------------------------------------------------

def _in_range(value: Optional[float], lo: Optional[float], hi: Optional[float]) -> bool:
    """
    True if (lo/hi)-bounded predicate fires for `value`. If both bounds are
    None the predicate is treated as "ignore this field" — returns True so it
    composes with AND-of-predicates correctly.
    """
    if lo is None and hi is None:
        return True
    if value is None:
        return False
    if lo is not None and value < lo:
        return False
    if hi is not None and value > hi:
        return False
    return True


def _rule_matches(
    rule: Rule, *,
    market_type: Optional[str],
    mid_price: Optional[float],
    direction: Optional[str] = None,
    confidence: Optional[float] = None,
    freshness_label: Optional[str] = None,
    question: Optional[str] = None,
) -> bool:
    if not rule.enabled:
        return False
    mt = market_type or ""
    if not rule.market_type_rx.search(mt):
        return False
    if not _in_range(mid_price, rule.mid_min, rule.mid_max):
        return False
    if rule.freshness_label is not None:
        if (freshness_label or "").upper() != rule.freshness_label:
            return False
    if rule.question_rx is not None:
        if question is None or not rule.question_rx.search(question):
            return False
    if rule.stage == "post_ai":
        if rule.direction != "any":
            if direction is None or direction.upper() != rule.direction.upper():
                return False
        if not _in_range(confidence, rule.confidence_min, rule.confidence_max):
            return False
    return True


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class RuleEngine:
    """
    Loads `seulgi/rules/*.md` once at construction. Use `gate_pre_ai` before
    calling the AI classifier and `gate_post_ai` after. Either returns the
    first matching rule's skip_reason, or None when no rule fires.
    """

    def __init__(self, rules_dir: Path) -> None:
        self._rules_dir = rules_dir
        self._rules: list[Rule] = []
        self.reload()

    def reload(self) -> None:
        rules: list[Rule] = []
        if not self._rules_dir.exists():
            logger.info("[rules] no rules dir at %s — engine inactive", self._rules_dir)
            self._rules = rules
            return
        for path in sorted(self._rules_dir.glob("*.md")):
            try:
                fm = _parse_frontmatter(path.read_text())
            except OSError as exc:
                logger.warning("[rules] %s: read error %s", path, exc)
                continue
            if not fm:
                logger.info("[rules] %s: no frontmatter — skipping", path)
                continue
            rule = _compile_rule(fm, path)
            if rule is None:
                continue
            rules.append(rule)
        self._rules = rules
        logger.info(
            "[rules] loaded %d rule(s): %s",
            len(rules), ", ".join(r.name for r in rules) or "(none)",
        )

    @property
    def rules(self) -> list[Rule]:
        return list(self._rules)

    def gate_pre_ai(
        self, *, market_type: Optional[str], mid_price: Optional[float],
        freshness_label: Optional[str] = None, question: Optional[str] = None,
    ) -> Optional[str]:
        for r in self._rules:
            if r.stage != "pre_ai":
                continue
            if _rule_matches(
                r, market_type=market_type, mid_price=mid_price,
                freshness_label=freshness_label, question=question,
            ):
                return r.skip_reason()
        return None

    def gate_post_ai(
        self, *, market_type: Optional[str], mid_price: Optional[float],
        direction: str, confidence: float,
        freshness_label: Optional[str] = None, question: Optional[str] = None,
    ) -> Optional[str]:
        for r in self._rules:
            if r.stage != "post_ai":
                continue
            if _rule_matches(
                r,
                market_type=market_type, mid_price=mid_price,
                direction=direction, confidence=confidence,
                freshness_label=freshness_label, question=question,
            ):
                return r.skip_reason()
        return None
