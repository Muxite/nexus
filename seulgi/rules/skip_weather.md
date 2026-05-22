---
name: weather
stage: pre_ai
action: skip
market_type: ^weather$
engines_exclude: super_scout
---

# Skip weather markets

## Evidence
Weather markets ("Will the highest temperature in Paris be 11°C on May 15?")
re-price off the National Weather Service / equivalent forecasts, not off
news headlines. They're a structurally-bad fit for this bot — by the time
a news article *mentions* the weather, the forecast (and therefore the
market) has already moved.

Weather is ~8% of the tagged market universe; skipping it removes a
material slice of pointless AI-token spend.

## Caveats
Not many — short of "category-5 hurricane unexpectedly announced" there's
no headline-driven edge here. If a rare extreme-event exception ever
matters, escalate to AI-gated.
