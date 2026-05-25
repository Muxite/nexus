Polymarket’s fee structure is dynamic and applies specifically to taker orders at the time of execution, with rules well-documented in their official developer resources.

Fee Formula and Execution
The official Polymarket documentation confirms the protocol uses the formula fee = C × feeRate × p × (1 - p), where C is the number of shares and p is the execution price (the taker's fill price, not the displayed mid). This fee is applied per-leg at match time exclusively to taker orders. No trading fees are charged at market resolution, as fees are only collected when orders are matched on the order book.

Fee Category Mapping
Your hardcoded table is perfectly accurate and originates from the primary docs.polymarket.com fee schedule. Polymarket categorizes markets and assigns base fee rates and rebate pool percentages accordingly.

Category	Taker Fee Rate	Maker Fee Rate	Maker Rebate Pool Share
Crypto	0.07	0	20%
Economics / Culture / Weather / Default	0.05	0	25%
Finance / Politics / Tech	0.04	0	25%
Sports	0.03	0	25%
Geopolitics	0.00	0	—
Maker Rebates
Makers are never charged a fee upon execution (the maker fee is exactly zero). Polymarket does offer a maker rebate, but it is not paid as a direct negative fee on the trade itself. Instead, a percentage of all collected taker fees (20% to 25%, depending on the category) is placed into a daily pool, which is distributed to makers proportionally based on the simulated fee value of their filled liquidity.

Share Count Calculation
The Polymarket formula applies the taker fee rate directly to the share count (C), not the USDC notional size. While the baseline calculation uses the share count, the final fee is valued in USDC. For actual settlement, the protocol collects these taker fees in shares for buy orders and in USDC for sell orders.





Here is the technical breakdown of the Polymarket Gamma API behavior, addressing volume representations, payload structures, fee configurations, and rate limits for your trading bot.

Volume Metrics and Payloads
The Gamma API exposes 24-hour market volume primarily through the volume24hr and volumeNum fields. These values represent the total notional volume traded in USDC, rather than the raw share count. A sample payload for a high-volume market condition typically structures these fields as floats or stringified floats:

json
{
  "conditionId": "0x27...",
  "volume24hr": 250450.50,
  "volumeNum": 1500000.75,
  "volume": "1500000.75"
}
Resolved Market Shape
Once a market resolves, the API updates its boolean flags so that closed is true and acceptingOrders is false. The umaResolutionStatus transitions to a "resolved" state, and the outcomePrices array strictly reflects the final payout probability, usually settling at ["1", "0"] or similar discrete values. A resolved market payload will explicitly identify the resolving authority in the resolvedBy field:

json
{
  "closed": true,
  "acceptingOrders": false,
  "umaResolutionStatus": "resolved",
  "resolvedBy": "0x...",
  "outcomePrices": ["1", "0"]
}
Fee Drivers and Tags Taxonomy
Fee rates are enforced canonically by the underlying CLOB configurations rather than the tags array on the Gamma API. The tags array is drawn from a fixed backend taxonomy of specific tag IDs, not free-text strings, which can be enumerated using the /tags endpoint. This array functions exclusively for UI categorization and market discovery, rather than protocol-level execution logic.

Rate Limits and Pagination
Polymarket enforces rate limits via Cloudflare, permitting an extremely generous 4,000 requests per 10 seconds on the Gamma API. Pagination is strictly offset-based, requiring you to iterate using limit and offset query parameters across list endpoints. If your polling bot triggers an HTTP 429 limit, it should parse the Retry-After header to execute a precise backoff delay.

News Repricing and Adjustment Speed
Exact empirical half-lives for breaking-news repricing on Polymarket are highly context-dependent, but structural evidence confirms the platform experiences slower information incorporation compared to traditional equities. Following major news shocks, empirical metrics indicate that reaching 80% price reversion or settling into a new equilibrium can take hours, with complete recovery of baseline spreads sometimes taking 24 to 48 hours.

Despite this long retail-driven tail, the initial repricing phase is fiercely contested by algorithmic bots engaging in latency arbitrage to exploit stale prices immediately after news breaks. The top 1% of traders capture approximately 84% of all trading gains by acting as this informed liquidity. For your closer.timeout and BREAKING-bypass logic, this implies a dual-phase adjustment: you must bypass standard cycle delays to participate in the initial algorithmic shock (which occurs in milliseconds to seconds), but you can widen your edge floor for a longer duration to capture the delayed retail reaction that spans several hours.

Operating Latency Floor for High-Volume Traders
High-volume Polymarket traders operate entirely in the sub-second domain, utilizing persistent WebSocket streams and co-located servers rather than multi-second HTTP polling. Polymarket's core Central Limit Order Book (CLOB) infrastructure is hosted on AWS in the eu-west-2 (London) region. Competitive algorithmic bots deploy out of proximal data centers (such as AWS Dublin or London), achieving raw network pings of 0.5ms to 5ms to the exchange's WebSocket endpoints.

While Polymarket's internal server matching engine inherently adds a processing overhead of roughly 70ms to 85ms to each transaction, engineers minimize total latency by batching multiple orders in a single payload and maintaining persistent TCP connections to eliminate new TLS handshakes. Crucially, Polymarket recently removed a 500-millisecond artificial delay penalty that was previously imposed on all taker orders. With this speed bump eliminated, the true operating latency floor for news-trading bots is under 100 milliseconds. Because of this, a 2-second budget is dangerously slow for execution if you are trying to compete directly on breaking news speed.





Your configurations for a quarter Kelly fraction and a 4% minimum edge strongly align with established quantitative standards for binary prediction markets. However, accurately validating your news arbitrage strategy will require careful handling of heavy-tailed event risks during a dedicated live testing period.

Kelly Fraction Sizing
In prediction markets, empirical backtests commonly cap the Kelly fraction at 0.25 (quarter Kelly) or 0.50 (half Kelly) to manage risk. This fractional approach is necessary because full Kelly sizing in binary options creates extreme volatility, including a 33% mathematical probability of halving a bankroll before doubling it. Using cfg.kelly_fraction = 0.25 is supported by quantitative literature as an optimal anchor to maximize logarithmic growth while heavily mitigating probability estimation errors and model inaccuracies.

Empirical Edge Thresholds
Published prediction market strategies typically require a 3% to 5% estimated edge over the market consensus to compound capital effectively. Open-source market bots like Kalshalyst employ a 3% absolute minimum edge for basic sizing, but specifically isolate opportunities with an edge of 4% or higher as optimal targets. Your cfg.trade.min_edge = 0.04 floor is a highly defensible threshold that provides a necessary buffer against platform fees, execution slippage, and compounding transactional friction.

Event Matching Cutoffs
Research on news event detection using cosine similarity establishes strict mathematical boundaries for automated deduplication and coreference. Academic frameworks typically use a strict upper cutoff of 0.90 for definite autonomous matches, whereas vector similarities between 0.80 and 0.90 are usually flagged for secondary iteration or human review. Setting your cfg.match.min_similarity and deduplication breaking distance in this 0.85 to 0.90 range is recommended to prevent false positives when piping real-time news into binary prediction contracts.

Arbitrage Variance and Evaluation
News arbitrage in prediction markets acts as a heavy-tailed strategy; algorithms can grind out consistent 1% to 3% returns with win rates exceeding 85%, but remain acutely vulnerable to rare, total-loss payouts caused by adverse selection and event misclassification. Because the average latency arbitrage opportunity now closes in roughly 2.7 seconds, consistent profitability is heavily dictated by sub-100ms execution rather than pure predictive accuracy. Industry literature suggests a minimum "soak time" of 30 days in live, small-position testing to confidently validate edge and risk management across multiple market cycles.




For a solo developer building a prediction market trading bot, relying on free RSS feeds from sources like Axios and AP News yields the highest signal-per-dollar, as tier-1 commercial wire services enforce strict anti-scraping policies and prohibitive enterprise-level pricing. Real-time push feeds are exclusively gatekept behind these expensive commercial API subscriptions rather than being publicly exposed.

Free RSS Feeds
Axios and the Associated Press (AP) provide the highest signal-per-dollar because they offer direct, free RSS feeds containing market-moving political and economic updates. You can directly poll Axios via their native feed at axios.com/feeds/feed.rss, while AP allows users to subscribe to specific category feeds for non-commercial use. The Financial Times also offers a "myFT" RSS feature that enables registered users to generate custom feeds based on specific topics they follow.

Scraping Policies
Tier-1 publishers like Bloomberg, Reuters, and the New York Times implement strict robots.txt directives and explicitly prohibit automated scraping in their Terms of Service. These organizations utilize web application firewalls to enforce strict rate limits, meaning unauthorized commercial scraping will trip security systems and risk IP blacklisting. Furthermore, major publishers have recently tightened their legal frameworks to block automated AI data extraction, expressly excluding live news feeds from any standard scraping permissions.

Real-Time Push Feeds
Major public news platforms do not expose free WebSockets or Server-Sent Events (SSE) for real-time article delivery to retail web users. While SSE is architecturally ideal for low-latency unidirectional news broadcasting, tier-1 providers reserve these push architectures strictly for their premium commercial API clients. To maintain your scraper's latency budget without a paid subscription, you must rely on polite, high-frequency polling of public RSS or JSON endpoints.

Commercial Wire Pricing
Enterprise wire services are aggressively priced for institutional clients, making direct access highly cost-prohibitive for solo developers. Bloomberg recently raised its prices for 2026, while platforms like Reuters Connect and AP Newsroom rely entirely on custom B2B quotes rather than transparent individual tiers.

News Source	Solo User Pricing (Estimated)	Access Model
Bloomberg Terminal	~$2,665/month ($31,980/year)	Single terminal or Bloomberg Anywhere; programmatic real-time API feeds (B-PIPE) cost extra 
.
LSEG Workspace	~$300 to $1,833/month	A stripped-down version starts around $3,600/year, while the full suite is roughly $22,000/year 
.
AP Newsroom	Custom Quote	B2B pricing model based entirely on intended usage, licensing rights, and access level 
.
Dow Jones Newswires	Custom Quote	Enterprise B2B licensing requiring direct sales contact and institutional minimums.



Claude Haiku 4.5 provides the best initial response time for short-form classification tasks, while calibration techniques can resolve high rejection rates, and prompt caching efficiency relies heavily on fixed token-block boundaries.

LLM Latency and Classification
Claude Haiku 4.5 is the optimal choice for latency-sensitive classification, achieving a highly consistent time-to-first-token (TTFT) of roughly 597 milliseconds. While GPT-5 Mini is highly regarded for JSON output reliability, Gemini 2.5 Flash and DeepSeek V4 Flash offer competitive throughput and lower cost profiles for high-volume trading workflows.

Model	Latency Profile	Throughput	JSON / Output Reliability	Cost (Input/Output per 1M)
Claude Haiku 4.5	~597ms TTFT 
Standard	High 
$1.00 / $5.00 
Gemini 2.5 Flash	~1,238ms Total 
~223 t/s 
Moderate	$0.15 / $0.60 
GPT-5 Mini	~2,264ms Total 
~114 t/s 
Superior 
$1.10 / $4.40 
DeepSeek V4 Flash	Fast 
Standard	Moderate	$0.14 / $0.28 
Asymmetric Classifier Calibration
A 70–80% rejection rate is a widely recognized failure mode in classifiers with an asymmetric reject option, occurring when the cost of misclassification vastly outweighs the cost of abstaining. Because uncalibrated or risk-averse models attempt to minimize expected losses, they will aggressively default to the less penalizing rejection class if confidence thresholds are unbalanced. Researchers mitigate this through cost-aware calibration, utilizing techniques like Isotonic regression, Platt scaling, and conformal prediction to realign probability estimations with asymmetric loss matrices.

Prompt Caching Limits
The theoretical cache-hit ceiling for your prompts depends entirely on exact text prefix matching, as caching systems evaluate tokens sequentially. Modern caching engines enforce minimum prefix limits (often 1,024 tokens) and only cache subsequent text in fixed 128-token increments. By placing the static news article at the absolute beginning of the prompt and the dynamic Polymarket questions at the end, the theoretical cacheable ceiling of your static text is bounded by 
⌊
x
128
⌋
×
128
⌊ 
128
x
​
 ⌋×128 where 
x
x represents the static tokens exceeding the initial 1024-token requirement.





Legal and Operational Constraints
While Ontario residents are strictly prohibited from using Polymarket following an Ontario Securities Commission settlement, British Columbia is not explicitly listed as a restricted jurisdiction in their Terms of Service. Polymarket operates as a decentralized platform primarily without mandatory Know Your Customer (KYC) processes, relying instead on non-custodial wallet connections. However, the platform actively geofences prohibited regions, and utilizing a VPN to obscure bot traffic violates their terms, carrying a risk of having your trading access frozen if your IP manipulation is detected.

Bridging USDC to Polygon
The most cost-effective method to fund a Polymarket wallet is purchasing USDC on a centralized exchange that supports direct Polygon network withdrawals, completely bypassing expensive Ethereum mainnet bridging and gas fees. Once the withdrawal is initiated, Polygon Proof-of-Stake (PoS) network confirmations are incredibly fast, typically resolving within a few seconds. Polymarket does not charge any hidden fees for deposits or withdrawals made natively on the Polygon network, meaning your only cost will be the exchange's small flat withdrawal fee.

Alternative Prediction Markets
If you need a strategic backup for your bot, several alternative prediction markets offer different liquidity models, fee structures, and API capabilities.

Platform	Currency	Fee Structure	Liquidity & API	Availability
Kalshi	Fiat (USD)	0.5% – 1%	High liquidity; excellent free API	Strictly US-regulated 
Manifold	Play Money	Free	High user-generated liquidity; strong API	Global 
Smarkets	Fiat (GBP/EUR)	2% on net winnings	Strong in sports/politics; robust API	UK-based, restricted in some regions 
Polymarket	Crypto (USDC)	0% – 1.8% depending on market	Highest volume; Web3 CLOB API	Global (Excluding US/Ontario) 
While Kalshi offers a highly regulated environment and excellent APIs, its strict US-only compliance policy makes it completely inaccessible for a Canadian resident.

Canadian Taxation Guidelines
The Canada Revenue Agency (CRA) taxes both cryptocurrency dispositions and prediction market earnings based on the specific nature and frequency of your trading activity. Because you are operating an automated trading bot, the CRA will almost certainly classify your operation as a systematic business, meaning your net earnings will be taxed as fully taxable business income rather than partially exempt capital gains. These taxable events are technically realized on a per-trade basis, meaning every profitable disposition or swap of a contract must be meticulously logged for your annual net reporting.

Bot Hosting Options
A Virtual Private Server (VPS) from providers like Hetzner or DigitalOcean is the optimal choice for your bot, offering the persistent computing environment required to maintain active WebSocket connections to Polymarket's order book for just $5 to $10 a month. Edge workers (like Vercel or Cloudflare Workers) are cost-effective but inherently stateless, meaning they cannot seamlessly sustain the long-lived socket connections necessary for real-time market data streaming. While a home server utilizes existing hardware, it introduces the risk of ISP downtime and higher latency, which can severely degrade a bot's execution speed.



The short version is: for Polymarket.com, I found strong evidence that market settlement runs through UMA’s Optimistic Oracle rather than a separate standard “auto-resolve” path, and closed=true should not be treated as finality. The documented timings are a 2-hour challenge window for undisputed proposals and a multi-day path for disputed ones, but I did not find a published median wall-clock from closed=true to final outcome publication.

O-D1 resolution flow
Polymarket’s own docs say markets are resolved through UMA’s Optimistic Oracle: someone proposes an outcome, a bond is posted, and the proposal becomes final only if it survives the challenge period. A Polymarket guide adds one caveat: Polymarket International uses the Markets Team for clarifications and UMA for the actual resolution flow, while Polymarket US is described separately as using the Markets Team to resolve according to the criteria.

The dispute window is 2 hours. If nobody disputes, resolution is effectively final after that window; if someone does dispute, the process expands into a 24–48 hour debate period plus roughly 48 hours of voting, and Polymarket’s docs summarize disputed resolution as about 4–6 days total. I did not find a published median from closed=true to final outcomePrices, so for your bot I would treat closed=true as “trading closed / event ended,” not “safe to auto-close position because settlement is final.”

O-D2 ambiguous deadlines
I could not verify a single platform-wide rule that “by June 30” always means UTC midnight, end-of-day, or 1 second past midnight on July 1. Polymarket’s help center says the market rules define the end date, resolution source, and edge cases, which means the market page—not the English phrase alone—should be your source of truth.

There is also evidence that at least some Polymarket markets use Eastern Time as the operative clock, because a DST-related incident was specifically described as arising from ET-based timing on the platform. So the safest implementation is: do not hardcode a universal UTC interpretation for “by June 30”; instead, read the explicit market end timestamp and timezone from that market’s rules, and only fall back to a natural-language assumption when the rules truly omit a time.

O-D3 disputes and overturns
Published incidence numbers do exist: UMA said that, as of August 2024, it had settled 11,093 Polymarket markets and seen 217 disputes, which implies about 1.96% disputed and about 98% undisputed. A later UMA post stated a 98.11% undisputed proposal rate and said more than 80% of disputes came from proposals submitted too early.

What I did not find was a published Polymarket-specific overturn rate for disputed cases, meaning a clean breakdown of how often the initial proposal was ultimately reversed after dispute. There are public examples of reversals in disputed markets, including a March 2025 case described as having its apparent outcome reversed by token-weighted voting, but that is anecdotal rather than a platform-wide rate.

Bot implications
For settlement logic, the conservative rule is to wait for finalized resolution data rather than closing on deactivation or closed=true alone. For expiry math, the conservative rule is to key days_to_expiry and near_expiry off the market’s explicit end timestamp and timezone in the rules, because Polymarket’s wording and timezone handling are market-specific enough that a universal UTC shortcut can be wrong.


Short version: your $25 minimum looks safely above any reported trade floor, the native Polymarket CLOB supports GTC, GTD, FOK, and FAK plus post-only semantics, and a “market buy” is implemented as a marketable limit order rather than as a separate native order type.

On spreads and latency, I found useful mechanics and rough public heuristics, but not a strong official spread-by-tier dataset or an official fill-latency study, so your own i-C1 and i-D1 measurements should still be the reference point.

Spreads
In the Polymarket materials I found, spread coverage is mostly mechanical rather than statistical: the docs explain that the displayed price is the midpoint of the bid-ask spread, unless the spread is wider than $0.10, in which case the UI shows the last traded price instead.

The closest public liquidity-tier guidance I found was a 2026 trading guide that treats $0.01-$0.02 spreads as tight/high-liquidity and $0.05+ spreads as wide/low-liquidity, which is useful as a rough bucket system but not a true distribution.

So for O-C1, I would treat your own spread measurements as primary, and I would add an explicit spread term to fee_buffer whenever your realized taker spread is non-trivial versus fees, especially outside the tightest books.

Trade floor
Polymarket’s help center says the orderbook “does not have trading size limits” by design, but multiple public integrations and bot docs consistently mention practical minimums around $1 or 5 shares, which likely reflects client-side checks or per-market metadata rather than a single universal floor.

Either way, min_position_usdc = $25 is comfortably above every reported minimum I found.

Also keep bridge minimums separate from trading minimums: Polymarket’s bridge docs show common L2 deposit minimums around $2, but that is about funding, not the minimum executable order once funds are already on venue.

Order handling
Official Polymarket docs say all orders are limit orders, and “market orders” are supported by submitting a limit order with a marketable price that executes against the best available resting orders.

The core documented order types are GTC, GTD, FOK, and FAK, plus post-only behavior; Polymarket documents FAK as “fill what’s available, cancel the rest,” while some third-party integrations expose similar immediate-or-cancel semantics under the IOC label.

During a fast move, GTC and GTD can partially fill and leave the remainder resting, FOK either completes in full or cancels immediately, FAK/IOC takes the available slice and cancels the balance, and post-only is rejected if it would cross the spread and execute right away.

Latency
I did not find an official Polymarket benchmark for end-to-end fill latency on $25-$200 orders, but Polymarket does describe a hybrid lifecycle with offchain order creation and matching plus onchain settlement, which is designed to keep matching fast.

The most concrete public datapoints I found were a user report of a 250 ms delay for taker orders and a third-party latency post claiming sub-50 ms book updates plus roughly 180-420 ms residential round trips, though those figures should be treated as anecdotal rather than canonical.

Taken together, that points to small aggressive orders in moderately liquid markets being more likely sub-second than multi-second under normal conditions, while multi-second outcomes are more likely to come from thin depth, partial-fill cleanup, or passive posting rather than the matching engine alone.



