# EDGES — every angle explored, ranked

Comprehensive catalogue of edge sources researched for Constantine. Each
entry includes: what the edge is, primary evidence, realism assessment,
expected ROI, infrastructure cost, integration plan, and kill criteria.

Ranked by **expected sharpe × capital capacity** at our scale (~$1500).

---

## Tier 1 — Implement next (high conviction, novel-ish, fits our infra)

### EDGE-A: Liquidity Rewards Market Making

**The edge:** Polymarket pays makers daily for resting two-sided GTC
limit orders that meet a max-spread + min-size threshold. As of May
2026, **the top market pays $4,000/day** to the makers collectively
sharing the score; many markets pay $40-$250/day. Confirmed via
`GET clob.polymarket.com/rewards/markets/current`.

**Reward formula (verbatim from docs):**
```
S(v, s) = ((v - s) / v)² · b
Q_min   = max(min(Q_one, Q_two), max(Q_one/c, Q_two/c))   if mid in [0.10, 0.90]
        = min(Q_one, Q_two)                                otherwise
Daily payout = sum(your share) / sum(all maker shares) × pool, paid at 00:00 UTC
```

Where:
- `v` = market's `rewards_max_spread` (cents from mid)
- `s` = your order's distance from adjusted midpoint
- `b` = in-game multiplier (varies)
- `c` = single-sided penalty (currently 3.0 — quote both sides or get 1/3 score)

**Quadratic on tightness** is the key insight: a quote at v/2 scores
25% of max, a quote at the edge scores ~0%. Means the rewards
preferentially flow to the tightest 1-3 quotes per market.

**Why retail can compete:**
- No volume tier requirement, no whitelisting
- Score is computed every 6 seconds (10,080 samples/epoch — i.e. one
  epoch ≈ 16.8 hours). High-frequency requoting matters less than
  consistently being in the top tier of tightness for sustained periods.
- **$5M+ paid out per month** (April 2026 campaign), so the pool is
  real, not promotional.
- Low-volume markets (under-served by pro MMs) often have a 2-3 maker
  competition — easy to be in top 3.

**Why it's risky:**
- Adverse selection: when news breaks, your stale quote on one side
  fills first and you're stuck holding directional inventory.
- Price moves require rapid re-quote or you lose the score (s → v).
- Other MMs are bots too — quote-tightness war.

**Expected ROI math (conservative):**
- Pick markets with `rate_per_day` $40-$200, `rewards_min_size` $200-$500.
- Quote at v/2 both sides, $300 + $300 = $600 capital working per market
- 1-of-3 maker share → ~$15-65/day per market deployed
- 5 markets × $600 = $3,000 capital → ~$75-300/day = **2.5%-10% daily yield**
- Realistic after adverse selection: **0.5%-3% daily yield** = 12-90% monthly
- ~~~

That's the headline number. Even haircut by 90% from gaming/competition
this beats every other strategy on the list.

**Maker rebates stack on top:** 20-25% of taker fees collected on your
filled orders is rebated daily. Small absolute amount but free.

**Infrastructure required (mostly already built):**
- `submit_order_as` with `OrderType::GTC + post_only=true` ✅ (clob.rs)
- Cancel-by-market endpoint (need to add — currently only cancel-by-id)
- Bulk-order submit (`postOrders` up to 15 at once — implement)
- WebSocket book subscription with depth-2 quote tracking
- Quote refresh loop (2-5 sec cadence is plenty given 6s sample window)
- Inventory skew logic (don't buy what we already hold; quote-only sell side)
- Inventory caps (max $X net delta per market before pulling quotes)
- Polling `/rewards/markets/current` every hour to pick tonight's targets

**Integration plan (Strategy 6 — Liquidity Rewards):**

```
Phase 0 — backtest (2-3 days)
  - Pull /rewards/markets/current every hour for 1 week
  - Snapshot the order book at 6s cadence on top 10 markets
  - Simulate quoting at v/2 with $200 size both sides
  - Log: would my fills have been profitable after holding 30s post-fill?

Phase 1 — quoting infra (4-6 days)
  - rust-core/src/strategies/lp_rewards.rs
  - LpQuoter struct: per-market state (target spread, size, last quote IDs)
  - tokio task per market: pull /book, compute target prices, submit/cancel
  - Inventory tracker reusing PositionStore
  - Risk gate: max_delta_per_market, kill on >X net inventory

Phase 2 — paper trading (3-5 days, DRY_RUN mode)
  - Verify quote/cancel cadence under load
  - Verify rewards API matches our prediction of who scored

Phase 3 — live ramp (1 week)
  - Start 1 market, $100 each side, 6h
  - Add markets one per day if PnL is positive
  - Pull back hard on first $50+ adverse selection day
```

**Kill criteria:**
- Daily inventory loss > 50% of expected reward yield → regime broken
- More than 3 makers consistently within 10% of v on every market we
  enter → too crowded for profit
- Polymarket changes formula or pool size by >50%

**Status:** This is genuinely the best opportunity we've found. The
existing executor already handles GTC limits — we need the quoting loop
and cancel-by-market endpoint.

---

### EDGE-B: Event Rebalancing Arb (Strategy 2 — already built)

**The edge:** Within a single multi-outcome event (e.g. "Who wins the
Eurovision jury vote?"), all YES tokens should sum to 1.0. When they
sum to <0.97, buy them all + lock guaranteed profit.

**Academic confirmation (arxiv 2508.03474):**
- April 2024 - April 2025: $23.6M of profit extracted from
  within-market rebalancing arb across Polymarket
- 41% of analyzed markets showed single-market arb at some point
- Median mispricing: markets that should sum to $1.00 summed to $0.60
- Top 10 users captured $7M (17.7%); top account: $2M from 4,049
  trades (~$493 avg, bot-scale — fits our capital)

**Status: BUILT** in `signals/event_arb.rs` + `execution/event_arb_executor.rs`.

**Validation result (our own):** 1-3% maker-only edges on top events
after multi-stage filter (sum range + title keyword filter to drop
multi-winner events that distort the sum-equals-1 assumption).

**What's left to do:**
- Run live for 2 weeks in observe mode, score realized vs predicted edge
- If realized < 50% of predicted → tighten title keyword filter
- Consider adding cross-market arb (Strategy 2.5): "Trump wins overall"
  vs "Trump wins (state X conditional)"  — but academic data shows only
  $95K extracted across full year, so depth is shallow; probably skip.

---

### EDGE-C: Pinnacle Devig Overlay (sports edge sanity check)

**The edge:** Pinnacle Sports is the sharpest book globally; their
moneylines are the closest thing to "true" odds. Polymarket sports
markets are driven by retail sentiment. When Polymarket diverges from
Pinnacle's no-vig fair odds by >3%, take the Pinnacle side.

**Evidence:**
- "Polymarket prices are driven by retail sentiment while Pinnacle
  prices are set by professional bettors" (TheOddsAPI blog)
- "When they diverge, one is wrong" — Pinnacle wins these by reputation

**Mechanic:**
1. Pinnacle posts e.g. Lakers -150 / Suns +130
2. Devig: implied prob Lakers = 150/(150+100) = 60%; Suns = 100/(100+130) = 43%
3. Vig = 103% → divide by 1.03 → fair: Lakers 58.3%, Suns 41.7%
4. If Polymarket has Lakers YES at 0.55, edge = 58.3 - 55 = 3.3%
5. Buy Polymarket Lakers YES; risk = vig + execution slippage

**Capital efficiency:**
- NBA games: 30 game-day per typical week
- ~2-3 games per night with edge >3% (rough heuristic)
- Bet $50-100 per game, ~30 bets/week × 3% edge = $45-90/week expected
- At $1500 capital this is plausible additive yield

**Infrastructure cost:**
- Pinnacle direct API access has been restricted in 2026 — use
  TheOddsAPI ($30/mo Hobby tier covers NBA/NFL) or SportsGameOdds
- ~50 lines Python sidecar to fetch + devig + write JSON
- ~150 lines Rust signal evaluator (SignalDecision::Oracle, like prop sidecar)
- Reuses existing executor

**Integration plan (Strategy 3, originally "cross-platform" — repurpose):**

```
Phase 0 — backtest (1-2 days)
  - Pull last month of Pinnacle NBA closing lines from TheOddsAPI
  - Pull Polymarket close prices for same games
  - Compute distribution of Polymarket - Pinnacle fair-odds
  - If 5th-95th percentile spread is meaningful → green

Phase 1 — sidecar (1 day)
  - scripts/sportsbook_devig.py: pulls TheOddsAPI, devigs,
    writes data/sportsbook_lines.json

Phase 2 — Rust signal (2 days)
  - rust-core/src/signals/sportsbook_devig.rs
  - Match Polymarket question → game by date + team names
  - Emit SignalDecision::Oracle when |edge| > 3%
  - Wire into runner same way as nba_projections

Phase 3 — paper + live (1 week each)
```

**Risk:** Pinnacle has occasional sharp moves we miss (closing line is
the gold standard; intraday isn't). Need to use closing lines or last
30-min lines to maximize signal.

**Kill criteria:**
- Realized edge < 1% over 50 bets → either devig is wrong or
  Polymarket has caught up; pause and re-validate

---

## Tier 2 — Implement after Tier 1 stabilizes

### EDGE-D: Whale Following / Smart Money Copy

**The edge:** Polymarket trades are public on Polygon. Some wallets
(Theo4 +$22M lifetime, Fredi9999, weflyhigh, swisstony) have proven
edge. Their entries can be detected within seconds via on-chain
monitoring; their public history identifies them.

**Mechanic:**
1. Maintain rolling list of top-50-by-30-day-PnL wallets from
   `polymarket.com/leaderboard/overall/monthly/profit`
2. Subscribe to Polygon mempool / latest blocks; filter for tx-to
   = CTF Exchange contract from any tracked wallet
3. Decode the order; if direction has >X notional, replicate at $50

**Lag tolerance:** Order shows up on-chain ~3s after submission.
Polymarket book absorbs roughly $5-50K per side before moving 1¢. If
the whale put in $100K, our $50 follows at near-zero impact.

**Why this works (probably):**
- 16.8% of all wallets are net-positive lifetime per Dune analytics —
  meaning following a *consistently profitable* wallet is genuinely
  edge if we can identify them robustly.
- Pro tools (Polymarket Whale Tracker, PolymarketScan, polycopytrade)
  exist and charge subscriptions — proves there's commercial demand.

**Why this might NOT work:**
- Top whales are on news-event markets often. By the time we follow,
  the next price tick may already absorb the news.
- Survivorship bias in "top 100 PnL" lists — last month's winner is
  next month's loser without skill.
- Many top wallets are Polymarket employees / insiders historically.

**Infrastructure cost:**
- Polygon node WebSocket (Alchemy / QuickNode free tier insufficient
  for low-latency mempool — need ~$30/mo paid)
- ~300 lines Rust: chain listener, tx decoder, whitelist filter
- Scoring layer: per-wallet rolling Sharpe to drop bad-streak whales

**Integration plan (Strategy 7 — Whale Follow):**

```
Phase 0 — backtest (3-4 days)
  - Use Bitquery or Dune to pull last 90 days of trades from top 50
    wallets by 30-day PnL
  - For each whale-trade, simulate following at +5s lag with $50 max
  - Compute realized PnL; require >2% per-trade after fees to pass
```

**Risk profile:** Highly correlated to whales' alpha decay. The moment
a whale hits the public lists (already public), part of their edge is
priced in by other followers.

**Kill criteria:**
- Realized follow-edge < 0.5% over 50 trades → skill is decayed
- Too many cancellations / wash trades from tracked whales

---

### EDGE-E: Hibernating Markets (low-vol, near-resolution mispricing)

**The edge:** Markets close to resolution where the outcome is near-
certain but the price is far from 0/1. Examples: a market
"Will Trump be president on June 1" sitting at $0.96 on May 30 — if
no impeachment process is active, $0.04 risk for $0.04 reward isn't
attractive *but* there's a long tail of markets pricing irrationally.

Specifically: low-volume markets where stale orders linger for days.

**Evidence:**
- arxiv 2508.03474: median mispricing in arb markets was $1.00 vs $0.60.
- arxiv 2605.00864 (Polymarket NBA arb): "76.9% of combinatorial
  episodes constrained to an average executable size of just 14.8
  shares" — i.e. small but present.
- "In low-liquidity markets, maker-taker fee asymmetry can lead to
  adverse selection if informed traders target stale limits."

**Mechanic:**
1. Scan all markets every 30 min via Gamma `/events`
2. Filter: end_date_ms within next 14 days, volume_24h < $1000,
   yes_bid in [0.02, 0.10] OR yes_ask in [0.90, 0.98]
3. Match against external truth source:
   - Sports: live ESPN box score / final whistle
   - Politics: news scrape for resolution criterion
   - Crypto: CoinGecko price for level markets
4. If external says "outcome is 99% certain" but Polymarket prices
   95-98%: take the certain side. Profit = 1¢-5¢ per trade, capital
   tied up for days, sharpe is poor but capacity is high.

**Why this is non-obvious:**
- Most arb hunters skip low-vol markets because capacity is small
- But $1500 IS small — we are sized perfectly for these
- 50 markets × $30 each × 3% edge × 1-week hold ≈ $45/week dependable

**Infrastructure cost:**
- Reuses existing market scanner
- Need: per-category truth source (sports = ESPN, crypto = CoinGecko)
- ~250 lines Rust + 1 Python news scraper

**Risk:**
- The "near-certain" outcome occasionally flips (market knew something
  we didn't). Must size each bet at <1% of capital and diversify across
  many simultaneous bets.

**Integration plan (Strategy 8 — Stale Limit Sniper):**

```
Phase 0 — survey (2 days)
  - Pull 1 month of closed low-volume markets from Gamma
  - For each: was final close-time price <0.05 or >0.95? did it resolve?
  - Compute: of markets at $0.95 with 7d to close, what % paid out?
```

Defer this to Tier 2 because edge per trade is small and operations
overhead is meaningful (need multiple truth sources, news scraping).

---

### EDGE-F: USDC Float Yield (Aave V3 Polygon)

**The edge:** USDC sitting in the trading account is dead capital.
Deposit it into Aave V3 Polygon when not deployed; withdraw on demand
when bot needs to size up.

**Math:**
- Aave Polygon USDC supply APY ranges 3-10% in 2026 (varies by
  utilization). Conservative: 4% APY = 0.011%/day = $0.16/day on
  $1500 idle.
- Polygon gas: < $0.01 per supply/withdraw
- Bot ticks every 200ms; trade sizing rare (<10/day for our scale)
- If 80% of capital sits idle on average: $1200 × 4% = $48/year free

**Why this is small but free:**
- Doesn't compete with anything; just a yield-on-rest layer
- Risk: smart contract risk on Aave (audited, $5B+ TVL — low)
- Risk: USDC depeg (Aave protects via liquidations; not a direct path
  for us)

**Integration plan (background utility, not a strategy):**

```
Phase 1 — capital manager
  - rust-core/src/finance/aave_pool.rs
  - On bot startup: deposit (USDC_balance - reserve) into aUSDC
  - Before trade: if free balance < target_bet, withdraw from Aave
  - Monitor utilization; pull all USDC if utilization >90% (liquidity risk)
```

**Pause criteria:**
- Aave V3 USDC market hits 95% utilization (withdrawal queue forms)
- USDC depegs >0.5% (rare, but trip the kill switch)

**Status:** Defer until live trading is profitable. Adding $48/year on
$1500 isn't worth a week of work; consider it bonus for v2.

---

## Tier 3 — Researched but skeptical (likely not worth implementation)

### EDGE-G: Funding Rate / Perp Basis as BTC Direction Signal

**Claim:** When Binance BTC perp funding rate > 0.05% / 8h, longs are
overcrowded; expect short-term mean reversion. Use as overlay on
Polymarket BTC binary markets.

**Evidence (mixed):**
- "Extreme positive rates (above 0.10% per 8 hours) often precede
  corrections by indicating overleveraged long positions"
- BUT: "In strong trending markets, elevated funding rates can persist;
  however, in ranging or late-stage markets, they are more likely to
  serve as early warning signals"
- "Strong trends can sustain extreme funding for weeks without reversing"

**Why I'd skip:**
- The signal is well-known (every crypto trader watches funding)
- Polymarket BTC binaries move fast; by the time funding hits "extreme",
  the market has already integrated
- Backtest-friendly but live-tradeable on Polymarket binaries is doubtful
- Better as confirmation overlay on existing Strategy G13 oracle rather
  than its own strategy

**Verdict:** Don't implement standalone. Optionally add as a feature
into the oracle signal weighting (e.g. require funding < +0.05% / 8h
to take the LONG side of an oracle pop).

---

### EDGE-H: Manifold Differential (play-money vs real-money)

**Claim:** Manifold (play-money) markets cover the same questions as
Polymarket. Differences in the prices reflect different participant
biases. When Manifold is way above Polymarket on Q, real-money will
catch up.

**Evidence:**
- Manifold has REST API (500 req/min/IP, no auth)
- Aggregator services like FinFeedAPI and PolyRouter expose both
- Polymarket = mid of best bid/ask; Manifold = AMM price (not directly
  comparable)

**Why I'd skip:**
- Play-money has no skin-in-game → noise dominates signal
- Most academically-studied differential is Polymarket vs Pinnacle
  (both real-money, both sharp), not Polymarket vs Manifold
- The "wisdom of play-money crowds" effect isn't reliable enough at
  the scales Manifold operates (small)

**Verdict:** Skip. Weak prior, small Manifold volumes, AMM pricing
adjustment is annoying.

---

### EDGE-I: Google Trends Leading Indicator

**Claim:** Search interest for political candidates / topics leads
prediction-market price changes by 12-72 hours.

**Evidence (mixed at best):**
- 2008 US Congress: only 33% of races correctly predicted
- 2010 US Congress: only 39%
- Some single-election studies show 100% — selection bias likely

**Why I'd skip:**
- Google Trends API is rate-limited and has 24-72h lag for sub-region data
- Sentiment trackers built on this consistently underperform direct
  poll aggregators (538, Polymarket itself)
- Prediction-market traders already track this — alpha is gone

**Verdict:** Skip.

---

### EDGE-J: Wikipedia Page Views

**Claim:** Page views spike before/during news events. Polymarket
catches up with delay.

**Verdict:** Skip — Wikipedia data is daily-resolution, not fast enough
for short-duration markets. For long-duration markets, the news that
spikes Wiki ALSO spikes Polymarket within minutes.

---

### EDGE-K: Time-of-Day / Off-Hours Quoting Inefficiency

**Claim:** During off-hours (Asia/Africa nighttime), spreads on
Polymarket widen and high-quality liquidity drops. MM during these
windows captures wider spreads at lower competition.

**Verdict:** This is essentially **already captured by EDGE-A
(Liquidity Rewards MM)**. The reward share formula rewards being on
the book during sparse periods. No need for a separate strategy.

---

### EDGE-L: NCAA Bracket / Tournament Path Arb

**Claim:** "Team X reaches the final" vs "Team X reaches the
semifinal" form a logical chain — final ⊂ semifinal. If
P(final) > P(semifinal) on Polymarket, that's logical arb.

**Evidence:**
- arxiv 2508.03474: cross-market combinatorial arb extracted **only
  $95K** in a full year. The opportunities exist but execution depth
  is tiny (14.8 share avg).
- Most NBA tournament markets pair-priced consistently by a few pro
  bots already.

**Verdict:** Defer / skip. Capacity too small for the dev time.

---

## Tier 4 — Considered, intentionally not pursued

| Idea | Reason for skip |
|------|-----------------|
| MEV / atomic frontrunning | Polymarket is on Polygon; orders aren't broadcast to a public mempool (CLOB exchange model). No atomic MEV path. |
| Insider information | Not legal, not the moat we want. |
| Pump-and-dump on illiquid markets | Self-defeating + ethical issues. |
| LLM-in-loop directional trading | Tested in research mode; too slow + expensive for 200ms tick. Cold-path only via daily review. |
| Custom LP on AMM legs | Polymarket is CLOB, no AMM for us to LP into. |
| Token launch sniping | Polymarket doesn't launch new tokens; this is a different game. |

---

## Roadmap — what to actually build, in what order

### Sprint 11 (next): EDGE-A Phase 0 + Phase 1 — Liquidity Rewards
**Time:** 1-2 weeks
**Why first:** Highest expected yield, validates a major thesis,
expands our infrastructure (cancel-by-market, postOrders bulk, depth-2
quoting) which we'll need anyway.

### Sprint 12: EDGE-C — Pinnacle devig
**Time:** 4-5 days
**Why second:** Cheapest to build (sidecar pattern already established),
sports markets are big on Polymarket.

### Sprint 13: EDGE-A live ramp + 2-week soak
**Time:** 2 weeks of running, code ~1 week
**Goal:** Prove a single market making 0.5-3% daily yield consistently.

### Sprint 14: EDGE-D — Whale follow
**Time:** 1-2 weeks
**Why fourth:** Higher conviction than D's tier on paper, but
implementation is heavier (Polygon WebSocket + tx decoding) and edge
likely declines as we follow. Only worth it once we have good live
infra.

### Sprint 15+: EDGE-E (hibernating), EDGE-F (Aave float), bug fixing,
operations.

---

## Honest assessment

**The single biggest finding:** EDGE-A (Liquidity Rewards MM) is
*structurally* better than every other strategy. It's not betting on
direction at all — it's collecting rent for providing a service
Polymarket directly pays for. The expected drawdown profile is much
flatter than directional strategies. Even at 50% haircut from
adverse selection, the yield is multiples of any other angle.

**The single most-overrated idea:** Funding rate / sentiment overlays.
Every crypto trader watches funding. If alpha existed, it'd be
arbed away by perp markets first.

**The single most-validated existing strategy:** Strategy 2 (Event
Arb). The arxiv paper saying $23.6M was extracted from within-market
rebalancing in one year on Polymarket is the strongest external
validation we have for any strategy in the bot.

**What separates winners from losers:** Top 10 wallets captured 17.7%
of all arb profit ($7M of $39.6M); top wallet alone captured $2M from
4,049 trades. That's $493/trade — which is exactly the per-trade size
range we operate in. The skilled retail-bot tier of operators IS
making real money on Polymarket. The alpha is real. Whether we
execute well enough to capture it is the only open question.
