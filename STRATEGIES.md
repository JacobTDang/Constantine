# Multi-Strategy Build Plan

Five strategies, each phased with explicit gates. Sequenced by ROI-of-effort.

```
  Strategy                        Capital  Effort   Time-to-MVP   Edge confidence
  ─────────────────────────────────────────────────────────────────────────────
1. Player Props (50/50 default)   $200+    Low      1 week        High
2. Event Sum-of-YES Arb           $500+    Medium   1-2 weeks     High (mechanical)
3. Cross-Platform Arb (Kalshi)    $1K each Medium   2-3 weeks     High
4. Geopolitical Fade              $200+    Low-Mid  1 week        Medium
5. News-Event Reaction            $500+    High     4-6 weeks     Medium
```

## Sequencing rule

**Do them in order 1 → 2 → 4 → 3 → 5.** Reasoning:
- 1 + 2 + 4 reuse 90% of the existing bot — same execution path, just different signals.
- 3 (cross-platform) needs a second-venue execution adapter — bigger lift, but highest absolute ROI once built.
- 5 (news) needs paid feeds + NLP infra — biggest capital outlay, defer until edges are proven.

Each strategy ships independently. Don't start strategy N+1 until N's Phase 2 (production hardening) is green.

---

# Shared Infrastructure (build first)

Things multiple strategies need. Build incrementally as each strategy demands them.

| Module | Purpose | Strategies that use it |
|---|---|---|
| `signals/external/` (NEW) | Pluggable signal sources beyond oracle/intramarket | 1, 4, 5 |
| `streams/external/stats.rs` (NEW) | Public stats fetchers (basketball-reference, NBA API) | 1 |
| `streams/external/news.rs` (NEW) | News firehose (any provider) | 4, 5 |
| `streams/external/kalshi.rs` (NEW) | Kalshi market + book streamer | 3 |
| `execution/venue.rs` (NEW) | Trait abstracting Polymarket / Kalshi / etc. | 3 |
| `signals/matching.rs` (NEW) | Cross-platform market matcher | 3, 5 |
| `storage/positions.rs` (existing) | Already supports multi-market via market_id | all |
| `risk/limits.rs` (existing) | Already gates kelly + exposure | all |

**No new infrastructure until a strategy demands it.** Don't pre-build.

---

# Strategy 1: Player Props at 50/50 default

## Edge thesis

Newly-listed NBA/NFL/MLB player prop markets sit at 0.50 / 0.50 with $0 volume and 98¢ spread until the first trade arrives. If we have a competent player projection (rolling stats × opponent defensive matchup), we can buy at 0.50 with 5-15¢ of true edge before MMs tighten the book.

Confirmed via API on 2026-05-04: ~10+ markets like "KAT Rebounds O/U 11.5" sitting at exact 0.50 with $2.3K liquidity each.

## Phase 0 — Validate edge (NO CODE)

**Goal:** prove the edge exists in retrospect before writing a single line.

Tasks:
1. Pull last 30 days of NBA player prop markets from Polymarket (use `closed=true` filter on Gamma API).
2. For each, capture: question, line, settled outcome, opening price, opening liquidity.
3. Compute: of markets that opened at 0.50 default, what was the actual win rate of OVER vs UNDER once the bet was placed?
4. **Pass criteria:** OVER+UNDER win-rate spread > 8% for low-liquidity opens. (We need > fee floor of 2.8% × 2 legs + slippage = ~6%.)
5. **Fail criteria:** spread < 4%. Don't proceed; the 50/50 default is closer to fair than I thought.

**Output:** a `data/research/player_props_backtest.csv` and a 1-page write-up. Either green-light Phase 1, or kill the strategy.

## Phase 1 — MVP (1 week)

**Goal:** bot trades 1 sport (NBA), 1 prop type (rebounds), $5/trade.

### 1.1 — Stats fetcher
- New crate `streams/external/nba_stats.rs`
- Pull from `nba_api` (Python wrapper) or `https://stats.nba.com/stats/`. Free, no auth.
- Cache in `data/stats/<player_id>/<season>.json`. Refresh nightly.

### 1.2 — Projection model
- New module `signals/external/player_props.rs`
- For each (player, stat, line):
  - Compute season-to-date avg + variance for that stat
  - Adjust for opponent's defensive rank against that position
  - Output `p_over: f64`
- Start dumb: linear regression on 5 features. Don't reach for ML until the dumb model is profitable.

### 1.3 — Polymarket prop discovery
- Extend `streams/polymarket.rs` to also fetch player prop markets (different tag than BTC binary).
- Filter to NBA rebounds initially. Match each market's question text → (player, stat, line) tuple.

### 1.4 — Signal trigger
- New `SignalDecision::PlayerProp` variant.
- Fire when: `p_over - 0.5 > min_edge` AND `bestAsk == 0.50` AND `liquidity < $5K` (we're early).
- Wire into existing `evaluate_all_markets`.

### 1.5 — Integration
- Reuse 100% of existing execution path. Sign-on-demand at Kelly bet (already do this).
- Settlement: existing `settlement_monitor_loop` works as-is — Polymarket resolves via UMA, our settle picks it up.

**Phase 1 done criteria:**
- 50+ NBA prop trades placed in DRY_RUN mode over a week
- Realised paper P&L > 0 at the end
- No false fires (bot never fired on a non-prop market)

## Phase 2 — Production hardening (3-5 days)

1. **Per-prop-type expansion:** add assists, points, blocks, steals. Each gets its own model coefficient.
2. **Injury data integration:** scrape ESPN injury report. Suppress signal if player is questionable/out.
3. **Late-news suppression:** if player is benched/limited 30 min before tipoff, fade or skip.
4. **Capacity caps:** never bet > $5 on a single prop (these are tiny markets). Total exposure cap $50/game.
5. **Settlement audit:** add a daily report comparing our model's `p_over` to actual outcomes — calibration check.

**Phase 2 done criteria:**
- 200+ trades in DRY_RUN, then 50+ in LIVE at $5/trade
- Calibration gap (predicted vs actual win rate) < 5%
- ROI after fees > 0% over 30 days

## Phase 3 — Scale (ongoing)

- Add NFL player props (passing yards, rushing yards, receptions). Different stat sources.
- Add MLB (hits, strikeouts, home runs).
- Bump bet size to $20-30 per prop as confidence grows.
- Track per-sport ROI separately. Cull whichever underperforms.

**Phase 3 done criteria:**
- $500+ realised P&L
- Daily Sharpe > 1.0 over 60 days
- Strategy survives a losing week without panic-tweaking

## Risks specific to Strategy 1

- **MM tightens books** as our flow gets noticed → edge compresses. Mitigation: rotate which props we trade, keep size small.
- **Player projection bug** → we systematically lose. Mitigation: backtest weekly, kill switch on calibration drift.
- **Polymarket changes prop market format** → discovery breaks. Mitigation: question-text parser is brittle; add structured-format detection.

---

# Strategy 2: Multi-Candidate Event Sum-of-YES Arb

## Edge thesis

For any event with N mutually-exclusive outcomes (election, tournament, awards), the sum of YES prices SHOULD be 100%. Polymarket retail consistently overprices long-shots, so the sum drifts to 105-130%. Capturing the surplus is mechanical.

Confirmed: Democratic 2028 Nominee event currently sums to **127.1% across 40+ candidates**.

## Phase 0 — Validate edge (NO CODE)

**Goal:** prove the captureable edge (after fees + spread) is meaningfully > 0.

Tasks:
1. Snapshot 5 multi-outcome events on Polymarket: pull all candidates' bestBid + bestAsk + liquidity.
2. For each event, compute:
   - `sum_of_asks` (cost to buy ALL YES — buying-side arb if < 100%)
   - `sum_of_bids` (revenue from selling NO on all — selling-side arb if > 100%)
   - **captureable_edge = sum_of_bids - 1.0 - (fees × N legs)**
3. Filter to events where capturable_edge > 3% with realistic liquidity (>$1K bid depth).
4. **Pass criteria:** ≥ 2 events show captureable_edge > 3% sustained over 24h.
5. **Fail criteria:** all "edges" disappear when you include fees + crossing the spread on each leg.

**Output:** `data/research/event_arb_snapshots.csv`. Green-light or kill.

## Phase 1 — MVP (1 week)

### 2.1 — Event scanner
- New module `signals/external/event_arb.rs`
- Hit Gamma `events?closed=false&limit=200` every 60s
- For each event with ≥ 5 markets, compute sum_of_bids
- If sum > 105% AND total liquidity > $5K → flag as candidate

### 2.2 — Portfolio sizer
- For a flagged event, decide WHICH legs to take (not all 40 — the long-shots have wider effective spreads)
- Algorithm: rank legs by `(bid - 0) / (ask - bid)` (best edge per spread). Take top K legs that together fit in `max_bet × N` budget.
- Sell NO on each (or equivalently, since this is a binary, you SELL the YES you don't own — same as a short).

Actually wait, on Polymarket you can't short YES directly. You **buy NO** which is the opposite leg. So:
- For each long-shot candidate priced at 5¢: buy NO at 95¢ (probably bid is at 94¢)
- If candidate doesn't win (high probability), NO pays $1, you profit 6¢
- Net cost per leg: 95¢, net revenue if loses: $1 → 5¢ profit
- Across 20 candidates: 20 × 5¢ = $1.00 guaranteed (minus the 1 winner who costs you $0.94 not paid)

Simpler framing: if sum_of_asks_for_NO across all candidates < (N-1), arb exists.

### 2.3 — Multi-leg execution
- Reuse intramarket multi-leg pattern (`execute_intramarket` in executor.rs)
- Generalize to N legs: tokio::join! or sequential with rollback
- Risk: partial fill → some legs filled at planned price, others didn't → directional exposure
- Mitigation: place all legs as resting orders at limit prices; let them fill naturally; if not all filled within 5 minutes, cancel remaining

### 2.4 — Position tracking
- Each leg gets its own PositionStore entry
- Tag with `event_id` so we can group P&L
- New ReconcileReport variant tracks "event_arb" positions specifically

**Phase 1 done criteria:**
- 3+ event arbs identified and traded in DRY_RUN
- Settlement reconciles correctly when event resolves (one winner, all NO bets pay out except for the winner's leg)

## Phase 2 — Production hardening (1 week)

1. **Long-hold capital management:** events resolve in months. Add `max_long_hold_exposure` separate from `max_open_exposure`. Don't tie up > 30% of bankroll in 6+ month positions.
2. **Re-check daily:** sum_of_asks can shift. Re-check our open events; if edge has flipped, exit early via SELL on the legs we own.
3. **New-candidate guardrails:** Polymarket sometimes adds candidates mid-event. Our N-leg portfolio breaks if a new long-shot appears at 8¢ that we didn't short. Detection: compare leg count daily; alert on change.
4. **Resolution timing:** UMA disputes can delay resolution. Test reconciliation handles 2+ week delays.

**Phase 2 done criteria:**
- 5+ events traded LIVE at $50-100 total exposure each
- All resolved cleanly, P&L matches expected within 2%

## Phase 3 — Scale (ongoing)

- Run the scanner on ALL multi-outcome events daily (sports tournaments, awards seasons, etc.)
- Bump per-event exposure to $200-500 as bankroll grows
- Cross-event correlation: don't take 5 simultaneous arbs that all settle the same day (settlement-day risk concentration)

## Risks specific to Strategy 2

- **Phantom arbs:** "Person" placeholder markets at $1.00 ask inflate sums. Filter to markets with non-zero 24h volume and liquidity > $500.
- **Capital lockup:** 6-month holds tie up money. Hard cap on long-hold positions.
- **Event cancellation:** rare but happens (election postponed). Markets get voided; everyone gets refunded. Net-zero outcome but capital is freed late.
- **Fee underestimate:** N legs × 1.8% taker = 36% fees on a 20-leg arb if you cross every spread. Has to fit as MAKER (resting orders). Maker gets 0% fees on Polymarket. Critical — this strategy ONLY works as a maker.

---

# Strategy 3: Cross-Platform Arb (Polymarket vs Kalshi)

## Edge thesis

The same political/economic/sports event prices differently on Polymarket vs Kalshi. Documented 2-8% gaps that persist for hours. Buy YES on the cheaper venue, buy NO (or YES on opposite outcome) on the more expensive venue. Both legs profit at resolution.

## Phase 0 — Validate edge + Kalshi access (NO CODE)

Tasks:
1. **Open a Kalshi account.** US-only, KYC required, takes 1-3 days. Fund with $1K minimum. *This is a precondition; nothing else can happen until this is done.*
2. Manually pick 10 markets that exist on both platforms (US politics is the easiest overlap). Snapshot their prices for 1 week.
3. Compute fee-adjusted spread: `polymarket_yes - (1 - kalshi_yes)` after both fees.
4. **Pass criteria:** ≥ 3 markets show > 2% sustained gap.
5. **Fail criteria:** all gaps are within fees → no real arb.

## Phase 1 — MVP (2-3 weeks)

### 3.1 — Kalshi venue adapter
- New module `streams/external/kalshi.rs`
- WebSocket subscription to Kalshi orderbook updates
- HTTP client for orders (similar to our ClobClient): `submit_order`, `list_open_orders`, `list_trades`
- Auth: Kalshi uses RSA-PSS signed requests (different from Polymarket's HMAC). Library: `rsa` crate.

### 3.2 — Venue trait abstraction
- New `execution/venue.rs` trait:
  ```rust
  pub trait Venue {
      async fn submit(&self, order: &Order) -> Result<SubmitOutcome>;
      async fn list_open(&self) -> Result<Vec<OpenOrder>>;
      async fn list_trades(&self) -> Result<Vec<Trade>>;
  }
  ```
- Implement for both Polymarket and Kalshi
- Existing `ClobClient` migrates behind this trait

### 3.3 — Market matcher
- New `signals/matching.rs`
- Given a Polymarket market, find the equivalent Kalshi market
- Match by topic + resolution date + question semantics
- Initial approach: hard-coded match list (e.g., "US presidential winner 2028" Polymarket = "PRES-2028" Kalshi). Don't try fuzzy matching first; build a yaml of explicit matches.

### 3.4 — Cross-arb signal
- New `SignalDecision::CrossArb { polymarket_market, kalshi_market, direction }`
- Fires when fee-adjusted spread > threshold
- Direction = which platform to buy YES on, which to sell

### 3.5 — Two-leg execution with rollback
- Submit both legs concurrently (tokio::join!)
- If one fills and other rejects: log alert, attempt to UNWIND the filled leg immediately
- Risk: counter-leg unwind may be at worse price → small loss
- Mitigation: the threshold for firing must include "expected unwind cost on rejection" as a buffer

**Phase 1 done criteria:**
- 5+ cross-arbs executed in DRY_RUN against both venues' DRY_RUN modes
- 2+ executed live at $20-50 per leg
- All settled with matching P&L per leg

## Phase 2 — Production hardening (1 week)

1. **Capital management across venues:** Polymarket capital and Kalshi capital are separate pools. If Polymarket leg fills but Kalshi capital is busy, we can't take the arb. Add a "minimum free capital on each venue" floor.
2. **Withdrawal mechanics:** post-resolution, you have to actually withdraw from each venue. Polymarket: 24-72h. Kalshi: 1-3 business days. Don't redeploy capital until withdrawn.
3. **Resolution timing mismatch:** Polymarket resolves via UMA (can be days late). Kalshi resolves via their internal team (usually faster). Position can be P&L-decided on Kalshi but still open on Polymarket — handle this in PositionStore.
4. **Account safety:** spread positions across both. Alerts if one venue locks our account (rare but happens).

**Phase 2 done criteria:**
- $200+ realised P&L from cross-arb alone
- 0 directional exposures left over from failed two-leg fills
- Reconciliation works across both venues' position lists

## Phase 3 — Scale (ongoing)

- Add more venues: Manifold (play-money — useful as price discovery only), Limitless, sport-specific books
- Auto-discover market matches via embedding similarity (FAISS over question texts)
- Higher per-arb size as bankroll grows

## Risks specific to Strategy 3

- **Account closure:** Kalshi can close US accounts that look algorithmic. Trade conservatively, no obvious bot patterns. (This risk is real.)
- **Capital fragmentation:** $1K on each venue means smaller absolute bets per venue.
- **Settlement asymmetry:** if one venue voids the market and the other doesn't, you take the full loss on the non-voided side.
- **Wire-format drift:** Kalshi changes their API. Allocate maintenance time monthly.

---

# Strategy 4: Geopolitical Retail-Fade

## Edge thesis

Markets like "Iran closes Strait of Hormuz by date X" or "Russia-Ukraine ceasefire by date Y" spike on news headlines and fade as the news ages. Retail panic-buys YES; the actual base rate (regime falls don't happen on 30-day timelines) is much lower. Fade the spike, hold to resolution.

## Phase 0 — Validate edge (NO CODE)

Tasks:
1. Pick 5 historical geopolitical Polymarket markets that resolved NO in the last 6 months.
2. Plot their price history. Identify spikes (>20% one-day move).
3. Compute: if you bought NO at the spike peak, what was the holding-period return to resolution?
4. **Pass criteria:** average return > 10% on spike-fade trades.
5. **Fail criteria:** spikes are usually correct (predictive of YES outcome) — don't fade.

## Phase 1 — MVP (1 week)

### 4.1 — Spike detector
- Extend signal evaluator: track 24h price change per geopolitical market.
- Fire when single-day move > 20% AND market is in geopolitical category AND days-to-resolution > 14.

### 4.2 — Base-rate model
- Tiny lookup table: `category → base_rate`
  - "Regime fall in N days" → 2% base
  - "Major military intervention in N days" → 5%
  - "Ceasefire in N days" → 10%
- Fire when current YES price > 2× base rate.

### 4.3 — Position management
- Hold-to-resolution by default
- Hard cap: $30 per spike
- Max 5 concurrent geopolitical positions (correlation risk: same news event spikes multiple markets)

**Phase 1 done criteria:**
- 5+ fade trades in DRY_RUN over 30 days
- Average held to resolution

## Phase 2 — Production (3-5 days)

1. Refine base rates with real data (look at 12 months of geopolitical resolutions)
2. News-aware suppression: if real escalation happens (verified via news API), skip the fade
3. Profit-taking: if YES price drops below base rate before resolution, close early

## Phase 3 — Scale

- Expand to non-geopolitical retail-emotion markets (celebrity deaths, scandal markets)
- Manual oversight remains — these markets need human judgment

## Risks specific to Strategy 4

- **Genuine escalation:** the 1-in-20 case where the spike is RIGHT. We lose 100% on that trade. Sizing must absorb this.
- **Correlated drawdown:** if WW3 actually starts, every fade we hold goes to YES. Total geopolitical exposure cap is critical.
- **Slow resolution:** geopolitical markets often resolve long after the news fades. Capital lockup similar to event arb.

---

# Strategy 5: News-Event Reaction (most expensive, defer)

## Edge thesis

Polymarket prices lag breaking news by 5-30 seconds. A bot with paid news firehose + NLP classifier + low-latency execution can capture the move before retail (and even before some MMs) react.

## Phase 0 — Validate edge + budget (NO CODE)

1. Pick news events from the last 30 days that should have moved Polymarket prices (FOMC, CPI, major political events, crypto news).
2. For each, measure: time from news drop → time when Polymarket price stabilized at new level. The window between is your edge window.
3. **Pass criteria:** edge window > 5 seconds, price moves > 3% (covers fees).
4. **Decide news source budget:**
   - Free APIs (NewsAPI, Reuters free): 5-15 min lag → useless
   - Paid mid-tier (Benzinga Pro $177/mo, Polygon.io news $249/mo): 1-30 sec lag → maybe usable
   - Premium (Bloomberg Terminal $24K/yr): sub-second → only for serious operators

Pick a budget tier or kill the strategy.

## Phase 1 — MVP (4-6 weeks total)

### 5.1 — News pipeline
- WebSocket subscriber to chosen news API
- Buffer + dedupe headlines

### 5.2 — NLP classifier
- Each headline → relevant Polymarket market(s) + direction
- Initial: rules-based (keyword matching + market-name fuzzy match)
- Better: small embedding model (sentence-transformers) → market matcher

### 5.3 — Pre-positioned signing
- Sign orders BEFORE news drops, indexed by market
- On news arrival, just submit the matching pre-signed order
- Latency: news → submit ≈ 100-300ms (achievable on AWS us-east-1)

### 5.4 — Execution
- AWS us-east-1 deployment (required for this strategy)
- Reuse existing ClobClient with upgraded latency budget

**Phase 1 done criteria:**
- 20+ news-triggered trades over 30 days
- Average time-from-news-to-submit < 500ms
- Realised P&L > 0 after news fees

## Phase 2 — Production (2-3 weeks)

1. Better NLP: fine-tune classifier on news → market mapping
2. False-positive suppression: don't trade on rumor headlines, only confirmed news
3. Multi-market coordination: one news event can move 5+ markets simultaneously
4. Backtest on captured history: did our classifier correctly identify the right markets?

## Phase 3 — Scale

- Add Twitter firehose (X API enterprise tier, $42K/yr for full firehose — probably too expensive)
- Earnings calls / SEC filings → political markets
- Crypto-specific: on-chain events → crypto Polymarket markets

## Risks specific to Strategy 5

- **Fee burn:** news-triggered moves are small (1-5%). Fees alone can kill.
- **False fires:** NLP misclassifies a headline → trade on noise. Calibrate thresholds aggressively.
- **MM speed:** if MMs widen spreads on news, you submit at a worse price. Mitigation: limit-only orders that won't take the worst spread.
- **News-source contracts:** API quotas, rate limits, ToS violations for re-distribution.

---

# Master Sequencing

```
Week 1-2:    Strategy 1 Phase 0 + Phase 1
             Strategy 2 Phase 0 (parallel — both can run during prop season)

Week 3-4:    Strategy 1 Phase 2 production
             Strategy 2 Phase 1 MVP
             Strategy 4 Phase 0 (data analysis only, no code yet)

Week 5-6:    Strategy 2 Phase 2
             Strategy 4 Phase 1
             Strategy 3 Phase 0 (Kalshi account opens — start now if interested)

Week 7-9:    Strategy 3 Phase 1 (Kalshi adapter, takes longest)
             Strategies 1-2 monitoring + tuning

Week 10-12:  Strategy 3 Phase 2
             Strategy 5 evaluated only if 1-3 are profitable
```

**Decision gate at end of each phase:** look at realised P&L. If a strategy is below break-even after 30+ days of live trading, kill it. Don't sink more time.

---

# Risk Register (cross-strategy)

| Risk | Affects | Mitigation |
|---|---|---|
| Polymarket account locked | All | Diversify with Kalshi (Strategy 3); document everything for appeals |
| Bot bug → wrong-side trades | All | Mandatory: each strategy ships with strategy-specific tests + 30 days DRY_RUN before LIVE |
| Capital concentration | All | Hard cap: no single strategy gets > 60% of bankroll |
| Strategy correlation | 4, 5 | Geopolitical fade + news reaction can both go wrong on same day; cap combined exposure |
| Edge decay | 1, 2 | Quarterly review of realised vs expected edge; cull underperforming substrategies |
| Vendor lock-in | 5 | Don't build NLP on a single news provider; abstract behind a trait |

---

# Universal "Done" Criteria (for any strategy to graduate to LIVE)

1. Backtested on ≥ 30 days of historical data with positive expected P&L after fees
2. Ran in DRY_RUN for ≥ 14 days with zero false fires
3. Manual review of every trade signal during DRY_RUN (humans verify the signal makes sense)
4. Fee model includes BOTH legs (or N legs for multi-leg) of the strategy
5. Position-tracking + settlement reconciliation works end-to-end
6. Has a kill switch separate from the global one (per-strategy halt)

If a strategy can't tick all 6, it doesn't trade real money.

---

# What to start with TODAY

**Strategy 1 Phase 0** (Player Props validation):
1. Write a script that fetches all closed NBA prop markets from Polymarket Gamma API
2. Compute the win rate for OVER vs UNDER on markets that opened at exactly 0.50
3. Either green-light Phase 1 or move to Strategy 2

This is 1-2 hours of work, requires no new infrastructure, and tells us if the headline opportunity I found in research is real.

If green: build Strategy 1 over the next week.
If red: skip to Strategy 2 Phase 0 (the event sum-of-YES arb), which has independent validation.

Either way, you're not committing to anything until the data confirms the edge.
