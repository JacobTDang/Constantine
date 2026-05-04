# Realistic Gap Review

Critical review of what could prevent this bot from making money. Honest
take, not a victory lap.

## The biggest question (read first)

**Does this bot actually have edge?**

Two signals fire today:

1. **Intramarket arb** (yes_ask + no_ask < $1 after fees). On a Polymarket
   BTC market with active market makers, real intramarket arbs are eaten
   in milliseconds by professional MM bots. What you see as a "fireable"
   arb in our logs is more often *stale book data on our side* than a
   real opportunity. **Realistic expectation: rare, mostly false signals.**

2. **Oracle arb** — fires when our lognormal fair value diverges from the
   market price by > 4¢. Polymarket's MMs use better fair-value models
   than ours, often colocated with HFT-grade infra. The H8 backtest
   already showed our ML model fails the Brier gate (better than coin
   flip is the bar, and we don't clear it). The lognormal model in
   production is even simpler than the ML one we trained.
   **Realistic expectation: many fires that don't beat fees.**

3. **The bot is well-built. Whether it's profitable depends entirely on
   whether these signals have edge in practice.** Nothing in the code can
   manufacture edge that isn't there.

**Mitigation:** observe-mode for at least a week before flipping execution
on. Look for sustained `realised_pnl_usd > 0` with > 100 settled
positions. If it's barely above zero or negative, don't go live.

---

## Critical gaps (could cause direct losses)

### G1: Settlement uses LATEST chainlink, not close-time chainlink

`storage/settlement.rs:102` reads `state.chainlink_price` (the most recent
oracle reading) when settling, but that may be 30-60 seconds *after* the
window's `close_time_ms`. For a fast-moving market, this could attribute
the *wrong outcome direction* — recording a "win" as a "loss" or vice versa.

**Why it matters:** Wrong P&L attribution → wrong slippage feedback →
cost-model auto-tunes in the wrong direction → bot makes worse trades.

**Fix:** fetch the chainlink price AT close_time_ms via historical RPC
call (Alchemy supports `eth_call` at a specific block number). Or, look
back through chainlink_price update history if we cache enough of it.

**Status:** unfixed. Architectural change.

### G2: USDC allowance can run out silently

The user sets allowance once via UI. If allowance is `$X` and the bot
spends `$X` over time, every subsequent order rejects with the same
opaque error. The bot would log dozens of `Rejected` outcomes but keep
trying — burning pool slots on doomed orders.

**Fix:** watchdog periodically calls `USDC.allowance(funder, exchange)`
via RPC and trips kill switch when balance < some threshold (e.g.
$1000 left).

**Status:** unfixed but documented. **Applying fix in this commit.**

### G3: Market discovery runs every 5 minutes

For 5-MINUTE markets! By the time we discover a new market, it's already
60-90 seconds into its 300-second life. We've missed the fresh-window
strike capture and probably the highest-edge trading window.

**Fix:** drop discovery interval to 30s. Polymarket's Gamma API can
handle the load.

**Status:** **applying fix in this commit.**

### G4: Pool prices stale within a window

We pre-sign orders at midpoint ± 15 cents at window OPEN. By minute 4
of a 5-minute window, the midpoint may have shifted 20+ cents. A signal
firing at the new price hits a "no pre-signed order" skip, and we miss
the trade. Every signal that fires later in the window is at risk.

**Fix:** detect midpoint drift > 5 cents → repopulate pool with fresh
orders at the new center. Tracking required.

**Status:** unfixed. Medium-complexity. Documented for follow-up.

### G5: Pool sizing fixed at max_bet, not Kelly bet

Pre-signed orders are sized for `max_bet_dollars` ($30). When the
strategy decides to bet $5 (kelly), we still submit a $30 order. We
either over-commit or fail to submit at all. **This means kelly sizing
is effectively bypassed in production.**

**Fix:** sign multiple bet-size variants per tick, OR re-sign at submit
time (kills the "pre-sign for low latency" idea). Architectural.

**Status:** unfixed. **High-impact, requires design discussion.**

---

## Important gaps (degrade performance, not catastrophic)

### G6: 500ms tick cadence is too slow

Signal eval ticks at 500ms. Real edge windows in BTC are < 100ms. By
the time we tick, the price has moved. Our 500ms-old fair value is
fighting against the MM's 1ms-old fair value.

**Fix:** drop to 100ms tick. CPU is cheap; we have 20 cores.

**Status:** **applying fix in this commit (drop runner tick to 200ms).**

### G7: Static bankroll doesn't update with realised P&L

`Config::bankroll = $1500` is constant. After losing $200, kelly still
sizes off $1500, not $1300. After winning $300, we don't grow bets to
match. **The bot can't compound.**

**Fix:** `RiskLimits` exposes `current_bankroll()` = config_bankroll +
realised_pnl. Kelly reads from there.

**Status:** unfixed. Medium-complexity. Documented.

### G8: Settlement-monitor cooldown vs runner cooldown overlap

`runner_cfg.min_market_cooldown_secs = 60` prevents stacking trades on
the same market. But the FIRST trade also blocks the second trade in
the same window. For a 5-minute window with a 60s cooldown, we can
trade at most 5 times — but we usually only want to trade ONCE per
window anyway, so this is conservative. Fine for safety; could be
loosened.

**Fix:** make per-market cooldown configurable per market type
(5min: 240s, 15min: 180s, hourly: 120s).

**Status:** working as designed for now. Not urgent.

### G9: No rate-limit awareness

`runner_cfg.max_concurrent_submits = 8`. Polymarket's CLOB API rate
limits aren't fully documented but 429 responses can occur under load.
Our retry logic doesn't honor `Retry-After` headers.

**Fix:** parse `Retry-After` from 429 responses; back off accordingly.

**Status:** unfixed. Low-impact at our trade volume.

### G10: No baseline comparison in observe report

`observe_report.py` shows "win rate 56%" — but is that better than
random? Need to compare against baseline (always-bet-Up, always-bet-NO,
random) to know if we have edge.

**Fix:** add baseline comparisons to the report.

**Status:** unfixed. **Applying fix in this commit.**

---

## Operational gaps (not profit-blocking, but risk-shaping)

### G11: Kill switch never auto-resets even on data recovery

Watchdog trips kill switch when chainlink stream is stale > 90s. If
chainlink comes back at 91s, we stay tripped until manual restart.
Result: we miss the rest of the trading day.

**Fix:** for data-loss trips ONLY (not loss-limit), auto-reset after
N seconds of healthy data (e.g. 5min).

**Status:** unfixed. Medium-complexity. Documented.

### G12: No NTP / clock drift check

Settlement timing relies on `chrono::Utc::now()`. A 30-second clock
drift could make us settle markets 30s too early/late, misattributing
outcomes. The watchdog doesn't check clock health.

**Fix:** preflight already could check; runtime watchdog could ping a
known time source (Alchemy block timestamp, Polymarket /time endpoint).

**Status:** unfixed. Low-likelihood on a server with NTP enabled.

### G13: No startup reconciliation against Polymarket's view

On bot restart, we replay positions.jsonl. But Polymarket may have
filled / cancelled / settled orders we didn't see. The `Submitted`
state in our log could be stale.

**Fix:** on startup, query Polymarket's `/orders` and `/trades`
endpoints for our address, reconcile against the local store.

**Status:** unfixed. Medium-complexity. **Important before going live
with real money.**

---

## Fixes applied in this commit

- G3: market discovery cadence 300s → 30s
- G6: runner tick 500ms → 200ms
- G2: USDC allowance monitoring in the watchdog
- G10: baseline comparison in observe_report.py

## Fixes NOT applied (need user decision or architectural work)

- G1: settlement timing — needs historical RPC pattern
- G4: pool re-population on midpoint drift
- G5: kelly-aware order sizing
- G7: dynamic bankroll
- G11: kill switch auto-reset
- G13: startup reconciliation against Polymarket

## Honest summary

The bot is engineered well. The remaining gaps are real but most won't
cause catastrophic losses — they'll just *prevent profit* in subtle
ways:

- Wrong outcome attribution (G1) → wrong P&L → wrong cost model
- Stale pool prices (G4) → missed trades
- Static kelly (G5) → all-or-nothing sizing
- Static bankroll (G7) → no compounding

**Most importantly:** the signal layer's edge is unproven. Run observe
mode for a week and look at the numbers before risking capital. If the
numbers say "no edge," no amount of plumbing fixes will make this
profitable.
