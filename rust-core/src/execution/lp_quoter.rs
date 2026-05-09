// EDGE-A — Liquidity Rewards market making quoter.
//
// Polymarket pays makers a daily reward proportional to the score
// formula S(v, s) = ((v-s)/v)^2, where v = market's max_spread (cents)
// and s = our quote's distance from mid. Two-sided quoting is required
// for the score boost; single-sided is divided by c=3.0.
//
// Architecture:
//   - LpQuoter owns one market: it keeps state per-market (latest book,
//     latest quote prices, last submit timestamps) and runs a refresh
//     cadence (default 5s) that re-quotes when:
//       a) target prices have moved by > 1 cent vs last submit, OR
//       b) our last submit is older than the max-stale window, OR
//       c) we just discovered the inventory cap was breached.
//   - Quoting works by submitting BUY YES at the bid AND BUY NO at the
//     "ask" (which on Polymarket is just BUY NO at price 1 - target_yes_ask,
//     since YES + NO must = 1).
//   - Bulk submit: both sides go in one /orders POST when possible.
//   - Cancel-by-market is used to clear the book at refresh time so the
//     new quotes don't compete with stale ones.
//
// What this layer does NOT do (deferred until live data informs it):
//   - Skew quotes for inventory imbalance (just pulls the over-inventoried
//     side entirely instead of skewing — simpler, safer first iteration)
//   - Adaptive max-stale based on observed mid-volatility
//   - Multi-level depth (only top-of-book quotes per side)
//   - Direct WebSocket subscription (polls /book at refresh cadence)

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::RwLock;

use crate::execution::clob::{
    BookSnapshot, ClobClient, OrderType, RewardMarket,
};
use crate::execution::executor::SigningParams;
use crate::execution::orders::{
    hex_to_address, private_key_to_address, u256_from_dec, SignatureType,
};
use crate::execution::presign::{sign_buy_now, BuyOrderInputs};
use crate::risk::limits::{RiskLimits, Strategy};
use crate::storage::PositionStore;

/// Config for the quoter. Defaults are conservative / capital-preserving.
#[derive(Debug, Clone)]
pub struct LpQuoterConfig {
    /// USD size per side, per market. Must satisfy market's min_size.
    pub quote_size_usd: f64,
    /// Where to quote relative to v. 0.5 = at v/2 (=> 25% of max score).
    /// Lower = tighter (more reward, more adverse selection).
    pub quote_fraction_of_v: f64,
    /// Max time between refreshes even if prices haven't moved (ms).
    pub max_stale_ms: u64,
    /// Re-quote if target price moved by >= this many cents.
    pub repost_threshold_cents: u8,
    /// Hard cap on net inventory per market in USD. Crossing pulls the
    /// over-inventoried side (e.g. if YES holdings > cap, pull YES bid).
    pub inventory_cap_usd: f64,
    /// How many markets to quote in parallel.
    pub max_markets: usize,
}

impl Default for LpQuoterConfig {
    fn default() -> Self {
        Self {
            quote_size_usd:        200.0,
            quote_fraction_of_v:   0.5,
            max_stale_ms:          5_000,
            repost_threshold_cents: 1,
            inventory_cap_usd:     400.0,
            max_markets:           5,
        }
    }
}

/// Per-market handle. Cheap to clone (all interior is Arc).
#[derive(Debug, Clone)]
pub struct LpMarket {
    pub market: RewardMarket,
    /// YES (= "Up") token id as decimal string.
    pub yes_token_id: String,
    /// NO  (= "Down") token id as decimal string.
    pub no_token_id:  String,
    /// Internal mutable state.
    pub state: Arc<LpMarketState>,
}

/// Atomic state shared between the quoter loop and any inspector tasks.
#[derive(Debug)]
pub struct LpMarketState {
    /// Last successfully submitted YES buy price in cents (0 = none).
    last_yes_bid_cents: AtomicI64,
    /// Last successfully submitted NO buy price in cents (0 = none).
    last_no_bid_cents:  AtomicI64,
    /// Wall time of the last refresh attempt, ms since epoch.
    last_refresh_ms:    AtomicU64,
    /// Net YES inventory in USD (signed). Positive = we hold YES.
    yes_inventory_usd:  AtomicI64,    // cents
    /// Net NO  inventory in USD (signed). Positive = we hold NO.
    no_inventory_usd:   AtomicI64,    // cents
}

impl Default for LpMarketState {
    fn default() -> Self {
        Self {
            last_yes_bid_cents: AtomicI64::new(0),
            last_no_bid_cents:  AtomicI64::new(0),
            last_refresh_ms:    AtomicU64::new(0),
            yes_inventory_usd:  AtomicI64::new(0),
            no_inventory_usd:   AtomicI64::new(0),
        }
    }
}

impl LpMarketState {
    pub fn yes_inventory_dollars(&self) -> f64 {
        self.yes_inventory_usd.load(Ordering::SeqCst) as f64 / 100.0
    }
    pub fn no_inventory_dollars(&self) -> f64 {
        self.no_inventory_usd.load(Ordering::SeqCst) as f64 / 100.0
    }
    pub fn last_refresh_ms(&self) -> u64 {
        self.last_refresh_ms.load(Ordering::SeqCst)
    }
    /// Apply a fill on the YES side. Positive size = we bought, negative = we sold.
    pub fn record_yes_fill(&self, size_usd: f64) {
        let cents = (size_usd * 100.0).round() as i64;
        self.yes_inventory_usd.fetch_add(cents, Ordering::SeqCst);
    }
    pub fn record_no_fill(&self, size_usd: f64) {
        let cents = (size_usd * 100.0).round() as i64;
        self.no_inventory_usd.fetch_add(cents, Ordering::SeqCst);
    }
}

/// Cache holding the discovered markets we currently quote on.
#[derive(Debug, Default)]
pub struct LpQuoterCache {
    pub markets: RwLock<HashMap<String, LpMarket>>,
}

impl LpQuoterCache {
    pub async fn replace(&self, fresh: Vec<LpMarket>) {
        let mut g = self.markets.write().await;
        let mut new_map = HashMap::with_capacity(fresh.len());
        for m in fresh {
            // Preserve state across refresh: if we already have this market,
            // keep its existing state Arc so inventory doesn't reset.
            if let Some(existing) = g.get(&m.market.condition_id) {
                let merged = LpMarket {
                    market:       m.market,
                    yes_token_id: m.yes_token_id,
                    no_token_id:  m.no_token_id,
                    state:        existing.state.clone(),
                };
                new_map.insert(merged.market.condition_id.clone(), merged);
            } else {
                new_map.insert(m.market.condition_id.clone(), m);
            }
        }
        *g = new_map;
    }

    pub async fn snapshot(&self) -> Vec<LpMarket> {
        self.markets.read().await.values().cloned().collect()
    }

    pub async fn len(&self) -> usize {
        self.markets.read().await.len()
    }
}

/// One refresh cycle for one market. Pure function of inputs / outputs
/// so it's testable without spawning tokio tasks.
#[derive(Debug, Clone)]
pub struct QuoteDecision {
    pub yes_bid_cents: Option<u8>,    // None = pull YES side
    pub no_bid_cents:  Option<u8>,    // None = pull NO side
    pub reason:        QuoteReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuoteReason {
    /// Refresh because last submit was older than max_stale_ms.
    Stale,
    /// Re-quote because target price moved by >= repost_threshold.
    PriceMoved,
    /// Pull a side because inventory exceeded cap.
    InventoryCapped,
    /// First quote on this market.
    Initial,
    /// Skip — book is bad / mid undefined.
    NoMid,
    /// No-op — current quotes are already at the right price and not
    /// stale. Caller should skip cancel + repost entirely. Distinct
    /// from Stale (which means "WAS at right price but now too old").
    NoChange,
}

/// Decide what to quote given the current book and per-market state.
/// Returns None for sides that should be pulled.
pub fn decide_quote(
    book:         &BookSnapshot,
    cfg:          &LpQuoterConfig,
    market:       &RewardMarket,
    state:        &LpMarketState,
    now_ms:       u64,
) -> QuoteDecision {
    let mid = match book.mid() {
        Some(m) if m > 0.0 && m < 1.0 => m,
        _ => return QuoteDecision {
            yes_bid_cents: None,
            no_bid_cents:  None,
            reason:        QuoteReason::NoMid,
        },
    };

    let max_spread_cents = market.rewards_max_spread;     // already in cents per docs
    let target_offset_cents = max_spread_cents * cfg.quote_fraction_of_v;
    let target_offset = target_offset_cents / 100.0;

    // Target YES bid = mid - offset. Target ASK side via NO bid = (1 - (mid + offset))
    // because BUY-NO at price q is the same as SELL-YES at price (1 - q).
    let target_yes_bid = (mid - target_offset).clamp(0.01, 0.99);
    let target_no_bid  = ((1.0 - (mid + target_offset))).clamp(0.01, 0.99);

    let yes_cents = (target_yes_bid * 100.0).round().clamp(1.0, 99.0) as u8;
    let no_cents  = (target_no_bid  * 100.0).round().clamp(1.0, 99.0) as u8;

    // Inventory caps
    let yes_inv = state.yes_inventory_dollars();
    let no_inv  = state.no_inventory_dollars();
    let yes_overcap = yes_inv >= cfg.inventory_cap_usd;
    let no_overcap  = no_inv  >= cfg.inventory_cap_usd;

    // Should we re-quote?
    let last_yes = state.last_yes_bid_cents.load(Ordering::SeqCst);
    let last_no  = state.last_no_bid_cents.load(Ordering::SeqCst);
    let last_ms  = state.last_refresh_ms.load(Ordering::SeqCst);

    let initial = last_ms == 0;
    let stale   = !initial && now_ms.saturating_sub(last_ms) >= cfg.max_stale_ms;
    let yes_moved = !initial &&
        ((yes_cents as i64 - last_yes).unsigned_abs() as u8) >= cfg.repost_threshold_cents;
    let no_moved  = !initial &&
        ((no_cents  as i64 - last_no ).unsigned_abs() as u8) >= cfg.repost_threshold_cents;

    // Inventory cap takes priority over every other condition: a side
    // that's over the cap must be pulled regardless of refresh trigger.
    let reason = if yes_overcap || no_overcap { QuoteReason::InventoryCapped }
        else if initial { QuoteReason::Initial }
        else if stale { QuoteReason::Stale }
        else if yes_moved || no_moved { QuoteReason::PriceMoved }
        else {
            // No reason to refresh — keep current quotes. Caller MUST
            // skip cancel + repost entirely (otherwise we'd hammer
            // Polymarket on every tick with no-op quote churn).
            return QuoteDecision {
                yes_bid_cents: Some(last_yes as u8),
                no_bid_cents:  Some(last_no  as u8),
                reason:        QuoteReason::NoChange,
            };
        };

    QuoteDecision {
        yes_bid_cents: if yes_overcap { None } else { Some(yes_cents) },
        no_bid_cents:  if no_overcap  { None } else { Some(no_cents)  },
        reason,
    }
}

/// Result of one refresh cycle on one market. Used for tests and
/// observability.
#[derive(Debug, Clone)]
pub struct RefreshOutcome {
    pub market_id:       String,
    pub canceled:        usize,
    pub submitted:       usize,
    pub accepted:        usize,
    pub rejected:        usize,
    pub yes_quote_cents: Option<u8>,
    pub no_quote_cents:  Option<u8>,
    pub reason:          QuoteReason,
}

/// Refresh quotes for one market: cancel-by-market + re-submit per
/// decide_quote. Uses bulk submit when both sides are present.
pub async fn refresh_market(
    market:   &LpMarket,
    cfg:      &LpQuoterConfig,
    risk:     &Arc<RiskLimits>,
    signing:  &Arc<SigningParams>,
    client:   &Arc<ClobClient>,
    positions: Option<&Arc<PositionStore>>,
    now_ms:   u64,
) -> Result<RefreshOutcome> {
    let _ = positions; // reserved for future fill replay

    if risk.is_strategy_killed(Strategy::LpRewards) {
        return Ok(RefreshOutcome {
            market_id: market.market.condition_id.clone(),
            canceled: 0, submitted: 0, accepted: 0, rejected: 0,
            yes_quote_cents: None, no_quote_cents: None,
            reason: QuoteReason::InventoryCapped,
        });
    }

    let book = client.fetch_book(&market.yes_token_id).await
        .with_context(|| format!("fetch_book {}", market.yes_token_id))?;
    let decision = decide_quote(&book, cfg, &market.market, &market.state, now_ms);

    // No-change case: prices and staleness window OK. Skip cancel +
    // repost entirely — current quotes are still our best quotes.
    // Without this short-circuit we'd hammer the API on every tick.
    if decision.reason == QuoteReason::NoChange {
        return Ok(RefreshOutcome {
            market_id: market.market.condition_id.clone(),
            canceled: 0, submitted: 0, accepted: 0, rejected: 0,
            yes_quote_cents: decision.yes_bid_cents,
            no_quote_cents:  decision.no_bid_cents,
            reason: QuoteReason::NoChange,
        });
    }

    // No-mid case: pull existing quotes and skip
    if decision.reason == QuoteReason::NoMid {
        let canceled = client.cancel_orders_by_market(&market.market.condition_id)
            .await.unwrap_or(0);
        return Ok(RefreshOutcome {
            market_id: market.market.condition_id.clone(),
            canceled, submitted: 0, accepted: 0, rejected: 0,
            yes_quote_cents: None, no_quote_cents: None,
            reason: QuoteReason::NoMid,
        });
    }

    // Cancel any stale resting orders before re-quoting
    let canceled = client.cancel_orders_by_market(&market.market.condition_id)
        .await.unwrap_or(0);

    // Resolve maker / signer once per refresh
    let signer = private_key_to_address(&signing.private_key_hex)
        .context("signer derivation")?;
    let maker = match (signing.signature_type, signing.funder_address.as_deref()) {
        (SignatureType::Eoa, _) => signer,
        (_, Some(a)) => hex_to_address(a).context("funder parse")?,
        (sig, None) => anyhow::bail!("{:?} requires funder_address", sig),
    };

    let mut entries: Vec<(crate::execution::orders::SignedOrder, OrderType, Option<bool>)>
        = Vec::with_capacity(2);
    let mut yes_intended_cents: Option<u8> = None;
    let mut no_intended_cents:  Option<u8> = None;

    if let Some(yes_cents) = decision.yes_bid_cents {
        let asset = u256_from_dec(&market.yes_token_id).context("yes token id")?;
        let inputs = BuyOrderInputs {
            asset_id: asset, maker, signer, taker: signing.taker,
            bet_dollars: cfg.quote_size_usd, price_cents: yes_cents,
            // GTC: 0 = no expiration (orders rest until cancelled)
            expiration_unix: 0,
            nonce: signing.nonce, fee_rate_bps: signing.fee_rate_bps,
            signature_type: signing.signature_type,
        };
        match sign_buy_now(&inputs, &signing.domain, &signing.private_key_hex) {
            Ok(s) => {
                entries.push((s, OrderType::Gtc, Some(true)));
                yes_intended_cents = Some(yes_cents);
            }
            Err(e) => tracing::warn!(error = %e, "lp quoter: yes sign failed"),
        }
    }
    if let Some(no_cents) = decision.no_bid_cents {
        let asset = u256_from_dec(&market.no_token_id).context("no token id")?;
        let inputs = BuyOrderInputs {
            asset_id: asset, maker, signer, taker: signing.taker,
            bet_dollars: cfg.quote_size_usd, price_cents: no_cents,
            // GTC: 0 = no expiration (orders rest until cancelled)
            expiration_unix: 0,
            nonce: signing.nonce, fee_rate_bps: signing.fee_rate_bps,
            signature_type: signing.signature_type,
        };
        match sign_buy_now(&inputs, &signing.domain, &signing.private_key_hex) {
            Ok(s) => {
                entries.push((s, OrderType::Gtc, Some(true)));
                no_intended_cents = Some(no_cents);
            }
            Err(e) => tracing::warn!(error = %e, "lp quoter: no sign failed"),
        }
    }

    if entries.is_empty() {
        // Both sides pulled (inventory cap). Mark refresh time and exit.
        market.state.last_refresh_ms.store(now_ms, Ordering::SeqCst);
        return Ok(RefreshOutcome {
            market_id: market.market.condition_id.clone(),
            canceled, submitted: 0, accepted: 0, rejected: 0,
            yes_quote_cents: None, no_quote_cents: None,
            reason: decision.reason,
        });
    }

    let outcomes = client.submit_orders_bulk(&entries).await
        .context("bulk submit")?;

    let accepted = outcomes.iter().filter(|o| o.success || o.dry_run).count();
    let rejected = outcomes.len() - accepted;

    // Update state on success
    market.state.last_refresh_ms.store(now_ms, Ordering::SeqCst);
    if let Some(c) = yes_intended_cents {
        market.state.last_yes_bid_cents.store(c as i64, Ordering::SeqCst);
    }
    if let Some(c) = no_intended_cents {
        market.state.last_no_bid_cents.store(c as i64, Ordering::SeqCst);
    }

    tracing::debug!(
        market = %market.market.condition_id,
        canceled, submitted = outcomes.len(), accepted, rejected,
        yes = ?yes_intended_cents, no = ?no_intended_cents,
        reason = ?decision.reason,
        "lp quoter refresh"
    );

    Ok(RefreshOutcome {
        market_id: market.market.condition_id.clone(),
        canceled, submitted: outcomes.len(), accepted, rejected,
        yes_quote_cents: yes_intended_cents,
        no_quote_cents:  no_intended_cents,
        reason: decision.reason,
    })
}

/// Drive refresh_market on a fixed cadence for every market in the
/// cache. Spawned once per bot run; lives until the bot exits.
pub async fn lp_quoter_loop(
    cache:    Arc<LpQuoterCache>,
    cfg:      Arc<LpQuoterConfig>,
    risk:     Arc<RiskLimits>,
    signing:  Arc<SigningParams>,
    client:   Arc<ClobClient>,
    positions: Option<Arc<PositionStore>>,
    cadence: Duration,
) {
    let mut tick = tokio::time::interval(cadence);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        if risk.is_strategy_killed(Strategy::LpRewards) {
            tracing::warn!("lp quoter: strategy killed, sleeping cadence");
            continue;
        }
        let markets = cache.snapshot().await;
        if markets.is_empty() { continue; }
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;

        // Spawn refresh per market in parallel — they're independent.
        let mut handles = Vec::with_capacity(markets.len());
        for m in markets {
            let cfg = cfg.clone();
            let risk = risk.clone();
            let signing = signing.clone();
            let client = client.clone();
            let positions = positions.clone();
            handles.push(tokio::spawn(async move {
                let res = refresh_market(
                    &m, &cfg, &risk, &signing, &client,
                    positions.as_ref(), now_ms,
                ).await;
                if let Err(e) = res {
                    tracing::warn!(market = %m.market.condition_id, error = %e,
                        "lp quoter refresh failed");
                }
            }));
        }
        for h in handles { let _ = h.await; }
    }
}

/// Pull the active rewards list and refresh the cache. Spawned as a
/// separate task on a slower cadence (default 1h) than the quoter loop.
///
/// `resolve_tokens` is a closure mapping condition_id -> (yes_token_id,
/// no_token_id). Injected so tests can stub Gamma without HTTP. Returns
/// None to skip markets we can't resolve.
pub async fn lp_discovery_loop(
    cache:    Arc<LpQuoterCache>,
    cfg:      Arc<LpQuoterConfig>,
    client:   Arc<ClobClient>,
    resolve_tokens: Arc<dyn Fn(String) -> std::pin::Pin<Box<
        dyn std::future::Future<Output = Option<(String, String)>> + Send>> + Send + Sync>,
    cadence: Duration,
) {
    let mut tick = tokio::time::interval(cadence);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        match client.fetch_active_rewards(cfg.max_markets * 4).await {
            Ok(rewards) => {
                let mut keepers: Vec<LpMarket> = Vec::new();
                for r in rewards {
                    // Filter: must accept our quote size
                    if r.rewards_min_size > cfg.quote_size_usd {
                        continue;
                    }
                    let r_id = r.condition_id.clone();
                    let resolved = (resolve_tokens)(r_id.clone()).await;
                    let Some((yes, no)) = resolved else { continue; };
                    keepers.push(LpMarket {
                        market: r,
                        yes_token_id: yes,
                        no_token_id:  no,
                        state: Arc::new(LpMarketState::default()),
                    });
                    if keepers.len() >= cfg.max_markets { break; }
                }
                let n = keepers.len();
                cache.replace(keepers).await;
                tracing::info!(n, "lp quoter: discovery refreshed");
            }
            Err(e) => tracing::warn!(error = %e, "lp quoter discovery failed"),
        }
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::clob::{BookSnapshot, RewardMarket};

    fn sample_book(mid: f64) -> BookSnapshot {
        BookSnapshot {
            token_id: "tok".to_string(),
            bids: vec![(mid - 0.005, 100.0)],
            asks: vec![(mid + 0.005, 100.0)],
        }
    }

    fn sample_market() -> RewardMarket {
        RewardMarket {
            condition_id:        "0xCONDITION".to_string(),
            rewards_max_spread:  3.5,        // 3.5 cents
            rewards_min_size:    200.0,
            rate_per_day:        100.0,
            sponsored_daily_rate: 0.0,
        }
    }

    fn sample_cfg() -> LpQuoterConfig { LpQuoterConfig::default() }

    #[test]
    fn decide_initial_quote_emits_both_sides() {
        let book = sample_book(0.50);
        let m    = sample_market();
        let st   = LpMarketState::default();
        let d    = decide_quote(&book, &sample_cfg(), &m, &st, 1_000_000);
        assert_eq!(d.reason, QuoteReason::Initial);
        assert!(d.yes_bid_cents.is_some());
        assert!(d.no_bid_cents.is_some());
    }

    #[test]
    fn decide_at_v_over_2_offsets_yes_below_mid() {
        let book = sample_book(0.50);
        let m    = sample_market();   // v = 3.5 cents, half = 1.75 cents
        let st   = LpMarketState::default();
        let d    = decide_quote(&book, &sample_cfg(), &m, &st, 0);
        // YES bid should be ~50c - 1.75c = 48.25c, rounded to 48
        assert_eq!(d.yes_bid_cents, Some(48));
        // NO bid should be ~50c - 1.75c = 48.25c (mirror), rounded to 48
        assert_eq!(d.no_bid_cents,  Some(48));
    }

    #[test]
    fn decide_pulls_yes_when_yes_inventory_capped() {
        let book = sample_book(0.50);
        let m    = sample_market();
        let st   = LpMarketState::default();
        st.record_yes_fill(500.0); // way over default 400 cap
        let d = decide_quote(&book, &sample_cfg(), &m, &st, 0);
        assert_eq!(d.reason, QuoteReason::InventoryCapped);
        assert!(d.yes_bid_cents.is_none());
        assert!(d.no_bid_cents.is_some());
    }

    #[test]
    fn decide_pulls_no_when_no_inventory_capped() {
        let book = sample_book(0.50);
        let m    = sample_market();
        let st   = LpMarketState::default();
        st.record_no_fill(500.0);
        let d = decide_quote(&book, &sample_cfg(), &m, &st, 0);
        assert_eq!(d.reason, QuoteReason::InventoryCapped);
        assert!(d.no_bid_cents.is_none());
        assert!(d.yes_bid_cents.is_some());
    }

    #[test]
    fn decide_no_mid_returns_pull_both() {
        let book = BookSnapshot {
            token_id: "x".into(), bids: vec![], asks: vec![],
        };
        let m  = sample_market();
        let st = LpMarketState::default();
        let d  = decide_quote(&book, &sample_cfg(), &m, &st, 0);
        assert_eq!(d.reason, QuoteReason::NoMid);
        assert!(d.yes_bid_cents.is_none());
        assert!(d.no_bid_cents.is_none());
    }

    #[test]
    fn decide_skips_repost_when_price_unchanged() {
        let book = sample_book(0.50);
        let m    = sample_market();
        let st   = LpMarketState::default();
        // Pretend we already submitted at 48 / 48 just 1s ago
        st.last_yes_bid_cents.store(48, Ordering::SeqCst);
        st.last_no_bid_cents .store(48, Ordering::SeqCst);
        st.last_refresh_ms   .store(1_000_000, Ordering::SeqCst);
        let d = decide_quote(&book, &sample_cfg(), &m, &st, 1_001_000);
        // 1s elapsed < 5s max_stale and price unchanged -> NoChange.
        // Caller (refresh_market) MUST short-circuit on NoChange so we
        // don't cancel + repost the same quotes every tick.
        assert_eq!(d.reason, QuoteReason::NoChange);
        assert_eq!(d.yes_bid_cents, Some(48));
        assert_eq!(d.no_bid_cents,  Some(48));
    }

    #[test]
    fn decide_reposts_when_stale() {
        let book = sample_book(0.50);
        let m    = sample_market();
        let st   = LpMarketState::default();
        st.last_yes_bid_cents.store(48, Ordering::SeqCst);
        st.last_no_bid_cents .store(48, Ordering::SeqCst);
        st.last_refresh_ms   .store(1_000_000, Ordering::SeqCst);
        let d = decide_quote(&book, &sample_cfg(), &m, &st, 1_010_000); // 10s later
        assert_eq!(d.reason, QuoteReason::Stale);
    }

    #[test]
    fn decide_reposts_when_price_moves() {
        let book = sample_book(0.55);    // mid moved up 5c
        let m    = sample_market();
        let st   = LpMarketState::default();
        st.last_yes_bid_cents.store(48, Ordering::SeqCst);
        st.last_no_bid_cents .store(48, Ordering::SeqCst);
        st.last_refresh_ms   .store(1_000_000, Ordering::SeqCst);
        let d = decide_quote(&book, &sample_cfg(), &m, &st, 1_001_000);
        assert_eq!(d.reason, QuoteReason::PriceMoved);
    }

    #[test]
    fn cache_preserves_state_across_replace() {
        let cache = LpQuoterCache::default();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build().unwrap();
        runtime.block_on(async {
            let m1 = LpMarket {
                market: sample_market(),
                yes_token_id: "tok-y".into(),
                no_token_id:  "tok-n".into(),
                state: Arc::new(LpMarketState::default()),
            };
            m1.state.record_yes_fill(50.0);
            cache.replace(vec![m1.clone()]).await;

            // Re-discover same market
            let m2 = LpMarket {
                market: sample_market(),
                yes_token_id: "tok-y".into(),
                no_token_id:  "tok-n".into(),
                state: Arc::new(LpMarketState::default()),
            };
            cache.replace(vec![m2]).await;

            let snap = cache.snapshot().await;
            assert_eq!(snap.len(), 1);
            // Inventory carried over from m1's state
            assert!((snap[0].state.yes_inventory_dollars() - 50.0).abs() < 1e-6);
        });
    }

    #[test]
    fn cache_evicts_disappeared_markets() {
        let cache = LpQuoterCache::default();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build().unwrap();
        runtime.block_on(async {
            let m1 = LpMarket {
                market: RewardMarket { condition_id: "A".into(), ..sample_market() },
                yes_token_id: "y1".into(), no_token_id: "n1".into(),
                state: Arc::new(LpMarketState::default()),
            };
            let m2 = LpMarket {
                market: RewardMarket { condition_id: "B".into(), ..sample_market() },
                yes_token_id: "y2".into(), no_token_id: "n2".into(),
                state: Arc::new(LpMarketState::default()),
            };
            cache.replace(vec![m1, m2]).await;
            assert_eq!(cache.len().await, 2);
            // Next discovery only returns A
            let m1b = LpMarket {
                market: RewardMarket { condition_id: "A".into(), ..sample_market() },
                yes_token_id: "y1".into(), no_token_id: "n1".into(),
                state: Arc::new(LpMarketState::default()),
            };
            cache.replace(vec![m1b]).await;
            assert_eq!(cache.len().await, 1);
        });
    }

    #[test]
    fn quoter_config_defaults_are_safe() {
        let c = LpQuoterConfig::default();
        assert!(c.quote_size_usd >= 5.0);            // Polymarket min order
        assert!(c.quote_fraction_of_v > 0.0 && c.quote_fraction_of_v < 1.0);
        assert!(c.inventory_cap_usd >= c.quote_size_usd);
        assert!(c.max_stale_ms <= 30_000);
    }

    #[test]
    fn state_inventory_round_trips_through_cents() {
        let st = LpMarketState::default();
        st.record_yes_fill(123.45);
        st.record_yes_fill(-23.45);
        assert!((st.yes_inventory_dollars() - 100.0).abs() < 1e-6);
        st.record_no_fill(50.50);
        assert!((st.no_inventory_dollars() - 50.50).abs() < 1e-6);
    }
}
