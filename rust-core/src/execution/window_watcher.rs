// Sprint 4 / J11 — Window-open hook.
//
// Watches FeatureState.window_strikes for newly-captured strikes (= a fresh
// 5-min BTC window has just opened). For each new market, spawns an
// independent tokio task that pre-signs the YES/NO ladder.
//
// Per-market parallelism: tokio::spawn fans the work across the runtime, and
// each populate call internally fans ECDSA signing across rayon's pool. With
// N concurrent markets × 31 ticks × 2 sides = 62N signings, we want all of
// them in flight at once — 20 cores plenty.
//
// Cadence: 500ms tick aligns with the rest of the system. A new strike is
// captured by the chainlink polling loop a few hundred ms after the on-chain
// round update, so the watcher sees it on the next tick.

use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::RwLock;
use tokio::time::{Duration, MissedTickBehavior};

use crate::execution::executor::{populate_pool_for_window, WindowSetup};
use crate::execution::orders::SignatureType;
use crate::execution::presign::OrderPool;
use crate::features::{FeatureState, BookState};
use crate::streams::polymarket::PolyMarket;

/// Configuration the watcher reads on every populate call.
#[derive(Debug, Clone)]
pub struct WindowWatcherConfig {
    /// Per-order USDC amount — typically max kelly bet for the bankroll.
    pub bet_dollars:      f64,
    /// ±N cents around each midpoint to pre-sign.
    pub depth:            u8,
    pub nonce:            u64,
    pub fee_rate_bps:     u64,
    pub signature_type:   SignatureType,
    pub private_key_hex:  String,
    /// Required for non-EOA signature types.
    pub funder_address:   Option<String>,
}

/// Long-running task: watch state.window_strikes, pre-sign on first sight.
///
/// Each newly-detected market is handled by its own tokio task — multiple
/// markets pre-sign in parallel.
pub async fn window_watcher_loop(
    state:   Arc<RwLock<FeatureState>>,
    markets: Arc<RwLock<Vec<PolyMarket>>>,
    pool:    Arc<OrderPool>,
    cfg:     WindowWatcherConfig,
) {
    let mut seen: HashSet<String> = HashSet::new();
    let mut tick = tokio::time::interval(Duration::from_millis(500));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tick.tick().await;
        let new_markets = scan_for_new_markets(&state, &markets, &mut seen).await;

        for (market, yes_mid, no_mid) in new_markets {
            // Pre-signing is CPU-bound; spawn_blocking lets the rayon pool
            // run without blocking tokio's I/O reactor.
            let pool_clone   = pool.clone();
            let cfg_clone    = cfg.clone();
            tokio::task::spawn_blocking(move || {
                let setup = WindowSetup::for_market(
                    &market,
                    yes_mid,
                    no_mid,
                    cfg_clone.bet_dollars,
                    cfg_clone.depth,
                    cfg_clone.nonce,
                    cfg_clone.fee_rate_bps,
                    cfg_clone.signature_type,
                    cfg_clone.private_key_hex.clone(),
                    cfg_clone.funder_address.clone(),
                );
                match populate_pool_for_window(&pool_clone, &setup) {
                    Ok(counts) => tracing::info!(
                        market = %market.id,
                        yes = counts.yes_count,
                        no  = counts.no_count,
                        expiration_unix = setup.expiration_unix,
                        "pre-signed window pool"
                    ),
                    Err(e) => tracing::error!(
                        market = %market.id,
                        error = %e,
                        "pre-sign failed"
                    ),
                }
            });
        }
    }
}

/// Scan once. Returns (market, yes_mid, no_mid) for each market we've not
/// pre-signed yet.
///
/// Split out from the loop so tests can drive a single tick without spinning
/// the loop.
pub async fn scan_for_new_markets(
    state:   &Arc<RwLock<FeatureState>>,
    markets: &Arc<RwLock<Vec<PolyMarket>>>,
    seen:    &mut HashSet<String>,
) -> Vec<(PolyMarket, f64, f64)> {
    let s   = state.read().await;
    let ms  = markets.read().await;

    let mut out = Vec::new();
    for market_id in s.window_strikes.keys() {
        if seen.contains(market_id) { continue; }
        let Some(market) = ms.iter().find(|m| &m.id == market_id) else {
            // Strike captured but the market isn't in our list yet — wait for
            // the discovery loop to refresh.
            continue;
        };
        let yes_mid = best_ask(&s.asset_books, &market.up_token_id);
        let no_mid  = best_ask(&s.asset_books, &market.down_token_id);
        if !valid_price(yes_mid) || !valid_price(no_mid) {
            // Books not yet populated — wait for the next tick.
            continue;
        }
        out.push((market.clone(), yes_mid, no_mid));
        seen.insert(market_id.clone());
    }
    out
}

/// Look up best ask for a token. Returns 0.0 if unknown.
fn best_ask(books: &std::collections::HashMap<String, BookState>, asset_id: &str) -> f64 {
    books.get(asset_id).map(|b| b.best_ask).unwrap_or(0.0)
}

/// A valid Polymarket binary-option price is strictly inside (0, 1).
fn valid_price(p: f64) -> bool {
    p.is_finite() && p > 0.0 && p < 1.0
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::orders::SignatureType;
    use crate::execution::presign::{OrderPool, PoolKey};
    use crate::execution::orders::{u256_from_dec, Side};

    const TEST_PK: &str =
        "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

    const UP_TOKEN_ID:   &str = "770286165307075478407821058745";
    const DOWN_TOKEN_ID: &str = "842741244417177364311129620769";

    fn sample_market() -> PolyMarket {
        PolyMarket {
            id:             "btc-w1".to_string(),
            question:       "BTC Up or Down?".to_string(),
            up_token_id:    UP_TOKEN_ID.to_string(),
            down_token_id:  DOWN_TOKEN_ID.to_string(),
            close_time_ms:  2_000_000_000_000,
            duration_min:   5,
            liquidity_usd:  1_000.0,
        }
    }

    fn sample_state(market_id: &str, yes_ask: f64, no_ask: f64) -> FeatureState {
        let mut s = FeatureState::default();
        // Strike captured at t=1000 with strike=80_000 — only the presence
        // of the entry is what triggers the watcher.
        s.window_strikes.insert(market_id.to_string(), (1_000, 80_000.0));
        // Books populated for both sides
        let mut yes_book = BookState::default(); yes_book.best_ask = yes_ask;
        let mut no_book  = BookState::default(); no_book.best_ask  = no_ask;
        s.asset_books.insert(UP_TOKEN_ID.to_string(),   yes_book);
        s.asset_books.insert(DOWN_TOKEN_ID.to_string(), no_book);
        s
    }

    fn sample_cfg() -> WindowWatcherConfig {
        WindowWatcherConfig {
            bet_dollars:     5.00,
            depth:           15,
            nonce:           0,
            fee_rate_bps:    0,
            signature_type:  SignatureType::Eoa,
            private_key_hex: TEST_PK.to_string(),
            funder_address:  None,
        }
    }

    // ── scan_for_new_markets ──────────────────────────────────────────────

    #[tokio::test]
    async fn scan_yields_new_market_with_valid_books() {
        let state   = Arc::new(RwLock::new(sample_state("btc-w1", 0.45, 0.55)));
        let markets = Arc::new(RwLock::new(vec![sample_market()]));
        let mut seen = HashSet::new();
        let new_markets = scan_for_new_markets(&state, &markets, &mut seen).await;
        assert_eq!(new_markets.len(), 1);
        let (m, y, n) = &new_markets[0];
        assert_eq!(m.id, "btc-w1");
        assert!((y - 0.45).abs() < 1e-9);
        assert!((n - 0.55).abs() < 1e-9);
        assert!(seen.contains("btc-w1"));
    }

    #[tokio::test]
    async fn scan_skips_already_seen_market() {
        let state   = Arc::new(RwLock::new(sample_state("btc-w1", 0.45, 0.55)));
        let markets = Arc::new(RwLock::new(vec![sample_market()]));
        let mut seen = HashSet::new();
        let _ = scan_for_new_markets(&state, &markets, &mut seen).await;
        // Second call: nothing new
        let again = scan_for_new_markets(&state, &markets, &mut seen).await;
        assert!(again.is_empty());
    }

    #[tokio::test]
    async fn scan_skips_when_market_unknown() {
        // Strike for "ghost" but no PolyMarket in the list — must not crash,
        // must not mark seen (so a later discovery refresh picks it up).
        let state   = Arc::new(RwLock::new(sample_state("ghost", 0.45, 0.55)));
        let markets: Arc<RwLock<Vec<PolyMarket>>> = Arc::new(RwLock::new(vec![]));
        let mut seen = HashSet::new();
        let new_markets = scan_for_new_markets(&state, &markets, &mut seen).await;
        assert!(new_markets.is_empty());
        assert!(!seen.contains("ghost"));
    }

    #[tokio::test]
    async fn scan_skips_when_books_empty() {
        // Strike captured but no book entry yet → can't determine midpoint
        let mut s = sample_state("btc-w1", 0.0, 0.0);  // best_ask = 0 for both
        s.asset_books.clear();
        let state   = Arc::new(RwLock::new(s));
        let markets = Arc::new(RwLock::new(vec![sample_market()]));
        let mut seen = HashSet::new();
        let new_markets = scan_for_new_markets(&state, &markets, &mut seen).await;
        assert!(new_markets.is_empty());
        assert!(!seen.contains("btc-w1"));   // not marked seen — retry next tick
    }

    #[tokio::test]
    async fn multiple_markets_all_returned_in_one_scan() {
        let mut s = FeatureState::default();
        s.window_strikes.insert("m1".to_string(), (1_000, 80_000.0));
        s.window_strikes.insert("m2".to_string(), (1_010, 80_100.0));
        // Distinct token IDs per market
        let m1 = PolyMarket {
            id: "m1".into(), question: "".into(),
            up_token_id: "111".into(), down_token_id: "112".into(),
            close_time_ms: 2_000_000_000_000, duration_min: 5, liquidity_usd: 1_000.0,
        };
        let m2 = PolyMarket {
            id: "m2".into(), question: "".into(),
            up_token_id: "211".into(), down_token_id: "212".into(),
            close_time_ms: 2_000_000_000_000, duration_min: 5, liquidity_usd: 1_000.0,
        };
        for tok in ["111","112","211","212"] {
            let mut bk = BookState::default(); bk.best_ask = 0.50;
            s.asset_books.insert(tok.to_string(), bk);
        }
        let state   = Arc::new(RwLock::new(s));
        let markets = Arc::new(RwLock::new(vec![m1, m2]));
        let mut seen = HashSet::new();
        let new_markets = scan_for_new_markets(&state, &markets, &mut seen).await;
        assert_eq!(new_markets.len(), 2);
    }

    // ── End-to-end loop tick (drive a single iteration) ───────────────────

    #[tokio::test]
    async fn loop_populates_pool_for_new_market() {
        let pool = Arc::new(OrderPool::new());
        let state = Arc::new(RwLock::new(sample_state("btc-w1", 0.45, 0.55)));
        let markets = Arc::new(RwLock::new(vec![sample_market()]));

        // We can't call the loop directly (it's infinite). Mimic one tick
        // by calling scan_for_new_markets and the populate spawn manually.
        let mut seen = HashSet::new();
        let new_markets = scan_for_new_markets(&state, &markets, &mut seen).await;
        assert_eq!(new_markets.len(), 1);

        let (market, y, n) = &new_markets[0];
        let cfg = sample_cfg();
        let setup = WindowSetup::for_market(
            market, *y, *n,
            cfg.bet_dollars, cfg.depth, cfg.nonce, cfg.fee_rate_bps,
            cfg.signature_type, cfg.private_key_hex.clone(), cfg.funder_address.clone(),
        );
        populate_pool_for_window(&pool, &setup).unwrap();

        // Expect a key for YES at 45¢
        let asset = u256_from_dec(UP_TOKEN_ID).unwrap();
        let key = PoolKey { asset_id: asset, side: Side::Buy, price_cents: 45 };
        assert!(pool.contains(&key));
        // Expiration is bound to market close — pull any order and verify
        let signed = pool.get(&key).unwrap();
        let expected_exp = setup.expiration_unix;
        let exp_dec = crate::execution::orders::u256_to_dec(&signed.order.expiration);
        assert_eq!(exp_dec, expected_exp.to_string());
        // 2_000_000_000_000ms / 1000 = 2_000_000_000 secs
        assert_eq!(expected_exp, 2_000_000_000);
    }

    // ── Concurrent populate: two markets at once ──────────────────────────

    #[tokio::test]
    async fn concurrent_populate_for_two_markets() {
        // Spawn two populate tasks concurrently; both must finish, total
        // time should be ~max(individual time), not sum.
        let pool = Arc::new(OrderPool::new());
        let cfg = sample_cfg();

        let m1 = PolyMarket {
            id: "m1".into(), question: "".into(),
            up_token_id: "111".into(), down_token_id: "112".into(),
            close_time_ms: 2_000_000_000_000, duration_min: 5, liquidity_usd: 1_000.0,
        };
        let m2 = PolyMarket {
            id: "m2".into(), question: "".into(),
            up_token_id: "211".into(), down_token_id: "212".into(),
            close_time_ms: 2_000_000_000_000, duration_min: 5, liquidity_usd: 1_000.0,
        };

        let p1 = pool.clone(); let p2 = pool.clone();
        let c1 = cfg.clone();  let c2 = cfg.clone();
        let h1 = tokio::task::spawn_blocking(move || {
            let setup = WindowSetup::for_market(
                &m1, 0.45, 0.55, c1.bet_dollars, c1.depth, c1.nonce, c1.fee_rate_bps,
                c1.signature_type, c1.private_key_hex, c1.funder_address,
            );
            populate_pool_for_window(&p1, &setup).unwrap()
        });
        let h2 = tokio::task::spawn_blocking(move || {
            let setup = WindowSetup::for_market(
                &m2, 0.45, 0.55, c2.bet_dollars, c2.depth, c2.nonce, c2.fee_rate_bps,
                c2.signature_type, c2.private_key_hex, c2.funder_address,
            );
            populate_pool_for_window(&p2, &setup).unwrap()
        });

        let (r1, r2) = (h1.await.unwrap(), h2.await.unwrap());
        assert_eq!(r1.yes_count, 31);
        assert_eq!(r1.no_count,  31);
        assert_eq!(r2.yes_count, 31);
        assert_eq!(r2.no_count,  31);
        assert_eq!(pool.len(), 4 * 31);  // 2 markets × 2 sides × 31 ticks
    }
}
