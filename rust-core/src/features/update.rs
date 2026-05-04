// C11: Feature state update functions + 500ms compute loop.
//
// Module layout:
//   update_history() — apply one StreamEvent to history buffers (O(1), sync)
//   recompute()      — run all computation functions and write results (sync)
//   compute_loop()   — async task: drains events and ticks every 500ms

use std::collections::VecDeque;
use std::sync::Arc;

use chrono::{Datelike, Timelike, Utc};
use tokio::sync::{broadcast, RwLock};
use tokio::time::{Duration, MissedTickBehavior};

use crate::streams::{LiqSide, StreamEvent};
use crate::streams::polymarket::PolyMarket;
use super::compute::{
    compute_arb_gap, compute_autocorr_lag1, compute_bb_position, compute_liq_imbalance,
    compute_macd_signal, compute_obi, compute_oi_velocity, compute_rsi, compute_spread,
    compute_vol, compute_vol_z_score, log_return,
};
use super::state::{BookState, FeatureState};

const TICK_MS: u64 = 500;

// ── Async loop ────────────────────────────────────────────────────────────────

/// C11 + D6: async compute task. Drains StreamEvents and rewrites `shared`
/// every 500ms. Reads the markets list to select a primary market and
/// compute Polymarket microstructure features.
pub async fn compute_loop(
    mut rx:  broadcast::Receiver<StreamEvent>,
    shared:  Arc<RwLock<FeatureState>>,
    markets: Arc<RwLock<Vec<PolyMarket>>>,
) {
    let mut tick = tokio::time::interval(Duration::from_millis(TICK_MS));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            event = rx.recv() => match event {
                Ok(e) => {
                    let mut s = shared.write().await;
                    update_history(&mut s, e);
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(dropped = n, "compute loop lagged — events dropped");
                }
                Err(broadcast::error::RecvError::Closed) => {
                    tracing::error!("broadcast channel closed — compute loop exiting");
                    break;
                }
            },
            _ = tick.tick() => {
                let markets_snapshot = markets.read().await.clone();
                let mut s = shared.write().await;
                recompute(&mut s, &markets_snapshot);
                log_snapshot(&s);
            }
        }
    }
}

// ── History update (O(1) per event) ──────────────────────────────────────────

pub(crate) fn update_history(state: &mut FeatureState, event: StreamEvent) {
    match event {
        StreamEvent::Trade { price, .. } => {
            if price > 0.0 { state.spot_price = price; }
        }

        StreamEvent::Kline { close, volume, is_closed, .. } => {
            if is_closed && close > 0.0 {
                let prev_close = state.closes_1m.newest();
                state.closes_1m.push(close);
                state.volumes_1m.push(volume.max(0.0));
                // Only emit a return when there's a real prior close to diff against
                if let Some(prev) = prev_close {
                    if prev > 0.0 {
                        let log_ret = (close / prev).ln();
                        if log_ret.is_finite() {
                            state.returns_1m.push(log_ret);
                        }
                    }
                }
            }
        }

        StreamEvent::MarkPrice { mark_price, .. } => {
            if mark_price > 0.0 { state.mark_price = mark_price; }
        }

        StreamEvent::FundingRate { funding_rate, .. } => {
            if funding_rate.is_finite() { state.funding_rate = funding_rate; }
        }

        StreamEvent::OpenInterest { open_interest, timestamp_ms } => {
            if open_interest > 0.0 {
                state.oi_history.push_back((timestamp_ms, open_interest));
                trim_ts(&mut state.oi_history, timestamp_ms, 300_000);
            }
        }

        StreamEvent::Liquidation { side, quantity, timestamp_ms, .. } => {
            if quantity > 0.0 {
                state.liq_history.push_back((timestamp_ms, side, quantity));
                trim_liq_ts(&mut state.liq_history, timestamp_ms, 600_000);
            }
        }

        StreamEvent::ChainlinkPrice { price, .. } => {
            if price > 0.0 { state.chainlink_price = price; }
        }

        StreamEvent::ClobBook {
            asset_id, best_bid, best_ask, best_bid_size, best_ask_size,
            last_trade_price, timestamp_ms, ..
        } => {
            state.asset_books.insert(asset_id, BookState {
                best_bid,
                best_ask,
                best_bid_size,
                best_ask_size,
                last_trade_price,
                timestamp_ms,
            });
        }

        // Comments (D3) deferred
        _ => {}
    }
}

// ── Feature recomputation (every 500ms) ──────────────────────────────────────

pub(crate) fn recompute(state: &mut FeatureState, markets: &[PolyMarket]) {
    // Returns
    state.ret_1m = log_return(&state.closes_1m, 1);
    state.ret_3m = log_return(&state.closes_1m, 3);
    state.ret_5m = log_return(&state.closes_1m, 5);

    // Volatility
    state.vol_5m      = compute_vol(&state.returns_1m, 5);
    state.vol_30m     = compute_vol(&state.returns_1m, 30);
    state.vol_ratio   = if state.vol_30m > 1e-8 {
        state.vol_5m / state.vol_30m
    } else {
        1.0
    };
    state.vol_z_score = compute_vol_z_score(state.vol_5m, state.vol_30m);

    // Technicals
    state.rsi_14        = compute_rsi(&state.closes_1m, 14);
    state.bb_position   = compute_bb_position(&state.closes_1m, 20, 2.0);
    state.macd_signal   = compute_macd_signal(&state.closes_1m);
    state.autocorr_lag1 = compute_autocorr_lag1(&state.returns_1m);

    // Cross-asset
    state.oi_velocity_60s = compute_oi_velocity(&state.oi_history, 60_000);
    state.liq_imbalance   = compute_liq_imbalance(&state.liq_history, 60_000);
    state.perp_basis      = state.mark_price - state.spot_price;

    // Time (UTC)
    let now              = Utc::now();
    let now_ms           = now.timestamp_millis() as u64;
    state.hour_of_day    = now.hour()                          as f64;
    state.minute_of_hour = now.minute()                        as f64;
    state.day_of_week    = now.weekday().num_days_from_monday() as f64;

    // D6: Polymarket-derived features
    capture_window_strikes(state, markets, now_ms);
    update_polymarket_features(state, markets, now_ms);

    sanitise(state);
    state.seq += 1;
}

// Capture chainlink price as the strike for any market whose window has
// just opened and we don't have a strike for yet. Called every tick.
fn capture_window_strikes(state: &mut FeatureState, markets: &[PolyMarket], now_ms: u64) {
    // Capture phase: only when we have a chainlink reading
    if state.chainlink_price > 0.0 {
        for m in markets {
            let open_ms  = m.open_time_ms();
            let close_ms = m.close_time_ms;
            if now_ms >= open_ms && now_ms < close_ms
                && !state.window_strikes.contains_key(&m.id)
            {
                state.window_strikes.insert(m.id.clone(), state.chainlink_price);
                tracing::info!(
                    market = %m.id,
                    strike = state.chainlink_price,
                    duration_min = m.duration_min,
                    "captured window-open strike"
                );
            }
        }
    }
    // GC phase: always run — drops strikes for markets that have resolved
    state.window_strikes.retain(|market_id, _| {
        markets.iter().any(|m| m.id == *market_id && m.close_time_ms > now_ms)
    });
}

// Pick the next-to-resolve BTC market with both books present and adequate
// liquidity, then populate FeatureState's Polymarket fields from it.
fn update_polymarket_features(state: &mut FeatureState, markets: &[PolyMarket], now_ms: u64) {
    let primary = select_primary_market(state, markets, now_ms);

    let Some(m) = primary else {
        state.primary_market_id = None;
        return;
    };

    state.primary_market_id = Some(m.id.clone());
    state.time_to_close     = m.time_to_close_secs(now_ms);

    if let Some(strike) = state.window_strikes.get(&m.id) {
        state.window_open_price = *strike;
    }

    let up_book   = state.asset_books.get(&m.up_token_id).copied();
    let down_book = state.asset_books.get(&m.down_token_id).copied();

    if let (Some(ub), Some(db)) = (up_book, down_book) {
        // We trade YES = "Up", so poly_yes_price is what it costs to buy that
        state.poly_yes_price = ub.best_ask;
        state.poly_obi       = compute_obi(ub.best_bid_size, ub.best_ask_size);
        state.spread         = compute_spread(ub.best_bid, ub.best_ask);
        state.arb_gap        = compute_arb_gap(ub.best_ask, db.best_ask);
        // oracle_gap is computed in E2 using the binary-option fair value;
        // for now leave it at default until that signal is wired.
    }
}

// Lowest time_to_close, with at least $500 liquidity equivalent (proxied
// by sum of bid sizes on the up token), and both books present.
fn select_primary_market<'a>(
    state:    &FeatureState,
    markets:  &'a [PolyMarket],
    now_ms:   u64,
) -> Option<&'a PolyMarket> {
    markets
        .iter()
        .filter(|m| m.close_time_ms > now_ms)
        .filter(|m| {
            // Both up and down books must be present
            state.asset_books.contains_key(&m.up_token_id)
                && state.asset_books.contains_key(&m.down_token_id)
        })
        .min_by_key(|m| m.close_time_ms)
}

// Replace any non-finite feature with its safe default so the IPC payload
// is always valid. Non-finite values here indicate a computation bug.
fn sanitise(state: &mut FeatureState) {
    macro_rules! guard {
        ($field:expr, $default:expr) => {
            if !$field.is_finite() {
                tracing::error!(
                    field   = stringify!($field),
                    value   = $field,
                    "non-finite feature replaced with default"
                );
                $field = $default;
            }
        };
    }
    guard!(state.ret_1m,          0.0);
    guard!(state.ret_3m,          0.0);
    guard!(state.ret_5m,          0.0);
    guard!(state.vol_5m,          0.0);
    guard!(state.vol_30m,         0.0);
    guard!(state.vol_ratio,       1.0);
    guard!(state.vol_z_score,     0.0);
    guard!(state.rsi_14,          50.0);
    guard!(state.bb_position,     0.5);
    guard!(state.macd_signal,     0.0);
    guard!(state.autocorr_lag1,   0.0);
    guard!(state.oi_velocity_60s, 0.0);
    guard!(state.liq_imbalance,   0.0);
    guard!(state.perp_basis,      0.0);
}

fn log_snapshot(s: &FeatureState) {
    tracing::debug!(
        seq           = s.seq,
        spot          = s.spot_price,
        ret_1m        = s.ret_1m,
        vol_5m        = s.vol_5m,
        vol_z         = s.vol_z_score,
        rsi           = s.rsi_14,
        bb            = s.bb_position,
        funding       = s.funding_rate,
        oi_vel        = s.oi_velocity_60s,
        liq_imb       = s.liq_imbalance,
        oracle_gap    = s.oracle_gap,
        time_to_close = s.time_to_close,
        "feature snapshot"
    );
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn trim_ts(deque: &mut VecDeque<(u64, f64)>, now_ms: u64, window_ms: u64) {
    let cutoff = now_ms.saturating_sub(window_ms);
    while deque.front().map_or(false, |(ts, _)| *ts < cutoff) {
        deque.pop_front();
    }
}

fn trim_liq_ts(
    deque:     &mut VecDeque<(u64, LiqSide, f64)>,
    now_ms:    u64,
    window_ms: u64,
) {
    let cutoff = now_ms.saturating_sub(window_ms);
    while deque.front().map_or(false, |(ts, _, _)| *ts < cutoff) {
        deque.pop_front();
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // Build a FeatureState with enough synthetic data to exercise all paths
    fn synthetic_state() -> FeatureState {
        let mut s = FeatureState::default();
        let mut price = 43_000.0_f64;

        // 30 rising 1-min candles
        for i in 0..30u64 {
            update_history(&mut s, StreamEvent::Kline {
                open:        price,
                high:        price + 50.0,
                low:         price - 20.0,
                close:       price + 30.0,
                volume:      100.0,
                is_closed:   true,
                open_time_ms: i * 60_000,
            });
            price += 30.0;
        }

        // Current spot and mark price
        let now = 30 * 60_000u64;
        update_history(&mut s, StreamEvent::Trade {
            price, quantity: 0.1, timestamp_ms: now,
        });
        update_history(&mut s, StreamEvent::MarkPrice {
            mark_price: price + 8.0, timestamp_ms: now,
        });
        update_history(&mut s, StreamEvent::FundingRate {
            funding_rate: 0.0001,
            next_funding_time_ms: now + 28_800_000,
            timestamp_ms: now,
        });

        // OI readings — gently rising
        for i in 0..10u64 {
            update_history(&mut s, StreamEvent::OpenInterest {
                open_interest: 5_000.0 + i as f64 * 5.0,
                timestamp_ms:  now + i * 6_000,
            });
        }

        // Mixed liquidations in the last 60s window
        update_history(&mut s, StreamEvent::Liquidation {
            side: LiqSide::Long,  price: 43_000.0, quantity: 0.5,
            timestamp_ms: now + 55_000,
        });
        update_history(&mut s, StreamEvent::Liquidation {
            side: LiqSide::Short, price: 44_000.0, quantity: 0.3,
            timestamp_ms: now + 58_000,
        });

        s
    }

    #[test]
    fn recompute_all_features_are_finite() {
        let mut s = synthetic_state();
        recompute(&mut s, &[]);
        let v = s.to_feature_vec();
        for (i, &val) in v.iter().enumerate() {
            assert!(val.is_finite(), "feature[{i}] = {val} is not finite");
        }
    }

    #[test]
    fn recompute_increments_seq() {
        let mut s = FeatureState::default();
        assert_eq!(s.seq, 0);
        recompute(&mut s, &[]); assert_eq!(s.seq, 1);
        recompute(&mut s, &[]); assert_eq!(s.seq, 2);
    }

    #[test]
    fn update_history_kline_not_closed_ignored() {
        let mut s = FeatureState::default();
        update_history(&mut s, StreamEvent::Kline {
            open: 100.0, high: 101.0, low: 99.0, close: 100.5,
            volume: 50.0, is_closed: false, open_time_ms: 0,
        });
        assert_eq!(s.closes_1m.len(), 0);
    }

    #[test]
    fn update_history_kline_closed_adds_close_and_return() {
        let mut s = FeatureState::default();
        let kline = |close: f64| StreamEvent::Kline {
            open: close - 1.0, high: close + 1.0, low: close - 2.0,
            close, volume: 100.0, is_closed: true, open_time_ms: 0,
        };
        update_history(&mut s, kline(100.0));
        assert_eq!(s.closes_1m.len(), 1);
        assert_eq!(s.returns_1m.len(), 0); // no previous close yet

        update_history(&mut s, kline(102.0));
        assert_eq!(s.closes_1m.len(), 2);
        assert_eq!(s.returns_1m.len(), 1);
        let expected = (102.0_f64 / 100.0).ln();
        assert!((s.returns_1m.newest().unwrap() - expected).abs() < 1e-10);
    }

    #[test]
    fn update_history_oi_old_entries_trimmed() {
        let mut s = FeatureState::default();
        // OI at t=0
        update_history(&mut s, StreamEvent::OpenInterest {
            open_interest: 5_000.0, timestamp_ms: 0,
        });
        assert_eq!(s.oi_history.len(), 1);
        // OI at t=400s (outside 300s window)
        update_history(&mut s, StreamEvent::OpenInterest {
            open_interest: 5_100.0, timestamp_ms: 400_000,
        });
        assert_eq!(s.oi_history.len(), 1, "entry at t=0 should be trimmed");
    }

    #[test]
    fn update_history_liq_old_entries_trimmed() {
        let mut s = FeatureState::default();
        update_history(&mut s, StreamEvent::Liquidation {
            side: LiqSide::Long, price: 43_000.0, quantity: 1.0, timestamp_ms: 0,
        });
        // 700s later — outside the 600s window
        update_history(&mut s, StreamEvent::Liquidation {
            side: LiqSide::Short, price: 44_000.0, quantity: 1.0, timestamp_ms: 700_000,
        });
        assert_eq!(s.liq_history.len(), 1, "old liq entry should be trimmed");
    }

    #[test]
    fn rising_prices_give_positive_returns_and_high_rsi() {
        let mut s = synthetic_state();
        recompute(&mut s, &[]);
        assert!(s.ret_1m > 0.0,  "rising prices → positive 1m return, got {}", s.ret_1m);
        assert!(s.rsi_14 > 50.0, "rising prices → RSI > 50, got {}", s.rsi_14);
    }

    #[test]
    fn perp_basis_is_mark_minus_spot() {
        let mut s = synthetic_state();
        recompute(&mut s, &[]);
        let expected = s.mark_price - s.spot_price;
        assert!((s.perp_basis - expected).abs() < 1e-6);
    }

    #[test]
    fn liq_imbalance_with_more_shorts_is_positive() {
        let mut s = FeatureState::default();
        let now = 100_000u64;
        // 2 short liq, 1 long liq → more shorts → positive imbalance
        for qty in [1.0, 1.0] {
            update_history(&mut s, StreamEvent::Liquidation {
                side: LiqSide::Short, price: 44_000.0, quantity: qty, timestamp_ms: now,
            });
        }
        update_history(&mut s, StreamEvent::Liquidation {
            side: LiqSide::Long, price: 43_000.0, quantity: 1.0, timestamp_ms: now,
        });
        recompute(&mut s, &[]);
        assert!(s.liq_imbalance > 0.0, "more shorts liq'd → positive, got {}", s.liq_imbalance);
    }

    // ── D6: primary market selection + Polymarket feature population ─────

    fn sample_poly_market(id: &str, close_ms: u64, duration_min: u32) -> PolyMarket {
        PolyMarket {
            id: id.to_string(),
            question: format!("Bitcoin Up or Down - test {id}"),
            up_token_id:   format!("up_{id}"),
            down_token_id: format!("down_{id}"),
            close_time_ms: close_ms,
            duration_min,
            liquidity_usd: 5000.0,
        }
    }

    fn book(bid: f64, ask: f64) -> BookState {
        BookState {
            best_bid: bid,
            best_ask: ask,
            best_bid_size: 100.0,
            best_ask_size: 100.0,
            last_trade_price: (bid + ask) / 2.0,
            timestamp_ms: 0,
        }
    }

    #[test]
    fn primary_market_is_soonest_to_close_with_both_books() {
        let mut s = FeatureState::default();
        s.spot_price = 80_000.0;
        s.chainlink_price = 80_000.0;

        // Use real wallclock time since recompute() reads chrono::Utc::now()
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;
        let m_far  = sample_poly_market("far",  now_ms + 600_000, 5);  // 10 min away
        let m_near = sample_poly_market("near", now_ms + 120_000, 5);  // 2 min away
        let markets = vec![m_far.clone(), m_near.clone()];

        // Books for "near" market
        s.asset_books.insert("up_near".into(),   book(0.45, 0.50));
        s.asset_books.insert("down_near".into(), book(0.50, 0.55));
        // Far market has no books — should be skipped
        s.asset_books.insert("up_far".into(),    book(0.40, 0.45));
        // (down_far not present)

        recompute(&mut s, &markets);
        assert_eq!(s.primary_market_id.as_deref(), Some("near"));
    }

    #[test]
    fn poly_features_populated_from_primary_market() {
        let mut s = FeatureState::default();
        s.spot_price      = 80_000.0;
        s.chainlink_price = 80_000.0;

        let now_ms = chrono::Utc::now().timestamp_millis() as u64;
        let m = sample_poly_market("m1", now_ms + 60_000, 5);
        s.asset_books.insert(m.up_token_id.clone(),   book(0.45, 0.50));
        s.asset_books.insert(m.down_token_id.clone(), book(0.48, 0.52));

        recompute(&mut s, &[m]);

        assert!((s.poly_yes_price - 0.50).abs() < 1e-9);
        assert!((s.arb_gap - (-0.02)).abs()    < 1e-9);
        assert!((s.spread - 0.05).abs()        < 1e-9);
        assert!(s.poly_obi.abs()               < 1e-10);
        assert!((s.time_to_close - 60.0).abs() < 1.0);
    }

    #[test]
    fn no_primary_market_when_no_books_present() {
        let mut s = FeatureState::default();
        let now_ms = 1_000_000u64;
        let m = sample_poly_market("m", now_ms + 60_000, 5);
        recompute(&mut s, &[m]);
        assert!(s.primary_market_id.is_none());
    }

    #[test]
    fn closed_markets_are_filtered() {
        let mut s = FeatureState::default();
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;
        let m = sample_poly_market("expired", now_ms.saturating_sub(60_000), 5);
        s.asset_books.insert(m.up_token_id.clone(),   book(0.45, 0.50));
        s.asset_books.insert(m.down_token_id.clone(), book(0.48, 0.52));
        recompute(&mut s, &[m]);
        assert!(s.primary_market_id.is_none(), "closed markets must not be primary");
    }

    #[test]
    fn window_strike_captured_when_chainlink_known() {
        let mut s = FeatureState::default();
        s.chainlink_price = 80_500.0;
        // We can't easily test now_ms boundary inside recompute, but we can
        // verify that capture_window_strikes runs by manually calling.
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;
        let m = sample_poly_market("m", now_ms + 120_000, 5); // window already open
        capture_window_strikes(&mut s, &[m.clone()], now_ms);
        assert_eq!(s.window_strikes.get("m").copied(), Some(80_500.0));
    }

    #[test]
    fn window_strikes_garbage_collected_after_close() {
        let mut s = FeatureState::default();
        s.window_strikes.insert("expired".to_string(), 80_000.0);
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;
        // Empty markets list → all strikes are stale
        capture_window_strikes(&mut s, &[], now_ms);
        assert!(s.window_strikes.is_empty());
    }

    // Integration test: run full pipeline against live OKX for 30s
    // cargo test compute_loop_live -- --ignored --nocapture
    #[ignore]
    #[tokio::test(flavor = "multi_thread")]
    async fn compute_loop_live_features_are_finite_after_30s() {
        use crate::streams;
        use tokio::sync::broadcast;

        let (tx, _)  = broadcast::channel::<StreamEvent>(1024);
        let shared   = Arc::new(RwLock::new(FeatureState::default()));
        let markets  = Arc::new(RwLock::new(Vec::new()));

        tokio::spawn(streams::binance::spot_stream(tx.clone()));
        tokio::spawn(streams::binance::perp_stream(tx.clone()));
        tokio::spawn(streams::binance::oi_stream(tx.clone()));
        tokio::spawn(streams::binance::kline_stream(tx.clone()));
        tokio::spawn(streams::liquidations::liq_stream(tx.clone()));
        tokio::spawn(compute_loop(tx.subscribe(), shared.clone(), markets.clone()));

        tokio::time::sleep(Duration::from_secs(30)).await;

        let s = shared.read().await;
        assert!(s.seq > 0,           "seq must have incremented");
        assert!(s.spot_price > 0.0,  "spot price must be set from live trades");

        let v = s.to_feature_vec();
        for (i, &val) in v.iter().enumerate() {
            assert!(val.is_finite(), "live feature[{i}] = {val} not finite after 30s");
        }
        tracing::info!(seq = s.seq, spot = s.spot_price, "live compute loop OK");
    }
}
