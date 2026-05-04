// C1: FeatureState — all rolling history buffers and computed feature values.
// Computation functions are added in C3-C10 and wired in C11.

use std::collections::{HashMap, VecDeque};

use crate::streams::LiqSide;
use super::compute::RollingWindow;

/// Latest CLOB book snapshot for a single asset (Up or Down token).
#[derive(Debug, Clone, Copy, Default)]
pub struct BookState {
    pub best_bid:         f64,
    pub best_ask:         f64,
    pub best_bid_size:    f64,
    pub best_ask_size:    f64,
    pub last_trade_price: f64,
    pub timestamp_ms:     u64,
}

// Capacities (in number of 1-minute candles unless noted)
const HIST_60M:  usize = 60;   // 60 candles = 1 hour of 1-min history
const OI_HIST:   usize = 120;  // ~2 min of OI ticks at 1-sec updates
const LIQ_HIST:  usize = 1000; // up to 1000 liquidation events in the buffer

/// All feature values computed from the live stream, plus the raw history
/// buffers needed to compute them. Updated every 500ms by the compute loop.
///
/// Fields are grouped as: history → returns → volatility → technicals →
/// cross-asset → Polymarket microstructure → time → raw prices.
#[derive(Debug, Clone)]
pub struct FeatureState {
    // ── History buffers (inputs to computation) ───────────────────────────
    pub closes_1m:   RollingWindow<f64>,  // 1-min close prices
    pub volumes_1m:  RollingWindow<f64>,  // 1-min volumes (contracts)
    pub returns_1m:  RollingWindow<f64>,  // log returns between closes
    pub oi_history:  VecDeque<(u64, f64)>,              // (timestamp_ms, oi_btc)
    pub liq_history: VecDeque<(u64, LiqSide, f64)>,    // (timestamp_ms, side, qty)

    // ── Polymarket per-market state (D5/D6) ───────────────────────────────
    pub asset_books:    HashMap<String, BookState>,    // asset_id -> latest book
    pub window_strikes: HashMap<String, (u64, f64)>,   // market_id -> (capture_ms, strike)
    pub primary_market_id:        Option<String>,
    pub primary_yes_liquidity_usd: f64,                // best_bid_size*price + best_ask_size*price
    pub primary_no_liquidity_usd:  f64,
    pub primary_window_age_secs:   f64,                // seconds since strike captured (0 if no strike)
    pub primary_duration_min:      u32,                // 5 or 15 — derived from primary market

    // ── Feature vector — 26 fields, written to IPC every 500ms ───────────

    // Returns (C3)
    pub ret_1m: f64,
    pub ret_3m: f64,
    pub ret_5m: f64,

    // Volatility (C7)
    pub vol_5m:      f64,
    pub vol_30m:     f64,
    pub vol_ratio:   f64, // vol_5m / vol_30m
    pub vol_z_score: f64,

    // Technicals (C4-C6)
    pub rsi_14:        f64,
    pub bb_position:   f64, // 0-1 position within Bollinger Bands (20, 2σ)
    pub macd_signal:   f64,
    pub autocorr_lag1: f64, // Pearson lag-1 autocorrelation of returns (C8)

    // Cross-asset (C9-C10)
    pub funding_rate:    f64,
    pub oi_velocity_60s: f64, // d(OI)/dt over last 60s  (C9)
    pub liq_imbalance:   f64, // normalised (short_liq - long_liq) (C10)
    pub perp_basis:      f64, // mark_price - spot_price

    // Polymarket microstructure (D5-D6)
    pub poly_obi:         f64, // order-book imbalance YES side
    pub yes_vol_velocity: f64, // d(yes_volume)/dt
    pub arb_gap:          f64, // 1 - (yes_ask + no_ask)
    pub oracle_gap:       f64, // binary-option fair value - poly_yes_price
    pub time_to_close:    f64, // seconds until window closes
    pub spread:           f64, // yes_ask - yes_bid

    // Time (categorical, encoded as f64)
    pub hour_of_day:    f64, // 0-23
    pub minute_of_hour: f64, // 0-59
    pub day_of_week:    f64, // 0-6 (Mon=0)

    // Raw prices — needed by signal logic after feature computation
    pub chainlink_price:   f64,
    pub spot_price:        f64, // latest OKX spot trade price
    pub mark_price:        f64, // latest OKX perp mark price
    pub poly_yes_price:    f64, // up_book.best_ask (cost to buy YES)
    pub poly_no_price:     f64, // down_book.best_ask (cost to buy NO) — internal, not in feature vec
    pub window_open_price: f64, // strike — chainlink price captured at window open

    // Sequence counter — incremented every 500ms tick, read by Python IPC
    pub seq: u64,
}

impl Default for FeatureState {
    fn default() -> Self {
        Self {
            closes_1m:   RollingWindow::new(HIST_60M),
            volumes_1m:  RollingWindow::new(HIST_60M),
            returns_1m:  RollingWindow::new(HIST_60M),
            oi_history:  VecDeque::with_capacity(OI_HIST),
            liq_history: VecDeque::with_capacity(LIQ_HIST),

            asset_books:                  HashMap::new(),
            window_strikes:               HashMap::new(),
            primary_market_id:            None,
            primary_yes_liquidity_usd:    0.0,
            primary_no_liquidity_usd:     0.0,
            primary_window_age_secs:      0.0,
            primary_duration_min:         0,

            ret_1m: 0.0,
            ret_3m: 0.0,
            ret_5m: 0.0,

            vol_5m:      0.0,
            vol_30m:     0.0,
            vol_ratio:   1.0,
            vol_z_score: 0.0,

            rsi_14:        50.0,
            bb_position:   0.5,
            macd_signal:   0.0,
            autocorr_lag1: 0.0,

            funding_rate:    0.0,
            oi_velocity_60s: 0.0,
            liq_imbalance:   0.0,
            perp_basis:      0.0,

            poly_obi:         0.0,
            yes_vol_velocity: 0.0,
            arb_gap:          0.0,
            oracle_gap:       0.0,
            time_to_close:    300.0,
            spread:           0.02,

            hour_of_day:    0.0,
            minute_of_hour: 0.0,
            day_of_week:    0.0,

            chainlink_price:   0.0,
            spot_price:        0.0,
            mark_price:        0.0,
            poly_yes_price:    0.5,
            poly_no_price:     0.5,
            window_open_price: 0.0,

            seq: 0,
        }
    }
}

impl FeatureState {
    /// The 26-element slice written to the IPC pipe every tick.
    /// Order must match the Python deserializer exactly.
    pub fn to_feature_vec(&self) -> [f64; 26] {
        [
            self.oracle_gap,
            self.chainlink_price,
            self.spot_price,
            self.funding_rate,
            self.poly_yes_price,
            self.poly_obi,
            self.yes_vol_velocity,
            self.liq_imbalance,
            self.oi_velocity_60s,
            self.perp_basis,
            self.ret_1m,
            self.ret_3m,
            self.ret_5m,
            self.vol_5m,
            self.vol_ratio,
            self.rsi_14,
            self.bb_position,
            self.macd_signal,
            self.autocorr_lag1,
            self.vol_z_score,
            self.arb_gap,
            self.time_to_close,
            self.hour_of_day,
            self.day_of_week,
            self.spread,
            self.mark_price,
        ]
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_constructs_without_panic() {
        let s = FeatureState::default();
        assert_eq!(s.closes_1m.capacity(), 60);
        assert_eq!(s.volumes_1m.capacity(), 60);
        assert_eq!(s.returns_1m.capacity(), 60);
        assert!(s.closes_1m.is_empty());
        assert_eq!(s.seq, 0);
    }

    #[test]
    fn feature_vec_has_correct_length() {
        let s = FeatureState::default();
        let v = s.to_feature_vec();
        assert_eq!(v.len(), 26);
    }

    #[test]
    fn all_default_feature_values_are_finite() {
        let s = FeatureState::default();
        for (i, &v) in s.to_feature_vec().iter().enumerate() {
            assert!(v.is_finite(), "feature[{i}] is not finite: {v}");
        }
    }

    #[test]
    fn all_fields_are_accessible() {
        let mut s = FeatureState::default();
        // Write to every computed field to confirm they're pub
        s.ret_1m         = 0.001;
        s.ret_3m         = 0.002;
        s.ret_5m         = 0.003;
        s.vol_5m         = 0.01;
        s.vol_30m        = 0.008;
        s.vol_ratio      = 1.25;
        s.vol_z_score    = 0.5;
        s.rsi_14         = 55.0;
        s.bb_position    = 0.7;
        s.macd_signal    = 0.0005;
        s.autocorr_lag1  = -0.1;
        s.funding_rate   = 0.0001;
        s.oi_velocity_60s = 100.0;
        s.liq_imbalance  = 0.3;
        s.perp_basis     = 5.0;
        s.poly_obi       = 0.2;
        s.yes_vol_velocity = 10.0;
        s.arb_gap        = 0.02;
        s.oracle_gap     = 0.05;
        s.time_to_close  = 120.0;
        s.spread         = 0.01;
        s.hour_of_day    = 14.0;
        s.minute_of_hour = 30.0;
        s.day_of_week    = 2.0;
        s.chainlink_price  = 43_000.0;
        s.spot_price       = 43_005.0;
        s.mark_price       = 43_010.0;
        s.poly_yes_price   = 0.48;
        s.window_open_price = 43_000.0;
        s.seq              = 42;

        // Verify feature vec reflects updated values
        let v = s.to_feature_vec();
        assert!((v[0] - s.oracle_gap).abs() < 1e-12);    // oracle_gap first
        assert!((v[3] - s.funding_rate).abs() < 1e-12);  // funding_rate at index 3
    }

    #[test]
    fn liq_history_accepts_events() {
        let mut s = FeatureState::default();
        s.liq_history.push_back((1_000, LiqSide::Long,  0.1));
        s.liq_history.push_back((2_000, LiqSide::Short, 0.2));
        assert_eq!(s.liq_history.len(), 2);
    }
}
