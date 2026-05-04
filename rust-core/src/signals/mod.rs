pub mod intramarket;
pub mod oracle_arb;
pub mod regime;

use std::sync::Arc;

use rayon::prelude::*;
use tokio::sync::RwLock;
use tokio::time::{Duration, MissedTickBehavior};

use crate::features::FeatureState;
use crate::storage::{SignalLog, SignalRow};
use crate::streams::polymarket::PolyMarket;
use intramarket::{check_intramarket_arb, IntramarketArbSignal};
use oracle_arb::{check_oracle_arb, OracleArbSignal};
use regime::{classify_regime, Regime};

/// Direction of a trade signal — which side of a binary market to buy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Buy the "Up" (YES) token — bet that BTC closes higher than the strike
    Up,
    /// Buy the "Down" (NO) token — bet that BTC closes equal-or-lower than the strike
    Down,
}

// ── E5: Signal integration ───────────────────────────────────────────────────

/// The decision emitted by the signal evaluator each tick. Variants are
/// listed in priority order — the first that fires wins.
#[derive(Debug, Clone, PartialEq)]
pub enum SignalDecision {
    /// No actionable signal this tick.
    None,
    /// Risk-free arb: buy both sides for less than $1 net of fees.
    /// Highest priority — always taken when present.
    Intramarket(IntramarketArbSignal),
    /// Oracle arb: market mispricing the binary option vs Chainlink.
    /// Second priority — the highest-edge directional signal.
    Oracle(OracleArbSignal),
}

/// Configuration thresholds for the signal evaluator. Pulled from .env
/// and passed in by the loop runner.
#[derive(Debug, Clone, Copy)]
pub struct SignalConfig {
    pub fee_rate:                   f64,   // Polymarket taker fee (typ. 0.018)
    pub intramarket_min_profit:     f64,   // Min net profit per share (typ. 0.005)
    pub oracle_arb_threshold:       f64,   // Min edge for oracle arb (typ. 0.04)
    pub min_time_remaining_secs:    f64,   // Skip if window resolves too soon (typ. 5)

    // Sanity gates — protect data quality during observe-only mode and
    // protect the bankroll once execution is live.
    pub min_liquidity_usd:          f64,   // Skip thin books (typ. 100)
    pub max_spread:                 f64,   // Skip broken books (typ. 0.10)
    pub min_window_age_secs:        f64,   // Skip just-opened windows (typ. 15)

    // S7.2 — depth-aware gates. A BUY oracle signal hits the ASK side
    // specifically — we want enough size at the touch to absorb our bet
    // without consuming the whole touch (which would mean huge slippage).
    /// Skip oracle BUYs if the touched ASK side has < this in dollars (typ. 50).
    pub min_ask_depth_usd:          f64,
    /// Reject a fill that would consume more than this fraction of the touch
    /// (typ. 0.5 = 50%). Caller passes their bet; we check at signal time
    /// using max_bet_dollars as the conservative bound.
    pub max_touch_consumption:      f64,
    /// Caller's max bet — used in the touch-consumption check. Default $30.
    pub max_bet_dollars:            f64,
}

impl Default for SignalConfig {
    fn default() -> Self {
        Self {
            fee_rate:                0.018,
            intramarket_min_profit:  0.005,
            oracle_arb_threshold:    0.04,
            min_time_remaining_secs: 5.0,
            min_liquidity_usd:       100.0,
            max_spread:              0.10,
            min_window_age_secs:     15.0,
            min_ask_depth_usd:       50.0,
            max_touch_consumption:   0.5,
            max_bet_dollars:         30.0,
        }
    }
}

/// Evaluate all signals against the current state in priority order.
///
/// Priority:
///   1. Intramarket arb — risk-free, fires regardless of regime
///   2. Oracle arb       — highest-edge directional, fires regardless of regime
///   3. (Future I3) ML signal — gated on regime != HighVolEvent
///
/// Returns the first signal that fires (priority order).
pub fn evaluate_signals(state: &FeatureState, cfg: &SignalConfig) -> SignalDecision {
    // No primary market → nothing to bet on
    if state.primary_market_id.is_none() {
        return SignalDecision::None;
    }

    // Window must be open with enough time left to actually fill
    if state.time_to_close < cfg.min_time_remaining_secs {
        return SignalDecision::None;
    }

    // Sanity gates — protect data quality and capital
    if state.spread > cfg.max_spread {
        return SignalDecision::None; // broken or thin book
    }
    if state.primary_yes_liquidity_usd < cfg.min_liquidity_usd
        || state.primary_no_liquidity_usd  < cfg.min_liquidity_usd {
        return SignalDecision::None; // not enough depth to fill cleanly
    }
    if state.primary_window_age_secs < cfg.min_window_age_secs {
        return SignalDecision::None; // strike just captured — wait for stable signal
    }

    let yes_ask = state.poly_yes_price;
    let no_ask  = state.poly_no_price;

    // ── Priority 1: Intramarket arb ───────────────────────────────────────
    if let Some(sig) = check_intramarket_arb(
        yes_ask, no_ask,
        cfg.fee_rate,
        cfg.intramarket_min_profit,
    ) {
        return SignalDecision::Intramarket(sig);
    }

    // ── Priority 2: Oracle arb ────────────────────────────────────────────
    // Strike is required — if we don't have a captured chainlink at window
    // open, skip oracle arb (window_strikes HashMap stays empty until the
    // chainlink polling loop has populated it, and primary market has it).
    if state.window_open_price > 0.0 {
        let time_remaining_min = state.time_to_close / 60.0;
        if let Some(sig) = check_oracle_arb(
            state.spot_price,
            state.window_open_price,
            state.vol_5m,
            time_remaining_min,
            yes_ask,
            no_ask,
            cfg.oracle_arb_threshold,
        ) {
            return SignalDecision::Oracle(sig);
        }
    }

    // (Future I3) ML signal — regime-gated, requires Python IPC

    SignalDecision::None
}

/// Compute the current market regime from FeatureState. Helper for the
/// future ML signal layer (I3) — already exposed so callers can log it.
pub fn current_regime(state: &FeatureState) -> Regime {
    classify_regime(state.vol_z_score, state.vol_ratio, state.autocorr_lag1)
}

// ── S6: per-market signal evaluation ────────────────────────────────────────

/// Stateless signal evaluation for a SPECIFIC market, independent of
/// `state.primary_market_id`. Looks up the per-market book in
/// `state.asset_books` and applies all gates.
///
/// `now_ms` is passed in so tests can drive deterministic timing without
/// mocking the system clock.
pub fn evaluate_signals_for_market(
    market: &PolyMarket,
    state:  &FeatureState,
    cfg:    &SignalConfig,
    now_ms: u64,
) -> SignalDecision {
    // Window must be open with enough time left to fill.
    let time_to_close_secs = market.time_to_close_secs(now_ms);
    if time_to_close_secs < cfg.min_time_remaining_secs {
        return SignalDecision::None;
    }

    // Per-market books — both sides required.
    let yes_book = match state.asset_books.get(&market.up_token_id) {
        Some(b) => b,
        None    => return SignalDecision::None,
    };
    let no_book  = match state.asset_books.get(&market.down_token_id) {
        Some(b) => b,
        None    => return SignalDecision::None,
    };

    let yes_ask = yes_book.best_ask;
    let no_ask  = no_book.best_ask;
    let spread  = (yes_book.best_ask - yes_book.best_bid).max(0.0);

    // Dollar liquidity at the touch — sum of bid/ask sides.
    let yes_liq_usd =
        yes_book.best_bid_size * yes_book.best_bid + yes_book.best_ask_size * yes_book.best_ask;
    let no_liq_usd  =
        no_book.best_bid_size  * no_book.best_bid  + no_book.best_ask_size  * no_book.best_ask;

    // Sanity gates
    if spread > cfg.max_spread {
        return SignalDecision::None;
    }
    if yes_liq_usd < cfg.min_liquidity_usd || no_liq_usd < cfg.min_liquidity_usd {
        return SignalDecision::None;
    }

    // Window age — we want a stable strike before firing.
    let (capture_ms, strike) = match state.window_strikes.get(&market.id) {
        Some(&(c, s)) => (c, s),
        None          => (0u64, 0.0),  // no strike yet → oracle path will skip
    };
    if capture_ms > 0 {
        let window_age = (now_ms.saturating_sub(capture_ms)) as f64 / 1000.0;
        if window_age < cfg.min_window_age_secs {
            return SignalDecision::None;
        }
    }

    // Priority 1: Intramarket arb — risk-free, fires regardless of regime.
    if let Some(sig) = check_intramarket_arb(
        yes_ask, no_ask, cfg.fee_rate, cfg.intramarket_min_profit,
    ) {
        return SignalDecision::Intramarket(sig);
    }

    // Priority 2: Oracle arb — needs a captured strike.
    if strike > 0.0 && state.spot_price > 0.0 {
        let time_remaining_min = time_to_close_secs / 60.0;
        if let Some(sig) = check_oracle_arb(
            state.spot_price, strike, state.vol_5m, time_remaining_min,
            yes_ask, no_ask, cfg.oracle_arb_threshold,
        ) {
            // S7.2 — depth-aware gate. We'd hit the ASK side of whichever
            // direction the signal points. Reject if depth is thin or our
            // bet would consume too much of the touch (worst-case max bet).
            use crate::signals::Direction;
            let (touch_size, touch_price) = match sig.direction {
                Direction::Up   => (yes_book.best_ask_size, yes_book.best_ask),
                Direction::Down => (no_book.best_ask_size,  no_book.best_ask),
            };
            let touch_usd = touch_size * touch_price;
            if touch_usd < cfg.min_ask_depth_usd {
                return SignalDecision::None;   // too thin to fill cleanly
            }
            if touch_usd > 0.0 && cfg.max_bet_dollars / touch_usd > cfg.max_touch_consumption {
                return SignalDecision::None;   // bet would eat too much of the touch
            }
            return SignalDecision::Oracle(sig);
        }
    }

    SignalDecision::None
}

/// Evaluate every active market in parallel via rayon. Returns one
/// (market_id, SignalDecision) per market — closed markets are filtered out.
pub fn evaluate_all_markets(
    state:   &FeatureState,
    markets: &[PolyMarket],
    cfg:     &SignalConfig,
    now_ms:  u64,
) -> Vec<(String, SignalDecision)> {
    markets
        .par_iter()
        .filter(|m| m.close_time_ms > now_ms)
        .map(|m| (m.id.clone(), evaluate_signals_for_market(m, state, cfg, now_ms)))
        .collect()
}

/// Async signal loop: evaluates every 500ms, logs whenever a signal fires.
/// Persists every fired signal to SQLite for observe-only validation.
/// Execution wiring (J5) reads the same shared state and consumes the signal.
pub async fn signal_loop(
    state:   Arc<RwLock<FeatureState>>,
    cfg:     SignalConfig,
    log_db:  Arc<SignalLog>,
) {
    let mut tick = tokio::time::interval(Duration::from_millis(500));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tick.tick().await;
        let s = state.read().await;
        let decision = evaluate_signals(&s, &cfg);
        let regime   = current_regime(&s);

        match &decision {
            SignalDecision::None => {} // Quiet on no-op
            SignalDecision::Intramarket(sig) => {
                tracing::info!(
                    market    = ?s.primary_market_id,
                    yes_ask   = sig.yes_ask,
                    no_ask    = sig.no_ask,
                    net       = sig.net_profit,
                    ?regime,
                    "INTRAMARKET ARB"
                );
                if let Err(e) = persist_signal(&log_db, &s, regime, &decision) {
                    tracing::error!(error = %e, "failed to persist signal to sqlite");
                }
            }
            SignalDecision::Oracle(sig) => {
                tracing::info!(
                    market     = ?s.primary_market_id,
                    direction  = ?sig.direction,
                    edge       = sig.edge,
                    fair       = sig.fair_value,
                    ask        = sig.market_price,
                    confidence = sig.confidence,
                    t_close_s  = sig.time_to_close_secs,
                    ?regime,
                    "ORACLE ARB"
                );
                if let Err(e) = persist_signal(&log_db, &s, regime, &decision) {
                    tracing::error!(error = %e, "failed to persist signal to sqlite");
                }
            }
        }
    }
}

fn persist_signal(
    log_db:   &SignalLog,
    state:    &FeatureState,
    regime:   Regime,
    decision: &SignalDecision,
) -> anyhow::Result<()> {
    let market_id = state.primary_market_id.clone()
        .ok_or_else(|| anyhow::anyhow!("no primary market"))?;
    let now_ms    = chrono::Utc::now().timestamp_millis() as u64;
    let regime_s = match regime {
        Regime::LowVolRanging  => "ranging",
        Regime::LowVolTrending => "trending",
        Regime::HighVolEvent   => "event",
    }.to_string();

    let (signal_type, direction, fair_value, edge, confidence, bet) = match decision {
        SignalDecision::Intramarket(s) => {
            ("intramarket".to_string(), None, 1.0, s.net_profit, 1.0, kelly_for_intramarket(s))
        }
        SignalDecision::Oracle(s) => {
            let dir = match s.direction {
                Direction::Up   => Some("up".to_string()),
                Direction::Down => Some("down".to_string()),
            };
            let bet = crate::execution::kelly::kelly_size(
                s.fair_value, s.market_price, 1500.0, 0.25, 30.0,
            );
            ("oracle".to_string(), dir, s.fair_value, s.edge, s.confidence, bet)
        }
        SignalDecision::None => return Ok(()),
    };

    log_db.insert_signal(&SignalRow {
        fired_at_ms:            now_ms,
        market_id,
        signal_type,
        direction,
        spot_price:             state.spot_price,
        chainlink_price:        state.chainlink_price,
        strike_price:           state.window_open_price,
        yes_ask:                state.poly_yes_price,
        no_ask:                 state.poly_no_price,
        vol_5m:                 state.vol_5m,
        time_to_close_secs:     state.time_to_close,
        window_age_secs:        state.primary_window_age_secs,
        yes_liquidity_usd:      state.primary_yes_liquidity_usd,
        no_liquidity_usd:       state.primary_no_liquidity_usd,
        spread:                 state.spread,
        regime:                 regime_s,
        fair_value,
        edge,
        confidence,
        would_have_bet_dollars: bet,
    })?;
    Ok(())
}

fn kelly_for_intramarket(s: &IntramarketArbSignal) -> f64 {
    // For intramarket, we'd buy both sides for a guaranteed payoff.
    // Practical position sizing: cap at $30 to match max_bet.
    // This is simplified for the observe-mode log — execution layer (J)
    // will compute a more nuanced size considering both legs.
    30.0_f64.min(1500.0 * 0.02 * (s.net_profit * 100.0).clamp(1.0, 10.0))
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::features::FeatureState;

    fn calm_state_with_market() -> FeatureState {
        let mut s = FeatureState::default();
        s.primary_market_id           = Some("test-market".to_string());
        s.primary_duration_min        = 5;
        s.time_to_close               = 60.0;
        s.spot_price                  = 80_000.0;
        s.window_open_price           = 80_000.0;
        s.vol_5m                      = 0.001;
        s.vol_30m                     = 0.001;
        s.vol_z_score                 = 0.0;
        s.vol_ratio                   = 1.0;
        s.autocorr_lag1               = 0.0;
        s.poly_yes_price              = 0.50;
        s.poly_no_price               = 0.50;
        s.spread                      = 0.02;     // healthy, below max
        s.primary_yes_liquidity_usd   = 500.0;    // above min
        s.primary_no_liquidity_usd    = 500.0;
        s.primary_window_age_secs     = 60.0;     // window has been open 1 min
        s
    }

    // ── No-op cases ───────────────────────────────────────────────────────

    #[test]
    fn no_primary_market_returns_none() {
        let mut s = calm_state_with_market();
        s.primary_market_id = None;
        let cfg = SignalConfig::default();
        assert_eq!(evaluate_signals(&s, &cfg), SignalDecision::None);
    }

    #[test]
    fn window_too_close_to_resolving_returns_none() {
        let mut s = calm_state_with_market();
        s.time_to_close = 3.0;  // less than 5s default
        let cfg = SignalConfig::default();
        assert_eq!(evaluate_signals(&s, &cfg), SignalDecision::None);
    }

    #[test]
    fn fairly_priced_market_returns_none() {
        // Fair = 0.5 (ATM), market at 0.50 → no edge anywhere
        let s = calm_state_with_market();
        let cfg = SignalConfig::default();
        assert_eq!(evaluate_signals(&s, &cfg), SignalDecision::None);
    }

    // ── Intramarket priority ──────────────────────────────────────────────

    #[test]
    fn intramarket_takes_priority_over_oracle() {
        let mut s = calm_state_with_market();
        // Price BTC strongly above strike (oracle would fire) AND give arb opportunity
        s.spot_price       = 80_500.0;
        s.poly_yes_price   = 0.40;  // both sides cheap = arb
        s.poly_no_price    = 0.40;
        let cfg = SignalConfig::default();
        match evaluate_signals(&s, &cfg) {
            SignalDecision::Intramarket(_) => {} // expected
            other => panic!("expected Intramarket, got {other:?}"),
        }
    }

    #[test]
    fn intramarket_fires_on_clear_arb() {
        let mut s = calm_state_with_market();
        s.poly_yes_price = 0.42;
        s.poly_no_price  = 0.42;
        let cfg = SignalConfig::default();
        match evaluate_signals(&s, &cfg) {
            SignalDecision::Intramarket(sig) => {
                assert!(sig.net_profit > 0.005);
            }
            other => panic!("expected Intramarket, got {other:?}"),
        }
    }

    // ── Oracle arb ────────────────────────────────────────────────────────

    #[test]
    fn oracle_fires_when_market_mispriced() {
        let mut s = calm_state_with_market();
        // BTC clearly up: strong fair value, but market ask is low → underpriced YES
        s.spot_price       = 80_200.0;
        s.window_open_price = 80_000.0;
        s.vol_5m           = 0.0005;
        s.time_to_close    = 30.0;  // 30s remaining, deeply ITM
        s.poly_yes_price   = 0.70;  // way underpriced — fair is ~1.0
        s.poly_no_price    = 0.35;  // total = 1.05 — no intramarket arb
        let cfg = SignalConfig::default();
        match evaluate_signals(&s, &cfg) {
            SignalDecision::Oracle(sig) => {
                assert_eq!(sig.direction, Direction::Up);
                assert!(sig.edge > 0.20);
            }
            other => panic!("expected Oracle, got {other:?}"),
        }
    }

    #[test]
    fn oracle_skipped_without_strike() {
        let mut s = calm_state_with_market();
        s.window_open_price = 0.0;  // strike not yet captured
        // Set up conditions that would otherwise trigger oracle arb
        s.spot_price     = 80_500.0;
        s.poly_yes_price = 0.55;
        s.poly_no_price  = 0.50;
        let cfg = SignalConfig::default();
        // Should be None — no strike means oracle math can't run
        assert_eq!(evaluate_signals(&s, &cfg), SignalDecision::None);
    }

    // ── Regime helper ─────────────────────────────────────────────────────

    #[test]
    fn current_regime_reads_state() {
        let mut s = FeatureState::default();
        s.vol_z_score = 3.0;
        assert_eq!(current_regime(&s), Regime::HighVolEvent);

        s.vol_z_score = 0.0;
        s.autocorr_lag1 = 0.30;
        assert_eq!(current_regime(&s), Regime::LowVolTrending);
    }

    // ── Direction enum ────────────────────────────────────────────────────

    #[test]
    fn direction_eq_and_clone() {
        assert_eq!(Direction::Up, Direction::Up);
        assert_ne!(Direction::Up, Direction::Down);
        let d = Direction::Up;
        assert_eq!(d, d.clone());
    }

    // ── Default config ────────────────────────────────────────────────────

    #[test]
    fn default_config_has_sensible_values() {
        let cfg = SignalConfig::default();
        assert!((cfg.fee_rate - 0.018).abs() < 1e-10);
        assert!(cfg.oracle_arb_threshold > 0.0 && cfg.oracle_arb_threshold < 0.5);
        assert!(cfg.intramarket_min_profit > 0.0);
        assert!(cfg.min_time_remaining_secs >= 0.0);
    }

    // ── Real-market scenario coverage ─────────────────────────────────────

    #[test]
    fn scenario_late_window_strong_lead_oracle_only() {
        let mut s = calm_state_with_market();
        s.spot_price       = 80_300.0;
        s.window_open_price = 80_000.0;
        s.vol_5m           = 0.0005;
        s.time_to_close    = 20.0;
        s.poly_yes_price   = 0.85;
        s.poly_no_price    = 0.20;  // sum 1.05 — no intramarket arb
        let cfg = SignalConfig::default();
        match evaluate_signals(&s, &cfg) {
            SignalDecision::Oracle(sig) => assert_eq!(sig.direction, Direction::Up),
            other => panic!("expected Oracle, got {other:?}"),
        }
    }

    #[test]
    fn scenario_news_spike_oracle_signals_calibrated() {
        // High vol regime — oracle math should pull fair value toward 0.5
        // Even a $50 lead at 1%/min vol with 3min left only gives fair ~0.514
        // If market shows YES at 0.55, oracle should NOT fire
        let mut s = calm_state_with_market();
        s.spot_price       = 80_050.0;
        s.window_open_price = 80_000.0;
        s.vol_5m           = 0.01;     // 1%/min — extreme
        s.time_to_close    = 180.0;
        s.poly_yes_price   = 0.55;
        s.poly_no_price    = 0.50;     // sum 1.05 — no intramarket
        let cfg = SignalConfig::default();
        assert_eq!(evaluate_signals(&s, &cfg), SignalDecision::None,
                   "high vol regime should not produce oracle signal on weak edge");
    }

    #[test]
    fn scenario_intramarket_overlap_with_oracle() {
        // Both sides have edge AND yes+no < 1
        // Intramarket should win priority
        let mut s = calm_state_with_market();
        s.spot_price       = 80_200.0;
        s.window_open_price = 80_000.0;
        s.vol_5m           = 0.0005;
        s.time_to_close    = 30.0;
        s.poly_yes_price   = 0.40;
        s.poly_no_price    = 0.40;     // sum 0.80 — clear intramarket arb
        let cfg = SignalConfig::default();
        match evaluate_signals(&s, &cfg) {
            SignalDecision::Intramarket(_) => {} // expected
            other => panic!("intramarket has priority; got {other:?}"),
        }
    }

    #[test]
    fn scenario_15min_window_just_opened() {
        // Window has 14 min remaining, BTC modestly above strike.
        // Should fire oracle signal IF significant mispricing exists.
        let mut s = calm_state_with_market();
        s.spot_price       = 80_300.0;
        s.window_open_price = 80_000.0;
        s.vol_5m           = 0.001;
        s.time_to_close    = 14.0 * 60.0;
        s.poly_yes_price   = 0.65;     // fair ~0.88, market 0.65 = 0.23 edge
        s.poly_no_price    = 0.40;     // sum 1.05, no intramarket
        let cfg = SignalConfig::default();
        match evaluate_signals(&s, &cfg) {
            SignalDecision::Oracle(sig) => assert_eq!(sig.direction, Direction::Up),
            other => panic!("expected Oracle, got {other:?}"),
        }
    }

    // ── Performance ───────────────────────────────────────────────────────

    // ── S6: per-market evaluator ──────────────────────────────────────────

    fn sample_market_for(id: &str, close_ms: u64) -> PolyMarket {
        PolyMarket {
            id: id.into(),
            question: "Bitcoin Up or Down?".into(),
            up_token_id:   format!("up-{id}"),
            down_token_id: format!("down-{id}"),
            close_time_ms: close_ms,
            duration_min:  5,
            liquidity_usd: 1_000.0,
        }
    }

    fn book(bid: f64, ask: f64, sz: f64) -> crate::features::BookState {
        crate::features::BookState {
            best_bid: bid,
            best_ask: ask,
            best_bid_size: sz,
            best_ask_size: sz,
            last_trade_price: (bid + ask) / 2.0,
            timestamp_ms: 0,
        }
    }

    #[test]
    fn per_market_eval_returns_none_without_books() {
        let s = FeatureState::default();
        let m = sample_market_for("m", 2_000_000_000_000);
        let d = evaluate_signals_for_market(&m, &s, &SignalConfig::default(), 1_000_000_000_000);
        assert_eq!(d, SignalDecision::None);
    }

    #[test]
    fn per_market_eval_uses_per_market_book() {
        let mut s = FeatureState::default();
        s.spot_price = 80_300.0;
        s.window_strikes.insert("m".into(), (0, 80_000.0));   // captured strike
        let now_ms = 1_000_000_000_000u64;
        let m = sample_market_for("m", now_ms + 60_000);
        // Books — yes underpriced (oracle should fire)
        s.asset_books.insert(m.up_token_id.clone(),   book(0.65, 0.70, 1000.0));
        s.asset_books.insert(m.down_token_id.clone(), book(0.30, 0.35, 1000.0));
        let mut cfg = SignalConfig::default();
        cfg.min_window_age_secs = 0.0;  // window is fresh in this test
        let d = evaluate_signals_for_market(&m, &s, &cfg, now_ms);
        match d {
            SignalDecision::Oracle(sig) => assert_eq!(sig.direction, Direction::Up),
            other => panic!("expected Oracle, got {other:?}"),
        }
    }

    #[test]
    fn per_market_eval_filters_thin_books() {
        let mut s = FeatureState::default();
        s.spot_price = 80_300.0;
        s.window_strikes.insert("m".into(), (0, 80_000.0));
        let now_ms = 1_000_000_000_000u64;
        let m = sample_market_for("m", now_ms + 60_000);
        // Tiny size — yes_liq_usd < 100
        s.asset_books.insert(m.up_token_id.clone(),   book(0.65, 0.70, 1.0));
        s.asset_books.insert(m.down_token_id.clone(), book(0.30, 0.35, 1.0));
        let cfg = SignalConfig::default();
        assert_eq!(
            evaluate_signals_for_market(&m, &s, &cfg, now_ms),
            SignalDecision::None,
        );
    }

    #[test]
    fn per_market_eval_filters_closed_market() {
        let mut s = FeatureState::default();
        s.spot_price = 80_300.0;
        let m = sample_market_for("m", 1_000_000_000_000);
        let now_ms = m.close_time_ms + 10_000;   // already past close
        s.asset_books.insert(m.up_token_id.clone(),   book(0.65, 0.70, 1000.0));
        s.asset_books.insert(m.down_token_id.clone(), book(0.30, 0.35, 1000.0));
        assert_eq!(
            evaluate_signals_for_market(&m, &s, &SignalConfig::default(), now_ms),
            SignalDecision::None,
        );
    }

    #[test]
    fn per_market_eval_intramarket_priority() {
        let mut s = FeatureState::default();
        s.spot_price = 80_300.0;
        s.window_strikes.insert("m".into(), (0, 80_000.0));
        let now_ms = 1_000_000_000_000u64;
        let m = sample_market_for("m", now_ms + 60_000);
        // Both sides cheap → intramarket arb
        s.asset_books.insert(m.up_token_id.clone(),   book(0.40, 0.42, 1000.0));
        s.asset_books.insert(m.down_token_id.clone(), book(0.40, 0.42, 1000.0));
        let mut cfg = SignalConfig::default();
        cfg.min_window_age_secs = 0.0;
        match evaluate_signals_for_market(&m, &s, &cfg, now_ms) {
            SignalDecision::Intramarket(_) => {}
            other => panic!("expected Intramarket, got {other:?}"),
        }
    }

    // ── evaluate_all_markets (parallel) ───────────────────────────────────

    #[test]
    fn evaluate_all_markets_runs_across_markets() {
        let mut s = FeatureState::default();
        s.spot_price = 80_300.0;
        let now_ms = 1_000_000_000_000u64;

        // 3 markets — m1 has oracle setup, m2 thin, m3 closed
        let m1 = sample_market_for("m1", now_ms + 60_000);
        let m2 = sample_market_for("m2", now_ms + 60_000);
        let m3 = sample_market_for("m3", now_ms - 1_000);   // already closed
        s.window_strikes.insert("m1".into(), (0, 80_000.0));
        s.window_strikes.insert("m2".into(), (0, 80_000.0));
        s.asset_books.insert(m1.up_token_id.clone(),   book(0.65, 0.70, 1000.0));
        s.asset_books.insert(m1.down_token_id.clone(), book(0.30, 0.35, 1000.0));
        s.asset_books.insert(m2.up_token_id.clone(),   book(0.65, 0.70, 1.0));     // thin
        s.asset_books.insert(m2.down_token_id.clone(), book(0.30, 0.35, 1.0));

        let mut cfg = SignalConfig::default();
        cfg.min_window_age_secs = 0.0;

        let results = evaluate_all_markets(&s, &[m1.clone(), m2.clone(), m3.clone()], &cfg, now_ms);
        // m3 filtered (closed) — only m1 and m2 returned
        assert_eq!(results.len(), 2);
        let m1_dec = results.iter().find(|(id, _)| id == "m1").map(|(_, d)| d.clone());
        let m2_dec = results.iter().find(|(id, _)| id == "m2").map(|(_, d)| d.clone());
        assert!(matches!(m1_dec, Some(SignalDecision::Oracle(_))));
        assert_eq!(m2_dec, Some(SignalDecision::None));   // thin → None
    }

    // ── S7.2: depth-aware gate ────────────────────────────────────────────

    #[test]
    fn oracle_signal_rejected_when_ask_depth_thin() {
        let mut s = FeatureState::default();
        s.spot_price = 80_300.0;
        s.window_strikes.insert("m".into(), (0, 80_000.0));
        let now_ms = 1_000_000_000_000u64;
        let m = sample_market_for("m", now_ms + 60_000);
        // YES underpriced (oracle would fire) but ASK touch is tiny — 5 shares × 0.45 = $2.25
        s.asset_books.insert(m.up_token_id.clone(),   book(0.40, 0.45, 5.0));
        s.asset_books.insert(m.down_token_id.clone(), book(0.50, 0.55, 1000.0));
        let mut cfg = SignalConfig::default();
        cfg.min_window_age_secs = 0.0;
        cfg.min_liquidity_usd   = 0.0;     // turn off OTHER gate so depth gate is exercised
        cfg.min_ask_depth_usd   = 50.0;
        // Should be None due to thin ask depth on YES
        assert_eq!(
            evaluate_signals_for_market(&m, &s, &cfg, now_ms),
            SignalDecision::None,
        );
    }

    #[test]
    fn oracle_signal_rejected_when_bet_too_big_for_touch() {
        let mut s = FeatureState::default();
        s.spot_price = 80_300.0;
        s.window_strikes.insert("m".into(), (0, 80_000.0));
        let now_ms = 1_000_000_000_000u64;
        let m = sample_market_for("m", now_ms + 60_000);
        // YES depth = 100 × 0.45 = $45 — passes min_ask_depth ($50? NO. set to $40)
        // But max_bet ($30) / $45 = 67% — exceeds 50% cap
        s.asset_books.insert(m.up_token_id.clone(),   book(0.40, 0.45, 100.0));
        s.asset_books.insert(m.down_token_id.clone(), book(0.50, 0.55, 1000.0));
        let mut cfg = SignalConfig::default();
        cfg.min_window_age_secs    = 0.0;
        cfg.min_liquidity_usd      = 0.0;
        cfg.min_ask_depth_usd      = 40.0;
        cfg.max_bet_dollars        = 30.0;
        cfg.max_touch_consumption  = 0.5;
        assert_eq!(
            evaluate_signals_for_market(&m, &s, &cfg, now_ms),
            SignalDecision::None,
        );
    }

    #[test]
    fn oracle_signal_accepted_with_deep_ask() {
        let mut s = FeatureState::default();
        s.spot_price = 80_300.0;
        s.window_strikes.insert("m".into(), (0, 80_000.0));
        let now_ms = 1_000_000_000_000u64;
        let m = sample_market_for("m", now_ms + 60_000);
        // Deep book — both sides ample
        s.asset_books.insert(m.up_token_id.clone(),   book(0.40, 0.45, 5000.0));
        s.asset_books.insert(m.down_token_id.clone(), book(0.50, 0.55, 5000.0));
        let mut cfg = SignalConfig::default();
        cfg.min_window_age_secs = 0.0;
        let d = evaluate_signals_for_market(&m, &s, &cfg, now_ms);
        match d {
            SignalDecision::Oracle(_) => {}   // expected
            other => panic!("expected Oracle, got {other:?}"),
        }
    }

    #[test]
    fn evaluate_all_markets_handles_empty_list() {
        let s = FeatureState::default();
        let r = evaluate_all_markets(&s, &[], &SignalConfig::default(), 1_000_000_000_000);
        assert!(r.is_empty());
    }

    #[test]
    fn perf_evaluate_under_5us() {
        let s = calm_state_with_market();
        let cfg = SignalConfig::default();
        let start = std::time::Instant::now();
        for _ in 0..10_000 {
            std::hint::black_box(evaluate_signals(&s, &cfg));
        }
        let per_ns = start.elapsed().as_nanos() / 10_000;
        assert!(per_ns < 5_000, "expected < 5us per eval, got {per_ns}ns");
    }
}
