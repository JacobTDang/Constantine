pub mod intramarket;
pub mod oracle_arb;
pub mod regime;

use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{Duration, MissedTickBehavior};

use crate::features::FeatureState;
use crate::storage::{SignalLog, SignalRow};
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
