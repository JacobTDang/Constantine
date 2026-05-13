// Strategy 05 — Cross-market statistical arbitrage signal layer.
//
// Pure-logic z-score evaluator. Given a pre-built StatArbPair (market A,
// market B, hedge ratio β, spread mean, spread stdev) and current mid
// prices on both legs, computes z = (spread - mean) / std and emits one
// of:
//   - EnterShortA: spread too high → short A, long B
//   - EnterLongA:  spread too low  → long A, short B
//   - Exit:        |z| < EXIT_THRESHOLD on an open pair
//   - StopOut:     |z| > STOPOUT_THRESHOLD (beta regime change)
//   - None
//
// The pair universe builder runs as a daily Python batch (Phase 3
// sidecar). This module only consumes its output and decides per-tick.

use serde::{Deserialize, Serialize};

pub const ENTRY_Z_THRESHOLD: f64 = 2.0;
pub const EXIT_Z_THRESHOLD: f64 = 0.5;
pub const STOPOUT_Z_THRESHOLD: f64 = 4.0;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StatArbPair {
    pub a_market_id:    String,
    pub a_token_id:     String,
    pub b_market_id:    String,
    pub b_token_id:     String,
    /// Hedge ratio: spread = a_price - beta * b_price
    pub beta:           f64,
    pub rho:            f64,
    pub spread_mean:    f64,
    pub spread_std:     f64,
    /// Whether we currently hold a position on this pair (true if open).
    pub is_open:        bool,
    /// If open, the side we entered: 1.0 = long A / short B, -1.0 = short A / long B
    pub open_direction: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum StatArbDecision {
    None,
    /// Spread too low (z < -ENTRY) → buy A, sell B
    EnterLongA { z: f64 },
    /// Spread too high (z > +ENTRY) → sell A, buy B
    EnterShortA { z: f64 },
    /// On an open position, |z| has reverted below EXIT → take profit
    Exit { z: f64 },
    /// |z| exceeded STOPOUT → cut loss
    StopOut { z: f64 },
}

pub fn evaluate_stat_arb(
    pair:        &StatArbPair,
    a_price:     f64,
    b_price:     f64,
) -> StatArbDecision {
    // Pre-conditions
    if pair.spread_std <= 0.0 {
        return StatArbDecision::None;
    }
    if !(0.02..=0.98).contains(&a_price) || !(0.02..=0.98).contains(&b_price) {
        return StatArbDecision::None;
    }

    let spread = a_price - pair.beta * b_price;
    let z = (spread - pair.spread_mean) / pair.spread_std;

    if pair.is_open {
        // Exit conditions take priority for open positions
        if z.abs() > STOPOUT_Z_THRESHOLD {
            return StatArbDecision::StopOut { z };
        }
        // Reversion exit: signed direction tells us which way we went in;
        // exit when spread has come back near mean
        if z.abs() < EXIT_Z_THRESHOLD {
            return StatArbDecision::Exit { z };
        }
        return StatArbDecision::None;
    }

    // Closed → look for entries
    if z > ENTRY_Z_THRESHOLD {
        StatArbDecision::EnterShortA { z }
    } else if z < -ENTRY_Z_THRESHOLD {
        StatArbDecision::EnterLongA { z }
    } else {
        StatArbDecision::None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pair(beta: f64, mean: f64, std: f64, is_open: bool, dir: f64) -> StatArbPair {
        StatArbPair {
            a_market_id:    "0xa".into(),
            a_token_id:     "yes-a".into(),
            b_market_id:    "0xb".into(),
            b_token_id:     "yes-b".into(),
            beta,
            rho:            0.80,
            spread_mean:    mean,
            spread_std:     std,
            is_open,
            open_direction: dir,
        }
    }

    #[test]
    fn closed_pair_high_spread_enters_short_a() {
        let p = pair(1.0, 0.0, 0.05, false, 0.0);
        // a = 0.60, b = 0.40 → spread = 0.20; mean 0, std 0.05 → z=4 (above ENTRY)
        let d = evaluate_stat_arb(&p, 0.60, 0.40);
        assert!(matches!(d, StatArbDecision::EnterShortA { z } if z > ENTRY_Z_THRESHOLD));
    }

    #[test]
    fn closed_pair_low_spread_enters_long_a() {
        let p = pair(1.0, 0.0, 0.05, false, 0.0);
        // a = 0.40, b = 0.60 → spread = -0.20; z = -4
        let d = evaluate_stat_arb(&p, 0.40, 0.60);
        assert!(matches!(d, StatArbDecision::EnterLongA { z } if z < -ENTRY_Z_THRESHOLD));
    }

    #[test]
    fn closed_pair_in_band_no_signal() {
        let p = pair(1.0, 0.0, 0.05, false, 0.0);
        // spread = 0.02 → z = 0.4 (below ENTRY 2.0)
        let d = evaluate_stat_arb(&p, 0.51, 0.49);
        assert_eq!(d, StatArbDecision::None);
    }

    #[test]
    fn open_pair_reverted_exits() {
        let p = pair(1.0, 0.0, 0.05, true, 1.0);
        // spread = 0.01 → z = 0.2 (below EXIT 0.5)
        let d = evaluate_stat_arb(&p, 0.50, 0.49);
        assert!(matches!(d, StatArbDecision::Exit { z } if z.abs() < EXIT_Z_THRESHOLD));
    }

    #[test]
    fn open_pair_stops_out_on_extreme_z() {
        let p = pair(1.0, 0.0, 0.05, true, 1.0);
        // spread = 0.30 → z = 6 (above STOPOUT 4)
        let d = evaluate_stat_arb(&p, 0.65, 0.35);
        assert!(matches!(d, StatArbDecision::StopOut { z } if z.abs() > STOPOUT_Z_THRESHOLD));
    }

    #[test]
    fn open_pair_within_thresholds_holds() {
        let p = pair(1.0, 0.0, 0.05, true, 1.0);
        // spread = 0.10 → z = 2.0 (between EXIT and STOPOUT)
        let d = evaluate_stat_arb(&p, 0.55, 0.45);
        assert_eq!(d, StatArbDecision::None);
    }

    #[test]
    fn zero_stdev_returns_none() {
        let p = pair(1.0, 0.0, 0.0, false, 0.0);
        assert_eq!(evaluate_stat_arb(&p, 0.50, 0.50), StatArbDecision::None);
    }

    #[test]
    fn negative_stdev_returns_none() {
        let p = pair(1.0, 0.0, -0.05, false, 0.0);
        assert_eq!(evaluate_stat_arb(&p, 0.50, 0.50), StatArbDecision::None);
    }

    #[test]
    fn extreme_price_returns_none() {
        let p = pair(1.0, 0.0, 0.05, false, 0.0);
        // a price at boundary
        assert_eq!(evaluate_stat_arb(&p, 0.01, 0.50), StatArbDecision::None);
        assert_eq!(evaluate_stat_arb(&p, 0.50, 0.99), StatArbDecision::None);
    }

    #[test]
    fn beta_scales_spread_correctly() {
        // beta = 2.0 means b moves twice as much as a per unit
        // spread = a - 2b. a=0.60, b=0.30 → spread = 0.00. z=0
        let p = pair(2.0, 0.0, 0.05, false, 0.0);
        let d = evaluate_stat_arb(&p, 0.60, 0.30);
        assert_eq!(d, StatArbDecision::None);
    }

    #[test]
    fn nonzero_mean_shifts_z() {
        // mean = 0.10. a = 0.55, b = 0.45 → spread = 0.10, exactly mean, z=0
        let p = pair(1.0, 0.10, 0.05, false, 0.0);
        let d = evaluate_stat_arb(&p, 0.55, 0.45);
        assert_eq!(d, StatArbDecision::None);
    }
}
