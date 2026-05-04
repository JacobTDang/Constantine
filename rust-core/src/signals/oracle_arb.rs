// E1 + E2 — Oracle arbitrage signal.
//
// Polymarket BTC "Up or Down" markets resolve via Chainlink: at window close,
// if chainlink_price > window_open_chainlink_price, the Up token pays $1.
//
// The market often misprices this binary outcome — especially in the last
// 60-90 seconds when BTC is clearly headed in one direction but the Polymarket
// order book hasn't caught up. That mispricing is our edge.
//
// E1 prices the binary fairly using a lognormal price model:
//
//   P(S_T > K) = N( log(S_t / K) / (sigma * sqrt(T_remaining)) )
//
// where:
//   S_t       — current spot
//   K         — strike (chainlink price at window open)
//   sigma     — volatility per minute (from recent 1-min returns)
//   T         — minutes remaining until window close
//   N         — standard normal CDF
//
// E2 compares fair value to the market ask on each side and emits a signal
// when the gap exceeds the threshold (which the caller pre-loads with fees
// + safety margin).

use super::Direction;

// ── E1: Binary option fair value ─────────────────────────────────────────────

/// Standard normal CDF, accurate to ~1.5e-7. Uses Abramowitz-Stegun 7.1.26.
pub fn norm_cdf(x: f64) -> f64 {
    if x.is_nan() {
        return 0.5;
    }
    if x.is_infinite() {
        return if x > 0.0 { 1.0 } else { 0.0 };
    }
    0.5 * (1.0 + erf(x / std::f64::consts::SQRT_2))
}

fn erf(x: f64) -> f64 {
    // Abramowitz-Stegun 7.1.26 — max abs error ~1.5e-7
    let sign = x.signum();
    let x = x.abs();
    let p = 0.3275911_f64;
    let a1 =  0.254829592_f64;
    let a2 = -0.284496736_f64;
    let a3 =  1.421413741_f64;
    let a4 = -1.453152027_f64;
    let a5 =  1.061405429_f64;
    let t = 1.0 / (1.0 + p * x);
    let y = 1.0
        - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * (-x * x).exp();
    sign * y
}

/// Probability that BTC closes above the strike at window expiry.
///
/// Returns 0.5 (uninformative) for any degenerate input — never propagates
/// NaN or Inf into trading signals.
///
/// At expiry (`time_remaining_min <= 0` or `vol_per_min == 0`) this becomes
/// a step function: 1 if `spot > strike`, 0 if `spot < strike`, 0.5 if equal.
pub fn fair_value_yes(
    spot:               f64,
    strike:             f64,
    vol_per_min:        f64,
    time_remaining_min: f64,
) -> f64 {
    // Reject any non-finite input
    if !spot.is_finite()
        || !strike.is_finite()
        || !vol_per_min.is_finite()
        || !time_remaining_min.is_finite()
    {
        return 0.5;
    }
    // Reject non-positive prices
    if spot <= 0.0 || strike <= 0.0 || vol_per_min < 0.0 {
        return 0.5;
    }

    let t = time_remaining_min.max(0.0);
    let sigma_sqrt_t = vol_per_min * t.sqrt();

    // At/past expiry — step function
    if sigma_sqrt_t < 1e-12 {
        return match spot.partial_cmp(&strike).unwrap_or(std::cmp::Ordering::Equal) {
            std::cmp::Ordering::Greater => 1.0,
            std::cmp::Ordering::Less    => 0.0,
            std::cmp::Ordering::Equal   => 0.5,
        };
    }

    let d = (spot / strike).ln() / sigma_sqrt_t;
    let p = norm_cdf(d);
    p.clamp(0.0, 1.0)
}

// ── E2: Oracle arb signal ────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct OracleArbSignal {
    pub direction:          Direction,
    /// fair_value - market_ask, always positive (only fires when buying is profitable)
    pub edge:               f64,
    /// Our model's estimate of P(this outcome wins)
    pub fair_value:         f64,
    /// What we'd pay to buy this side
    pub market_price:       f64,
    /// 0-1, scales linearly with edge above threshold (capped)
    pub confidence:         f64,
    /// Seconds until window resolves
    pub time_to_close_secs: f64,
}

/// Detect an oracle arb opportunity. Returns `None` if no edge above threshold.
///
/// Inputs:
///   `spot`               — current BTC spot (typically OKX last trade)
///   `strike`             — chainlink price captured at window open
///   `vol_per_min`        — std dev of recent 1-min log returns
///   `time_remaining_min` — minutes until window close
///   `yes_ask`            — cost to buy the Up (YES) token, in (0, 1)
///   `no_ask`             — cost to buy the Down (NO) token, in (0, 1)
///   `threshold`          — minimum edge to fire (e.g. 0.04 — caller pre-loads
///                           this with fees + safety margin)
///
/// Robust to missing data: returns `None` for any non-finite or out-of-range
/// input rather than emitting a bad signal.
pub fn check_oracle_arb(
    spot:               f64,
    strike:             f64,
    vol_per_min:        f64,
    time_remaining_min: f64,
    yes_ask:            f64,
    no_ask:             f64,
    threshold:          f64,
) -> Option<OracleArbSignal> {
    // Window already closed — nothing to bet on
    if time_remaining_min <= 0.0 {
        return None;
    }
    // Validate prices: must be in (0, 1) — Polymarket binary outcome tokens
    if !yes_ask.is_finite() || !no_ask.is_finite() {
        return None;
    }
    if yes_ask <= 0.0 || yes_ask >= 1.0 {
        return None;
    }
    if no_ask <= 0.0 || no_ask >= 1.0 {
        return None;
    }
    // Threshold should be positive
    if !threshold.is_finite() || threshold < 0.0 {
        return None;
    }

    let fair_yes = fair_value_yes(spot, strike, vol_per_min, time_remaining_min);
    let fair_no  = 1.0 - fair_yes;

    let yes_edge = fair_yes - yes_ask;
    let no_edge  = fair_no  - no_ask;

    // Pick the bigger-edge side
    let (best_edge, direction, fair, market) = if yes_edge >= no_edge {
        (yes_edge, Direction::Up,   fair_yes, yes_ask)
    } else {
        (no_edge,  Direction::Down, fair_no,  no_ask)
    };

    if best_edge < threshold {
        return None;
    }

    // Confidence: linear in edge above threshold, capped at 1.0 (10c above)
    let confidence = ((best_edge - threshold) / 0.10).clamp(0.0, 1.0);

    Some(OracleArbSignal {
        direction,
        edge: best_edge,
        fair_value: fair,
        market_price: market,
        confidence,
        time_to_close_secs: time_remaining_min * 60.0,
    })
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    const TOL_CDF:  f64 = 2e-7;     // erf approximation accuracy
    const TOL_ARB:  f64 = 1e-9;     // exact arithmetic
    const TOL_REAL: f64 = 1e-3;     // real-world scenarios — looser tolerance

    // ── norm_cdf accuracy ─────────────────────────────────────────────────
    // Reference values from standard normal tables / scipy.stats.norm.cdf

    #[test]
    fn cdf_at_zero_is_half() {
        assert!((norm_cdf(0.0) - 0.5).abs() < TOL_CDF);
    }

    #[test]
    fn cdf_symmetric_around_zero() {
        for x in [0.5, 1.0, 1.5, 2.0, 2.5, 3.0] {
            assert!((norm_cdf(x) + norm_cdf(-x) - 1.0).abs() < TOL_CDF);
        }
    }

    #[test]
    fn cdf_known_values() {
        // scipy: norm.cdf(1.0)    = 0.8413447460685429
        assert!((norm_cdf(1.0) - 0.8413447460685429).abs() < TOL_CDF);
        // scipy: norm.cdf(1.96)   = 0.9750021048517795 (95th percentile)
        assert!((norm_cdf(1.96) - 0.9750021048517795).abs() < TOL_CDF);
        // scipy: norm.cdf(2.5758) = 0.99500    (99.5th)
        assert!((norm_cdf(2.5758) - 0.995).abs() < 1e-4);
        // scipy: norm.cdf(-1.96)  = 0.024997...
        assert!((norm_cdf(-1.96) - 0.0249978951482205).abs() < TOL_CDF);
        // scipy: norm.cdf(-3.0)   = 0.001349898031
        assert!((norm_cdf(-3.0) - 0.001349898031630).abs() < TOL_CDF);
    }

    #[test]
    fn cdf_extreme_values() {
        assert_eq!(norm_cdf(f64::INFINITY),      1.0);
        assert_eq!(norm_cdf(f64::NEG_INFINITY),  0.0);
        assert_eq!(norm_cdf(f64::NAN),           0.5); // safe default
        // Far in the tails
        assert!(norm_cdf(10.0)  > 1.0 - 1e-10);
        assert!(norm_cdf(-10.0) < 1e-10);
    }

    #[test]
    fn cdf_monotonically_increasing() {
        let xs: Vec<f64> = (-30..=30).map(|i| i as f64 / 5.0).collect();
        let mut prev = 0.0;
        for x in xs {
            let p = norm_cdf(x);
            assert!(p >= prev - 1e-12, "non-monotone at x={x}");
            prev = p;
        }
    }

    // ── fair_value_yes: boundary conditions ──────────────────────────────

    #[test]
    fn fv_at_expiry_step_function() {
        // time = 0 → step function
        assert_eq!(fair_value_yes(80_001.0, 80_000.0, 0.001, 0.0), 1.0);
        assert_eq!(fair_value_yes(79_999.0, 80_000.0, 0.001, 0.0), 0.0);
        assert_eq!(fair_value_yes(80_000.0, 80_000.0, 0.001, 0.0), 0.5);
    }

    #[test]
    fn fv_zero_vol_step_function() {
        // No uncertainty → step function regardless of time
        assert_eq!(fair_value_yes(80_500.0, 80_000.0, 0.0, 5.0), 1.0);
        assert_eq!(fair_value_yes(79_500.0, 80_000.0, 0.0, 5.0), 0.0);
        assert_eq!(fair_value_yes(80_000.0, 80_000.0, 0.0, 5.0), 0.5);
    }

    #[test]
    fn fv_at_the_money_is_half_regardless_of_time_or_vol() {
        // log(1.0) = 0 → d = 0 → norm_cdf(0) = 0.5
        // erf approximation has ~1.5e-7 error so we use TOL_CDF
        for t in [0.5, 1.0, 5.0, 15.0] {
            for v in [0.0001, 0.001, 0.01, 0.1] {
                let fv = fair_value_yes(80_000.0, 80_000.0, v, t);
                assert!((fv - 0.5).abs() < TOL_CDF,
                        "ATM should be 0.5; got {fv} at t={t} v={v}");
            }
        }
    }

    #[test]
    fn fv_deep_itm_approaches_one() {
        // BTC $1000 above strike, calm market, 30s remaining
        let fv = fair_value_yes(81_000.0, 80_000.0, 0.0005, 0.5);
        assert!(fv > 0.999, "deep ITM should be ~1.0, got {fv}");
    }

    #[test]
    fn fv_deep_otm_approaches_zero() {
        // BTC $1000 below strike, calm market, 30s remaining
        let fv = fair_value_yes(79_000.0, 80_000.0, 0.0005, 0.5);
        assert!(fv < 0.001, "deep OTM should be ~0.0, got {fv}");
    }

    // ── fair_value_yes: monotonicity ─────────────────────────────────────

    #[test]
    fn fv_monotonic_in_spot() {
        let v = 0.001;
        let t = 5.0;
        let strike = 80_000.0;
        let fvs: Vec<f64> = (-100..=100)
            .step_by(10)
            .map(|i| fair_value_yes(strike + i as f64, strike, v, t))
            .collect();
        for w in fvs.windows(2) {
            assert!(w[1] >= w[0] - 1e-10, "fair value should rise with spot");
        }
    }

    #[test]
    fn fv_more_time_pulls_toward_half_when_itm() {
        // ITM scenario: more time = more uncertainty = closer to 0.5
        let v = 0.001;
        let s = 80_100.0; let k = 80_000.0;
        let fv_short = fair_value_yes(s, k, v, 0.5);  // 30s remaining
        let fv_long  = fair_value_yes(s, k, v, 15.0); // 15min remaining
        assert!(fv_short > fv_long, "ITM: short window should be more confident");
        assert!(fv_long > 0.5,      "still ITM, just less confident");
    }

    #[test]
    fn fv_higher_vol_pulls_toward_half_when_itm() {
        let t = 5.0;
        let s = 80_100.0; let k = 80_000.0;
        let fv_low_vol  = fair_value_yes(s, k, 0.0001, t);
        let fv_high_vol = fair_value_yes(s, k, 0.01,   t);
        assert!(fv_low_vol > fv_high_vol);
        assert!(fv_high_vol > 0.5); // still ITM
    }

    // ── fair_value_yes: numerical stability ──────────────────────────────

    #[test]
    fn fv_zero_spot_returns_half() {
        assert_eq!(fair_value_yes(0.0, 80_000.0, 0.001, 5.0), 0.5);
    }

    #[test]
    fn fv_zero_strike_returns_half() {
        assert_eq!(fair_value_yes(80_000.0, 0.0, 0.001, 5.0), 0.5);
    }

    #[test]
    fn fv_negative_inputs_return_half() {
        assert_eq!(fair_value_yes(-1.0,    80_000.0, 0.001, 5.0), 0.5);
        assert_eq!(fair_value_yes(80_000.0,    -1.0, 0.001, 5.0), 0.5);
        assert_eq!(fair_value_yes(80_000.0, 80_000.0,  -0.1, 5.0), 0.5);
    }

    #[test]
    fn fv_nan_inf_inputs_return_half() {
        assert_eq!(fair_value_yes(f64::NAN,      80_000.0, 0.001, 5.0), 0.5);
        assert_eq!(fair_value_yes(80_000.0, f64::NAN,      0.001, 5.0), 0.5);
        assert_eq!(fair_value_yes(80_000.0, 80_000.0, f64::NAN, 5.0), 0.5);
        assert_eq!(fair_value_yes(80_000.0, 80_000.0, 0.001, f64::NAN), 0.5);
        assert_eq!(fair_value_yes(f64::INFINITY, 80_000.0, 0.001, 5.0), 0.5);
    }

    #[test]
    fn fv_extreme_prices_dont_panic() {
        // Don't crash on absurd inputs
        let _ = fair_value_yes(1e15,      1.0,       0.01, 1.0);
        let _ = fair_value_yes(1.0,       1e15,      0.01, 1.0);
        let _ = fair_value_yes(80_000.0,  80_000.0,  100.0, 1000.0);
    }

    #[test]
    fn fv_negative_time_clamps_to_zero() {
        let fv = fair_value_yes(80_500.0, 80_000.0, 0.001, -1.0);
        assert_eq!(fv, 1.0);
    }

    #[test]
    fn fv_output_always_in_zero_one() {
        for i in 0..1000 {
            let s = 80_000.0 + (i as f64 - 500.0);
            let fv = fair_value_yes(s, 80_000.0, 0.001, 5.0);
            assert!((0.0..=1.0).contains(&fv), "fv={fv} out of [0,1] for s={s}");
        }
    }

    // ── fair_value_yes: real-world scenario tests ─────────────────────────
    //
    // These are computed by hand using the formula:
    //   d = log(spot/strike) / (vol * sqrt(t_remaining))
    //   fair = norm_cdf(d)

    #[test]
    fn scenario_late_window_clear_uptrend() {
        // 5-min window, 30s left, BTC $200 above strike, calm 0.05% vol/min
        // d = log(80200/80000) / (0.0005 * sqrt(0.5))
        //   = 0.0024969 / 0.0003536 = 7.062
        // fair = N(7.062) ~= 1.0
        let fv = fair_value_yes(80_200.0, 80_000.0, 0.0005, 0.5);
        assert!(fv > 0.999, "near-certain Up should be ~1.0, got {fv}");
    }

    #[test]
    fn scenario_mid_window_atm_uncertain() {
        // 5-min window, 4min left, BTC $50 above strike, normal 0.1% vol
        // d = log(80050/80000) / (0.001 * sqrt(4)) = 0.000625 / 0.002 = 0.3125
        // fair = N(0.3125) ~= 0.6227
        let fv = fair_value_yes(80_050.0, 80_000.0, 0.001, 4.0);
        assert!((fv - 0.6227).abs() < TOL_REAL, "got {fv}, expected ~0.6227");
    }

    #[test]
    fn scenario_one_min_left_clear_lead() {
        // 5-min window, 1min left, BTC $100 above strike, vol 0.1%/min
        // d = log(80100/80000) / 0.001 = 0.001249 / 0.001 = 1.249
        // fair = N(1.249) ~= 0.8942
        let fv = fair_value_yes(80_100.0, 80_000.0, 0.001, 1.0);
        assert!((fv - 0.8942).abs() < TOL_REAL, "got {fv}, expected ~0.8942");
    }

    #[test]
    fn scenario_high_vol_event_dont_overcommit() {
        // 5-min window, 3min left, BTC $50 above strike, EVENT vol 1%/min
        // d = log(80050/80000) / (0.01 * sqrt(3)) = 0.000625 / 0.01732 = 0.0361
        // fair = N(0.0361) ~= 0.5144 — barely above 50%
        let fv = fair_value_yes(80_050.0, 80_000.0, 0.01, 3.0);
        assert!((fv - 0.5144).abs() < TOL_REAL, "got {fv}, expected ~0.5144");
    }

    #[test]
    fn scenario_15min_window_huge_lead_still_strong() {
        // 15-min window, 14min left, BTC $1000 above strike, vol 0.1%/min
        // d = log(81000/80000) / (0.001 * sqrt(14)) = 0.01242 / 0.003742 = 3.319
        // fair = N(3.319) ~= 0.9995
        let fv = fair_value_yes(81_000.0, 80_000.0, 0.001, 14.0);
        assert!(fv > 0.99,  "huge lead even with 14min left should be very likely; got {fv}");
        assert!(fv < 1.0,   "should still admit some uncertainty");
    }

    // ── check_oracle_arb: triggering ──────────────────────────────────────

    #[test]
    fn arb_fires_on_underpriced_yes() {
        // Setup: YES is fairly worth ~0.89 (1min, $100 lead, 0.1% vol),
        // but market shows YES at 0.70 — clear mispricing in our favor
        let sig = check_oracle_arb(
            80_100.0, 80_000.0, 0.001, 1.0,
            0.70,    // YES ask
            0.30,    // NO ask
            0.04,    // threshold
        ).expect("should fire");
        assert_eq!(sig.direction, Direction::Up);
        assert!(sig.edge > 0.15, "edge should be ~0.19, got {}", sig.edge);
        assert!(sig.fair_value > 0.85);
        assert!((sig.time_to_close_secs - 60.0).abs() < 1e-6);
    }

    #[test]
    fn arb_fires_on_underpriced_no() {
        // Symmetric: BTC clearly DOWN, but NO is underpriced
        let sig = check_oracle_arb(
            79_900.0, 80_000.0, 0.001, 1.0,
            0.30,    // YES ask
            0.70,    // NO ask
            0.04,
        ).expect("should fire");
        assert_eq!(sig.direction, Direction::Down);
        assert!(sig.edge > 0.15, "edge should be ~0.19, got {}", sig.edge);
    }

    #[test]
    fn arb_picks_bigger_edge_side() {
        // YES fair ~0.62, market 0.55 → YES edge = 0.07
        // NO fair  ~0.38, market 0.30 → NO  edge = 0.08
        // Should pick NO
        let sig = check_oracle_arb(
            80_050.0, 80_000.0, 0.001, 4.0,
            0.55,    // YES ask
            0.30,    // NO ask
            0.04,
        ).expect("should fire");
        assert_eq!(sig.direction, Direction::Down);
    }

    // ── check_oracle_arb: not firing ──────────────────────────────────────

    #[test]
    fn arb_does_not_fire_when_market_correct() {
        // Fair ~0.62 (4min, $50 lead, 0.1% vol), market correctly at 0.62 → no edge
        assert!(check_oracle_arb(
            80_050.0, 80_000.0, 0.001, 4.0,
            0.62, 0.38, 0.04,
        ).is_none());
    }

    #[test]
    fn arb_does_not_fire_below_threshold() {
        // Fair ~0.89, market 0.86 → edge 0.03, below 0.04 threshold
        assert!(check_oracle_arb(
            80_100.0, 80_000.0, 0.001, 1.0,
            0.86, 0.14, 0.04,
        ).is_none());
    }

    #[test]
    fn arb_does_not_fire_when_window_closed() {
        assert!(check_oracle_arb(80_500.0, 80_000.0, 0.001, 0.0,  0.40, 0.60, 0.04).is_none());
        assert!(check_oracle_arb(80_500.0, 80_000.0, 0.001, -1.0, 0.40, 0.60, 0.04).is_none());
    }

    #[test]
    fn arb_does_not_fire_on_high_vol_when_market_calibrated() {
        // High vol regime: fair value pulls toward 0.5
        // Fair ~0.514 in 1%/min vol scenario, market at 0.55 → edge negative
        assert!(check_oracle_arb(
            80_050.0, 80_000.0, 0.01, 3.0,
            0.55, 0.45, 0.04,
        ).is_none());
    }

    // ── check_oracle_arb: input validation ────────────────────────────────

    #[test]
    fn arb_rejects_invalid_yes_ask() {
        for bad in [0.0, -0.1, 1.0, 1.5, f64::NAN, f64::INFINITY] {
            assert!(check_oracle_arb(80_100.0, 80_000.0, 0.001, 1.0, bad, 0.30, 0.04).is_none(),
                    "should reject yes_ask = {bad}");
        }
    }

    #[test]
    fn arb_rejects_invalid_no_ask() {
        for bad in [0.0, -0.1, 1.0, 1.5, f64::NAN, f64::INFINITY] {
            assert!(check_oracle_arb(80_100.0, 80_000.0, 0.001, 1.0, 0.70, bad, 0.04).is_none(),
                    "should reject no_ask = {bad}");
        }
    }

    #[test]
    fn arb_rejects_invalid_threshold() {
        for bad in [-0.01, f64::NAN, f64::INFINITY] {
            assert!(check_oracle_arb(80_100.0, 80_000.0, 0.001, 1.0, 0.70, 0.30, bad).is_none(),
                    "should reject threshold = {bad}");
        }
    }

    #[test]
    fn arb_zero_threshold_fires_on_any_positive_edge() {
        // With threshold=0, any positive edge fires
        let sig = check_oracle_arb(
            80_100.0, 80_000.0, 0.001, 1.0,
            0.85, 0.15, 0.0,
        ).expect("should fire with zero threshold");
        assert!(sig.edge > 0.0);
    }

    // ── check_oracle_arb: confidence calculation ─────────────────────────

    #[test]
    fn arb_confidence_scales_with_edge() {
        // Just-above-threshold → low confidence
        let sig1 = check_oracle_arb(
            80_100.0, 80_000.0, 0.001, 1.0,
            0.85,    // edge ~0.044
            0.15,
            0.04,
        ).expect("fires");
        // Way above threshold → high confidence
        let sig2 = check_oracle_arb(
            80_100.0, 80_000.0, 0.001, 1.0,
            0.50,    // edge ~0.394
            0.50,
            0.04,
        ).expect("fires");
        assert!(sig1.confidence < sig2.confidence);
        assert!(sig1.confidence < 0.10, "barely-above threshold should be low conf");
        assert!(sig2.confidence > 0.99, "deep edge should saturate confidence");
    }

    #[test]
    fn arb_confidence_clamped_at_one() {
        // Massive edge → should clamp to 1.0
        let sig = check_oracle_arb(
            81_000.0, 80_000.0, 0.0001, 0.5,
            0.10,    // YES priced at 0.10 when fair value ~1.0 → edge ~0.9
            0.90,
            0.04,
        ).expect("fires");
        assert_eq!(sig.confidence, 1.0);
    }

    #[test]
    fn arb_confidence_in_zero_one() {
        // Property test: confidence is always in [0, 1]
        for offset in [-200, -100, -50, -20, 20, 50, 100, 200] {
            for ya in [0.10, 0.30, 0.50, 0.70, 0.90] {
                let sig = check_oracle_arb(
                    80_000.0 + offset as f64, 80_000.0, 0.001, 2.0,
                    ya, 1.0 - ya, 0.04,
                );
                if let Some(s) = sig {
                    assert!((0.0..=1.0).contains(&s.confidence),
                            "conf = {} out of [0,1]", s.confidence);
                }
            }
        }
    }

    // ── End-to-end "playbook" scenarios — the real money ─────────────────

    #[test]
    fn playbook_late_window_easy_money() {
        // 30 seconds left, BTC $250 above strike, calm market.
        // Fair value should be near 1.0; if market shows YES at 0.85,
        // we should emit a strong Up signal.
        let sig = check_oracle_arb(
            80_250.0,  // spot
            80_000.0,  // strike (chainlink at window open)
            0.0005,    // vol 0.05%/min
            0.5,       // 30 seconds remaining
            0.85,      // YES ask — underpriced
            0.15,      // NO ask
            0.04,
        ).expect("late-window strong lead must fire");
        assert_eq!(sig.direction, Direction::Up);
        assert!(sig.edge > 0.10);
        assert!(sig.fair_value > 0.99);
        assert!(sig.confidence > 0.5);
    }

    #[test]
    fn playbook_mid_window_low_certainty_no_signal() {
        // 4 minutes left, BTC $30 above strike, normal vol.
        // Fair value barely above 0.5. Market roughly correct → no signal.
        let sig = check_oracle_arb(
            80_030.0, 80_000.0, 0.001, 4.0,
            0.57, 0.43, 0.04,
        );
        assert!(sig.is_none(), "no clear edge → must not fire");
    }

    #[test]
    fn playbook_news_spike_high_vol_no_overcommit() {
        // High volatility regime — fair value pulls toward 0.5 even with
        // some price deviation. Caller's regime gate should also skip this,
        // but the oracle arb itself shouldn't see edge here.
        let sig = check_oracle_arb(
            80_100.0, 80_000.0,
            0.02,      // 2%/min vol — extreme
            3.0,       // 3min remaining
            0.55, 0.45, 0.04,
        );
        assert!(sig.is_none(), "high vol with small lead → no overconfident bet");
    }

    #[test]
    fn playbook_15min_window_oversold_yes() {
        // 15-min window, 5min in, BTC $300 above strike, normal vol.
        // d = log(80300/80000) / (0.001 * sqrt(10)) = 0.003744 / 0.003162 = 1.184
        // fair ~ 0.882; if market shows YES at 0.65, we have a clear edge.
        let sig = check_oracle_arb(
            80_300.0, 80_000.0, 0.001, 10.0,
            0.65, 0.35, 0.04,
        ).expect("15min in-the-money mispricing must fire");
        assert_eq!(sig.direction, Direction::Up);
        assert!(sig.edge > 0.20);
        assert!((sig.time_to_close_secs - 600.0).abs() < 1e-6);
    }

    #[test]
    fn playbook_intramarket_arb_overlap() {
        // YES + NO < 1.0 — both sides have edge.
        // Fair YES ~0.89, fair NO ~0.11; market YES=0.40, NO=0.50.
        // YES edge = 0.49, NO edge = -0.39 → pick YES.
        let sig = check_oracle_arb(
            80_100.0, 80_000.0, 0.001, 1.0,
            0.40, 0.50, 0.04,
        ).expect("fires");
        assert_eq!(sig.direction, Direction::Up);
        assert!(sig.edge > 0.45);
    }

    // ── Performance ──────────────────────────────────────────────────────

    #[test]
    fn perf_check_arb_under_microsecond_per_call() {
        // Hot path budget: signal eval must be << 100us per market on every tick
        let start = std::time::Instant::now();
        let n = 10_000;
        let mut hits = 0_u64;
        for i in 0..n {
            let offset = (i % 200) as f64 - 100.0;
            if let Some(_) = check_oracle_arb(
                80_000.0 + offset, 80_000.0, 0.001, 2.0,
                0.45, 0.50, 0.04,
            ) {
                hits += 1;
            }
        }
        let dur = start.elapsed();
        let per = dur.as_nanos() / n as u128;
        assert!(per < 5_000, "expected < 5us per call, got {per}ns; hits={hits}");
    }

    // ── Inversion property: yes_edge + no_edge == fair_yes - yes_ask + (1 - fair_yes) - no_ask ─

    #[test]
    fn property_edge_sum_equals_market_inefficiency() {
        // yes_edge + no_edge == 1 - (yes_ask + no_ask)
        // If both sides have edge, the order book is inefficient.
        let s = 80_100.0; let k = 80_000.0; let v = 0.001; let t = 2.0;
        let ya = 0.40; let na = 0.45;  // yes+no = 0.85 → 0.15 of inefficiency
        let fv_y = fair_value_yes(s, k, v, t);
        let fv_n = 1.0 - fv_y;
        let sum = (fv_y - ya) + (fv_n - na);
        assert!((sum - (1.0 - ya - na)).abs() < TOL_ARB);
    }

    // ── Symmetry: flipping spot vs strike inverts the signal ────────────

    #[test]
    fn property_flipping_strike_above_below_inverts_fair_value() {
        let v = 0.001;
        let t = 2.0;
        // Mirror: $X above vs $X below should be symmetric in log-space approximately
        let fv_above = fair_value_yes(80_100.0, 80_000.0, v, t);
        let fv_below = fair_value_yes(79_900.0, 80_000.0, v, t);
        // Approximate symmetry — log(80100/80000) ≈ -log(79900/80000) at this scale
        assert!((fv_above + fv_below - 1.0).abs() < 1e-3);
    }

    #[test]
    fn signal_struct_clone_works() {
        // Sanity: PartialEq + Clone should round-trip
        let sig1 = check_oracle_arb(80_100.0, 80_000.0, 0.001, 1.0, 0.70, 0.30, 0.04).unwrap();
        let sig2 = sig1.clone();
        assert_eq!(sig1, sig2);
    }
}
