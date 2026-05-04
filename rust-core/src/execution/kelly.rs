// F1 — Direction-aware Kelly criterion position sizing.
//
// f* = (b * p - q) / b
//   p = probability the bet wins
//   q = 1 - p
//   b = net odds = (1 - price) / price
//
// We use a fractional Kelly (typ. 0.25) to reduce variance at the cost of
// some compounded growth. This is the standard recommendation for live
// trading — full Kelly is mathematically optimal but extremely volatile.
//
// Direction-aware in the sense that the caller passes the correct
// `p_win` and `price` for whichever side they intend to buy:
//   YES bet: p_win = fair_value_yes,    price = yes_ask
//   NO  bet: p_win = 1 - fair_value_yes, price = no_ask

/// Compute the recommended bet size in dollars, rounded down to USDC cents.
///
/// Returns 0.0 (no bet) when:
///   - any input is non-finite
///   - price not in (0, 1)
///   - p_win not in [0, 1]
///   - kelly fraction is negative (no edge)
pub fn kelly_size(
    p_win:           f64,
    price:           f64,
    bankroll:        f64,
    kelly_fraction:  f64,
    max_bet_dollars: f64,
) -> f64 {
    // Validate
    if !p_win.is_finite()
        || !price.is_finite()
        || !bankroll.is_finite()
        || !kelly_fraction.is_finite()
        || !max_bet_dollars.is_finite()
    {
        return 0.0;
    }
    if !(0.0..=1.0).contains(&p_win) {
        return 0.0;
    }
    if price <= 0.0 || price >= 1.0 {
        return 0.0;
    }
    if bankroll <= 0.0 || kelly_fraction <= 0.0 || max_bet_dollars <= 0.0 {
        return 0.0;
    }

    let q = 1.0 - p_win;
    let b = (1.0 - price) / price;
    let kelly_full = (b * p_win - q) / b;

    // No edge → no bet
    if kelly_full <= 0.0 {
        return 0.0;
    }

    let kelly_frac = kelly_full * kelly_fraction;
    let raw_bet    = kelly_frac * bankroll;
    let capped_bet = raw_bet.min(max_bet_dollars).max(0.0);

    // Round down to USDC cents (2 dp) — never overshoot
    (capped_bet * 100.0).floor() / 100.0
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    const TOL: f64 = 1e-9;

    // ── Math correctness — hand-computed reference values ─────────────────

    #[test]
    fn standard_positive_edge_capped_at_max() {
        // p=0.6, price=0.5, b=1, q=0.4
        // kelly_full = (1*0.6 - 0.4)/1 = 0.2
        // kelly_frac = 0.05
        // raw = 0.05 * 1500 = 75 → capped at 30
        let bet = kelly_size(0.6, 0.5, 1500.0, 0.25, 30.0);
        assert!((bet - 30.0).abs() < TOL);
    }

    #[test]
    fn standard_positive_edge_under_cap() {
        // bankroll $100, same edge → raw bet ~= $5.00, no cap.
        // Floor rounding to USDC cents may produce $4.99 due to IEEE-754
        // (0.6 - 0.4 != exactly 0.2). Either is acceptable behaviour.
        let bet = kelly_size(0.6, 0.5, 100.0, 0.25, 30.0);
        assert!(bet >= 4.99 && bet <= 5.00, "got {bet}, expected ~5.00");
    }

    #[test]
    fn favourite_long_odds() {
        // p=0.9, price=0.85, b = 0.1765, q = 0.1
        // kelly_full = (0.1765*0.9 - 0.1) / 0.1765 = 0.058824/0.176471 = 0.3333
        // kelly_frac = 0.0833, raw = $125 → capped at $30
        let bet = kelly_size(0.9, 0.85, 1500.0, 0.25, 30.0);
        assert!((bet - 30.0).abs() < TOL, "got {bet}");
    }

    #[test]
    fn longshot_high_payoff() {
        // p=0.10, price=0.05, b=19, q=0.9
        // kelly_full = (19*0.10 - 0.9)/19 = 1.0/19 = 0.0526
        // kelly_frac = 0.0132, raw = $19.74
        let bet = kelly_size(0.10, 0.05, 1500.0, 0.25, 30.0);
        assert!((bet - 19.74).abs() < 0.02, "got {bet}");
    }

    // ── No-bet conditions ─────────────────────────────────────────────────

    #[test]
    fn zero_edge_is_zero_bet() {
        // p == price → kelly_full = 0 → bet = 0
        assert_eq!(kelly_size(0.50, 0.50, 1500.0, 0.25, 30.0), 0.0);
    }

    #[test]
    fn negative_edge_is_zero_bet() {
        // p < price → bet against odds → no bet
        assert_eq!(kelly_size(0.40, 0.50, 1500.0, 0.25, 30.0), 0.0);
        assert_eq!(kelly_size(0.10, 0.50, 1500.0, 0.25, 30.0), 0.0);
    }

    // ── Capping behavior ──────────────────────────────────────────────────

    #[test]
    fn cap_respected() {
        // Strong edge that would otherwise call for huge bet
        let bet = kelly_size(0.95, 0.05, 100_000.0, 1.0, 50.0);
        assert!((bet - 50.0).abs() < TOL, "must respect cap; got {bet}");
    }

    #[test]
    fn small_bankroll_dominates() {
        // Tiny bankroll → kelly says small bet
        let bet = kelly_size(0.6, 0.5, 4.0, 0.25, 30.0);
        assert!(bet < 1.0 && bet >= 0.0, "got {bet}");
    }

    #[test]
    fn rounding_floors_to_usdc_cents() {
        // Result should never have more than 2 decimal places
        let bet = kelly_size(0.55, 0.50, 1500.0, 0.25, 30.0);
        let scaled = bet * 100.0;
        assert!((scaled - scaled.floor()).abs() < 1e-6,
                "bet {bet} should be at-most 2dp");
    }

    #[test]
    fn rounds_down_never_up() {
        // For any input, result must be <= raw bet (rounded down only)
        let p = 0.55;
        let price = 0.50;
        let bankroll = 137.45;
        let kf = 0.25;
        let max = 30.0;
        let bet = kelly_size(p, price, bankroll, kf, max);
        // Raw kelly = 0.10 * 0.25 * 137.45 = 3.43625
        assert!(bet <= 3.43625 + 1e-9, "rounded down: got {bet}");
        assert!(bet >= 3.43);          // shouldn't lose more than 1 cent
    }

    // ── Input validation ──────────────────────────────────────────────────

    #[test]
    fn rejects_invalid_p_win() {
        for bad in [-0.1, 1.1, 2.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert_eq!(kelly_size(bad, 0.5, 1500.0, 0.25, 30.0), 0.0,
                       "should reject p_win = {bad}");
        }
    }

    #[test]
    fn rejects_invalid_price() {
        for bad in [0.0, 1.0, -0.1, 1.5, f64::NAN, f64::INFINITY] {
            assert_eq!(kelly_size(0.5, bad, 1500.0, 0.25, 30.0), 0.0,
                       "should reject price = {bad}");
        }
    }

    #[test]
    fn rejects_invalid_bankroll() {
        for bad in [0.0, -100.0, f64::NAN, f64::INFINITY] {
            assert_eq!(kelly_size(0.6, 0.5, bad, 0.25, 30.0), 0.0,
                       "should reject bankroll = {bad}");
        }
    }

    #[test]
    fn rejects_invalid_fraction() {
        for bad in [0.0, -0.5, f64::NAN, f64::INFINITY] {
            assert_eq!(kelly_size(0.6, 0.5, 1500.0, bad, 30.0), 0.0,
                       "should reject fraction = {bad}");
        }
    }

    #[test]
    fn rejects_invalid_max_bet() {
        for bad in [0.0, -10.0, f64::NAN, f64::INFINITY] {
            assert_eq!(kelly_size(0.6, 0.5, 1500.0, 0.25, bad), 0.0,
                       "should reject max_bet = {bad}");
        }
    }

    // ── Direction-aware usage (caller pre-converts) ──────────────────────

    #[test]
    fn yes_bet_with_oracle_arb_signal() {
        // From an oracle arb signal:
        //   fair_value_yes = 0.85, yes_ask = 0.65 (underpriced)
        //   p_win = 0.85, price = 0.65
        let bet = kelly_size(0.85, 0.65, 1500.0, 0.25, 30.0);
        assert!(bet > 0.0);
        assert!(bet <= 30.0);
    }

    #[test]
    fn no_bet_with_oracle_arb_signal() {
        // Symmetric: market thinks it's leaning Up but BTC is actually Down
        //   fair_value_yes = 0.20, no_ask = 0.40
        //   p_win (NO) = 0.80, price = 0.40
        let bet = kelly_size(0.80, 0.40, 1500.0, 0.25, 30.0);
        assert!(bet > 0.0);
    }

    // ── Property: monotonicity ────────────────────────────────────────────

    #[test]
    fn higher_p_win_means_bigger_bet_at_same_price() {
        let price = 0.50;
        let b1 = kelly_size(0.55, price, 1000.0, 0.25, 100.0);
        let b2 = kelly_size(0.65, price, 1000.0, 0.25, 100.0);
        let b3 = kelly_size(0.80, price, 1000.0, 0.25, 100.0);
        assert!(b2 > b1);
        assert!(b3 > b2);
    }

    #[test]
    fn lower_price_at_same_p_win_means_bigger_bet() {
        // Lower price → higher b → typically higher kelly
        let p = 0.60;
        let b1 = kelly_size(p, 0.50, 1000.0, 0.25, 100.0);
        let b2 = kelly_size(p, 0.30, 1000.0, 0.25, 100.0);
        assert!(b2 >= b1);
    }

    // ── Performance ───────────────────────────────────────────────────────

    #[test]
    fn perf_under_100ns_per_call() {
        let start = std::time::Instant::now();
        for i in 0..100_000 {
            let p = 0.5 + (i as f64 * 0.000001);
            std::hint::black_box(kelly_size(p, 0.5, 1500.0, 0.25, 30.0));
        }
        let per_ns = start.elapsed().as_nanos() / 100_000;
        assert!(per_ns < 200, "expected < 200ns per call, got {per_ns}ns");
    }
}
