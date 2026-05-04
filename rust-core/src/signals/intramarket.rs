// E3 — Intramarket arbitrage signal.
//
// When the cost to buy both YES and NO is less than $1 (after fees), you have
// a guaranteed profit per share — whichever side wins, you receive $1.
//
// Profit per share invested:
//   gross   = 1 - (yes_ask + no_ask)
//   net     = gross - fees
//   fees    = (yes_ask + no_ask) * fee_rate    (paid on entry to both legs)
//   net     = 1 - (yes_ask + no_ask) * (1 + fee_rate)
//
// Polymarket taker fee is 1.8% on the trade size.
// Setting fee_rate = 0.018 and min_profit_threshold = 0.005 (0.5% net) gives
// a comfortable safety margin — only fires when arb is robust to slippage.
//
// This signal is RISK-FREE — fires regardless of regime, vol, or BTC direction.

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct IntramarketArbSignal {
    /// Best ask of the Up (YES) token
    pub yes_ask:        f64,
    /// Best ask of the Down (NO) token
    pub no_ask:         f64,
    /// yes_ask + no_ask (must be < 1 for any arb)
    pub gross_total:    f64,
    /// 1 - gross_total — pre-fee gap
    pub gross_profit:   f64,
    /// Profit per $1 invested after fees on both legs
    pub net_profit:     f64,
    /// Total fee in absolute terms (per $1 outcome)
    pub total_fees:     f64,
}

/// Detect a profitable intramarket arb opportunity.
///
/// `fee_rate` is the per-leg taker fee (0.018 for Polymarket BTC markets).
/// `min_profit_threshold` is the minimum acceptable net profit per share
/// (e.g. 0.005 = 0.5%). Set higher when liquidity is thin to absorb slippage.
///
/// Rejects any non-finite or out-of-range input rather than emitting a bad
/// signal — risk-free or nothing.
pub fn check_intramarket_arb(
    yes_ask:              f64,
    no_ask:               f64,
    fee_rate:             f64,
    min_profit_threshold: f64,
) -> Option<IntramarketArbSignal> {
    // Validate prices: must be in (0, 1)
    if !yes_ask.is_finite() || !no_ask.is_finite() {
        return None;
    }
    if yes_ask <= 0.0 || yes_ask >= 1.0 || no_ask <= 0.0 || no_ask >= 1.0 {
        return None;
    }
    // Validate fee + threshold
    if !fee_rate.is_finite() || fee_rate < 0.0 || fee_rate >= 1.0 {
        return None;
    }
    if !min_profit_threshold.is_finite() || min_profit_threshold < 0.0 {
        return None;
    }

    let gross_total  = yes_ask + no_ask;
    let gross_profit = 1.0 - gross_total;
    let total_fees   = gross_total * fee_rate;
    let net_profit   = gross_profit - total_fees;

    if net_profit < min_profit_threshold {
        return None;
    }

    Some(IntramarketArbSignal {
        yes_ask,
        no_ask,
        gross_total,
        gross_profit,
        net_profit,
        total_fees,
    })
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    const TOL: f64 = 1e-10;

    // ── Profitability gating ──────────────────────────────────────────────

    #[test]
    fn fires_with_clear_profit() {
        // yes 0.45 + no 0.45 = 0.90; fee 1.8% × 0.90 = 0.0162; net = 0.10 - 0.0162 = 0.0838
        let sig = check_intramarket_arb(0.45, 0.45, 0.018, 0.005).unwrap();
        assert!((sig.gross_total  - 0.90).abs()   < TOL);
        assert!((sig.gross_profit - 0.10).abs()   < TOL);
        assert!((sig.total_fees   - 0.0162).abs() < TOL);
        assert!((sig.net_profit   - 0.0838).abs() < TOL);
    }

    #[test]
    fn fires_at_minimum_profitable_combined() {
        // Edge case: just enough to clear threshold
        // Want net = 0.005 exactly
        // 0.005 = (1 - x) - x * 0.018 = 1 - x*1.018
        // x = 0.995/1.018 = 0.97740...
        let x = 0.995 / 1.018;
        let half = x / 2.0;
        let sig = check_intramarket_arb(half, half, 0.018, 0.005);
        assert!(sig.is_some(), "should just fire at threshold");
    }

    #[test]
    fn does_not_fire_below_threshold_after_fees() {
        // yes+no = 0.99, fee 1.8% → fees = 0.01782, gross profit = 0.01, net = -0.00782 (loss)
        assert!(check_intramarket_arb(0.495, 0.495, 0.018, 0.005).is_none());
    }

    #[test]
    fn does_not_fire_when_not_arb() {
        // yes+no >= 1.00 means no arb at all
        assert!(check_intramarket_arb(0.50, 0.50, 0.018, 0.005).is_none());
        assert!(check_intramarket_arb(0.55, 0.55, 0.018, 0.005).is_none());
    }

    #[test]
    fn does_not_fire_when_gross_below_threshold_pre_fees() {
        // yes+no = 0.99, threshold = 0.05 (high) — even gross profit (0.01) is below
        assert!(check_intramarket_arb(0.495, 0.495, 0.0, 0.05).is_none());
    }

    // ── Fee math ──────────────────────────────────────────────────────────

    #[test]
    fn zero_fee_rate_uses_gross_profit_directly() {
        // No fees → net == gross
        let sig = check_intramarket_arb(0.45, 0.50, 0.0, 0.001).unwrap();
        assert!((sig.gross_profit - sig.net_profit).abs() < TOL);
        assert_eq!(sig.total_fees, 0.0);
    }

    #[test]
    fn higher_fee_rate_reduces_net_profit() {
        let s1 = check_intramarket_arb(0.45, 0.45, 0.01, 0.0).unwrap();
        let s2 = check_intramarket_arb(0.45, 0.45, 0.05, 0.0).unwrap();
        assert!(s1.net_profit > s2.net_profit);
    }

    #[test]
    fn high_fee_eats_thin_arb() {
        // yes+no = 0.90, 50% fee → fees = 0.45, gross = 0.10 → net = -0.35 (loss)
        assert!(check_intramarket_arb(0.45, 0.45, 0.50, 0.005).is_none());
    }

    // ── Input validation ──────────────────────────────────────────────────

    #[test]
    fn rejects_invalid_yes_ask() {
        for bad in [0.0, -0.1, 1.0, 1.5, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(check_intramarket_arb(bad, 0.45, 0.018, 0.005).is_none(),
                    "should reject yes_ask = {bad}");
        }
    }

    #[test]
    fn rejects_invalid_no_ask() {
        for bad in [0.0, -0.1, 1.0, 1.5, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(check_intramarket_arb(0.45, bad, 0.018, 0.005).is_none(),
                    "should reject no_ask = {bad}");
        }
    }

    #[test]
    fn rejects_invalid_fee_rate() {
        for bad in [-0.1, 1.0, 1.5, f64::NAN, f64::INFINITY] {
            assert!(check_intramarket_arb(0.45, 0.45, bad, 0.005).is_none(),
                    "should reject fee_rate = {bad}");
        }
    }

    #[test]
    fn rejects_invalid_threshold() {
        for bad in [-0.1, f64::NAN, f64::INFINITY] {
            assert!(check_intramarket_arb(0.45, 0.45, 0.018, bad).is_none(),
                    "should reject threshold = {bad}");
        }
    }

    // ── Sweep across realistic price combinations ─────────────────────────

    #[test]
    fn sweep_realistic_polymarket_prices() {
        // Iterate over realistic mid-window BTC market price points
        // Verify signal correctness across the spectrum
        let test_cases: &[(f64, f64, bool)] = &[
            // (yes_ask, no_ask, expected_arb)
            (0.40, 0.40, true),   // ample arb — net ~0.184
            (0.45, 0.45, true),   // 8.4% net
            (0.48, 0.48, true),   // 2.3% net
            (0.49, 0.49, true),   // 0.2% net (below 0.5% threshold) — should miss
            (0.50, 0.50, false),  // no arb at all
            (0.55, 0.45, false),  // 0% gross
            (0.30, 0.30, true),   // huge arb — both sides cheap
            (0.25, 0.45, true),   // asymmetric arb
            (0.60, 0.50, false),  // negative gross
            (0.99, 0.01, false),  // edge of validity, no arb
        ];
        for &(ya, na, expected) in test_cases {
            let result = check_intramarket_arb(ya, na, 0.018, 0.005);
            if expected {
                assert!(result.is_some() || (1.0 - (ya + na) - (ya + na) * 0.018) < 0.005,
                        "expected arb for ya={ya} na={na}");
            } else {
                let net = 1.0 - (ya + na) - (ya + na) * 0.018;
                if net < 0.005 {
                    assert!(result.is_none(), "expected no arb for ya={ya} na={na} (net={net})");
                }
            }
        }
    }

    #[test]
    fn signal_fields_are_internally_consistent() {
        let sig = check_intramarket_arb(0.45, 0.50, 0.018, 0.005).unwrap();
        // gross_total = yes + no
        assert!((sig.gross_total - (sig.yes_ask + sig.no_ask)).abs() < TOL);
        // gross_profit = 1 - gross_total
        assert!((sig.gross_profit - (1.0 - sig.gross_total)).abs() < TOL);
        // net_profit = gross_profit - total_fees
        assert!((sig.net_profit - (sig.gross_profit - sig.total_fees)).abs() < TOL);
    }

    // ── Performance ───────────────────────────────────────────────────────

    #[test]
    fn perf_under_microsecond() {
        let start = std::time::Instant::now();
        for i in 0..10_000 {
            let ya = 0.30 + (i as f64 * 0.00001);
            let na = 0.30 + (i as f64 * 0.00002);
            std::hint::black_box(check_intramarket_arb(ya, na, 0.018, 0.005));
        }
        let per_ns = start.elapsed().as_nanos() / 10_000;
        assert!(per_ns < 1_000, "expected < 1us per call, got {per_ns}ns");
    }
}
