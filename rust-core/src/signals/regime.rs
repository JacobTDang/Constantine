// E4 — Volatility regime classifier.
//
// Classifies the current market into one of three regimes used to gate the
// ML signal (Milestone I). Oracle arb and intramarket arb are NOT gated:
//   - Oracle arb math handles high-vol correctly (fair value pulls toward 0.5)
//   - Intramarket arb is risk-free and always safe to take
//
// Inputs come from the FeatureState computed by the 500ms compute loop:
//   vol_z_score   = (vol_5m - vol_30m) / vol_30m  → "elevated short-term vol"
//   vol_ratio     = vol_5m / vol_30m              → "ratio form of the same"
//   autocorr_lag1 = Pearson lag-1 of returns      → "trend vs mean-revert"

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Regime {
    /// Calm market with persistent direction (autocorr > 0.15) — favourable
    /// conditions for ML directional signals.
    LowVolTrending,
    /// Calm market without persistent direction — neutral; ML signals
    /// allowed but expect lower edge.
    LowVolRanging,
    /// Volatility spike or news event — block ML signals (oracle/intramarket
    /// arb still allowed because their math is regime-robust).
    HighVolEvent,
}

// Tunable thresholds. These err on the side of being too permissive — better
// to occasionally trade in marginal regimes than to be silent during real
// opportunities. Live ML Brier monitoring (I3) will catch any regime drift.
pub const VOL_Z_HIGH:   f64 = 2.0;   // current vol > 200% of baseline
pub const VOL_RATIO_HI: f64 = 3.0;   // 5m vol > 3x 30m vol
pub const TREND_AC_MIN: f64 = 0.15;  // lag-1 autocorr threshold for trending

/// Classify the current regime. Defaults to `LowVolRanging` on any non-finite
/// input — never propagates NaN into trading decisions.
pub fn classify_regime(
    vol_z_score:   f64,
    vol_ratio:     f64,
    autocorr_lag1: f64,
) -> Regime {
    let z   = if vol_z_score.is_finite()   { vol_z_score }   else { 0.0 };
    let r   = if vol_ratio.is_finite()     { vol_ratio }     else { 1.0 };
    let ac  = if autocorr_lag1.is_finite() { autocorr_lag1 } else { 0.0 };

    if z > VOL_Z_HIGH || r > VOL_RATIO_HI {
        Regime::HighVolEvent
    } else if ac > TREND_AC_MIN {
        Regime::LowVolTrending
    } else {
        Regime::LowVolRanging
    }
}

/// Convenience: should the ML signal be allowed to fire?
pub fn ml_signal_permitted(regime: Regime) -> bool {
    !matches!(regime, Regime::HighVolEvent)
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Classification logic ──────────────────────────────────────────────

    #[test]
    fn calm_no_trend_is_ranging() {
        assert_eq!(classify_regime(0.0, 1.0, 0.0), Regime::LowVolRanging);
    }

    #[test]
    fn calm_with_trend_is_trending() {
        assert_eq!(classify_regime(0.0, 1.0, 0.30), Regime::LowVolTrending);
    }

    #[test]
    fn high_z_score_is_event() {
        assert_eq!(classify_regime(2.5, 1.0, 0.0), Regime::HighVolEvent);
        assert_eq!(classify_regime(5.0, 1.0, 0.5), Regime::HighVolEvent); // even with trend
    }

    #[test]
    fn high_vol_ratio_is_event() {
        assert_eq!(classify_regime(0.5, 4.0, 0.0), Regime::HighVolEvent);
    }

    #[test]
    fn either_high_signal_triggers_event() {
        // Just z high
        assert_eq!(classify_regime(2.1, 1.0, 0.0), Regime::HighVolEvent);
        // Just ratio high
        assert_eq!(classify_regime(0.0, 3.1, 0.0), Regime::HighVolEvent);
        // Both high
        assert_eq!(classify_regime(2.1, 3.1, 0.0), Regime::HighVolEvent);
    }

    #[test]
    fn boundary_z_score_is_not_event() {
        // z = 2.0 exactly is NOT > 2.0
        assert_eq!(classify_regime(2.0, 1.0, 0.0), Regime::LowVolRanging);
    }

    #[test]
    fn boundary_vol_ratio_is_not_event() {
        assert_eq!(classify_regime(0.0, 3.0, 0.0), Regime::LowVolRanging);
    }

    #[test]
    fn boundary_trend_threshold() {
        assert_eq!(classify_regime(0.0, 1.0, 0.15), Regime::LowVolRanging);  // not > 0.15
        assert_eq!(classify_regime(0.0, 1.0, 0.16), Regime::LowVolTrending);
    }

    #[test]
    fn negative_autocorr_is_ranging_not_trending() {
        // Mean-reverting regime — not trending
        assert_eq!(classify_regime(0.0, 1.0, -0.30), Regime::LowVolRanging);
    }

    // ── NaN / Inf safety ──────────────────────────────────────────────────

    #[test]
    fn nan_inputs_default_to_ranging() {
        assert_eq!(classify_regime(f64::NAN, 1.0, 0.0), Regime::LowVolRanging);
        assert_eq!(classify_regime(0.0, f64::NAN, 0.0), Regime::LowVolRanging);
        assert_eq!(classify_regime(0.0, 1.0, f64::NAN), Regime::LowVolRanging);
        assert_eq!(classify_regime(f64::NAN, f64::NAN, f64::NAN), Regime::LowVolRanging);
    }

    #[test]
    fn inf_inputs_default_to_ranging_or_event() {
        // +Inf z_score is treated as 0 (default) — safe
        assert_eq!(classify_regime(f64::INFINITY, 1.0, 0.0), Regime::LowVolRanging);
    }

    // ── ml_signal_permitted ───────────────────────────────────────────────

    #[test]
    fn ml_permitted_in_calm_regimes() {
        assert!(ml_signal_permitted(Regime::LowVolRanging));
        assert!(ml_signal_permitted(Regime::LowVolTrending));
    }

    #[test]
    fn ml_blocked_during_event() {
        assert!(!ml_signal_permitted(Regime::HighVolEvent));
    }

    // ── Realistic scenario combinations ───────────────────────────────────

    #[test]
    fn scenario_btc_normal_trending_morning() {
        // Calm trend day: vol_5m ~ vol_30m, autocorr indicates mild momentum
        assert_eq!(classify_regime(-0.1, 0.9, 0.20), Regime::LowVolTrending);
    }

    #[test]
    fn scenario_btc_choppy_consolidation() {
        // Mean-reverting, range-bound
        assert_eq!(classify_regime(0.05, 1.05, -0.20), Regime::LowVolRanging);
    }

    #[test]
    fn scenario_fomc_announcement_spike() {
        // Sudden vol spike — e.g. CPI release
        assert_eq!(classify_regime(3.5, 5.0, 0.10), Regime::HighVolEvent);
    }

    #[test]
    fn scenario_post_event_vol_decay() {
        // vol_5m still elevated but ratio decaying
        assert_eq!(classify_regime(1.5, 2.0, 0.05), Regime::LowVolRanging);
    }
}
