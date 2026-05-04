// Concern C3 — Slippage tracker + cost-model drift detection.
//
// Aggregates (expected, actual) fill prices into a rolling window and
// surfaces the realised slippage. Designed to feed two consumers:
//
//   1. The cost model (compute_paper_pnl) — once we have enough samples
//      we can replace the static ENTRY_SLIPPAGE constant with the
//      observed mean (or p95 for conservative sizing).
//   2. The watchdog — if realised slippage drifts above a threshold
//      (e.g. 2x the static assumption) we trip the kill switch.
//
// Lives on the execution side (close to where fills land) rather than in
// risk/ because it's a measurement, not a gate. The kill-switch wiring
// stays in the risk module.
//
// Parallelism: tracker is internally locked but operations are O(1) so
// contention is negligible. The aggregation (mean / p95) reads a
// snapshot copy, so heartbeat / watchdog can poll without blocking
// writers.

use std::sync::Mutex;

#[derive(Debug, Clone, Copy)]
pub struct FillRecord {
    /// Price the strategy expected at signal time (the order's limit price).
    pub expected_price: f64,
    /// Price actually filled (from the user-channel websocket).
    pub actual_price:   f64,
    /// "yes" / "no" — kept for per-side analytics later.
    pub side:           &'static str,
    pub timestamp_ms:   u64,
}

impl FillRecord {
    /// Slippage as a fraction of the expected price. Positive means we
    /// paid MORE than expected (worse for BUY). Negative = price improvement.
    pub fn slippage(&self) -> f64 {
        if self.expected_price <= 0.0 { return 0.0; }
        (self.actual_price - self.expected_price) / self.expected_price
    }
}

#[derive(Debug, Clone, Default)]
pub struct SlippageStats {
    pub n:               usize,
    pub mean_slippage:   f64,    // average fractional slippage
    pub max_slippage:    f64,    // worst-case observed
    pub p95_slippage:    f64,    // 95th percentile (conservative)
    pub mean_abs_slippage: f64,  // |slippage| — penalises both directions
}

#[derive(Debug)]
pub struct SlippageTracker {
    capacity: usize,
    inner:    Mutex<Vec<FillRecord>>,
}

impl SlippageTracker {
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.max(8);   // keep a meaningful minimum
        Self {
            capacity: cap,
            inner:    Mutex::new(Vec::with_capacity(cap)),
        }
    }

    /// Record one fill. Drops the oldest record when the window is full.
    pub fn record(&self, fill: FillRecord) {
        if !fill.actual_price.is_finite() || !fill.expected_price.is_finite() {
            return;   // garbage in, garbage out — refuse to taint the stats
        }
        let mut g = self.inner.lock().expect("poisoned");
        if g.len() >= self.capacity {
            g.remove(0);   // O(n) for small capacity (≤ a few hundred) is fine
        }
        g.push(fill);
    }

    /// Read-only snapshot. Aggregates over the current window.
    pub fn stats(&self) -> SlippageStats {
        let g = self.inner.lock().expect("poisoned");
        if g.is_empty() { return SlippageStats::default(); }

        let n = g.len();
        let slips: Vec<f64> = g.iter().map(|f| f.slippage()).collect();
        let mean = slips.iter().sum::<f64>() / n as f64;
        let max  = slips.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let mean_abs = slips.iter().map(|s| s.abs()).sum::<f64>() / n as f64;

        // p95 — sort a copy ascending, pick (n*0.95) index
        let mut sorted = slips.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let p95_idx = ((n as f64 * 0.95).ceil() as usize).saturating_sub(1).min(n - 1);
        let p95 = sorted[p95_idx];

        SlippageStats {
            n, mean_slippage: mean,
            max_slippage: max, p95_slippage: p95,
            mean_abs_slippage: mean_abs,
        }
    }

    pub fn len(&self) -> usize {
        self.inner.lock().expect("poisoned").len()
    }
    pub fn is_empty(&self) -> bool { self.len() == 0 }
    pub fn clear(&self) {
        self.inner.lock().expect("poisoned").clear();
    }

    /// Health gate. Returns true if realised slippage is > `factor` x the
    /// static assumption. Caller decides what to do (warn vs trip).
    pub fn drift_exceeds(&self, static_assumption: f64, factor: f64) -> bool {
        let s = self.stats();
        if s.n < 10 { return false; }   // not enough data to be confident
        s.mean_abs_slippage > static_assumption * factor
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn fill(expected: f64, actual: f64) -> FillRecord {
        FillRecord {
            expected_price: expected,
            actual_price:   actual,
            side:           "yes",
            timestamp_ms:   0,
        }
    }

    #[test]
    fn slippage_positive_when_actual_higher_than_expected() {
        let f = fill(0.50, 0.51);
        assert!((f.slippage() - 0.02).abs() < 1e-6);   // 2% slippage
    }

    #[test]
    fn slippage_negative_when_price_improvement() {
        let f = fill(0.50, 0.49);
        assert!((f.slippage() + 0.02).abs() < 1e-6);
    }

    #[test]
    fn slippage_zero_for_zero_expected() {
        let f = fill(0.0, 0.0);
        assert_eq!(f.slippage(), 0.0);
    }

    // ── Tracker basics ────────────────────────────────────────────────────

    #[test]
    fn empty_tracker_has_zero_stats() {
        let t = SlippageTracker::new(100);
        let s = t.stats();
        assert_eq!(s.n, 0);
        assert_eq!(s.mean_slippage, 0.0);
    }

    #[test]
    fn record_appends_and_capacity_evicts_oldest() {
        let t = SlippageTracker::new(8);    // minimum
        for i in 0..20 {
            t.record(fill(0.50, 0.50 + i as f64 * 0.001));
        }
        assert_eq!(t.len(), 8);
    }

    #[test]
    fn stats_compute_mean_correctly() {
        let t = SlippageTracker::new(100);
        // Three fills: 1%, 2%, 3% slippage → mean 2%
        t.record(fill(0.50, 0.505));   // 1%
        t.record(fill(0.50, 0.510));   // 2%
        t.record(fill(0.50, 0.515));   // 3%
        let s = t.stats();
        assert_eq!(s.n, 3);
        assert!((s.mean_slippage - 0.02).abs() < 1e-9);
        assert!((s.max_slippage  - 0.03).abs() < 1e-9);
    }

    #[test]
    fn p95_is_near_top_of_distribution() {
        let t = SlippageTracker::new(100);
        // 100 fills, slippage 1%-100% (linear)
        for i in 1..=100 {
            t.record(fill(1.0, 1.0 + i as f64 * 0.01));
        }
        let s = t.stats();
        assert_eq!(s.n, 100);
        // p95 should be around 0.95 (the 95th value in 0.01..1.00 ≈ 0.95)
        assert!((s.p95_slippage - 0.95).abs() < 0.01,
                "expected p95 ~0.95, got {}", s.p95_slippage);
    }

    #[test]
    fn mean_abs_treats_both_directions_as_slippage() {
        let t = SlippageTracker::new(100);
        // Symmetric: +2%, -2% → mean 0, mean_abs 2%
        t.record(fill(0.50, 0.51));
        t.record(fill(0.50, 0.49));
        let s = t.stats();
        assert!(s.mean_slippage.abs() < 1e-9);
        assert!((s.mean_abs_slippage - 0.02).abs() < 1e-9);
    }

    // ── Bad-input rejection ───────────────────────────────────────────────

    #[test]
    fn record_rejects_nan() {
        let t = SlippageTracker::new(100);
        t.record(fill(0.50, f64::NAN));
        assert_eq!(t.len(), 0);
    }

    #[test]
    fn record_rejects_infinity() {
        let t = SlippageTracker::new(100);
        t.record(fill(f64::INFINITY, 0.50));
        t.record(fill(0.50, f64::NEG_INFINITY));
        assert_eq!(t.len(), 0);
    }

    // ── Drift gate ─────────────────────────────────────────────────────────

    #[test]
    fn drift_gate_returns_false_with_too_few_samples() {
        let t = SlippageTracker::new(100);
        // Only 5 samples — gate suppresses to avoid noisy trips
        for _ in 0..5 {
            t.record(fill(0.50, 0.60));   // 20% slippage — would otherwise trip
        }
        assert!(!t.drift_exceeds(0.01, 2.0));
    }

    #[test]
    fn drift_gate_trips_when_drift_exceeds_factor() {
        let t = SlippageTracker::new(100);
        for _ in 0..20 {
            t.record(fill(0.50, 0.515));   // 3% slippage
        }
        // Static assumption 1%, factor 2x → 2% threshold. 3% > 2% → trip.
        assert!(t.drift_exceeds(0.01, 2.0));
    }

    #[test]
    fn drift_gate_quiet_when_within_assumption() {
        let t = SlippageTracker::new(100);
        for _ in 0..20 {
            t.record(fill(0.50, 0.503));   // 0.6% slippage, well below 2%
        }
        assert!(!t.drift_exceeds(0.01, 2.0));
    }

    // ── Concurrency smoke test ─────────────────────────────────────────────

    #[test]
    fn concurrent_records_do_not_corrupt() {
        use std::sync::Arc;
        let t = Arc::new(SlippageTracker::new(1024));
        let mut handles = Vec::new();
        for i in 0..8 {
            let t = t.clone();
            handles.push(std::thread::spawn(move || {
                for j in 0..100 {
                    t.record(fill(0.50, 0.50 + (i*100 + j) as f64 * 0.0001));
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(t.len(), 800);
    }

    // ── Clear ──────────────────────────────────────────────────────────────

    #[test]
    fn clear_resets_window() {
        let t = SlippageTracker::new(100);
        t.record(fill(0.50, 0.51));
        assert_eq!(t.len(), 1);
        t.clear();
        assert_eq!(t.len(), 0);
        assert_eq!(t.stats().n, 0);
    }
}
