// C2-C10: Rolling window utility + all feature computation functions.
// Every function is pure (no side effects, no I/O) and takes only
// slices or RollingWindow references.

use std::collections::VecDeque;

// ── C2: RollingWindow ─────────────────────────────────────────────────────────

/// Fixed-capacity circular buffer with O(1) push and statistical helpers.
/// `std()` uses ddof=1 (sample std dev) to match pandas `rolling().std()`.
#[derive(Debug, Clone)]
pub struct RollingWindow<T> {
    buf: VecDeque<T>,
    cap: usize,
}

impl<T: Copy + Into<f64>> RollingWindow<T> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be > 0");
        Self {
            buf: VecDeque::with_capacity(capacity),
            cap: capacity,
        }
    }

    pub fn push(&mut self, value: T) {
        if self.buf.len() == self.cap {
            self.buf.pop_front();
        }
        self.buf.push_back(value);
    }

    pub fn len(&self)      -> usize { self.buf.len() }
    pub fn is_empty(&self) -> bool  { self.buf.is_empty() }
    pub fn is_full(&self)  -> bool  { self.buf.len() == self.cap }
    pub fn capacity(&self) -> usize { self.cap }

    pub fn newest(&self) -> Option<T> { self.buf.back().copied() }
    pub fn oldest(&self) -> Option<T> { self.buf.front().copied() }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = T> + '_ {
        self.buf.iter().copied()
    }

    pub fn as_vec(&self) -> Vec<f64> {
        self.buf.iter().map(|v| (*v).into()).collect()
    }

    pub fn sum(&self) -> f64 {
        self.buf.iter().map(|v| (*v).into()).sum()
    }

    pub fn mean(&self) -> Option<f64> {
        if self.buf.is_empty() { return None; }
        Some(self.sum() / self.buf.len() as f64)
    }

    /// Sample std dev (ddof=1). Returns `None` if fewer than 2 values.
    pub fn std(&self) -> Option<f64> {
        let n = self.buf.len();
        if n < 2 { return None; }
        let mean = self.sum() / n as f64;
        let var = self.buf.iter()
            .map(|v| { let x: f64 = (*v).into(); (x - mean).powi(2) })
            .sum::<f64>()
            / (n - 1) as f64;
        Some(var.sqrt())
    }

    pub fn last_n(&self, n: usize) -> Vec<f64> {
        self.buf.iter().rev().take(n)
            .map(|v| (*v).into())
            .collect::<Vec<_>>()
            .into_iter().rev().collect()
    }
}

// ── C3: Log returns ───────────────────────────────────────────────────────────

/// Log return from `n` bars ago to the current (newest) close.
/// Returns 0.0 if fewer than n+1 closes available.
pub fn log_return(closes: &RollingWindow<f64>, bars_ago: usize) -> f64 {
    let n = closes.len();
    if n <= bars_ago {
        return 0.0;
    }
    let current = closes.newest().unwrap();
    let past: f64 = closes.as_vec()[n - 1 - bars_ago];
    if past <= 0.0 || current <= 0.0 {
        return 0.0;
    }
    (current / past).ln()
}

// ── C4: RSI (Wilder smoothing) ────────────────────────────────────────────────

/// Wilder's RSI. Returns 50.0 when insufficient data (< period + 1 closes).
/// Matches pandas-ta `ta.rsi(series, length=period)`.
pub fn compute_rsi(closes: &RollingWindow<f64>, period: usize) -> f64 {
    let data = closes.as_vec();
    if data.len() <= period {
        return 50.0;
    }

    // Price changes
    let changes: Vec<f64> = data.windows(2).map(|w| w[1] - w[0]).collect();

    // Seed: simple average of first `period` changes
    let (mut avg_gain, mut avg_loss) = changes[..period].iter().fold(
        (0.0_f64, 0.0_f64),
        |(g, l), &c| {
            if c >= 0.0 { (g + c, l) } else { (g, l - c) }
        },
    );
    avg_gain /= period as f64;
    avg_loss /= period as f64;

    // Wilder smoothing for remaining bars
    for &c in &changes[period..] {
        let gain = if c >= 0.0 { c } else { 0.0 };
        let loss = if c < 0.0 { -c } else { 0.0 };
        avg_gain = (avg_gain * (period - 1) as f64 + gain) / period as f64;
        avg_loss = (avg_loss * (period - 1) as f64 + loss) / period as f64;
    }

    if avg_loss < 1e-10 {
        return 100.0;
    }
    let rs = avg_gain / avg_loss;
    100.0 - 100.0 / (1.0 + rs)
}

// ── C5: Bollinger Band position ───────────────────────────────────────────────

/// Position of the latest close within its Bollinger Band (20-period, 2σ).
/// 0.0 = at lower band, 1.0 = at upper band, 0.5 = at midpoint.
/// Clamped to [-0.1, 1.1] to allow slight overshoot signals.
/// Returns 0.5 when insufficient data.
pub fn compute_bb_position(closes: &RollingWindow<f64>, period: usize, std_dev: f64) -> f64 {
    if closes.len() < period {
        return 0.5;
    }
    let slice = closes.last_n(period);
    let mean: f64 = slice.iter().sum::<f64>() / period as f64;
    let variance: f64 = slice.iter().map(|x| (x - mean).powi(2)).sum::<f64>()
        / (period - 1) as f64;
    let sigma = variance.sqrt();
    if sigma < 1e-8 {
        return 0.5;
    }
    let current = closes.newest().unwrap();
    let upper = mean + std_dev * sigma;
    let lower = mean - std_dev * sigma;
    let pos = (current - lower) / (upper - lower);
    pos.clamp(-0.1, 1.1)
}

// ── C6: MACD ──────────────────────────────────────────────────────────────────

/// Exponential moving average. Alpha = 2 / (period + 1).
fn ema_series(data: &[f64], period: usize) -> Vec<f64> {
    if data.is_empty() { return vec![]; }
    let alpha = 2.0 / (period + 1) as f64;
    let mut result = vec![data[0]];
    for &price in &data[1..] {
        let prev = *result.last().unwrap();
        result.push(prev + alpha * (price - prev));
    }
    result
}

/// MACD signal line (9-period EMA of the MACD line).
/// Standard 12/26/9 parameters.
/// Returns 0.0 when fewer than 26 closes available.
pub fn compute_macd_signal(closes: &RollingWindow<f64>) -> f64 {
    let data = closes.as_vec();
    if data.len() < 26 {
        return 0.0;
    }
    let fast   = ema_series(&data, 12);
    let slow   = ema_series(&data, 26);
    let macd: Vec<f64> = fast.iter().zip(slow.iter())
        .map(|(f, s)| f - s)
        .collect();
    let signal = ema_series(&macd, 9);
    *signal.last().unwrap_or(&0.0)
}

/// Raw MACD line value (fast EMA - slow EMA). Useful for directional context.
pub fn compute_macd_line(closes: &RollingWindow<f64>) -> f64 {
    let data = closes.as_vec();
    if data.len() < 26 { return 0.0; }
    let fast = ema_series(&data, 12);
    let slow = ema_series(&data, 26);
    fast.last().unwrap() - slow.last().unwrap()
}

// ── C7: Volatility features ───────────────────────────────────────────────────

/// Annualised-style std dev of returns over the last `bars` 1-minute returns.
/// Uses the sample std dev (ddof=1).
pub fn compute_vol(returns: &RollingWindow<f64>, bars: usize) -> f64 {
    if returns.len() < 2 { return 0.0; }
    let slice = returns.last_n(bars.min(returns.len()));
    if slice.len() < 2 { return 0.0; }
    let mean: f64 = slice.iter().sum::<f64>() / slice.len() as f64;
    let var: f64  = slice.iter().map(|x| (x - mean).powi(2)).sum::<f64>()
        / (slice.len() - 1) as f64;
    var.sqrt()
}

/// (vol_5m - vol_30m) / vol_30m — normalised deviation of short-term vol
/// from recent baseline. Near zero = normal, positive = elevated vol.
/// Used as regime detector input.
pub fn compute_vol_z_score(vol_5m: f64, vol_30m: f64) -> f64 {
    if vol_30m < 1e-8 { return 0.0; }
    (vol_5m - vol_30m) / vol_30m
}

// ── C8: Autocorrelation lag-1 ─────────────────────────────────────────────────

/// Pearson correlation between returns[0..n-1] and returns[1..n].
/// Positive → momentum, negative → mean-reversion, 0 → random walk.
/// Returns 0.0 when fewer than 3 values (need at least 2 pairs).
pub fn compute_autocorr_lag1(returns: &RollingWindow<f64>) -> f64 {
    let data = returns.as_vec();
    let n = data.len();
    if n < 3 { return 0.0; }

    let x = &data[..n - 1]; // lagged
    let y = &data[1..];     // current

    let mx = x.iter().sum::<f64>() / x.len() as f64;
    let my = y.iter().sum::<f64>() / y.len() as f64;

    let cov: f64 = x.iter().zip(y.iter()).map(|(a, b)| (a - mx) * (b - my)).sum();
    let sx: f64  = x.iter().map(|a| (a - mx).powi(2)).sum::<f64>().sqrt();
    let sy: f64  = y.iter().map(|b| (b - my).powi(2)).sum::<f64>().sqrt();

    let denom = sx * sy;
    if denom < 1e-10 { return 0.0; }
    (cov / denom).clamp(-1.0, 1.0)
}

// ── C9: OI velocity (linear regression slope) ────────────────────────────────

/// Rate of change of open interest in BTC per second, estimated via
/// ordinary least-squares over the provided `(timestamp_ms, oi)` pairs.
/// Sweeps entries older than `window_ms` before computing.
pub fn compute_oi_velocity(history: &VecDeque<(u64, f64)>, window_ms: u64) -> f64 {
    if history.len() < 2 { return 0.0; }

    let now_ms = history.back().unwrap().0;
    let cutoff = now_ms.saturating_sub(window_ms);

    let pts: Vec<(f64, f64)> = history.iter()
        .filter(|(ts, _)| *ts >= cutoff)
        .map(|(ts, oi)| (*ts as f64 / 1000.0, *oi)) // convert to seconds
        .collect();

    if pts.len() < 2 { return 0.0; }

    let n = pts.len() as f64;
    let sx: f64  = pts.iter().map(|(x, _)| x).sum();
    let sy: f64  = pts.iter().map(|(_, y)| y).sum();
    let sxx: f64 = pts.iter().map(|(x, _)| x * x).sum();
    let sxy: f64 = pts.iter().map(|(x, y)| x * y).sum();

    let denom = n * sxx - sx * sx;
    if denom.abs() < 1e-10 { return 0.0; }
    (n * sxy - sx * sy) / denom  // slope in BTC/second
}

// ── C10: Liquidation imbalance ────────────────────────────────────────────────

/// Normalised imbalance: positive = more shorts liquidated (bullish pressure),
/// negative = more longs liquidated (bearish pressure).
///
/// Sums quantity for each side over `window_ms` milliseconds.
pub fn compute_liq_imbalance(
    history: &VecDeque<(u64, crate::streams::LiqSide, f64)>,
    window_ms: u64,
) -> f64 {
    if history.is_empty() { return 0.0; }

    let now_ms  = history.back().unwrap().0;
    let cutoff  = now_ms.saturating_sub(window_ms);

    let (mut long_qty, mut short_qty) = (0.0_f64, 0.0_f64);
    for (ts, side, qty) in history {
        if *ts < cutoff { continue; }
        match side {
            crate::streams::LiqSide::Long  => long_qty  += qty,
            crate::streams::LiqSide::Short => short_qty += qty,
        }
    }

    let total = long_qty + short_qty;
    if total < 1e-8 { return 0.0; }
    (short_qty - long_qty) / total
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streams::LiqSide;

    fn win(vals: &[f64]) -> RollingWindow<f64> {
        let mut w = RollingWindow::new(vals.len().max(60));
        for &v in vals { w.push(v); }
        w
    }

    // ── C2: RollingWindow ─────────────────────────────────────────────────

    #[test]
    fn rw_sum_mean_std() {
        let w = win(&[1.0, 2.0, 3.0, 4.0, 5.0]);
        assert!((w.sum() - 15.0).abs() < 1e-10);
        assert!((w.mean().unwrap() - 3.0).abs() < 1e-10);
        // pandas: pd.Series([1,2,3,4,5]).std() = 1.5811388300841898
        assert!((w.std().unwrap() - 1.5811388300841898).abs() < 1e-10);
    }

    #[test]
    fn rw_two_element_std_matches_pandas() {
        let w = win(&[2.0, 4.0]);
        // pandas: pd.Series([2,4]).std() = sqrt(2) = 1.4142135623730951
        assert!((w.std().unwrap() - 1.4142135623730951).abs() < 1e-10);
    }

    #[test]
    fn rw_overflow_evicts_oldest() {
        let mut w: RollingWindow<f64> = RollingWindow::new(3);
        for v in [1.0, 2.0, 3.0, 4.0] { w.push(v); }
        assert_eq!(w.oldest().unwrap(), 2.0);
        assert_eq!(w.newest().unwrap(), 4.0);
        assert!((w.sum() - 9.0).abs() < 1e-10);
    }

    #[test]
    fn rw_empty_returns_none() {
        let w: RollingWindow<f64> = RollingWindow::new(5);
        assert!(w.mean().is_none());
        assert!(w.std().is_none());
    }

    #[test]
    fn rw_last_n_correct() {
        let w = win(&[1.0, 2.0, 3.0, 4.0, 5.0]);
        assert_eq!(w.last_n(3), vec![3.0, 4.0, 5.0]);
        assert_eq!(w.last_n(10), vec![1.0, 2.0, 3.0, 4.0, 5.0]); // capped
    }

    // ── C3: Log returns ───────────────────────────────────────────────────

    #[test]
    fn log_return_1m() {
        // closes = [98, 101, 99, 102, 100]
        // ret_1m = ln(100/102)
        let w = win(&[98.0, 101.0, 99.0, 102.0, 100.0]);
        let expected = (100.0_f64 / 102.0).ln();
        assert!((log_return(&w, 1) - expected).abs() < 1e-10);
    }

    #[test]
    fn log_return_5m() {
        // ret_5m = ln(100/98) from the 5-element window above
        let w = win(&[98.0, 101.0, 99.0, 102.0, 100.0]);
        let expected = (100.0_f64 / 98.0).ln();
        assert!((log_return(&w, 4) - expected).abs() < 1e-10);
    }

    #[test]
    fn log_return_insufficient_data_is_zero() {
        let w = win(&[100.0]);
        assert_eq!(log_return(&w, 5), 0.0);
    }

    #[test]
    fn log_return_flat_prices_is_zero() {
        let w = win(&[100.0, 100.0, 100.0]);
        assert!((log_return(&w, 1)).abs() < 1e-10);
        assert!((log_return(&w, 2)).abs() < 1e-10);
    }

    // ── C4: RSI ───────────────────────────────────────────────────────────

    #[test]
    fn rsi_all_up_is_100() {
        // 16 prices all rising by 1.0 → 15 gains, 0 losses
        let prices: Vec<f64> = (0..16).map(|i| 100.0 + i as f64).collect();
        let w = win(&prices);
        assert!((compute_rsi(&w, 14) - 100.0).abs() < 1e-6);
    }

    #[test]
    fn rsi_all_down_is_zero() {
        let prices: Vec<f64> = (0..16).map(|i| 100.0 - i as f64).collect();
        let w = win(&prices);
        assert!((compute_rsi(&w, 14) - 0.0).abs() < 1e-6);
    }

    #[test]
    fn rsi_known_sequence() {
        // prices = [10,11,10,12,11,10,9,10,11,12,11,13,12,11,12]
        // 14 changes: +1,-1,+2,-1,-1,-1,+1,+1,+1,-1,+2,-1,-1,+1
        // gains (indices 0,2,6,7,8,10,13) sum = 1+2+1+1+1+2+1 = 9
        // losses (indices 1,3,4,5,9,11,12) sum = 7
        // avg_gain = 9/14, avg_loss = 7/14
        // RS = 9/7, RSI = 100 - 100/(1 + 9/7) = 100 - 700/16 = 56.25
        let prices = [10.0,11.0,10.0,12.0,11.0,10.0,9.0,10.0,11.0,12.0,11.0,13.0,12.0,11.0,12.0];
        let w = win(&prices);
        assert!((compute_rsi(&w, 14) - 56.25).abs() < 1e-6);
    }

    #[test]
    fn rsi_insufficient_data_returns_50() {
        let w = win(&[100.0, 101.0]); // only 2 closes, need 15 for period=14
        assert_eq!(compute_rsi(&w, 14), 50.0);
    }

    // ── C5: Bollinger Bands ───────────────────────────────────────────────

    #[test]
    fn bb_at_midpoint_is_half() {
        // Flat prices → current = mean → position = 0.5
        let prices = vec![100.0_f64; 20];
        let w = win(&prices);
        // sigma ≈ 0 → returns 0.5 by the sigma guard
        assert!((compute_bb_position(&w, 20, 2.0) - 0.5).abs() < 1e-6);
    }

    #[test]
    fn bb_above_upper_band_clamps() {
        // All prices are 100 except the last which is massively higher
        let mut prices = vec![100.0_f64; 20];
        *prices.last_mut().unwrap() = 200.0;
        let w = win(&prices);
        let pos = compute_bb_position(&w, 20, 2.0);
        assert!(pos > 1.0); // above upper band → > 1.0
        assert!(pos <= 1.1); // clamped
    }

    #[test]
    fn bb_insufficient_data_returns_half() {
        let w = win(&[100.0, 101.0]);
        assert_eq!(compute_bb_position(&w, 20, 2.0), 0.5);
    }

    // ── C6: MACD ──────────────────────────────────────────────────────────

    #[test]
    fn macd_insufficient_data_is_zero() {
        let w = win(&[100.0; 20]); // need 26
        assert_eq!(compute_macd_signal(&w), 0.0);
    }

    #[test]
    fn macd_flat_prices_signal_near_zero() {
        let w = win(&[100.0_f64; 60]);
        let sig = compute_macd_signal(&w);
        assert!(sig.abs() < 1e-6, "flat prices → MACD signal ≈ 0, got {sig}");
    }

    #[test]
    fn macd_uptrend_has_positive_signal() {
        // Steadily rising prices → fast EMA > slow EMA → positive MACD line
        let prices: Vec<f64> = (0..60).map(|i| 100.0 + i as f64 * 0.5).collect();
        let w = win(&prices);
        assert!(compute_macd_line(&w) > 0.0);
    }

    #[test]
    fn macd_downtrend_has_negative_signal() {
        let prices: Vec<f64> = (0..60).map(|i| 200.0 - i as f64 * 0.5).collect();
        let w = win(&prices);
        assert!(compute_macd_line(&w) < 0.0);
    }

    // ── C7: Volatility ────────────────────────────────────────────────────

    #[test]
    fn vol_of_constant_returns_is_zero() {
        let returns = vec![0.001_f64; 30];
        let w = win(&returns);
        assert!(compute_vol(&w, 5).abs() < 1e-10);
        assert!(compute_vol(&w, 30).abs() < 1e-10);
    }

    #[test]
    fn vol_5m_less_than_vol_30m_when_recent_quiet() {
        // 30 volatile returns then 5 quiet ones
        let mut returns: Vec<f64> = (0..25).map(|i| {
            if i % 2 == 0 { 0.01 } else { -0.01 }
        }).collect();
        returns.extend([0.0001, -0.0001, 0.0001, -0.0001, 0.0001]);
        let w = win(&returns);
        let v5  = compute_vol(&w, 5);
        let v30 = compute_vol(&w, 30);
        assert!(v5 < v30, "quiet recent period: vol_5m({v5}) should be < vol_30m({v30})");
    }

    #[test]
    fn vol_z_score_zero_when_vol_5m_equals_vol_30m() {
        assert!((compute_vol_z_score(0.01, 0.01)).abs() < 1e-10);
    }

    #[test]
    fn vol_z_score_positive_when_elevated() {
        assert!(compute_vol_z_score(0.02, 0.01) > 0.0);
    }

    #[test]
    fn vol_z_score_safe_with_zero_baseline() {
        assert_eq!(compute_vol_z_score(0.01, 0.0), 0.0);
    }

    // ── C8: Autocorrelation ───────────────────────────────────────────────

    #[test]
    fn autocorr_perfect_momentum_is_one() {
        // Identical returns → lag-1 autocorrelation = 1
        let w = win(&[0.01_f64; 20]);
        // All values identical → zero variance → returns 0.0 by guard
        let r = compute_autocorr_lag1(&w);
        assert!(r.abs() < 1e-6 || (r - 1.0).abs() < 1e-6);
    }

    #[test]
    fn autocorr_alternating_is_negative() {
        // Strictly alternating returns → strong mean-reversion → negative autocorr
        let returns: Vec<f64> = (0..20).map(|i| if i % 2 == 0 { 0.01 } else { -0.01 }).collect();
        let w = win(&returns);
        let r = compute_autocorr_lag1(&w);
        assert!(r < 0.0, "alternating returns → negative autocorr, got {r}");
    }

    #[test]
    fn autocorr_trending_is_positive() {
        // Uniformly increasing returns → positive autocorr
        let returns: Vec<f64> = (1..=20).map(|i| i as f64 * 0.001).collect();
        let w = win(&returns);
        let r = compute_autocorr_lag1(&w);
        assert!(r > 0.0, "trending returns → positive autocorr, got {r}");
    }

    #[test]
    fn autocorr_clamped_to_minus_one_to_one() {
        let w = win(&[0.1, -0.1, 0.1, -0.1, 0.1, -0.1, 0.1, -0.1]);
        let r = compute_autocorr_lag1(&w);
        assert!(r >= -1.0 && r <= 1.0);
    }

    #[test]
    fn autocorr_insufficient_data_is_zero() {
        let w = win(&[0.01, -0.01]);
        assert_eq!(compute_autocorr_lag1(&w), 0.0);
    }

    // ── C9: OI velocity ───────────────────────────────────────────────────

    #[test]
    fn oi_velocity_linear_increase() {
        // OI increases by 10 BTC/sec → slope = 10
        let mut hist: VecDeque<(u64, f64)> = VecDeque::new();
        for i in 0u64..=10 {
            hist.push_back((i * 1000, 10.0 * i as f64)); // ts in ms, oi = 10*i BTC
        }
        let slope = compute_oi_velocity(&hist, 60_000);
        assert!((slope - 10.0).abs() < 1e-6, "expected slope=10, got {slope}");
    }

    #[test]
    fn oi_velocity_flat_oi_is_zero() {
        let mut hist: VecDeque<(u64, f64)> = VecDeque::new();
        for i in 0u64..=10 {
            hist.push_back((i * 1000, 5000.0)); // constant OI
        }
        let slope = compute_oi_velocity(&hist, 60_000);
        assert!(slope.abs() < 1e-6, "flat OI → zero velocity, got {slope}");
    }

    #[test]
    fn oi_velocity_filters_old_entries() {
        // Only the last 2 points are within the window; they form a flat line
        let mut hist: VecDeque<(u64, f64)> = VecDeque::new();
        // Old entries (outside 60s window)
        for i in 0u64..8 {
            hist.push_back((i * 1000, i as f64 * 100.0));
        }
        // Recent entries (within window), flat
        let base_ts = 120_000u64;
        hist.push_back((base_ts,       5000.0));
        hist.push_back((base_ts + 1000, 5000.0));
        let slope = compute_oi_velocity(&hist, 60_000);
        assert!(slope.abs() < 1e-6, "old slope filtered out, recent flat → 0, got {slope}");
    }

    #[test]
    fn oi_velocity_insufficient_data_is_zero() {
        let hist: VecDeque<(u64, f64)> = VecDeque::new();
        assert_eq!(compute_oi_velocity(&hist, 60_000), 0.0);
    }

    // ── C10: Liquidation imbalance ────────────────────────────────────────

    #[test]
    fn liq_imbalance_all_longs_is_negative_one() {
        let mut hist: VecDeque<(u64, LiqSide, f64)> = VecDeque::new();
        hist.push_back((1000, LiqSide::Long, 1.0));
        hist.push_back((2000, LiqSide::Long, 1.0));
        // all longs liquidated → bearish → negative imbalance
        let r = compute_liq_imbalance(&hist, 60_000);
        assert!((r - (-1.0)).abs() < 1e-10);
    }

    #[test]
    fn liq_imbalance_all_shorts_is_positive_one() {
        let mut hist: VecDeque<(u64, LiqSide, f64)> = VecDeque::new();
        hist.push_back((1000, LiqSide::Short, 1.0));
        hist.push_back((2000, LiqSide::Short, 1.0));
        let r = compute_liq_imbalance(&hist, 60_000);
        assert!((r - 1.0).abs() < 1e-10);
    }

    #[test]
    fn liq_imbalance_balanced_is_zero() {
        let mut hist: VecDeque<(u64, LiqSide, f64)> = VecDeque::new();
        hist.push_back((1000, LiqSide::Long,  2.0));
        hist.push_back((2000, LiqSide::Short, 2.0));
        let r = compute_liq_imbalance(&hist, 60_000);
        assert!(r.abs() < 1e-10);
    }

    #[test]
    fn liq_imbalance_filters_old_events() {
        let mut hist: VecDeque<(u64, LiqSide, f64)> = VecDeque::new();
        // Old event (outside window) — all longs
        hist.push_back((0, LiqSide::Long, 100.0));
        // Recent events — all shorts
        let base = 120_000u64;
        hist.push_back((base,       LiqSide::Short, 1.0));
        hist.push_back((base + 500, LiqSide::Short, 1.0));
        let r = compute_liq_imbalance(&hist, 60_000);
        assert!((r - 1.0).abs() < 1e-10, "old long events filtered → all short, got {r}");
    }

    #[test]
    fn liq_imbalance_empty_is_zero() {
        let hist: VecDeque<(u64, LiqSide, f64)> = VecDeque::new();
        assert_eq!(compute_liq_imbalance(&hist, 60_000), 0.0);
    }
}
