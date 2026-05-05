// F2-F4 — Risk management: limits, kill switch, P&L tracking.
//
// All counters are atomic so any task can update them without locks. The
// kill switch is sticky once auto-tripped — only manual reset clears it.
//
// Layout:
//   F2: RiskLimits struct + atomic counters
//   F3: can_trade() — checks all limits before allowing an order to submit
//   F4: record_open / record_close hooks update counters; auto-trip kill
//       switch when limits are breached

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};

use thiserror::Error;

const DAY_MS:  u64 = 24 * 60 * 60 * 1000;
const WEEK_MS: u64 = 7 * DAY_MS;

// ── Configuration ────────────────────────────────────────────────────────────

/// Risk parameters loaded from `.env` (see Config).
#[derive(Debug, Clone, Copy)]
pub struct RiskConfig {
    pub bankroll:                  f64,
    pub max_bet_dollars:           f64,
    pub kelly_fraction:            f64,
    pub min_edge:                  f64,
    pub daily_loss_limit_dollars:  f64,
    pub weekly_loss_limit_dollars: f64,
    pub max_open_exposure_dollars: f64,
    pub max_daily_trades:          u32,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            bankroll:                  1500.0,
            max_bet_dollars:           30.0,
            kelly_fraction:            0.25,
            min_edge:                  0.05,
            daily_loss_limit_dollars:  75.0,
            weekly_loss_limit_dollars: 150.0,
            max_open_exposure_dollars: 300.0,
            max_daily_trades:          100,
        }
    }
}

// ── Errors ───────────────────────────────────────────────────────────────────

#[derive(Debug, Error, PartialEq)]
pub enum RiskError {
    #[error("kill switch active — manual reset required")]
    KillSwitchActive,

    #[error("data stale — waiting for stream recovery")]
    DataStale,

    #[error("daily loss limit hit: ${current:.2} >= ${limit:.2}")]
    DailyLossLimitHit { current: f64, limit: f64 },

    #[error("weekly loss limit hit: ${current:.2} >= ${limit:.2}")]
    WeeklyLossLimitHit { current: f64, limit: f64 },

    #[error("would exceed max open exposure: ${requested:.2} + ${current:.2} > ${limit:.2}")]
    ExposureLimitHit { current: f64, requested: f64, limit: f64 },

    #[error("daily trade count exceeded: {count} >= {limit}")]
    TradeCountExceeded { count: u32, limit: u32 },

    #[error("bet size invalid or above max: ${bet:.2} (max ${limit:.2})")]
    BetSizeInvalid { bet: f64, limit: f64 },
}

// ── F2: RiskLimits struct ────────────────────────────────────────────────────

#[derive(Debug)]
pub struct RiskLimits {
    /// Accumulated losses today (cents, always >= 0)
    daily_loss_cents:     AtomicI64,
    /// Accumulated losses this ISO week (cents, always >= 0)
    weekly_loss_cents:    AtomicI64,
    /// Sum of currently-open position sizes in cents
    open_exposure_cents:  AtomicI64,
    /// Trades opened today
    daily_trade_count:    AtomicU32,
    /// Sticky once tripped — only manual reset clears
    kill_switch:          AtomicBool,
    /// G11: transient halt for stale-data scenarios. Auto-clears when the
    /// watchdog sees streams recover. Distinct from kill_switch (which
    /// represents a real failure that needs human review).
    data_stale:           AtomicBool,
    /// G7: net realised P&L in cents (signed — gains add, losses subtract).
    /// Used by current_bankroll() to compound Kelly sizing with results.
    /// Distinct from daily_loss_cents/weekly_loss_cents (those track losses
    /// only, used for limit enforcement). Never resets on day/week boundary.
    realised_pnl_cents:   AtomicI64,
    /// UTC midnight of last daily counter reset (ms since epoch)
    last_daily_reset_ms:  AtomicU64,
    /// Start of last weekly counter reset (ms since epoch)
    last_weekly_reset_ms: AtomicU64,
}

impl Default for RiskLimits {
    fn default() -> Self {
        Self::new()
    }
}

impl RiskLimits {
    pub fn new() -> Self {
        Self {
            daily_loss_cents:     AtomicI64::new(0),
            weekly_loss_cents:    AtomicI64::new(0),
            open_exposure_cents:  AtomicI64::new(0),
            daily_trade_count:    AtomicU32::new(0),
            kill_switch:          AtomicBool::new(false),
            data_stale:           AtomicBool::new(false),
            realised_pnl_cents:   AtomicI64::new(0),
            last_daily_reset_ms:  AtomicU64::new(0),
            last_weekly_reset_ms: AtomicU64::new(0),
        }
    }

    // ── F3: can_trade ──────────────────────────────────────────────────────

    /// Production entry point — uses real wall-clock time.
    pub fn can_trade(&self, bet_dollars: f64, cfg: &RiskConfig) -> Result<(), RiskError> {
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;
        self.can_trade_at(bet_dollars, now_ms, cfg)
    }

    /// Test-friendly variant — caller supplies the clock.
    pub fn can_trade_at(
        &self,
        bet_dollars: f64,
        now_ms:      u64,
        cfg:         &RiskConfig,
    ) -> Result<(), RiskError> {
        self.maybe_reset(now_ms);

        // Validate bet itself
        if !bet_dollars.is_finite() || bet_dollars <= 0.0 || bet_dollars > cfg.max_bet_dollars {
            return Err(RiskError::BetSizeInvalid { bet: bet_dollars, limit: cfg.max_bet_dollars });
        }

        // Order matters: kill switch first (cheapest, most decisive)
        if self.kill_switch.load(Ordering::SeqCst) {
            return Err(RiskError::KillSwitchActive);
        }
        // G11: data-stale halt is a transient gate (auto-clears on recovery).
        // Distinct from kill_switch — won't survive watchdog observing fresh
        // streams again.
        if self.data_stale.load(Ordering::SeqCst) {
            return Err(RiskError::DataStale);
        }

        let daily_loss = self.daily_loss_dollars();
        if daily_loss >= cfg.daily_loss_limit_dollars {
            return Err(RiskError::DailyLossLimitHit {
                current: daily_loss,
                limit:   cfg.daily_loss_limit_dollars,
            });
        }

        let weekly_loss = self.weekly_loss_dollars();
        if weekly_loss >= cfg.weekly_loss_limit_dollars {
            return Err(RiskError::WeeklyLossLimitHit {
                current: weekly_loss,
                limit:   cfg.weekly_loss_limit_dollars,
            });
        }

        let current_exposure = self.open_exposure_dollars();
        if current_exposure + bet_dollars > cfg.max_open_exposure_dollars {
            return Err(RiskError::ExposureLimitHit {
                current:   current_exposure,
                requested: bet_dollars,
                limit:     cfg.max_open_exposure_dollars,
            });
        }

        let count = self.daily_trade_count.load(Ordering::SeqCst);
        if count >= cfg.max_daily_trades {
            return Err(RiskError::TradeCountExceeded {
                count,
                limit: cfg.max_daily_trades,
            });
        }

        Ok(())
    }

    // ── F4: P&L update hooks ──────────────────────────────────────────────

    /// Call this when an order has been placed and exposure should grow.
    pub fn record_open(&self, bet_dollars: f64) {
        if !bet_dollars.is_finite() || bet_dollars <= 0.0 {
            return;
        }
        let cents = (bet_dollars * 100.0).round() as i64;
        self.open_exposure_cents.fetch_add(cents, Ordering::SeqCst);
        self.daily_trade_count.fetch_add(1, Ordering::SeqCst);
    }

    /// Call this when a position resolves. Pass the original `bet_dollars`
    /// (so exposure can be released) and `pnl_dollars` (negative = loss).
    /// Auto-trips the kill switch if daily/weekly loss limits are exceeded.
    pub fn record_close(&self, bet_dollars: f64, pnl_dollars: f64, cfg: &RiskConfig) {
        if bet_dollars.is_finite() && bet_dollars > 0.0 {
            let cents = (bet_dollars * 100.0).round() as i64;
            self.open_exposure_cents.fetch_sub(cents, Ordering::SeqCst);
            // Don't let exposure go negative due to rounding
            let v = self.open_exposure_cents.load(Ordering::SeqCst);
            if v < 0 {
                self.open_exposure_cents.store(0, Ordering::SeqCst);
            }
        }

        // G7: track signed realised P&L for current_bankroll() / kelly compounding.
        // Updated on EVERY finite pnl (gains AND losses), independent of the
        // loss-only counters used for daily/weekly limit enforcement below.
        if pnl_dollars.is_finite() {
            let pnl_cents = (pnl_dollars * 100.0).round() as i64;
            self.realised_pnl_cents.fetch_add(pnl_cents, Ordering::SeqCst);
        }

        if pnl_dollars.is_finite() && pnl_dollars < 0.0 {
            let loss_cents = (-pnl_dollars * 100.0).round() as i64;
            self.daily_loss_cents.fetch_add(loss_cents, Ordering::SeqCst);
            self.weekly_loss_cents.fetch_add(loss_cents, Ordering::SeqCst);

            // Check whether we just crossed a limit — auto-trip kill switch
            let dl = self.daily_loss_dollars();
            let wl = self.weekly_loss_dollars();
            if dl >= cfg.daily_loss_limit_dollars {
                self.activate_kill_switch();
                tracing::error!(daily_loss = dl, "DAILY LOSS LIMIT HIT — kill switch activated");
            }
            if wl >= cfg.weekly_loss_limit_dollars {
                self.activate_kill_switch();
                tracing::error!(weekly_loss = wl, "WEEKLY LOSS LIMIT HIT — kill switch activated");
            }
        }
    }

    // ── Kill switch ───────────────────────────────────────────────────────

    pub fn activate_kill_switch(&self) {
        self.kill_switch.store(true, Ordering::SeqCst);
    }

    pub fn reset_kill_switch(&self) {
        self.kill_switch.store(false, Ordering::SeqCst);
    }

    pub fn is_kill_switch_active(&self) -> bool {
        self.kill_switch.load(Ordering::SeqCst)
    }

    // ── Read-only views ───────────────────────────────────────────────────

    pub fn daily_loss_dollars(&self) -> f64 {
        self.daily_loss_cents.load(Ordering::SeqCst) as f64 / 100.0
    }

    pub fn weekly_loss_dollars(&self) -> f64 {
        self.weekly_loss_cents.load(Ordering::SeqCst) as f64 / 100.0
    }

    pub fn open_exposure_dollars(&self) -> f64 {
        self.open_exposure_cents.load(Ordering::SeqCst).max(0) as f64 / 100.0
    }

    pub fn daily_trade_count(&self) -> u32 {
        self.daily_trade_count.load(Ordering::SeqCst)
    }

    // ── G7: dynamic bankroll ──────────────────────────────────────────────

    /// Net realised P&L (in dollars). Positive on net gains, negative on
    /// net losses. Never resets — accumulates across day/week boundaries.
    pub fn realised_pnl_dollars(&self) -> f64 {
        self.realised_pnl_cents.load(Ordering::SeqCst) as f64 / 100.0
    }

    /// Current bankroll = `starting + realised_pnl`, floored at 0 to keep
    /// kelly_size from doing negative-bankroll math. Read by execute_signal
    /// each tick so kelly compounds with results.
    pub fn current_bankroll(&self, starting_bankroll: f64) -> f64 {
        (starting_bankroll + self.realised_pnl_dollars()).max(0.0)
    }

    // ── G11: data-stale flag (auto-reset by the watchdog) ─────────────────

    pub fn set_data_stale(&self, stale: bool) {
        self.data_stale.store(stale, Ordering::SeqCst);
    }

    pub fn is_data_stale(&self) -> bool {
        self.data_stale.load(Ordering::SeqCst)
    }

    // ── Resets (lazy, called from can_trade) ──────────────────────────────

    fn maybe_reset(&self, now_ms: u64) {
        // Daily
        let last_daily = self.last_daily_reset_ms.load(Ordering::SeqCst);
        if last_daily == 0 {
            // First call — initialise without resetting (preserves any
            // pre-existing counters set by record_close before the first
            // can_trade call)
            let _ = self.last_daily_reset_ms.compare_exchange(
                0, now_ms, Ordering::SeqCst, Ordering::SeqCst,
            );
        } else {
            let now_day  = now_ms / DAY_MS;
            let last_day = last_daily / DAY_MS;
            if now_day > last_day {
                if self.last_daily_reset_ms
                    .compare_exchange(last_daily, now_ms, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    self.daily_loss_cents.store(0, Ordering::SeqCst);
                    self.daily_trade_count.store(0, Ordering::SeqCst);
                    tracing::info!(now_ms, "daily risk counters reset");
                }
            }
        }

        // Weekly
        let last_weekly = self.last_weekly_reset_ms.load(Ordering::SeqCst);
        if last_weekly == 0 {
            let _ = self.last_weekly_reset_ms.compare_exchange(
                0, now_ms, Ordering::SeqCst, Ordering::SeqCst,
            );
        } else {
            let now_week  = now_ms / WEEK_MS;
            let last_week = last_weekly / WEEK_MS;
            if now_week > last_week {
                if self.last_weekly_reset_ms
                    .compare_exchange(last_weekly, now_ms, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    self.weekly_loss_cents.store(0, Ordering::SeqCst);
                    tracing::info!(now_ms, "weekly risk counters reset");
                }
            }
        }
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> RiskConfig {
        RiskConfig::default()
    }

    // ── F2: initial state ─────────────────────────────────────────────────

    #[test]
    fn fresh_limits_have_zero_counters() {
        let r = RiskLimits::new();
        assert_eq!(r.daily_loss_dollars(), 0.0);
        assert_eq!(r.weekly_loss_dollars(), 0.0);
        assert_eq!(r.open_exposure_dollars(), 0.0);
        assert_eq!(r.daily_trade_count(), 0);
        assert!(!r.is_kill_switch_active());
        // G7
        assert_eq!(r.realised_pnl_dollars(), 0.0);
        assert_eq!(r.current_bankroll(1500.0), 1500.0);
        // G11
        assert!(!r.is_data_stale());
    }

    // ── G7: dynamic bankroll ──────────────────────────────────────────────

    #[test]
    fn realised_pnl_accumulates_signed_across_record_close() {
        let r = RiskLimits::new();
        r.record_close(20.0,  5.0, &cfg());   // +$5 win
        r.record_close(20.0, -8.0, &cfg());   // -$8 loss
        r.record_close(20.0,  3.0, &cfg());   // +$3 win
        // Net: +5 - 8 + 3 = $0
        assert!((r.realised_pnl_dollars() - 0.0).abs() < 1e-6);
        // But losses-only counter still holds the $8
        assert!((r.daily_loss_dollars() - 8.0).abs() < 1e-6);
    }

    #[test]
    fn current_bankroll_compounds_with_pnl() {
        let r = RiskLimits::new();
        assert_eq!(r.current_bankroll(1500.0), 1500.0);
        r.record_close(20.0, -100.0, &cfg());
        r.reset_kill_switch();   // loss tripped it; reset for the test
        assert!((r.current_bankroll(1500.0) - 1400.0).abs() < 1e-6);
        r.record_close(20.0, 250.0, &cfg());
        assert!((r.current_bankroll(1500.0) - 1650.0).abs() < 1e-6);
    }

    #[test]
    fn current_bankroll_floors_at_zero() {
        let r = RiskLimits::new();
        // Catastrophic loss > starting bankroll
        r.record_close(20.0, -2000.0, &cfg());
        assert_eq!(r.current_bankroll(1500.0), 0.0);
    }

    #[test]
    fn realised_pnl_ignores_non_finite() {
        let r = RiskLimits::new();
        r.record_close(20.0, f64::NAN,         &cfg());
        r.record_close(20.0, f64::INFINITY,    &cfg());
        r.record_close(20.0, f64::NEG_INFINITY, &cfg());
        assert_eq!(r.realised_pnl_dollars(), 0.0);
    }

    // ── G11: data_stale flag ──────────────────────────────────────────────

    #[test]
    fn data_stale_blocks_can_trade() {
        let r = RiskLimits::new();
        r.set_data_stale(true);
        let err = r.can_trade_at(20.0, 1_700_000_000_000, &cfg()).unwrap_err();
        assert_eq!(err, RiskError::DataStale);
    }

    #[test]
    fn data_stale_clears_independently_of_kill_switch() {
        let r = RiskLimits::new();
        r.activate_kill_switch();
        r.set_data_stale(true);
        // Clear data_stale — kill switch must remain tripped
        r.set_data_stale(false);
        assert!(r.is_kill_switch_active());
        // can_trade still blocks (kill switch reason this time)
        let err = r.can_trade_at(20.0, 1_700_000_000_000, &cfg()).unwrap_err();
        assert_eq!(err, RiskError::KillSwitchActive);
    }

    #[test]
    fn fresh_limits_allows_normal_trade() {
        let r = RiskLimits::new();
        assert!(r.can_trade_at(20.0, 1_700_000_000_000, &cfg()).is_ok());
    }

    // ── F3: error variants ────────────────────────────────────────────────

    #[test]
    fn kill_switch_blocks_trade() {
        let r = RiskLimits::new();
        r.activate_kill_switch();
        let err = r.can_trade_at(20.0, 1_700_000_000_000, &cfg()).unwrap_err();
        assert_eq!(err, RiskError::KillSwitchActive);
    }

    #[test]
    fn daily_loss_blocks_trade() {
        let r = RiskLimits::new();
        // Inject $80 of losses (above $75 daily limit)
        r.record_close(20.0, -80.0, &cfg());
        // Kill switch will have auto-tripped — reset to test daily limit alone
        r.reset_kill_switch();
        let err = r.can_trade_at(20.0, 1_700_000_000_000, &cfg()).unwrap_err();
        match err {
            RiskError::DailyLossLimitHit { current, limit } => {
                assert!((current - 80.0).abs() < 1e-6);
                assert!((limit - 75.0).abs()   < 1e-6);
            }
            other => panic!("expected DailyLossLimitHit, got {other:?}"),
        }
    }

    #[test]
    fn weekly_loss_blocks_trade() {
        // Spread losses across 3 days so daily resets but weekly accumulates.
        // Anchor base to start-of-week so all 3 days stay in the same week.
        let r = RiskLimits::new();
        let week_start = (1_700_000_000_000_u64 / WEEK_MS) * WEEK_MS;
        let day1 = week_start + DAY_MS / 2;       // Monday noon UTC
        let day2 = day1 + DAY_MS + 1_000;         // Tuesday
        let day3 = day2 + DAY_MS + 1_000;         // Wednesday

        r.maybe_reset(day1);
        r.record_close(20.0, -50.0, &cfg());      // daily $50, weekly $50
        r.reset_kill_switch();

        r.maybe_reset(day2);                       // resets daily, weekly stays
        r.record_close(20.0, -50.0, &cfg());      // daily $50, weekly $100
        r.reset_kill_switch();

        r.maybe_reset(day3);
        r.record_close(20.0, -60.0, &cfg());      // daily $60, weekly $160 > $150
        r.reset_kill_switch();

        let err = r.can_trade_at(20.0, day3, &cfg()).unwrap_err();
        match err {
            RiskError::WeeklyLossLimitHit { current, limit } => {
                assert!((current - 160.0).abs() < 1e-6);
                assert!((limit   - 150.0).abs() < 1e-6);
            }
            other => panic!("expected WeeklyLossLimitHit, got {other:?}"),
        }
    }

    #[test]
    fn exposure_blocks_when_full() {
        let r = RiskLimits::new();
        r.record_open(280.0); // close to $300 cap
        let err = r.can_trade_at(25.0, 1_700_000_000_000, &cfg()).unwrap_err();
        match err {
            RiskError::ExposureLimitHit { current, requested, limit } => {
                assert!((current - 280.0).abs()    < 1e-6);
                assert!((requested - 25.0).abs()   < 1e-6);
                assert!((limit - 300.0).abs()      < 1e-6);
            }
            other => panic!("expected ExposureLimitHit, got {other:?}"),
        }
    }

    #[test]
    fn exposure_allows_at_boundary() {
        let r = RiskLimits::new();
        r.record_open(270.0);
        // 270 + 30 = 300 exactly = limit — should be allowed (>, not >=)
        assert!(r.can_trade_at(30.0, 1_700_000_000_000, &cfg()).is_ok());
    }

    #[test]
    fn trade_count_blocks_when_exceeded() {
        let r = RiskLimits::new();
        let c = cfg();
        for _ in 0..c.max_daily_trades {
            r.record_open(1.0);
        }
        let err = r.can_trade_at(20.0, 1_700_000_000_000, &c).unwrap_err();
        match err {
            RiskError::TradeCountExceeded { count, limit } => {
                assert_eq!(count, c.max_daily_trades);
                assert_eq!(limit, c.max_daily_trades);
            }
            other => panic!("expected TradeCountExceeded, got {other:?}"),
        }
    }

    #[test]
    fn bet_size_validation() {
        let r = RiskLimits::new();
        let c = cfg();
        let now = 1_700_000_000_000_u64;
        for bad in [0.0, -1.0, f64::NAN, f64::INFINITY, c.max_bet_dollars + 0.01] {
            let err = r.can_trade_at(bad, now, &c).unwrap_err();
            assert!(matches!(err, RiskError::BetSizeInvalid { .. }),
                    "bet {bad} should be invalid; got {err:?}");
        }
    }

    // ── F4: record_open / record_close ────────────────────────────────────

    #[test]
    fn record_open_grows_exposure() {
        let r = RiskLimits::new();
        r.record_open(15.0);
        assert!((r.open_exposure_dollars() - 15.0).abs() < 1e-6);
        r.record_open(10.0);
        assert!((r.open_exposure_dollars() - 25.0).abs() < 1e-6);
    }

    #[test]
    fn record_open_increments_trade_count() {
        let r = RiskLimits::new();
        r.record_open(10.0);
        r.record_open(10.0);
        r.record_open(10.0);
        assert_eq!(r.daily_trade_count(), 3);
    }

    #[test]
    fn record_close_releases_exposure() {
        let r = RiskLimits::new();
        r.record_open(20.0);
        r.record_close(20.0, 5.0, &cfg());
        assert!((r.open_exposure_dollars() - 0.0).abs() < 1e-6);
    }

    #[test]
    fn record_close_with_gain_does_not_increase_loss() {
        let r = RiskLimits::new();
        r.record_open(20.0);
        r.record_close(20.0, 15.0, &cfg());
        assert_eq!(r.daily_loss_dollars(), 0.0);
        assert_eq!(r.weekly_loss_dollars(), 0.0);
    }

    #[test]
    fn record_close_with_loss_grows_daily_and_weekly() {
        let r = RiskLimits::new();
        r.record_close(20.0, -10.0, &cfg());
        assert!((r.daily_loss_dollars()  - 10.0).abs() < 1e-6);
        assert!((r.weekly_loss_dollars() - 10.0).abs() < 1e-6);
    }

    #[test]
    fn record_close_does_not_make_exposure_negative() {
        let r = RiskLimits::new();
        // Mismatched: close without an open first
        r.record_close(20.0, 5.0, &cfg());
        assert_eq!(r.open_exposure_dollars(), 0.0);
    }

    // ── Auto-tripped kill switch ─────────────────────────────────────────

    #[test]
    fn kill_switch_auto_trips_on_daily_loss() {
        let r = RiskLimits::new();
        assert!(!r.is_kill_switch_active());
        // $80 loss > $75 limit → trip
        r.record_close(20.0, -80.0, &cfg());
        assert!(r.is_kill_switch_active());
    }

    #[test]
    fn kill_switch_auto_trips_on_weekly_loss() {
        let r = RiskLimits::new();
        // Spread losses over multiple closes to avoid daily trip first
        for _ in 0..10 {
            r.record_close(20.0, -10.0, &cfg());
        }
        // Total $100 weekly < $150 limit, daily $100 > $75 → daily trip
        // Reset and add more to push over weekly
        r.reset_kill_switch();
        // Daily loss is at $100. Push to $200.
        for _ in 0..10 {
            r.record_close(20.0, -10.0, &cfg());
        }
        // Daily $200, weekly $200 — both tripped
        assert!(r.is_kill_switch_active());
    }

    #[test]
    fn kill_switch_sticky_after_trip() {
        let r = RiskLimits::new();
        r.activate_kill_switch();
        // Exposure decreases, doesn't reset kill switch
        r.record_close(20.0, 0.0, &cfg());
        assert!(r.is_kill_switch_active());
        // Manual reset clears
        r.reset_kill_switch();
        assert!(!r.is_kill_switch_active());
    }

    // ── Daily / weekly resets ─────────────────────────────────────────────

    #[test]
    fn daily_counters_reset_on_day_boundary() {
        let r = RiskLimits::new();
        let day1 = 1_700_000_000_000_u64;       // some day
        let day2 = day1 + DAY_MS + 1_000;       // next day

        r.maybe_reset(day1);                    // initialize day-of-record
        r.record_open(20.0);
        r.record_close(20.0, -10.0, &cfg());
        assert_eq!(r.daily_trade_count(),  1);
        assert!((r.daily_loss_dollars() - 10.0).abs() < 1e-6);

        r.maybe_reset(day2);
        assert_eq!(r.daily_trade_count(),  0);
        assert_eq!(r.daily_loss_dollars(), 0.0);
    }

    #[test]
    fn weekly_counters_reset_on_week_boundary() {
        let r = RiskLimits::new();
        let week1 = 1_700_000_000_000_u64;
        let week2 = week1 + WEEK_MS + 1_000;

        r.maybe_reset(week1);
        r.record_close(20.0, -50.0, &cfg());
        assert!((r.weekly_loss_dollars() - 50.0).abs() < 1e-6);

        r.maybe_reset(week2);
        assert_eq!(r.weekly_loss_dollars(), 0.0);
    }

    // ── Realistic flow: 10 trades hit daily limit ─────────────────────────

    #[test]
    fn realistic_10_losses_trip_daily_limit() {
        let r   = RiskLimits::new();
        let c   = cfg();
        let now = 1_700_000_000_000_u64;

        // 9 trades of ~$8 loss each = $72 (still under limit)
        for _ in 0..9 {
            assert!(r.can_trade_at(20.0, now, &c).is_ok());
            r.record_open(20.0);
            r.record_close(20.0, -8.0, &c);
        }
        assert!(!r.is_kill_switch_active());

        // 10th trade pushes total to $80 — auto-trip
        r.record_open(20.0);
        r.record_close(20.0, -8.0, &c);
        assert!(r.is_kill_switch_active());

        // 11th trade attempt blocked
        let err = r.can_trade_at(20.0, now, &c).unwrap_err();
        assert_eq!(err, RiskError::KillSwitchActive);
    }

    // ── Concurrent atomicity smoke test ───────────────────────────────────

    #[test]
    fn concurrent_record_open_count_is_correct() {
        use std::sync::Arc;
        use std::thread;

        let r = Arc::new(RiskLimits::new());
        let mut handles = vec![];
        for _ in 0..10 {
            let r = r.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    r.record_open(1.0);
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(r.daily_trade_count(),     1000);
        assert!((r.open_exposure_dollars() - 1000.0).abs() < 1e-6);
    }

    // ── Read-only views ───────────────────────────────────────────────────

    #[test]
    fn read_only_views_match_atomic_state() {
        let r = RiskLimits::new();
        r.record_open(50.0);
        r.record_close(50.0, -25.0, &cfg());
        assert_eq!(r.daily_loss_dollars(),    25.0);
        assert_eq!(r.weekly_loss_dollars(),   25.0);
        assert_eq!(r.open_exposure_dollars(), 0.0);
        assert_eq!(r.daily_trade_count(),     1);
    }
}
