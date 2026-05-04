// Sprint 7 / S7.1+S7.3 — Data-loss + time-sync watchdog.
//
// Background task that wakes every 5s and inspects FeatureState's
// last-update timestamps. If any critical stream is stale beyond its
// threshold, trip the kill switch via RiskLimits.activate_kill_switch().
//
// Why a separate task: detection is event-agnostic (we don't care about
// WHICH event came in, only that ANY did). Keeping it out of the hot
// signal path means a stuck stream doesn't block trading on healthy ones.
//
// Thresholds default to ~6x the expected event interval — enough to
// tolerate a transient gap but tight enough to catch real disconnects:
//   spot trades:    Binance emits ~10/sec → 30s = 300x interval
//   chainlink:      polled every 12s     → 90s
//   clob book:      streamed continuously → 60s
//   kline (closed): one per minute       → 180s

use std::sync::Arc;

use tokio::sync::RwLock;
use tokio::time::{Duration, MissedTickBehavior};

use crate::features::FeatureState;
use crate::risk::limits::RiskLimits;

const POLL_INTERVAL_SECS: u64 = 5;

#[derive(Debug, Clone, Copy)]
pub struct WatchdogConfig {
    pub spot_stale_ms:      u64,
    pub mark_stale_ms:      u64,
    pub chainlink_stale_ms: u64,
    pub book_stale_ms:      u64,
    pub kline_stale_ms:     u64,
    /// If true, watchdog activates the kill switch on any stale stream.
    /// In observe mode (no execution), set false so the bot keeps logging.
    pub trip_kill_switch:   bool,
}

impl Default for WatchdogConfig {
    fn default() -> Self {
        Self {
            spot_stale_ms:       30_000,
            mark_stale_ms:       30_000,
            chainlink_stale_ms:  90_000,
            book_stale_ms:       60_000,
            kline_stale_ms:     180_000,
            trip_kill_switch:    true,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct StalenessReport {
    pub now_ms:                u64,
    pub spot_age_ms:           Option<u64>,
    pub mark_age_ms:           Option<u64>,
    pub chainlink_age_ms:      Option<u64>,
    pub book_age_ms:           Option<u64>,
    pub kline_age_ms:          Option<u64>,
    pub stale_streams:         Vec<&'static str>,
}

impl StalenessReport {
    pub fn any_stale(&self) -> bool { !self.stale_streams.is_empty() }
}

/// Pure check: given a FeatureState snapshot + config, list which streams
/// are stale. Returns None for "never seen" streams (age < threshold IS
/// treated as healthy — startup grace).
pub fn check_staleness(
    state:  &FeatureState,
    cfg:    &WatchdogConfig,
    now_ms: u64,
) -> StalenessReport {
    let age = |last: u64| -> Option<u64> {
        if last == 0 { None } else { Some(now_ms.saturating_sub(last)) }
    };
    let mut stale_streams = Vec::new();
    let push_if_stale = |
        stream:    &'static str,
        last:      u64,
        threshold: u64,
        out:       &mut Vec<&'static str>,
    | {
        // last == 0 means "never seen" — give startup grace.
        if last > 0 && now_ms.saturating_sub(last) >= threshold {
            out.push(stream);
        }
    };

    push_if_stale("spot",      state.last_spot_update_ms,      cfg.spot_stale_ms,      &mut stale_streams);
    push_if_stale("mark",      state.last_mark_update_ms,      cfg.mark_stale_ms,      &mut stale_streams);
    push_if_stale("chainlink", state.last_chainlink_update_ms, cfg.chainlink_stale_ms, &mut stale_streams);
    push_if_stale("book",      state.last_book_update_ms,      cfg.book_stale_ms,      &mut stale_streams);
    push_if_stale("kline",     state.last_kline_update_ms,     cfg.kline_stale_ms,     &mut stale_streams);

    StalenessReport {
        now_ms,
        spot_age_ms:      age(state.last_spot_update_ms),
        mark_age_ms:      age(state.last_mark_update_ms),
        chainlink_age_ms: age(state.last_chainlink_update_ms),
        book_age_ms:      age(state.last_book_update_ms),
        kline_age_ms:     age(state.last_kline_update_ms),
        stale_streams,
    }
}

/// Long-running task: poll FeatureState, trip kill switch on staleness.
pub async fn watchdog_loop(
    state: Arc<RwLock<FeatureState>>,
    risk:  Arc<RiskLimits>,
    cfg:   WatchdogConfig,
) {
    let mut tick = tokio::time::interval(Duration::from_secs(POLL_INTERVAL_SECS));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    tracing::info!(
        spot_stale_ms      = cfg.spot_stale_ms,
        chainlink_stale_ms = cfg.chainlink_stale_ms,
        book_stale_ms      = cfg.book_stale_ms,
        trip_kill_switch   = cfg.trip_kill_switch,
        "watchdog started"
    );

    loop {
        tick.tick().await;
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;
        let report = {
            let s = state.read().await;
            check_staleness(&s, &cfg, now_ms)
        };

        if report.any_stale() {
            tracing::error!(
                stale = ?report.stale_streams,
                spot_age_ms      = report.spot_age_ms,
                chainlink_age_ms = report.chainlink_age_ms,
                book_age_ms      = report.book_age_ms,
                "DATA STALE — tripping kill switch"
            );
            if cfg.trip_kill_switch && !risk.is_kill_switch_active() {
                risk.activate_kill_switch();
            }
        } else {
            tracing::trace!(
                spot_age_ms = report.spot_age_ms,
                book_age_ms = report.book_age_ms,
                "watchdog OK"
            );
        }
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_state(now_ms: u64) -> FeatureState {
        let mut s = FeatureState::default();
        s.last_spot_update_ms      = now_ms - 1_000;
        s.last_mark_update_ms      = now_ms - 1_000;
        s.last_chainlink_update_ms = now_ms - 5_000;
        s.last_book_update_ms      = now_ms - 1_000;
        s.last_kline_update_ms     = now_ms - 30_000;
        s
    }

    #[test]
    fn fresh_state_reports_no_stale() {
        let now_ms = 1_700_000_000_000u64;
        let s = fresh_state(now_ms);
        let r = check_staleness(&s, &WatchdogConfig::default(), now_ms);
        assert!(!r.any_stale());
        assert_eq!(r.spot_age_ms, Some(1_000));
        assert_eq!(r.chainlink_age_ms, Some(5_000));
    }

    #[test]
    fn never_seen_streams_are_not_flagged_stale() {
        // Startup grace: last_*_ms == 0 means "never seen", treated as healthy
        let now_ms = 1_700_000_000_000u64;
        let s = FeatureState::default();   // all zeros
        let r = check_staleness(&s, &WatchdogConfig::default(), now_ms);
        assert!(!r.any_stale());
        assert_eq!(r.spot_age_ms, None);
        assert_eq!(r.chainlink_age_ms, None);
    }

    #[test]
    fn stale_spot_is_flagged() {
        let now_ms = 1_700_000_000_000u64;
        let mut s = fresh_state(now_ms);
        s.last_spot_update_ms = now_ms - 31_000;   // > 30s threshold
        let r = check_staleness(&s, &WatchdogConfig::default(), now_ms);
        assert!(r.any_stale());
        assert!(r.stale_streams.contains(&"spot"));
    }

    #[test]
    fn stale_chainlink_is_flagged() {
        let now_ms = 1_700_000_000_000u64;
        let mut s = fresh_state(now_ms);
        s.last_chainlink_update_ms = now_ms - 95_000;
        let r = check_staleness(&s, &WatchdogConfig::default(), now_ms);
        assert!(r.stale_streams.contains(&"chainlink"));
    }

    #[test]
    fn stale_book_is_flagged() {
        let now_ms = 1_700_000_000_000u64;
        let mut s = fresh_state(now_ms);
        s.last_book_update_ms = now_ms - 65_000;
        let r = check_staleness(&s, &WatchdogConfig::default(), now_ms);
        assert!(r.stale_streams.contains(&"book"));
    }

    #[test]
    fn multiple_stale_streams_all_listed() {
        let now_ms = 1_700_000_000_000u64;
        let mut s = fresh_state(now_ms);
        s.last_spot_update_ms      = now_ms - 60_000;
        s.last_chainlink_update_ms = now_ms - 120_000;
        let r = check_staleness(&s, &WatchdogConfig::default(), now_ms);
        assert!(r.stale_streams.contains(&"spot"));
        assert!(r.stale_streams.contains(&"chainlink"));
        assert_eq!(r.stale_streams.len(), 2);
    }

    #[test]
    fn custom_thresholds_apply() {
        // Tighter threshold catches what default would miss
        let now_ms = 1_700_000_000_000u64;
        let mut s = fresh_state(now_ms);
        s.last_spot_update_ms = now_ms - 5_000;
        let mut cfg = WatchdogConfig::default();
        cfg.spot_stale_ms = 3_000;
        let r = check_staleness(&s, &cfg, now_ms);
        assert!(r.stale_streams.contains(&"spot"));
    }

    // ── Integration: full loop trip ───────────────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn watchdog_trips_kill_switch_on_stale() {
        let mut s = FeatureState::default();
        // Set spot to stale
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;
        s.last_spot_update_ms = now_ms - 60_000;   // 60s old, > 30s threshold
        let state = Arc::new(RwLock::new(s));
        let risk = Arc::new(RiskLimits::new());
        let cfg = WatchdogConfig {
            spot_stale_ms:      30_000,
            mark_stale_ms:      30_000,
            chainlink_stale_ms: 90_000,
            book_stale_ms:      60_000,
            kline_stale_ms:    180_000,
            trip_kill_switch:    true,
        };

        // Run the watchdog loop briefly
        let r2 = risk.clone();
        let h = tokio::spawn(watchdog_loop(state, r2, cfg));
        // Wait long enough for one tick (>5s default).
        // Use a smaller poll interval pattern by manually running check_staleness:
        // ↑ The actual loop has 5s intervals; for the test we just call check()
        //   indirectly by sleeping a tick + epsilon. Tighten below if flaky.
        tokio::time::sleep(Duration::from_secs(6)).await;
        h.abort();

        assert!(risk.is_kill_switch_active(), "kill switch should be activated");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn watchdog_does_not_trip_on_fresh_data() {
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;
        let state = Arc::new(RwLock::new(fresh_state(now_ms)));
        let risk = Arc::new(RiskLimits::new());
        let cfg = WatchdogConfig::default();

        let r2 = risk.clone();
        let s2 = state.clone();
        let h = tokio::spawn(watchdog_loop(s2, r2, cfg));
        tokio::time::sleep(Duration::from_secs(6)).await;
        h.abort();

        assert!(!risk.is_kill_switch_active());
    }
}
