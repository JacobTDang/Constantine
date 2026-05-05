// Sprint 7 / S7.4 — Heartbeat task.
//
// Periodic structured log emit so ops can grep for "HB" lines and trace
// the bot's state over time. One concise log line every N seconds.
//
// Designed to be cheap and rare — ~1 line every 30 seconds — so it never
// competes with hot-path tasks for CPU. No tokio::spawn fan-out here;
// just a single async loop.

use std::sync::Arc;

use tokio::sync::RwLock;
use tokio::time::{Duration, MissedTickBehavior};

use crate::features::FeatureState;
use crate::risk::limits::RiskLimits;
use crate::storage::PositionStore;

const DEFAULT_HEARTBEAT_SECS: u64 = 30;

#[derive(Debug, Clone)]
pub struct HeartbeatSnapshot {
    pub now_ms:               u64,
    pub seq:                  u64,
    pub spot_price:           f64,
    pub primary_market:       Option<String>,
    pub time_to_close_secs:   f64,
    pub kill_switch_active:   bool,
    pub data_stale:           bool,                // G11
    pub realised_pnl_dollars: f64,                 // G7
    pub current_bankroll_usd: f64,                 // G7
    pub daily_loss_dollars:   f64,
    pub weekly_loss_dollars:  f64,
    pub open_exposure_usd:    f64,
    pub daily_trade_count:    u32,
    pub positions_total:      usize,
    pub positions_pending:    usize,
}

/// Compose one heartbeat snapshot. Public so tests can drive it without
/// running the loop. Caller passes `starting_bankroll` so the snapshot's
/// `current_bankroll_usd` reflects the compounded value.
pub fn snapshot(
    state:             &FeatureState,
    risk:              &RiskLimits,
    positions:         &PositionStore,
    starting_bankroll: f64,
    now_ms:            u64,
) -> HeartbeatSnapshot {
    HeartbeatSnapshot {
        now_ms,
        seq:                  state.seq,
        spot_price:           state.spot_price,
        primary_market:       state.primary_market_id.clone(),
        time_to_close_secs:   state.time_to_close,
        kill_switch_active:   risk.is_kill_switch_active(),
        data_stale:           risk.is_data_stale(),
        realised_pnl_dollars: risk.realised_pnl_dollars(),
        current_bankroll_usd: risk.current_bankroll(starting_bankroll),
        daily_loss_dollars:   risk.daily_loss_dollars(),
        weekly_loss_dollars:  risk.weekly_loss_dollars(),
        open_exposure_usd:    risk.open_exposure_dollars(),
        daily_trade_count:    risk.daily_trade_count(),
        positions_total:      positions.len(),
        positions_pending:    positions.pending_settlement().len(),
    }
}

/// Long-running task: emit a "HB" log line every N seconds.
pub async fn heartbeat_loop(
    state:             Arc<RwLock<FeatureState>>,
    risk:              Arc<RiskLimits>,
    positions:         Arc<PositionStore>,
    starting_bankroll: f64,
    interval_secs:     u64,
) {
    let secs = if interval_secs == 0 { DEFAULT_HEARTBEAT_SECS } else { interval_secs };
    let mut tick = tokio::time::interval(Duration::from_secs(secs));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    tracing::info!(interval_secs = secs, "heartbeat started");

    loop {
        tick.tick().await;
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;
        let snap = {
            let s = state.read().await;
            snapshot(&s, &risk, &positions, starting_bankroll, now_ms)
        };

        tracing::info!(
            target: "heartbeat",
            seq           = snap.seq,
            spot          = snap.spot_price,
            primary       = ?snap.primary_market,
            t_close_s     = snap.time_to_close_secs,
            kill          = snap.kill_switch_active,
            data_stale    = snap.data_stale,
            realised_pnl  = snap.realised_pnl_dollars,
            bankroll      = snap.current_bankroll_usd,
            daily_loss    = snap.daily_loss_dollars,
            weekly_loss   = snap.weekly_loss_dollars,
            exposure_usd  = snap.open_exposure_usd,
            trades_today  = snap.daily_trade_count,
            pos_total     = snap.positions_total,
            pos_pending   = snap.positions_pending,
            "HB"
        );
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "constantine-hb-{}", uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn snapshot_reflects_state() {
        let mut s = FeatureState::default();
        s.seq               = 42;
        s.spot_price        = 80_500.0;
        s.primary_market_id = Some("btc-1".to_string());
        s.time_to_close     = 120.0;

        let risk = RiskLimits::new();
        risk.record_open(15.0);
        risk.record_close(15.0, -5.0, &Default::default());

        let store = PositionStore::open(&temp_dir()).unwrap();
        store.record_open("a", "btc-1", "yes", 5.0, 0.45).unwrap();

        let snap = snapshot(&s, &risk, &store, 1500.0, 1_000_000);
        assert_eq!(snap.seq, 42);
        assert!((snap.spot_price - 80_500.0).abs() < 1e-9);
        assert_eq!(snap.primary_market.as_deref(), Some("btc-1"));
        assert!((snap.time_to_close_secs - 120.0).abs() < 1e-9);
        assert_eq!(snap.daily_trade_count, 1);
        assert!((snap.daily_loss_dollars - 5.0).abs() < 1e-9);
        assert_eq!(snap.positions_total, 1);
        assert_eq!(snap.positions_pending, 1);
    }

    #[test]
    fn snapshot_includes_kill_switch_state() {
        let s = FeatureState::default();
        let risk = RiskLimits::new();
        let store = PositionStore::open(&temp_dir()).unwrap();
        let snap = snapshot(&s, &risk, &store, 1500.0, 0);
        assert!(!snap.kill_switch_active);
        risk.activate_kill_switch();
        let snap2 = snapshot(&s, &risk, &store, 1500.0, 0);
        assert!(snap2.kill_switch_active);
    }

    #[test]
    fn snapshot_handles_settled_vs_pending() {
        let s = FeatureState::default();
        let risk = RiskLimits::new();
        let store = PositionStore::open(&temp_dir()).unwrap();
        store.record_open("a", "m", "yes", 5.0, 0.45).unwrap();
        store.record_open("b", "m", "yes", 5.0, 0.50).unwrap();
        store.record_settle("a", true, 1.0).unwrap();   // settled
        let snap = snapshot(&s, &risk, &store, 1500.0, 0);
        assert_eq!(snap.positions_total, 2);
        assert_eq!(snap.positions_pending, 1);   // only b is pending
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn heartbeat_loop_runs_without_panicking() {
        // Just verify the loop spins and we can abort it cleanly.
        let state = Arc::new(RwLock::new(FeatureState::default()));
        let risk  = Arc::new(RiskLimits::new());
        let store = Arc::new(PositionStore::open(&temp_dir()).unwrap());
        let h = tokio::spawn(heartbeat_loop(state, risk, store, 1500.0, 1));
        tokio::time::sleep(Duration::from_secs(2)).await;
        h.abort();
        // No assertion — if we got here without a panic, we're good.
    }
}
