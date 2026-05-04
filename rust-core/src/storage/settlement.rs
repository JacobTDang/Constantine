// Settlement monitor task — for each market in the discovery list, when its
// close_time passes, append a settlement row capturing strike + chainlink
// price at close.
//
// Reconciliation (joining signals → settlements to compute realised P&L)
// happens at analysis time in scripts/observe_report.py. The live process
// only appends, never updates.

use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::RwLock;
use tokio::time::{Duration, MissedTickBehavior};

use crate::features::FeatureState;
use crate::streams::polymarket::PolyMarket;
use super::{SettlementRow, SignalLog};

const POLL_INTERVAL_SECS: u64 = 2;

pub async fn settlement_monitor_loop(
    markets:  Arc<RwLock<Vec<PolyMarket>>>,
    state:    Arc<RwLock<FeatureState>>,
    log_db:   Arc<SignalLog>,
) {
    let mut tick = tokio::time::interval(Duration::from_secs(POLL_INTERVAL_SECS));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut already_settled: HashSet<String> = HashSet::new();

    loop {
        tick.tick().await;
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;

        let markets_snapshot = markets.read().await.clone();
        let s = state.read().await;
        let chainlink   = s.chainlink_price;
        let strikes_now = s.window_strikes.clone();
        drop(s);

        for m in &markets_snapshot {
            if already_settled.contains(&m.id) || now_ms < m.close_time_ms {
                continue;
            }
            if chainlink <= 0.0 {
                continue;
            }

            let strike = strikes_now.get(&m.id).map(|(_, p)| *p).unwrap_or(0.0);
            if strike <= 0.0 {
                tracing::warn!(market = %m.id, "no strike captured, skipping settlement");
                already_settled.insert(m.id.clone());
                continue;
            }

            let outcome_up = chainlink > strike;
            let row = SettlementRow {
                market_id:            m.id.clone(),
                close_time_ms:        m.close_time_ms,
                strike_price:         strike,
                settlement_chainlink: chainlink,
                outcome_up,
                settled_at_ms:        now_ms,
            };

            match log_db.insert_settlement(&row) {
                Ok(()) => tracing::info!(
                    market = %m.id, strike, chainlink, outcome_up, "market settled",
                ),
                Err(e) => tracing::error!(error = %e, market = %m.id, "settlement write failed"),
            }
            already_settled.insert(m.id.clone());
        }

        // Bound the cache — drop entries for markets no longer in discovery list
        already_settled.retain(|id| markets_snapshot.iter().any(|m| m.id == *id));
    }
}
