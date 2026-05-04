// Settlement monitor task — for each market in the discovery list, when its
// close_time passes, append a settlement row capturing strike + chainlink
// price at close.
//
// Reconciliation (joining signals → settlements to compute realised P&L)
// happens at analysis time in scripts/observe_report.py. The live process
// only appends, never updates.

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Result;
use rayon::prelude::*;
use tokio::sync::RwLock;
use tokio::time::{Duration, MissedTickBehavior};

use crate::features::FeatureState;
use crate::streams::polymarket::PolyMarket;
use super::{Position, PositionStatus, PositionStore, SettlementRow, SignalLog};

const POLL_INTERVAL_SECS: u64 = 2;

// ── Realised-P&L cost model ───────────────────────────────────────────────────
// Mirrors scripts/observe_report.py and scripts/backtest.py — keep these in sync.
//
// effective_buy_price = price * (1 + entry_slippage) * (1 + fee_rate)
// shares              = bet / effective_buy_price
// on_win_payout       = shares * (1 - exit_slippage) * (1 - fee_rate)
// pnl_win             = on_win_payout - bet
// pnl_loss            = -bet  (entry fees + bet itself are gone)

pub const FEE_RATE:        f64 = 0.018;
pub const ENTRY_SLIPPAGE:  f64 = 0.010;
pub const EXIT_SLIPPAGE:   f64 = 0.005;

/// Compute realised P&L (in dollars) for a paper-traded BUY position.
///
/// `bet`   = USDC committed (maker_amount in dollars)
/// `price` = the (0,1) probability at which we entered
/// `won`   = whether our chosen side resolved to the winning outcome
pub fn compute_paper_pnl(bet: f64, price: f64, won: bool) -> f64 {
    if !won { return -bet; }
    if !price.is_finite() || price <= 0.0 || price >= 1.0 {
        return -bet;
    }
    let effective_buy = price * (1.0 + ENTRY_SLIPPAGE) * (1.0 + FEE_RATE);
    let shares        = bet / effective_buy;
    let payout        = shares * (1.0 - EXIT_SLIPPAGE) * (1.0 - FEE_RATE);
    payout - bet
}

/// Settle every pending position for `market_id` against the market outcome.
/// Returns the count of positions settled.
///
/// **Parallel:** the per-position P&L computation runs across rayon's pool.
/// Cheap per-position (<1µs of float math), but the structure scales for
/// future cost models that include book-walk simulations or fill replay.
pub fn settle_positions_for_market(
    positions:  &PositionStore,
    market_id:  &str,
    outcome_up: bool,
) -> Result<usize> {
    let candidates: Vec<Position> = positions.all().into_iter()
        .filter(|p| p.market_id == market_id
            && matches!(p.status, PositionStatus::Submitted | PositionStatus::Filled))
        .collect();

    // Phase 1 — compute P&L per position in parallel
    let resolved: Vec<(String, f64)> = candidates
        .par_iter()
        .map(|p| {
            let won = (p.side == "yes") == outcome_up;
            let pnl = compute_paper_pnl(p.bet_dollars, p.price, won);
            (p.order_id.clone(), pnl)
        })
        .collect();

    // Phase 2 — single-threaded JSONL append (DashMap mutations are concurrent)
    let n = resolved.len();
    for (order_id, pnl) in resolved {
        positions.record_settle(&order_id, outcome_up, pnl)?;
    }
    Ok(n)
}

pub async fn settlement_monitor_loop(
    markets:   Arc<RwLock<Vec<PolyMarket>>>,
    state:     Arc<RwLock<FeatureState>>,
    log_db:    Arc<SignalLog>,
    positions: Option<Arc<PositionStore>>,
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

            // Reconcile every open position for this market — parallel over
            // positions, then sequential ledger append.
            if let Some(positions) = &positions {
                match settle_positions_for_market(positions, &m.id, outcome_up) {
                    Ok(n) if n > 0 => tracing::info!(
                        market = %m.id, settled = n, "positions reconciled",
                    ),
                    Ok(_) => {}
                    Err(e) => tracing::error!(error = %e, market = %m.id, "position reconcile failed"),
                }
            }

            already_settled.insert(m.id.clone());
        }

        // Bound the cache — drop entries for markets no longer in discovery list
        already_settled.retain(|id| markets_snapshot.iter().any(|m| m.id == *id));
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "constantine-settle-{}", uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    // ── compute_paper_pnl ────────────────────────────────────────────────

    #[test]
    fn loss_returns_negative_bet() {
        assert_eq!(compute_paper_pnl(5.0, 0.45, false), -5.0);
        assert_eq!(compute_paper_pnl(30.0, 0.65, false), -30.0);
    }

    #[test]
    fn win_at_50c_after_costs_is_smaller_than_naive() {
        // Naive: $5 at 50¢ → 10 shares → $10 payout → +$5 P&L.
        // After fees + slippage: less than $5.
        let pnl = compute_paper_pnl(5.0, 0.50, true);
        assert!(pnl > 0.0, "win must have positive pnl, got {pnl}");
        assert!(pnl < 5.0, "fees/slippage must reduce naive payout, got {pnl}");
    }

    #[test]
    fn win_at_low_price_has_higher_payout() {
        // $5 at 25¢ should net more than $5 at 75¢
        let p25 = compute_paper_pnl(5.0, 0.25, true);
        let p75 = compute_paper_pnl(5.0, 0.75, true);
        assert!(p25 > p75, "p25={p25} should beat p75={p75}");
    }

    #[test]
    fn pnl_handles_invalid_inputs_safely() {
        // price out of (0,1) → treat as a loss
        assert_eq!(compute_paper_pnl(5.0, 0.0,  true), -5.0);
        assert_eq!(compute_paper_pnl(5.0, 1.0,  true), -5.0);
        assert_eq!(compute_paper_pnl(5.0, -0.5, true), -5.0);
        assert_eq!(compute_paper_pnl(5.0, f64::NAN, true), -5.0);
    }

    // ── settle_positions_for_market ──────────────────────────────────────

    #[test]
    fn settle_positions_for_market_reconciles_yes_winners() {
        let dir = temp_dir();
        let store = PositionStore::open(&dir).unwrap();
        // 3 positions on the SAME market; 2 yes (which wins), 1 no (loses)
        store.record_open("a", "btc-1", "yes", 5.0, 0.45).unwrap();
        store.record_open("b", "btc-1", "yes", 5.0, 0.50).unwrap();
        store.record_open("c", "btc-1", "no",  5.0, 0.55).unwrap();
        // One unrelated market — must NOT be touched
        store.record_open("d", "btc-2", "yes", 5.0, 0.40).unwrap();

        let n = settle_positions_for_market(&store, "btc-1", true).unwrap();
        assert_eq!(n, 3);

        let a = store.get("a").unwrap();
        let b = store.get("b").unwrap();
        let c = store.get("c").unwrap();
        let d = store.get("d").unwrap();
        assert_eq!(a.status, PositionStatus::Settled);
        assert_eq!(b.status, PositionStatus::Settled);
        assert_eq!(c.status, PositionStatus::Settled);
        assert_eq!(d.status, PositionStatus::Submitted);  // untouched
        // a, b: yes side won → positive pnl
        assert!(a.pnl_dollars.unwrap() > 0.0);
        assert!(b.pnl_dollars.unwrap() > 0.0);
        // c: no side lost → -bet
        assert!((c.pnl_dollars.unwrap() - (-5.0)).abs() < 1e-9);
    }

    #[test]
    fn settle_positions_for_market_skips_already_settled() {
        let dir = temp_dir();
        let store = PositionStore::open(&dir).unwrap();
        store.record_open("a", "btc-1", "yes", 5.0, 0.45).unwrap();
        store.record_settle("a", true, 1.0).unwrap();   // already settled
        store.record_open("b", "btc-1", "yes", 5.0, 0.45).unwrap();

        // Only b should be reconciled
        let n = settle_positions_for_market(&store, "btc-1", true).unwrap();
        assert_eq!(n, 1);
        // a's pnl untouched (still 1.0, not the recomputed value)
        assert!((store.get("a").unwrap().pnl_dollars.unwrap() - 1.0).abs() < 1e-9);
    }

    #[test]
    fn settle_positions_for_market_skips_failed() {
        let dir = temp_dir();
        let store = PositionStore::open(&dir).unwrap();
        store.record_open("a", "btc-1", "yes", 5.0, 0.45).unwrap();
        store.record_fail("a", "transport").unwrap();   // failed before fill
        let n = settle_positions_for_market(&store, "btc-1", true).unwrap();
        assert_eq!(n, 0);
        // Still Failed
        assert_eq!(store.get("a").unwrap().status, PositionStatus::Failed);
    }

    #[test]
    fn no_pending_positions_returns_zero() {
        let dir = temp_dir();
        let store = PositionStore::open(&dir).unwrap();
        assert_eq!(settle_positions_for_market(&store, "anything", true).unwrap(), 0);
    }

    #[test]
    fn down_outcome_inverts_winners() {
        let dir = temp_dir();
        let store = PositionStore::open(&dir).unwrap();
        store.record_open("yes-bet", "m", "yes", 5.0, 0.45).unwrap();
        store.record_open("no-bet",  "m", "no",  5.0, 0.55).unwrap();
        // Outcome: down → no wins, yes loses
        settle_positions_for_market(&store, "m", false).unwrap();
        assert!(store.get("no-bet").unwrap().pnl_dollars.unwrap() > 0.0);
        assert!((store.get("yes-bet").unwrap().pnl_dollars.unwrap() - (-5.0)).abs() < 1e-9);
    }

    #[test]
    fn settle_50_positions_completes_quickly() {
        // Sanity: parallelism shouldn't introduce regressions on small loads.
        let dir = temp_dir();
        let store = PositionStore::open(&dir).unwrap();
        for i in 0..50 {
            store.record_open(format!("ord-{i}"), "m", "yes", 5.0, 0.45).unwrap();
        }
        let start = std::time::Instant::now();
        let n = settle_positions_for_market(&store, "m", true).unwrap();
        let dt = start.elapsed();
        assert_eq!(n, 50);
        assert!(dt.as_millis() < 200, "settle 50 should be fast, got {}ms", dt.as_millis());
    }
}
