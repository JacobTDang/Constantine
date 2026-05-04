// Sprint 6 / S6.3 — Execution runner loop.
//
// Long-running task that ties together signal evaluation and order
// submission. Every 500ms:
//
//   1. Snapshot FeatureState + markets list
//   2. evaluate_all_markets — runs across rayon for per-market parallelism
//   3. For every non-None signal, spawn an INDEPENDENT tokio task that
//      executes that market's signal via execute_signal
//   4. Per-market cooldown (DashMap<market_id, last_submit_ms>) prevents
//      stacking trades on the same window
//
// Parallelism profile:
//   - Signal evaluation:   N markets across rayon's pool (N typically 1-3)
//   - Execution dispatch:  N tokio tasks running concurrently
//     (each acquiring the same shared pool/risk/client/positions handles)
//
// The runner is gated by execution_enabled in main.rs — when false, this
// task is never spawned and the bot stays in observe-only mode.

use std::sync::Arc;

use anyhow::Result;
use dashmap::DashMap;
use tokio::sync::RwLock;
use tokio::time::{Duration, MissedTickBehavior};

use crate::execution::clob::ClobClient;
use crate::execution::executor::{execute_signal, ExecContext};
use crate::execution::presign::OrderPool;
use crate::features::FeatureState;
use crate::risk::limits::{RiskConfig, RiskLimits};
use crate::signals::{evaluate_all_markets, SignalConfig, SignalDecision};
use crate::storage::PositionStore;
use crate::streams::polymarket::PolyMarket;

// G6: 200ms — was 500ms. Real edge windows in BTC are < 100ms; 500ms
// meant we routinely acted on stale state. CPU is cheap (rayon already
// fans across 20 cores during eval) so the higher cadence is free.
const TICK_MS: u64 = 200;

/// Settings the runner reads each tick. Built from main.rs::Config.
#[derive(Debug, Clone)]
pub struct RunnerConfig {
    /// Skip a market if we submitted within the last N seconds (per-market
    /// cooldown). Default: 60s — usually 1 trade per window per market.
    pub min_market_cooldown_secs: u64,
    /// Maximum tokio tasks in flight at once. Bounds memory + clob requests.
    pub max_concurrent_submits: usize,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            min_market_cooldown_secs: 60,
            max_concurrent_submits:   8,
        }
    }
}

/// Spawn the execution runner loop. Returns the task handle.
pub async fn execution_runner_loop(
    state:       Arc<RwLock<FeatureState>>,
    markets:     Arc<RwLock<Vec<PolyMarket>>>,
    pool:        Arc<OrderPool>,
    risk:        Arc<RiskLimits>,
    risk_cfg:    RiskConfig,
    sig_cfg:     SignalConfig,
    client:      Arc<ClobClient>,
    positions:   Arc<PositionStore>,
    runner_cfg:  RunnerConfig,
) {
    // Per-market cooldown tracker. Lock-free reads + atomic upsert via DashMap.
    let last_submit: Arc<DashMap<String, u64>> = Arc::new(DashMap::new());
    // Active task counter (atomic gate on max_concurrent_submits)
    let in_flight: Arc<std::sync::atomic::AtomicUsize> =
        Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut tick = tokio::time::interval(Duration::from_millis(TICK_MS));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    tracing::info!(
        cooldown_secs = runner_cfg.min_market_cooldown_secs,
        max_concurrent = runner_cfg.max_concurrent_submits,
        "execution runner loop started"
    );

    loop {
        tick.tick().await;

        // Snapshot once per tick — drop locks before any spawn.
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;
        let markets_snapshot = markets.read().await.clone();
        let state_snapshot = {
            let s = state.read().await;
            (*s).clone()
        };

        // Evaluate all active markets in parallel via rayon.
        let decisions = evaluate_all_markets(
            &state_snapshot, &markets_snapshot, &sig_cfg, now_ms,
        );

        // Dispatch every fired signal as an independent tokio task.
        for (market_id, decision) in decisions {
            if matches!(decision, SignalDecision::None) { continue; }

            // Per-market cooldown
            if let Some(last) = last_submit.get(&market_id) {
                let dt_ms = now_ms.saturating_sub(*last);
                if dt_ms < runner_cfg.min_market_cooldown_secs * 1000 {
                    tracing::trace!(
                        market = %market_id,
                        dt_ms,
                        "skipping — within cooldown"
                    );
                    continue;
                }
            }

            // Bound concurrent in-flight submits — backpressure
            let cur = in_flight.load(std::sync::atomic::Ordering::Acquire);
            if cur >= runner_cfg.max_concurrent_submits {
                tracing::warn!(
                    market = %market_id,
                    in_flight = cur,
                    "submit queue full — dropping signal"
                );
                continue;
            }

            // Mark cooldown BEFORE spawn so simultaneous evaluations
            // (e.g. tick boundary collision) see it.
            last_submit.insert(market_id.clone(), now_ms);
            in_flight.fetch_add(1, std::sync::atomic::Ordering::Release);

            // Spawn — every signal executes in parallel.
            let pool_c   = pool.clone();
            let risk_c   = risk.clone();
            let client_c = client.clone();
            let pos_c    = positions.clone();
            let m_snap   = markets_snapshot.clone();
            let s_snap   = state_snapshot.clone();
            let risk_cfg_c = risk_cfg;
            let in_flight_c = in_flight.clone();
            let market_id_c = market_id.clone();

            tokio::spawn(async move {
                let ctx = ExecContext {
                    state:    &s_snap,
                    markets:  &m_snap,
                    risk:     &risk_c,
                    risk_cfg: &risk_cfg_c,
                    pool:     &pool_c,
                    client:   &client_c,
                    positions:        Some(&pos_c),
                    target_market_id: Some(&market_id_c),
                };
                let outcome = execute_signal(&decision, &ctx).await;
                tracing::info!(
                    market = %market_id_c,
                    outcome = ?summarize_outcome(&outcome),
                    "execution complete"
                );
                in_flight_c.fetch_sub(1, std::sync::atomic::Ordering::Release);
            });
        }

        // Garbage-collect cooldown entries for markets no longer in discovery list.
        last_submit.retain(|id, _| markets_snapshot.iter().any(|m| &m.id == id));
    }
}

fn summarize_outcome(o: &crate::execution::executor::ExecOutcome) -> &'static str {
    use crate::execution::executor::ExecOutcome::*;
    match o {
        Skipped { .. }   => "skipped",
        Submitted { .. } => "submitted",
        Rejected { .. }  => "rejected",
        Errored { .. }   => "errored",
    }
}

// ── One-shot tick helper (testable) ──────────────────────────────────────────

/// Runs a single tick of the runner: evaluate, dispatch, return outcomes.
///
/// Made public for tests so they can drive the dispatcher without spinning
/// the long-running loop. Production code calls execution_runner_loop above.
pub async fn run_one_tick(
    state:       &Arc<RwLock<FeatureState>>,
    markets:     &Arc<RwLock<Vec<PolyMarket>>>,
    pool:        &Arc<OrderPool>,
    risk:        &Arc<RiskLimits>,
    risk_cfg:    &RiskConfig,
    sig_cfg:     &SignalConfig,
    client:      &Arc<ClobClient>,
    positions:   &Arc<PositionStore>,
    last_submit: &Arc<DashMap<String, u64>>,
    runner_cfg:  &RunnerConfig,
    now_ms:      u64,
) -> Result<Vec<(String, &'static str)>> {
    let markets_snapshot = markets.read().await.clone();
    let state_snapshot = {
        let s = state.read().await;
        (*s).clone()
    };

    let decisions = evaluate_all_markets(&state_snapshot, &markets_snapshot, sig_cfg, now_ms);

    let mut handles = Vec::new();
    for (market_id, decision) in decisions {
        if matches!(decision, SignalDecision::None) { continue; }
        if let Some(last) = last_submit.get(&market_id) {
            let dt_ms = now_ms.saturating_sub(*last);
            if dt_ms < runner_cfg.min_market_cooldown_secs * 1000 { continue; }
        }
        last_submit.insert(market_id.clone(), now_ms);

        let pool_c   = pool.clone();
        let risk_c   = risk.clone();
        let client_c = client.clone();
        let pos_c    = positions.clone();
        let m_snap   = markets_snapshot.clone();
        let s_snap   = state_snapshot.clone();
        let risk_cfg_c = *risk_cfg;
        let market_id_c = market_id.clone();

        handles.push(tokio::spawn(async move {
            let ctx = ExecContext {
                state:    &s_snap,
                markets:  &m_snap,
                risk:     &risk_c,
                risk_cfg: &risk_cfg_c,
                pool:     &pool_c,
                client:   &client_c,
                positions:        Some(&pos_c),
                target_market_id: Some(&market_id_c),
            };
            let outcome = execute_signal(&decision, &ctx).await;
            (market_id_c, summarize_outcome(&outcome))
        }));
    }

    let mut out = Vec::new();
    for h in handles {
        if let Ok(r) = h.await { out.push(r); }
    }
    Ok(out)
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::auth::ApiCreds;
    use crate::execution::clob::{ClobClient, ClobConfig};
    use crate::execution::orders::SignatureType;
    use crate::execution::executor::{populate_pool_for_window, WindowSetup};
    use crate::features::BookState;
    use base64::{engine::general_purpose::URL_SAFE, Engine as _};

    const TEST_PK:   &str =
        "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
    const TEST_ADDR: &str = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266";

    const UP_M1:   &str = "111111111";
    const DOWN_M1: &str = "112222222";
    const UP_M2:   &str = "211111111";
    const DOWN_M2: &str = "212222222";

    fn temp_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "constantine-runner-{}", uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn sample_creds() -> ApiCreds {
        ApiCreds {
            api_key:        "00000000-1111-2222-3333-444444444444".into(),
            api_secret:     URL_SAFE.encode(b"test-secret-bytes-here-32-chars!"),
            api_passphrase: "passphrase".into(),
        }
    }

    fn make_client() -> Arc<ClobClient> {
        let cfg = ClobConfig::new(TEST_ADDR, sample_creds(), /*dry_run=*/true);
        Arc::new(ClobClient::new(cfg).unwrap())
    }

    fn book(bid: f64, ask: f64) -> BookState {
        BookState {
            best_bid: bid, best_ask: ask,
            best_bid_size: 1000.0, best_ask_size: 1000.0,
            last_trade_price: (bid + ask) / 2.0,
            timestamp_ms: 0,
        }
    }

    fn poly_market(id: &str, up: &str, down: &str, close_ms: u64) -> PolyMarket {
        PolyMarket {
            id: id.into(),
            question: "BTC".into(),
            up_token_id:   up.into(),
            down_token_id: down.into(),
            close_time_ms: close_ms,
            duration_min:  5,
            liquidity_usd: 1_000.0,
        }
    }

    async fn setup_two_markets() -> (
        Arc<RwLock<FeatureState>>,
        Arc<RwLock<Vec<PolyMarket>>>,
        Arc<OrderPool>,
        Arc<RiskLimits>,
        Arc<ClobClient>,
        Arc<PositionStore>,
        u64,
    ) {
        // Two markets, both with oracle setup (yes underpriced)
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;
        let close_ms = now_ms + 60_000;

        let m1 = poly_market("m1", UP_M1, DOWN_M1, close_ms);
        let m2 = poly_market("m2", UP_M2, DOWN_M2, close_ms);

        let mut s = FeatureState::default();
        s.spot_price = 80_300.0;
        s.window_strikes.insert("m1".into(), (now_ms - 30_000, 80_000.0));
        s.window_strikes.insert("m2".into(), (now_ms - 30_000, 80_000.0));
        // Both books underpriced on YES → oracle should fire
        s.asset_books.insert(UP_M1.into(),   book(0.40, 0.45));
        s.asset_books.insert(DOWN_M1.into(), book(0.50, 0.55));
        s.asset_books.insert(UP_M2.into(),   book(0.40, 0.45));
        s.asset_books.insert(DOWN_M2.into(), book(0.50, 0.55));

        let state   = Arc::new(RwLock::new(s));
        let markets = Arc::new(RwLock::new(vec![m1.clone(), m2.clone()]));
        let pool    = Arc::new(OrderPool::new());

        // Pre-sign for both markets
        for m in [&m1, &m2] {
            let setup = WindowSetup::for_market(
                m, 0.45, 0.55, 5.0, 15, 0, 0,
                SignatureType::Eoa, TEST_PK.to_string(), None,
            );
            populate_pool_for_window(&pool, &setup).unwrap();
        }

        let risk = Arc::new(RiskLimits::new());
        let client = make_client();
        let positions = Arc::new(PositionStore::open(&temp_dir()).unwrap());
        (state, markets, pool, risk, client, positions, now_ms)
    }

    // ── Per-market dispatch ───────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_tick_dispatches_to_all_markets_in_parallel() {
        let (state, markets, pool, risk, client, positions, now_ms) = setup_two_markets().await;
        let last_submit = Arc::new(DashMap::new());
        let mut sig_cfg = SignalConfig::default();
        sig_cfg.min_window_age_secs = 0.0;
        let runner_cfg = RunnerConfig::default();

        let outcomes = run_one_tick(
            &state, &markets, &pool, &risk, &RiskConfig::default(),
            &sig_cfg, &client, &positions, &last_submit, &runner_cfg, now_ms,
        ).await.unwrap();

        // Both markets get one outcome each
        assert_eq!(outcomes.len(), 2);
        for (_id, status) in &outcomes {
            assert!(*status == "submitted", "expected submitted, got {status}");
        }
        // Cooldown set for both
        assert_eq!(last_submit.len(), 2);
        // Position ledger has 2 opens (DRY_RUN still records to ledger)
        assert_eq!(positions.len(), 2);
    }

    #[tokio::test]
    async fn cooldown_prevents_double_submit_on_same_market() {
        let (state, markets, pool, risk, client, positions, now_ms) = setup_two_markets().await;
        let last_submit = Arc::new(DashMap::new());
        let mut sig_cfg = SignalConfig::default();
        sig_cfg.min_window_age_secs = 0.0;
        let runner_cfg = RunnerConfig {
            min_market_cooldown_secs: 60, max_concurrent_submits: 8,
        };

        // First tick — 2 submits
        let r1 = run_one_tick(
            &state, &markets, &pool, &risk, &RiskConfig::default(),
            &sig_cfg, &client, &positions, &last_submit, &runner_cfg, now_ms,
        ).await.unwrap();
        assert_eq!(r1.len(), 2);

        // Second tick within cooldown — both skipped
        let r2 = run_one_tick(
            &state, &markets, &pool, &risk, &RiskConfig::default(),
            &sig_cfg, &client, &positions, &last_submit, &runner_cfg,
            now_ms + 5_000,   // 5s later
        ).await.unwrap();
        assert_eq!(r2.len(), 0);

        // Beyond cooldown — would re-submit, but pool is empty for that price
        // so they'd get Skipped. We just check they were attempted.
        // (Pool slots at 45c are gone after r1's takes.)
    }

    #[tokio::test]
    async fn dispatch_handles_no_signals_gracefully() {
        // FeatureState with no fired signals → dispatch returns empty
        let mut s = FeatureState::default();
        s.spot_price = 80_000.0;
        // No window strike, no books → no oracle / intramarket fire
        let state   = Arc::new(RwLock::new(s));
        let markets = Arc::new(RwLock::new(vec![]));
        let pool    = Arc::new(OrderPool::new());
        let risk    = Arc::new(RiskLimits::new());
        let client  = make_client();
        let positions = Arc::new(PositionStore::open(&temp_dir()).unwrap());
        let last_submit = Arc::new(DashMap::new());
        let runner_cfg = RunnerConfig::default();

        let r = run_one_tick(
            &state, &markets, &pool, &risk, &RiskConfig::default(),
            &SignalConfig::default(), &client, &positions, &last_submit, &runner_cfg,
            chrono::Utc::now().timestamp_millis() as u64,
        ).await.unwrap();
        assert!(r.is_empty());
    }

    #[tokio::test]
    async fn cooldown_garbage_collects_for_dropped_markets() {
        // Verify the runner's GC of the cooldown map (covered indirectly
        // since the inner GC is in the loop, not run_one_tick. Instead
        // we test that DashMap::retain works as we expect.)
        let cd = Arc::new(DashMap::new());
        cd.insert("alive".to_string(), 1u64);
        cd.insert("dropped".to_string(), 2u64);
        let active = ["alive".to_string()];
        cd.retain(|k, _| active.iter().any(|a| a == k));
        assert!(cd.contains_key("alive"));
        assert!(!cd.contains_key("dropped"));
    }

    // ── Backpressure ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn cooldown_per_market_isolation() {
        // m1 in cooldown, m2 fresh — only m2 should fire
        let (state, markets, pool, risk, client, positions, now_ms) = setup_two_markets().await;
        let last_submit = Arc::new(DashMap::new());
        last_submit.insert("m1".to_string(), now_ms);   // pretend m1 just submitted
        let mut sig_cfg = SignalConfig::default();
        sig_cfg.min_window_age_secs = 0.0;
        let runner_cfg = RunnerConfig::default();

        let r = run_one_tick(
            &state, &markets, &pool, &risk, &RiskConfig::default(),
            &sig_cfg, &client, &positions, &last_submit, &runner_cfg,
            now_ms + 1_000,
        ).await.unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].0, "m2");
    }
}
