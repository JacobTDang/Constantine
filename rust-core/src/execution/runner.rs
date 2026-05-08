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

use std::path::PathBuf;

use crate::execution::clob::ClobClient;
use crate::execution::event_arb_executor::{execute_event_arb, EventArbConfig};
use crate::execution::executor::{execute_signal, ExecContext, SigningParams};
use crate::features::FeatureState;
use crate::risk::limits::{RiskConfig, RiskLimits};
use crate::signals::event_arb::EventArbCache;
use crate::signals::player_props::{evaluate_all_props, ProjectionsCache};
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
    /// Strategy 1 — NBA player props. When set, runner periodically reloads
    /// the projections cache from disk and emits prop signals alongside
    /// oracle/intramarket. None = props strategy disabled.
    pub projections_cache:        Option<Arc<ProjectionsCache>>,
    pub projections_path:         Option<PathBuf>,
    pub projections_reload_secs:  u64,
    /// Strategy 2 — Event sum-of-YES arb. When set, runner periodically
    /// dispatches portfolios from the cache. Cache is populated by a
    /// background scanner task (caller's responsibility).
    pub event_arb_cache:          Option<Arc<EventArbCache>>,
    pub event_arb_cooldown_secs:  u64,
    pub event_arb_cfg:            EventArbConfig,
    /// EDGE-C — Sportsbook devig overlay. Reloaded periodically.
    pub devig_cache:              Option<Arc<crate::signals::sportsbook_devig::DevigCache>>,
    pub devig_path:               Option<PathBuf>,
    pub devig_reload_secs:        u64,
    /// EDGE-D — Whale follow. Reloaded periodically (tail-only read).
    pub whale_cache:              Option<Arc<crate::signals::whale_follow::WhaleCache>>,
    pub whale_path:               Option<PathBuf>,
    pub whale_reload_secs:        u64,
    /// SignalLog — multi-strategy signal events get logged here with
    /// strategy_tag so daily_review.py can slice by strategy.
    pub signal_log:               Option<Arc<crate::storage::SignalLog>>,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            min_market_cooldown_secs: 60,
            max_concurrent_submits:   8,
            projections_cache:        None,
            projections_path:         None,
            projections_reload_secs:  30,
            event_arb_cache:          None,
            event_arb_cooldown_secs:  3600,    // 1h between portfolio attempts
            event_arb_cfg:            EventArbConfig {
                event_budget_usd: 60.0,
                min_leg_dollars:  5.0,
            },
            devig_cache:              None,
            devig_path:               None,
            devig_reload_secs:        30,
            whale_cache:              None,
            whale_path:               None,
            whale_reload_secs:        30,
            signal_log:               None,
        }
    }
}

/// Spawn the execution runner loop. Returns the task handle.
pub async fn execution_runner_loop(
    state:       Arc<RwLock<FeatureState>>,
    markets:     Arc<RwLock<Vec<PolyMarket>>>,
    signing:     Arc<SigningParams>,
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
        props_enabled = runner_cfg.projections_cache.is_some(),
        "execution runner loop started"
    );

    // Strategy 1 — periodic projections reload. Runs as a sibling task so
    // file I/O doesn't block the tick. Errors are logged and swallowed.
    if let (Some(cache), Some(path)) =
        (runner_cfg.projections_cache.clone(), runner_cfg.projections_path.clone())
    {
        let secs = runner_cfg.projections_reload_secs.max(5);
        tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_secs(secs));
            t.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                t.tick().await;
                match cache.reload(&path) {
                    Ok(n) => tracing::trace!(n_markets = n, "projections cache reload"),
                    Err(e) => tracing::warn!(error = %e, "projections reload failed"),
                }
            }
        });
    }

    // EDGE-C — Sportsbook devig periodic reload (sibling task).
    if let (Some(cache), Some(path)) =
        (runner_cfg.devig_cache.clone(), runner_cfg.devig_path.clone())
    {
        let secs = runner_cfg.devig_reload_secs.max(5);
        tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_secs(secs));
            t.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                t.tick().await;
                match cache.reload(&path) {
                    Ok(n) => tracing::trace!(n_markets = n, "devig cache reload"),
                    Err(e) => tracing::warn!(error = %e, "devig reload failed"),
                }
            }
        });
    }

    // EDGE-D — Whale-trades tail-reader (sibling task).
    if let (Some(cache), Some(path)) =
        (runner_cfg.whale_cache.clone(), runner_cfg.whale_path.clone())
    {
        let secs = runner_cfg.whale_reload_secs.max(5);
        tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_secs(secs));
            t.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                t.tick().await;
                match cache.refresh(&path) {
                    Ok(n) => if n > 0 {
                        tracing::info!(n_new = n, "whale trades reloaded")
                    },
                    Err(e) => tracing::warn!(error = %e, "whale refresh failed"),
                }
            }
        });
    }

    loop {
        tick.tick().await;

        // Snapshot once per tick — drop locks before any spawn.
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;
        let mut markets_snapshot = markets.read().await.clone();
        let state_snapshot = {
            let s = state.read().await;
            (*s).clone()
        };

        // Strategy 1: append synthetic prop markets so resolve_target can
        // route prop signals via the standard markets-list lookup.
        // (evaluate_all_markets safely skips them — no asset_books for props.)
        if let Some(cache) = runner_cfg.projections_cache.as_ref() {
            markets_snapshot.extend(cache.synthesize_polymarkets());
        }
        // EDGE-C: same trick for sportsbook devig markets.
        if let Some(cache) = runner_cfg.devig_cache.as_ref() {
            markets_snapshot.extend(cache.synthesize_polymarkets());
        }

        // Evaluate all active markets in parallel via rayon (oracle + intramarket).
        let mut decisions = evaluate_all_markets(
            &state_snapshot, &markets_snapshot, &sig_cfg, now_ms,
        );

        // Strategy 1: append prop signals from the projections cache.
        let mut signal_tags: Vec<(String, &'static str)> = Vec::new();
        if let Some(cache) = runner_cfg.projections_cache.as_ref() {
            let props = evaluate_all_props(cache);
            for (mid, _) in &props { signal_tags.push((mid.clone(), "player_props")); }
            decisions.extend(props);
        }
        // EDGE-C: append sportsbook devig signals.
        if let Some(cache) = runner_cfg.devig_cache.as_ref() {
            if !risk.is_strategy_killed(crate::risk::limits::Strategy::SportsbookDevig) {
                let dev = crate::signals::sportsbook_devig::evaluate_all_devig(cache);
                for (mid, _) in &dev { signal_tags.push((mid.clone(), "sportsbook_devig")); }
                decisions.extend(dev);
            }
        }
        // EDGE-D: drain whale-cache and append. Drain semantics — each
        // whale trade fires at most once.
        if let Some(cache) = runner_cfg.whale_cache.as_ref() {
            if !risk.is_strategy_killed(crate::risk::limits::Strategy::WhaleFollow) {
                let whales = crate::signals::whale_follow::evaluate_all_whales(cache);
                for (mid, _) in &whales { signal_tags.push((mid.clone(), "whale_follow")); }
                decisions.extend(whales);
            }
        }

        // Multi-strategy signal logging — write each fired signal to
        // signals.jsonl with its strategy_tag so daily_review.py can
        // slice by strategy.
        if let Some(log) = runner_cfg.signal_log.as_ref() {
            for (mid, dec) in &decisions {
                if matches!(dec, SignalDecision::None) { continue; }
                let tag = signal_tags.iter()
                    .find(|(m, _)| m == mid)
                    .map(|(_, t)| *t)
                    .unwrap_or("oracle");   // fall through for BTC oracle/intra
                if let Err(e) = log_signal_with_tag(log, mid, tag, dec) {
                    tracing::debug!(error = %e, "multi-strategy signal log failed");
                }
            }
        }

        // Strategy 2: dispatch any event-arb portfolios in cache. Each
        // portfolio dispatch is one event_id with its own cooldown.
        // Spawn so HTTP submits run concurrently with the main tick.
        if let Some(cache) = runner_cfg.event_arb_cache.as_ref() {
            for edge in cache.snapshot() {
                // Per-event cooldown to avoid re-submitting on every tick.
                let key = format!("event-arb::{}", edge.event_id);
                if let Some(last) = last_submit.get(&key) {
                    let dt = now_ms.saturating_sub(*last);
                    if dt < runner_cfg.event_arb_cooldown_secs * 1000 { continue; }
                }
                last_submit.insert(key, now_ms);

                let cfg     = runner_cfg.event_arb_cfg;
                let r2      = risk.clone();
                let cl      = client.clone();
                let pos     = positions.clone();
                let sg      = signing.clone();
                let rcfg    = risk_cfg;
                tokio::spawn(async move {
                    let _ = execute_event_arb(
                        &edge, &cfg, &r2, &rcfg, &sg, &cl, Some(&pos),
                    ).await;
                });
            }
        }

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
            let sign_c   = signing.clone();
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
                    signing:  &sign_c,
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

/// Multi-strategy signal log helper. Translates a SignalDecision +
/// strategy_tag into a SignalRow and appends it to signals.jsonl.
/// Used by execution_runner_loop to capture every emitted multi-strategy
/// signal for offline analysis.
fn log_signal_with_tag(
    log:          &Arc<crate::storage::SignalLog>,
    market_id:    &str,
    strategy_tag: &str,
    decision:     &SignalDecision,
) -> anyhow::Result<()> {
    use crate::signals::Direction;
    let row = match decision {
        SignalDecision::None => return Ok(()),
        SignalDecision::Intramarket(s) => crate::storage::SignalRow::for_strategy(
            strategy_tag, market_id, "intramarket", None,
            1.0, 0.5, s.net_profit, 1.0, 30.0, 0.0,
        ),
        SignalDecision::Oracle(s) => {
            let dir_s = match s.direction {
                Direction::Up   => "up",
                Direction::Down => "down",
            };
            let bet = crate::execution::kelly::kelly_size(
                s.fair_value, s.market_price, 1500.0, 0.25, 30.0,
            );
            crate::storage::SignalRow::for_strategy(
                strategy_tag, market_id, "oracle", Some(dir_s),
                s.fair_value, s.market_price, s.edge, s.confidence, bet,
                s.time_to_close_secs,
            )
        }
    };
    log.insert_signal(&row)
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
    signing:     &Arc<SigningParams>,
    risk:        &Arc<RiskLimits>,
    risk_cfg:    &RiskConfig,
    sig_cfg:     &SignalConfig,
    client:      &Arc<ClobClient>,
    positions:   &Arc<PositionStore>,
    last_submit: &Arc<DashMap<String, u64>>,
    runner_cfg:  &RunnerConfig,
    now_ms:      u64,
) -> Result<Vec<(String, &'static str)>> {
    let mut markets_snapshot = markets.read().await.clone();
    let state_snapshot = {
        let s = state.read().await;
        (*s).clone()
    };

    // Mirror execution_runner_loop's eval shape so tests can drive the
    // full prop-signal path through this helper.
    if let Some(cache) = runner_cfg.projections_cache.as_ref() {
        markets_snapshot.extend(cache.synthesize_polymarkets());
    }
    let mut decisions = evaluate_all_markets(&state_snapshot, &markets_snapshot, sig_cfg, now_ms);
    if let Some(cache) = runner_cfg.projections_cache.as_ref() {
        decisions.extend(evaluate_all_props(cache));
    }

    let mut handles = Vec::new();
    for (market_id, decision) in decisions {
        if matches!(decision, SignalDecision::None) { continue; }
        if let Some(last) = last_submit.get(&market_id) {
            let dt_ms = now_ms.saturating_sub(*last);
            if dt_ms < runner_cfg.min_market_cooldown_secs * 1000 { continue; }
        }
        last_submit.insert(market_id.clone(), now_ms);

        let sign_c   = signing.clone();
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
                signing:  &sign_c,
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
    use crate::execution::orders::{Domain, SignatureType};
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

    fn sample_signing_arc() -> Arc<SigningParams> {
        Arc::new(SigningParams {
            domain:          Domain::polymarket_polygon(),
            private_key_hex: TEST_PK.to_string(),
            signature_type:  SignatureType::Eoa,
            funder_address:  None,
            nonce:           0,
            fee_rate_bps:    0,
            taker:           [0u8; 20],
        })
    }

    async fn setup_two_markets() -> (
        Arc<RwLock<FeatureState>>,
        Arc<RwLock<Vec<PolyMarket>>>,
        Arc<SigningParams>,
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
        let markets = Arc::new(RwLock::new(vec![m1, m2]));
        let signing = sample_signing_arc();

        let risk = Arc::new(RiskLimits::new());
        let client = make_client();
        let positions = Arc::new(PositionStore::open(&temp_dir()).unwrap());
        (state, markets, signing, risk, client, positions, now_ms)
    }

    // ── Per-market dispatch ───────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_tick_dispatches_to_all_markets_in_parallel() {
        let (state, markets, signing, risk, client, positions, now_ms) = setup_two_markets().await;
        let last_submit = Arc::new(DashMap::new());
        let mut sig_cfg = SignalConfig::default();
        sig_cfg.min_window_age_secs = 0.0;
        let runner_cfg = RunnerConfig::default();

        let outcomes = run_one_tick(
            &state, &markets, &signing, &risk, &RiskConfig::default(),
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
        let (state, markets, signing, risk, client, positions, now_ms) = setup_two_markets().await;
        let last_submit = Arc::new(DashMap::new());
        let mut sig_cfg = SignalConfig::default();
        sig_cfg.min_window_age_secs = 0.0;
        let runner_cfg = RunnerConfig::default();

        // First tick — 2 submits
        let r1 = run_one_tick(
            &state, &markets, &signing, &risk, &RiskConfig::default(),
            &sig_cfg, &client, &positions, &last_submit, &runner_cfg, now_ms,
        ).await.unwrap();
        assert_eq!(r1.len(), 2);

        // Second tick within cooldown — both skipped
        let r2 = run_one_tick(
            &state, &markets, &signing, &risk, &RiskConfig::default(),
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
        let signing = sample_signing_arc();
        let risk    = Arc::new(RiskLimits::new());
        let client  = make_client();
        let positions = Arc::new(PositionStore::open(&temp_dir()).unwrap());
        let last_submit = Arc::new(DashMap::new());
        let runner_cfg = RunnerConfig::default();

        let r = run_one_tick(
            &state, &markets, &signing, &risk, &RiskConfig::default(),
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
        let (state, markets, signing, risk, client, positions, now_ms) = setup_two_markets().await;
        let last_submit = Arc::new(DashMap::new());
        last_submit.insert("m1".to_string(), now_ms);   // pretend m1 just submitted
        let mut sig_cfg = SignalConfig::default();
        sig_cfg.min_window_age_secs = 0.0;
        let runner_cfg = RunnerConfig::default();

        let r = run_one_tick(
            &state, &markets, &signing, &risk, &RiskConfig::default(),
            &sig_cfg, &client, &positions, &last_submit, &runner_cfg,
            now_ms + 1_000,
        ).await.unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].0, "m2");
    }

    // ── Strategy 1: prop signal integration ──────────────────────────────

    async fn write_prop_cache(p_over: f64, yes_ask: f64, yes_bid: f64,
                              cid: &str, yes_tok: &str, no_tok: &str) -> ProjectionsCache {
        let path = std::env::temp_dir().join(format!(
            "constantine-runner-props-{}.json", uuid::Uuid::new_v4()
        ));
        let now_ms = chrono::Utc::now().timestamp_millis();
        let payload = serde_json::json!({
            "generated_at_ms": now_ms,
            "n_markets": 1,
            "projections": [{
                "market_id":    cid, "condition_id": cid,
                "question":     "Joel Embiid: Rebounds O/U 8.5",
                "player": "Joel Embiid", "stat": "Rebounds",
                "stat_code": "REB", "line": 8.5,
                "yes_token_id": yes_tok, "no_token_id": no_tok,
                "yes_ask": yes_ask, "yes_bid": yes_bid,
                "end_date_ms":  9_999_999_999_000_u64,
                "liquidity": 5000.0, "volume": 500.0,
                "p_over": p_over,
                "edge_over": p_over - yes_ask, "edge_under": (1.0 - p_over) - (1.0 - yes_bid),
                "diagnostics": {},
            }],
        });
        std::fs::write(&path, payload.to_string()).unwrap();
        let cache = ProjectionsCache::new();
        cache.reload(&path).unwrap();
        cache
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn run_one_tick_dispatches_prop_signal_when_cache_set() {
        // No regular markets, just a prop. Cache says big OVER edge.
        let state = Arc::new(RwLock::new(FeatureState::default()));
        let markets = Arc::new(RwLock::new(vec![]));
        let signing = sample_signing_arc();
        let risk = Arc::new(RiskLimits::new());
        let client = make_client();
        let positions = Arc::new(PositionStore::open(&temp_dir()).unwrap());
        let last_submit = Arc::new(DashMap::new());

        let cache = write_prop_cache(
            0.65, 0.50, 0.49,
            "0xPROP", "111", "222",
        ).await;
        let mut runner_cfg = RunnerConfig::default();
        runner_cfg.projections_cache = Some(Arc::new(cache));

        let mut sig_cfg = SignalConfig::default();
        sig_cfg.min_window_age_secs = 0.0;

        let r = run_one_tick(
            &state, &markets, &signing, &risk, &RiskConfig::default(),
            &sig_cfg, &client, &positions, &last_submit, &runner_cfg,
            chrono::Utc::now().timestamp_millis() as u64,
        ).await.unwrap();

        // Prop signal fired → dispatched, executed in DRY_RUN
        assert_eq!(r.len(), 1, "expected 1 prop signal, got {:?}", r);
        assert_eq!(r[0].0, "0xPROP");
        assert_eq!(r[0].1, "submitted");
        assert_eq!(positions.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn no_prop_cache_means_no_prop_signals() {
        let state = Arc::new(RwLock::new(FeatureState::default()));
        let markets = Arc::new(RwLock::new(vec![]));
        let signing = sample_signing_arc();
        let risk = Arc::new(RiskLimits::new());
        let client = make_client();
        let positions = Arc::new(PositionStore::open(&temp_dir()).unwrap());
        let last_submit = Arc::new(DashMap::new());
        let runner_cfg = RunnerConfig::default();   // projections_cache: None

        let r = run_one_tick(
            &state, &markets, &signing, &risk, &RiskConfig::default(),
            &SignalConfig::default(), &client, &positions, &last_submit, &runner_cfg,
            chrono::Utc::now().timestamp_millis() as u64,
        ).await.unwrap();
        assert!(r.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn prop_cooldown_blocks_repeat_within_window() {
        let state = Arc::new(RwLock::new(FeatureState::default()));
        let markets = Arc::new(RwLock::new(vec![]));
        let signing = sample_signing_arc();
        let risk = Arc::new(RiskLimits::new());
        let client = make_client();
        let positions = Arc::new(PositionStore::open(&temp_dir()).unwrap());
        let last_submit = Arc::new(DashMap::new());

        let cache = write_prop_cache(0.65, 0.50, 0.49, "0xPROP", "111", "222").await;
        let mut runner_cfg = RunnerConfig::default();
        runner_cfg.projections_cache = Some(Arc::new(cache));
        let mut sig_cfg = SignalConfig::default();
        sig_cfg.min_window_age_secs = 0.0;

        let now = chrono::Utc::now().timestamp_millis() as u64;
        // First tick: fires
        let r1 = run_one_tick(
            &state, &markets, &signing, &risk, &RiskConfig::default(),
            &sig_cfg, &client, &positions, &last_submit, &runner_cfg, now,
        ).await.unwrap();
        assert_eq!(r1.len(), 1);
        // Second tick within cooldown: skipped
        let r2 = run_one_tick(
            &state, &markets, &signing, &risk, &RiskConfig::default(),
            &sig_cfg, &client, &positions, &last_submit, &runner_cfg, now + 5_000,
        ).await.unwrap();
        assert!(r2.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn synthetic_market_does_not_fire_oracle_path() {
        // Verify the safety property: synthetic prop markets get added to
        // markets_snapshot, but evaluate_all_markets shouldn't fire on
        // them (state.asset_books is empty for prop tokens).
        // Prop p_over below MIN_EDGE → prop path won't fire either.
        let state = Arc::new(RwLock::new(FeatureState::default()));
        let markets = Arc::new(RwLock::new(vec![]));
        let signing = sample_signing_arc();
        let risk = Arc::new(RiskLimits::new());
        let client = make_client();
        let positions = Arc::new(PositionStore::open(&temp_dir()).unwrap());
        let last_submit = Arc::new(DashMap::new());

        // Cache row that's too contested for prop signal to fire (model
        // = 0.52, market = 0.50 → 2pp edge < 7pp MIN_EDGE)
        let cache = write_prop_cache(0.52, 0.50, 0.49, "0xPROP", "111", "222").await;
        let mut runner_cfg = RunnerConfig::default();
        runner_cfg.projections_cache = Some(Arc::new(cache));
        let mut sig_cfg = SignalConfig::default();
        sig_cfg.min_window_age_secs = 0.0;

        let r = run_one_tick(
            &state, &markets, &signing, &risk, &RiskConfig::default(),
            &sig_cfg, &client, &positions, &last_submit, &runner_cfg,
            chrono::Utc::now().timestamp_millis() as u64,
        ).await.unwrap();
        // No oracle/intramarket signal (no books for the synthetic market),
        // no prop signal (edge below threshold) → empty.
        assert!(r.is_empty(), "expected empty dispatch, got {:?}", r);
    }
}
