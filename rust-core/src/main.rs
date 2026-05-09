use std::sync::Arc;

use polymarket_bot::{
    config::Config,
    execution, features, ipc, logging, risk, signals, storage, streams,
};
use polymarket_bot::features::FeatureState;

/// EDGE-A: resolve a Polymarket condition_id to its (yes_token_id,
/// no_token_id) pair via the public Gamma /markets endpoint. Returns
/// None on any failure — discovery skips unresolvable markets.
async fn resolve_condition_to_tokens(
    http: &reqwest::Client,
    condition_id: &str,
) -> Option<(String, String)> {
    let url = format!(
        "https://gamma-api.polymarket.com/markets?condition_ids={}",
        condition_id
    );
    let resp = http.get(&url).send().await.ok()?;
    if !resp.status().is_success() { return None; }
    let body = resp.text().await.ok()?;
    let arr: serde_json::Value = serde_json::from_str(&body).ok()?;
    let market = arr.as_array()?.first()?;
    let raw = market.get("clobTokenIds")?.as_str()?;
    let tokens: Vec<String> = serde_json::from_str(raw).ok()?;
    if tokens.len() != 2 { return None; }
    Some((tokens[0].clone(), tokens[1].clone()))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    let log_level = std::env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string());
    logging::init(&log_level);

    // Try to load full config — only required when EXECUTION_ENABLED=true.
    // In pure observe mode we tolerate missing creds so the bot still runs
    // for data collection.
    let cfg_result = Config::from_env();
    let exec_cfg = match &cfg_result {
        Ok(c) if c.execution_enabled => {
            tracing::info!(
                bankroll = c.bankroll,
                max_bet  = c.max_bet_dollars,
                kelly    = c.kelly_fraction,
                dry_run  = c.dry_run,
                sig_type = ?c.polymarket_signature_type,
                "execution path ENABLED"
            );
            Some(c.clone())
        }
        Ok(_) => {
            tracing::info!("execution path disabled — observe mode only");
            None
        }
        Err(e) => {
            tracing::warn!(error = %e, "config not fully loaded — observe mode only");
            None
        }
    };

    // B7: single broadcast channel — all streams fan out into one event bus
    let (tx, _) =
        tokio::sync::broadcast::channel::<streams::StreamEvent>(1024);

    // C11: shared feature state, written by compute_loop, read by IPC + signals
    let state = Arc::new(tokio::sync::RwLock::new(FeatureState::default()));

    // D1: shared list of active BTC markets, refreshed every 5 minutes
    let markets: Arc<tokio::sync::RwLock<Vec<streams::polymarket::PolyMarket>>> =
        Arc::new(tokio::sync::RwLock::new(Vec::new()));

    let mut tasks = tokio::task::JoinSet::new();

    // Data streams (B1-B6)
    tasks.spawn(streams::binance::spot_stream(tx.clone()));
    tasks.spawn(streams::binance::perp_stream(tx.clone()));
    tasks.spawn(streams::binance::oi_stream(tx.clone()));
    tasks.spawn(streams::binance::kline_stream(tx.clone()));
    tasks.spawn(streams::liquidations::liq_stream(tx.clone()));

    // Feature compute loop (C11 + D6 — reads markets to compute poly features)
    tasks.spawn(features::compute_loop(tx.subscribe(), state.clone(), markets.clone()));

    // Polymarket: market discovery (D1) + Chainlink polling (D2) + CLOB orchestrator (D4/D6)
    tasks.spawn(streams::polymarket::market_discovery_loop(markets.clone()));
    tasks.spawn(streams::polymarket::chainlink_polling_loop(tx.clone()));
    tasks.spawn(streams::polymarket::clob_orchestrator(markets.clone(), tx.clone()));

    // Observe-mode JSONL logs: every fired signal + settlement outcome
    // analysed offline via scripts/observe_report.py
    let db_dir = std::path::PathBuf::from("data/db");
    let signal_log = Arc::new(
        storage::SignalLog::open(&db_dir)
            .expect("failed to open signal log dir")
    );

    // S5: Position ledger — records every order we send through its lifecycle
    // (Submitted → Filled → Settled / Failed). Always opened so we can replay
    // a previous run's positions even in observe-only mode.
    let positions = Arc::new(
        storage::PositionStore::open(&db_dir)
            .expect("failed to open position store")
    );
    tracing::info!(positions = positions.len(), "position store opened");

    // S5 + J11: Pre-signed order pool. Shared across the window-watcher
    // (which fills it) and the executor (which drains it).
    let order_pool = Arc::new(execution::presign::OrderPool::new());

    // S5/S6 — risk limits + execution runner shared across spawns.
    let risk_limits = Arc::new(risk::limits::RiskLimits::new());

    // S7.1+S7.3 — Watchdog: trips kill switch on data-loss / time-sync drift.
    // trip_kill_switch only takes effect when execution is enabled (in observe
    // mode the kill switch is harmless because we never submit anyway).
    let watchdog_cfg = risk::watchdog::WatchdogConfig {
        trip_kill_switch: exec_cfg.is_some(),
        ..Default::default()
    };
    tasks.spawn(risk::watchdog::watchdog_loop(
        state.clone(), risk_limits.clone(), watchdog_cfg,
    ));

    // S7.4 — Heartbeat: 30s status log so ops can grep "HB". G7 adds
    // realised P&L + current bankroll so ops can see compounding live.
    let starting_bankroll = exec_cfg.as_ref()
        .map(|c| c.bankroll)
        .unwrap_or(1500.0);
    tasks.spawn(risk::heartbeat::heartbeat_loop(
        state.clone(), risk_limits.clone(), positions.clone(),
        starting_bankroll, 30,
    ));

    // G2 — USDC allowance watchdog. Polls the CTF Exchange's allowance
    // every 5 minutes via Polygon RPC; trips kill switch when balance
    // falls below threshold. Only spawned when execution is enabled AND
    // we have an Alchemy key (the Config gates this).
    if let Some(c) = &exec_cfg {
        if !c.alchemy_polygon_key.is_empty() {
            // Derive the owner address (funder for proxy types, EOA otherwise)
            let owner = c.polymarket_funder_address.clone()
                .or_else(|| {
                    execution::orders::private_key_to_address(&c.polymarket_private_key)
                        .ok()
                        .map(|a| execution::orders::address_to_hex(&a))
                });
            if let Some(owner) = owner {
                let rpc_url = format!(
                    "https://polygon-mainnet.g.alchemy.com/v2/{}",
                    c.alchemy_polygon_key
                );
                let watchdog_cfg = risk::allowance::AllowanceWatchdogConfig::for_polygon_eoa(
                    rpc_url, owner,
                );
                tasks.spawn(risk::allowance::allowance_watchdog_loop(
                    watchdog_cfg, risk_limits.clone(),
                ));
                tracing::info!("allowance watchdog spawned");
            } else {
                tracing::warn!("allowance watchdog NOT spawned — couldn't derive owner address");
            }
        }
    }

    // J11: Window-open hook — populates the order pool the moment a new
    // BTC window's strike is captured. Spawning is gated on full config
    // since pre-signing requires a private key + sig type.
    if let Some(c) = &exec_cfg {
        let watcher_cfg = execution::window_watcher::WindowWatcherConfig {
            bet_dollars:     c.max_bet_dollars,
            depth:           15,
            nonce:           0,
            fee_rate_bps:    0,
            signature_type:  c.polymarket_signature_type,
            private_key_hex: c.polymarket_private_key.clone(),
            funder_address:  c.polymarket_funder_address.clone(),
        };
        tasks.spawn(execution::window_watcher::window_watcher_loop(
            state.clone(),
            markets.clone(),
            order_pool.clone(),
            watcher_cfg,
        ));
        tracing::info!("window_watcher spawned");
    }

    // S6.4: Execution runner — every 500ms, evaluate every active market in
    // parallel (rayon) and dispatch a tokio task per fired signal. Per-market
    // cooldown prevents stacking trades on the same window.
    if let Some(c) = &exec_cfg {
        let creds = execution::auth::ApiCreds {
            api_key:        c.polymarket_api_key.clone(),
            api_secret:     c.polymarket_api_secret.clone(),
            api_passphrase: c.polymarket_api_passphrase.clone(),
        };
        let address = c.polymarket_funder_address.clone()
            .unwrap_or_else(|| {
                // EOA path: derive from private key
                match execution::orders::private_key_to_address(&c.polymarket_private_key) {
                    Ok(a) => execution::orders::address_to_hex(&a),
                    Err(e) => {
                        tracing::error!(error = %e, "couldn't derive address — using zero");
                        "0x0000000000000000000000000000000000000000".to_string()
                    }
                }
            });
        let mut clob_cfg = execution::clob::ClobConfig::new(address, creds, c.dry_run);
        clob_cfg.order_type = execution::clob::OrderType::Fak;
        match execution::clob::ClobClient::new(clob_cfg) {
            Ok(client) => {
                let client = Arc::new(client);
                let risk_cfg = risk::limits::RiskConfig {
                    bankroll:                  c.bankroll,
                    max_bet_dollars:           c.max_bet_dollars,
                    kelly_fraction:            c.kelly_fraction,
                    min_edge:                  c.min_edge,
                    daily_loss_limit_dollars:  c.daily_loss_limit_dollars,
                    weekly_loss_limit_dollars: c.weekly_loss_limit_dollars,
                    max_open_exposure_dollars: c.max_open_exposure_dollars,
                    max_daily_trades:          100,
                };
                let sig_cfg = signals::SignalConfig {
                    fee_rate:                0.018,
                    intramarket_min_profit:  0.005,
                    oracle_arb_threshold:    c.oracle_arb_threshold,
                    min_time_remaining_secs: 5.0,
                    min_liquidity_usd:       c.min_liquidity,
                    max_spread:              0.10,
                    min_window_age_secs:     15.0,
                    min_ask_depth_usd:       50.0,
                    max_touch_consumption:   0.5,
                    max_bet_dollars:         c.max_bet_dollars,
                    // Per-tick eval used by the runner; signal_loop has
                    // its own SignalConfig where persist_to_db gates
                    // double-logging. Doesn't matter here either way.
                    persist_to_db:           true,
                };
                // Strategy 1: NBA player-prop projections cache. Reloaded
                // every 30s from data/nba_projections.json. The Python
                // sidecar (scripts/nba_projections.py --watch) writes this
                // file. If the sidecar isn't running, the cache stays
                // empty and props are silently skipped.
                let projections_cache = Arc::new(
                    signals::player_props::ProjectionsCache::new()
                );
                // Strategy 2: event-arb scanner cache populated by a
                // sibling task that hits Gamma every 5 min.
                let event_arb_cache = Arc::new(
                    signals::event_arb::EventArbCache::new()
                );
                tasks.spawn(signals::event_arb::event_arb_scanner_loop(
                    event_arb_cache.clone(),
                    300,
                ));
                let mut runner_cfg = execution::runner::RunnerConfig::default();
                runner_cfg.projections_cache = Some(projections_cache.clone());
                runner_cfg.projections_path  = Some(
                    std::path::PathBuf::from(
                        signals::player_props::DEFAULT_PROJECTIONS_PATH
                    )
                );
                runner_cfg.projections_reload_secs = 30;
                runner_cfg.event_arb_cache = Some(event_arb_cache.clone());

                // EDGE-C: Sportsbook devig — Python sidecar writes the JSON.
                if c.sportsbook_devig_enabled {
                    let cache = Arc::new(signals::sportsbook_devig::DevigCache::new());
                    runner_cfg.devig_cache       = Some(cache.clone());
                    runner_cfg.devig_path        = Some(
                        std::path::PathBuf::from(&c.sportsbook_devig_path)
                    );
                    runner_cfg.devig_reload_secs = c.sportsbook_devig_reload_secs;
                    tracing::info!(
                        path = %c.sportsbook_devig_path,
                        reload_secs = c.sportsbook_devig_reload_secs,
                        "EDGE-C sportsbook devig enabled"
                    );
                }
                // EDGE-D: Whale follow — Python sidecar appends JSONL.
                if c.whale_follow_enabled {
                    let cache = Arc::new(signals::whale_follow::WhaleCache::new());
                    runner_cfg.whale_cache       = Some(cache.clone());
                    runner_cfg.whale_path        = Some(
                        std::path::PathBuf::from(&c.whale_trades_path)
                    );
                    runner_cfg.whale_reload_secs = c.whale_reload_secs;
                    tracing::info!(
                        path = %c.whale_trades_path,
                        reload_secs = c.whale_reload_secs,
                        "EDGE-D whale follow enabled"
                    );
                }
                // Multi-strategy signal logging.
                runner_cfg.signal_log = Some(signal_log.clone());
                // G5: SigningParams replaces the pool. Built once, cloned
                // per-task by the runner. Kelly bet is honored exactly.
                let signing = Arc::new(execution::executor::SigningParams {
                    domain:          execution::orders::Domain::polymarket_polygon(),
                    private_key_hex: c.polymarket_private_key.clone(),
                    signature_type:  c.polymarket_signature_type,
                    funder_address:  c.polymarket_funder_address.clone(),
                    nonce:           0,
                    fee_rate_bps:    0,
                    taker:           [0u8; 20],
                });
                // G13: reconcile our local ledger against Polymarket BEFORE
                // spawning the runner. Catches positions that filled or
                // failed while the bot was down. Best-effort: any failure
                // is logged but doesn't block boot.
                let address_for_reconcile = signing.funder_address.clone()
                    .unwrap_or_else(|| {
                        execution::orders::private_key_to_address(&c.polymarket_private_key)
                            .map(|a| execution::orders::address_to_hex(&a))
                            .unwrap_or_default()
                    });
                if !address_for_reconcile.is_empty() {
                    match positions
                        .reconcile_with_polymarket(&client, &address_for_reconcile)
                        .await
                    {
                        Ok(r) => {
                            tracing::info!(
                                checked = r.checked,
                                filled_late = r.filled_late,
                                failed_late = r.failed_late,
                                discrepancies = r.discrepancies,
                                rpc_errors = r.rpc_errors,
                                released_usd = r.bet_dollars_to_release,
                                "G13: startup reconciliation complete"
                            );
                            // F11: release exposure for positions that
                            // transitioned to Failed in this reconcile pass.
                            // record_open had booked exposure pessimistically
                            // when the order was first submitted.
                            if r.bet_dollars_to_release > 0.0 {
                                risk_limits.release_exposure(r.bet_dollars_to_release);
                            }
                        }
                        Err(e) => tracing::warn!(error = %e,
                            "G13: reconciliation failed — continuing with local state"),
                    }
                }

                // AUTO.A + AUTO.B: maintenance tasks for autonomous running.
                let maint_cfg = execution::maintenance::MaintenanceConfig::for_owner(
                    address_for_reconcile.clone(),
                );
                tasks.spawn(execution::maintenance::order_ttl_loop(
                    client.clone(), positions.clone(), maint_cfg.clone(),
                ));
                tasks.spawn(execution::maintenance::periodic_reconcile_loop(
                    client.clone(), positions.clone(),
                    risk_limits.clone(),
                    maint_cfg,
                ));

                // EDGE-A: Liquidity rewards quoter. Off by default — flip
                // LP_QUOTER_ENABLED=true after running Phase 0 backtest.
                if c.lp_quoter_enabled {
                    let lp_cfg = Arc::new(execution::lp_quoter::LpQuoterConfig {
                        quote_size_usd:      c.lp_quote_size_usd,
                        max_markets:         c.lp_max_markets,
                        inventory_cap_usd:   c.lp_inventory_cap_usd,
                        ..execution::lp_quoter::LpQuoterConfig::default()
                    });
                    let lp_cache = Arc::new(execution::lp_quoter::LpQuoterCache::default());
                    let resolver_http = reqwest::Client::builder()
                        .timeout(std::time::Duration::from_secs(15))
                        .build()
                        .expect("reqwest client build (lp resolver)");
                    let resolver = Arc::new(move |condition_id: String| {
                        let http = resolver_http.clone();
                        Box::pin(async move {
                            resolve_condition_to_tokens(&http, &condition_id).await
                        }) as std::pin::Pin<Box<dyn std::future::Future<
                            Output = Option<(String, String)>> + Send>>
                    });
                    tasks.spawn(execution::lp_quoter::lp_discovery_loop(
                        lp_cache.clone(),
                        lp_cfg.clone(),
                        client.clone(),
                        resolver,
                        std::time::Duration::from_secs(3600),
                    ));
                    tasks.spawn(execution::lp_quoter::lp_quoter_loop(
                        lp_cache,
                        lp_cfg,
                        risk_limits.clone(),
                        signing.clone(),
                        client.clone(),
                        Some(positions.clone()),
                        std::time::Duration::from_secs(c.lp_refresh_secs),
                    ));
                    tracing::info!(
                        size_usd = c.lp_quote_size_usd,
                        max_markets = c.lp_max_markets,
                        cap_usd = c.lp_inventory_cap_usd,
                        refresh_secs = c.lp_refresh_secs,
                        "EDGE-A lp_quoter spawned"
                    );
                }

                tasks.spawn(execution::runner::execution_runner_loop(
                    state.clone(),
                    markets.clone(),
                    signing,
                    risk_limits.clone(),
                    risk_cfg,
                    sig_cfg,
                    client,
                    positions.clone(),
                    runner_cfg,
                ));
                tracing::info!(dry_run = c.dry_run, "execution_runner spawned");
            }
            Err(e) => {
                tracing::error!(error = %e, "ClobClient build failed — runner NOT spawned");
            }
        }
    }

    // Signal evaluation loop (E5) — every 500ms, picks intramarket > oracle.
    // When execution is enabled the runner is the canonical source of truth
    // for signals.jsonl (it sees ALL markets, not just the primary BTC one),
    // so we disable signal_loop's DB persist in that mode to avoid double-
    // logging the primary BTC market on every fired signal.
    let mut sig_loop_cfg = signals::SignalConfig::default();
    if let Ok(c) = &cfg_result {
        if c.execution_enabled {
            sig_loop_cfg.persist_to_db = false;
        }
    }
    tasks.spawn(signals::signal_loop(
        state.clone(),
        sig_loop_cfg,
        signal_log.clone(),
    ));

    // EDGE-E: Hibernating market scanner — observe-only (no SignalDecision
    // emitted; logs candidates for offline analysis). Independent of
    // execution_enabled because it only reads public Gamma data.
    if let Ok(cfg) = &cfg_result {
        if cfg.hibernating_enabled {
            let hib_cache = Arc::new(signals::hibernating::HibernatingCache::new());
            let scan_secs = cfg.hibernating_scan_secs.max(60);
            tasks.spawn(signals::hibernating::hibernating_scanner_loop(
                hib_cache.clone(),
                std::time::Duration::from_secs(scan_secs),
            ));
            // Periodic logger: dump candidates to signals.jsonl for
            // post-hoc analysis. Same cadence as scan. Dedup by
            // (condition_id, side) so a market sitting in cache for
            // hours doesn't get logged on every tick.
            let log = signal_log.clone();
            let seen: Arc<std::sync::Mutex<std::collections::HashSet<String>>> =
                Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
            tokio::spawn(async move {
                let mut t = tokio::time::interval(
                    std::time::Duration::from_secs(scan_secs)
                );
                t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    t.tick().await;
                    let candidates = hib_cache.snapshot().await;
                    for c in candidates {
                        let dir = match c.side_to_take {
                            signals::hibernating::SideToTake::Yes => Some("up"),
                            signals::hibernating::SideToTake::No  => Some("down"),
                        };
                        let key = format!("{}|{}",
                            c.condition_id, c.side_to_take.as_str());
                        {
                            let mut g = seen.lock().expect("poisoned");
                            if g.contains(&key) { continue; }
                            // 10k entries ~= 14 days at typical scan rates;
                            // full-clear is fine — re-logging on day 14+ is
                            // desirable for forward test
                            if g.len() >= 10_000 { g.clear(); }
                            g.insert(key);
                        }
                        let market_price = if dir == Some("up") { c.yes_ask }
                                           else { (1.0 - c.yes_bid).clamp(0.01, 0.99) };
                        let row = storage::SignalRow::for_strategy(
                            "hibernating", &c.condition_id, "oracle", dir,
                            // fair = "near certain" anchor: 0.97 for YES side,
                            // 0.03 for NO side (we don't have a true model)
                            if dir == Some("up") { 0.97 } else { 0.97 },
                            market_price,
                            (0.97 - market_price).max(0.0),
                            0.5,
                            0.0,        // observe-only: no bet sized
                            (c.end_date_ms.max(0) as f64
                                - chrono::Utc::now().timestamp_millis() as f64) / 1000.0,
                        );
                        let _ = log.insert_signal(&row);
                    }
                }
            });
            tracing::info!(scan_secs, "EDGE-E hibernating scanner spawned");
        }
    }

    // Settlement monitor — when markets resolve, append a SettlementRow and
    // reconcile every open Position for that market with realised P&L
    // (parallel via rayon::par_iter inside settle_positions_for_market).
    tasks.spawn(storage::settlement::settlement_monitor_loop(
        markets.clone(),
        state.clone(),
        signal_log.clone(),
        Some(positions.clone()),
    ));

    // IPC bridge with Python ML layer (G2 + G3)
    let py_signal: Arc<tokio::sync::RwLock<Option<ipc::PythonSignal>>> =
        Arc::new(tokio::sync::RwLock::new(None));
    {
        let st = state.clone();
        tasks.spawn(async move {
            if let Err(e) = ipc::features_writer_loop(st, ipc::default_features_path()).await {
                tracing::error!(error = %e, "features writer loop crashed");
            }
        });
    }
    tasks.spawn(ipc::signal_reader_loop(py_signal.clone(), ipc::default_signal_path()));

    tracing::info!("all tasks started — streams + features + signals + IPC bridge");

    // Signal, execution, IPC tasks join in later phases.
    tasks.join_all().await;
    Ok(())
}
