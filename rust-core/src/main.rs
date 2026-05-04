use std::sync::Arc;

use polymarket_bot::{
    config::Config,
    execution, features, ipc, logging, risk, signals, storage, streams,
};
use polymarket_bot::features::FeatureState;

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

    // S7.4 — Heartbeat: 30s status log so ops can grep "HB".
    tasks.spawn(risk::heartbeat::heartbeat_loop(
        state.clone(), risk_limits.clone(), positions.clone(), 30,
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
                };
                let runner_cfg = execution::runner::RunnerConfig::default();
                tasks.spawn(execution::runner::execution_runner_loop(
                    state.clone(),
                    markets.clone(),
                    order_pool.clone(),
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

    // Signal evaluation loop (E5) — every 500ms, picks intramarket > oracle
    tasks.spawn(signals::signal_loop(
        state.clone(),
        signals::SignalConfig::default(),
        signal_log.clone(),
    ));

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
