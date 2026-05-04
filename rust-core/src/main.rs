mod config;
mod execution;
mod features;
mod ipc;
mod logging;
mod risk;
mod signals;
mod streams;

use std::sync::Arc;

pub use config::Config;
use features::FeatureState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    let log_level = std::env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string());
    logging::init(&log_level);

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

    tracing::info!("all tasks started — streams + features + market discovery + chainlink + clob");

    // Signal, execution, IPC tasks join in later phases.
    tasks.join_all().await;
    Ok(())
}
