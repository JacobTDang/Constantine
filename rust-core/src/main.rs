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

    let mut tasks = tokio::task::JoinSet::new();

    // Data streams (B1-B6)
    tasks.spawn(streams::binance::spot_stream(tx.clone()));
    tasks.spawn(streams::binance::perp_stream(tx.clone()));
    tasks.spawn(streams::binance::oi_stream(tx.clone()));
    tasks.spawn(streams::binance::kline_stream(tx.clone()));
    tasks.spawn(streams::liquidations::liq_stream(tx.clone()));

    // Feature compute loop (C11)
    tasks.spawn(features::compute_loop(tx.subscribe(), state.clone()));

    tracing::info!("all tasks started — streams + feature compute loop active");

    // Signal, execution, IPC tasks join in later phases.
    tasks.join_all().await;
    Ok(())
}
