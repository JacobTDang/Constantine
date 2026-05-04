// polymarket-bot library — shared code for the main bot binary, the
// dashboard binary, and any future utility binaries (testnet drills,
// reconciliation tools, etc.).

pub mod config;
pub mod execution;
pub mod features;
pub mod ipc;
pub mod logging;
pub mod risk;
pub mod signals;
pub mod storage;
pub mod streams;

pub use config::Config;
