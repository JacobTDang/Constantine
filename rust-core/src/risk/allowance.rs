// Concern G2 — USDC allowance runtime watchdog.
//
// Polymarket's CTF Exchange spends USDC on our behalf when filling
// orders. Without an active `approve()` allowance, every order rejects.
// The user sets allowance once via the Polymarket UI, but it can drain
// over time as fills consume it.
//
// This watchdog polls the USDC contract's `allowance(owner, spender)`
// every N minutes and trips the kill switch if balance falls below a
// threshold. Catches the failure mode early — before all our pool slots
// burn on rejected orders.
//
// Why no ethers/web3 dep: the call is a single read of one 32-byte word.
// We hand-roll the JSON-RPC + ABI encoding (~30 lines) rather than pull
// in a 50k-line wallet library.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};
use tokio::time::{interval, MissedTickBehavior};

use crate::risk::limits::RiskLimits;

// Polygon mainnet contract addresses (mirrors py-clob-client::config.py)
pub const USDC_POLYGON:        &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";
pub const CTF_EXCHANGE_POLYGON: &str = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E";

/// Function selector for `allowance(address,address)`. First 4 bytes of
/// keccak256("allowance(address,address)") = 0xdd62ed3e.
const ALLOWANCE_SELECTOR_HEX: &str = "dd62ed3e";

/// USDC has 6 decimals on Polygon.
const USDC_DECIMALS: u32 = 6;

#[derive(Debug, Clone)]
pub struct AllowanceWatchdogConfig {
    pub rpc_url:            String,
    pub owner_address:      String,    // 0x-prefixed wallet (or proxy) address
    pub spender_address:    String,    // CTF exchange or NegRisk exchange
    pub min_allowance_usdc: f64,       // trip below this (e.g. $100)
    pub poll_interval_secs: u64,
    /// If true, trip the kill switch when allowance is below threshold.
    pub trip_kill_switch:   bool,
}

impl AllowanceWatchdogConfig {
    pub fn for_polygon_eoa(rpc_url: impl Into<String>, owner: impl Into<String>) -> Self {
        Self {
            rpc_url:            rpc_url.into(),
            owner_address:      owner.into(),
            spender_address:    CTF_EXCHANGE_POLYGON.to_string(),
            min_allowance_usdc: 100.0,
            poll_interval_secs: 300,    // 5 minutes
            trip_kill_switch:   true,
        }
    }
}

/// One allowance reading.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AllowanceReading {
    pub raw_wei:      u128,
    pub usdc_dollars: f64,
}

impl AllowanceReading {
    pub fn from_wei(raw_wei: u128) -> Self {
        Self {
            raw_wei,
            usdc_dollars: raw_wei as f64 / 10f64.powi(USDC_DECIMALS as i32),
        }
    }
}

// ── ABI encoding ─────────────────────────────────────────────────────────────

/// Build the eth_call data field for `allowance(owner, spender)`.
/// Returns 0x-prefixed hex: selector(4) || owner_padded(32) || spender_padded(32) = 68 bytes
pub fn build_allowance_calldata(owner: &str, spender: &str) -> Result<String> {
    let owner_bytes = parse_address(owner)?;
    let spender_bytes = parse_address(spender)?;
    let mut buf = Vec::with_capacity(4 + 32 + 32);
    buf.extend_from_slice(&hex::decode(ALLOWANCE_SELECTOR_HEX).expect("static hex"));
    // EVM addresses are 20 bytes, left-padded to 32 in calldata
    buf.extend_from_slice(&[0u8; 12]);
    buf.extend_from_slice(&owner_bytes);
    buf.extend_from_slice(&[0u8; 12]);
    buf.extend_from_slice(&spender_bytes);
    Ok(format!("0x{}", hex::encode(&buf)))
}

fn parse_address(s: &str) -> Result<[u8; 20]> {
    let stripped = s.strip_prefix("0x").unwrap_or(s);
    let bytes = hex::decode(stripped).context("address hex")?;
    if bytes.len() != 20 {
        return Err(anyhow!("expected 20-byte address, got {}", bytes.len()));
    }
    let mut out = [0u8; 20];
    out.copy_from_slice(&bytes);
    Ok(out)
}

/// Decode a 32-byte response from `allowance()` to u128 (USDC fits easily —
/// even max u96 covers more USDC than has been minted).
///
/// Input is the eth_call response: 0x-prefixed hex of 32 bytes (big-endian uint256).
pub fn decode_allowance_response(hex_resp: &str) -> Result<u128> {
    let stripped = hex_resp.strip_prefix("0x").unwrap_or(hex_resp);
    let bytes = hex::decode(stripped).context("response hex")?;
    if bytes.len() != 32 {
        return Err(anyhow!("expected 32-byte response, got {}", bytes.len()));
    }
    // High 16 bytes must be zero for fit-in-u128. USDC supply is ~$30B,
    // far below 2^96 — but we check anyway to fail loudly on uint256 overflow.
    if bytes[..16].iter().any(|&b| b != 0) {
        return Err(anyhow!("allowance value exceeds u128 — should never happen for USDC"));
    }
    let mut out = [0u8; 16];
    out.copy_from_slice(&bytes[16..]);
    Ok(u128::from_be_bytes(out))
}

// ── RPC call ─────────────────────────────────────────────────────────────────

/// Query the USDC allowance via JSON-RPC `eth_call`.
pub async fn query_allowance(
    cfg: &AllowanceWatchdogConfig,
    http: &reqwest::Client,
) -> Result<AllowanceReading> {
    let calldata = build_allowance_calldata(&cfg.owner_address, &cfg.spender_address)?;

    let body = json!({
        "jsonrpc": "2.0",
        "id":      1,
        "method":  "eth_call",
        "params": [
            { "to": USDC_POLYGON, "data": calldata },
            "latest"
        ],
    });

    let resp = http.post(&cfg.rpc_url)
        .json(&body)
        .send().await
        .context("RPC POST failed")?;
    let status = resp.status();
    let text = resp.text().await.context("RPC body read")?;
    if !status.is_success() {
        return Err(anyhow!("RPC HTTP {}: {}", status, truncate(&text, 200)));
    }
    let v: Value = serde_json::from_str(&text)
        .with_context(|| format!("parse RPC response: {}", truncate(&text, 200)))?;
    if let Some(err) = v.get("error") {
        return Err(anyhow!("RPC error: {}", err));
    }
    let result_hex = v.get("result")
        .and_then(|x| x.as_str())
        .ok_or_else(|| anyhow!("RPC result missing or not a string: {}", truncate(&text, 200)))?;
    let raw_wei = decode_allowance_response(result_hex)?;
    Ok(AllowanceReading::from_wei(raw_wei))
}

// ── Watchdog loop ────────────────────────────────────────────────────────────

/// Long-running task: poll allowance, trip kill switch on shortage.
pub async fn allowance_watchdog_loop(
    cfg:  AllowanceWatchdogConfig,
    risk: Arc<RiskLimits>,
) {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("reqwest client");
    let mut tick = interval(Duration::from_secs(cfg.poll_interval_secs.max(60)));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    tracing::info!(
        owner   = %cfg.owner_address,
        spender = %cfg.spender_address,
        interval_secs = cfg.poll_interval_secs,
        min_usdc = cfg.min_allowance_usdc,
        "allowance watchdog started"
    );

    loop {
        tick.tick().await;
        match query_allowance(&cfg, &http).await {
            Ok(reading) => {
                if reading.usdc_dollars < cfg.min_allowance_usdc {
                    tracing::error!(
                        usdc = reading.usdc_dollars,
                        min  = cfg.min_allowance_usdc,
                        "USDC ALLOWANCE LOW — re-approve before further trading"
                    );
                    if cfg.trip_kill_switch && !risk.is_kill_switch_active() {
                        risk.activate_kill_switch();
                    }
                } else {
                    tracing::info!(
                        usdc = reading.usdc_dollars,
                        "allowance OK"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "allowance query failed");
                // Don't trip on a single RPC failure — could be a hiccup.
            }
        }
    }
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n { s.to_string() } else { format!("{}…", &s[..n.saturating_sub(1)]) }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── ABI encoding ─────────────────────────────────────────────────────

    #[test]
    fn calldata_has_selector_and_two_padded_addresses() {
        let cd = build_allowance_calldata(
            "0x1111111111111111111111111111111111111111",
            "0x2222222222222222222222222222222222222222",
        ).unwrap();
        // 0x + selector(8) + owner_padded(64) + spender_padded(64) = 0x + 136 hex
        assert_eq!(cd.len(), 2 + 4 * 2 + 32 * 2 + 32 * 2);
        assert!(cd.starts_with("0xdd62ed3e"));
        // Owner padding is 24 hex zeros (12 bytes)
        assert!(cd.contains("0000000000000000000000001111111111111111111111111111111111111111"));
        assert!(cd.contains("0000000000000000000000002222222222222222222222222222222222222222"));
    }

    #[test]
    fn calldata_strips_0x_or_handles_unprefixed() {
        let with_prefix = build_allowance_calldata(
            "0x1111111111111111111111111111111111111111",
            "0x2222222222222222222222222222222222222222",
        ).unwrap();
        let without = build_allowance_calldata(
            "1111111111111111111111111111111111111111",
            "2222222222222222222222222222222222222222",
        ).unwrap();
        assert_eq!(with_prefix, without);
    }

    #[test]
    fn calldata_rejects_malformed_address() {
        assert!(build_allowance_calldata("0xnotvalid", "0x2222222222222222222222222222222222222222").is_err());
        assert!(build_allowance_calldata("0x11", "0x2222222222222222222222222222222222222222").is_err());
    }

    // ── Response decoding ─────────────────────────────────────────────────

    #[test]
    fn decode_zero_is_zero() {
        let resp = format!("0x{}", "0".repeat(64));
        assert_eq!(decode_allowance_response(&resp).unwrap(), 0);
    }

    #[test]
    fn decode_one_dollar_usdc() {
        // 1 USDC = 10^6 raw = 0xf4240 (= 3 bytes). In a 32-byte big-endian
        // word the value sits in bytes 29..32 (0-indexed).
        let mut padded = [0u8; 32];
        padded[29] = 0x0f;
        padded[30] = 0x42;
        padded[31] = 0x40;
        let resp = format!("0x{}", hex::encode(padded));
        let n = decode_allowance_response(&resp).unwrap();
        assert_eq!(n, 1_000_000);
        assert!((AllowanceReading::from_wei(n).usdc_dollars - 1.0).abs() < 1e-9);
    }

    #[test]
    fn decode_max_uint128_works() {
        // High 16 bytes zero, low 16 bytes all 1s → u128::MAX
        let mut padded = [0u8; 32];
        for b in padded[16..].iter_mut() { *b = 0xff; }
        let resp = format!("0x{}", hex::encode(padded));
        assert_eq!(decode_allowance_response(&resp).unwrap(), u128::MAX);
    }

    #[test]
    fn decode_rejects_overflow_to_uint128() {
        // Set high byte non-zero — would overflow u128
        let mut padded = [0u8; 32];
        padded[0] = 0x01;
        let resp = format!("0x{}", hex::encode(padded));
        assert!(decode_allowance_response(&resp).is_err());
    }

    #[test]
    fn decode_rejects_short_response() {
        assert!(decode_allowance_response("0x1234").is_err());
    }

    // ── Reading → dollars ─────────────────────────────────────────────────

    #[test]
    fn reading_converts_to_dollars_at_6_decimals() {
        let r = AllowanceReading::from_wei(150_000_000);   // 150 USDC
        assert!((r.usdc_dollars - 150.0).abs() < 1e-9);
    }

    // ── End-to-end via mock RPC ──────────────────────────────────────────

    #[tokio::test]
    async fn query_allowance_against_mock_rpc() {
        // Start a tiny mock RPC that returns 1000 USDC.
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // 1000 USDC = 1_000_000_000 raw = 0x3b9aca00
        let response_body = r#"{"jsonrpc":"2.0","id":1,"result":"0x000000000000000000000000000000000000000000000000000000003b9aca00"}"#;
        let response_owned = response_body.to_string();
        tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = [0u8; 8192];
                let _ = sock.read(&mut buf).await;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
                    response_owned.len(), response_owned,
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            }
        });

        let cfg = AllowanceWatchdogConfig {
            rpc_url: format!("http://{}", addr),
            owner_address:      "0x1111111111111111111111111111111111111111".into(),
            spender_address:    CTF_EXCHANGE_POLYGON.to_string(),
            min_allowance_usdc: 100.0,
            poll_interval_secs: 60,
            trip_kill_switch:   true,
        };
        let http = reqwest::Client::new();
        let reading = query_allowance(&cfg, &http).await.unwrap();
        assert_eq!(reading.raw_wei, 1_000_000_000);
        assert!((reading.usdc_dollars - 1000.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn query_allowance_handles_rpc_error_payload() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let response_body = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"oops"}}"#;
        let response_owned = response_body.to_string();
        tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = [0u8; 8192];
                let _ = sock.read(&mut buf).await;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
                    response_owned.len(), response_owned,
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            }
        });

        let cfg = AllowanceWatchdogConfig::for_polygon_eoa(
            format!("http://{}", addr),
            "0x1111111111111111111111111111111111111111",
        );
        let http = reqwest::Client::new();
        let result = query_allowance(&cfg, &http).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("RPC error"));
    }
}
