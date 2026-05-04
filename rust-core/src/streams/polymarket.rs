// D1: Polymarket BTC market discovery via the Gamma REST API.
//
// Active "Bitcoin Up or Down" markets fall into 5-min and 15-min windows.
// Question format example:
//   "Bitcoin Up or Down - May 4, 10:55PM-11:00PM ET"   (5-min)
//   "Bitcoin Up or Down - May 4, 10:45PM-11:00PM ET"   (15-min)
//
// `clobTokenIds` is a JSON-encoded array — index 0 = "Up", index 1 = "Down".
// For our internal model:
//   yes (Up)   token  → bet that BTC closes higher than window-open chainlink price
//   no  (Down) token  → bet that BTC closes equal-or-lower
//
// The `window_open_price` (strike) is NOT in the question — it's the chainlink
// price captured at window open, fed in via the RTDS stream (D2). D1 only
// surfaces the markets and their close times.

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tokio::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use super::StreamEvent;

const GAMMA_URL: &str =
    "https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=100&order=startDate&ascending=false";

// G3: 30s — was 300s. For 5-MINUTE markets, 5-minute discovery cadence
// meant we'd miss the first 60-90 seconds of every market's life,
// including the high-edge fresh-window window.
const POLL_INTERVAL_SECS: u64 = 30;
const REQUEST_TIMEOUT_SECS: u64 = 10;

// ── Public type ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct PolyMarket {
    pub id:             String,
    pub question:       String,
    pub up_token_id:    String, // clobTokenIds[0] — "Up" outcome
    pub down_token_id:  String, // clobTokenIds[1] — "Down" outcome
    pub close_time_ms:  u64,    // window resolution (endDate)
    pub duration_min:   u32,    // 5 or 15
    pub liquidity_usd:  f64,
}

impl PolyMarket {
    /// Window open time = close_time - duration. The chainlink price captured
    /// at this instant becomes the "strike" for oracle arb.
    pub fn open_time_ms(&self) -> u64 {
        self.close_time_ms
            .saturating_sub(self.duration_min as u64 * 60_000)
    }

    /// Seconds remaining until window resolves. Clamped to 0 when in the past.
    pub fn time_to_close_secs(&self, now_ms: u64) -> f64 {
        if now_ms >= self.close_time_ms {
            0.0
        } else {
            (self.close_time_ms - now_ms) as f64 / 1000.0
        }
    }
}

// ── Public API ───────────────────────────────────────────────────────────────

/// Hit Gamma API and return all currently-active BTC Up-or-Down markets.
pub async fn discover_btc_markets() -> Result<Vec<PolyMarket>> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
        .build()?;
    let body = client.get(GAMMA_URL).send().await?.text().await?;
    parse_btc_markets(&body)
}

/// Background task: refresh the shared market list every 5 minutes.
/// Logs counts and the closest-to-resolving market on every tick.
pub async fn market_discovery_loop(shared: Arc<RwLock<Vec<PolyMarket>>>) {
    loop {
        match discover_btc_markets().await {
            Ok(markets) => {
                let n = markets.len();
                let next_close = markets.iter().map(|m| m.close_time_ms).min();
                {
                    let mut w = shared.write().await;
                    *w = markets;
                }
                tracing::info!(
                    btc_markets   = n,
                    next_close_ms = ?next_close,
                    "polymarket discovery refreshed"
                );
            }
            Err(e) => {
                tracing::error!(error = %e, "polymarket discovery failed");
            }
        }
        tokio::time::sleep(Duration::from_secs(POLL_INTERVAL_SECS)).await;
    }
}

// ── Parsing ──────────────────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct GammaMarket {
    #[serde(default)]
    id: serde_json::Value,
    #[serde(default)]
    question: Option<String>,
    #[serde(rename = "endDate", default)]
    end_date: Option<String>,
    #[serde(rename = "clobTokenIds", default)]
    clob_token_ids: Option<String>,
    /// Gamma sometimes returns `0` (number) and sometimes `"0"` (string).
    #[serde(default)]
    liquidity: Option<serde_json::Value>,
}

fn coerce_f64(v: &serde_json::Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

fn parse_btc_markets(body: &str) -> Result<Vec<PolyMarket>> {
    let raws: Vec<GammaMarket> = serde_json::from_str(body)
        .context("gamma response was not a JSON array of markets")?;
    Ok(raws.into_iter().filter_map(parse_one).collect())
}

fn parse_one(raw: GammaMarket) -> Option<PolyMarket> {
    let question = raw.question?;
    if !question.starts_with("Bitcoin Up or Down") {
        return None;
    }
    let duration_min = parse_window_duration_min(&question)?;

    let end_iso = raw.end_date?;
    let close_time_ms = iso_to_ms(&end_iso)?;

    let raw_clob = raw.clob_token_ids?;
    let tokens: Vec<String> = serde_json::from_str(&raw_clob).ok()?;
    if tokens.len() != 2 {
        return None;
    }

    let id = match raw.id {
        serde_json::Value::String(s) => s,
        serde_json::Value::Number(n) => n.to_string(),
        _ => return None,
    };

    Some(PolyMarket {
        id,
        question,
        up_token_id:   tokens[0].clone(),
        down_token_id: tokens[1].clone(),
        close_time_ms,
        duration_min,
        liquidity_usd: raw.liquidity.as_ref().and_then(coerce_f64).unwrap_or(0.0),
    })
}

fn iso_to_ms(iso: &str) -> Option<u64> {
    let dt = DateTime::parse_from_rfc3339(iso).ok()?;
    let ms = dt.with_timezone(&Utc).timestamp_millis();
    if ms < 0 { None } else { Some(ms as u64) }
}

/// Parse "Bitcoin Up or Down - May 4, 10:55PM-11:00PM ET" → 5
/// Returns minutes between the two clock times.
fn parse_window_duration_min(question: &str) -> Option<u32> {
    // Trim the trailing " ET"
    let q = question
        .trim_end()
        .trim_end_matches("ET")
        .trim_end_matches("et")
        .trim_end();

    // Time range is the last whitespace-separated token: "10:55PM-11:00PM"
    let range = q.split_whitespace().last()?;
    let dash_pos = range.find('-')?;
    let start = parse_clock_min(&range[..dash_pos])?;
    let end   = parse_clock_min(&range[dash_pos + 1..])?;

    let diff = if end >= start { end - start } else { end + 24 * 60 - start };
    if diff == 0 || diff > 60 { None } else { Some(diff) }
}

fn parse_clock_min(s: &str) -> Option<u32> {
    let s = s.trim();
    let upper = s.to_ascii_uppercase();
    let (time_part, am_pm) = if let Some(t) = upper.strip_suffix("AM") {
        (t, false)
    } else if let Some(t) = upper.strip_suffix("PM") {
        (t, true)
    } else {
        return None;
    };
    let time_part = time_part.trim();
    let colon = time_part.find(':')?;
    let h: u32 = time_part[..colon].parse().ok()?;
    let m: u32 = time_part[colon + 1..].parse().ok()?;
    if h > 12 || m > 59 { return None; }

    let h24 = match (h, am_pm) {
        (12, false) => 0,       // 12 AM = midnight
        (12, true)  => 12,      // 12 PM = noon
        (h,  false) => h,       // 1-11 AM
        (h,  true)  => h + 12,  // 1-11 PM
    };
    Some(h24 * 60 + m)
}

// Convenience for callers that want a one-shot fetch + bubble parsing errors.
#[allow(dead_code)]
pub async fn discover_btc_markets_or_err() -> Result<Vec<PolyMarket>> {
    let markets = discover_btc_markets().await?;
    if markets.is_empty() {
        return Err(anyhow!("no BTC Up-or-Down markets currently active"));
    }
    Ok(markets)
}

// ── D2: Chainlink BTC/USD on-chain polling ───────────────────────────────────
//
// Replaces the original Polymarket RTDS WebSocket plan — the RTDS endpoint
// stopped emitting reliable updates after the initial snapshot.
//
// Chainlink BTC/USD aggregator on Polygon mainnet is the same on-chain feed
// that Polymarket uses for resolution. Polling it directly is more reliable
// and authoritative, with no API keys required (free public RPCs work).

const CHAINLINK_BTC_USD_POLYGON:    &str = "0xc907E116054Ad103354f2D350FD2514433D57F6f";
const LATEST_ROUND_DATA_SELECTOR:   &str = "0xfeaf968c"; // latestRoundData()
const CHAINLINK_POLL_INTERVAL_SECS: u64  = 5;

const POLYGON_RPC_URLS: &[&str] = &[
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon.drpc.org",
    "https://1rpc.io/matic",
];

/// Background task: poll Chainlink BTC/USD every 5s, emit ChainlinkPrice event.
/// Rotates through public RPC endpoints on failure.
pub async fn chainlink_polling_loop(tx: broadcast::Sender<StreamEvent>) {
    let mut url_idx = 0usize;
    loop {
        let url = POLYGON_RPC_URLS[url_idx % POLYGON_RPC_URLS.len()];
        match fetch_chainlink_btc(url).await {
            Ok(event) => {
                if let StreamEvent::ChainlinkPrice { price, .. } = &event {
                    tracing::debug!(rpc = url, btc_usd = price, "chainlink update");
                }
                let _ = tx.send(event);
            }
            Err(e) => {
                tracing::warn!(rpc = url, error = %e, "chainlink poll failed, rotating RPC");
                url_idx = url_idx.wrapping_add(1);
            }
        }
        tokio::time::sleep(Duration::from_secs(CHAINLINK_POLL_INTERVAL_SECS)).await;
    }
}

#[derive(serde::Deserialize)]
struct JsonRpcResp {
    result: Option<String>,
    error:  Option<serde_json::Value>,
}

async fn fetch_chainlink_btc(rpc_url: &str) -> Result<StreamEvent> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(8))
        .build()?;

    let req = serde_json::json!({
        "jsonrpc": "2.0",
        "method":  "eth_call",
        "params":  [
            {"to": CHAINLINK_BTC_USD_POLYGON, "data": LATEST_ROUND_DATA_SELECTOR},
            "latest"
        ],
        "id": 1
    });

    let resp: JsonRpcResp = client.post(rpc_url).json(&req).send().await?.json().await?;

    if let Some(err) = resp.error {
        return Err(anyhow!("rpc error: {err}"));
    }
    let result = resp.result.ok_or_else(|| anyhow!("rpc response missing result"))?;
    let (price, ts_secs) = decode_latest_round_data(&result)
        .ok_or_else(|| anyhow!("failed to decode chainlink response: {result}"))?;

    Ok(StreamEvent::ChainlinkPrice {
        price,
        timestamp_ms: ts_secs * 1000,
    })
}

// ── D4: Polymarket CLOB order-book stream ────────────────────────────────────

const CLOB_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const CLOB_BACKOFF_INIT_SECS: u64 = 1;
const CLOB_BACKOFF_MAX_SECS:  u64 = 30;

/// Subscribe to one CLOB connection for the given asset IDs (mix of Up/Down
/// tokens across multiple BTC markets) and emit ClobBook events.
///
/// Reconnects with exponential backoff. On reconnect, resubscribes.
pub async fn clob_stream(tx: broadcast::Sender<StreamEvent>, asset_ids: Vec<String>) {
    if asset_ids.is_empty() {
        tracing::warn!("clob_stream called with no asset_ids — exiting");
        return;
    }
    let subscribe_msg = serde_json::json!({
        "type": "MARKET",
        "assets_ids": asset_ids,
    })
    .to_string();

    let mut backoff = Duration::from_secs(CLOB_BACKOFF_INIT_SECS);
    loop {
        tracing::info!(n = asset_ids.len(), "connecting to polymarket CLOB");
        match run_clob(&subscribe_msg, &tx).await {
            Ok(()) => {
                tracing::warn!("clob stream closed cleanly, reconnecting");
                backoff = Duration::from_secs(CLOB_BACKOFF_INIT_SECS);
            }
            Err(e) => {
                tracing::error!(error = %e, retry_secs = backoff.as_secs(), "clob error");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(CLOB_BACKOFF_MAX_SECS));
            }
        }
    }
}

async fn run_clob(subscribe_msg: &str, tx: &broadcast::Sender<StreamEvent>) -> Result<()> {
    let (ws, _) = connect_async(CLOB_WS_URL).await?;
    tracing::info!("clob connected");
    let (mut write, mut read) = ws.split();
    write
        .send(Message::Text(subscribe_msg.to_string()))
        .await?;

    while let Some(msg) = read.next().await {
        match msg? {
            Message::Text(text) => {
                if text.trim().is_empty() {
                    continue; // CLOB occasionally sends empty keepalives
                }
                for event in parse_clob_payload(&text) {
                    let _ = tx.send(event);
                }
            }
            Message::Close(_) => {
                tracing::warn!("clob server closed");
                break;
            }
            Message::Ping(p) => write.send(Message::Pong(p)).await?,
            _ => {}
        }
    }
    Ok(())
}

/// CLOB messages are JSON arrays of book/price-change events, OR a single
/// JSON object. We accept either shape and emit ClobBook events for "book"
/// snapshots. price_change handling is deferred — book snapshots are
/// frequent enough for current trading logic.
pub fn parse_clob_payload(text: &str) -> Vec<StreamEvent> {
    let v: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return vec![],
    };

    let items: Vec<&serde_json::Value> = match &v {
        serde_json::Value::Array(arr) => arr.iter().collect(),
        serde_json::Value::Object(_)  => vec![&v],
        _ => return vec![],
    };

    let mut out = Vec::with_capacity(items.len());
    for item in items {
        if let Some(ev) = parse_book_event(item) {
            out.push(ev);
        }
    }
    out
}

fn parse_book_event(v: &serde_json::Value) -> Option<StreamEvent> {
    if v.get("event_type")?.as_str()? != "book" {
        return None;
    }
    let market_id = v.get("market")?.as_str()?.to_string();
    let asset_id  = v.get("asset_id")?.as_str()?.to_string();
    let timestamp_ms: u64 = v.get("timestamp")?.as_str()?.parse().ok()?;

    let bids = v.get("bids")?.as_array()?;
    let asks = v.get("asks")?.as_array()?;

    let (best_bid, best_bid_size) = best_level(bids, BookSide::Bid);
    let (best_ask, best_ask_size) = best_level(asks, BookSide::Ask);

    if best_bid <= 0.0 || best_ask <= 0.0 || best_bid >= 1.0 || best_ask >= 1.0 {
        return None;
    }

    let last_trade_price = v
        .get("last_trade_price")
        .and_then(|x| x.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|p| p.is_finite() && *p > 0.0)
        .unwrap_or(0.0);

    Some(StreamEvent::ClobBook {
        market_id,
        asset_id,
        best_bid,
        best_ask,
        best_bid_size,
        best_ask_size,
        last_trade_price,
        timestamp_ms,
    })
}

#[derive(Copy, Clone)]
enum BookSide { Bid, Ask }

/// Find the best price (max for bids, min for asks) among levels, plus the
/// size at that price level.
fn best_level(levels: &[serde_json::Value], side: BookSide) -> (f64, f64) {
    let mut best_price = match side {
        BookSide::Bid => f64::NEG_INFINITY,
        BookSide::Ask => f64::INFINITY,
    };
    let mut best_size = 0.0_f64;
    for lvl in levels {
        let p: Option<f64> = lvl.get("price").and_then(|x| x.as_str()).and_then(|s| s.parse().ok());
        let s: Option<f64> = lvl.get("size").and_then(|x| x.as_str()).and_then(|s| s.parse().ok());
        if let (Some(p), Some(s)) = (p, s) {
            if !p.is_finite() || !s.is_finite() || s <= 0.0 { continue; }
            let better = match side {
                BookSide::Bid => p > best_price,
                BookSide::Ask => p < best_price,
            };
            if better {
                best_price = p;
                best_size  = s;
            }
        }
    }
    if best_price.is_finite() {
        (best_price, best_size)
    } else {
        (0.0, 0.0)
    }
}

// ── D6: CLOB orchestrator ────────────────────────────────────────────────────

/// Watches the shared markets list and (re)spawns clob_stream whenever the
/// set of asset IDs changes. Polls every 60 seconds.
///
/// This is what keeps the CLOB connection in sync with D1's market discovery
/// — when new windows open and old ones resolve, the asset_ids change and
/// we restart the CLOB stream.
pub async fn clob_orchestrator(
    markets: Arc<RwLock<Vec<PolyMarket>>>,
    tx:      broadcast::Sender<StreamEvent>,
) {
    let mut last_ids:  Vec<String> = Vec::new();
    let mut current:   Option<tokio::task::JoinHandle<()>> = None;

    loop {
        let mut ids: Vec<String> = markets
            .read()
            .await
            .iter()
            .flat_map(|m| [m.up_token_id.clone(), m.down_token_id.clone()])
            .collect();
        ids.sort();
        ids.dedup();

        if ids != last_ids {
            if let Some(h) = current.take() {
                h.abort();
            }
            if !ids.is_empty() {
                let tx = tx.clone();
                let new_ids = ids.clone();
                current = Some(tokio::spawn(clob_stream(tx, new_ids)));
                tracing::info!(n = ids.len(), "clob stream restarted with new asset list");
            }
            last_ids = ids;
        }
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}

/// Decode `latestRoundData()` ABI-encoded response.
/// Layout (5 × 32-byte values): [roundId, answer, startedAt, updatedAt, answeredInRound]
/// Returns (price_usd, updated_at_unix_secs).
fn decode_latest_round_data(hex_result: &str) -> Option<(f64, u64)> {
    let hex = hex_result.strip_prefix("0x").unwrap_or(hex_result);
    if hex.len() < 256 {
        return None;
    }
    // answer is the second 32-byte chunk
    let answer = u128::from_str_radix(&hex[64..128], 16).ok()?;
    // updatedAt is the fourth 32-byte chunk
    let updated_at = u64::from_str_radix(&hex[192..256], 16).ok()?;

    // Polygon BTC/USD feed: 8 decimals
    let price = answer as f64 / 1e8;
    if !price.is_finite() || price <= 0.0 {
        return None;
    }
    Some((price, updated_at))
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── parse_clock_min ───────────────────────────────────────────────────

    #[test]
    fn clock_min_midnight_is_zero() {
        assert_eq!(parse_clock_min("12:00AM"), Some(0));
    }

    #[test]
    fn clock_min_noon_is_720() {
        assert_eq!(parse_clock_min("12:00PM"), Some(720));
    }

    #[test]
    fn clock_min_1pm_is_780() {
        assert_eq!(parse_clock_min("1:00PM"), Some(780));
    }

    #[test]
    fn clock_min_11_55pm() {
        assert_eq!(parse_clock_min("11:55PM"), Some(23 * 60 + 55));
    }

    #[test]
    fn clock_min_invalid_returns_none() {
        assert!(parse_clock_min("25:00PM").is_none());
        assert!(parse_clock_min("12:60AM").is_none());
        assert!(parse_clock_min("not a time").is_none());
        assert!(parse_clock_min("").is_none());
    }

    // ── parse_window_duration_min ─────────────────────────────────────────

    #[test]
    fn duration_5min_simple() {
        let q = "Bitcoin Up or Down - May 4, 10:55PM-11:00PM ET";
        assert_eq!(parse_window_duration_min(q), Some(5));
    }

    #[test]
    fn duration_15min() {
        let q = "Bitcoin Up or Down - May 4, 10:45PM-11:00PM ET";
        assert_eq!(parse_window_duration_min(q), Some(15));
    }

    #[test]
    fn duration_across_noon_boundary() {
        let q = "Bitcoin Up or Down - May 4, 11:55AM-12:00PM ET";
        assert_eq!(parse_window_duration_min(q), Some(5));
    }

    #[test]
    fn duration_across_midnight_boundary() {
        let q = "Bitcoin Up or Down - May 4, 11:55PM-12:00AM ET";
        assert_eq!(parse_window_duration_min(q), Some(5));
    }

    #[test]
    fn duration_garbage_returns_none() {
        assert!(parse_window_duration_min("Bitcoin Up or Down").is_none());
        assert!(parse_window_duration_min("").is_none());
    }

    // ── iso_to_ms ─────────────────────────────────────────────────────────

    #[test]
    fn iso_to_ms_valid() {
        // 2026-05-05T03:00:00Z → 1_777_950_000_000 ms (verified via chrono)
        let ms = iso_to_ms("2026-05-05T03:00:00Z").unwrap();
        assert_eq!(ms, 1_777_950_000_000);
    }

    #[test]
    fn iso_to_ms_round_trip() {
        // Round-trip: ms → DateTime → ms
        let ms = iso_to_ms("2026-01-01T00:00:00Z").unwrap();
        let back = DateTime::<Utc>::from_timestamp_millis(ms as i64).unwrap();
        assert_eq!(back.to_rfc3339_opts(chrono::SecondsFormat::Secs, true), "2026-01-01T00:00:00Z");
    }

    #[test]
    fn iso_to_ms_invalid() {
        assert!(iso_to_ms("not a date").is_none());
        assert!(iso_to_ms("2026-13-99T99:99:99Z").is_none());
    }

    // ── PolyMarket helpers ────────────────────────────────────────────────

    fn sample_market() -> PolyMarket {
        PolyMarket {
            id:             "2152242".to_string(),
            question:       "Bitcoin Up or Down - May 4, 10:55PM-11:00PM ET".to_string(),
            up_token_id:    "770286165307075478407821058745".to_string(),
            down_token_id:  "842741244417177364311129620769".to_string(),
            close_time_ms:  1_777_986_000_000,
            duration_min:   5,
            liquidity_usd:  8678.49,
        }
    }

    #[test]
    fn open_time_for_5min_market() {
        let m = sample_market();
        assert_eq!(m.open_time_ms(), m.close_time_ms - 300_000);
    }

    #[test]
    fn open_time_for_15min_market() {
        let mut m = sample_market();
        m.duration_min = 15;
        assert_eq!(m.open_time_ms(), m.close_time_ms - 900_000);
    }

    #[test]
    fn time_to_close_counts_down() {
        let m = sample_market();
        let t1 = m.time_to_close_secs(m.close_time_ms - 300_000);
        let t2 = m.time_to_close_secs(m.close_time_ms - 60_000);
        let t3 = m.time_to_close_secs(m.close_time_ms);
        let t4 = m.time_to_close_secs(m.close_time_ms + 1_000);
        assert!((t1 - 300.0).abs() < 1e-6);
        assert!((t2 -  60.0).abs() < 1e-6);
        assert_eq!(t3, 0.0);
        assert_eq!(t4, 0.0);
    }

    // ── parse_btc_markets — fixture body ──────────────────────────────────

    #[test]
    fn parses_market_with_string_liquidity() {
        // Gamma occasionally returns liquidity as a string "0" instead of 0.0
        let body = r#"[{
            "id": 1,
            "question": "Bitcoin Up or Down - May 4, 10:55PM-11:00PM ET",
            "endDate": "2026-05-05T03:00:00Z",
            "clobTokenIds": "[\"a\", \"b\"]",
            "liquidity": "0"
        }]"#;
        let m = &parse_btc_markets(body).unwrap()[0];
        assert!((m.liquidity_usd - 0.0).abs() < 1e-10);
    }

    #[test]
    fn parses_valid_market_array() {
        let body = r#"[
            {
                "id": 2152242,
                "question": "Bitcoin Up or Down - May 4, 10:55PM-11:00PM ET",
                "endDate": "2026-05-05T03:00:00Z",
                "clobTokenIds": "[\"7702861\", \"8427412\"]",
                "liquidity": 8678.49
            },
            {
                "id": 2152243,
                "question": "Ethereum Up or Down - May 4, 10:55PM-11:00PM ET",
                "endDate": "2026-05-05T03:00:00Z",
                "clobTokenIds": "[\"x\", \"y\"]",
                "liquidity": 100.0
            }
        ]"#;
        let markets = parse_btc_markets(body).unwrap();
        assert_eq!(markets.len(), 1, "ETH market must be filtered out");
        let m = &markets[0];
        assert_eq!(m.id, "2152242");
        assert_eq!(m.duration_min, 5);
        assert_eq!(m.up_token_id,   "7702861");
        assert_eq!(m.down_token_id, "8427412");
        assert!((m.liquidity_usd - 8678.49).abs() < 1e-6);
    }

    #[test]
    fn parse_skips_markets_with_wrong_token_count() {
        let body = r#"[{
            "id": 1,
            "question": "Bitcoin Up or Down - May 4, 10:55PM-11:00PM ET",
            "endDate": "2026-05-05T03:00:00Z",
            "clobTokenIds": "[\"only-one-token\"]",
            "liquidity": 100.0
        }]"#;
        assert_eq!(parse_btc_markets(body).unwrap().len(), 0);
    }

    #[test]
    fn parse_skips_markets_with_missing_question() {
        let body = r#"[{
            "id": 1,
            "endDate": "2026-05-05T03:00:00Z",
            "clobTokenIds": "[\"a\", \"b\"]"
        }]"#;
        assert_eq!(parse_btc_markets(body).unwrap().len(), 0);
    }

    #[test]
    fn parse_returns_err_on_non_array_body() {
        let body = r#"{"error": "not an array"}"#;
        assert!(parse_btc_markets(body).is_err());
    }

    // ── D4: CLOB book parser ──────────────────────────────────────────────

    fn sample_book_json(asset_id: &str, bids: &[(&str, &str)], asks: &[(&str, &str)]) -> String {
        let bids_json: Vec<String> = bids.iter()
            .map(|(p, s)| format!(r#"{{"price":"{p}","size":"{s}"}}"#))
            .collect();
        let asks_json: Vec<String> = asks.iter()
            .map(|(p, s)| format!(r#"{{"price":"{p}","size":"{s}"}}"#))
            .collect();
        format!(
            r#"{{
                "event_type": "book",
                "market": "0xabc",
                "asset_id": "{asset_id}",
                "timestamp": "1700000000000",
                "hash": "deadbeef",
                "bids": [{}],
                "asks": [{}],
                "tick_size": "0.01",
                "last_trade_price": "0.49"
            }}"#,
            bids_json.join(","), asks_json.join(",")
        )
    }

    #[test]
    fn parse_book_finds_max_bid_and_min_ask() {
        let json = sample_book_json(
            "asset123",
            &[("0.45","100"), ("0.46","50"), ("0.47","25")],   // best bid = 0.47
            &[("0.50","200"), ("0.51","30"), ("0.49","10")],   // best ask = 0.49
        );
        let events = parse_clob_payload(&json);
        assert_eq!(events.len(), 1);
        match &events[0] {
            StreamEvent::ClobBook {
                market_id, asset_id, best_bid, best_ask,
                best_bid_size, best_ask_size, last_trade_price, ..
            } => {
                assert_eq!(market_id, "0xabc");
                assert_eq!(asset_id, "asset123");
                assert!((best_bid - 0.47).abs() < 1e-10);
                assert!((best_ask - 0.49).abs() < 1e-10);
                assert!((*best_bid_size - 25.0).abs() < 1e-6);
                assert!((*best_ask_size - 10.0).abs() < 1e-6);
                assert!((last_trade_price - 0.49).abs() < 1e-10);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_book_array_emits_one_per_asset() {
        // Two books in one CLOB message — typical at subscribe time
        let book1 = sample_book_json("up_token",   &[("0.45","100")], &[("0.50","200")]);
        let book2 = sample_book_json("down_token", &[("0.50","100")], &[("0.55","200")]);
        let payload = format!("[{},{}]", book1, book2);
        let events = parse_clob_payload(&payload);
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn parse_book_rejects_invalid_prices() {
        // bid > 1.0 should reject (probabilities must be in (0,1))
        let json = sample_book_json("a", &[("1.50","100")], &[("0.50","100")]);
        assert_eq!(parse_clob_payload(&json).len(), 0);
    }

    #[test]
    fn parse_book_skips_non_book_event_types() {
        let json = r#"[{"event_type":"price_change","asset_id":"x","changes":[]}]"#;
        assert_eq!(parse_clob_payload(json).len(), 0);
    }

    #[test]
    fn parse_book_ignores_garbage() {
        assert_eq!(parse_clob_payload("not json").len(), 0);
        assert_eq!(parse_clob_payload("").len(), 0);
        assert_eq!(parse_clob_payload("[]").len(), 0);
    }

    #[test]
    fn parse_book_skips_zero_size_levels() {
        let json = sample_book_json(
            "a",
            &[("0.45","100"), ("0.50","0")],   // 0.50 has zero size — should be skipped
            &[("0.55","100")],
        );
        let events = parse_clob_payload(&json);
        assert_eq!(events.len(), 1);
        match &events[0] {
            StreamEvent::ClobBook { best_bid, .. } => {
                assert!((best_bid - 0.45).abs() < 1e-10, "should pick 0.45, not 0.50 (zero size)");
            }
            _ => panic!("wrong variant"),
        }
    }

    // ── D4: live CLOB integration ─────────────────────────────────────────

    #[ignore]
    #[tokio::test(flavor = "multi_thread")]
    async fn integration_clob_live() {
        // Pull a live BTC market from Gamma, then subscribe to its CLOB tokens
        let markets = discover_btc_markets().await.expect("gamma");
        assert!(!markets.is_empty(), "need a live BTC market");
        let m = &markets[0];
        let asset_ids = vec![m.up_token_id.clone(), m.down_token_id.clone()];

        let (tx, mut rx) = broadcast::channel::<StreamEvent>(64);
        tokio::spawn(clob_stream(tx, asset_ids));

        let event = tokio::time::timeout(Duration::from_secs(15), rx.recv())
            .await
            .expect("timed out waiting for CLOB book event")
            .expect("recv");

        match event {
            StreamEvent::ClobBook { best_bid, best_ask, market_id, asset_id, .. } => {
                assert!(best_bid > 0.0 && best_bid < 1.0, "bid: {best_bid}");
                assert!(best_ask > 0.0 && best_ask < 1.0, "ask: {best_ask}");
                assert!(best_ask >= best_bid, "ask {best_ask} should be >= bid {best_bid}");
                assert!(!market_id.is_empty());
                assert!(!asset_id.is_empty());
                tracing::info!(market_id, asset_id, best_bid, best_ask, "CLOB OK");
            }
            _ => panic!("wrong variant"),
        }
    }

    // ── D2: Chainlink decoder unit tests ──────────────────────────────────

    #[test]
    fn decode_chainlink_response_known_value() {
        // BTC/USD ~ $80,185.16 with answer = 8018516000000 (8 decimals = e8)
        // updatedAt = 1777864914 (unix sec)
        // Layout 5×32 bytes. Hex below has roundId=0, answer, 0, updatedAt, 0.
        let answer_hex = format!("{:064x}", 8_018_516_000_000_u128);
        let updated_hex = format!("{:064x}", 1_777_864_914_u64);
        let zero64      = "0".repeat(64);
        let hex = format!("0x{}{}{}{}{}", zero64, answer_hex, zero64, updated_hex, zero64);
        let (price, ts) = decode_latest_round_data(&hex).expect("decode");
        assert!((price - 80_185.16).abs() < 0.01);
        assert_eq!(ts, 1_777_864_914);
    }

    #[test]
    fn decode_chainlink_too_short_returns_none() {
        assert!(decode_latest_round_data("0xdeadbeef").is_none());
        assert!(decode_latest_round_data("").is_none());
    }

    #[test]
    fn decode_chainlink_zero_price_returns_none() {
        let zero64 = "0".repeat(64);
        let hex = format!("0x{}", zero64.repeat(5));
        assert!(decode_latest_round_data(&hex).is_none());
    }

    #[test]
    fn decode_chainlink_strips_0x_prefix() {
        let answer_hex = format!("{:064x}", 5_000_000_000_000_u128); // $50,000
        let updated   = format!("{:064x}", 1_700_000_000_u64);
        let zero64    = "0".repeat(64);
        let with_prefix    = format!("0x{}{}{}{}{}", zero64, answer_hex, zero64, updated, zero64);
        let without_prefix =          format!("{}{}{}{}{}", zero64, answer_hex, zero64, updated, zero64);
        assert_eq!(decode_latest_round_data(&with_prefix),
                   decode_latest_round_data(&without_prefix));
    }

    // ── D2: live chainlink polling integration ────────────────────────────

    #[ignore]
    #[tokio::test(flavor = "multi_thread")]
    async fn integration_chainlink_btc_live() {
        let event = fetch_chainlink_btc(POLYGON_RPC_URLS[0])
            .await
            .expect("chainlink fetch");
        match event {
            StreamEvent::ChainlinkPrice { price, timestamp_ms } => {
                // Sanity bounds — BTC has been between $1k and $1M for years
                assert!(price > 1_000.0  && price < 1_000_000.0, "absurd price: {price}");
                assert!(timestamp_ms > 1_700_000_000_000, "timestamp from a real round");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[ignore]
    #[tokio::test(flavor = "multi_thread")]
    async fn integration_chainlink_polling_loop_emits() {
        let (tx, mut rx) = broadcast::channel::<StreamEvent>(64);
        tokio::spawn(chainlink_polling_loop(tx));
        let event = tokio::time::timeout(Duration::from_secs(15), rx.recv())
            .await
            .expect("timed out waiting for chainlink event")
            .expect("recv");
        match event {
            StreamEvent::ChainlinkPrice { price, .. } => assert!(price > 1_000.0),
            _ => panic!("wrong variant"),
        }
    }

    // ── D1: Live Gamma integration ────────────────────────────────────────

    #[ignore]
    #[tokio::test(flavor = "multi_thread")]
    async fn integration_discover_btc_live() {
        let markets = discover_btc_markets().await.expect("gamma fetch failed");
        assert!(!markets.is_empty(), "expected at least one live BTC market");
        for m in &markets {
            assert!(m.question.starts_with("Bitcoin Up or Down"));
            assert!(m.duration_min == 5 || m.duration_min == 15);
            assert!(m.close_time_ms > 0);
            assert!(!m.up_token_id.is_empty());
            assert!(!m.down_token_id.is_empty());
        }
        tracing::info!(count = markets.len(), "discovered BTC markets");
    }
}
