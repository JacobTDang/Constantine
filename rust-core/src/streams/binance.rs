// All BTC market data comes from OKX.
// binance.com and binance.us WebSocket endpoints are geo-blocked in the US.
// OKX is accessible, uses the same asset (BTC/USDT), and provides equivalent
// spot + perp data.
//
// Connection layout:
//   public ws   -> spot trades (B1), perp mark/funding/OI (B3/B4)
//   business ws -> candle1m klines (B5)
//
// Liquidations (B6) live in streams/liquidations.rs but share the same
// reconnect() helper exported from this module.

use super::{LiqSide, StreamEvent};
use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::sync::broadcast;
use tokio::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::Message};

pub const OKX_PUBLIC_URL:   &str = "wss://ws.okx.com:8443/ws/v5/public";
pub const OKX_BUSINESS_URL: &str = "wss://ws.okx.com:8443/ws/v5/business";

const BACKOFF_INIT_SECS: u64 = 1;
const BACKOFF_MAX_SECS:  u64 = 30;

// ── Public entry points ───────────────────────────────────────────────────────

/// B1 — real-time BTC/USDT spot trades.
pub async fn spot_stream(tx: broadcast::Sender<StreamEvent>) {
    let sub = json!({
        "op": "subscribe",
        "args": [{"channel": "trades", "instId": "BTC-USDT"}]
    })
    .to_string();

    reconnect(OKX_PUBLIC_URL, sub, "spot", |text, tx| {
        if let Some(e) = parse_trade(text) {
            let _ = tx.send(e);
        }
    }, tx)
    .await;
}

/// B3 — perp mark price + funding rate.
/// Both channels share one connection; messages are routed by channel name.
pub async fn perp_stream(tx: broadcast::Sender<StreamEvent>) {
    let sub = json!({
        "op": "subscribe",
        "args": [
            {"channel": "mark-price",   "instId": "BTC-USDT-SWAP"},
            {"channel": "funding-rate", "instId": "BTC-USDT-SWAP"},
        ]
    })
    .to_string();

    reconnect(OKX_PUBLIC_URL, sub, "perp", |text, tx| {
        route_by_channel(text, tx, &[
            ("mark-price",   parse_mark_price   as fn(&str) -> Option<StreamEvent>),
            ("funding-rate", parse_funding_rate as fn(&str) -> Option<StreamEvent>),
        ]);
    }, tx)
    .await;
}

/// B4 — open interest.
pub async fn oi_stream(tx: broadcast::Sender<StreamEvent>) {
    let sub = json!({
        "op": "subscribe",
        "args": [{"channel": "open-interest", "instId": "BTC-USDT-SWAP"}]
    })
    .to_string();

    reconnect(OKX_PUBLIC_URL, sub, "oi", |text, tx| {
        if let Some(e) = parse_open_interest(text) {
            let _ = tx.send(e);
        }
    }, tx)
    .await;
}

/// B5 — 1-minute klines. Uses the OKX business WebSocket.
pub async fn kline_stream(tx: broadcast::Sender<StreamEvent>) {
    let sub = json!({
        "op": "subscribe",
        "args": [{"channel": "candle1m", "instId": "BTC-USDT"}]
    })
    .to_string();

    reconnect(OKX_BUSINESS_URL, sub, "klines", |text, tx| {
        if let Some(e) = parse_kline(text) {
            let _ = tx.send(e);
        }
    }, tx)
    .await;
}

// ── Generic reconnect loop (also used by liquidations.rs) ────────────────────

pub async fn reconnect<F>(
    url:           &'static str,
    subscribe_msg: String,
    label:         &'static str,
    mut on_message: F,
    tx:            broadcast::Sender<StreamEvent>,
) where
    F: FnMut(&str, &broadcast::Sender<StreamEvent>),
{
    let mut backoff = Duration::from_secs(BACKOFF_INIT_SECS);
    loop {
        tracing::info!(stream = label, "connecting");
        match run_connection(url, &subscribe_msg, &tx, &mut on_message).await {
            Ok(()) => {
                tracing::warn!(stream = label, "closed cleanly, reconnecting");
                backoff = Duration::from_secs(BACKOFF_INIT_SECS);
            }
            Err(e) => {
                tracing::error!(
                    stream     = label,
                    error      = %e,
                    retry_secs = backoff.as_secs(),
                    "stream error, retrying"
                );
                tokio::time::sleep(backoff).await;
                backoff = next_backoff(backoff, Duration::from_secs(BACKOFF_MAX_SECS));
            }
        }
    }
}

async fn run_connection<F>(
    url:           &str,
    subscribe_msg: &str,
    tx:            &broadcast::Sender<StreamEvent>,
    on_message:    &mut F,
) -> Result<()>
where
    F: FnMut(&str, &broadcast::Sender<StreamEvent>),
{
    let (ws, _) = connect_async(url).await?;
    tracing::info!(url, "connected");
    let (mut write, mut read) = ws.split();

    write
        .send(Message::Text(subscribe_msg.to_string()))
        .await?;

    while let Some(msg) = read.next().await {
        match msg? {
            Message::Text(text) => {
                // OKX sends plain-text "ping" as server heartbeat
                if text == "ping" {
                    write
                        .send(Message::Text("pong".to_string()))
                        .await?;
                    tracing::debug!("okx pong sent");
                    continue;
                }
                on_message(&text, tx);
            }
            Message::Close(_) => {
                tracing::warn!(url, "server closed connection");
                break;
            }
            Message::Ping(payload) => {
                write.send(Message::Pong(payload)).await?;
            }
            _ => {}
        }
    }
    Ok(())
}

// ── Channel router ────────────────────────────────────────────────────────────

fn route_by_channel(
    text:    &str,
    tx:      &broadcast::Sender<StreamEvent>,
    parsers: &[(&str, fn(&str) -> Option<StreamEvent>)],
) {
    #[derive(serde::Deserialize)]
    struct Envelope {
        arg: Option<Arg>,
    }
    #[derive(serde::Deserialize)]
    struct Arg {
        channel: String,
    }

    let Ok(env) = serde_json::from_str::<Envelope>(text) else { return };
    let Some(arg) = env.arg else { return };

    for (channel, parser) in parsers {
        if arg.channel == *channel {
            if let Some(e) = parser(text) {
                let _ = tx.send(e);
            }
            return;
        }
    }
}

// ── Parsers ───────────────────────────────────────────────────────────────────

// B1 — spot trades
#[derive(serde::Deserialize)]
struct OkxTradeEnvelope {
    data: Vec<OkxTrade>,
}
#[derive(serde::Deserialize)]
struct OkxTrade {
    #[serde(rename = "px")]
    price: String,
    #[serde(rename = "sz")]
    size: String,
    #[serde(rename = "ts")]
    timestamp_ms: String,
}

pub fn parse_trade(text: &str) -> Option<StreamEvent> {
    let env: OkxTradeEnvelope = serde_json::from_str(text).ok()?;
    let t = env.data.into_iter().next()?;
    let price:        f64 = t.price.parse().ok()?;
    let quantity:     f64 = t.size.parse().ok()?;
    let timestamp_ms: u64 = t.timestamp_ms.parse().ok()?;
    if !price.is_finite() || !quantity.is_finite() {
        return None;
    }
    Some(StreamEvent::Trade { price, quantity, timestamp_ms })
}

// B3 — mark price
#[derive(serde::Deserialize)]
struct MarkPriceEnvelope {
    data: Vec<MarkPriceData>,
}
#[derive(serde::Deserialize)]
struct MarkPriceData {
    #[serde(rename = "markPx")]
    mark_px: String,
    ts: String,
}

pub fn parse_mark_price(text: &str) -> Option<StreamEvent> {
    let env: MarkPriceEnvelope = serde_json::from_str(text).ok()?;
    let d = env.data.into_iter().next()?;
    let mark_price:   f64 = d.mark_px.parse().ok()?;
    let timestamp_ms: u64 = d.ts.parse().ok()?;
    if !mark_price.is_finite() {
        return None;
    }
    Some(StreamEvent::MarkPrice { mark_price, timestamp_ms })
}

// B3 — funding rate
#[derive(serde::Deserialize)]
struct FundingRateEnvelope {
    data: Vec<FundingRateData>,
}
#[derive(serde::Deserialize)]
struct FundingRateData {
    #[serde(rename = "fundingRate")]
    funding_rate: String,
    #[serde(rename = "fundingTime")]
    funding_time: String,
    ts: String,
}

pub fn parse_funding_rate(text: &str) -> Option<StreamEvent> {
    let env: FundingRateEnvelope = serde_json::from_str(text).ok()?;
    let d = env.data.into_iter().next()?;
    let funding_rate:         f64 = d.funding_rate.parse().ok()?;
    let next_funding_time_ms: u64 = d.funding_time.parse().ok()?;
    let timestamp_ms:         u64 = d.ts.parse().ok()?;
    if !funding_rate.is_finite() {
        return None;
    }
    Some(StreamEvent::FundingRate { funding_rate, next_funding_time_ms, timestamp_ms })
}

// B4 — open interest
#[derive(serde::Deserialize)]
struct OiEnvelope {
    data: Vec<OiData>,
}
#[derive(serde::Deserialize)]
struct OiData {
    #[serde(rename = "oiCcy")]
    oi_ccy: String,
    ts: String,
}

pub fn parse_open_interest(text: &str) -> Option<StreamEvent> {
    let env: OiEnvelope = serde_json::from_str(text).ok()?;
    let d = env.data.into_iter().next()?;
    let open_interest: f64 = d.oi_ccy.parse().ok()?;
    let timestamp_ms:  u64 = d.ts.parse().ok()?;
    if !open_interest.is_finite() {
        return None;
    }
    Some(StreamEvent::OpenInterest { open_interest, timestamp_ms })
}

// B5 — klines
// OKX data array: [ts, open, high, low, close, vol, volCcy, volCcyQuote, confirm]
#[derive(serde::Deserialize)]
struct KlineEnvelope {
    data: Vec<Vec<serde_json::Value>>,
}

pub fn parse_kline(text: &str) -> Option<StreamEvent> {
    let env: KlineEnvelope = serde_json::from_str(text).ok()?;
    let row = env.data.into_iter().next()?;
    if row.len() < 9 {
        return None;
    }

    let ts_ms:  u64 = row[0].as_str()?.parse().ok()?;
    let open:   f64 = row[1].as_str()?.parse().ok()?;
    let high:   f64 = row[2].as_str()?.parse().ok()?;
    let low:    f64 = row[3].as_str()?.parse().ok()?;
    let close:  f64 = row[4].as_str()?.parse().ok()?;
    let volume: f64 = row[5].as_str()?.parse().ok()?;
    let is_closed = row[8].as_str()? == "1";

    if ![open, high, low, close, volume].iter().all(|v| v.is_finite()) {
        return None;
    }
    Some(StreamEvent::Kline { open, high, low, close, volume, is_closed, open_time_ms: ts_ms })
}

// B6 — liquidations (used by liquidations.rs)
#[derive(serde::Deserialize)]
pub struct LiqEnvelope {
    pub data: Vec<LiqData>,
}
#[derive(serde::Deserialize)]
pub struct LiqData {
    #[serde(rename = "instId")]
    pub inst_id: String,
    pub details: Vec<LiqDetail>,
}
#[derive(serde::Deserialize)]
pub struct LiqDetail {
    #[serde(rename = "posSide")]
    pub pos_side: String,
    #[serde(rename = "bkPx")]
    pub bk_px: String,
    pub sz: String,
    pub ts: String,
}

pub fn parse_liquidations(text: &str) -> Vec<StreamEvent> {
    let env: LiqEnvelope = match serde_json::from_str(text) {
        Ok(e) => e,
        Err(_) => return vec![],
    };
    let mut events = Vec::new();
    for item in env.data {
        if item.inst_id != "BTC-USDT-SWAP" {
            continue;
        }
        for d in item.details {
            let side = match d.pos_side.as_str() {
                "long"  => LiqSide::Long,
                "short" => LiqSide::Short,
                _       => continue,
            };
            let price: f64 = match d.bk_px.parse::<f64>() {
                Ok(p) if p.is_finite() && p > 0.0 => p,
                _ => continue,
            };
            let quantity: f64 = match d.sz.parse::<f64>() {
                Ok(q) if q.is_finite() && q > 0.0 => q,
                _ => continue,
            };
            let timestamp_ms: u64 = match d.ts.parse() {
                Ok(t) => t,
                Err(_) => continue,
            };
            events.push(StreamEvent::Liquidation { side, price, quantity, timestamp_ms });
        }
    }
    events
}

// ── Backoff helper ────────────────────────────────────────────────────────────

pub fn next_backoff(current: Duration, max: Duration) -> Duration {
    (current * 2).min(max)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // B1 — spot trade parsing
    #[test]
    fn parse_valid_trade() {
        let json = r#"{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","tradeId":"1","px":"43210.50","sz":"0.012","side":"buy","ts":"1700000000001"}]}"#;
        match parse_trade(json).unwrap() {
            StreamEvent::Trade { price, quantity, timestamp_ms } => {
                assert!((price - 43210.50).abs() < 1e-6);
                assert!((quantity - 0.012).abs() < 1e-9);
                assert_eq!(timestamp_ms, 1700000000001);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_trade_empty_data_is_none() {
        assert!(parse_trade(r#"{"arg":{},"data":[]}"#).is_none());
    }

    #[test]
    fn parse_trade_rejects_inf_price() {
        let json = r#"{"data":[{"px":"Inf","sz":"1.0","ts":"1000"}]}"#;
        assert!(parse_trade(json).is_none());
    }

    // B3 — mark price parsing
    #[test]
    fn parse_valid_mark_price() {
        let json = r#"{"arg":{"channel":"mark-price","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","markPx":"43500.2","ts":"1700000000000"}]}"#;
        match parse_mark_price(json).unwrap() {
            StreamEvent::MarkPrice { mark_price, timestamp_ms } => {
                assert!((mark_price - 43500.2).abs() < 1e-6);
                assert_eq!(timestamp_ms, 1700000000000);
            }
            _ => panic!("wrong variant"),
        }
    }

    // B3 — funding rate parsing
    #[test]
    fn parse_valid_funding_rate() {
        let json = r#"{"arg":{"channel":"funding-rate","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","fundingRate":"0.0001234","nextFundingRate":"0.0001","fundingTime":"1700000800000","ts":"1700000000000"}]}"#;
        match parse_funding_rate(json).unwrap() {
            StreamEvent::FundingRate { funding_rate, next_funding_time_ms, timestamp_ms } => {
                assert!((funding_rate - 0.0001234).abs() < 1e-10);
                assert_eq!(next_funding_time_ms, 1700000800000);
                assert_eq!(timestamp_ms, 1700000000000);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_funding_rate_negative_allowed() {
        let json = r#"{"data":[{"fundingRate":"-0.0003","fundingTime":"1700000800000","ts":"1700000000000"}]}"#;
        match parse_funding_rate(json).unwrap() {
            StreamEvent::FundingRate { funding_rate, .. } => {
                assert!(funding_rate < 0.0);
            }
            _ => panic!("wrong variant"),
        }
    }

    // B4 — open interest parsing
    #[test]
    fn parse_valid_open_interest() {
        let json = r#"{"arg":{"channel":"open-interest","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","oi":"3198824.70","oiCcy":"31988.247","ts":"1700000000000"}]}"#;
        match parse_open_interest(json).unwrap() {
            StreamEvent::OpenInterest { open_interest, timestamp_ms } => {
                assert!((open_interest - 31988.247).abs() < 1e-3);
                assert_eq!(timestamp_ms, 1700000000000);
            }
            _ => panic!("wrong variant"),
        }
    }

    // B5 — kline parsing
    #[test]
    fn parse_valid_kline_closed() {
        let json = r#"{"arg":{"channel":"candle1m","instId":"BTC-USDT"},"data":[["1630048500000","43200.5","43210.5","43195.0","43205.0","1.5234","65821.5","65821.5","1"]]}"#;
        match parse_kline(json).unwrap() {
            StreamEvent::Kline { open, high, low, close, volume, is_closed, open_time_ms } => {
                assert!((open  - 43200.5).abs() < 1e-6);
                assert!((high  - 43210.5).abs() < 1e-6);
                assert!((low   - 43195.0).abs() < 1e-6);
                assert!((close - 43205.0).abs() < 1e-6);
                assert!((volume - 1.5234).abs() < 1e-6);
                assert!(is_closed);
                assert_eq!(open_time_ms, 1630048500000);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_valid_kline_not_closed() {
        let json = r#"{"data":[["1630048500000","43200.5","43210.5","43195.0","43205.0","1.5234","0","0","0"]]}"#;
        match parse_kline(json).unwrap() {
            StreamEvent::Kline { is_closed, .. } => assert!(!is_closed),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_kline_too_short_returns_none() {
        let json = r#"{"data":[["1630048500000","43200.5","43210.5"]]}"#;
        assert!(parse_kline(json).is_none());
    }

    // B6 — liquidation parsing
    #[test]
    fn parse_long_liquidation() {
        let json = r#"{"arg":{"channel":"liquidation-orders","instType":"SWAP"},"data":[{"instId":"BTC-USDT-SWAP","instType":"SWAP","instFamily":"BTC-USDT","uly":"BTC-USDT","totalLoss":"0","details":[{"bkLoss":"0","bkPx":"43000.5","ccy":"","posSide":"long","side":"sell","sz":"0.1","ts":"1700000000000"}]}]}"#;
        let events = parse_liquidations(json);
        assert_eq!(events.len(), 1);
        match &events[0] {
            StreamEvent::Liquidation { side, price, quantity, timestamp_ms } => {
                assert_eq!(*side, LiqSide::Long);
                assert!((price - 43000.5).abs() < 1e-6);
                assert!((quantity - 0.1).abs() < 1e-9);
                assert_eq!(*timestamp_ms, 1700000000000);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_short_liquidation() {
        let json = r#"{"data":[{"instId":"BTC-USDT-SWAP","details":[{"bkPx":"45000.0","posSide":"short","side":"buy","sz":"0.5","ts":"1700000001000"}]}]}"#;
        let events = parse_liquidations(json);
        assert_eq!(events.len(), 1);
        match &events[0] {
            StreamEvent::Liquidation { side, .. } => assert_eq!(*side, LiqSide::Short),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_liquidation_filters_non_btc() {
        let json = r#"{"data":[{"instId":"ETH-USDT-SWAP","details":[{"bkPx":"2000.0","posSide":"long","sz":"1.0","ts":"1000"}]}]}"#;
        assert!(parse_liquidations(json).is_empty());
    }

    #[test]
    fn parse_liquidation_multiple_details() {
        let json = r#"{"data":[{"instId":"BTC-USDT-SWAP","details":[{"bkPx":"43000.0","posSide":"long","sz":"0.1","ts":"1000"},{"bkPx":"43100.0","posSide":"short","sz":"0.2","ts":"1001"}]}]}"#;
        let events = parse_liquidations(json);
        assert_eq!(events.len(), 2);
    }

    // Backoff
    #[test]
    fn backoff_doubles_to_max() {
        let max = Duration::from_secs(30);
        let steps: Vec<u64> = std::iter::successors(
            Some(Duration::from_secs(1)),
            |d| Some(next_backoff(*d, max)),
        )
        .take(6)
        .map(|d| d.as_secs())
        .collect();
        assert_eq!(steps, vec![1, 2, 4, 8, 16, 30]);
    }

    // ── Integration tests (require live OKX connection) ───────────────────
    // Run with: cargo test <test_name> -- --ignored --nocapture

    #[ignore]
    #[tokio::test(flavor = "multi_thread")]
    async fn integration_spot_stream() {
        let (tx, mut rx) = broadcast::channel::<StreamEvent>(256);
        tokio::spawn(spot_stream(tx));
        let count = collect_events(&mut rx, 1, Duration::from_secs(10)).await;
        assert!(count >= 1, "expected >= 1 spot trade in 10s, got {count}");
    }

    #[ignore]
    #[tokio::test(flavor = "multi_thread")]
    async fn integration_perp_stream() {
        let (tx, mut rx) = broadcast::channel::<StreamEvent>(256);
        tokio::spawn(perp_stream(tx));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        let mut got_mark = false;
        let mut got_funding = false;
        loop {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Ok(StreamEvent::MarkPrice { mark_price, .. })) => {
                    assert!(mark_price > 1_000.0);
                    got_mark = true;
                }
                Ok(Ok(StreamEvent::FundingRate { .. })) => {
                    got_funding = true;
                }
                Ok(Ok(_)) => {}
                Ok(Err(_)) | Err(_) => break,
            }
            if got_mark && got_funding { break; }
        }
        assert!(got_mark,    "no MarkPrice event in 15s");
        assert!(got_funding, "no FundingRate event in 15s");
    }

    #[ignore]
    #[tokio::test(flavor = "multi_thread")]
    async fn integration_oi_stream() {
        let (tx, mut rx) = broadcast::channel::<StreamEvent>(256);
        tokio::spawn(oi_stream(tx));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Ok(StreamEvent::OpenInterest { open_interest, .. })) => {
                    assert!(open_interest > 0.0);
                    return;
                }
                Ok(Ok(_)) => {}
                Ok(Err(_)) | Err(_) => break,
            }
        }
        panic!("no OpenInterest event in 15s");
    }

    #[ignore]
    #[tokio::test(flavor = "multi_thread")]
    async fn integration_kline_stream() {
        let (tx, mut rx) = broadcast::channel::<StreamEvent>(256);
        tokio::spawn(kline_stream(tx));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Ok(StreamEvent::Kline { open, close, .. })) => {
                    assert!(open > 0.0 && close > 0.0);
                    return;
                }
                Ok(Ok(_)) => {}
                Ok(Err(_)) | Err(_) => break,
            }
        }
        panic!("no Kline event in 15s");
    }

    async fn collect_events(
        rx:      &mut broadcast::Receiver<StreamEvent>,
        target:  usize,
        timeout: Duration,
    ) -> usize {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut count = 0;
        loop {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Ok(_)) => {
                    count += 1;
                    if count >= target { break; }
                }
                Ok(Err(_)) | Err(_) => break,
            }
        }
        count
    }
}
