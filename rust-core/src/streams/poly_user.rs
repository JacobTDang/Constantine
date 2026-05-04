// Concern C1 — Polymarket user-channel websocket → fill confirmation.
//
// Subscribes to Polymarket's "user" CLOB channel, which pushes order
// lifecycle events for our wallet specifically (placements, matches,
// cancellations). When a trade event arrives for one of OUR submitted
// orders, we record the fill to PositionStore.
//
// Connect: wss://ws-subscriptions-clob.polymarket.com/ws/user
// Auth: L2 creds passed in the subscribe message body.
//
// Event shape (representative — Polymarket may add fields over time):
//
//   {
//     "event_type":     "trade",
//     "asset_id":       "<uint256>",
//     "price":          "0.45",
//     "size":           "10.5",
//     "side":           "BUY" | "SELL",
//     "status":         "MATCHED" | "MINED" | "CONFIRMED" | "FAILED",
//     "trader_side":    "MAKER" | "TAKER",
//     "taker_order_id": "<our order_id>",
//     "trade_id":       "...",
//     "match_time":     "...",
//     "timestamp":      "<unix ms>",
//     ...
//   }
//
// Multiple events can arrive in a single frame as a JSON array.

use std::sync::Arc;

use anyhow::{Context, Result};
use serde_json::Value;
use tokio::time::{sleep, Duration};

use crate::execution::auth::{auth_headers, ApiCreds};
use crate::storage::PositionStore;

const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/user";
const RECONNECT_BACKOFF_SECS: u64 = 5;

/// One parsed event from the Polymarket user channel.
///
/// Polymarket sends many event_types; we only care about trades that
/// actually fill (MATCHED+) and order updates that imply cancellation.
#[derive(Debug, Clone, PartialEq)]
pub enum UserStreamEvent {
    /// A fill occurred for our wallet — record_fill to the position ledger.
    Trade {
        order_id:  String,
        asset_id:  String,
        price:     f64,
        size:      f64,
        side:      String,         // "BUY" / "SELL"
        status:    String,         // "MATCHED" / "MINED" / "CONFIRMED" / "FAILED"
        timestamp_ms: u64,
    },
    /// Order placement / cancellation lifecycle event. We don't act on these
    /// today (positions go straight to Submitted on POST /order success),
    /// but the parser captures them for forensics.
    OrderUpdate {
        order_id:  String,
        kind:      String,         // "PLACEMENT" / "UPDATE" / "CANCELLATION"
        timestamp_ms: u64,
    },
    /// Anything we don't model — kept for trace-level logging.
    Unknown { raw: Value },
}

// ── Parsing ──────────────────────────────────────────────────────────────────

/// Parse a single text frame from the user websocket. Accepts both
/// single-object and array-of-objects payloads. Filters out heartbeat /
/// connection-status messages (they have no `event_type`).
pub fn parse_user_message(text: &str) -> Result<Vec<UserStreamEvent>> {
    let value: Value = serde_json::from_str(text)
        .with_context(|| format!("not valid JSON: {}", truncate(text, 120)))?;
    let array = match value {
        Value::Array(a)   => a,
        Value::Object(_)  => vec![value],
        other             => {
            return Err(anyhow::anyhow!(
                "expected JSON object or array, got {}",
                other_kind(&other)
            ));
        }
    };
    let mut out = Vec::with_capacity(array.len());
    for v in array {
        out.push(parse_one(v));
    }
    Ok(out)
}

fn parse_one(v: Value) -> UserStreamEvent {
    let event_type = v.get("event_type").and_then(|x| x.as_str()).unwrap_or("");
    match event_type {
        "trade" => UserStreamEvent::Trade {
            order_id:     get_str(&v, &["taker_order_id", "id", "trade_id"]).unwrap_or_default(),
            asset_id:     get_str(&v, &["asset_id"]).unwrap_or_default(),
            price:        get_f64(&v, &["price"]).unwrap_or(0.0),
            size:         get_f64(&v, &["size"]).unwrap_or(0.0),
            side:         get_str(&v, &["side"]).unwrap_or_default(),
            status:       get_str(&v, &["status"]).unwrap_or_default(),
            timestamp_ms: get_u64(&v, &["timestamp", "match_time"]).unwrap_or(0),
        },
        "order" => UserStreamEvent::OrderUpdate {
            order_id:     get_str(&v, &["id", "order_id"]).unwrap_or_default(),
            kind:         get_str(&v, &["type"]).unwrap_or_default(),
            timestamp_ms: get_u64(&v, &["timestamp"]).unwrap_or(0),
        },
        _ => UserStreamEvent::Unknown { raw: v },
    }
}

fn get_str(v: &Value, keys: &[&str]) -> Option<String> {
    for k in keys {
        if let Some(s) = v.get(k).and_then(|x| x.as_str()) {
            return Some(s.to_string());
        }
    }
    None
}

fn get_f64(v: &Value, keys: &[&str]) -> Option<f64> {
    for k in keys {
        if let Some(x) = v.get(k) {
            if let Some(n) = x.as_f64() { return Some(n); }
            // Polymarket sometimes sends numbers as strings ("0.45")
            if let Some(s) = x.as_str() {
                if let Ok(n) = s.parse::<f64>() { return Some(n); }
            }
        }
    }
    None
}

fn get_u64(v: &Value, keys: &[&str]) -> Option<u64> {
    for k in keys {
        if let Some(x) = v.get(k) {
            if let Some(n) = x.as_u64() { return Some(n); }
            if let Some(s) = x.as_str() {
                if let Ok(n) = s.parse::<u64>() { return Some(n); }
            }
        }
    }
    None
}

fn other_kind(v: &Value) -> &'static str {
    match v {
        Value::Null      => "null",
        Value::Bool(_)   => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_)  => "array",
        Value::Object(_) => "object",
    }
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n { s.to_string() } else { format!("{}…", &s[..n.saturating_sub(1)]) }
}

// ── Dispatching events to the position ledger ────────────────────────────────

/// Apply one event to the position ledger. Returns Ok(true) if the event
/// resulted in a state-change (fill recorded), Ok(false) if it was a
/// no-op, Err on persistence failure.
pub fn dispatch_event(positions: &PositionStore, event: &UserStreamEvent) -> Result<bool> {
    match event {
        UserStreamEvent::Trade { order_id, price, size, status, .. } => {
            // Only record the fill on a confirmed match. Polymarket's status
            // ladder: MATCHED (off-chain match) → MINED (on-chain) → CONFIRMED.
            // We accept any of these as a fill — the order is settled either
            // way; it's just gas confirmation. FAILED skipped intentionally.
            if matches!(status.as_str(), "MATCHED" | "MINED" | "CONFIRMED") {
                if order_id.is_empty() {
                    tracing::warn!("trade event with empty order_id, skipping");
                    return Ok(false);
                }
                positions.record_fill(order_id, *price, *size)
                    .with_context(|| format!("record_fill for {order_id}"))?;
                tracing::info!(
                    order_id = %order_id, price, size, status = %status,
                    "FILL CONFIRMED"
                );
                return Ok(true);
            }
            Ok(false)
        }
        UserStreamEvent::OrderUpdate { .. } | UserStreamEvent::Unknown { .. } => {
            // No-op — order updates are informational; we already track our
            // own placement state from the POST /order response.
            Ok(false)
        }
    }
}

/// Apply a batch of events. Returns the number of state-changes. Errors
/// from any single dispatch are logged but do not halt the batch.
pub fn dispatch_batch(positions: &PositionStore, events: &[UserStreamEvent]) -> usize {
    let mut n = 0;
    for ev in events {
        match dispatch_event(positions, ev) {
            Ok(true)  => n += 1,
            Ok(false) => {}
            Err(e)    => tracing::error!(error = %e, "dispatch_event failed"),
        }
    }
    n
}

// ── Subscribe message + connection loop ──────────────────────────────────────

/// Build the subscribe payload Polymarket expects. The user channel needs
/// L2 creds in the body; condition_ids scope which markets we want
/// notifications for.
pub fn build_subscribe_msg(
    creds:        &ApiCreds,
    condition_ids: &[String],
) -> Result<String> {
    // The user-channel subscribe message:
    //   { "auth": {apiKey, secret, passphrase}, "markets": [...], "type": "user" }
    let payload = serde_json::json!({
        "auth": {
            "apiKey":     creds.api_key,
            "secret":     creds.api_secret,
            "passphrase": creds.api_passphrase,
        },
        "markets": condition_ids,
        "type":    "user",
    });
    serde_json::to_string(&payload).context("serialize subscribe")
}

/// Long-running task: connect to the user channel, dispatch fills.
///
/// `condition_ids` should be the market condition IDs we're trading
/// (refreshable from market discovery). On reconnect we re-send the
/// subscribe message — Polymarket holds no per-connection state for
/// us beyond the active subscription.
pub async fn user_stream_loop(
    creds:         ApiCreds,
    address:       String,
    condition_ids: Vec<String>,
    positions:     Arc<PositionStore>,
) {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let subscribe = match build_subscribe_msg(&creds, &condition_ids) {
        Ok(s)  => s,
        Err(e) => {
            tracing::error!(error = %e, "couldn't build subscribe — user_stream not starting");
            return;
        }
    };

    loop {
        tracing::info!(markets = condition_ids.len(), "connecting to Polymarket user stream");
        match tokio_tungstenite::connect_async(WS_URL).await {
            Ok((mut ws, _resp)) => {
                if let Err(e) = ws.send(Message::Text(subscribe.clone())).await {
                    tracing::error!(error = %e, "subscribe send failed — reconnecting");
                    sleep(Duration::from_secs(RECONNECT_BACKOFF_SECS)).await;
                    continue;
                }

                while let Some(frame) = ws.next().await {
                    match frame {
                        Ok(Message::Text(text)) => {
                            match parse_user_message(&text) {
                                Ok(events) => {
                                    let n = dispatch_batch(&positions, &events);
                                    if n > 0 {
                                        tracing::debug!(events = events.len(), fills = n,
                                            "user stream batch processed");
                                    }
                                }
                                Err(e) => tracing::error!(error = %e, "parse failed"),
                            }
                        }
                        Ok(Message::Ping(p)) => {
                            let _ = ws.send(Message::Pong(p)).await;
                        }
                        Ok(Message::Close(_)) => {
                            tracing::warn!("user stream closed by server");
                            break;
                        }
                        Ok(_) => {}
                        Err(e) => {
                            tracing::error!(error = %e, "user stream read error");
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "user stream connect failed");
            }
        }
        // address kept on the loop for future signed-subscribe variants
        let _ = &address;
        sleep(Duration::from_secs(RECONNECT_BACKOFF_SECS)).await;
    }
}

// Compile-time use of auth_headers so the import isn't dropped — we'll
// switch to header-based auth if Polymarket migrates the user channel.
#[allow(dead_code)]
fn _auth_headers_warmup() {
    let _ = auth_headers;
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "constantine-userstream-{}", uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn sample_creds() -> ApiCreds {
        ApiCreds {
            api_key:        "00000000-1111-2222-3333-444444444444".into(),
            api_secret:     "c2VjcmV0X2J5dGVz".into(),
            api_passphrase: "passphrase".into(),
        }
    }

    // ── Parser ────────────────────────────────────────────────────────────

    #[test]
    fn parse_single_trade_event() {
        let raw = r#"{
            "event_type":     "trade",
            "asset_id":       "12345",
            "price":          "0.45",
            "size":           "10.5",
            "side":           "BUY",
            "status":         "MATCHED",
            "taker_order_id": "0xORDER",
            "timestamp":      "1700000000123"
        }"#;
        let events = parse_user_message(raw).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            UserStreamEvent::Trade { order_id, price, size, side, status, .. } => {
                assert_eq!(order_id, "0xORDER");
                assert!((*price - 0.45).abs() < 1e-9);
                assert!((*size - 10.5).abs() < 1e-9);
                assert_eq!(side, "BUY");
                assert_eq!(status, "MATCHED");
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn parse_array_of_events() {
        let raw = r#"[
            {"event_type":"trade","taker_order_id":"a","price":"0.5","size":"1","status":"MATCHED"},
            {"event_type":"trade","taker_order_id":"b","price":"0.6","size":"2","status":"FAILED"}
        ]"#;
        let events = parse_user_message(raw).unwrap();
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn parse_handles_numeric_or_string_amounts() {
        // Some servers send numbers, others strings — be tolerant.
        let raw = r#"{"event_type":"trade","taker_order_id":"x","price":0.45,"size":10.5,"status":"MATCHED"}"#;
        let events = parse_user_message(raw).unwrap();
        match &events[0] {
            UserStreamEvent::Trade { price, size, .. } => {
                assert!((*price - 0.45).abs() < 1e-9);
                assert!((*size - 10.5).abs() < 1e-9);
            }
            _ => panic!("expected Trade"),
        }
    }

    #[test]
    fn parse_order_update_event() {
        let raw = r#"{"event_type":"order","id":"o1","type":"PLACEMENT","timestamp":"100"}"#;
        let events = parse_user_message(raw).unwrap();
        match &events[0] {
            UserStreamEvent::OrderUpdate { order_id, kind, .. } => {
                assert_eq!(order_id, "o1");
                assert_eq!(kind, "PLACEMENT");
            }
            other => panic!("expected OrderUpdate, got {other:?}"),
        }
    }

    #[test]
    fn parse_unknown_event_type_does_not_panic() {
        let raw = r#"{"event_type":"heartbeat","ts":"100"}"#;
        let events = parse_user_message(raw).unwrap();
        match &events[0] {
            UserStreamEvent::Unknown { .. } => {}
            other => panic!("expected Unknown, got {other:?}"),
        }
    }

    #[test]
    fn parse_invalid_json_returns_err() {
        let raw = "not valid json";
        assert!(parse_user_message(raw).is_err());
    }

    #[test]
    fn parse_empty_array_returns_empty() {
        let raw = "[]";
        let events = parse_user_message(raw).unwrap();
        assert!(events.is_empty());
    }

    // ── Dispatch ──────────────────────────────────────────────────────────

    #[test]
    fn dispatch_matched_trade_records_fill() {
        let store = PositionStore::open(&temp_dir()).unwrap();
        store.record_open("ord-1", "btc-1", "yes", 5.0, 0.45).unwrap();

        let ev = UserStreamEvent::Trade {
            order_id: "ord-1".into(),
            asset_id: "asset".into(),
            price: 0.46, size: 11.0,
            side: "BUY".into(),
            status: "MATCHED".into(),
            timestamp_ms: 1_000,
        };
        let changed = dispatch_event(&store, &ev).unwrap();
        assert!(changed);

        let p = store.get("ord-1").unwrap();
        assert_eq!(p.status, crate::storage::PositionStatus::Filled);
        assert!((p.fill_price.unwrap() - 0.46).abs() < 1e-9);
        assert!((p.fill_size.unwrap()  - 11.0).abs() < 1e-9);
    }

    #[test]
    fn dispatch_failed_trade_does_not_fill() {
        let store = PositionStore::open(&temp_dir()).unwrap();
        store.record_open("ord-1", "btc-1", "yes", 5.0, 0.45).unwrap();

        let ev = UserStreamEvent::Trade {
            order_id: "ord-1".into(),
            asset_id: "asset".into(),
            price: 0.46, size: 11.0, side: "BUY".into(),
            status: "FAILED".into(),
            timestamp_ms: 1_000,
        };
        let changed = dispatch_event(&store, &ev).unwrap();
        assert!(!changed);
        // Still Submitted, no fill recorded
        let p = store.get("ord-1").unwrap();
        assert_eq!(p.status, crate::storage::PositionStatus::Submitted);
    }

    #[test]
    fn dispatch_skips_empty_order_id() {
        let store = PositionStore::open(&temp_dir()).unwrap();
        let ev = UserStreamEvent::Trade {
            order_id: "".into(),
            asset_id: "asset".into(),
            price: 0.46, size: 11.0, side: "BUY".into(),
            status: "MATCHED".into(),
            timestamp_ms: 1_000,
        };
        let changed = dispatch_event(&store, &ev).unwrap();
        assert!(!changed);
    }

    #[test]
    fn dispatch_unknown_event_is_noop() {
        let store = PositionStore::open(&temp_dir()).unwrap();
        let ev = UserStreamEvent::Unknown {
            raw: serde_json::json!({"weird":"thing"}),
        };
        let changed = dispatch_event(&store, &ev).unwrap();
        assert!(!changed);
    }

    #[test]
    fn dispatch_batch_counts_state_changes() {
        let store = PositionStore::open(&temp_dir()).unwrap();
        store.record_open("a", "btc-1", "yes", 5.0, 0.45).unwrap();
        store.record_open("b", "btc-1", "yes", 5.0, 0.45).unwrap();
        store.record_open("c", "btc-1", "yes", 5.0, 0.45).unwrap();

        let events = vec![
            UserStreamEvent::Trade { order_id: "a".into(), asset_id: "x".into(),
                price: 0.45, size: 11.0, side: "BUY".into(), status: "MATCHED".into(),
                timestamp_ms: 1 },
            UserStreamEvent::Trade { order_id: "b".into(), asset_id: "x".into(),
                price: 0.46, size: 10.0, side: "BUY".into(), status: "FAILED".into(),
                timestamp_ms: 2 },
            UserStreamEvent::Unknown { raw: serde_json::Value::Null },
            UserStreamEvent::Trade { order_id: "c".into(), asset_id: "x".into(),
                price: 0.45, size: 11.0, side: "BUY".into(), status: "CONFIRMED".into(),
                timestamp_ms: 3 },
        ];
        let n = dispatch_batch(&store, &events);
        // a and c match → 2 state changes; b failed → 0
        assert_eq!(n, 2);
    }

    // ── Subscribe message ─────────────────────────────────────────────────

    #[test]
    fn subscribe_message_includes_creds_and_markets() {
        let creds = sample_creds();
        let msg = build_subscribe_msg(&creds, &["0xa".into(), "0xb".into()]).unwrap();
        let v: Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(v["auth"]["apiKey"],     "00000000-1111-2222-3333-444444444444");
        assert_eq!(v["auth"]["passphrase"], "passphrase");
        assert_eq!(v["type"],               "user");
        assert_eq!(v["markets"][0],         "0xa");
        assert_eq!(v["markets"][1],         "0xb");
    }
}
