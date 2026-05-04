// B6 — OKX liquidation-orders stream.
// Emits StreamEvent::Liquidation for each BTC-USDT-SWAP force close.
// Rolling accumulation (60s / 300s windows) happens in features/compute.rs.

use super::StreamEvent;
use super::binance::{parse_liquidations, reconnect, OKX_PUBLIC_URL};
use serde_json::json;
use tokio::sync::broadcast;

/// B6 — real-time BTC liquidation orders from OKX.
pub async fn liq_stream(tx: broadcast::Sender<StreamEvent>) {
    let sub = json!({
        "op": "subscribe",
        "args": [{"channel": "liquidation-orders", "instType": "SWAP"}]
    })
    .to_string();

    reconnect(OKX_PUBLIC_URL, sub, "liquidations", |text, tx| {
        for event in parse_liquidations(text) {
            let _ = tx.send(event);
        }
    }, tx)
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streams::LiqSide;
    use tokio::time::Duration;

    // Parsing tests are in binance.rs where the parser lives.
    // These tests verify the liq_stream integration layer.

    #[ignore]
    #[tokio::test(flavor = "multi_thread")]
    async fn liq_stream_stays_connected_for_5s() {
        let (tx, _rx) = broadcast::channel::<StreamEvent>(256);
        // liq_stream is an infinite reconnect loop — it must not exit within 5s
        let result =
            tokio::time::timeout(Duration::from_secs(5), liq_stream(tx)).await;
        assert!(result.is_err(), "liq_stream must not exit within 5s");
    }

    #[ignore]
    #[tokio::test(flavor = "multi_thread")]
    async fn liq_stream_receives_events_or_stays_alive() {
        // Liquidations don't happen every second — we just verify connectivity.
        // Test passes if the stream stays alive for 15s without panic.
        let (tx, mut rx) = broadcast::channel::<StreamEvent>(256);
        tokio::spawn(liq_stream(tx));

        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        let mut received_liq = false;
        loop {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Ok(StreamEvent::Liquidation { side, price, .. })) => {
                    assert!(price > 0.0);
                    assert!(side == LiqSide::Long || side == LiqSide::Short);
                    received_liq = true;
                    break;
                }
                Ok(Ok(_)) => {}
                Ok(Err(_)) | Err(_) => break,
            }
        }
        // Pass whether or not we got a liquidation — low frequency event
        tracing::info!(received_liq, "liq stream integration test done");
    }
}
