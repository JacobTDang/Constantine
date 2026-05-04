pub mod binance;
pub mod liquidations;
pub mod polymarket;

/// Central event type broadcast to all consumers.
/// Variants are defined upfront — only new ones are appended as streams are added.
#[derive(Debug, Clone)]
pub enum StreamEvent {
    // ── B1: OKX spot trades ───────────────────────────────────────────────
    Trade {
        price:        f64,
        quantity:     f64,
        timestamp_ms: u64,
    },

    // ── B3: OKX perp mark price (mark-price channel) ──────────────────────
    MarkPrice {
        mark_price:   f64,
        timestamp_ms: u64,
    },

    // ── B3: OKX perp funding rate (funding-rate channel) ──────────────────
    FundingRate {
        funding_rate:          f64,
        next_funding_time_ms:  u64,
        timestamp_ms:          u64,
    },

    // ── B4: OKX open interest ─────────────────────────────────────────────
    OpenInterest {
        open_interest: f64,   // in BTC (oiCcy field)
        timestamp_ms:  u64,
    },

    // ── B5: OKX 1-min klines (business WS) ───────────────────────────────
    Kline {
        open:         f64,
        high:         f64,
        low:          f64,
        close:        f64,
        volume:       f64,    // in contracts
        is_closed:    bool,   // confirm == "1"
        open_time_ms: u64,
    },

    // ── B6: OKX liquidation orders ────────────────────────────────────────
    Liquidation {
        side:         LiqSide,
        price:        f64,    // bankruptcy price
        quantity:     f64,    // contracts
        timestamp_ms: u64,
    },

    // ── D2: Polymarket RTDS ───────────────────────────────────────────────
    BinancePriceFromPoly {
        price:        f64,
        timestamp_ms: u64,
    },
    ChainlinkPrice {
        price:        f64,
        timestamp_ms: u64,
    },
    Comment {
        text:         String,
        market_id:    String,
        timestamp_ms: u64,
    },

    // ── D4: Polymarket CLOB ───────────────────────────────────────────────
    /// Book snapshot for a single CLOB asset (Up token OR Down token).
    /// Compute layer correlates Up + Down books per `market_id`.
    ClobBook {
        market_id:        String, // parent condition ID (0x...)
        asset_id:         String, // CLOB token ID (Up or Down)
        best_bid:         f64,
        best_ask:         f64,
        best_bid_size:    f64,
        best_ask_size:    f64,
        last_trade_price: f64,    // 0.0 when unknown
        timestamp_ms:     u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiqSide {
    Long,  // SELL-side force order — long position liquidated
    Short, // BUY-side force order — short position liquidated
}

// ── B7 tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::broadcast;

    #[tokio::test]
    async fn broadcast_no_drops_under_burst_of_1000() {
        let (tx, mut rx) = broadcast::channel::<StreamEvent>(1024);

        for i in 0..1000u64 {
            tx.send(StreamEvent::Trade {
                price:        43_000.0,
                quantity:     0.001,
                timestamp_ms: i,
            })
            .expect("send failed");
        }

        let mut count = 0usize;
        while let Ok(_) = rx.try_recv() {
            count += 1;
        }

        assert_eq!(count, 1000, "all 1000 events must arrive without drops");
    }

    #[tokio::test]
    async fn multiple_subscribers_each_see_all_events() {
        let (tx, mut rx1) = broadcast::channel::<StreamEvent>(256);
        let mut rx2 = tx.subscribe();

        for i in 0..100u64 {
            tx.send(StreamEvent::Trade {
                price:        1.0,
                quantity:     1.0,
                timestamp_ms: i,
            })
            .unwrap();
        }

        let mut c1 = 0usize;
        while let Ok(_) = rx1.try_recv() { c1 += 1; }
        let mut c2 = 0usize;
        while let Ok(_) = rx2.try_recv() { c2 += 1; }

        assert_eq!(c1, 100);
        assert_eq!(c2, 100);
    }
}
