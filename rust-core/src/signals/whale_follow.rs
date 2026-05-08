// EDGE-D — Whale-follow signal layer.
//
// Reads `data/whale_trades.jsonl` (written by scripts/whale_tracker.py)
// and emits a follow-trade signal whenever a tracked whale's recent
// position is large enough and recent enough to have edge available.
//
// Tail-only reader: we remember the last byte offset we processed and
// only parse new lines on each tick. This means the JSONL grows
// forever (rotate manually with `gzip` once a month) but reads stay
// O(new lines).
//
// Signals:
//   - Pulled when whale BUYs YES at price p > FOLLOW_MIN_PRICE_LOWER and
//     trade_size_usd >= FOLLOW_MIN_SIZE_USD
//   - Trade must be < FOLLOW_MAX_AGE_SECS (default 5 min) old to fire;
//     by 5 minutes the follow-edge has decayed too much
//
// Why pull-based instead of execute-on-every-trade: we want the
// runner's existing risk + cooldown gates to apply normally.

use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::Path;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use serde::Deserialize;

use crate::signals::{Direction, SignalDecision};
use crate::signals::oracle_arb::OracleArbSignal;

pub const DEFAULT_WHALE_TRADES_PATH: &str = "data/whale_trades.jsonl";

/// Min trade size to follow. Smaller whale trades are noise.
pub const FOLLOW_MIN_SIZE_USD: f64 = 2_000.0;

/// Trades older than this are stale -- price has likely already moved.
pub const FOLLOW_MAX_AGE_SECS: u64 = 300;

/// Lower bound for follow price. Anything <0.10 is a long-shot retail
/// punt with negative expected value across the population.
pub const FOLLOW_MIN_PRICE_LOWER: f64 = 0.10;

/// Upper bound. >0.90 means the whale paid near-cap; little upside.
pub const FOLLOW_MAX_PRICE_UPPER: f64 = 0.90;

#[derive(Debug, Clone, Deserialize)]
pub struct WhaleTrade {
    pub trade_id:       String,
    pub whale_address:  String,
    pub whale_nickname: String,
    pub market_id:      String,
    pub asset_id:       String,
    pub side:           String,    // "BUY" / "SELL"
    pub outcome:        String,    // "yes" / "no" / ""
    pub price:          f64,
    pub size_shares:    f64,
    pub size_usd:       f64,
    pub ts_ms:          u64,
}

/// Tail-reading cache. Keeps last byte offset and a dedup set.
#[derive(Debug, Default)]
pub struct WhaleCache {
    last_offset:   RwLock<u64>,
    seen_ids:      RwLock<HashSet<String>>,
    pending:       RwLock<Vec<WhaleTrade>>,
}

impl WhaleCache {
    pub fn new() -> Self { Self::default() }

    /// Read any new lines appended since the last call. Returns the
    /// number of new trades parsed.
    pub fn refresh(&self, path: &Path) -> Result<usize> {
        if !path.exists() { return Ok(0); }
        let file = OpenOptions::new().read(true).open(path)?;
        let mut reader = BufReader::new(file);
        let last = *self.last_offset.read().expect("poisoned");
        reader.seek(SeekFrom::Start(last))?;

        let mut new_trades = Vec::new();
        let mut bytes_read: u64 = 0;
        for line in reader.lines() {
            let line: String = match line {
                Ok(l) => l,
                Err(_) => continue,
            };
            bytes_read += line.len() as u64 + 1;     // +1 for newline
            if line.is_empty() { continue; }
            let trade: WhaleTrade = match serde_json::from_str(&line) {
                Ok(t) => t,
                Err(e) => {
                    tracing::debug!(error = %e, "skip malformed whale row");
                    continue;
                }
            };
            // Dedup by trade_id (sidecar already does, but defensive)
            let mut seen = self.seen_ids.write().expect("poisoned");
            if !seen.insert(trade.trade_id.clone()) { continue; }
            drop(seen);
            new_trades.push(trade);
        }
        *self.last_offset.write().expect("poisoned") = last + bytes_read;
        let n = new_trades.len();
        if n > 0 {
            self.pending.write().expect("poisoned").extend(new_trades);
        }
        Ok(n)
    }

    /// Drain pending trades — caller is expected to evaluate them and
    /// route them through the normal cooldown / risk gates.
    pub fn drain(&self) -> Vec<WhaleTrade> {
        std::mem::take(&mut *self.pending.write().expect("poisoned"))
    }

    pub fn pending_len(&self) -> usize {
        self.pending.read().expect("poisoned").len()
    }
}

/// Convert a whale BUY into a SignalDecision::Oracle. The "fair value"
/// is the whale's price -- we don't know better, but their edge is
/// imputed from the population statistic that profitable wallets exist.
/// Edge = our notional buffer above the whale's price (we'll buy at the
/// whale price + a few cents at most).
pub fn evaluate_whale_trade(t: &WhaleTrade, now_ms: u64) -> SignalDecision {
    if t.side.to_uppercase() != "BUY" { return SignalDecision::None; }
    if t.size_usd < FOLLOW_MIN_SIZE_USD { return SignalDecision::None; }
    if t.price < FOLLOW_MIN_PRICE_LOWER || t.price > FOLLOW_MAX_PRICE_UPPER {
        return SignalDecision::None;
    }
    let age_secs = now_ms.saturating_sub(t.ts_ms) / 1_000;
    if age_secs > FOLLOW_MAX_AGE_SECS { return SignalDecision::None; }

    // Direction inference: outcome=="yes" -> Up, outcome=="no" -> Down.
    // If outcome is missing, skip rather than guess.
    let direction = match t.outcome.to_lowercase().as_str() {
        "yes" => Direction::Up,
        "no"  => Direction::Down,
        _     => return SignalDecision::None,
    };

    // We're following — our "fair_value" is the whale's price; we'd
    // pay up to that minus a small slippage budget.
    SignalDecision::Oracle(OracleArbSignal {
        direction,
        edge:               0.02,      // assumed 2% follow edge; refined post-hoc
        fair_value:         t.price,
        market_price:       t.price,
        confidence:         (t.size_usd / 50_000.0).clamp(0.0, 1.0),
        time_to_close_secs: 0.0,       // unknown without market metadata
    })
}

/// Drain the cache and evaluate every pending whale trade. Returns
/// (market_id, decision) pairs the runner can dispatch through its
/// normal cooldown loop.
pub fn evaluate_all_whales(cache: &WhaleCache) -> Vec<(String, SignalDecision)> {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let pending = cache.drain();
    let mut out = Vec::with_capacity(pending.len());
    for t in pending {
        let dec = evaluate_whale_trade(&t, now_ms);
        if !matches!(dec, SignalDecision::None) {
            out.push((t.market_id.clone(), dec));
        }
    }
    out
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::path::PathBuf;

    fn temp_path() -> PathBuf {
        std::env::temp_dir().join(format!("constantine-whale-{}.jsonl",
            uuid::Uuid::new_v4()))
    }

    fn write_line(path: &Path, t: &WhaleTrade) {
        let mut f = OpenOptions::new().create(true).append(true).open(path).unwrap();
        writeln!(f, "{}", serde_json::to_string(&serde_json::json!({
            "trade_id": t.trade_id, "whale_address": t.whale_address,
            "whale_nickname": t.whale_nickname, "market_id": t.market_id,
            "asset_id": t.asset_id, "side": t.side, "outcome": t.outcome,
            "price": t.price, "size_shares": t.size_shares,
            "size_usd": t.size_usd, "ts_ms": t.ts_ms,
        })).unwrap()).unwrap();
    }

    fn sample_trade(side: &str, outcome: &str, price: f64, size_usd: f64,
                    ts_ms: u64) -> WhaleTrade {
        WhaleTrade {
            trade_id:       format!("t-{}", uuid::Uuid::new_v4()),
            whale_address:  "0xabc".into(),
            whale_nickname: "Theo4".into(),
            market_id:      "0xCONDITION".into(),
            asset_id:       "tok-y".into(),
            side:           side.into(),
            outcome:        outcome.into(),
            price,
            size_shares:    size_usd / price.max(0.01),
            size_usd,
            ts_ms,
        }
    }

    #[test]
    fn evaluate_buy_yes_fires_up() {
        let t = sample_trade("BUY", "yes", 0.55, 5_000.0,
            chrono::Utc::now().timestamp_millis() as u64);
        let now = chrono::Utc::now().timestamp_millis() as u64;
        match evaluate_whale_trade(&t, now) {
            SignalDecision::Oracle(s) => assert_eq!(s.direction, Direction::Up),
            d => panic!("expected Oracle Up, got {:?}", d),
        }
    }

    #[test]
    fn evaluate_buy_no_fires_down() {
        let t = sample_trade("BUY", "no", 0.45, 5_000.0,
            chrono::Utc::now().timestamp_millis() as u64);
        let now = chrono::Utc::now().timestamp_millis() as u64;
        match evaluate_whale_trade(&t, now) {
            SignalDecision::Oracle(s) => assert_eq!(s.direction, Direction::Down),
            d => panic!("expected Oracle Down, got {:?}", d),
        }
    }

    #[test]
    fn evaluate_skips_sell_side() {
        let t = sample_trade("SELL", "yes", 0.55, 5_000.0,
            chrono::Utc::now().timestamp_millis() as u64);
        let now = chrono::Utc::now().timestamp_millis() as u64;
        assert!(matches!(evaluate_whale_trade(&t, now), SignalDecision::None));
    }

    #[test]
    fn evaluate_skips_small_trade() {
        let t = sample_trade("BUY", "yes", 0.55, 100.0,
            chrono::Utc::now().timestamp_millis() as u64);
        let now = chrono::Utc::now().timestamp_millis() as u64;
        assert!(matches!(evaluate_whale_trade(&t, now), SignalDecision::None));
    }

    #[test]
    fn evaluate_skips_stale_trade() {
        let now = chrono::Utc::now().timestamp_millis() as u64;
        let t = sample_trade("BUY", "yes", 0.55, 5_000.0, now - 10 * 60 * 1000);
        assert!(matches!(evaluate_whale_trade(&t, now), SignalDecision::None));
    }

    #[test]
    fn evaluate_skips_extreme_prices() {
        let now = chrono::Utc::now().timestamp_millis() as u64;
        let t1 = sample_trade("BUY", "yes", 0.05, 5_000.0, now);
        assert!(matches!(evaluate_whale_trade(&t1, now), SignalDecision::None));
        let t2 = sample_trade("BUY", "yes", 0.95, 5_000.0, now);
        assert!(matches!(evaluate_whale_trade(&t2, now), SignalDecision::None));
    }

    #[test]
    fn evaluate_skips_unknown_outcome() {
        let now = chrono::Utc::now().timestamp_millis() as u64;
        let t = sample_trade("BUY", "", 0.55, 5_000.0, now);
        assert!(matches!(evaluate_whale_trade(&t, now), SignalDecision::None));
    }

    #[test]
    fn cache_refresh_picks_up_new_lines() {
        let path = temp_path();
        let cache = WhaleCache::new();
        let now = chrono::Utc::now().timestamp_millis() as u64;
        let t1 = sample_trade("BUY", "yes", 0.55, 5_000.0, now);
        write_line(&path, &t1);
        let n = cache.refresh(&path).unwrap();
        assert_eq!(n, 1);
        assert_eq!(cache.pending_len(), 1);
        let t2 = sample_trade("BUY", "no",  0.40, 6_000.0, now);
        write_line(&path, &t2);
        let n2 = cache.refresh(&path).unwrap();
        assert_eq!(n2, 1);
        assert_eq!(cache.pending_len(), 2);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn cache_dedups_duplicate_ids() {
        let path = temp_path();
        let cache = WhaleCache::new();
        let now = chrono::Utc::now().timestamp_millis() as u64;
        let mut t = sample_trade("BUY", "yes", 0.55, 5_000.0, now);
        t.trade_id = "fixed-id".into();
        write_line(&path, &t);
        write_line(&path, &t);    // same trade_id again
        let _ = cache.refresh(&path).unwrap();
        // Cache stores 1 unique even though 2 lines were appended
        assert_eq!(cache.pending_len(), 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn drain_clears_pending() {
        let cache = WhaleCache::new();
        let now = chrono::Utc::now().timestamp_millis() as u64;
        let t = sample_trade("BUY", "yes", 0.55, 5_000.0, now);
        cache.pending.write().unwrap().push(t);
        assert_eq!(cache.pending_len(), 1);
        let drained = cache.drain();
        assert_eq!(drained.len(), 1);
        assert_eq!(cache.pending_len(), 0);
    }

    #[test]
    fn evaluate_all_returns_empty_when_pending_empty() {
        let cache = WhaleCache::new();
        assert!(evaluate_all_whales(&cache).is_empty());
    }

    #[test]
    fn refresh_handles_missing_file() {
        let cache = WhaleCache::new();
        let nonexistent = std::env::temp_dir().join("definitely-not-here.jsonl");
        assert_eq!(cache.refresh(&nonexistent).unwrap(), 0);
    }

    #[test]
    fn refresh_skips_malformed_lines() {
        let path = temp_path();
        let cache = WhaleCache::new();
        std::fs::write(&path, "not json\n{}\n").unwrap();
        assert_eq!(cache.refresh(&path).unwrap(), 0);
        let _ = std::fs::remove_file(&path);
    }
}
