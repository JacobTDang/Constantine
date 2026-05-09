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

fn default_outcome_index() -> i32 { -1 }

#[derive(Debug, Clone, Deserialize)]
pub struct WhaleTrade {
    pub trade_id:       String,
    pub whale_address:  String,
    pub whale_nickname: String,
    pub market_id:      String,
    pub asset_id:       String,
    pub side:           String,    // "BUY" / "SELL"
    pub outcome:        String,    // text label: "Yes" / "No" / "Lakers" / ...
    /// 0 = first outcome (YES/Up), 1 = second outcome (NO/Down).
    /// `-1` if absent (older log entries pre-dating the dual-source fix).
    /// Always populated by data-api/trades; reliable across binary AND
    /// multi-outcome events (where `outcome` is a candidate name).
    #[serde(default = "default_outcome_index")]
    pub outcome_index:  i32,
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
    ///
    /// Robust to:
    ///   - log rotation / truncation (file shorter than last_offset → reset)
    ///   - \r\n vs \n line endings (uses raw byte counts, not str::len)
    ///   - partial trailing line (we only advance offset to last full
    ///     newline; the unfinished tail is re-read on the next refresh
    ///     once the writer flushes its newline)
    pub fn refresh(&self, path: &Path) -> Result<usize> {
        if !path.exists() { return Ok(0); }
        let file = OpenOptions::new().read(true).open(path)?;
        let file_len = file.metadata().map(|m| m.len()).unwrap_or(0);

        let last = *self.last_offset.read().expect("poisoned");
        // Rotation / truncation: file shrunk below our offset. Reset and
        // reread from the start. trade_id dedup keeps us from re-emitting
        // anything we've already seen this process lifetime.
        if file_len < last {
            tracing::info!(
                path = %path.display(), prev_offset = last, file_len,
                "whale_trades.jsonl rotated/truncated; resetting offset"
            );
            *self.last_offset.write().expect("poisoned") = 0;
        }

        let mut reader = BufReader::new(file);
        let start = *self.last_offset.read().expect("poisoned");
        reader.seek(SeekFrom::Start(start))?;

        let mut new_trades = Vec::new();
        let mut last_complete_offset: u64 = start;
        let mut buf: Vec<u8> = Vec::with_capacity(512);

        loop {
            buf.clear();
            // read_until returns the actual bytes consumed (including the
            // newline if found) — handles \r\n correctly because we count
            // raw bytes rather than str::len of the trimmed result.
            let n = match reader.read_until(b'\n', &mut buf) {
                Ok(0) => break,             // EOF
                Ok(n) => n,
                Err(_) => break,
            };
            // No newline in the chunk → partial trailing line. Don't
            // advance offset past the start of this chunk so we re-read
            // it after the writer flushes a newline.
            if !buf.ends_with(b"\n") { break; }
            // Strip trailing \r\n or \n for parsing
            let mut end = n;
            if end > 0 && buf[end - 1] == b'\n' { end -= 1; }
            if end > 0 && buf[end - 1] == b'\r' { end -= 1; }
            let line_bytes = &buf[..end];

            last_complete_offset = last_complete_offset.saturating_add(n as u64);

            if line_bytes.is_empty() { continue; }
            let trade: WhaleTrade = match serde_json::from_slice(line_bytes) {
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
        *self.last_offset.write().expect("poisoned") = last_complete_offset;
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

    // Direction inference: prefer outcome_index (reliable for both
    // binary and multi-outcome events), fall back to outcome string.
    let direction = if t.outcome_index == 0 { Direction::Up }
        else if t.outcome_index == 1 { Direction::Down }
        else {
            match t.outcome.to_lowercase().as_str() {
                "yes" => Direction::Up,
                "no"  => Direction::Down,
                _     => return SignalDecision::None,
            }
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
            "outcome_index": t.outcome_index,
            "price": t.price, "size_shares": t.size_shares,
            "size_usd": t.size_usd, "ts_ms": t.ts_ms,
        })).unwrap()).unwrap();
    }

    fn sample_trade(side: &str, outcome: &str, price: f64, size_usd: f64,
                    ts_ms: u64) -> WhaleTrade {
        sample_trade_idx(side, outcome, -1, price, size_usd, ts_ms)
    }

    fn sample_trade_idx(side: &str, outcome: &str, outcome_index: i32,
                        price: f64, size_usd: f64, ts_ms: u64) -> WhaleTrade {
        WhaleTrade {
            trade_id:       format!("t-{}", uuid::Uuid::new_v4()),
            whale_address:  "0xabc".into(),
            whale_nickname: "Theo4".into(),
            market_id:      "0xCONDITION".into(),
            asset_id:       "tok-y".into(),
            side:           side.into(),
            outcome:        outcome.into(),
            outcome_index,
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

    #[test]
    fn refresh_handles_crlf_line_endings() {
        // Windows sidecar may write \r\n. The refresh must count raw
        // bytes (not str::len) so the offset doesn't drift by 1/line.
        let path = temp_path();
        let cache = WhaleCache::new();
        let now = chrono::Utc::now().timestamp_millis() as u64;
        let t1 = sample_trade("BUY", "yes", 0.55, 5_000.0, now);
        let t2 = sample_trade("BUY", "no",  0.40, 6_000.0, now);
        // Manually craft CRLF JSONL
        let line1 = serde_json::to_string(&serde_json::json!({
            "trade_id": t1.trade_id, "whale_address": t1.whale_address,
            "whale_nickname": t1.whale_nickname, "market_id": t1.market_id,
            "asset_id": t1.asset_id, "side": t1.side, "outcome": t1.outcome,
            "outcome_index": t1.outcome_index,
            "price": t1.price, "size_shares": t1.size_shares,
            "size_usd": t1.size_usd, "ts_ms": t1.ts_ms,
        })).unwrap();
        let line2 = serde_json::to_string(&serde_json::json!({
            "trade_id": t2.trade_id, "whale_address": t2.whale_address,
            "whale_nickname": t2.whale_nickname, "market_id": t2.market_id,
            "asset_id": t2.asset_id, "side": t2.side, "outcome": t2.outcome,
            "outcome_index": t2.outcome_index,
            "price": t2.price, "size_shares": t2.size_shares,
            "size_usd": t2.size_usd, "ts_ms": t2.ts_ms,
        })).unwrap();
        std::fs::write(&path, format!("{line1}\r\n{line2}\r\n")).unwrap();
        let n = cache.refresh(&path).unwrap();
        assert_eq!(n, 2);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn refresh_resets_offset_on_rotation() {
        // Simulate logrotate: write file, refresh, truncate file, write
        // fresh content. The cache should detect file_len < last_offset
        // and re-read from byte 0.
        let path = temp_path();
        let cache = WhaleCache::new();
        let now = chrono::Utc::now().timestamp_millis() as u64;
        let t1 = sample_trade("BUY", "yes", 0.55, 5_000.0, now);
        write_line(&path, &t1);
        let n1 = cache.refresh(&path).unwrap();
        assert_eq!(n1, 1);
        // Simulate rotation: truncate + new content with a different trade_id
        let mut t2 = sample_trade("BUY", "no", 0.40, 6_000.0, now);
        t2.trade_id = "post-rotation".into();
        std::fs::write(&path, "").unwrap();
        write_line(&path, &t2);
        let n2 = cache.refresh(&path).unwrap();
        // dedup keeps t1 as already-seen, but t2 is new → n2 = 1
        assert_eq!(n2, 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn refresh_holds_partial_trailing_line() {
        // Sidecar mid-write: file ends without a newline. We should
        // NOT advance offset past the start of that partial line, so
        // the next refresh (after writer flushes \n) re-reads it.
        let path = temp_path();
        let cache = WhaleCache::new();
        let now = chrono::Utc::now().timestamp_millis() as u64;
        let t = sample_trade("BUY", "yes", 0.55, 5_000.0, now);
        let line = serde_json::to_string(&serde_json::json!({
            "trade_id": t.trade_id, "whale_address": t.whale_address,
            "whale_nickname": t.whale_nickname, "market_id": t.market_id,
            "asset_id": t.asset_id, "side": t.side, "outcome": t.outcome,
            "outcome_index": t.outcome_index,
            "price": t.price, "size_shares": t.size_shares,
            "size_usd": t.size_usd, "ts_ms": t.ts_ms,
        })).unwrap();
        // Write WITHOUT trailing newline
        std::fs::write(&path, &line).unwrap();
        let n1 = cache.refresh(&path).unwrap();
        assert_eq!(n1, 0, "partial line must not be parsed yet");
        // Now finalize the line by appending the newline
        let f = OpenOptions::new().append(true).open(&path).unwrap();
        use std::io::Write as _;
        (&f).write_all(b"\n").unwrap();
        let n2 = cache.refresh(&path).unwrap();
        assert_eq!(n2, 1, "completed line should now parse");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn evaluate_uses_outcome_index_when_outcome_string_unknown() {
        // Multi-outcome event: outcome label is a candidate name like
        // "Lakers", but outcome_index=0 means YES side. We should follow
        // it as Direction::Up despite the unknown string.
        let now = chrono::Utc::now().timestamp_millis() as u64;
        let t = sample_trade_idx("BUY", "Lakers", 0, 0.55, 5_000.0, now);
        match evaluate_whale_trade(&t, now) {
            SignalDecision::Oracle(s) => assert_eq!(s.direction, Direction::Up),
            d => panic!("expected Oracle Up via outcome_index, got {:?}", d),
        }

        // outcome_index=1 with multi-outcome label → Down
        let t2 = sample_trade_idx("BUY", "Warriors", 1, 0.45, 5_000.0, now);
        match evaluate_whale_trade(&t2, now) {
            SignalDecision::Oracle(s) => assert_eq!(s.direction, Direction::Down),
            d => panic!("expected Oracle Down via outcome_index, got {:?}", d),
        }
    }

    #[test]
    fn evaluate_falls_back_to_outcome_string_when_index_missing() {
        // Old log entry style: outcome_index = -1 (the default), but
        // outcome string is "Yes" — should still fire as Up.
        let now = chrono::Utc::now().timestamp_millis() as u64;
        let t = sample_trade_idx("BUY", "Yes", -1, 0.55, 5_000.0, now);
        match evaluate_whale_trade(&t, now) {
            SignalDecision::Oracle(s) => assert_eq!(s.direction, Direction::Up),
            d => panic!("expected Oracle Up via fallback, got {:?}", d),
        }
        // outcome_index missing AND outcome unknown → None (preserves
        // old behavior — no random guessing on opaque rows).
        let t2 = sample_trade_idx("BUY", "Lakers", -1, 0.55, 5_000.0, now);
        assert!(matches!(evaluate_whale_trade(&t2, now), SignalDecision::None));
    }
}
