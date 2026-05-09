// EDGE-C — Sportsbook devig overlay signal layer.
//
// Reads `data/sportsbook_devig.json` (written by
// scripts/sportsbook_devig.py) and emits a trade signal whenever
// Polymarket's price diverges from Pinnacle's no-vig fair odds by
// more than MIN_EDGE.
//
// Why a sidecar: Pinnacle does not have a public Rust SDK. TheOddsAPI
// is a paid HTTP service and the bot's hot path should not be doing
// its rate-limiting / pagination directly.
//
// The Rust runner uses the same dispatch path as Strategy 1 (prop
// signals): we synthesize PolyMarket entries so executor's
// resolve_target works unchanged, and we re-use SignalDecision::Oracle.

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::signals::{Direction, SignalDecision};
use crate::signals::oracle_arb::OracleArbSignal;
use crate::streams::polymarket::PolyMarket;

/// Default location the python sidecar writes to.
pub const DEFAULT_DEVIG_PATH: &str = "data/sportsbook_devig.json";

/// Sidecar refreshes every 5 min; suppress signals after 30 min stale.
pub const STALE_AFTER_MS: u64 = 30 * 60 * 1000;

/// Minimum edge after devig before we trade. Pinnacle's vig is small
/// but real; 3% covers the residual + Polymarket fees + slippage.
pub const MIN_EDGE: f64 = 0.03;

#[derive(Debug, Clone, Deserialize)]
pub struct DevigRow {
    pub market_id:    String,
    pub condition_id: String,
    pub question:     String,
    pub sport_key:    String,
    pub home_team:    String,
    pub away_team:    String,
    pub matched_book: String,
    pub yes_token_id: String,
    pub no_token_id:  String,
    pub end_date_ms:  i64,
    pub yes_ask:      f64,
    pub yes_bid:      f64,
    pub fair_yes:     f64,
    pub edge_yes:     f64,
    pub edge_no:      f64,
    pub liquidity:    f64,
    pub volume:       f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DevigFile {
    pub generated_at_ms: u64,
    pub n_markets:       usize,
    #[serde(default)]
    pub rows:            Vec<DevigRow>,
    #[serde(default)]
    pub note:            String,
}

#[derive(Debug, Default)]
pub struct DevigCache {
    inner:         RwLock<HashMap<String, DevigRow>>,
    last_loaded:   RwLock<u64>,
    file_mtime_ms: RwLock<u64>,
}

impl DevigCache {
    pub fn new() -> Self { Self::default() }

    pub fn reload(&self, path: &Path) -> Result<usize> {
        let meta = match fs::metadata(path) {
            Ok(m) => m,
            Err(_) => return Ok(0),
        };
        let mtime = meta.modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let prev = *self.file_mtime_ms.read().expect("poisoned");
        if mtime <= prev {
            return Ok(self.inner.read().expect("poisoned").len());
        }

        let raw = fs::read_to_string(path)
            .with_context(|| format!("read {}", path.display()))?;
        let parsed: DevigFile = serde_json::from_str(&raw)
            .context("parse devig JSON")?;

        let mut map = HashMap::with_capacity(parsed.rows.len());
        for row in parsed.rows {
            let key = if row.condition_id.is_empty() {
                row.market_id.clone()
            } else {
                row.condition_id.clone()
            };
            map.insert(key, row);
        }
        let n = map.len();
        *self.inner.write().expect("poisoned")         = map;
        *self.last_loaded.write().expect("poisoned")   = parsed.generated_at_ms;
        *self.file_mtime_ms.write().expect("poisoned") = mtime;
        tracing::info!(n_markets = n, "sportsbook devig reloaded");
        Ok(n)
    }

    pub fn lookup(&self, condition_id: &str) -> Option<DevigRow> {
        self.inner.read().expect("poisoned").get(condition_id).cloned()
    }

    pub fn is_stale(&self, now_ms: u64) -> bool {
        let last = *self.last_loaded.read().expect("poisoned");
        if last == 0 { return true; }
        now_ms.saturating_sub(last) > STALE_AFTER_MS
    }

    pub fn len(&self) -> usize {
        self.inner.read().expect("poisoned").len()
    }

    /// Build a Vec<PolyMarket> view of the cache. Same idea as the
    /// projections cache: execution-runner concatenates these into the
    /// markets snapshot so the executor's resolve_target lookup works
    /// unchanged for sportsbook-overlay signals.
    pub fn synthesize_polymarkets(&self) -> Vec<PolyMarket> {
        let map = self.inner.read().expect("poisoned");
        map.values().map(|r| PolyMarket {
            id:             r.condition_id.clone(),
            question:       r.question.clone(),
            up_token_id:    r.yes_token_id.clone(),
            down_token_id:  r.no_token_id.clone(),
            close_time_ms:  r.end_date_ms.max(0) as u64,
            duration_min:   0,
            liquidity_usd:  r.liquidity,
        }).collect()
    }
}

/// Evaluate one row. Returns a SignalDecision::Oracle on either side
/// of the line if devig edge clears MIN_EDGE.
pub fn evaluate_devig(row: &DevigRow, now_ms: u64) -> SignalDecision {
    let end_ms = row.end_date_ms.max(0) as u64;
    if now_ms >= end_ms { return SignalDecision::None; }
    if row.yes_ask <= 0.0 || row.yes_ask >= 1.0 { return SignalDecision::None; }

    let secs_to_close = (end_ms.saturating_sub(now_ms) / 1_000) as f64;

    // Sanity: BOTH edges shouldn't exceed MIN_EDGE simultaneously.
    // Pinnacle's no-vig probabilities sum to 1, so by construction
    // edge_yes + edge_no = (yes_bid - yes_ask) ≈ -spread (slightly
    // negative). If the sidecar reports both > MIN_EDGE the input
    // is internally inconsistent — refuse the trade rather than
    // pick the larger one and bet wrong-direction.
    if row.edge_yes >= MIN_EDGE && row.edge_no >= MIN_EDGE {
        tracing::warn!(
            condition_id = %row.condition_id,
            edge_yes = row.edge_yes,
            edge_no  = row.edge_no,
            "sportsbook_devig: dual-edge inconsistency — refusing signal"
        );
        return SignalDecision::None;
    }

    // Buy YES if Pinnacle says the home/correct side is underpriced
    if row.edge_yes >= MIN_EDGE {
        return SignalDecision::Oracle(OracleArbSignal {
            direction:          Direction::Up,
            edge:               row.edge_yes,
            fair_value:         row.fair_yes,
            market_price:       row.yes_ask,
            confidence:         (row.edge_yes / 0.20).clamp(0.0, 1.0),
            time_to_close_secs: secs_to_close,
        });
    }
    // Buy NO if the OTHER side is underpriced
    if row.edge_no >= MIN_EDGE {
        let no_ask = (1.0 - row.yes_bid).clamp(0.01, 0.99);
        return SignalDecision::Oracle(OracleArbSignal {
            direction:          Direction::Down,
            edge:               row.edge_no,
            fair_value:         1.0 - row.fair_yes,
            market_price:       no_ask,
            confidence:         (row.edge_no / 0.20).clamp(0.0, 1.0),
            time_to_close_secs: secs_to_close,
        });
    }
    SignalDecision::None
}

/// Convenience: evaluate every cached row, return all firing signals.
pub fn evaluate_all_devig(cache: &DevigCache) -> Vec<(String, SignalDecision)> {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    if cache.is_stale(now_ms) {
        tracing::debug!("devig cache stale — suppressing sportsbook signals");
        return Vec::new();
    }
    let map = cache.inner.read().expect("poisoned");
    let mut out = Vec::new();
    for (cid, row) in map.iter() {
        let dec = evaluate_devig(row, now_ms);
        if !matches!(dec, SignalDecision::None) {
            out.push((cid.clone(), dec));
        }
    }
    out
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_path() -> PathBuf {
        std::env::temp_dir().join(format!("constantine-devig-{}.json",
            uuid::Uuid::new_v4()))
    }

    fn sample_row(edge_yes: f64, edge_no: f64) -> DevigRow {
        DevigRow {
            market_id:    "m1".into(),
            condition_id: "0xc1".into(),
            question:     "Lakers vs Warriors winner".into(),
            sport_key:    "basketball_nba".into(),
            home_team:    "Lakers".into(),
            away_team:    "Warriors".into(),
            matched_book: "pinnacle".into(),
            yes_token_id: "tok-y".into(),
            no_token_id:  "tok-n".into(),
            end_date_ms:  100_000_000,
            yes_ask:      0.45,
            yes_bid:      0.43,
            fair_yes:     0.50,
            edge_yes,
            edge_no,
            liquidity:    1500.0,
            volume:       3000.0,
        }
    }

    #[test]
    fn fires_buy_yes_when_yes_edge_above_threshold() {
        let row = sample_row(0.05, 0.0);
        let d = evaluate_devig(&row, 1_000);
        match d {
            SignalDecision::Oracle(s) => {
                assert!(matches!(s.direction, Direction::Up));
                assert!(s.edge >= 0.05);
            }
            _ => panic!("expected Oracle Up signal, got {:?}", d),
        }
    }

    #[test]
    fn fires_buy_no_when_no_edge_above_threshold() {
        let row = sample_row(0.0, 0.05);
        let d = evaluate_devig(&row, 1_000);
        match d {
            SignalDecision::Oracle(s) => assert!(matches!(s.direction, Direction::Down)),
            _ => panic!("expected Oracle Down signal"),
        }
    }

    #[test]
    fn no_signal_when_both_edges_below_threshold() {
        let row = sample_row(0.01, 0.01);
        let d = evaluate_devig(&row, 1_000);
        assert!(matches!(d, SignalDecision::None));
    }

    #[test]
    fn no_signal_when_both_edges_above_threshold() {
        // Inconsistent input: sidecar reports both YES + NO underpriced
        // simultaneously, which can't be true with no-vig probabilities
        // summing to 1. Refuse the signal rather than picking arbitrarily.
        let row = sample_row(0.10, 0.10);
        let d = evaluate_devig(&row, 1_000);
        assert!(matches!(d, SignalDecision::None),
            "dual-edge inconsistency must produce no signal");
    }

    #[test]
    fn no_signal_after_market_closes() {
        let row = sample_row(0.10, 0.0);
        let d = evaluate_devig(&row, row.end_date_ms as u64 + 1);
        assert!(matches!(d, SignalDecision::None));
    }

    #[test]
    fn no_signal_when_yes_ask_invalid() {
        let mut row = sample_row(0.10, 0.0);
        row.yes_ask = 0.0;
        let d = evaluate_devig(&row, 1_000);
        assert!(matches!(d, SignalDecision::None));
    }

    #[test]
    fn cache_reload_populates_map() {
        let path = temp_path();
        let payload = r#"{
            "generated_at_ms": 1000,
            "n_markets": 1,
            "rows": [{
                "market_id": "m1",
                "condition_id": "0xc1",
                "question": "Lakers vs Warriors winner",
                "sport_key": "basketball_nba",
                "home_team": "Lakers",
                "away_team": "Warriors",
                "matched_book": "pinnacle",
                "yes_token_id": "tok-y",
                "no_token_id":  "tok-n",
                "end_date_ms": 100000000,
                "yes_ask": 0.45,
                "yes_bid": 0.43,
                "fair_yes": 0.50,
                "edge_yes": 0.05,
                "edge_no":  0.00,
                "liquidity": 1500.0,
                "volume":    3000.0
            }]
        }"#;
        std::fs::write(&path, payload).unwrap();
        let cache = DevigCache::new();
        let n = cache.reload(&path).unwrap();
        assert_eq!(n, 1);
        assert!(cache.lookup("0xc1").is_some());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn cache_handles_empty_rows() {
        let path = temp_path();
        let payload = r#"{"generated_at_ms": 0, "n_markets": 0, "rows": [], "note": "no key"}"#;
        std::fs::write(&path, payload).unwrap();
        let cache = DevigCache::new();
        assert_eq!(cache.reload(&path).unwrap(), 0);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn cache_is_stale_when_never_loaded() {
        let cache = DevigCache::new();
        assert!(cache.is_stale(1_000_000));
    }

    #[test]
    fn cache_is_stale_after_30_min() {
        let cache = DevigCache::new();
        *cache.last_loaded.write().unwrap() = 1_000;
        assert!(cache.is_stale(1_000 + 31 * 60 * 1000));
        assert!(!cache.is_stale(1_000 + 5 * 60 * 1000));
    }

    #[test]
    fn synthesize_polymarkets_round_trips() {
        let cache = DevigCache::new();
        let path = temp_path();
        let payload = r#"{
            "generated_at_ms": 1000, "n_markets": 1,
            "rows": [{
                "market_id":"m1","condition_id":"0xc1","question":"q",
                "sport_key":"s","home_team":"H","away_team":"A","matched_book":"pinnacle",
                "yes_token_id":"y","no_token_id":"n","end_date_ms":1,
                "yes_ask":0.5,"yes_bid":0.49,"fair_yes":0.5,"edge_yes":0,"edge_no":0,
                "liquidity":1.0,"volume":1.0
            }]
        }"#;
        std::fs::write(&path, payload).unwrap();
        cache.reload(&path).unwrap();
        let mkts = cache.synthesize_polymarkets();
        assert_eq!(mkts.len(), 1);
        assert_eq!(mkts[0].id, "0xc1");
        assert_eq!(mkts[0].up_token_id, "y");
        assert_eq!(mkts[0].down_token_id, "n");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn evaluate_all_returns_empty_when_stale() {
        let cache = DevigCache::new();
        assert!(evaluate_all_devig(&cache).is_empty());
    }
}
