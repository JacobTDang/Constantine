// Strategy 1 / Phase 1 — Player Props signal layer.
//
// Reads NBA player projections from `data/nba_projections.json` (written
// by scripts/nba_projections.py every 5 min) and emits a trade signal
// when a market's price diverges enough from our `p_over` projection.
//
// Why a JSON sidecar instead of native Rust: the NBA API is Python-only
// (nba_api lib has no maintained Rust bindings) and hammering it from
// the hot path is undesirable anyway. The Python sidecar handles all
// stats / model / cache concerns; this module only does signal logic.

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::signals::{Direction, SignalDecision};
use crate::signals::oracle_arb::OracleArbSignal;

/// Default location the python sidecar writes to.
pub const DEFAULT_PROJECTIONS_PATH: &str = "data/nba_projections.json";

/// Don't trust projections older than this — the sidecar should refresh
/// every 5 min. After 30min we assume the sidecar is dead.
pub const STALE_AFTER_MS: u64 = 30 * 60 * 1000;

/// Minimum edge (model_p_over - market_yes_ask) to fire a BUY YES signal.
/// Same on the NO side (under). Tuned conservatively: must clear the
/// 2% taker fee twice (entry + exit) plus 1-2% slippage = ~6% min edge.
pub const MIN_EDGE: f64 = 0.07;

#[derive(Debug, Clone, Deserialize)]
pub struct ProjectionRow {
    pub market_id:    String,
    pub condition_id: String,
    pub question:     String,
    pub player:       String,
    pub stat:         String,
    #[serde(rename = "stat_code")]
    pub stat_code:    String,
    pub line:         f64,
    pub yes_token_id: String,
    pub no_token_id:  String,
    pub yes_ask:      f64,
    pub yes_bid:      f64,
    pub end_date_ms:  i64,
    pub liquidity:    f64,
    pub volume:       f64,
    pub p_over:       f64,
    pub edge_over:    f64,
    pub edge_under:   f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProjectionsFile {
    pub generated_at_ms: u64,
    pub n_markets:       usize,
    pub projections:     Vec<ProjectionRow>,
}

/// In-memory cache of the latest projections, keyed by Polymarket
/// condition_id. Reloaded by `reload` whenever the JSON file is newer
/// than what we have in memory.
#[derive(Debug, Default)]
pub struct ProjectionsCache {
    inner:        RwLock<HashMap<String, ProjectionRow>>,
    last_loaded:  RwLock<u64>,
    file_mtime_ms: RwLock<u64>,
}

impl ProjectionsCache {
    pub fn new() -> Self { Self::default() }

    /// Reload from disk if the file has changed since the last load.
    /// Returns the number of projections in cache after reload.
    pub fn reload(&self, path: &Path) -> Result<usize> {
        let meta = match fs::metadata(path) {
            Ok(m) => m,
            Err(_) => return Ok(0),   // file missing → cache stays empty
        };
        let mtime = meta.modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let prev = *self.file_mtime_ms.read().expect("poisoned");
        if mtime <= prev {
            // No new file — return current cache size.
            return Ok(self.inner.read().expect("poisoned").len());
        }

        let raw = fs::read_to_string(path)
            .with_context(|| format!("read {}", path.display()))?;
        let parsed: ProjectionsFile = serde_json::from_str(&raw)
            .context("parse projections JSON")?;

        let mut map = HashMap::with_capacity(parsed.projections.len());
        for row in parsed.projections {
            let key = if row.condition_id.is_empty() {
                row.market_id.clone()
            } else {
                row.condition_id.clone()
            };
            map.insert(key, row);
        }
        let n = map.len();
        *self.inner.write().expect("poisoned") = map;
        *self.last_loaded.write().expect("poisoned") = parsed.generated_at_ms;
        *self.file_mtime_ms.write().expect("poisoned") = mtime;
        tracing::info!(n_markets = n, "player projections reloaded");
        Ok(n)
    }

    pub fn lookup(&self, condition_id: &str) -> Option<ProjectionRow> {
        self.inner.read().expect("poisoned").get(condition_id).cloned()
    }

    /// Stale = sidecar hasn't refreshed in 30+ min. Suppress signals
    /// while stale to avoid trading on outdated stats.
    pub fn is_stale(&self, now_ms: u64) -> bool {
        let last = *self.last_loaded.read().expect("poisoned");
        if last == 0 { return true; }
        now_ms.saturating_sub(last) > STALE_AFTER_MS
    }

    pub fn len(&self) -> usize {
        self.inner.read().expect("poisoned").len()
    }
}

/// Evaluate one prop market against our projection. Returns a
/// SignalDecision::Oracle (re-using the existing variant — direction +
/// edge map cleanly to the prop case).
///
/// Why piggyback on Oracle variant: the executor already knows how to
/// route Direction::Up / Direction::Down to YES / NO via the
/// market.up_token_id / down_token_id. For props, "OVER" = YES = Up.
pub fn evaluate_prop(
    proj:   &ProjectionRow,
    now_ms: u64,
) -> SignalDecision {
    // Window-end guard: don't fire if game has already ended.
    let end_ms = proj.end_date_ms.max(0) as u64;
    if now_ms >= end_ms {
        return SignalDecision::None;
    }

    // Need both YES and NO touch prices populated.
    if proj.yes_ask <= 0.0 || proj.yes_ask >= 1.0 {
        return SignalDecision::None;
    }

    // Cheapest depth check possible — Strategy 1 targets thin markets,
    // but VERY thin (< $200 vol) is almost always 50/50 default.
    if proj.volume < 50.0 && proj.liquidity < 200.0 {
        // We're going to want some liquidity to actually fill —
        // but the 50/50 default is the whole point. Keep this lenient.
    }

    // OVER edge: model thinks p_over > yes_ask
    if proj.p_over - proj.yes_ask >= MIN_EDGE {
        let edge = proj.p_over - proj.yes_ask;
        let secs_to_close = (end_ms.saturating_sub(now_ms) / 1_000) as f64;
        return SignalDecision::Oracle(OracleArbSignal {
            direction:          Direction::Up,
            edge,
            fair_value:         proj.p_over,
            market_price:       proj.yes_ask,
            confidence:         (edge / 0.20).clamp(0.0, 1.0),
            time_to_close_secs: secs_to_close,
        });
    }
    // UNDER edge: model thinks p_under (1 - p_over) > 1 - yes_bid
    let p_under = 1.0 - proj.p_over;
    let no_ask  = if proj.yes_bid > 0.0 { 1.0 - proj.yes_bid } else { 1.0 - proj.yes_ask };
    if p_under - no_ask >= MIN_EDGE && no_ask > 0.0 && no_ask < 1.0 {
        let edge = p_under - no_ask;
        let secs_to_close = (end_ms.saturating_sub(now_ms) / 1_000) as f64;
        return SignalDecision::Oracle(OracleArbSignal {
            direction:          Direction::Down,
            edge,
            fair_value:         p_under,
            market_price:       no_ask,
            confidence:         (edge / 0.20).clamp(0.0, 1.0),
            time_to_close_secs: secs_to_close,
        });
    }
    SignalDecision::None
}

/// Convenience: evaluate every cached projection, return all firing
/// signals. The execution runner calls this every tick.
pub fn evaluate_all_props(cache: &ProjectionsCache) -> Vec<(String, SignalDecision)> {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    if cache.is_stale(now_ms) {
        tracing::debug!("projections cache stale — suppressing prop signals");
        return Vec::new();
    }

    let map = cache.inner.read().expect("poisoned");
    let mut out = Vec::new();
    for (cid, row) in map.iter() {
        let dec = evaluate_prop(row, now_ms);
        if !matches!(dec, SignalDecision::None) {
            out.push((cid.clone(), dec));
        }
    }
    out
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_path() -> PathBuf {
        std::env::temp_dir().join(format!("constantine-proj-{}.json", uuid::Uuid::new_v4()))
    }

    fn sample_row(p_over: f64, yes_ask: f64, yes_bid: f64) -> ProjectionRow {
        ProjectionRow {
            market_id:    "m1".into(),
            condition_id: "c1".into(),
            question:     "Joel Embiid: Rebounds O/U 8.5".into(),
            player:       "Joel Embiid".into(),
            stat:         "Rebounds".into(),
            stat_code:    "REB".into(),
            line:         8.5,
            yes_token_id: "y1".into(),
            no_token_id:  "n1".into(),
            yes_ask, yes_bid,
            end_date_ms:  9_999_999_999_000,
            liquidity:    5000.0,
            volume:       500.0,
            p_over,
            edge_over:    0.0,
            edge_under:   0.0,
        }
    }

    #[test]
    fn fires_over_when_model_above_market_by_min_edge() {
        let row = sample_row(0.65, 0.50, 0.49);    // model 65%, market ask 50% → 15pp edge
        let now = 1_700_000_000_000_u64;
        match evaluate_prop(&row, now) {
            SignalDecision::Oracle(s) => {
                assert_eq!(s.direction, Direction::Up);
                assert!((s.edge - 0.15).abs() < 1e-9);
                assert!((s.fair_value - 0.65).abs() < 1e-9);
            }
            other => panic!("expected Oracle Up, got {:?}", other),
        }
    }

    #[test]
    fn fires_under_when_model_below_market() {
        // Model says 35% over → 65% under. Market YES bid 0.50, so NO ask = 0.50.
        // Edge = 0.65 - 0.50 = 0.15 → fires UNDER.
        let row = sample_row(0.35, 0.50, 0.50);
        let now = 1_700_000_000_000_u64;
        match evaluate_prop(&row, now) {
            SignalDecision::Oracle(s) => {
                assert_eq!(s.direction, Direction::Down);
                assert!((s.fair_value - 0.65).abs() < 1e-9);
            }
            other => panic!("expected Oracle Down, got {:?}", other),
        }
    }

    #[test]
    fn skips_when_edge_below_min() {
        // Model 0.55, market 0.52 → edge 3% < MIN_EDGE 7%
        let row = sample_row(0.55, 0.52, 0.51);
        let now = 1_700_000_000_000_u64;
        assert_eq!(evaluate_prop(&row, now), SignalDecision::None);
    }

    #[test]
    fn skips_when_market_invalid() {
        let mut row = sample_row(0.65, 0.0, 0.0);   // no ask
        assert_eq!(evaluate_prop(&row, 1_700_000_000_000), SignalDecision::None);
        row.yes_ask = 1.0;                          // ask at extreme
        assert_eq!(evaluate_prop(&row, 1_700_000_000_000), SignalDecision::None);
    }

    #[test]
    fn skips_when_game_already_ended() {
        let mut row = sample_row(0.80, 0.40, 0.39);
        row.end_date_ms = 1_000_000_000_000;
        assert_eq!(evaluate_prop(&row, 1_500_000_000_000), SignalDecision::None);
    }

    // ── Cache loading ────────────────────────────────────────────────────

    #[test]
    fn cache_loads_from_disk_and_lookups_work() {
        let path = temp_path();
        let payload = serde_json::json!({
            "generated_at_ms": chrono::Utc::now().timestamp_millis(),
            "n_markets": 1,
            "projections": [{
                "market_id":    "m1",
                "condition_id": "c1",
                "question":     "Joel Embiid: Rebounds O/U 8.5",
                "player":       "Joel Embiid",
                "stat":         "Rebounds",
                "stat_code":    "REB",
                "line":         8.5,
                "yes_token_id": "y",
                "no_token_id":  "n",
                "yes_ask":      0.5, "yes_bid": 0.49,
                "end_date_ms":  9_999_999_999_000_u64,
                "liquidity":    1000.0, "volume": 100.0,
                "p_over":       0.62,
                "edge_over":    0.12, "edge_under": -0.12,
                "diagnostics":  {},
            }],
        });
        std::fs::write(&path, payload.to_string()).unwrap();

        let cache = ProjectionsCache::new();
        cache.reload(&path).unwrap();
        assert_eq!(cache.len(), 1);
        let p = cache.lookup("c1").unwrap();
        assert_eq!(p.player, "Joel Embiid");
        assert!((p.p_over - 0.62).abs() < 1e-9);
    }

    #[test]
    fn cache_skips_reload_when_file_unchanged() {
        let path = temp_path();
        let payload = serde_json::json!({
            "generated_at_ms": chrono::Utc::now().timestamp_millis(),
            "n_markets": 0, "projections": [],
        });
        std::fs::write(&path, payload.to_string()).unwrap();
        let cache = ProjectionsCache::new();
        cache.reload(&path).unwrap();
        // Second reload — file unchanged
        let n = cache.reload(&path).unwrap();
        assert_eq!(n, 0);   // cache size = 0 (empty payload)
    }

    #[test]
    fn cache_marked_stale_when_payload_old() {
        let path = temp_path();
        let payload = serde_json::json!({
            "generated_at_ms": 1_000,    // ancient
            "n_markets": 0, "projections": [],
        });
        std::fs::write(&path, payload.to_string()).unwrap();
        let cache = ProjectionsCache::new();
        cache.reload(&path).unwrap();
        let now = chrono::Utc::now().timestamp_millis() as u64;
        assert!(cache.is_stale(now));
    }

    // ── evaluate_all_props ──────────────────────────────────────────────

    #[test]
    fn evaluate_all_returns_only_firing_signals() {
        let path = temp_path();
        let now_ms = chrono::Utc::now().timestamp_millis();
        let payload = serde_json::json!({
            "generated_at_ms": now_ms,
            "n_markets": 3,
            "projections": [
                {
                    "market_id": "a", "condition_id": "ca", "question": "Player A: Points O/U 20",
                    "player": "A", "stat": "Points", "stat_code": "PTS", "line": 20.0,
                    "yes_token_id": "y", "no_token_id": "n",
                    "yes_ask": 0.50, "yes_bid": 0.49,
                    "end_date_ms": 9_999_999_999_000_u64,
                    "liquidity": 1000.0, "volume": 100.0,
                    "p_over": 0.65,    // ← strong edge OVER
                    "edge_over": 0.15, "edge_under": 0.0, "diagnostics": {},
                },
                {
                    "market_id": "b", "condition_id": "cb", "question": "Player B: Points O/U 20",
                    "player": "B", "stat": "Points", "stat_code": "PTS", "line": 20.0,
                    "yes_token_id": "y", "no_token_id": "n",
                    "yes_ask": 0.52, "yes_bid": 0.51,
                    "end_date_ms": 9_999_999_999_000_u64,
                    "liquidity": 1000.0, "volume": 100.0,
                    "p_over": 0.55,    // ← edge < min
                    "edge_over": 0.03, "edge_under": 0.0, "diagnostics": {},
                },
                {
                    "market_id": "c", "condition_id": "cc", "question": "Player C: Points O/U 20",
                    "player": "C", "stat": "Points", "stat_code": "PTS", "line": 20.0,
                    "yes_token_id": "y", "no_token_id": "n",
                    "yes_ask": 0.50, "yes_bid": 0.49,
                    "end_date_ms": 9_999_999_999_000_u64,
                    "liquidity": 1000.0, "volume": 100.0,
                    "p_over": 0.30,    // ← strong edge UNDER
                    "edge_over": 0.0, "edge_under": 0.20, "diagnostics": {},
                },
            ],
        });
        std::fs::write(&path, payload.to_string()).unwrap();
        let cache = ProjectionsCache::new();
        cache.reload(&path).unwrap();

        let signals = evaluate_all_props(&cache);
        let mut by_id: std::collections::HashMap<String, SignalDecision> =
            signals.into_iter().collect();
        // a fires Up, c fires Down, b doesn't fire
        assert!(matches!(by_id.remove("ca"), Some(SignalDecision::Oracle(s)) if s.direction == Direction::Up));
        assert!(matches!(by_id.remove("cc"), Some(SignalDecision::Oracle(s)) if s.direction == Direction::Down));
        assert!(by_id.is_empty());
    }
}
