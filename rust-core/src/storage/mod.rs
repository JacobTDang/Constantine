// Observe-only mode storage: append-only JSONL files, written by the live
// Rust process. Two files:
//
//   data/db/signals.jsonl     — one row per fired signal evaluation
//   data/db/settlements.jsonl — one row per resolved market
//
// Why JSONL instead of SQLite: Windows-GNU Rust toolchain doesn't ship gcc,
// and rusqlite's bundled feature requires it. JSONL works without any C deps,
// is trivially readable by pandas/jq for analysis, and is append-only durable
// (one fsync per write — no transaction concerns).
//
// Settlement reconciliation (computing realised P&L for each signal) happens
// at analysis time via the Python reporter (scripts/observe_report.py),
// which reads both files and joins on market_id. The live process only
// appends — it doesn't have to update past records.

pub mod settlement;

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{Context, Result};
use serde::Serialize;

// ── Public types — flat structs for clean JSON ───────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct SignalRow {
    pub fired_at_ms:            u64,
    pub market_id:              String,
    pub signal_type:            String,           // "intramarket" | "oracle"
    pub direction:              Option<String>,   // "up" | "down" | None
    pub spot_price:             f64,
    pub chainlink_price:        f64,
    pub strike_price:           f64,
    pub yes_ask:                f64,
    pub no_ask:                 f64,
    pub vol_5m:                 f64,
    pub time_to_close_secs:     f64,
    pub window_age_secs:        f64,
    pub yes_liquidity_usd:      f64,
    pub no_liquidity_usd:       f64,
    pub spread:                 f64,
    pub regime:                 String,           // "ranging"|"trending"|"event"
    pub fair_value:             f64,
    pub edge:                   f64,
    pub confidence:             f64,
    pub would_have_bet_dollars: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SettlementRow {
    pub market_id:            String,
    pub close_time_ms:        u64,
    pub strike_price:         f64,
    pub settlement_chainlink: f64,
    pub outcome_up:           bool,        // true if Up won
    pub settled_at_ms:        u64,
}

// ── Append-only JSONL log ────────────────────────────────────────────────────

pub struct SignalLog {
    signals_path:     PathBuf,
    settlements_path: PathBuf,
    write_lock:       Mutex<()>,  // serialise writes; both files share lock
}

impl SignalLog {
    pub fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("creating dir {}", dir.display()))?;
        Ok(Self {
            signals_path:     dir.join("signals.jsonl"),
            settlements_path: dir.join("settlements.jsonl"),
            write_lock:       Mutex::new(()),
        })
    }

    pub fn insert_signal(&self, row: &SignalRow) -> Result<()> {
        let _g = self.write_lock.lock().expect("poisoned");
        let line = serde_json::to_string(row)
            .context("serialising SignalRow")?;
        let mut f = OpenOptions::new()
            .create(true).append(true)
            .open(&self.signals_path)
            .with_context(|| format!("opening {}", self.signals_path.display()))?;
        writeln!(f, "{line}").context("writing signal")?;
        f.flush().context("flushing signal")?;
        Ok(())
    }

    pub fn insert_settlement(&self, row: &SettlementRow) -> Result<()> {
        let _g = self.write_lock.lock().expect("poisoned");
        let line = serde_json::to_string(row)
            .context("serialising SettlementRow")?;
        let mut f = OpenOptions::new()
            .create(true).append(true)
            .open(&self.settlements_path)
            .with_context(|| format!("opening {}", self.settlements_path.display()))?;
        writeln!(f, "{line}").context("writing settlement")?;
        f.flush().context("flushing settlement")?;
        Ok(())
    }

    pub fn signals_path(&self)     -> &Path { &self.signals_path }
    pub fn settlements_path(&self) -> &Path { &self.settlements_path }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "constantine-storage-{}", uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn sample_signal(market_id: &str, dir: &str, edge: f64) -> SignalRow {
        SignalRow {
            fired_at_ms:            1_700_000_000_000,
            market_id:              market_id.to_string(),
            signal_type:            "oracle".to_string(),
            direction:              Some(dir.to_string()),
            spot_price:             80_000.0,
            chainlink_price:        80_010.0,
            strike_price:           80_000.0,
            yes_ask:                0.65,
            no_ask:                 0.40,
            vol_5m:                 0.001,
            time_to_close_secs:     60.0,
            window_age_secs:        240.0,
            yes_liquidity_usd:      500.0,
            no_liquidity_usd:       500.0,
            spread:                 0.02,
            regime:                 "trending".to_string(),
            fair_value:             0.85,
            edge,
            confidence:             0.7,
            would_have_bet_dollars: 25.00,
        }
    }

    #[test]
    fn open_creates_dir() {
        let dir = temp_dir();
        let nested = dir.join("a/b/c");
        let log = SignalLog::open(&nested).unwrap();
        assert!(log.signals_path().parent().unwrap().exists());
    }

    #[test]
    fn insert_signal_appends_line() {
        let dir = temp_dir();
        let log = SignalLog::open(&dir).unwrap();

        log.insert_signal(&sample_signal("m1", "up", 0.20)).unwrap();
        log.insert_signal(&sample_signal("m2", "down", 0.15)).unwrap();

        let content = std::fs::read_to_string(log.signals_path()).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2);
        // Each line is valid JSON
        for line in lines {
            let _: serde_json::Value = serde_json::from_str(line).expect("valid json");
        }
    }

    #[test]
    fn insert_signal_round_trips_through_json() {
        let dir = temp_dir();
        let log = SignalLog::open(&dir).unwrap();
        let original = sample_signal("m1", "up", 0.20);
        log.insert_signal(&original).unwrap();

        let content = std::fs::read_to_string(log.signals_path()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(content.trim()).unwrap();
        assert_eq!(parsed["market_id"],  "m1");
        assert_eq!(parsed["direction"],  "up");
        assert_eq!(parsed["fired_at_ms"], 1_700_000_000_000_i64);
        let edge = parsed["edge"].as_f64().unwrap();
        assert!((edge - 0.20).abs() < 1e-9);
    }

    #[test]
    fn insert_settlement_appends() {
        let dir = temp_dir();
        let log = SignalLog::open(&dir).unwrap();

        let row = SettlementRow {
            market_id:            "m1".to_string(),
            close_time_ms:        1_700_000_300_000,
            strike_price:         80_000.0,
            settlement_chainlink: 80_500.0,
            outcome_up:           true,
            settled_at_ms:        1_700_000_301_000,
        };
        log.insert_settlement(&row).unwrap();
        log.insert_settlement(&row).unwrap();

        let content = std::fs::read_to_string(log.settlements_path()).unwrap();
        assert_eq!(content.lines().count(), 2);
    }

    #[test]
    fn separate_locks_dont_block_each_other_excessively() {
        // Smoke test: 100 sequential writes complete quickly
        let dir = temp_dir();
        let log = SignalLog::open(&dir).unwrap();
        let start = std::time::Instant::now();
        for i in 0..100 {
            log.insert_signal(&sample_signal(&format!("m{i}"), "up", 0.10)).unwrap();
        }
        let elapsed = start.elapsed();
        assert!(elapsed.as_secs() < 5, "100 writes took too long: {elapsed:?}");
    }

    #[test]
    fn nul_in_string_does_not_break_parser() {
        // serde_json escapes control chars; verify it's not a panic source
        let dir = temp_dir();
        let log = SignalLog::open(&dir).unwrap();
        let mut row = sample_signal("m1", "up", 0.20);
        row.market_id = "weird\nid".to_string();
        log.insert_signal(&row).unwrap();
        let content = std::fs::read_to_string(log.signals_path()).unwrap();
        // Newline in field → JSON-encoded as \n → exactly one line
        assert_eq!(content.lines().count(), 1);
    }
}
