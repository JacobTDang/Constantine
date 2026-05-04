// Sprint 8 / S8.1 — Dashboard aggregation logic.
//
// Pure aggregation over the JSONL ledgers. Designed to be cheap to run
// every 1-2 seconds (re-reads positions and last-N signal lines).
//
// Anything stateful belongs in PositionStore — this module just produces
// a read-only snapshot suitable for rendering.

use std::path::Path;

use anyhow::{Context, Result};

use crate::storage::{Position, PositionStatus, PositionStore};

#[derive(Debug, Clone, Default)]
pub struct DashboardSnapshot {
    pub now_ms:                u64,
    // Position counts by status
    pub n_submitted:           usize,
    pub n_filled:              usize,
    pub n_settled:             usize,
    pub n_failed:              usize,
    // P&L (settled positions only)
    pub realised_pnl_usd:      f64,
    pub n_wins:                usize,
    pub n_losses:              usize,
    pub win_rate:              f64,
    // Exposure (sum of bet_dollars on still-open positions)
    pub open_exposure_usd:     f64,
    // Top positions by absolute P&L (so user sees big wins/losses on screen)
    pub top_positions:         Vec<Position>,
}

impl DashboardSnapshot {
    pub fn n_total(&self) -> usize {
        self.n_submitted + self.n_filled + self.n_settled + self.n_failed
    }
    pub fn n_open(&self) -> usize { self.n_submitted + self.n_filled }
}

/// Build a snapshot from a live PositionStore.
pub fn from_store(store: &PositionStore, now_ms: u64) -> DashboardSnapshot {
    aggregate(store.all().as_slice(), now_ms, 5)
}

/// Build a snapshot by reopening the store from disk — used by the
/// dashboard binary (which is a separate process from the bot).
pub fn from_path(dir: &Path, now_ms: u64) -> Result<DashboardSnapshot> {
    let store = PositionStore::open(dir)
        .with_context(|| format!("open position store at {}", dir.display()))?;
    Ok(from_store(&store, now_ms))
}

/// Aggregate a slice of positions into a snapshot.
/// `top_n` controls how many positions are kept in `top_positions`.
pub fn aggregate(positions: &[Position], now_ms: u64, top_n: usize) -> DashboardSnapshot {
    let mut snap = DashboardSnapshot {
        now_ms,
        ..Default::default()
    };

    for p in positions {
        match p.status {
            PositionStatus::Submitted => {
                snap.n_submitted += 1;
                snap.open_exposure_usd += p.bet_dollars.max(0.0);
            }
            PositionStatus::Filled => {
                snap.n_filled += 1;
                snap.open_exposure_usd += p.bet_dollars.max(0.0);
            }
            PositionStatus::Settled => {
                snap.n_settled += 1;
                if let Some(pnl) = p.pnl_dollars {
                    snap.realised_pnl_usd += pnl;
                    if pnl > 0.0      { snap.n_wins   += 1; }
                    else if pnl < 0.0 { snap.n_losses += 1; }
                }
            }
            PositionStatus::Failed => {
                snap.n_failed += 1;
            }
        }
    }

    let decided = snap.n_wins + snap.n_losses;
    snap.win_rate = if decided > 0 {
        snap.n_wins as f64 / decided as f64
    } else { 0.0 };

    // Top |pnl| positions
    let mut pnl_sorted: Vec<&Position> = positions.iter()
        .filter(|p| p.pnl_dollars.is_some())
        .collect();
    pnl_sorted.sort_by(|a, b| {
        let aa = a.pnl_dollars.unwrap_or(0.0).abs();
        let bb = b.pnl_dollars.unwrap_or(0.0).abs();
        bb.partial_cmp(&aa).unwrap_or(std::cmp::Ordering::Equal)
    });
    snap.top_positions = pnl_sorted.into_iter().take(top_n).cloned().collect();

    snap
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "constantine-dash-{}", uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn aggregate_counts_each_status() {
        let dir = temp_dir();
        let s = PositionStore::open(&dir).unwrap();
        s.record_open("a", "m", "yes", 5.0, 0.45).unwrap();
        s.record_open("b", "m", "yes", 5.0, 0.45).unwrap();
        s.record_fill("b", 0.45, 11.11).unwrap();
        s.record_open("c", "m", "yes", 5.0, 0.45).unwrap();
        s.record_settle("c", true, 6.11).unwrap();
        s.record_open("d", "m", "yes", 5.0, 0.45).unwrap();
        s.record_fail("d", "test").unwrap();

        let snap = from_store(&s, 0);
        assert_eq!(snap.n_submitted, 1);   // a
        assert_eq!(snap.n_filled,    1);   // b
        assert_eq!(snap.n_settled,   1);   // c
        assert_eq!(snap.n_failed,    1);   // d
        assert_eq!(snap.n_total(),   4);
        assert_eq!(snap.n_open(),    2);
    }

    #[test]
    fn realised_pnl_sums_only_settled() {
        let dir = temp_dir();
        let s = PositionStore::open(&dir).unwrap();
        s.record_open("a", "m", "yes", 5.0, 0.45).unwrap();
        s.record_settle("a", true, 6.0).unwrap();
        s.record_open("b", "m", "yes", 5.0, 0.50).unwrap();
        s.record_settle("b", false, -5.0).unwrap();
        s.record_open("c", "m", "yes", 5.0, 0.45).unwrap(); // still open

        let snap = from_store(&s, 0);
        assert!((snap.realised_pnl_usd - 1.0).abs() < 1e-9);
        assert_eq!(snap.n_wins, 1);
        assert_eq!(snap.n_losses, 1);
        assert!((snap.win_rate - 0.5).abs() < 1e-9);
    }

    #[test]
    fn open_exposure_sums_submitted_and_filled() {
        let dir = temp_dir();
        let s = PositionStore::open(&dir).unwrap();
        s.record_open("a", "m", "yes", 5.0,  0.45).unwrap();
        s.record_open("b", "m", "yes", 10.0, 0.50).unwrap();
        s.record_fill("b", 0.50, 20.0).unwrap();
        s.record_open("c", "m", "yes", 7.0,  0.45).unwrap();
        s.record_settle("c", true, 1.5).unwrap();   // settled, NOT in exposure

        let snap = from_store(&s, 0);
        assert!((snap.open_exposure_usd - 15.0).abs() < 1e-9);   // a + b only
    }

    #[test]
    fn win_rate_zero_when_no_settlements() {
        let dir = temp_dir();
        let s = PositionStore::open(&dir).unwrap();
        s.record_open("a", "m", "yes", 5.0, 0.45).unwrap();
        let snap = from_store(&s, 0);
        assert_eq!(snap.win_rate, 0.0);
    }

    #[test]
    fn top_positions_sorted_by_abs_pnl() {
        let dir = temp_dir();
        let s = PositionStore::open(&dir).unwrap();
        for (id, pnl) in [("a", 1.0), ("b", -5.0), ("c", 3.0), ("d", -10.0), ("e", 2.0)] {
            s.record_open(id, "m", "yes", 5.0, 0.45).unwrap();
            s.record_settle(id, pnl > 0.0, pnl).unwrap();
        }
        let snap = aggregate(&s.all(), 0, 3);
        assert_eq!(snap.top_positions.len(), 3);
        // Largest absolute pnl first
        assert!((snap.top_positions[0].pnl_dollars.unwrap()).abs() >= 5.0);
    }

    #[test]
    fn from_path_reads_disk_state() {
        // Build state in a temp dir, then read it back via from_path.
        let dir = temp_dir();
        {
            let s = PositionStore::open(&dir).unwrap();
            s.record_open("a", "m", "yes", 5.0, 0.45).unwrap();
            s.record_settle("a", true, 1.5).unwrap();
        }
        let snap = from_path(&dir, 0).unwrap();
        assert_eq!(snap.n_settled, 1);
        assert!((snap.realised_pnl_usd - 1.5).abs() < 1e-9);
    }

    #[test]
    fn empty_store_produces_empty_snapshot() {
        let dir = temp_dir();
        let s = PositionStore::open(&dir).unwrap();
        let snap = from_store(&s, 1000);
        assert_eq!(snap.n_total(), 0);
        assert_eq!(snap.realised_pnl_usd, 0.0);
        assert_eq!(snap.win_rate, 0.0);
        assert!(snap.top_positions.is_empty());
        assert_eq!(snap.now_ms, 1000);
    }
}
