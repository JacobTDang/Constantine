// Sprint 4 / J12 — Position tracker with parallel settlement.
//
// Tracks every order we submit through its full lifecycle:
//   Submitted -> Filled -> Settled (with realised P&L)
//
// Storage model: append-only JSONL event log + in-memory DashMap snapshot.
//
//   data/db/positions.jsonl
//
// Each line is a single PositionEvent (Open / Fill / Settle / Fail). On
// startup, replay the log to rebuild the live snapshot. This is the same
// pattern as SignalLog but with state instead of just rows.
//
// Parallelism:
//   - DashMap for the snapshot lets readers/writers go concurrently
//   - settle_open_positions takes a closure and runs it across all open
//     positions via rayon::par_iter — each position's chainlink lookup is
//     independent, so 20 cores can resolve 20 positions in the time it
//     takes to do 1.

use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{Context, Result};
use chrono::Utc;
use dashmap::DashMap;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

// ── Public types ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum PositionStatus {
    /// Order submitted to the CLOB but no fill confirmation yet.
    Submitted,
    /// Order matched on-chain — at least partially filled.
    Filled,
    /// Position resolved at window close; pnl_dollars is finalised.
    Settled,
    /// Submission failed (4xx/5xx after retries, or success=false).
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub order_id:        String,
    pub market_id:       String,
    pub side:            String,        // "yes" / "no"
    pub bet_dollars:     f64,
    pub price:           f64,
    pub submit_time_ms:  u64,
    pub status:          PositionStatus,
    pub fill_time_ms:    Option<u64>,
    pub fill_price:      Option<f64>,
    pub fill_size:       Option<f64>,
    pub settle_time_ms:  Option<u64>,
    pub pnl_dollars:     Option<f64>,
    /// Last error message if status == Failed.
    pub error_msg:       Option<String>,
}

/// One line in positions.jsonl. Tagged enum so we know which transition the
/// event represents.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PositionEvent {
    Open {
        event_at_ms:    u64,
        order_id:       String,
        market_id:      String,
        side:           String,
        bet_dollars:    f64,
        price:          f64,
    },
    Fill {
        event_at_ms:    u64,
        order_id:       String,
        fill_price:     f64,
        fill_size:      f64,
    },
    Settle {
        event_at_ms:    u64,
        order_id:       String,
        outcome_up:     bool,
        pnl_dollars:    f64,
    },
    Fail {
        event_at_ms:    u64,
        order_id:       String,
        error_msg:      String,
    },
}

impl PositionEvent {
    pub fn order_id(&self) -> &str {
        match self {
            Self::Open  { order_id, .. } => order_id,
            Self::Fill  { order_id, .. } => order_id,
            Self::Settle{ order_id, .. } => order_id,
            Self::Fail  { order_id, .. } => order_id,
        }
    }
    pub fn event_at_ms(&self) -> u64 {
        match self {
            Self::Open  { event_at_ms, .. } => *event_at_ms,
            Self::Fill  { event_at_ms, .. } => *event_at_ms,
            Self::Settle{ event_at_ms, .. } => *event_at_ms,
            Self::Fail  { event_at_ms, .. } => *event_at_ms,
        }
    }
}

// ── PositionStore ────────────────────────────────────────────────────────────

pub struct PositionStore {
    path:    PathBuf,
    /// Live snapshot, keyed by order_id. DashMap = lock-free concurrent access.
    inner:   DashMap<String, Position>,
    /// Serialises file appends so multi-thread writers never interleave a line.
    writer:  Mutex<()>,
}

impl PositionStore {
    /// Open (or create) the position log directory and replay any existing
    /// events to rebuild the in-memory snapshot.
    pub fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("creating dir {}", dir.display()))?;
        let path = dir.join("positions.jsonl");
        let store = Self {
            path: path.clone(),
            inner: DashMap::new(),
            writer: Mutex::new(()),
        };
        store.replay()?;
        Ok(store)
    }

    fn replay(&self) -> Result<()> {
        if !self.path.exists() { return Ok(()); }
        let f = std::fs::File::open(&self.path)
            .with_context(|| format!("open {}", self.path.display()))?;
        let mut count = 0;
        for (line_no, line) in BufReader::new(f).lines().enumerate() {
            let line = line.with_context(|| format!("read line {} of {}", line_no, self.path.display()))?;
            if line.trim().is_empty() { continue; }
            let event: PositionEvent = serde_json::from_str(&line)
                .with_context(|| format!("parse line {}", line_no))?;
            self.apply(event);
            count += 1;
        }
        if count > 0 {
            tracing::info!(events = count, file = %self.path.display(), "replayed position log");
        }
        Ok(())
    }

    /// Apply an event to the in-memory snapshot. Pure: callers writing to
    /// disk should invoke `append_and_apply` instead.
    fn apply(&self, ev: PositionEvent) {
        match ev {
            PositionEvent::Open { event_at_ms, order_id, market_id, side, bet_dollars, price } => {
                let pos = Position {
                    order_id:       order_id.clone(),
                    market_id, side, bet_dollars, price,
                    submit_time_ms: event_at_ms,
                    status:         PositionStatus::Submitted,
                    fill_time_ms:   None,
                    fill_price:     None,
                    fill_size:      None,
                    settle_time_ms: None,
                    pnl_dollars:    None,
                    error_msg:      None,
                };
                self.inner.insert(order_id, pos);
            }
            PositionEvent::Fill { event_at_ms, order_id, fill_price, fill_size } => {
                if let Some(mut pos) = self.inner.get_mut(&order_id) {
                    pos.status        = PositionStatus::Filled;
                    pos.fill_time_ms  = Some(event_at_ms);
                    pos.fill_price    = Some(fill_price);
                    pos.fill_size     = Some(fill_size);
                }
            }
            PositionEvent::Settle { event_at_ms, order_id, outcome_up: _, pnl_dollars } => {
                if let Some(mut pos) = self.inner.get_mut(&order_id) {
                    pos.status         = PositionStatus::Settled;
                    pos.settle_time_ms = Some(event_at_ms);
                    pos.pnl_dollars    = Some(pnl_dollars);
                }
            }
            PositionEvent::Fail { event_at_ms: _, order_id, error_msg } => {
                if let Some(mut pos) = self.inner.get_mut(&order_id) {
                    pos.status    = PositionStatus::Failed;
                    pos.error_msg = Some(error_msg);
                }
            }
        }
    }

    fn append(&self, ev: &PositionEvent) -> Result<()> {
        let _g = self.writer.lock().expect("poisoned");
        let line = serde_json::to_string(ev).context("serialise PositionEvent")?;
        let mut f = OpenOptions::new()
            .create(true).append(true)
            .open(&self.path)
            .with_context(|| format!("open {}", self.path.display()))?;
        writeln!(f, "{line}").context("write event")?;
        f.flush().context("flush")?;
        Ok(())
    }

    fn append_and_apply(&self, ev: PositionEvent) -> Result<()> {
        self.append(&ev)?;
        self.apply(ev);
        Ok(())
    }

    // ── Public mutators ───────────────────────────────────────────────────

    pub fn record_open(
        &self,
        order_id:    impl Into<String>,
        market_id:   impl Into<String>,
        side:        impl Into<String>,
        bet_dollars: f64,
        price:       f64,
    ) -> Result<()> {
        self.append_and_apply(PositionEvent::Open {
            event_at_ms: now_ms(),
            order_id:    order_id.into(),
            market_id:   market_id.into(),
            side:        side.into(),
            bet_dollars,
            price,
        })
    }

    pub fn record_fill(
        &self,
        order_id:   impl Into<String>,
        fill_price: f64,
        fill_size:  f64,
    ) -> Result<()> {
        self.append_and_apply(PositionEvent::Fill {
            event_at_ms: now_ms(),
            order_id:    order_id.into(),
            fill_price,
            fill_size,
        })
    }

    pub fn record_settle(
        &self,
        order_id:    impl Into<String>,
        outcome_up:  bool,
        pnl_dollars: f64,
    ) -> Result<()> {
        self.append_and_apply(PositionEvent::Settle {
            event_at_ms: now_ms(),
            order_id:    order_id.into(),
            outcome_up,
            pnl_dollars,
        })
    }

    pub fn record_fail(
        &self,
        order_id:  impl Into<String>,
        error_msg: impl Into<String>,
    ) -> Result<()> {
        self.append_and_apply(PositionEvent::Fail {
            event_at_ms: now_ms(),
            order_id:    order_id.into(),
            error_msg:   error_msg.into(),
        })
    }

    // ── Public readers ────────────────────────────────────────────────────

    pub fn get(&self, order_id: &str) -> Option<Position> {
        self.inner.get(order_id).map(|r| r.value().clone())
    }

    /// All positions in the store, regardless of status. Snapshot at time
    /// of call — callers don't see updates after.
    pub fn all(&self) -> Vec<Position> {
        self.inner.iter().map(|r| r.value().clone()).collect()
    }

    /// Positions waiting for settlement (Submitted or Filled, NOT Settled/Failed).
    pub fn pending_settlement(&self) -> Vec<Position> {
        self.inner.iter()
            .filter_map(|r| {
                let p = r.value();
                if matches!(p.status, PositionStatus::Submitted | PositionStatus::Filled) {
                    Some(p.clone())
                } else { None }
            })
            .collect()
    }

    pub fn len(&self) -> usize { self.inner.len() }
    pub fn is_empty(&self) -> bool { self.inner.is_empty() }
    pub fn path(&self) -> &Path { &self.path }

    // ── Parallel settlement reconciliation ────────────────────────────────

    /// Resolve every pending position in parallel. `resolver` maps a
    /// position to (outcome_up, settle_chainlink) — typically by querying
    /// chainlink for the close-time price. Returns Ok(None) to skip (still
    /// pending).
    ///
    /// Uses rayon::par_iter so N pending positions resolve in time(1)
    /// instead of time(N) on a multi-core box. Each closure call is
    /// independent (no shared state), so this is embarrassingly parallel.
    pub fn settle_pending<F>(&self, resolver: F) -> Result<usize>
    where
        F: Fn(&Position) -> Option<SettleOutcome> + Send + Sync,
    {
        let pending = self.pending_settlement();
        // Phase 1 — resolve in parallel
        let resolved: Vec<(Position, SettleOutcome)> = pending
            .par_iter()
            .filter_map(|p| resolver(p).map(|o| (p.clone(), o)))
            .collect();

        // Phase 2 — single-threaded write to the JSONL log
        // (file append must be serial; in-memory updates use DashMap inside apply)
        let n = resolved.len();
        for (pos, out) in resolved {
            self.record_settle(pos.order_id, out.outcome_up, out.pnl_dollars)?;
        }
        Ok(n)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SettleOutcome {
    pub outcome_up:  bool,
    pub pnl_dollars: f64,
}

fn now_ms() -> u64 { Utc::now().timestamp_millis() as u64 }

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "constantine-positions-{}", uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    // ── Lifecycle: Open → Fill → Settle ────────────────────────────────────

    #[test]
    fn open_creates_position_with_submitted_status() {
        let dir = temp_dir();
        let store = PositionStore::open(&dir).unwrap();
        store.record_open("ord-1", "btc-w1", "yes", 5.0, 0.45).unwrap();
        let p = store.get("ord-1").unwrap();
        assert_eq!(p.status, PositionStatus::Submitted);
        assert_eq!(p.market_id, "btc-w1");
        assert_eq!(p.side, "yes");
        assert!((p.bet_dollars - 5.0).abs() < 1e-9);
    }

    #[test]
    fn fill_after_open_advances_status() {
        let dir = temp_dir();
        let store = PositionStore::open(&dir).unwrap();
        store.record_open("ord-1", "btc-w1", "yes", 5.0, 0.45).unwrap();
        store.record_fill("ord-1", 0.46, 10.0).unwrap();
        let p = store.get("ord-1").unwrap();
        assert_eq!(p.status, PositionStatus::Filled);
        assert!((p.fill_price.unwrap() - 0.46).abs() < 1e-9);
        assert!((p.fill_size.unwrap()  - 10.0).abs() < 1e-9);
    }

    #[test]
    fn settle_finalises_pnl() {
        let dir = temp_dir();
        let store = PositionStore::open(&dir).unwrap();
        store.record_open("ord-1", "btc-w1", "yes", 5.0, 0.45).unwrap();
        store.record_fill("ord-1", 0.45, 11.11).unwrap();
        store.record_settle("ord-1", true, 6.11).unwrap();
        let p = store.get("ord-1").unwrap();
        assert_eq!(p.status, PositionStatus::Settled);
        assert!((p.pnl_dollars.unwrap() - 6.11).abs() < 1e-9);
    }

    #[test]
    fn fail_marks_position_failed_with_error() {
        let dir = temp_dir();
        let store = PositionStore::open(&dir).unwrap();
        store.record_open("ord-1", "btc-w1", "yes", 5.0, 0.45).unwrap();
        store.record_fail("ord-1", "insufficient balance").unwrap();
        let p = store.get("ord-1").unwrap();
        assert_eq!(p.status, PositionStatus::Failed);
        assert_eq!(p.error_msg.as_deref(), Some("insufficient balance"));
    }

    // ── Persistence: replay from disk ──────────────────────────────────────

    #[test]
    fn replay_rebuilds_state_after_reopen() {
        let dir = temp_dir();
        {
            let store = PositionStore::open(&dir).unwrap();
            store.record_open("ord-1", "btc-w1", "yes", 5.0, 0.45).unwrap();
            store.record_fill("ord-1", 0.45, 11.11).unwrap();
            store.record_open("ord-2", "btc-w1", "no", 5.0, 0.55).unwrap();
            store.record_settle("ord-1", true, 6.11).unwrap();
        }
        // Reopen — must replay everything
        let store2 = PositionStore::open(&dir).unwrap();
        assert_eq!(store2.len(), 2);
        let p1 = store2.get("ord-1").unwrap();
        assert_eq!(p1.status, PositionStatus::Settled);
        assert!((p1.pnl_dollars.unwrap() - 6.11).abs() < 1e-9);
        let p2 = store2.get("ord-2").unwrap();
        assert_eq!(p2.status, PositionStatus::Submitted);
    }

    #[test]
    fn jsonl_format_one_event_per_line() {
        let dir = temp_dir();
        let store = PositionStore::open(&dir).unwrap();
        store.record_open("ord-1", "btc-w1", "yes", 5.0, 0.45).unwrap();
        store.record_fill("ord-1", 0.45, 11.11).unwrap();
        store.record_settle("ord-1", true, 6.11).unwrap();

        let content = std::fs::read_to_string(store.path()).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 3);
        for line in lines {
            let _: PositionEvent = serde_json::from_str(line).expect("valid event JSON");
        }
    }

    #[test]
    fn replay_skips_blank_lines() {
        let dir = temp_dir();
        let path = dir.join("positions.jsonl");
        // Simulate a file with stray blank lines (e.g. partial write recovered by retry)
        std::fs::write(&path,
            "\n\
             {\"kind\":\"open\",\"event_at_ms\":1,\"order_id\":\"x\",\"market_id\":\"m\",\"side\":\"yes\",\"bet_dollars\":5.0,\"price\":0.5}\n\
             \n").unwrap();
        let store = PositionStore::open(&dir).unwrap();
        assert_eq!(store.len(), 1);
    }

    // ── Readers ────────────────────────────────────────────────────────────

    #[test]
    fn pending_settlement_excludes_settled_and_failed() {
        let dir = temp_dir();
        let store = PositionStore::open(&dir).unwrap();
        store.record_open("a", "m", "yes", 5.0, 0.45).unwrap();
        store.record_open("b", "m", "yes", 5.0, 0.45).unwrap();
        store.record_open("c", "m", "yes", 5.0, 0.45).unwrap();
        store.record_settle("a", true, 1.0).unwrap();
        store.record_fail("c", "bad").unwrap();

        let pending = store.pending_settlement();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].order_id, "b");
    }

    // ── Parallel settlement ────────────────────────────────────────────────

    #[test]
    fn settle_pending_runs_resolvers_in_parallel() {
        let dir = temp_dir();
        let store = Arc::new(PositionStore::open(&dir).unwrap());
        // 50 open positions
        for i in 0..50 {
            store.record_open(format!("ord-{i}"), "m", "yes", 5.0, 0.45).unwrap();
        }
        assert_eq!(store.pending_settlement().len(), 50);

        // Resolver: pretend half win, half lose; no real sleep (test runs fast)
        let store_for_settle = store.clone();
        let n = store_for_settle.settle_pending(|p| {
            let won: bool = p.order_id.ends_with(|c: char| c.is_ascii_digit() && (c as u8) % 2 == 0);
            Some(SettleOutcome {
                outcome_up:  won,
                pnl_dollars: if won { 6.0 } else { -5.0 },
            })
        }).unwrap();

        assert_eq!(n, 50);
        assert_eq!(store.pending_settlement().len(), 0);
        // Sanity: every settled has a pnl
        for p in store.all() {
            assert_eq!(p.status, PositionStatus::Settled);
            assert!(p.pnl_dollars.is_some());
        }
    }

    #[test]
    fn settle_pending_returns_none_keeps_position_pending() {
        let dir = temp_dir();
        let store = PositionStore::open(&dir).unwrap();
        store.record_open("ord-1", "m", "yes", 5.0, 0.45).unwrap();
        // Resolver returns None — position still pending
        let n = store.settle_pending(|_| None).unwrap();
        assert_eq!(n, 0);
        assert_eq!(store.pending_settlement().len(), 1);
    }

    // ── Concurrent writes ─────────────────────────────────────────────────

    #[test]
    fn concurrent_record_open_does_not_lose_events() {
        let dir = temp_dir();
        let store = Arc::new(PositionStore::open(&dir).unwrap());
        let mut handles = vec![];
        for t in 0..8 {
            let s = store.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..50 {
                    s.record_open(format!("t{t}-i{i}"), "m", "yes", 1.0, 0.5).unwrap();
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(store.len(), 8 * 50);

        // File reflects all events
        let content = std::fs::read_to_string(store.path()).unwrap();
        assert_eq!(content.lines().count(), 8 * 50);
    }

    // ── Idempotency: missing record_fill before record_settle ─────────────

    #[test]
    fn settle_without_prior_fill_still_works() {
        // Order of events is best-effort — settlement might arrive without
        // a prior Fill (e.g. if we lost the websocket fill notification).
        // The settle event still finalises pnl and advances status.
        let dir = temp_dir();
        let store = PositionStore::open(&dir).unwrap();
        store.record_open("ord-1", "m", "yes", 5.0, 0.45).unwrap();
        store.record_settle("ord-1", true, 6.11).unwrap();
        let p = store.get("ord-1").unwrap();
        assert_eq!(p.status, PositionStatus::Settled);
        assert!((p.pnl_dollars.unwrap() - 6.11).abs() < 1e-9);
        assert!(p.fill_time_ms.is_none());  // never filled, just settled
    }

    // ── Unknown order_id is silently ignored on Fill/Settle/Fail ─────────

    #[test]
    fn fill_for_unknown_order_id_is_a_noop() {
        let dir = temp_dir();
        let store = PositionStore::open(&dir).unwrap();
        store.record_fill("never-existed", 0.5, 10.0).unwrap();
        // Append happens (the event hits disk for forensics) but no entry
        // appears in the snapshot.
        assert_eq!(store.len(), 0);
    }

    // ── Map iteration safety ───────────────────────────────────────────────

    #[test]
    fn all_returns_independent_clones() {
        let dir = temp_dir();
        let store = PositionStore::open(&dir).unwrap();
        store.record_open("a", "m", "yes", 5.0, 0.45).unwrap();
        let snap = store.all();
        // Mutate the store
        store.record_fill("a", 0.45, 10.0).unwrap();
        // Old snapshot unchanged
        assert_eq!(snap[0].status, PositionStatus::Submitted);
        assert_eq!(store.get("a").unwrap().status, PositionStatus::Filled);
    }

    // ── Tag round-trip ─────────────────────────────────────────────────────

    #[test]
    fn position_event_serde_round_trip_for_all_kinds() {
        let evs = vec![
            PositionEvent::Open  { event_at_ms: 1, order_id: "a".into(), market_id: "m".into(), side: "yes".into(), bet_dollars: 5.0, price: 0.5 },
            PositionEvent::Fill  { event_at_ms: 2, order_id: "a".into(), fill_price: 0.5, fill_size: 10.0 },
            PositionEvent::Settle{ event_at_ms: 3, order_id: "a".into(), outcome_up: true, pnl_dollars: 5.0 },
            PositionEvent::Fail  { event_at_ms: 4, order_id: "a".into(), error_msg: "bad".into() },
        ];
        for ev in &evs {
            let s = serde_json::to_string(ev).unwrap();
            let back: PositionEvent = serde_json::from_str(&s).unwrap();
            assert_eq!(back.order_id(), ev.order_id());
            assert_eq!(back.event_at_ms(), ev.event_at_ms());
        }
    }

    // ── Helper helpers ─────────────────────────────────────────────────────

    #[test]
    fn position_status_enum_round_trips() {
        for s in [
            PositionStatus::Submitted,
            PositionStatus::Filled,
            PositionStatus::Settled,
            PositionStatus::Failed,
        ] {
            let json = serde_json::to_string(&s).unwrap();
            let back: PositionStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(back, s);
        }
    }

    // ── Map shape sanity ───────────────────────────────────────────────────
    fn _assert_send_sync<T: Send + Sync>() {}
    #[test]
    fn position_store_is_send_sync() {
        _assert_send_sync::<PositionStore>();
    }

}
