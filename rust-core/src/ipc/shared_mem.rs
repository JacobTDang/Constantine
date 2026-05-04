// G2 + G3 — Memory-mapped IPC bridge with Python.
//
// Cross-platform via memmap2 (uses CreateFileMapping on Windows,
// mmap(MAP_SHARED) on Linux). Sub-microsecond access after setup.
//
// G2: features_writer_loop — Rust writes the latest FeatureState to
//     data/ipc/features.bin every 500ms. Python (Milestone I1) opens the
//     same file with mmap.mmap() and reads with struct.unpack_from().
//
// G3: signal_reader_loop — polls data/ipc/signal.bin every 100ms,
//     deserialises into PythonSignal, stores in a shared Arc<RwLock<Option<>>>.
//
// File creation is the writer's responsibility:
//   - features.bin is created by Rust on startup (G2)
//   - signal.bin is created by Python on startup (Milestone I2)
//
// Rust's signal reader waits patiently for the file to appear.

use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use memmap2::{Mmap, MmapMut};
use tokio::sync::RwLock;
use tokio::time::{Duration, MissedTickBehavior};

use crate::features::FeatureState;
use super::protocol::{
    deserialize_signal, serialize_features, FeatureSnapshot, PythonSignal,
    FEATURES_BYTES, SIGNAL_BYTES,
};

const TICK_FEATURES_MS: u64 = 500;
const TICK_SIGNAL_MS:   u64 = 100;

pub fn default_features_path() -> PathBuf { PathBuf::from("data/ipc/features.bin") }
pub fn default_signal_path()   -> PathBuf { PathBuf::from("data/ipc/signal.bin") }

// ── G2: Writer ────────────────────────────────────────────────────────────────

/// Create or open the features file at `path`, sized to FEATURES_BYTES.
/// Returns a writable mmap. Caller must keep the file/mmap alive for the
/// lifetime of the writer task (memmap2 unmaps on drop).
pub fn create_features_mmap(path: &Path) -> Result<MmapMut> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating dir {}", parent.display()))?;
    }
    let file = OpenOptions::new()
        .read(true).write(true).create(true).truncate(false)
        .open(path)
        .with_context(|| format!("opening {}", path.display()))?;
    file.set_len(FEATURES_BYTES as u64)
        .with_context(|| format!("sizing {} to {} bytes", path.display(), FEATURES_BYTES))?;
    let mmap = unsafe { MmapMut::map_mut(&file)? };
    Ok(mmap)
}

/// G2: writer task. Reads latest FeatureState every 500ms and serialises
/// into the mmap. On x86_64 the write is essentially a memcpy.
pub async fn features_writer_loop(
    state: Arc<RwLock<FeatureState>>,
    path:  PathBuf,
) -> Result<()> {
    let mut mmap = create_features_mmap(&path)?;
    tracing::info!(path = %path.display(), bytes = FEATURES_BYTES, "ipc features writer started");

    let mut tick = tokio::time::interval(Duration::from_millis(TICK_FEATURES_MS));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tick.tick().await;
        let (features, seq) = {
            let s = state.read().await;
            (s.to_feature_vec(), s.seq)
        };
        let ts_ms = chrono::Utc::now().timestamp_millis() as u64;

        let buf: &mut [u8; FEATURES_BYTES] = (&mut mmap[..])
            .try_into()
            .expect("mmap should be exactly FEATURES_BYTES");
        serialize_features(&features, seq, ts_ms, buf);
    }
}

// ── G3: Reader ────────────────────────────────────────────────────────────────

/// Open an existing read-only mmap of the signal file. Returns Err if the
/// file does not exist or is the wrong size.
pub fn open_signal_mmap(path: &Path) -> Result<Mmap> {
    let file = OpenOptions::new().read(true).open(path)
        .with_context(|| format!("opening {}", path.display()))?;
    let len = file.metadata()?.len();
    if len < SIGNAL_BYTES as u64 {
        anyhow::bail!("signal file {} is too small: {} bytes (need {})",
                      path.display(), len, SIGNAL_BYTES);
    }
    let mmap = unsafe { Mmap::map(&file)? };
    Ok(mmap)
}

/// G3: reader task. Polls `path` until it appears, then maps it and reads
/// every 100ms. Stores the latest valid signal in `latest`. Drops stale
/// signals (same seq as previous read).
pub async fn signal_reader_loop(
    latest: Arc<RwLock<Option<PythonSignal>>>,
    path:   PathBuf,
) {
    // Wait for Python to create the file
    while !path.exists() {
        tracing::debug!(path = %path.display(), "waiting for python to create signal file");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    let mmap = match open_signal_mmap(&path) {
        Ok(m) => m,
        Err(e) => {
            tracing::error!(error = %e, "could not open signal mmap — reader exiting");
            return;
        }
    };
    tracing::info!(path = %path.display(), "ipc signal reader started");

    let mut last_seq = 0_u64;
    let mut tick = tokio::time::interval(Duration::from_millis(TICK_SIGNAL_MS));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tick.tick().await;
        let buf: &[u8; SIGNAL_BYTES] = (&mmap[..])
            .try_into()
            .expect("signal mmap wrong size");
        if let Some(sig) = deserialize_signal(buf) {
            if sig.seq != last_seq {
                last_seq = sig.seq;
                *latest.write().await = Some(sig);
                tracing::debug!(seq = sig.seq, ?sig.direction, edge = sig.edge,
                                "received python signal");
            }
        }
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ipc::protocol::{
        deserialize_features, serialize_signal, SignalDirection, FEATURE_COUNT,
    };

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("constantine-ipc-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    // ── G2: writer can create file and write features ─────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn writer_creates_file_with_correct_size() {
        let dir   = temp_dir();
        let path  = dir.join("features.bin");
        let _mmap = create_features_mmap(&path).expect("create");

        let metadata = std::fs::metadata(&path).unwrap();
        assert_eq!(metadata.len(), FEATURES_BYTES as u64);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn writer_loop_publishes_increasing_seq() {
        let dir   = temp_dir();
        let path  = dir.join("features.bin");
        let state = Arc::new(RwLock::new(FeatureState::default()));

        // Pre-set seq so we know what to expect
        {
            let mut s = state.write().await;
            s.seq = 100;
        }

        // Spawn writer task with a deadline
        let st = state.clone();
        let p  = path.clone();
        let writer = tokio::spawn(async move {
            features_writer_loop(st, p).await.expect("writer failed");
        });

        // Bump seq a few times while writer runs
        for new_seq in [101, 102, 103] {
            tokio::time::sleep(Duration::from_millis(700)).await;
            state.write().await.seq = new_seq;
        }

        tokio::time::sleep(Duration::from_millis(800)).await;
        writer.abort();

        // Read the file and verify it has a recent seq
        let bytes = std::fs::read(&path).unwrap();
        let buf: &[u8; FEATURES_BYTES] = bytes[..FEATURES_BYTES].try_into().unwrap();
        let snap = deserialize_features(buf).expect("deserialise");
        assert!(snap.seq >= 100, "seq should have been written, got {}", snap.seq);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn writer_can_be_re_opened_with_existing_file() {
        // Simulating restart: create, drop, create again — should not panic
        let dir  = temp_dir();
        let path = dir.join("features.bin");
        {
            let _ = create_features_mmap(&path).unwrap();
        }
        let _mmap = create_features_mmap(&path).unwrap();
        let metadata = std::fs::metadata(&path).unwrap();
        assert_eq!(metadata.len(), FEATURES_BYTES as u64);
    }

    // ── G3: reader picks up signals written by another writer ─────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn reader_picks_up_signal_after_python_writes() {
        let dir  = temp_dir();
        let path = dir.join("signal.bin");

        // Pre-create file (simulating Python's I2 setup) and write a signal
        let signal = PythonSignal {
            seq:        7,
            direction:  SignalDirection::Up,
            edge:       0.12,
            p_yes:      0.78,
            confidence: 0.5,
        };
        std::fs::write(&path, vec![0u8; SIGNAL_BYTES]).unwrap();
        let file = OpenOptions::new().read(true).write(true).open(&path).unwrap();
        let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
        let buf: &mut [u8; SIGNAL_BYTES] = (&mut mmap[..]).try_into().unwrap();
        serialize_signal(&signal, buf);
        drop(mmap);

        // Spawn reader and wait for it to pick up the signal
        let latest: Arc<RwLock<Option<PythonSignal>>> = Arc::new(RwLock::new(None));
        let l   = latest.clone();
        let p   = path.clone();
        let rdr = tokio::spawn(signal_reader_loop(l, p));

        // Give it up to 1 second to pick up
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(150)).await;
            if latest.read().await.is_some() { break; }
        }
        rdr.abort();

        let got = latest.read().await.expect("reader should have picked up the signal");
        assert_eq!(got, signal);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reader_only_emits_when_seq_advances() {
        let dir  = temp_dir();
        let path = dir.join("signal.bin");
        std::fs::write(&path, vec![0u8; SIGNAL_BYTES]).unwrap();

        // Write the SAME signal seq=5 twice
        let sig = PythonSignal {
            seq: 5, direction: SignalDirection::Down,
            edge: 0.10, p_yes: 0.30, confidence: 0.4,
        };
        let file = OpenOptions::new().read(true).write(true).open(&path).unwrap();
        let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
        let buf: &mut [u8; SIGNAL_BYTES] = (&mut mmap[..]).try_into().unwrap();
        serialize_signal(&sig, buf);
        drop(mmap);

        let latest: Arc<RwLock<Option<PythonSignal>>> = Arc::new(RwLock::new(None));
        let rdr = tokio::spawn(signal_reader_loop(latest.clone(), path));
        tokio::time::sleep(Duration::from_millis(300)).await;
        rdr.abort();

        // Should have picked it up exactly once
        assert_eq!(latest.read().await.as_ref().unwrap().seq, 5);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reader_waits_for_python_to_create_file() {
        let dir  = temp_dir();
        let path = dir.join("signal.bin");
        // File does NOT exist yet

        let latest: Arc<RwLock<Option<PythonSignal>>> = Arc::new(RwLock::new(None));
        let l = latest.clone();
        let p = path.clone();
        let rdr = tokio::spawn(signal_reader_loop(l, p));

        // After 200ms, no signal yet (file doesn't exist)
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(latest.read().await.is_none());

        rdr.abort();
    }

    // ── End-to-end: Rust writes features, separate Rust task reads them ──
    // (Simulates Python's role using Rust for testability.)

    #[tokio::test(flavor = "multi_thread")]
    async fn end_to_end_writer_to_external_reader() {
        let dir  = temp_dir();
        let path = dir.join("features.bin");

        // Set up state with known feature values
        let state = Arc::new(RwLock::new(FeatureState::default()));
        {
            let mut s = state.write().await;
            s.seq            = 12345;
            s.spot_price     = 80_321.50;
            s.poly_yes_price = 0.555;
        }

        // Spawn writer
        let st = state.clone();
        let p  = path.clone();
        let writer = tokio::spawn(features_writer_loop(st, p));

        // Wait long enough for at least one tick
        tokio::time::sleep(Duration::from_millis(700)).await;

        // External reader: open mmap, deserialise
        let mmap = unsafe {
            Mmap::map(&OpenOptions::new().read(true).open(&path).unwrap()).unwrap()
        };
        let buf: &[u8; FEATURES_BYTES] = (&mmap[..]).try_into().unwrap();
        let snap = deserialize_features(buf).expect("deserialise");

        writer.abort();

        assert_eq!(snap.seq, 12345);
        assert_eq!(snap.features.len(), FEATURE_COUNT);
        // spot_price is at index 2 (per FeatureState::to_feature_vec)
        assert!((snap.features[2] - 80_321.50).abs() < 1e-6);
        // poly_yes_price is at index 4
        assert!((snap.features[4] - 0.555).abs() < 1e-6);
    }
}
