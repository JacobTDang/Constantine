#!/usr/bin/env bash
# Phase K2: Launch both processes in dry-run mode with watchdog restart

set -euo pipefail
export DRY_RUN=true

echo "[dry_run] Building release binary..."
cargo build --release

echo "[dry_run] Starting Rust core..."
./target/release/polymarket-bot &
RUST_PID=$!

echo "[dry_run] Starting Python signal engine..."
.venv/Scripts/python python-ml/signal_engine.py &
PYTHON_PID=$!

echo "[dry_run] Watchdog active. Rust PID=$RUST_PID  Python PID=$PYTHON_PID"
echo "[dry_run] Ctrl-C to stop."

while true; do
    sleep 30
    if ! kill -0 "$RUST_PID" 2>/dev/null; then
        echo "[watchdog] Rust process died — restarting..."
        ./target/release/polymarket-bot &
        RUST_PID=$!
    fi
    if ! kill -0 "$PYTHON_PID" 2>/dev/null; then
        echo "[watchdog] Python process died — restarting..."
        .venv/Scripts/python python-ml/signal_engine.py &
        PYTHON_PID=$!
    fi
done
