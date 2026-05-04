#!/usr/bin/env bash
# Phase M4: Launch in live trading mode — DO NOT RUN before completing checklist in PHASES.md M1

set -euo pipefail

if [[ "${1:-}" != "--i-have-completed-the-checklist" ]]; then
    echo "ERROR: You must complete the pre-deployment checklist in PHASES.md (Milestone M1)"
    echo "       before running live. Then re-run with flag: --i-have-completed-the-checklist"
    exit 1
fi

export DRY_RUN=false

echo "[live] Building release binary..."
cargo build --release

echo "[live] Starting Rust core (LIVE MODE)..."
./target/release/polymarket-bot &
RUST_PID=$!

echo "[live] Starting Python signal engine (LIVE MODE)..."
.venv/Scripts/python python-ml/signal_engine.py &
PYTHON_PID=$!

echo "[live] LIVE TRADING ACTIVE. Rust PID=$RUST_PID  Python PID=$PYTHON_PID"

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
