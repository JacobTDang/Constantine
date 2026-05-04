"""H2 — tests for feature engineering. Verified against the Rust impl.

The Rust unit tests use known input sequences and hand-computed expected
values; we replicate those same fixtures here to ensure parity.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from model.features import (
    ML_FEATURES,
    autocorr_lag1,
    bollinger_position,
    compute_features,
    macd_signal,
    wilder_rsi,
)


def _klines(closes: list[float], start_ms: int = 0) -> pd.DataFrame:
    return pd.DataFrame({
        "open_time": [start_ms + i * 60_000 for i in range(len(closes))],
        "close":     closes,
    })


# ── Wilder RSI — verified against Rust unit tests ─────────────────────────────

def test_rsi_known_sequence_matches_rust_56_25():
    """Same input as Rust's `rsi_known_sequence` test → expected 56.25."""
    prices = np.array([10.0, 11.0, 10.0, 12.0, 11.0, 10.0, 9.0,
                       10.0, 11.0, 12.0, 11.0, 13.0, 12.0, 11.0, 12.0])
    rsi = wilder_rsi(prices, 14)
    # Final value should be 56.25
    assert abs(rsi[-1] - 56.25) < 1e-6, f"got {rsi[-1]}"


def test_rsi_all_up_is_100():
    prices = np.array([100.0 + i for i in range(16)])
    assert abs(wilder_rsi(prices, 14)[-1] - 100.0) < 1e-6


def test_rsi_all_down_is_zero():
    prices = np.array([100.0 - i for i in range(16)])
    assert abs(wilder_rsi(prices, 14)[-1]) < 1e-6


def test_rsi_insufficient_data_returns_50():
    prices = np.array([100.0, 101.0])
    rsi = wilder_rsi(prices, 14)
    assert (rsi == 50.0).all()


# ── Bollinger Bands ──────────────────────────────────────────────────────────

def test_bb_flat_prices_returns_half():
    closes = pd.Series([100.0] * 25)
    bb = bollinger_position(closes, period=20, std_dev=2.0)
    # Sigma is 0 → fallback to 0.5
    assert abs(bb.iloc[-1] - 0.5) < 1e-6


def test_bb_clamps_above_upper():
    closes = pd.Series([100.0] * 24 + [200.0])
    bb = bollinger_position(closes, period=20, std_dev=2.0)
    assert bb.iloc[-1] <= 1.1
    assert bb.iloc[-1] > 0.9  # well above middle


def test_bb_position_within_range():
    rng = np.random.default_rng(42)
    closes = pd.Series(80_000.0 + rng.normal(0, 50, 200))
    bb = bollinger_position(closes, period=20, std_dev=2.0)
    # Strictly within [-0.1, 1.1]
    valid = bb.dropna()
    assert (valid >= -0.1).all()
    assert (valid <=  1.1).all()


# ── MACD ─────────────────────────────────────────────────────────────────────

def test_macd_uptrend_positive_signal():
    closes = pd.Series([100.0 + i * 0.5 for i in range(60)])
    sig = macd_signal(closes)
    assert sig.iloc[-1] > 0


def test_macd_downtrend_negative_signal():
    closes = pd.Series([200.0 - i * 0.5 for i in range(60)])
    sig = macd_signal(closes)
    assert sig.iloc[-1] < 0


def test_macd_zero_for_first_26_rows():
    closes = pd.Series([100.0 + i * 0.5 for i in range(60)])
    sig = macd_signal(closes)
    assert (sig.iloc[:26] == 0.0).all()


# ── Autocorrelation ──────────────────────────────────────────────────────────

def test_autocorr_alternating_returns_negative():
    rets = pd.Series([0.01, -0.01] * 30)
    ac = autocorr_lag1(rets, window=60)
    assert ac.iloc[-1] < 0


def test_autocorr_trending_returns_positive():
    rets = pd.Series([i * 0.001 for i in range(1, 61)])
    ac = autocorr_lag1(rets, window=60)
    assert ac.iloc[-1] > 0


def test_autocorr_empty_or_constant_returns_zero():
    rets = pd.Series([0.001] * 60)
    ac = autocorr_lag1(rets, window=60)
    assert abs(ac.iloc[-1]) < 1e-9


# ── Returns ──────────────────────────────────────────────────────────────────

def test_log_returns_match_hand_computed():
    df = _klines([98.0, 101.0, 99.0, 102.0, 100.0])
    out = compute_features(df)
    expected_1m = np.log(100.0 / 102.0)
    expected_5m_invalid = np.nan  # need 5 prior bars; we have 4
    assert abs(out["ret_1m"].iloc[-1] - expected_1m) < 1e-12


def test_log_returns_nan_at_start():
    df = _klines([100.0, 101.0])
    out = compute_features(df)
    assert pd.isna(out["ret_1m"].iloc[0])
    assert not pd.isna(out["ret_1m"].iloc[1])


# ── Volatility ───────────────────────────────────────────────────────────────

def test_vol_5m_constant_returns_is_zero():
    closes = [100.0 * (1.001 ** i) for i in range(30)]  # constant ret_1m
    df = _klines(closes)
    out = compute_features(df)
    # All 1-min returns are ln(1.001), which is constant → vol_5m = 0
    assert abs(out["vol_5m"].iloc[-1]) < 1e-12


def test_vol_ratio_exists_and_nonneg_when_30m_nonzero():
    rng = np.random.default_rng(0)
    closes = [80_000.0]
    for _ in range(60):
        closes.append(closes[-1] * float(np.exp(rng.normal(0, 0.001))))
    df = _klines(closes)
    out = compute_features(df)
    val = out["vol_ratio"].iloc[-1]
    assert val >= 0
    assert pd.notna(val)


# ── Integration — compute_features end-to-end ────────────────────────────────

def test_compute_features_emits_all_columns():
    # 60 random-walk bars
    rng = np.random.default_rng(1)
    closes = [80_000.0]
    for _ in range(99):
        closes.append(closes[-1] * float(np.exp(rng.normal(0, 0.001))))
    df = _klines(closes)
    out = compute_features(df)
    for col in ML_FEATURES:
        assert col in out.columns, f"missing feature: {col}"


def test_compute_features_preserves_open_time_close():
    df = _klines([100.0, 101.0, 102.0, 100.0, 99.0])
    out = compute_features(df)
    assert (out["open_time"].values == df["open_time"].values).all()
    assert (out["close"].values     == df["close"].values).all()


# ── LOOKAHEAD GUARANTEE ──────────────────────────────────────────────────────
# This is THE critical test. We mutate values AFTER row T and verify that
# features at row T do NOT change. If any feature at T uses data from > T,
# this test will fail.

def test_no_lookahead_features_only_use_past_data():
    rng = np.random.default_rng(123)
    closes_base = [80_000.0]
    for _ in range(100):
        closes_base.append(closes_base[-1] * float(np.exp(rng.normal(0, 0.001))))

    df_base = _klines(closes_base)
    out_base = compute_features(df_base)

    # Take a row in the middle and remember its features
    cut = 50
    base_row = out_base[ML_FEATURES].iloc[cut].copy()

    # Now mutate every close from row cut+1 onwards (massively)
    closes_mut = closes_base.copy()
    for i in range(cut + 1, len(closes_mut)):
        closes_mut[i] = closes_mut[i] * 5.0
    df_mut = _klines(closes_mut)
    out_mut = compute_features(df_mut)
    mut_row = out_mut[ML_FEATURES].iloc[cut]

    # Every feature at row `cut` must be IDENTICAL — it can only use data <= cut
    for col in ML_FEATURES:
        b = base_row[col]
        m = mut_row[col]
        if pd.isna(b) and pd.isna(m):
            continue
        assert b == m, f"LOOKAHEAD LEAK in '{col}' at row {cut}: base={b}, mut={m}"


# ── ML_FEATURES contract ─────────────────────────────────────────────────────

def test_ml_features_list_has_13_entries():
    assert len(ML_FEATURES) == 13
    # No duplicates
    assert len(set(ML_FEATURES)) == 13
