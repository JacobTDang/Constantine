"""H3 — tests for label creation and feature/label joining."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from data.label import join_features_and_labels, label_windows
from model.features import ML_FEATURES, compute_features


def _klines(closes: list[float], start_ms: int = 0) -> pd.DataFrame:
    return pd.DataFrame({
        "open_time": [start_ms + i * 60_000 for i in range(len(closes))],
        "close":     closes,
    })


# ── Window aggregation ────────────────────────────────────────────────────────

def test_label_windows_5min_complete():
    # 10 bars = 2 complete 5-min windows
    closes = [100.0, 101.0, 102.0, 103.0, 104.0,
              105.0, 104.0, 103.0, 102.0, 101.0]
    df = _klines(closes, start_ms=0)
    labels = label_windows(df, duration_min=5)
    assert len(labels) == 2
    # Window 0: closes 100→104 → label = 1 (Up)
    # Window 1: closes 105→101 → label = 0 (Down)
    assert labels.iloc[0]["label"] == 1
    assert labels.iloc[1]["label"] == 0


def test_label_windows_15min_complete():
    closes = list(range(100, 130))  # 30 bars = 2 windows of 15 min
    df = _klines(closes, start_ms=0)
    labels = label_windows(df, duration_min=15)
    assert len(labels) == 2
    # All windows are uptrend: label = 1
    assert (labels["label"] == 1).all()


def test_partial_window_dropped():
    closes = list(range(100, 107))  # 7 bars — only 1 complete 5-min window
    df = _klines(closes, start_ms=0)
    labels = label_windows(df, duration_min=5)
    assert len(labels) == 1


def test_empty_klines_yields_empty_labels():
    df = pd.DataFrame(columns=["open_time", "close"])
    out = label_windows(df, 5)
    assert out.empty


def test_invalid_duration_rejected():
    df = _klines([100.0, 101.0])
    with pytest.raises(ValueError):
        label_windows(df, duration_min=10)
    with pytest.raises(ValueError):
        label_windows(df, duration_min=0)


# ── Field correctness ────────────────────────────────────────────────────────

def test_open_price_is_first_bar_close():
    closes = [100.0, 200.0, 300.0, 400.0, 500.0]
    df = _klines(closes, start_ms=0)
    labels = label_windows(df, duration_min=5)
    assert labels.iloc[0]["open_price"] == 100.0


def test_close_price_is_last_bar_close():
    closes = [100.0, 200.0, 300.0, 400.0, 500.0]
    df = _klines(closes, start_ms=0)
    labels = label_windows(df, duration_min=5)
    assert labels.iloc[0]["close_price"] == 500.0


def test_prediction_time_aligned_to_window_open():
    # base_ms must be aligned to a 5-min boundary (multiple of 300_000)
    base_ms = (1_700_000_000_000 // 300_000) * 300_000
    df = _klines([100.0] * 5, start_ms=base_ms)
    labels = label_windows(df, duration_min=5)
    assert labels.iloc[0]["prediction_time_ms"] == base_ms


def test_resolution_time_is_window_close():
    base_ms = (1_700_000_000_000 // 300_000) * 300_000
    df = _klines([100.0] * 5, start_ms=base_ms)
    labels = label_windows(df, duration_min=5)
    # Window close = open_time of last bar + 60_000 (one minute)
    assert labels.iloc[0]["resolution_time_ms"] == base_ms + 5 * 60_000


# ── Lookahead guarantee at the join boundary ─────────────────────────────────

def test_join_aligns_features_at_prediction_time():
    """The joined feature row must come from PRECISELY the prediction_time
    boundary — i.e. computed using only data BEFORE the window opens."""
    rng = np.random.default_rng(7)
    closes = [80_000.0]
    for _ in range(120):
        closes.append(closes[-1] * float(np.exp(rng.normal(0, 0.001))))
    df = _klines(closes, start_ms=0)

    feat = compute_features(df)
    labels = label_windows(df, duration_min=5)
    joined = join_features_and_labels(feat, labels, ML_FEATURES)

    # For every joined row, the prediction_time_ms must equal an open_time
    # in the feature DataFrame
    feat_times = set(feat["open_time"].astype(int).tolist())
    for t in joined["prediction_time_ms"]:
        assert int(t) in feat_times


def test_join_drops_unmatched_labels():
    closes = [100.0] * 30
    df = _klines(closes, start_ms=0)
    feat = compute_features(df)
    labels = label_windows(df, duration_min=5)

    # Add a synthetic label that doesn't have a matching feature row
    bogus = labels.iloc[[0]].copy()
    bogus["prediction_time_ms"] = 999_999_999_999_999
    labels_with_bogus = pd.concat([labels, bogus], ignore_index=True)

    joined = join_features_and_labels(feat, labels_with_bogus, ML_FEATURES)
    assert len(joined) == len(labels)  # bogus dropped


def test_label_value_consistent_with_inputs():
    # Manually verify a single window
    df = _klines([100.0, 99.0, 99.5, 99.8, 101.0], start_ms=0)
    labels = label_windows(df, duration_min=5)
    assert labels.iloc[0]["open_price"]  == 100.0
    assert labels.iloc[0]["close_price"] == 101.0
    assert labels.iloc[0]["label"]       == 1
