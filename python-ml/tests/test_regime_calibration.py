"""H6 + H7 — tests for HMM regime detector and calibration metrics."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from model.calibration import (
    brier_score,
    calibrate_model,
    expected_calibration_error,
    reliability_curve,
    save_calibration_report,
)
from model.regime import Regime, RegimeDetector, fit_regime_hmm


# ── H6: HMM regime detector ──────────────────────────────────────────────────

def _synthetic_vol_data(n_per_state: int = 500, seed: int = 0) -> pd.DataFrame:
    """Synthesise vol_5m / vol_30m with 3 distinct regimes."""
    rng = np.random.default_rng(seed)
    # Low-vol ranging: low vol, no trend
    rng_lo = np.column_stack([
        rng.normal(0.001, 0.0002, n_per_state),
        rng.normal(0.001, 0.0002, n_per_state),
    ])
    # Low-vol trending: similar vol but slight elevation in 5m
    rng_tr = np.column_stack([
        rng.normal(0.0015, 0.0003, n_per_state),
        rng.normal(0.0012, 0.0003, n_per_state),
    ])
    # High-vol event: much higher
    rng_hi = np.column_stack([
        rng.normal(0.010, 0.002, n_per_state),
        rng.normal(0.008, 0.002, n_per_state),
    ])
    obs = np.vstack([rng_lo, rng_tr, rng_hi])
    return pd.DataFrame(obs, columns=["vol_5m", "vol_30m"])


def test_hmm_fits_and_identifies_high_vol_state():
    df = _synthetic_vol_data(n_per_state=500)
    detector = fit_regime_hmm(df, n_iter=50)
    # The high-vol-state regime should be HIGH_VOL_EVENT
    assert detector.state_to_regime[detector.high_vol_state] == Regime.HIGH_VOL_EVENT


def test_hmm_predicts_high_vol_on_high_vol_input():
    df = _synthetic_vol_data(n_per_state=500)
    detector = fit_regime_hmm(df, n_iter=50)

    # Feed it a clearly high-vol observation history
    high_vol_obs = np.tile(np.array([[0.012, 0.010]]), (50, 1))
    regime = detector.predict_current(high_vol_obs)
    assert regime == Regime.HIGH_VOL_EVENT


def test_hmm_predicts_calm_on_calm_input():
    df = _synthetic_vol_data(n_per_state=500)
    detector = fit_regime_hmm(df, n_iter=50)
    calm_obs = np.tile(np.array([[0.001, 0.001]]), (50, 1))
    regime = detector.predict_current(calm_obs)
    assert regime != Regime.HIGH_VOL_EVENT


def test_is_tradeable_blocks_high_vol():
    df = _synthetic_vol_data(n_per_state=500)
    detector = fit_regime_hmm(df, n_iter=50)
    high_vol_obs = np.tile(np.array([[0.015, 0.012]]), (30, 1))
    assert detector.is_tradeable(high_vol_obs) is False


def test_hmm_rejects_too_short_dataset():
    df = pd.DataFrame({"vol_5m": [0.001] * 50, "vol_30m": [0.001] * 50})
    with pytest.raises(ValueError):
        fit_regime_hmm(df)


def test_hmm_rejects_missing_columns():
    df = pd.DataFrame({"vol_5m": [0.001] * 200})
    with pytest.raises(ValueError):
        fit_regime_hmm(df)


def test_predict_current_rejects_wrong_shape():
    df = _synthetic_vol_data(n_per_state=500)
    detector = fit_regime_hmm(df, n_iter=50)
    with pytest.raises(ValueError):
        detector.predict_current(np.array([0.001, 0.001]))  # 1D


def test_save_and_load_roundtrip(tmp_path: Path):
    df = _synthetic_vol_data(n_per_state=500)
    detector = fit_regime_hmm(df, n_iter=50)
    save_path = tmp_path / "hmm.pkl"
    detector.save(save_path)
    assert save_path.exists()

    loaded = RegimeDetector.load(save_path)
    high_obs = np.tile(np.array([[0.012, 0.010]]), (30, 1))
    assert loaded.predict_current(high_obs) == detector.predict_current(high_obs)


# ── H7: Calibration metrics ──────────────────────────────────────────────────

def test_brier_perfect_predictions():
    y = np.array([0, 0, 1, 1, 0, 1])
    p = np.array([0.0, 0.0, 1.0, 1.0, 0.0, 1.0])
    assert brier_score(y, p) == 0.0


def test_brier_worst_predictions():
    y = np.array([0, 1])
    p = np.array([1.0, 0.0])
    # |1-0|^2 + |0-1|^2 / 2 = 1.0
    assert brier_score(y, p) == 1.0


def test_brier_random_50_50():
    rng = np.random.default_rng(42)
    n = 10_000
    y = rng.integers(0, 2, n)
    p = np.full(n, 0.5)
    # E[(0.5 - y)^2] = 0.25
    assert abs(brier_score(y, p) - 0.25) < 0.01


def test_ece_zero_for_perfectly_calibrated():
    n = 2000
    rng = np.random.default_rng(0)
    p = rng.uniform(0, 1, n)
    y = (rng.uniform(0, 1, n) < p).astype(int)  # outcome generated FROM p
    ece = expected_calibration_error(y, p, n_bins=10)
    # With 2k samples it's usually < 0.04 for truly calibrated p
    assert ece < 0.05


def test_ece_high_when_overconfident():
    # Model says 0.9 always, but only 50% are 1
    n = 1000
    y = np.concatenate([np.ones(n // 2), np.zeros(n // 2)])
    p = np.full(n, 0.9)
    ece = expected_calibration_error(y, p, n_bins=10)
    assert ece > 0.3


def test_reliability_curve_shape():
    y = np.array([0, 0, 1, 1, 0, 1, 0, 1])
    p = np.array([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8])
    centers, pred_mean, actual, counts = reliability_curve(y, p, n_bins=10)
    assert len(centers) == 10
    assert len(pred_mean) == 10
    assert len(actual) == 10
    assert sum(counts) == len(y)


def test_calibrate_model_full_report():
    rng = np.random.default_rng(1)
    n = 2000
    p = rng.uniform(0, 1, n)
    y = (rng.uniform(0, 1, n) < p).astype(int)
    report = calibrate_model(y, p, n_bins=10)
    assert report.n == n
    assert 0.0 <= report.brier <= 1.0
    assert 0.0 <= report.ece   <= 1.0
    assert report.log_loss >= 0
    assert len(report.bin_centers) == 10


def test_save_calibration_report(tmp_path: Path):
    rng = np.random.default_rng(0)
    n = 200
    p = rng.uniform(0, 1, n)
    y = rng.integers(0, 2, n)
    report = calibrate_model(y, p)
    path = tmp_path / "calibration.json"
    save_calibration_report(report, path)
    assert path.exists()
    import json
    loaded = json.loads(path.read_text())
    assert loaded["n"] == n
    assert "brier" in loaded
    assert "ece" in loaded


def test_calibrate_handles_empty():
    report = calibrate_model(np.array([]), np.array([]))
    assert report.n == 0
    # ECE on empty should be 0
    assert report.ece == 0.0
