"""H4 + H5 — Walk-forward CV split logic and LightGBM training.

The training tests use SYNTHETIC data with a known label-determining
feature so we can verify the model converges. Random data would never
beat baseline Brier (~0.25), making it impossible to test the gate.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from model.features import ML_FEATURES
from model.train import (
    BRIER_FAIL_GATE,
    DAY_MS,
    DAYS_HOLDOUT,
    DAYS_PER_VAL,
    N_FOLDS,
    evaluate,
    run_walk_forward_training,
    train_lgbm,
    walk_forward_splits,
)


def _synthetic_dataset(
    days:          int = 90,
    seed:          int = 42,
    label_signal:  bool = True,
) -> pd.DataFrame:
    """Generate a synthetic joined feature+label DataFrame.

    If `label_signal=True`, the label is a function of `ret_5m` (with noise),
    so a competent model should achieve Brier well below the gate.
    If False, label is pure random — model should fail the gate.
    """
    rng = np.random.default_rng(seed)
    # 5-min windows over `days` days
    n_windows = days * (24 * 12)  # 12 windows per hour
    timestamps = np.arange(n_windows, dtype=np.int64) * (5 * 60_000)

    df = pd.DataFrame({"prediction_time_ms": timestamps})
    for col in ML_FEATURES:
        df[col] = rng.normal(0, 1, n_windows)
    # Make a few features have realistic ranges
    df["rsi_14"]      = rng.uniform(0, 100, n_windows)
    df["bb_position"] = rng.uniform(0, 1, n_windows)
    df["hour_of_day"] = rng.integers(0, 24, n_windows)
    df["day_of_week"] = rng.integers(0, 7, n_windows)

    if label_signal:
        # Strong signal: label = 1 when ret_5m + ret_1m is positive (with noise)
        score = df["ret_5m"] + 0.5 * df["ret_1m"] + 0.3 * rng.normal(0, 1, n_windows)
        df["label"] = (score > 0).astype(int)
    else:
        df["label"] = rng.integers(0, 2, n_windows)

    return df


# ── H4: walk_forward_splits ──────────────────────────────────────────────────

def test_split_produces_n_folds_plus_holdout():
    df = _synthetic_dataset(days=90)
    folds, holdout = walk_forward_splits(df)
    assert len(folds) == N_FOLDS
    assert len(holdout) > 0


def test_holdout_is_last_14_days():
    df = _synthetic_dataset(days=90)
    folds, holdout = walk_forward_splits(df)
    holdout_span_ms = int(holdout["prediction_time_ms"].max()) \
                    - int(holdout["prediction_time_ms"].min())
    assert holdout_span_ms <= DAYS_HOLDOUT * DAY_MS
    assert holdout_span_ms >= (DAYS_HOLDOUT - 1) * DAY_MS  # close to 14d


def test_train_grows_each_fold():
    df = _synthetic_dataset(days=90)
    folds, _ = walk_forward_splits(df)
    train_lengths = [len(f[0]) for f in folds]
    for i in range(1, len(train_lengths)):
        assert train_lengths[i] > train_lengths[i - 1], \
            f"train should grow: fold {i-1}={train_lengths[i-1]}, fold {i}={train_lengths[i]}"


def test_val_periods_non_overlapping():
    df = _synthetic_dataset(days=90)
    folds, _ = walk_forward_splits(df)
    for i in range(len(folds) - 1):
        v0_end = folds[i][1]["prediction_time_ms"].max()
        v1_start = folds[i+1][1]["prediction_time_ms"].min()
        assert v1_start > v0_end, f"vals must not overlap: fold {i}.end={v0_end} fold {i+1}.start={v1_start}"


def test_no_train_val_overlap():
    df = _synthetic_dataset(days=90)
    folds, holdout = walk_forward_splits(df)
    for i, (train, val) in enumerate(folds):
        train_end = train["prediction_time_ms"].max()
        val_start = val["prediction_time_ms"].min()
        assert val_start > train_end, f"fold {i} train/val overlap"


def test_holdout_disjoint_from_all_folds():
    df = _synthetic_dataset(days=90)
    folds, holdout = walk_forward_splits(df)
    holdout_start = holdout["prediction_time_ms"].min()
    for i, (train, val) in enumerate(folds):
        train_end = train["prediction_time_ms"].max()
        val_end   = val["prediction_time_ms"].max()
        assert holdout_start > val_end,   f"fold {i} val overlaps holdout"
        assert holdout_start > train_end, f"fold {i} train overlaps holdout"


def test_split_rejects_too_short_dataset():
    df = _synthetic_dataset(days=20)  # not enough for 5 folds + holdout
    with pytest.raises(ValueError):
        walk_forward_splits(df)


def test_each_val_is_seven_days():
    df = _synthetic_dataset(days=90)
    folds, _ = walk_forward_splits(df)
    for train, val in folds:
        span_ms = int(val["prediction_time_ms"].max()) \
                - int(val["prediction_time_ms"].min())
        # Should be just under 7 days (last bar at start + ~7d)
        assert span_ms <= DAYS_PER_VAL * DAY_MS
        assert span_ms >= (DAYS_PER_VAL - 1) * DAY_MS


# ── H5: training & Brier gate ────────────────────────────────────────────────

def test_train_lgbm_converges_on_signal_data():
    df = _synthetic_dataset(days=90, label_signal=True)
    folds, _ = walk_forward_splits(df)
    train, val = folds[0]
    booster = train_lgbm(train, val, num_boost_round=200, early_stopping=10)
    brier, ll, auc = evaluate(booster, val, ML_FEATURES)
    # Synthetic data has a strong signal — Brier should be well under gate
    assert brier < 0.20, f"Brier on signal data should be < 0.20, got {brier}"
    assert auc > 0.6,    f"AUC should be > 0.6, got {auc}"


def test_brier_gate_passes_for_signal_data():
    df = _synthetic_dataset(days=90, label_signal=True)
    report = run_walk_forward_training(df, fail_on_gate=True)
    assert report.passed_gate is True
    assert all(f.brier < BRIER_FAIL_GATE for f in report.folds)


def test_brier_gate_raises_for_random_data():
    """Random labels → Brier ~0.25 every fold → gate must FAIL LOUD."""
    df = _synthetic_dataset(days=90, label_signal=False)
    with pytest.raises(RuntimeError, match="exceeds gate"):
        run_walk_forward_training(df, fail_on_gate=True)


def test_brier_gate_warns_when_fail_on_gate_disabled():
    """With fail_on_gate=False, must not raise even on bad data."""
    df = _synthetic_dataset(days=90, label_signal=False)
    report = run_walk_forward_training(df, fail_on_gate=False)
    assert report.passed_gate is False


def test_report_contains_fold_metadata():
    df = _synthetic_dataset(days=90, label_signal=True)
    report = run_walk_forward_training(df, fail_on_gate=False)
    assert len(report.folds) == N_FOLDS
    for f in report.folds:
        assert f.train_n > 0 and f.val_n > 0
        assert f.brier >= 0 and f.brier <= 1
        assert f.train_start_ms < f.train_end_ms
        assert f.val_start_ms   <= f.val_end_ms


def test_feature_importance_populated():
    df = _synthetic_dataset(days=90, label_signal=True)
    report = run_walk_forward_training(df, fail_on_gate=False)
    assert len(report.feature_importance) == len(ML_FEATURES)
    assert sum(report.feature_importance.values()) > 0


def test_holdout_evaluated():
    df = _synthetic_dataset(days=90, label_signal=True)
    report = run_walk_forward_training(df, fail_on_gate=False)
    assert report.holdout_brier is not None
    assert report.holdout_n > 0
    # On signal data, holdout should also be well below gate
    assert report.holdout_brier < BRIER_FAIL_GATE


def test_save_model_writes_file(tmp_path):
    df = _synthetic_dataset(days=90, label_signal=True)
    save_path = tmp_path / "btc_lgbm.txt"
    report = run_walk_forward_training(df, save_path=save_path, fail_on_gate=False)
    assert save_path.exists()
    # Sidecar metadata
    assert (save_path.with_suffix(".report.json")).exists()
    # Saved model can be reloaded
    import lightgbm as lgb
    loaded = lgb.Booster(model_file=str(save_path))
    folds, _ = walk_forward_splits(df)
    _, val = folds[0]
    pred = loaded.predict(val[ML_FEATURES])
    assert len(pred) == len(val)
