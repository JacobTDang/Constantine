"""H4 + H5 — Walk-forward CV and LightGBM training pipeline.

Backtests over a single train/val split lie. The market regime in the
validation period may not match the training period. Walk-forward CV
forces the model to prove itself across multiple non-overlapping
validation windows that progressively move forward in time.

Layout (76 days available for CV after holding out the final 14):
  Fold 0: train [day  0, 41), val [day 41, 48)
  Fold 1: train [day  0, 48), val [day 48, 55)
  Fold 2: train [day  0, 55), val [day 55, 62)
  Fold 3: train [day  0, 62), val [day 62, 69)
  Fold 4: train [day  0, 69), val [day 69, 76)
  Holdout (untouched until final eval): [day 76, 90)

H5 — LightGBM training:
  - Conservative hyperparams (low LR, small leaves, anti-overfit subsampling)
  - PER-FOLD Brier score; if ANY fold's Brier > 0.23, raise loudly
  - Final fit on entire pre-holdout dataset, saved as data/models/btc_lgbm.txt
"""
from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score

from model.features import ML_FEATURES

log = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────

DAYS_HOLDOUT:    int = 14
DAYS_TOTAL:      int = 90
N_FOLDS:         int = 5
DAYS_PER_VAL:    int = 7
BRIER_FAIL_GATE: float = 0.23   # any fold above this -> abort training

DEFAULT_LGBM_PARAMS: dict = {
    "objective":         "binary",
    "metric":            ["binary_logloss", "auc"],
    "learning_rate":     0.05,
    "num_leaves":        31,
    "min_child_samples": 50,
    "feature_fraction":  0.8,
    "bagging_fraction":  0.8,
    "bagging_freq":      5,
    "max_depth":         -1,
    "verbose":           -1,
    "deterministic":     True,
    "force_col_wise":    True,
}

DAY_MS: int = 86_400_000

# ── Reports ──────────────────────────────────────────────────────────────────

@dataclass
class FoldReport:
    fold_idx:       int
    train_n:        int
    val_n:          int
    train_start_ms: int
    train_end_ms:   int
    val_start_ms:   int
    val_end_ms:     int
    brier:          float
    log_loss:       float
    auc:            float
    n_trees:        int


@dataclass
class TrainingReport:
    folds:           list[FoldReport] = field(default_factory=list)
    final_n:         int = 0
    final_n_trees:   int = 0
    feature_importance: dict[str, int] = field(default_factory=dict)
    holdout_brier:   Optional[float] = None
    holdout_n:       int = 0
    passed_gate:     bool = False


# ── H4: Walk-forward splits ──────────────────────────────────────────────────

def walk_forward_splits(
    joined: pd.DataFrame,
    *,
    days_holdout:    int = DAYS_HOLDOUT,
    days_per_val:    int = DAYS_PER_VAL,
    n_folds:         int = N_FOLDS,
) -> tuple[list[tuple[pd.DataFrame, pd.DataFrame]], pd.DataFrame]:
    """Split rows into N walk-forward (train, val) tuples + a final holdout.

    `joined` must have a 'prediction_time_ms' column. Rows are split by
    timestamp range, NEVER shuffled — order matters.
    """
    if "prediction_time_ms" not in joined.columns:
        raise ValueError("joined must have prediction_time_ms")
    if len(joined) == 0:
        raise ValueError("joined is empty")

    df = joined.sort_values("prediction_time_ms").reset_index(drop=True)
    t_min = int(df["prediction_time_ms"].iloc[0])
    t_max = int(df["prediction_time_ms"].iloc[-1])

    holdout_ms        = days_holdout * DAY_MS
    val_window_ms     = days_per_val * DAY_MS

    # Holdout occupies the LAST `days_holdout` of the dataset
    holdout_start_ms = t_max - holdout_ms
    holdout = df[df["prediction_time_ms"] >= holdout_start_ms].copy()
    cv_pool = df[df["prediction_time_ms"] <  holdout_start_ms].copy()

    # CV folds: validation windows tile the LAST `n_folds * days_per_val` of the
    # cv_pool; each fold's training set is everything before its val window.
    val_total_ms     = n_folds * val_window_ms
    cv_pool_t_max    = int(cv_pool["prediction_time_ms"].iloc[-1])
    first_val_start  = cv_pool_t_max - val_total_ms

    if first_val_start <= t_min:
        raise ValueError(
            f"insufficient data for {n_folds} × {days_per_val}-day folds "
            f"plus {days_holdout}-day holdout (need {(val_total_ms + holdout_ms)/DAY_MS:.0f} days, "
            f"have {(t_max - t_min)/DAY_MS:.1f})"
        )

    folds: list[tuple[pd.DataFrame, pd.DataFrame]] = []
    for i in range(n_folds):
        val_start = first_val_start + i * val_window_ms
        val_end   = val_start + val_window_ms
        train = cv_pool[cv_pool["prediction_time_ms"] <  val_start].copy()
        val   = cv_pool[(cv_pool["prediction_time_ms"] >= val_start) &
                        (cv_pool["prediction_time_ms"] <  val_end)].copy()
        if len(train) == 0 or len(val) == 0:
            raise ValueError(f"fold {i} has empty train/val: {len(train)}/{len(val)}")
        folds.append((train, val))

    return folds, holdout


# ── H5: LightGBM training ────────────────────────────────────────────────────

def train_lgbm(
    train_df:        pd.DataFrame,
    val_df:          Optional[pd.DataFrame] = None,
    *,
    feature_cols:    list[str] = None,
    params:          Optional[dict] = None,
    num_boost_round: int = 500,
    early_stopping:  int = 30,
) -> lgb.Booster:
    """Fit a LightGBM binary classifier on `train_df`.

    `train_df` must have all `feature_cols` plus a 'label' column.
    """
    feature_cols = feature_cols or ML_FEATURES
    params       = {**DEFAULT_LGBM_PARAMS, **(params or {})}

    X_train = train_df[feature_cols]
    y_train = train_df["label"].astype(int)

    train_set = lgb.Dataset(X_train, label=y_train)
    valid_sets   = [train_set]
    valid_names  = ["train"]
    callbacks    = []

    if val_df is not None and len(val_df) > 0:
        X_val = val_df[feature_cols]
        y_val = val_df["label"].astype(int)
        val_set = lgb.Dataset(X_val, label=y_val, reference=train_set)
        valid_sets.append(val_set)
        valid_names.append("val")
        callbacks = [
            lgb.early_stopping(early_stopping, verbose=False),
            lgb.log_evaluation(period=0),
        ]
    else:
        callbacks = [lgb.log_evaluation(period=0)]

    booster = lgb.train(
        params=params,
        train_set=train_set,
        num_boost_round=num_boost_round,
        valid_sets=valid_sets,
        valid_names=valid_names,
        callbacks=callbacks,
    )
    return booster


def evaluate(booster: lgb.Booster, df: pd.DataFrame, feature_cols: list[str]) -> tuple[float, float, float]:
    """Return (brier, log_loss, auc) on `df`."""
    X = df[feature_cols]
    y = df["label"].astype(int).values
    p = booster.predict(X, num_iteration=booster.best_iteration or booster.current_iteration())
    brier = float(brier_score_loss(y, p))
    ll    = float(log_loss(y, p, labels=[0, 1]))
    # AUC undefined when only one class present
    if len(np.unique(y)) < 2:
        auc = float("nan")
    else:
        auc = float(roc_auc_score(y, p))
    return brier, ll, auc


def run_walk_forward_training(
    joined:          pd.DataFrame,
    *,
    feature_cols:    list[str] = None,
    params:          Optional[dict] = None,
    brier_gate:      float = BRIER_FAIL_GATE,
    fail_on_gate:    bool = True,
    save_path:       Optional[Path] = None,
) -> TrainingReport:
    """Full pipeline: split → train each fold → final fit → optional save.

    Raises RuntimeError if any fold's Brier exceeds `brier_gate` and
    `fail_on_gate` is True.
    """
    feature_cols = feature_cols or ML_FEATURES
    folds, holdout = walk_forward_splits(joined)
    report = TrainingReport()

    for i, (train, val) in enumerate(folds):
        booster = train_lgbm(train, val, feature_cols=feature_cols, params=params)
        brier, ll, auc = evaluate(booster, val, feature_cols)

        fr = FoldReport(
            fold_idx=i,
            train_n=len(train),
            val_n=len(val),
            train_start_ms=int(train["prediction_time_ms"].iloc[0]),
            train_end_ms=int(train["prediction_time_ms"].iloc[-1]),
            val_start_ms=int(val["prediction_time_ms"].iloc[0]),
            val_end_ms=int(val["prediction_time_ms"].iloc[-1]),
            brier=brier,
            log_loss=ll,
            auc=auc,
            n_trees=booster.best_iteration or booster.current_iteration(),
        )
        report.folds.append(fr)
        log.info(
            "fold %d: train=%d val=%d brier=%.4f ll=%.4f auc=%.4f trees=%d",
            i, fr.train_n, fr.val_n, fr.brier, fr.log_loss, fr.auc, fr.n_trees,
        )

        if brier > brier_gate:
            msg = (f"fold {i} Brier {brier:.4f} exceeds gate {brier_gate:.4f} "
                   f"— model is no better than baseline noise; aborting training")
            if fail_on_gate:
                raise RuntimeError(msg)
            log.warning(msg)

    # Final fit on all CV pool (no validation set, fixed iteration count)
    cv_pool = pd.concat([f[0] for f in folds] + [folds[-1][1]])
    cv_pool = cv_pool.sort_values("prediction_time_ms").reset_index(drop=True)

    avg_trees = int(np.mean([f.n_trees for f in report.folds]))
    final_params = {**DEFAULT_LGBM_PARAMS, **(params or {})}
    final = train_lgbm(
        cv_pool,
        val_df=None,
        feature_cols=feature_cols,
        params=final_params,
        num_boost_round=avg_trees,
    )
    report.final_n        = len(cv_pool)
    report.final_n_trees  = avg_trees

    importance = final.feature_importance(importance_type="gain")
    report.feature_importance = dict(zip(feature_cols, importance.tolist()))

    # Final eval on holdout (the ONLY time we touch it)
    if len(holdout) > 0:
        b, _, _ = evaluate(final, holdout, feature_cols)
        report.holdout_brier = b
        report.holdout_n     = len(holdout)
        log.info("holdout: n=%d brier=%.4f", len(holdout), b)

    report.passed_gate = all(f.brier <= brier_gate for f in report.folds)

    if save_path:
        save_path = Path(save_path)
        save_path.parent.mkdir(parents=True, exist_ok=True)
        final.save_model(str(save_path))
        meta_path = save_path.with_suffix(".report.json")
        meta_path.write_text(json.dumps(asdict(report), indent=2))
        log.info("saved model: %s", save_path)

    return report
