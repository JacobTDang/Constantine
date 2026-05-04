"""H7 — Calibration metrics for the trained model.

Brier alone tells you about the model's accuracy. ECE (Expected Calibration
Error) tells you whether the model's CONFIDENCE is well-aligned with reality:
when the model says 0.7, do those bets actually resolve True ~70% of the time?

A well-calibrated model with mediocre Brier is more useful than a confident
mis-calibrated one — Kelly sizing depends on calibrated p_win.

Outputs:
  brier_score(y, p)
  expected_calibration_error(y, p, n_bins=10)
  reliability_curve(y, p, n_bins=10)  → (mean_pred, mean_actual, count) per bin
  save_reliability_plot(...) (optional matplotlib output)
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

import numpy as np

log = logging.getLogger(__name__)


@dataclass
class CalibrationReport:
    brier:         float
    log_loss:      float
    ece:           float
    n:             int
    bin_centers:   list[float]
    bin_pred_mean: list[float]
    bin_actual:   list[float]
    bin_count:    list[int]


def brier_score(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """Standard Brier score: mean((p - y)^2)."""
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    return float(np.mean((y_pred - y_true) ** 2))


def expected_calibration_error(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    n_bins: int = 10,
) -> float:
    """ECE: weighted abs gap between predicted prob and actual frequency.

    For each bin i: gap_i = |mean(p in bin_i) - mean(y in bin_i)|
    ECE = sum_i (count_i / N) * gap_i
    """
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    n = len(y_true)
    if n == 0:
        return 0.0

    edges = np.linspace(0.0, 1.0, n_bins + 1)
    ece = 0.0
    for i in range(n_bins):
        lo = edges[i]
        hi = edges[i + 1]
        mask = (y_pred >= lo) & (y_pred < hi if i < n_bins - 1 else y_pred <= hi)
        cnt = int(mask.sum())
        if cnt == 0:
            continue
        bin_pred   = float(y_pred[mask].mean())
        bin_actual = float(y_true[mask].mean())
        ece += (cnt / n) * abs(bin_pred - bin_actual)
    return float(ece)


def reliability_curve(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    n_bins: int = 10,
) -> tuple[list[float], list[float], list[float], list[int]]:
    """Return (bin_centers, bin_pred_mean, bin_actual_mean, bin_count).

    Used to plot a reliability diagram: x=bin_pred_mean, y=bin_actual_mean.
    A perfectly-calibrated model gives the y=x line.
    """
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)

    edges   = np.linspace(0.0, 1.0, n_bins + 1)
    centers, pred_means, actual_means, counts = [], [], [], []
    for i in range(n_bins):
        lo = edges[i]
        hi = edges[i + 1]
        mask = (y_pred >= lo) & (y_pred < hi if i < n_bins - 1 else y_pred <= hi)
        c = int(mask.sum())
        centers.append(float((lo + hi) / 2.0))
        if c == 0:
            pred_means.append(0.0)
            actual_means.append(0.0)
        else:
            pred_means.append(float(y_pred[mask].mean()))
            actual_means.append(float(y_true[mask].mean()))
        counts.append(c)
    return centers, pred_means, actual_means, counts


def calibrate_model(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    *,
    n_bins: int = 10,
) -> CalibrationReport:
    """Compute the full calibration report."""
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    n = len(y_true)

    brier = brier_score(y_true, y_pred)
    ece   = expected_calibration_error(y_true, y_pred, n_bins=n_bins)

    # log loss with epsilon clipping to avoid -inf
    eps = 1e-15
    p_clip = np.clip(y_pred, eps, 1.0 - eps)
    ll = float(-np.mean(y_true * np.log(p_clip) + (1.0 - y_true) * np.log(1.0 - p_clip)))

    centers, pred_mean, actual, counts = reliability_curve(y_true, y_pred, n_bins=n_bins)

    return CalibrationReport(
        brier=brier,
        log_loss=ll,
        ece=ece,
        n=n,
        bin_centers=centers,
        bin_pred_mean=pred_mean,
        bin_actual=actual,
        bin_count=counts,
    )


def save_calibration_report(report: CalibrationReport, path: Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(report), indent=2))


def save_reliability_plot(
    report: CalibrationReport,
    path:   Path,
    title:  str = "Reliability diagram",
) -> Optional[Path]:
    """Save a PNG plot of the reliability curve. Returns path or None
    if matplotlib is unavailable. Optional dep — never crashes the pipeline."""
    try:
        import matplotlib
        matplotlib.use("Agg")  # headless
        import matplotlib.pyplot as plt
    except ImportError:
        log.warning("matplotlib not available — skipping reliability plot")
        return None

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(6, 6))
    ax.plot([0, 1], [0, 1], "k--", linewidth=1, label="perfect")
    sizes = np.array(report.bin_count)
    sizes = 50 + 200 * sizes / max(1, sizes.max())
    ax.scatter(report.bin_pred_mean, report.bin_actual, s=sizes,
               alpha=0.7, edgecolor="black")
    ax.set_xlim(0, 1); ax.set_ylim(0, 1)
    ax.set_xlabel("Mean predicted probability")
    ax.set_ylabel("Empirical frequency")
    ax.set_title(f"{title} — Brier={report.brier:.4f}  ECE={report.ece:.4f}")
    ax.grid(alpha=0.3)
    ax.legend(loc="best")
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)
    return path
