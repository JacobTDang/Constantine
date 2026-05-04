"""H3 — Label creation aligned to 5-min and 15-min Polymarket windows.

Polymarket BTC "Up or Down" markets ask: "Will BTC be higher than the
window-open price at window close?" Labels are 1 if close > open, else 0.

CRITICAL: features computed at row t MUST only use data from <= t. The
label is observed at the CLOSE of the window, but features used to predict
it must be from BEFORE the window opens.

Window alignment:
  - 5-min  windows: aligned to :00, :05, :10, ...
  - 15-min windows: aligned to :00, :15, :30, :45

For each window we record:
  prediction_time_ms = window_open_ms        (last second features can use)
  resolution_time_ms = window_close_ms       (when label is known)
  open_price                                   (strike at window open)
  close_price                                  (price at window close)
  label                                        (1 if close > open else 0)
"""
from __future__ import annotations

import pandas as pd


def label_windows(
    klines:        pd.DataFrame,
    duration_min:  int,
) -> pd.DataFrame:
    """Aggregate 1-min klines into windowed labels.

    Args:
      klines:       DataFrame with 'open_time' (ms) and 'close' (float)
      duration_min: 5 or 15 (Polymarket BTC window lengths)

    Returns:
      DataFrame with one row per closed window, columns:
        prediction_time_ms, resolution_time_ms, open_price, close_price, label
    """
    if duration_min not in (5, 15):
        raise ValueError(f"duration_min must be 5 or 15, got {duration_min}")

    df = klines.sort_values("open_time").reset_index(drop=True)
    if df.empty:
        return _empty_labels()

    duration_ms = duration_min * 60_000
    # Bucket each 1-min bar into its window by integer division of open_time
    df = df.assign(window_id=df["open_time"] // duration_ms)

    # Keep only complete windows (those that have all `duration_min` bars present)
    counts = df.groupby("window_id").size()
    complete_window_ids = counts[counts == duration_min].index
    df = df[df["window_id"].isin(complete_window_ids)]

    if df.empty:
        return _empty_labels()

    # Aggregate
    grouped = df.groupby("window_id")
    open_prices  = grouped.first()["close"]   # close of the FIRST bar in window
    close_prices = grouped.last()["close"]    # close of the LAST bar in window
    open_times   = grouped.first()["open_time"].astype("int64")
    close_times  = grouped.last()["open_time"].astype("int64") + 60_000  # +1min for actual close

    # IMPORTANT: open_price is taken from the FIRST 1-min bar's close in the window.
    # Polymarket actually uses Chainlink at exact window-open time. Using the
    # 1-min bar close as a proxy introduces a ~30s timing skew but is the
    # cleanest available approximation from 1-min historical data.

    out = pd.DataFrame({
        "prediction_time_ms": open_times.values,           # use features <= this
        "resolution_time_ms": close_times.values,
        "open_price":         open_prices.values,
        "close_price":        close_prices.values,
        "label":              (close_prices.values > open_prices.values).astype(int),
    })
    return out.sort_values("prediction_time_ms").reset_index(drop=True)


def _empty_labels() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "prediction_time_ms", "resolution_time_ms",
        "open_price", "close_price", "label",
    ])


def join_features_and_labels(
    feature_df:       pd.DataFrame,
    labels:           pd.DataFrame,
    feature_cols:     list[str],
) -> pd.DataFrame:
    """Inner-join features with labels by prediction_time_ms.

    This is the lookahead-safe joining step: for each label row at
    prediction_time_ms = T, we attach the FEATURE row whose open_time = T.
    The feature values reflect data computed using bars 0..T-1, so they're
    knowable at moment T (the window-open boundary).

    Returns DataFrame with: features..., label, prediction_time_ms,
    resolution_time_ms.
    """
    if feature_df.empty or labels.empty:
        return pd.DataFrame(columns=feature_cols + [
            "label", "prediction_time_ms", "resolution_time_ms",
        ])

    fdf = feature_df[["open_time"] + feature_cols].copy()
    fdf = fdf.rename(columns={"open_time": "prediction_time_ms"})

    joined = labels.merge(fdf, on="prediction_time_ms", how="inner")
    return joined.sort_values("prediction_time_ms").reset_index(drop=True)
