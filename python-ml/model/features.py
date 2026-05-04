"""H2 — Feature engineering for the ML training pipeline.

These features must match the live Rust feature computation exactly.
Functions here mirror `rust-core/src/features/compute.rs` so the model
trained offline behaves the same on live inference.

Subset rationale: only features that have a 90-day historical proxy are
included. Polymarket-specific fields (oracle_gap, poly_obi, arb_gap,
spread, time_to_close, poly_yes_price) are LIVE-ONLY and stay out of the
ML model — they're used by the rule-based signals (E1-E5).

Feature list (13):
  ret_1m, ret_3m, ret_5m            (C3)
  vol_5m, vol_30m, vol_ratio, vol_z_score   (C7)
  rsi_14                            (C4 — Wilder smoothing)
  bb_position                       (C5)
  macd_signal                       (C6)
  autocorr_lag1                     (C8)
  hour_of_day, day_of_week          (time encoding)
"""
from __future__ import annotations

import numpy as np
import pandas as pd

ML_FEATURES: list[str] = [
    "ret_1m", "ret_3m", "ret_5m",
    "vol_5m", "vol_30m", "vol_ratio", "vol_z_score",
    "rsi_14", "bb_position", "macd_signal", "autocorr_lag1",
    "hour_of_day", "day_of_week",
]

# ── Indicators ────────────────────────────────────────────────────────────────

def wilder_rsi(close: np.ndarray, period: int = 14) -> np.ndarray:
    """Wilder's RSI matching `compute_rsi` in rust-core/src/features/compute.rs.

    Initial averages: simple mean of first `period` price changes.
    Subsequent: avg[t] = avg[t-1] * (period-1)/period + value[t]/period.
    Returns 50.0 (neutral) for indices where there's not enough data.
    """
    n = len(close)
    out = np.full(n, 50.0)
    if n <= period:
        return out

    deltas = np.diff(close)  # length n-1
    # Initial seed from first `period` deltas
    seed_gains  = np.where(deltas[:period] >= 0,  deltas[:period], 0.0)
    seed_losses = np.where(deltas[:period] <  0, -deltas[:period], 0.0)
    avg_gain = float(seed_gains.mean())
    avg_loss = float(seed_losses.mean())

    out[period] = _rsi_value(avg_gain, avg_loss)

    for i in range(period + 1, n):
        d = deltas[i - 1]
        gain = max(d,  0.0)
        loss = max(-d, 0.0)
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        out[i] = _rsi_value(avg_gain, avg_loss)

    return out


def _rsi_value(avg_gain: float, avg_loss: float) -> float:
    if avg_loss < 1e-10:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - 100.0 / (1.0 + rs)


def bollinger_position(close: pd.Series, period: int = 20, std_dev: float = 2.0) -> pd.Series:
    """Position within Bollinger Bands. Matches `compute_bb_position`.

    Returns 0.5 (band centre) when sigma is degenerate. Clamps to [-0.1, 1.1].
    """
    rolling = close.rolling(window=period, min_periods=2)
    mean   = rolling.mean()
    sigma  = rolling.std(ddof=1)
    upper  = mean + std_dev * sigma
    lower  = mean - std_dev * sigma
    band_range = (upper - lower).replace(0, np.nan)

    position = (close - lower) / band_range
    position = position.fillna(0.5)
    return position.clip(-0.1, 1.1)


def macd_signal(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.Series:
    """MACD signal line. Matches `compute_macd_signal`.

    Both Rust and pandas use EMA with alpha=2/(period+1) and seed = first value.
    Returns 0.0 for indices before the slow EMA has enough data.
    """
    ema_fast    = close.ewm(alpha=2.0 / (fast + 1),   adjust=False).mean()
    ema_slow    = close.ewm(alpha=2.0 / (slow + 1),   adjust=False).mean()
    macd_line   = ema_fast - ema_slow
    signal_line = macd_line.ewm(alpha=2.0 / (signal + 1), adjust=False).mean()

    # Rust code returns 0 when len < 26; mirror that to avoid teaching the model
    # spurious early-warmup signal values.
    out = signal_line.copy()
    out.iloc[: slow] = 0.0
    return out


def autocorr_lag1(returns: pd.Series, window: int = 60) -> pd.Series:
    """Rolling Pearson lag-1 autocorrelation. Matches `compute_autocorr_lag1`.

    Returns 0.0 when fewer than 3 values or std is degenerate.
    """
    def _calc(x: np.ndarray) -> float:
        if len(x) < 3:
            return 0.0
        a = x[:-1]
        b = x[1:]
        sa = a.std()
        sb = b.std()
        if sa < 1e-10 or sb < 1e-10:
            return 0.0
        return float(np.corrcoef(a, b)[0, 1])

    return returns.rolling(window=window, min_periods=3).apply(_calc, raw=True).fillna(0.0)


# ── Public entry point ────────────────────────────────────────────────────────

def compute_features(klines: pd.DataFrame) -> pd.DataFrame:
    """Compute the full ML feature set from raw 1-min klines.

    Input:  DataFrame with 'open_time' (int64 ms) and 'close' (float).
    Output: Same DataFrame with feature columns appended; original cols kept.

    LOOKAHEAD GUARANTEE: every feature at row t uses only data from rows <= t.
    Verified by `test_no_lookahead` in tests/test_features.py.
    """
    if "open_time" not in klines.columns or "close" not in klines.columns:
        raise ValueError("input must have 'open_time' and 'close' columns")

    df = klines.sort_values("open_time").reset_index(drop=True).copy()
    close = df["close"].astype(float)

    # Log returns at multiple horizons
    df["ret_1m"] = np.log(close / close.shift(1))
    df["ret_3m"] = np.log(close / close.shift(3))
    df["ret_5m"] = np.log(close / close.shift(5))

    # Volatility from 1-min log returns
    ret_1m = df["ret_1m"]
    df["vol_5m"]   = ret_1m.rolling(5,  min_periods=2).std(ddof=1)
    df["vol_30m"]  = ret_1m.rolling(30, min_periods=2).std(ddof=1)
    safe_30m       = df["vol_30m"].replace(0, np.nan)
    df["vol_ratio"]   = df["vol_5m"] / safe_30m
    df["vol_z_score"] = (df["vol_5m"] - df["vol_30m"]) / safe_30m

    # Technicals
    df["rsi_14"]        = wilder_rsi(close.to_numpy(), 14)
    df["bb_position"]   = bollinger_position(close)
    df["macd_signal"]   = macd_signal(close)
    df["autocorr_lag1"] = autocorr_lag1(ret_1m)

    # Time encoding (UTC)
    ts = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["hour_of_day"] = ts.dt.hour.astype(float)
    df["day_of_week"] = ts.dt.dayofweek.astype(float)  # Mon=0..Sun=6

    return df
