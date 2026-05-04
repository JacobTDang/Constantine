"""H1 - Binance.US 1-minute OHLCV historical downloader.

binance.com is geo-blocked for US users; binance.us has the same kline
schema. We pull 90 days of BTCUSDT 1-min bars in 1000-bar pages, dedupe
by open_time, and persist to Parquet for downstream feature engineering.

Saves:
  data/raw/btcusdt_1m_<from>_<to>.parquet      # full dataset
  data/raw/btcusdt_1m_metadata.json            # gap report + provenance
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

log = logging.getLogger(__name__)

BINANCE_US_KLINES = "https://api.binance.us/api/v3/klines"

# Binance returns 12-tuple per kline:
#   [openTime, open, high, low, close, volume,
#    closeTime, quoteVolume, trades,
#    takerBuyBase, takerBuyQuote, ignore]
KLINE_COLS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trades",
    "taker_buy_base", "taker_buy_quote", "ignore",
]


@dataclass
class DownloadMetadata:
    symbol:        str
    interval:      str
    start_ms:      int
    end_ms:        int
    rows:          int
    expected_rows: int
    gap_count:     int
    largest_gap_minutes: int
    downloaded_at_ms:    int
    source:        str = "binance.us"


def download_klines(
    symbol:   str = "BTCUSDT",
    interval: str = "1m",
    days:     int = 90,
    *,
    out_dir:  Optional[Path] = None,
    sleep_s:  float = 0.05,
    timeout_s: float = 15.0,
) -> tuple[pd.DataFrame, DownloadMetadata]:
    """Download N days of klines, dedupe, return DataFrame + metadata."""
    out_dir = Path(out_dir) if out_dir else Path("data/raw")
    out_dir.mkdir(parents=True, exist_ok=True)

    end_ms     = int(time.time() * 1000)
    start_ms   = end_ms - days * 86_400_000
    cur        = start_ms
    all_rows: list = []

    log.info("downloading %s %s for %d days (start_ms=%d end_ms=%d)",
             symbol, interval, days, start_ms, end_ms)

    while cur < end_ms:
        resp = requests.get(
            BINANCE_US_KLINES,
            params={
                "symbol":    symbol,
                "interval":  interval,
                "startTime": cur,
                "limit":     1000,
            },
            timeout=timeout_s,
        )
        # Handle rate-limit responses politely
        if resp.status_code == 429:
            log.warning("rate limited; sleeping 5s")
            time.sleep(5.0)
            continue
        resp.raise_for_status()
        rows = resp.json()
        if not rows:
            log.warning("empty response at cur=%d, stopping", cur)
            break
        all_rows.extend(rows)
        # Advance to one ms after last kline's closeTime
        cur = int(rows[-1][6]) + 1
        if sleep_s > 0:
            time.sleep(sleep_s)

    df = _frame_from_raw_rows(all_rows)
    expected_rows = days * 1440  # 1440 minutes per day
    gap_count, largest_gap_min = _gap_report(df)

    meta = DownloadMetadata(
        symbol=symbol,
        interval=interval,
        start_ms=start_ms,
        end_ms=end_ms,
        rows=len(df),
        expected_rows=expected_rows,
        gap_count=gap_count,
        largest_gap_minutes=largest_gap_min,
        downloaded_at_ms=int(time.time() * 1000),
    )

    log.info("downloaded %d rows (expected ~%d), %d gaps, largest %d min",
             meta.rows, meta.expected_rows, meta.gap_count, meta.largest_gap_minutes)

    return df, meta


def save_to_parquet(df: pd.DataFrame, meta: DownloadMetadata, out_dir: Path) -> Path:
    """Write parquet + sidecar metadata.json. Returns path to parquet."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    fname = f"{meta.symbol.lower()}_{meta.interval}_{meta.start_ms}_{meta.end_ms}.parquet"
    parquet_path = out_dir / fname
    df.to_parquet(parquet_path, engine="pyarrow", compression="snappy")

    meta_path = out_dir / f"{meta.symbol.lower()}_{meta.interval}_metadata.json"
    meta_path.write_text(json.dumps(asdict(meta), indent=2))

    return parquet_path


def _frame_from_raw_rows(rows: list) -> pd.DataFrame:
    """Convert raw API rows to a typed DataFrame, dedupe, sort, drop ignore."""
    if not rows:
        return pd.DataFrame(columns=KLINE_COLS)

    df = pd.DataFrame(rows, columns=KLINE_COLS)
    # Cast numeric columns from strings
    num_cols = ["open", "high", "low", "close", "volume",
                "quote_volume", "taker_buy_base", "taker_buy_quote"]
    for c in num_cols:
        df[c] = df[c].astype(float)
    df["trades"] = df["trades"].astype(int)
    df["open_time"]  = df["open_time"].astype("int64")
    df["close_time"] = df["close_time"].astype("int64")

    # Drop the unused "ignore" field (Binance reserved)
    df = df.drop(columns=["ignore"])
    # Dedupe on open_time, keep first occurrence
    df = df.drop_duplicates(subset=["open_time"], keep="first")
    df = df.sort_values("open_time").reset_index(drop=True)
    return df


def _gap_report(df: pd.DataFrame) -> tuple[int, int]:
    """Count gaps (missing 1-min bars) and report the largest one in minutes."""
    if len(df) < 2:
        return (0, 0)
    deltas_ms = df["open_time"].diff().dropna().astype("int64")
    expected_ms = 60_000  # 1 minute
    gaps_mask = deltas_ms > expected_ms
    gap_count = int(gaps_mask.sum())
    largest_ms = int(deltas_ms.max())
    largest_minutes = largest_ms // 60_000
    return (gap_count, largest_minutes)


# ── CLI: python -m data.binance_history ────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    df, meta = download_klines("BTCUSDT", "1m", 90)
    out = save_to_parquet(df, meta, Path("data/raw"))
    print(f"saved {out} ({len(df)} rows, {meta.gap_count} gaps)")
