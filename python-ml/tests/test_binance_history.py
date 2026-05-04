"""H1 — tests for the Binance.US historical downloader."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from data.binance_history import (
    DownloadMetadata,
    KLINE_COLS,
    _frame_from_raw_rows,
    _gap_report,
    download_klines,
    save_to_parquet,
)


def _raw_kline(open_time_ms: int, close: float = 80_000.0, volume: float = 1.0) -> list:
    """Build one kline tuple matching Binance's response shape."""
    close_time_ms = open_time_ms + 60_000 - 1  # 1 minute - 1 ms
    return [
        open_time_ms,                # openTime
        f"{close - 5:.2f}",         # open
        f"{close + 10:.2f}",        # high
        f"{close - 10:.2f}",        # low
        f"{close:.2f}",             # close
        f"{volume:.4f}",            # volume
        close_time_ms,               # closeTime
        f"{close * volume:.2f}",    # quoteVolume
        100,                         # trades
        f"{volume * 0.5:.4f}",      # takerBuyBase
        f"{close * volume * 0.5:.2f}",
        "0",                         # ignore
    ]


# ── _frame_from_raw_rows ──────────────────────────────────────────────────────

def test_empty_rows_returns_empty_frame():
    df = _frame_from_raw_rows([])
    assert df.empty
    assert list(df.columns) == KLINE_COLS


def test_typed_columns():
    rows = [_raw_kline(0), _raw_kline(60_000)]
    df = _frame_from_raw_rows(rows)
    assert df["close"].dtype == float
    assert df["volume"].dtype == float
    # Drops 'ignore' column
    assert "ignore" not in df.columns
    assert df["open_time"].dtype.name == "int64"


def test_deduplicates_on_open_time():
    rows = [_raw_kline(0, close=1.0), _raw_kline(0, close=2.0), _raw_kline(60_000, close=3.0)]
    df = _frame_from_raw_rows(rows)
    assert len(df) == 2
    # Keep-first semantics
    assert df.iloc[0]["close"] == 1.0


def test_sorts_by_open_time():
    rows = [_raw_kline(120_000), _raw_kline(0), _raw_kline(60_000)]
    df = _frame_from_raw_rows(rows)
    assert list(df["open_time"]) == [0, 60_000, 120_000]


# ── _gap_report ───────────────────────────────────────────────────────────────

def test_no_gaps_in_dense_series():
    rows = [_raw_kline(i * 60_000) for i in range(100)]
    df = _frame_from_raw_rows(rows)
    gap_count, largest = _gap_report(df)
    assert gap_count == 0
    assert largest == 1  # consecutive 1-min bars


def test_gap_detected_when_minute_missing():
    rows = [_raw_kline(0), _raw_kline(60_000), _raw_kline(180_000)]  # skip 120_000
    df = _frame_from_raw_rows(rows)
    gap_count, largest = _gap_report(df)
    assert gap_count == 1
    assert largest >= 2


def test_gap_report_handles_short_series():
    df = pd.DataFrame(columns=KLINE_COLS)
    assert _gap_report(df) == (0, 0)
    df_one = _frame_from_raw_rows([_raw_kline(0)])
    assert _gap_report(df_one) == (0, 0)


# ── download_klines (mocked HTTP) ─────────────────────────────────────────────

class _MockResp:
    def __init__(self, json_data, status_code: int = 200):
        self._json = json_data
        self.status_code = status_code
    def json(self):           return self._json
    def raise_for_status(self):
        if self.status_code >= 400 and self.status_code != 429:
            raise RuntimeError(f"http {self.status_code}")


def test_pagination_advances_by_close_time_plus_one():
    """After receiving 100 bars, should request startTime = last_close + 1."""
    page1 = [_raw_kline(i * 60_000) for i in range(100)]
    page2: list = []  # signal end
    calls = []

    def fake_get(url, params=None, timeout=None):
        calls.append(params["startTime"])
        if len(calls) == 1:
            return _MockResp(page1)
        return _MockResp(page2)

    with patch("data.binance_history.requests.get", side_effect=fake_get), \
         patch("data.binance_history.time.sleep"), \
         patch("data.binance_history.time.time", return_value=10_000.0):
        df, meta = download_klines("BTCUSDT", "1m", days=1, sleep_s=0)

    assert len(df) == 100
    # Second call's startTime = page1[-1][6] + 1
    assert calls[1] == int(page1[-1][6]) + 1


def test_handles_rate_limit_429():
    """A 429 response should retry rather than raising."""
    success_page = [_raw_kline(0)]

    call_count = {"n": 0}
    def fake_get(url, params=None, timeout=None):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return _MockResp({}, status_code=429)
        if call_count["n"] == 2:
            return _MockResp(success_page)
        return _MockResp([])  # signal end

    with patch("data.binance_history.requests.get", side_effect=fake_get), \
         patch("data.binance_history.time.sleep"), \
         patch("data.binance_history.time.time", return_value=10_000.0):
        df, meta = download_klines("BTCUSDT", "1m", days=1, sleep_s=0)

    assert call_count["n"] >= 3
    assert len(df) == 1


def test_metadata_fields_populated():
    rows = [_raw_kline(i * 60_000) for i in range(100)]
    with patch("data.binance_history.requests.get", return_value=_MockResp(rows)), \
         patch("data.binance_history.time.sleep"), \
         patch("data.binance_history.time.time", return_value=10_000.0):
        # Force end after first page so we don't loop forever
        with patch("data.binance_history.requests.get",
                   side_effect=[_MockResp(rows), _MockResp([])]):
            df, meta = download_klines("BTCUSDT", "1m", days=1, sleep_s=0)
    assert isinstance(meta, DownloadMetadata)
    assert meta.symbol == "BTCUSDT"
    assert meta.interval == "1m"
    assert meta.rows == 100
    assert meta.expected_rows == 1440
    assert meta.gap_count == 0


# ── save_to_parquet ───────────────────────────────────────────────────────────

def test_save_to_parquet_writes_files(tmp_path: Path):
    rows = [_raw_kline(i * 60_000) for i in range(10)]
    df = _frame_from_raw_rows(rows)
    meta = DownloadMetadata(
        symbol="BTCUSDT", interval="1m",
        start_ms=0, end_ms=600_000, rows=10, expected_rows=10,
        gap_count=0, largest_gap_minutes=1, downloaded_at_ms=1_700_000_000_000,
    )
    parquet_path = save_to_parquet(df, meta, tmp_path)
    assert parquet_path.exists()
    # Sidecar metadata.json
    meta_path = tmp_path / "btcusdt_1m_metadata.json"
    assert meta_path.exists()
    # Round-trip
    df2 = pd.read_parquet(parquet_path)
    assert len(df2) == len(df)
    assert (df["close"] == df2["close"]).all()


# ── Live test against actual binance.us ───────────────────────────────────────

@pytest.mark.skip(reason="hits live binance.us — run manually with `pytest -m live`")
def test_live_download_smoke():
    """Hit binance.us for a small slice; verify it returns sane data."""
    df, meta = download_klines("BTCUSDT", "1m", days=1, sleep_s=0.05)
    assert len(df) > 1000
    assert df["close"].between(1_000, 1_000_000).all()
    assert meta.rows == len(df)
