"""EDGE-D — Whale-watch sidecar.

Polls Polymarket's public data-api for recent trades by a configured
list of "whale" wallets (high-PnL traders). Writes new trades to
`data/whale_trades.jsonl` (append-only, deduplicated by trade id).
The Rust runner reads this file and emits a follow-trade signal when
a fresh whale entry appears.

Wallet list lives at `data/whale_wallets.json`. Format:
  [
    {"address": "0x...", "nickname": "Theo4",      "min_size_usd": 5000},
    {"address": "0x...", "nickname": "weflyhigh",  "min_size_usd": 3000}
  ]

Get the list from https://polymarket.com/leaderboard/overall/monthly/profit
and copy the wallet addresses. We intentionally don't auto-scrape the
leaderboard -- whale identity is a slow-changing signal you should
manually curate.

Required env: nothing. The Polymarket data-api is public.

Usage:
  python scripts/whale_tracker.py             # one-shot
  python scripts/whale_tracker.py --watch     # poll every 60s
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import requests

# Polymarket exposes user-trade history via the public data-api.
# (NB: response shape may shift; the script is tolerant of missing fields.)
DATA_API_TRADES = "https://data-api.polymarket.com/trades"

WALLETS_PATH = Path("data/whale_wallets.json")
OUTPUT_PATH  = Path("data/whale_trades.jsonl")
SEEN_PATH    = Path("data/.whale_seen.json")  # dedup state

REFRESH_SECS_DEFAULT = 60

log = logging.getLogger("whale_tracker")


@dataclass
class WhaleTrade:
    trade_id:       str
    whale_address:  str
    whale_nickname: str
    market_id:      str       # condition_id
    asset_id:       str       # token id
    side:           str       # "BUY" / "SELL"
    outcome:        str       # text label, e.g. "Yes" / "No" / "Lakers"
    outcome_index:  int       # 0 = first outcome (YES/Up), 1 = second (NO/Down), -1 unknown
    price:          float
    size_shares:    float
    size_usd:       float
    ts_ms:          int


def load_whales() -> list[dict]:
    if not WALLETS_PATH.exists():
        log.warning(
            "%s missing — create it with a JSON array of whale entries; "
            "see scripts/whale_tracker.py docstring for format",
            WALLETS_PATH,
        )
        return []
    try:
        return json.loads(WALLETS_PATH.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        log.warning("read %s failed: %s", WALLETS_PATH, exc)
        return []


def load_seen() -> set[str]:
    if not SEEN_PATH.exists():
        return set()
    try:
        return set(json.loads(SEEN_PATH.read_text()))
    except (OSError, json.JSONDecodeError):
        return set()


def save_seen(seen: set[str]) -> None:
    SEEN_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = SEEN_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(sorted(seen)[-50_000:]))   # cap at 50k IDs
    tmp.replace(SEEN_PATH)


# ── Fetch ───────────────────────────────────────────────────────────────────

def fetch_recent_trades(address: str, limit: int = 50) -> list[dict]:
    params = {"user": address, "limit": str(limit)}
    try:
        r = requests.get(DATA_API_TRADES, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, ValueError) as exc:
        log.warning("fetch %s failed: %s", address, exc)
        return []
    if isinstance(data, dict):
        data = data.get("data") or data.get("trades") or []
    if not isinstance(data, list):
        return []
    return data


def parse_trade(raw: dict, whale: dict) -> WhaleTrade | None:
    trade_id = raw.get("id") or raw.get("transactionHash") or raw.get("tx_hash")
    if not trade_id:
        return None
    market_id = raw.get("conditionId") or raw.get("market_id") or ""
    asset_id  = raw.get("asset") or raw.get("asset_id") or raw.get("tokenId") or ""
    side      = (raw.get("side") or "").upper()
    outcome   = (raw.get("outcome") or "").lower()
    # outcomeIndex is the reliable direction marker (0=YES, 1=NO) and is
    # always populated by data-api/trades; outcome string varies for
    # multi-outcome events (e.g. "Lakers" instead of "Yes").
    raw_idx = raw.get("outcomeIndex")
    try:
        outcome_index = int(raw_idx) if raw_idx is not None else -1
    except (TypeError, ValueError):
        outcome_index = -1

    try:
        price = float(raw.get("price") or 0)
        size_shares = float(raw.get("size") or raw.get("amount") or 0)
    except (TypeError, ValueError):
        return None
    if price <= 0 or size_shares <= 0:
        return None
    size_usd = price * size_shares
    if size_usd < float(whale.get("min_size_usd", 0)):
        return None

    ts = raw.get("timestamp") or raw.get("matchTime") or raw.get("ts")
    try:
        ts_ms = int(ts) * 1000 if ts and int(ts) < 10**12 else int(ts or 0)
    except (TypeError, ValueError):
        ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    return WhaleTrade(
        trade_id       = str(trade_id),
        whale_address  = whale["address"],
        whale_nickname = whale.get("nickname", whale["address"][:10]),
        market_id      = str(market_id),
        asset_id       = str(asset_id),
        side           = side or "BUY",
        outcome        = outcome,
        outcome_index  = outcome_index,
        price          = price,
        size_shares    = size_shares,
        size_usd       = size_usd,
        ts_ms          = ts_ms,
    )


# ── Output ──────────────────────────────────────────────────────────────────

def append_jsonl(rows: list[WhaleTrade]) -> None:
    if not rows:
        return
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps({
                "trade_id":       r.trade_id,
                "whale_address":  r.whale_address,
                "whale_nickname": r.whale_nickname,
                "market_id":      r.market_id,
                "asset_id":       r.asset_id,
                "side":           r.side,
                "outcome":        r.outcome,
                "outcome_index":  r.outcome_index,
                "price":          r.price,
                "size_shares":    r.size_shares,
                "size_usd":       r.size_usd,
                "ts_ms":          r.ts_ms,
            }) + "\n")


# ── Main ────────────────────────────────────────────────────────────────────

def run_once() -> int:
    whales = load_whales()
    if not whales:
        log.info("no whales configured — skipping run")
        return 0

    seen   = load_seen()
    new_rows: list[WhaleTrade] = []
    for whale in whales:
        addr = (whale.get("address") or "").lower()
        if not addr:
            continue
        raws = fetch_recent_trades(addr, limit=50)
        for raw in raws:
            trade = parse_trade(raw, whale)
            if trade is None:
                continue
            if trade.trade_id in seen:
                continue
            seen.add(trade.trade_id)
            new_rows.append(trade)
        time.sleep(0.4)

    append_jsonl(new_rows)
    save_seen(seen)
    log.info("found %d new whale trades across %d whales", len(new_rows), len(whales))
    return 0


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--watch", action="store_true")
    p.add_argument("--cadence", type=int, default=REFRESH_SECS_DEFAULT)
    args = p.parse_args()

    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        level=logging.INFO,
    )

    if not args.watch:
        return run_once()
    while True:
        try:
            run_once()
        except Exception as exc:
            log.exception("run failed: %s", exc)
        time.sleep(args.cadence)


if __name__ == "__main__":
    sys.exit(main())
