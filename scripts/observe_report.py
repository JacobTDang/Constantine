"""Observe-mode report — read JSONL logs, compute signal validation stats.

Usage:
    python scripts/observe_report.py [--db data/db]

Reads:
    data/db/signals.jsonl      — every fired signal evaluation
    data/db/settlements.jsonl  — each market's chainlink settlement

Joins signals → settlements by market_id and computes:
    - Win rate per signal type
    - Realised P&L per signal (hypothetical, observe-only)
    - Edge accuracy: how predicted edge compared to realised
    - Per-day breakdown
    - Daily loss limit trips (would-have-tripped count)
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def load_jsonl(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"warning: skipping malformed line in {path}: {e}", file=sys.stderr)
    return pd.DataFrame(rows)


def compute_realized_pnl(signal: pd.Series, settlement: pd.Series) -> float:
    """Hypothetical P&L if we'd executed the signal."""
    bet = float(signal["would_have_bet_dollars"])
    if signal["signal_type"] == "intramarket":
        # Risk-free arb: profit = bet * net_profit (edge field stores net)
        return bet * float(signal["edge"])

    # Oracle arb: paid yes_ask or no_ask, won iff direction matches outcome
    direction   = signal["direction"]
    outcome_up  = bool(settlement["outcome_up"])
    if direction == "up":
        won = outcome_up
        price = float(signal["yes_ask"])
    elif direction == "down":
        won = not outcome_up
        price = float(signal["no_ask"])
    else:
        return 0.0

    if won:
        # Bought `bet/price` shares, each pays out $1 on win → profit = shares × (1 - price)
        if price <= 0:
            return -bet
        shares = bet / price
        return shares * (1.0 - price)
    return -bet


def main() -> int:
    ap = argparse.ArgumentParser(description="observe-mode signal validation report")
    ap.add_argument("--db", type=Path, default=Path("data/db"),
                    help="dir containing signals.jsonl + settlements.jsonl")
    args = ap.parse_args()

    sigs_path = args.db / "signals.jsonl"
    setl_path = args.db / "settlements.jsonl"
    if not sigs_path.exists():
        print(f"ERROR: {sigs_path} not found — has the bot run yet?", file=sys.stderr)
        return 1

    sigs = load_jsonl(sigs_path)
    setl = load_jsonl(setl_path)

    print(f"loaded {len(sigs):>4} signals from {sigs_path}")
    print(f"loaded {len(setl):>4} settlements from {setl_path}")
    print()

    if len(sigs) == 0:
        print("no signals fired yet")
        return 0

    # Period covered
    sig_start_ms = int(sigs["fired_at_ms"].min())
    sig_end_ms   = int(sigs["fired_at_ms"].max())
    span_hours = (sig_end_ms - sig_start_ms) / 3_600_000
    print(f"period: {datetime.fromtimestamp(sig_start_ms/1000, tz=timezone.utc):%Y-%m-%d %H:%M} "
          f"-> {datetime.fromtimestamp(sig_end_ms/1000, tz=timezone.utc):%Y-%m-%d %H:%M}  "
          f"({span_hours:.1f}h)")
    print()

    # Per signal type counts
    print("Signal counts:")
    for stype, count in sigs["signal_type"].value_counts().items():
        rate_per_hr = count / max(span_hours, 1.0)
        print(f"  {stype:14s}  count={count:>4}   ({rate_per_hr:.2f}/hr)")
    print()

    # Join with settlements
    if len(setl) > 0:
        merged = sigs.merge(setl, on="market_id", how="left", suffixes=("_sig", "_set"))
        settled = merged[merged["outcome_up"].notna()].copy()
        pending = merged[merged["outcome_up"].isna()]
        print(f"settled signals: {len(settled)}    pending: {len(pending)}")
        print()

        if len(settled) > 0:
            # Compute realised P&L per row
            settled["realized_pnl"] = settled.apply(
                lambda r: compute_realized_pnl(r, r), axis=1
            )

            print("Realised P&L (hypothetical, observe-only):")
            for stype, grp in settled.groupby("signal_type"):
                wins = ((grp["signal_type"] == "intramarket") |
                        (grp["realized_pnl"] > 0)).sum()
                total = len(grp)
                win_rate = wins / total if total else 0.0
                total_pnl = grp["realized_pnl"].sum()
                avg_pnl = grp["realized_pnl"].mean()
                avg_bet = grp["would_have_bet_dollars"].mean()
                print(f"  {stype:14s}  n={total:>4}  win_rate={win_rate*100:5.1f}%  "
                      f"total_pnl=${total_pnl:+9.2f}  avg/trade=${avg_pnl:+6.2f}  "
                      f"avg_bet=${avg_bet:5.2f}")
            print()

            # Edge accuracy: predicted edge vs realised win rate per direction
            oracle = settled[settled["signal_type"] == "oracle"].copy()
            if len(oracle) > 0:
                oracle["correct"] = (
                    ((oracle["direction"] == "up")   & oracle["outcome_up"]) |
                    ((oracle["direction"] == "down") & ~oracle["outcome_up"])
                )
                actual_win_rate = oracle["correct"].mean()
                avg_predicted_p = oracle.apply(
                    lambda r: r["fair_value"] if r["direction"] == "up" else (1.0 - r["fair_value"]),
                    axis=1,
                ).mean()
                print(f"Oracle arb calibration:")
                print(f"  predicted avg P(win): {avg_predicted_p:.3f}")
                print(f"  actual    win rate:   {actual_win_rate:.3f}")
                gap = abs(avg_predicted_p - actual_win_rate)
                print(f"  calibration gap:      {gap:.3f}    ({'GOOD' if gap < 0.05 else 'POOR' if gap > 0.10 else 'OK'})")
            print()

        # Daily breakdown
        sigs_with_day = sigs.copy()
        sigs_with_day["day"] = pd.to_datetime(sigs_with_day["fired_at_ms"], unit="ms", utc=True).dt.date
        daily = sigs_with_day.groupby("day").size()
        print("Signals per day:")
        for day, count in daily.items():
            print(f"  {day}  {count:>4}")
        print()

    # Sanity-gate trip counts (signals that fired vs would have fired)
    print("Quality flags:")
    too_thin = (sigs["yes_liquidity_usd"] < 100) | (sigs["no_liquidity_usd"] < 100)
    too_wide = sigs["spread"] > 0.10
    fresh    = sigs["window_age_secs"] < 15
    print(f"  fired despite thin book (would have skipped): {too_thin.sum()}")
    print(f"  fired despite wide spread (would have skipped): {too_wide.sum()}")
    print(f"  fired despite fresh window (would have skipped): {fresh.sum()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
