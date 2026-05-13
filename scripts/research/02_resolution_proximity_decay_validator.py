"""Strategy 02 / Phase 0 -- Resolution-proximity decay validator.

Hypothesis (from research/strategies/02_resolution_proximity_decay.md):
  Polymarket's published accuracy at 4 hours pre-resolution is 95%.
  Markets near close routinely trade at 85-98 cents when their
  outcome is functionally determined; capturing the last 4-5 cents
  is a structural edge gated by truth-source confirmation.

Phase 0 plan from spec:
  "Manually trace 20 recent low-vol markets that resolved in the
   last 30 days. For each: was the outcome knowable from the
   specified source 4h before resolution? At what price was the
   winning side trading at that 4h-prior moment? Pass if average
   winning-side entry would have been < 0.97."

This validator automates that backtest using Polymarket Gamma + the
public prices-history endpoint. For each market that has closed in
the last 60 days, we:
  1. Identify which side (YES/NO) won from outcomePrices
  2. Fetch the price of the winning side at T - 4h
  3. If that price was < 0.97 AND > 0.50, count it as a "decay
     candidate" (we'd have bought into the winning side)
  4. Compute realized P&L: payoff (1.0) - entry price - 2% taker fee

The KEY question is whether the average entry-and-hold trade
produces net positive P&L AFTER fees. Markets where the eventual
winner was already trading at >= 0.98 are NOT exploitable (no
spread left).

Pass criteria (per research/cycles/IMPL_LOOP.md quality bar):
  GREEN:  n_candidates >= 30 AND hit_rate >= 92%
          AND avg_net_pct (after fees) >= 1.5%
  YELLOW: positive net but below thresholds
  RED:    negative net or fewer than 10 candidates

Usage:
  python scripts/research/02_resolution_proximity_decay_validator.py
"""
from __future__ import annotations

import json
import statistics
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
CLOB_HISTORY_URL = "https://clob.polymarket.com/prices-history"

LOOKBACK_DAYS         = 14       # CLOB prices-history caps at ~14d windows
PAGE_SIZE             = 100
MAX_GAMMA_PAGES       = 6
MAX_MARKETS           = 200
PRE_HOURS             = 4
MIN_ENTRY_PRICE       = 0.50            # below this would be a long-shot
MAX_ENTRY_PRICE       = 0.97            # above this leaves no spread
HARD_RUNTIME_SECS     = 270
SLEEP_BETWEEN_REQS    = 0.35

FEE_TAKER = 0.02


@dataclass
class DecayCandidate:
    slug:           str
    end_ts:         datetime
    yes_won:        bool
    pre_close_yes:  float           # YES side price at T-4h
    winning_entry:  float           # entry price on the side that won
    gross_pnl:      float           # payoff - entry
    net_pnl:        float           # gross - 2 * fees on entry
    net_pct:        float           # net_pnl / entry


@dataclass
class ValidationResult:
    n_markets_scanned:     int = 0
    n_resolved:            int = 0
    n_candidates:          int = 0
    n_wins:                int = 0
    n_losses:              int = 0
    n_skipped_above_max:   int = 0       # already at >=0.97 4h pre-close
    n_skipped_below_min:   int = 0       # 4h price was on the OTHER side
    n_skipped_no_history:  int = 0
    sample:                list[DecayCandidate] = field(default_factory=list)
    net_pcts:              list[float] = field(default_factory=list)

    @property
    def hit_rate(self) -> float:
        return self.n_wins / self.n_candidates if self.n_candidates else 0.0

    @property
    def avg_net_pct(self) -> float:
        return statistics.mean(self.net_pcts) if self.net_pcts else 0.0


def fetch_recent_closed_markets() -> list[dict]:
    out: list[dict] = []
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    for page in range(MAX_GAMMA_PAGES):
        params = {
            "closed":    "true",
            "limit":     str(PAGE_SIZE),
            "offset":    str(page * PAGE_SIZE),
            "order":     "endDate",
            "ascending": "false",
        }
        try:
            r = requests.get(GAMMA_EVENTS_URL, params=params, timeout=15)
            r.raise_for_status()
            events = r.json()
        except (requests.RequestException, ValueError) as e:
            print(f"  gamma error (page={page}): {e}", file=sys.stderr)
            break
        if not events:
            break
        for e in events:
            end_raw = e.get("endDate", "")
            try:
                end_dt = datetime.fromisoformat(end_raw.replace("Z", "+00:00"))
            except (TypeError, AttributeError, ValueError):
                continue
            if end_dt < cutoff:
                continue
            for m in e.get("markets", []):
                m.setdefault("_event_end", end_raw)
                out.append(m)
        last_raw = events[-1].get("endDate", "")
        try:
            if datetime.fromisoformat(last_raw.replace("Z", "+00:00")) < cutoff:
                break
        except (TypeError, AttributeError, ValueError):
            pass
        time.sleep(SLEEP_BETWEEN_REQS)
        if len(out) >= MAX_MARKETS * 2:
            break

    seen: set[str] = set()
    out2: list[dict] = []
    for m in out:
        mid = str(m.get("id") or m.get("conditionId") or m.get("slug"))
        if mid in seen:
            continue
        seen.add(mid)
        out2.append(m)
    return out2[:MAX_MARKETS]


def first_token_id(m: dict) -> str | None:
    raw = m.get("clobTokenIds") or "[]"
    try:
        ids = json.loads(raw) if isinstance(raw, str) else raw
    except json.JSONDecodeError:
        return None
    if not isinstance(ids, list) or not ids:
        return None
    return str(ids[0]) if ids[0] not in (0, "0") else None


def fetch_price_window(token_id: str, around: datetime) -> list[tuple[datetime, float]]:
    start_ts = int((around - timedelta(hours=PRE_HOURS + 2)).timestamp())
    end_ts = int((around + timedelta(hours=1)).timestamp())
    # interval + startTs/endTs is mutually exclusive on this endpoint
    params = {
        "market":   token_id,
        "startTs":  str(start_ts),
        "endTs":    str(end_ts),
        "fidelity": "60",
    }
    try:
        r = requests.get(CLOB_HISTORY_URL, params=params, timeout=15)
        r.raise_for_status()
        payload = r.json()
    except (requests.RequestException, ValueError):
        return []
    history = payload.get("history") or payload.get("h") or []
    out: list[tuple[datetime, float]] = []
    for pt in history:
        ts = pt.get("t") or pt.get("timestamp")
        px = pt.get("p") or pt.get("price")
        try:
            dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
            out.append((dt, float(px)))
        except (TypeError, ValueError, OSError):
            continue
    out.sort(key=lambda x: x[0])
    return out


def price_at_or_before(history: list[tuple[datetime, float]],
                       target: datetime) -> float | None:
    last = None
    for ts, px in history:
        if ts <= target:
            last = px
        else:
            break
    return last


def main() -> int:
    started = time.time()
    out_dir = Path("data/research")
    out_dir.mkdir(parents=True, exist_ok=True)

    markets = fetch_recent_closed_markets()
    res = ValidationResult()
    res.n_markets_scanned = len(markets)
    print(f"scanning {len(markets)} recently-closed markets")

    for i, m in enumerate(markets):
        if time.time() - started > HARD_RUNTIME_SECS:
            print(f"  runtime cap at {i}/{len(markets)}")
            break

        outcome_prices_raw = m.get("outcomePrices")
        try:
            prices = (json.loads(outcome_prices_raw)
                      if isinstance(outcome_prices_raw, str) else outcome_prices_raw)
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(prices, list) or len(prices) != 2:
            continue
        try:
            yes_p = float(prices[0])
            no_p = float(prices[1])
        except (ValueError, TypeError):
            continue
        if yes_p == no_p:
            continue
        yes_won = yes_p > no_p
        res.n_resolved += 1

        end_raw = m.get("_event_end") or m.get("endDate", "")
        try:
            end_dt = datetime.fromisoformat(end_raw.replace("Z", "+00:00"))
        except (TypeError, AttributeError, ValueError):
            continue

        tid = first_token_id(m)
        if not tid:
            continue

        history = fetch_price_window(tid, end_dt)
        if not history:
            res.n_skipped_no_history += 1
            time.sleep(SLEEP_BETWEEN_REQS / 4)
            continue

        pre_close = price_at_or_before(history, end_dt - timedelta(hours=PRE_HOURS))
        if pre_close is None:
            res.n_skipped_no_history += 1
            continue

        # winning_entry = price of the side that won
        winning_entry = pre_close if yes_won else (1.0 - pre_close)
        if winning_entry > MAX_ENTRY_PRICE:
            res.n_skipped_above_max += 1
            continue
        if winning_entry < MIN_ENTRY_PRICE:
            res.n_skipped_below_min += 1
            continue

        gross = 1.0 - winning_entry
        # 2% taker on entry only — exit is automatic via resolution
        net = gross - FEE_TAKER * winning_entry
        net_pct = net / winning_entry * 100.0
        res.n_candidates += 1
        if net > 0:
            res.n_wins += 1
        else:
            res.n_losses += 1
        res.net_pcts.append(net_pct)

        if len(res.sample) < 25:
            res.sample.append(DecayCandidate(
                slug=          m.get("slug", ""),
                end_ts=        end_dt,
                yes_won=       yes_won,
                pre_close_yes= pre_close,
                winning_entry= winning_entry,
                gross_pnl=     gross,
                net_pnl=       net,
                net_pct=       net_pct,
            ))

        if (i + 1) % 20 == 0:
            print(f"  scanned {i+1}/{len(markets)} (candidates={res.n_candidates})",
                  flush=True)
        time.sleep(SLEEP_BETWEEN_REQS)

    # Artefacts
    csv_path = out_dir / "02_resolution_proximity_decay_phase0.csv"
    with csv_path.open("w", encoding="utf-8") as f:
        f.write("slug,end_ts,yes_won,pre_close_yes,winning_entry,"
                "gross_pnl,net_pnl,net_pct\n")
        for c in res.sample:
            f.write(f"{c.slug},{c.end_ts.isoformat()},{int(c.yes_won)},"
                    f"{c.pre_close_yes:.4f},{c.winning_entry:.4f},"
                    f"{c.gross_pnl:+.4f},{c.net_pnl:+.4f},{c.net_pct:+.2f}\n")

    summary = {
        "n_markets_scanned":    res.n_markets_scanned,
        "n_resolved":           res.n_resolved,
        "n_candidates":         res.n_candidates,
        "n_wins":               res.n_wins,
        "n_losses":             res.n_losses,
        "n_skipped_above_max":  res.n_skipped_above_max,
        "n_skipped_below_min":  res.n_skipped_below_min,
        "hit_rate":             round(res.hit_rate, 4),
        "avg_net_pct":          round(res.avg_net_pct, 4),
        "pre_close_hours":      PRE_HOURS,
        "min_entry":            MIN_ENTRY_PRICE,
        "max_entry":            MAX_ENTRY_PRICE,
        "fee_taker":            FEE_TAKER,
    }
    json_path = out_dir / "02_resolution_proximity_decay_phase0.json"
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print()
    print("=" * 64)
    print("STRATEGY 02 / PHASE 0 -- RESOLUTION-PROXIMITY DECAY VALIDATION")
    print("=" * 64)
    print(f"Markets scanned:       {res.n_markets_scanned}")
    print(f"Resolved 0/1 cleanly:  {res.n_resolved}")
    print(f"Decay candidates:      {res.n_candidates}")
    print(f"Wins / Losses:         {res.n_wins} / {res.n_losses}")
    print(f"Skipped (already >=0.97 4h pre): {res.n_skipped_above_max}")
    print(f"Skipped (winner was <0.50 4h pre): {res.n_skipped_below_min}")
    print(f"Hit rate:              {res.hit_rate*100:.1f}%")
    print(f"Avg net % per trade:   {res.avg_net_pct:+.2f}% (after {FEE_TAKER*100:.0f}% taker)")
    print()

    if res.n_candidates < 10:
        print("[RED] KILL: fewer than 10 decay candidates in the window.")
        print("           Either price-history coverage too thin or the band")
        print("           filter excluded too many markets.")
        return 3

    avg = res.avg_net_pct
    hit = res.hit_rate
    n = res.n_candidates

    if n >= 30 and hit >= 0.92 and avg >= 1.5:
        print(f"[GREEN] GREEN-LIGHT Strategy 02 to Phase 1:")
        print(f"        hit_rate {hit*100:.1f}% >= 92%, "
              f"avg_net {avg:+.2f}% >= 1.5%, n={n}.")
        return 0
    if avg > 0:
        print(f"[YELLOW] MARGINAL: positive avg {avg:+.2f}% but below promotion bar.")
        print("         Phase 1 should tighten the band or per-source filter.")
        return 1
    print(f"[RED] KILL: avg net {avg:+.2f}% — favorite-longshot bias dominates")
    print("           without a per-source truth filter.")
    return 3


if __name__ == "__main__":
    sys.exit(main())
