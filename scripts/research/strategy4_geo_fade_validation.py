"""Strategy 4 / Phase 0 — Geopolitical Retail-Fade validation.

Thesis: markets like "Iran closes Strait of Hormuz by date X" or
"Russia-Ukraine ceasefire by date Y" spike on news headlines and fade
as the news ages. Retail panic-buys YES; the actual base rate of these
events on short timelines (< 30 days) is very low. Bet NO at panic
peaks, hold to resolution.

What this script does: pull recently-resolved geopolitical markets
from Polymarket, fetch their full price history, and compute the
realized P&L of a "fade the spike" strategy.

Pass criteria:
  - >= 3 historical markets where a 20%+ spike occurred AND the
    market eventually resolved opposite the spike (proving fade-able).
  - Average ROI on hypothetical fade trades > 5%.

Usage:
  python scripts/research/strategy4_geo_fade_validation.py
"""
from __future__ import annotations

import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

EVENTS_URL  = "https://gamma-api.polymarket.com/events"
PRICE_URL   = "https://clob.polymarket.com/prices-history"

GEO_TAGS = ["geopolitics", "middle-east", "russia-ukraine", "israel-gaza",
            "iran", "north-korea", "world", "war", "politics-world"]

# Polymarket question keywords that suggest geopolitical / event-driven
GEO_KEYWORDS = [
    "ceasefire", "regime", "invade", "invasion", "war",
    "nuclear", "missile", "airspace", "strait", "treaty",
    "sanction", "embargo", "coup", "ukraine", "russia",
    "iran", "israel", "gaza", "hezbollah", "hamas",
    "north korea", "taiwan", "china taiwan",
]

PAGE_SIZE = 200
SPIKE_THRESHOLD = 0.20      # 20% one-day move = "spike"
HOLD_DAYS_MAX   = 60        # only consider markets that resolve within 60d


@dataclass
class GeoMarket:
    question:     str
    slug:         str
    end_date:     datetime
    yes_won:      bool
    token_id:     str | None
    volume:       float


@dataclass
class FadeOutcome:
    market:           GeoMarket
    spike_date:       datetime
    spike_yes_price:  float    # peak price during the spike
    pre_spike_price:  float    # price before the spike
    final_resolution: bool     # did YES win at resolution? (fade succeeds when False)
    days_held:        int
    realised_pnl_pct: float    # if we bought NO at spike peak, what was the return?


# ── Fetching ────────────────────────────────────────────────────────────────

def fetch_closed_geo_events() -> list[dict]:
    """Pull recently-closed events that look geopolitical."""
    out: list[dict] = []
    seen_slugs = set()
    for tag in GEO_TAGS:
        for offset in range(0, 600, PAGE_SIZE):
            params = {
                "closed":    "true",
                "limit":     str(PAGE_SIZE),
                "offset":    str(offset),
                "tag_slug":  tag,
                "order":     "endDate",
                "ascending": "false",
            }
            try:
                r = requests.get(EVENTS_URL, params=params, timeout=20)
                r.raise_for_status()
                events = r.json()
            except Exception as e:
                print(f"  fetch failed for tag {tag}: {e}")
                break
            if not events:
                break
            for e in events:
                slug = e.get("slug", "")
                if slug and slug not in seen_slugs:
                    seen_slugs.add(slug)
                    out.append(e)
            time.sleep(0.4)
    print(f"fetched {len(out)} closed geopolitical events across {len(GEO_TAGS)} tags")
    return out


def event_to_markets(event: dict) -> list[GeoMarket]:
    out: list[GeoMarket] = []
    for m in event.get("markets", []):
        question = m.get("question") or event.get("title", "")
        slug = m.get("slug") or event.get("slug", "")

        end_date_raw = m.get("endDate") or event.get("endDate")
        try:
            end_date = datetime.fromisoformat(end_date_raw.replace("Z", "+00:00"))
        except (TypeError, AttributeError, ValueError):
            continue

        outcome_prices = m.get("outcomePrices")
        if not outcome_prices:
            continue
        try:
            prices = json.loads(outcome_prices) if isinstance(outcome_prices, str) else outcome_prices
            yes_p = float(prices[0])
            no_p  = float(prices[1])
        except Exception:
            continue
        if yes_p == 0 and no_p == 0:
            continue
        yes_won = yes_p > no_p

        # Token id for price-history lookup
        token_ids_raw = m.get("clobTokenIds")
        token_id = None
        try:
            ids = json.loads(token_ids_raw) if isinstance(token_ids_raw, str) else token_ids_raw
            if ids and len(ids) >= 1:
                token_id = ids[0]   # YES token
        except Exception:
            pass

        try:
            vol = float(m.get("volume", 0) or 0)
        except (ValueError, TypeError):
            vol = 0.0

        out.append(GeoMarket(
            question=  question,
            slug=      slug,
            end_date=  end_date,
            yes_won=   yes_won,
            token_id=  token_id,
            volume=    vol,
        ))
    return out


def is_geopolitical(question: str) -> bool:
    q = question.lower()
    return any(kw in q for kw in GEO_KEYWORDS)


# ── Price history ──────────────────────────────────────────────────────────

def fetch_price_history(token_id: str) -> list[tuple[int, float]]:
    """Daily YES-price history. Returns list of (timestamp_unix_seconds, price).

    Polymarket's /prices-history endpoint requires BOTH `interval=max`
    AND `fidelity=1440` (minutes per bucket — 1440 = daily) to actually
    return data. Other combinations silently return empty arrays.
    """
    if not token_id:
        return []
    params = {"market": token_id, "interval": "max", "fidelity": "1440"}
    try:
        r = requests.get(PRICE_URL, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return []
    history = data.get("history", [])
    out = []
    for pt in history:
        try:
            ts = int(pt["t"])
            p  = float(pt["p"])
            out.append((ts, p))
        except (KeyError, ValueError, TypeError):
            continue
    return out


# ── Spike detection ────────────────────────────────────────────────────────

def detect_largest_spike(history: list[tuple[int, float]]) -> tuple[int, float, float] | None:
    """Find the largest single-day upward move in YES price.
    Returns (spike_timestamp, pre_spike_price, spike_peak_price) or None."""
    if len(history) < 2:
        return None
    best = None
    for i in range(1, len(history)):
        prev_ts, prev_p = history[i-1]
        cur_ts,  cur_p  = history[i]
        delta = cur_p - prev_p
        if delta < SPIKE_THRESHOLD:
            continue
        if best is None or delta > best[3]:
            best = (cur_ts, prev_p, cur_p, delta)
    if not best:
        return None
    return best[0], best[1], best[2]


def evaluate_fade(market: GeoMarket, history: list[tuple[int, float]]) -> FadeOutcome | None:
    spike = detect_largest_spike(history)
    if not spike:
        return None
    spike_ts, pre_p, peak_p = spike
    spike_date = datetime.fromtimestamp(spike_ts, tz=timezone.utc)

    days_held = (market.end_date - spike_date).days
    if days_held > HOLD_DAYS_MAX or days_held < 1:
        return None

    # If we bought NO at the peak: cost = (1 - peak_p), payout = $1 if YES loses.
    no_entry_cost = 1.0 - peak_p
    if no_entry_cost <= 0:
        return None
    if market.yes_won:
        # Fade lost: NO paid 0
        pnl_pct = -1.0
    else:
        # Fade won: NO paid $1
        pnl_pct = (1.0 - no_entry_cost) / no_entry_cost

    return FadeOutcome(
        market=           market,
        spike_date=       spike_date,
        spike_yes_price=  peak_p,
        pre_spike_price=  pre_p,
        final_resolution= market.yes_won,
        days_held=        days_held,
        realised_pnl_pct= pnl_pct,
    )


# ── Main ────────────────────────────────────────────────────────────────────

def main() -> int:
    out_dir = Path("data/research")
    out_dir.mkdir(parents=True, exist_ok=True)

    events = fetch_closed_geo_events()
    if not events:
        print("no events fetched — likely no GEO_TAGS hit")
        return 2

    markets: list[GeoMarket] = []
    for e in events:
        for m in event_to_markets(e):
            if not is_geopolitical(m.question):
                continue
            if m.volume < 1000:   # need real liquidity to fade
                continue
            markets.append(m)
    print(f"matched {len(markets)} geopolitical markets with vol >= $1K")

    # Sample to keep API calls manageable
    if len(markets) > 80:
        import random
        random.seed(42)
        markets = random.sample(markets, 80)

    outcomes: list[FadeOutcome] = []
    for i, m in enumerate(markets):
        if not m.token_id:
            continue
        history = fetch_price_history(m.token_id)
        time.sleep(0.3)
        out = evaluate_fade(m, history)
        if out is not None:
            outcomes.append(out)
        if (i + 1) % 10 == 0:
            print(f"  processed {i+1}/{len(markets)}, found {len(outcomes)} fade-able spikes")

    if not outcomes:
        print()
        print("=" * 60)
        print("NO QUALIFYING SPIKES — strategy unvalidated")
        print("=" * 60)
        return 3

    # Save and report
    csv = out_dir / "geo_fade_outcomes.csv"
    with csv.open("w", encoding="utf-8") as f:
        f.write("question,spike_date,pre_spike,spike_peak,resolved_yes,days_held,fade_pnl_pct\n")
        for o in outcomes:
            f.write(
                f"{o.market.question.replace(',', ' ')[:120]},"
                f"{o.spike_date.date()},{o.pre_spike_price:.3f},"
                f"{o.spike_yes_price:.3f},{o.final_resolution},"
                f"{o.days_held},{o.realised_pnl_pct:.3f}\n"
            )

    n_total = len(outcomes)
    n_wins  = sum(1 for o in outcomes if not o.final_resolution)
    avg_pnl = sum(o.realised_pnl_pct for o in outcomes) / n_total
    median_held = sorted(o.days_held for o in outcomes)[n_total // 2]

    print()
    print("=" * 60)
    print("STRATEGY 4 / PHASE 0 -- GEOPOLITICAL FADE VALIDATION")
    print("=" * 60)
    print(f"Spikes evaluated:        {n_total}")
    print(f"Fade wins (NO resolved): {n_wins} ({n_wins/n_total*100:.1f}%)")
    print(f"Average fade ROI:        {avg_pnl*100:.1f}%")
    print(f"Median holding period:   {median_held} days")
    print()

    # Sample results
    print("Sample fade outcomes:")
    print(f"  {'Question':<50}  {'spike':>5}  {'won?':>4}  {'pnl%':>7}")
    for o in sorted(outcomes, key=lambda x: -x.realised_pnl_pct)[:5]:
        won = "NO" if not o.final_resolution else "YES"
        print(f"  {o.market.question[:50]:<50}  "
              f"{o.spike_yes_price:>5.2f}  {won:>4}  {o.realised_pnl_pct*100:>+6.1f}%")
    print()

    if n_total >= 3 and avg_pnl >= 0.05:
        print("[GREEN] Strategy 4 GREEN-LIT: 3+ qualifying spikes, avg ROI > 5%.")
        return 0
    elif avg_pnl > 0:
        print("[YELLOW] Strategy 4 marginal: positive ROI but below 5% threshold.")
        return 1
    else:
        print("[RED] Strategy 4 KILL: average fade is unprofitable.")
        return 3


if __name__ == "__main__":
    sys.exit(main())
