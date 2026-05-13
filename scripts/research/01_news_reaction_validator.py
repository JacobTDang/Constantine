"""Strategy 01 / Phase 0 -- News-reaction lag-window validator.

Hypothesis (from research/strategies/01_news_reaction.md):
  When material news drops, Polymarket prices take 30s-several minutes
  to fully incorporate the information. A latency-tuned ingestion
  pipeline can place a fill against stale quotes inside that window.

We do NOT have a free historical-news-timestamp source we trust to
ground-truth "news event happened at T", so this validator INVERTS
the methodology: it looks at Polymarket's own price history for
political / macro / regulatory markets and asks the literal question
the strategy depends on:

  WHEN a market exhibits a jump (>=JUMP_THRESHOLD_PP within a
  short window), does the next REACTION_WINDOW_MIN of price action
  continue in the SAME direction by at least CONTINUATION_PP?

If yes consistently, there IS a tradeable reprice window after big
moves, and an LLM that classifies the news source in real time can
plausibly capture it. If no, the jumps are already fully priced and
the strategy has no edge regardless of news pipeline quality.

This validator uses the Polymarket public CLOB prices-history endpoint,
no auth, no LLM, no news feed. ~30-90 days of free data per market.

Pass criteria (from research/cycles/IMPL_LOOP.md quality bar):
  GREEN:  continuation_rate >= 35% AND avg_continuation_pp >= 1.5
          AND net_monthly_pct (after fees) >= 1%
  YELLOW: continuation_rate 20-35% OR net_monthly_pct positive but < 1%
  RED:    continuation_rate < 20% OR net_monthly_pct negative
          OR fewer than 5 jump events found in the window

Usage:
  python scripts/research/01_news_reaction_validator.py
"""
from __future__ import annotations

import json
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
CLOB_HISTORY_URL = "https://clob.polymarket.com/prices-history"

# Hypothesis parameters
JUMP_THRESHOLD_PP = 2.0          # >=2pp move within JUMP_WINDOW_MIN counts as a jump
JUMP_WINDOW_MIN = 5              # window over which we measure the jump
REACTION_WINDOW_MIN = 30         # window after jump where continuation is measured
CONTINUATION_PP = 1.0            # >=1pp same-direction follow-through = continuation
MIN_PRICE = 0.05                 # ignore markets pinned below 5c (no meaningful drift)
MAX_PRICE = 0.95                 # ignore markets pinned above 95c

# Market filters
LOOKBACK_DAYS = 60               # pull markets resolved in last N days
MAX_EVENTS = 80                  # cap to keep runtime under 5 min
SLEEP_BETWEEN_REQS = 0.4         # be polite to gamma + clob
PAGE_SIZE = 200
MAX_PAGES = 5

# News-y tag slugs Polymarket uses
CATEGORY_TAGS = ("politics", "elections", "trump", "world", "geopolitics")

# Fee model
FEE_TAKER = 0.02                 # 2% taker assumption (conservative)


@dataclass
class JumpEvent:
    market_slug:           str
    token_id:              str
    t_start:               datetime
    t_jump_end:            datetime
    price_before:          float
    price_at_jump_end:     float
    jump_pp:               float                 # signed
    price_at_react_end:    float                 # 30min later
    continuation_pp:       float                 # signed continuation amount


@dataclass
class ValidationResult:
    n_markets_scanned:        int = 0
    n_markets_with_history:   int = 0
    n_jumps:                  int = 0
    n_continuations:          int = 0
    continuations:            list[float] = field(default_factory=list)
    sample_events:            list[JumpEvent] = field(default_factory=list)

    @property
    def continuation_rate(self) -> float:
        return self.n_continuations / self.n_jumps if self.n_jumps else 0.0

    @property
    def avg_continuation_pp(self) -> float:
        if not self.continuations:
            return 0.0
        return sum(self.continuations) / len(self.continuations)

    def projected_monthly_net_pct(self) -> float:
        """Rough projection: if every jump → a $50 trade at the touch,
        held for REACTION_WINDOW_MIN, capturing avg_continuation_pp.
        Trades that don't continue lose CONTINUATION_PP (stop-out).
        Fees applied on entry + exit (taker × 2).
        Returns expected NET monthly return on per-trade capital."""
        if self.n_jumps == 0:
            return 0.0
        win_rate = self.continuation_rate
        win_pp = max(self.avg_continuation_pp, 0.0)
        loss_pp = CONTINUATION_PP            # symmetric assumption
        gross_per_trade_pp = win_rate * win_pp - (1 - win_rate) * loss_pp
        # pp = % move in price; on a 50c market, a 1pp move = 2% return.
        # Approximate: ROI per trade ≈ gross_per_trade_pp / 50 (mid-price)
        roi_per_trade = gross_per_trade_pp / 50.0
        # Subtract 2 × taker fee per round-trip
        roi_per_trade -= 2 * FEE_TAKER
        # Trades per month: scale n_jumps from lookback to 30 days
        trades_per_month = self.n_jumps * (30.0 / LOOKBACK_DAYS)
        return roi_per_trade * trades_per_month * 100.0  # as %


# ── Polymarket fetch ────────────────────────────────────────────────────────

def fetch_recent_political_markets() -> list[dict]:
    """Pull resolved markets in news-relevant categories from the past
    LOOKBACK_DAYS. Returns a flat list of market dicts each containing
    a `clobTokenIds` field we can use to query price history."""
    out: list[dict] = []
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

    for tag in CATEGORY_TAGS:
        for page in range(MAX_PAGES):
            params = {
                "closed":    "true",
                "limit":     str(PAGE_SIZE),
                "offset":    str(page * PAGE_SIZE),
                "tag_slug":  tag,
                "order":     "endDate",
                "ascending": "false",
            }
            try:
                r = requests.get(GAMMA_EVENTS_URL, params=params, timeout=15)
                r.raise_for_status()
                page_events = r.json()
            except (requests.RequestException, ValueError) as e:
                print(f"  gamma error (tag={tag} page={page}): {e}", file=sys.stderr)
                break

            if not page_events:
                break

            for e in page_events:
                end_raw = e.get("endDate", "")
                try:
                    end_dt = datetime.fromisoformat(end_raw.replace("Z", "+00:00"))
                except (TypeError, AttributeError, ValueError):
                    continue
                if end_dt < cutoff:
                    continue
                for m in e.get("markets", []):
                    m.setdefault("_event_title", e.get("title", ""))
                    m.setdefault("_event_end", end_raw)
                    out.append(m)

            # Early break if we've gone past the lookback window
            last_raw = page_events[-1].get("endDate", "")
            try:
                if datetime.fromisoformat(last_raw.replace("Z", "+00:00")) < cutoff:
                    break
            except (TypeError, AttributeError, ValueError):
                pass

            time.sleep(SLEEP_BETWEEN_REQS)

            if len(out) >= MAX_EVENTS * 3:
                break

        if len(out) >= MAX_EVENTS * 3:
            break

    # Dedupe by market id
    seen: set[str] = set()
    deduped: list[dict] = []
    for m in out:
        mid = str(m.get("id") or m.get("conditionId") or m.get("slug"))
        if mid in seen:
            continue
        seen.add(mid)
        deduped.append(m)
    print(f"fetched {len(deduped)} unique candidate markets across {len(CATEGORY_TAGS)} tags")
    return deduped[:MAX_EVENTS]


def fetch_price_history(token_id: str) -> list[tuple[datetime, float]]:
    """Pull minute-granularity price history for a CLOB token. Returns
    list of (timestamp, price) sorted ascending. Empty list on failure."""
    end_ts = int(datetime.now(timezone.utc).timestamp())
    start_ts = end_ts - (LOOKBACK_DAYS * 86400)
    params = {
        "market":  token_id,
        "interval": "1h",   # gamma's prices-history finest cheap interval
        "startTs": str(start_ts),
        "endTs":   str(end_ts),
        "fidelity": "60",   # 60-minute bucket; matches "1h"
    }
    try:
        r = requests.get(CLOB_HISTORY_URL, params=params, timeout=20)
        r.raise_for_status()
        payload = r.json()
    except (requests.RequestException, ValueError):
        return []

    history = payload.get("history") or payload.get("h") or []
    out: list[tuple[datetime, float]] = []
    for pt in history:
        ts = pt.get("t") or pt.get("timestamp")
        px = pt.get("p") or pt.get("price")
        if ts is None or px is None:
            continue
        try:
            dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
            out.append((dt, float(px)))
        except (ValueError, TypeError, OSError):
            continue
    out.sort(key=lambda x: x[0])
    return out


# ── Jump detection ──────────────────────────────────────────────────────────

def detect_jumps(history: list[tuple[datetime, float]],
                 market_slug: str,
                 token_id: str) -> list[JumpEvent]:
    """Scan price history for jumps. With 1h bucket data, the
    JUMP_WINDOW_MIN parameter is approximated by the next adjacent
    bucket (so effectively 1h-over-1h). REACTION_WINDOW_MIN is the
    same: we look at the next 1-2 buckets after the jump."""
    if len(history) < 3:
        return []

    jumps: list[JumpEvent] = []
    for i in range(len(history) - 2):
        t0, p0 = history[i]
        t1, p1 = history[i + 1]
        t2, p2 = history[i + 2]

        # Skip if either side is at the price boundary (no headroom)
        if not (MIN_PRICE <= p0 <= MAX_PRICE) or not (MIN_PRICE <= p1 <= MAX_PRICE):
            continue

        jump_pp = (p1 - p0) * 100.0
        if abs(jump_pp) < JUMP_THRESHOLD_PP:
            continue

        # Reaction: continuation in same direction over next bucket
        cont_pp = (p2 - p1) * 100.0
        # Same-sign continuation magnitude (clipped at 0 if reverse)
        if jump_pp > 0:
            signed_cont = max(cont_pp, -CONTINUATION_PP * 2)  # cap loss at 2*stop
        else:
            signed_cont = max(-cont_pp, -CONTINUATION_PP * 2)

        jumps.append(JumpEvent(
            market_slug=        market_slug,
            token_id=           token_id,
            t_start=            t0,
            t_jump_end=         t1,
            price_before=       p0,
            price_at_jump_end=  p1,
            jump_pp=            jump_pp,
            price_at_react_end= p2,
            continuation_pp=    signed_cont,
        ))
    return jumps


# ── Main ────────────────────────────────────────────────────────────────────

def main() -> int:
    started = time.time()
    out_dir = Path("data/research")
    out_dir.mkdir(parents=True, exist_ok=True)

    markets = fetch_recent_political_markets()
    if not markets:
        print("No markets returned from Gamma. Verdict: RED.")
        return 3

    res = ValidationResult()
    res.n_markets_scanned = len(markets)

    for idx, m in enumerate(markets):
        slug = m.get("slug", "")
        # `clobTokenIds` is a JSON-stringified pair: [yes_id, no_id]
        raw_ids = m.get("clobTokenIds") or "[]"
        try:
            ids = json.loads(raw_ids) if isinstance(raw_ids, str) else raw_ids
        except json.JSONDecodeError:
            continue
        if not ids or not isinstance(ids, list):
            continue
        token_id = str(ids[0])
        if not token_id or token_id == "0":
            continue

        history = fetch_price_history(token_id)
        if not history:
            time.sleep(SLEEP_BETWEEN_REQS / 4)
            continue
        res.n_markets_with_history += 1

        jumps = detect_jumps(history, slug, token_id)
        for j in jumps:
            res.n_jumps += 1
            res.continuations.append(j.continuation_pp)
            if j.continuation_pp >= CONTINUATION_PP:
                res.n_continuations += 1
            if len(res.sample_events) < 10:
                res.sample_events.append(j)

        if (idx + 1) % 10 == 0:
            elapsed = time.time() - started
            print(f"  scanned {idx+1}/{len(markets)} markets "
                  f"(jumps={res.n_jumps} elapsed={elapsed:.0f}s)", flush=True)

        time.sleep(SLEEP_BETWEEN_REQS)
        if time.time() - started > 270:    # 4.5 min hard cap
            print("  runtime cap hit; stopping market scan early", flush=True)
            break

    # Save raw evidence for later inspection
    csv_path = out_dir / "01_news_reaction_phase0.csv"
    with csv_path.open("w", encoding="utf-8") as f:
        f.write("market_slug,t_start,t_jump_end,price_before,"
                "price_at_jump_end,jump_pp,price_at_react_end,continuation_pp\n")
        for j in res.sample_events:
            f.write(f"{j.market_slug},{j.t_start.isoformat()},"
                    f"{j.t_jump_end.isoformat()},{j.price_before:.4f},"
                    f"{j.price_at_jump_end:.4f},{j.jump_pp:+.2f},"
                    f"{j.price_at_react_end:.4f},{j.continuation_pp:+.2f}\n")

    json_path = out_dir / "01_news_reaction_phase0.json"
    summary = {
        "n_markets_scanned":      res.n_markets_scanned,
        "n_markets_with_history": res.n_markets_with_history,
        "n_jumps":                res.n_jumps,
        "n_continuations":        res.n_continuations,
        "continuation_rate":      round(res.continuation_rate, 4),
        "avg_continuation_pp":    round(res.avg_continuation_pp, 4),
        "projected_monthly_pct":  round(res.projected_monthly_net_pct(), 4),
        "lookback_days":          LOOKBACK_DAYS,
        "jump_threshold_pp":      JUMP_THRESHOLD_PP,
        "reaction_window_min":    REACTION_WINDOW_MIN,
        "fee_taker":              FEE_TAKER,
        "samples":                [
            {
                "slug":              j.market_slug,
                "t_jump_end":        j.t_jump_end.isoformat(),
                "jump_pp":           round(j.jump_pp, 2),
                "continuation_pp":   round(j.continuation_pp, 2),
            }
            for j in res.sample_events
        ],
    }
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print()
    print("=" * 64)
    print("STRATEGY 01 / PHASE 0 -- NEWS REACTION LAG WINDOW VALIDATION")
    print("=" * 64)
    print(f"Markets scanned:          {res.n_markets_scanned}")
    print(f"Markets with price hist:  {res.n_markets_with_history}")
    print(f"Jumps detected (>={JUMP_THRESHOLD_PP}pp):   {res.n_jumps}")
    print(f"Continuations (>={CONTINUATION_PP}pp): {res.n_continuations}")
    print(f"Continuation rate:        {res.continuation_rate*100:.1f}%")
    print(f"Avg continuation pp:      {res.avg_continuation_pp:+.2f}pp")
    print(f"Projected monthly net:    {res.projected_monthly_net_pct():+.2f}%  "
          f"(after {FEE_TAKER*100:.0f}% taker × 2)")
    print()

    if res.n_jumps < 5:
        print("[RED] KILL: fewer than 5 jump events in window — strategy has no")
        print("            data surface to operate on at our lookback.")
        return 3

    monthly = res.projected_monthly_net_pct()
    rate = res.continuation_rate
    avg_cont = res.avg_continuation_pp

    if rate >= 0.35 and avg_cont >= 1.5 and monthly >= 1.0:
        print("[GREEN] GREEN-LIGHT Strategy 01 to Phase 1:")
        print(f"        continuation_rate {rate*100:.1f}% >= 35%, "
              f"avg_cont {avg_cont:.2f}pp >= 1.5pp, "
              f"monthly net {monthly:+.2f}% >= 1%.")
        return 0
    if rate >= 0.20 or monthly > 0.0:
        print("[YELLOW] MARGINAL: positive but below promotion thresholds.")
        print(f"         Build Phase 1 cautiously; consider tightening JUMP_THRESHOLD_PP.")
        return 1
    print("[RED] KILL: continuation rate or net edge too weak.")
    print("           News-reaction lag may already be priced out.")
    return 3


if __name__ == "__main__":
    sys.exit(main())
