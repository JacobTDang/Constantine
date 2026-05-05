"""Strategy 2 / Phase 0 -- Multi-candidate Event Sum-of-YES Arb validation.

Mechanic: for any event with N mutually-exclusive outcomes (election,
tournament winner, awards), the sum of YES prices SHOULD = 100% in an
efficient market. Polymarket retail consistently overprices long-shots,
so the sum drifts to 105-130%. Capturing the surplus is mechanical:

  - SHORT each YES (or equivalently, BUY each NO) with a portfolio
    weighted by edge per leg.
  - At resolution, exactly one candidate wins; the other (N-1) NOs pay
    out, netting `sum_of_no_asks - (N-1)` profit per dollar of capital,
    minus fees.

The KEY question this script answers: after fees + spread crossing,
does enough captureable edge actually exist?

  - Naive: sum of bestAsk often > 100% but is inflated by placeholder
    candidates priced at $1.00 (those don't fill).
  - Realistic: sum of TRADEABLE NO asks (= 1 - YES bid for liquid legs).
    If you can sell every liquid candidate's NO and the proceeds < 1.0,
    the "arb" is illusory.

Pass criteria from STRATEGIES.md:
  - >= 2 events show captureable_edge > 3% sustained over the snapshot
  - Liquidity > $1K bid depth on enough legs to hit our position cap

Usage:
  python scripts/research/strategy2_event_arb_validation.py
"""
from __future__ import annotations

import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import requests

EVENTS_URL = "https://gamma-api.polymarket.com/events"
PAGE_SIZE  = 200

# Polymarket fee schedule. Maker is 0%, taker is 2% (BTC binaries -- other
# markets vary, but 2% is the conservative default). For this strategy
# we'd ideally place LIMIT orders that get filled as maker (0% fees);
# the taker assumption here is a conservative worst-case.
FEE_TAKER     = 0.02
FEE_MAKER     = 0.00

# Min volume / liquidity per leg to count it as "tradeable"
MIN_LEG_LIQUIDITY_USD = 500
MIN_LEG_VOLUME_24H    = 50

# Capital per leg cap: $5 minimum order size on Polymarket
MIN_ORDER_USD = 5.0


@dataclass
class Leg:
    candidate:    str          # "Pete Buttigieg"
    yes_ask:      float        # cost to buy YES
    yes_bid:      float        # what YES would sell for
    no_ask:       float        # cost to buy NO  = approx 1 - yes_bid
    no_bid:       float        # what NO would sell for = approx 1 - yes_ask
    liquidity:    float
    volume_24h:   float
    @property
    def is_tradeable(self) -> bool:
        return (self.liquidity >= MIN_LEG_LIQUIDITY_USD
                and self.volume_24h >= MIN_LEG_VOLUME_24H
                and 0 < self.yes_ask < 1
                and 0 < self.no_ask  < 1)


@dataclass
class EventSnapshot:
    title:                str
    slug:                 str
    n_legs_total:         int
    legs:                 list[Leg]
    @property
    def tradeable_legs(self) -> list[Leg]:
        return [l for l in self.legs if l.is_tradeable]
    @property
    def sum_yes_ask(self) -> float:
        return sum(l.yes_ask for l in self.legs)
    @property
    def sum_yes_bid(self) -> float:
        return sum(l.yes_bid for l in self.legs)
    @property
    def sum_no_ask_tradeable(self) -> float:
        """Cost to buy NO on every tradeable leg, taker fees included."""
        return sum(l.no_ask * (1 + FEE_TAKER) for l in self.tradeable_legs)
    @property
    def sum_no_ask_maker(self) -> float:
        """Same but assuming we get filled as maker (0% fees) -- best case."""
        return sum(l.no_ask for l in self.tradeable_legs)


# ── Fetching ────────────────────────────────────────────────────────────────

def fetch_open_events(min_markets: int = 5) -> list[dict]:
    """Fetch open events with at least `min_markets` candidate markets.
    Multi-candidate events are the targets -- single-market events have no
    sum-of-YES arb because YES + NO already = 1 (binary)."""
    out: list[dict] = []
    for offset in range(0, 1000, PAGE_SIZE):
        params = {
            "closed":    "false",
            "active":    "true",
            "limit":     str(PAGE_SIZE),
            "offset":    str(offset),
            "order":     "volume",
            "ascending": "false",
        }
        r = requests.get(EVENTS_URL, params=params, timeout=20)
        r.raise_for_status()
        events = r.json()
        if not events:
            break
        for e in events:
            if len(e.get("markets", [])) >= min_markets:
                out.append(e)
        time.sleep(0.3)
    print(f"found {len(out)} events with >= {min_markets} markets")
    return out


# ── Leg extraction ──────────────────────────────────────────────────────────

def parse_leg(market: dict) -> Leg | None:
    """Extract bid/ask + liquidity for one candidate market."""
    # outcomePrices: ["YES_price", "NO_price"] -- implied probability
    # bestBid / bestAsk: actual orderbook touches
    try:
        yes_ask = float(market.get("bestAsk")  or market.get("outcomePrices", [None])[0] or 0)
        yes_bid = float(market.get("bestBid")  or 0)
    except (TypeError, ValueError):
        return None
    if yes_ask <= 0 or yes_ask >= 1.0:   # placeholder or unreal
        return None

    # NO side: price = 1 - YES (binary identity).
    no_ask = round(1.0 - yes_bid, 4) if yes_bid > 0 else 1.0 - yes_ask
    no_bid = round(1.0 - yes_ask, 4) if yes_ask > 0 else 0.0

    liq = float(market.get("liquidity",     0) or 0)
    vol = float(market.get("volume24hr",    market.get("volume", 0)) or 0)

    # Try multiple ways to extract the candidate name
    candidate = (market.get("groupItemTitle")
                 or market.get("title")
                 or market.get("question", ""))
    return Leg(
        candidate=  candidate,
        yes_ask=    yes_ask,
        yes_bid=    yes_bid,
        no_ask=     no_ask,
        no_bid=     no_bid,
        liquidity=  liq,
        volume_24h= vol,
    )


def snapshot_event(event: dict) -> EventSnapshot:
    legs: list[Leg] = []
    for m in event.get("markets", []):
        leg = parse_leg(m)
        if leg:
            legs.append(leg)
    return EventSnapshot(
        title=         event.get("title", ""),
        slug=          event.get("slug",  ""),
        n_legs_total=  len(event.get("markets", [])),
        legs=          legs,
    )


# ── Edge computation ────────────────────────────────────────────────────────

@dataclass
class EdgeReport:
    snapshot:                EventSnapshot
    n_tradeable:             int
    sum_no_taker:            float    # cost to buy all NOs at ask + 2% fees
    sum_no_maker:            float    # if we fill as maker
    captureable_edge_taker:  float    # = (n_tradeable - 1) - sum_no_taker
    captureable_edge_maker:  float
    bid_depth_total:         float


def compute_edge(snap: EventSnapshot) -> EdgeReport:
    n = len(snap.tradeable_legs)
    sum_no_taker = snap.sum_no_ask_tradeable
    sum_no_maker = snap.sum_no_ask_maker
    # Buying NO on N legs: at resolution, exactly N-1 of them pay $1
    # (the loser candidates), and 1 pays $0 (the winner). So total payout
    # is (N-1) per dollar invested... wait, that's not quite right.
    # Each NO cost no_ask; each NO that wins (= candidate loses) pays $1.
    # Of N legs, (N-1) NOs pay out (since 1 candidate wins).
    # Total invested = sum(no_ask × (1+fee)).
    # Total payout   = (N-1) × $1 (well, actually $1 per leg that wins).
    # Profit         = (N-1) - sum_no_ask × (1+fee).
    # We profit if  (N-1) > sum_no_ask × (1+fee), i.e. captureable > 0.
    edge_taker = (n - 1) - sum_no_taker
    edge_maker = (n - 1) - sum_no_maker
    bid_depth = sum(l.liquidity for l in snap.tradeable_legs)
    return EdgeReport(
        snapshot=               snap,
        n_tradeable=            n,
        sum_no_taker=           sum_no_taker,
        sum_no_maker=           sum_no_maker,
        captureable_edge_taker= edge_taker,
        captureable_edge_maker= edge_maker,
        bid_depth_total=        bid_depth,
    )


# ── Reporting ───────────────────────────────────────────────────────────────

# ── Critical filter: exclusive-winner events only ───────────────────────────
#
# The (N-1)-payout arb math ONLY works when EXACTLY ONE candidate wins.
# Most multi-market Polymarket events are NOT exclusive-winner:
#
#   - "Top 10 X" → 10 winners → arb math breaks
#   - "Above $X on date Y" → if price > Y, ALL "above" thresholds win
#   - "Lakers vs Thunder" → 21 independent binary questions, not a race
#   - "Who will Trump speak to" → multiple speakings → K>1 winners
#
# Heuristic: an exclusive-winner event has sum_yes_ask near 1.0 (slightly
# inflated by retail overpricing of long-shots). If sum_yes >> 1, the
# event has K>1 winners and the arb is illusory.

EXCLUSIVE_SUM_LO = 0.95   # below this = market mispriced overall (or multi-loser)
EXCLUSIVE_SUM_HI = 1.30   # above this = K>1 winners; not arb-able

def is_exclusive_winner(snap: EventSnapshot) -> bool:
    """Return True only for events plausibly with exactly 1 winner.
    Combines a sum-of-YES range check with title-keyword heuristics."""
    s = snap.sum_yes_ask
    if not (EXCLUSIVE_SUM_LO <= s <= EXCLUSIVE_SUM_HI):
        return False
    title = snap.title.lower()
    # Negative keywords: definitive multi-winner patterns
    multi_winner_kw = [
        "top ", "first ", "states ", "players ", "above $", "above ",
        "below $", "below ", "total games", "vs.", " vs ", "semi-final",
        "speakings", "meetings", "appearances", "events",
    ]
    if any(kw in title for kw in multi_winner_kw):
        return False
    # Positive keywords: singular-winner patterns
    winner_kw = [" winner", " win?", "nominee", "champion", "mvp",
                 "president 20", "rookie of the year", "of the year"]
    if any(kw in title for kw in winner_kw):
        return True
    # Default: if sum is near 1.0 and no title hints, treat as exclusive
    # (conservative — relies on the sum-near-1 invariant alone)
    return EXCLUSIVE_SUM_LO <= s <= 1.10


def main() -> int:
    out_dir = Path("data/research")
    out_dir.mkdir(parents=True, exist_ok=True)

    events = fetch_open_events(min_markets=5)
    if not events:
        print("no multi-candidate events found")
        return 2

    reports: list[EdgeReport] = []
    skipped_multi = 0
    for e in events:
        snap = snapshot_event(e)
        if not snap.tradeable_legs:
            continue
        if snap.n_legs_total < 5:
            continue
        if not is_exclusive_winner(snap):
            skipped_multi += 1
            continue
        rep = compute_edge(snap)
        reports.append(rep)
    print(f"after exclusive-winner filter: {len(reports)} events kept, {skipped_multi} skipped as multi-winner")

    if not reports:
        print("no events with tradeable legs found")
        return 2

    reports.sort(key=lambda r: r.captureable_edge_maker, reverse=True)

    # Save full snapshot
    csv = out_dir / "event_arb_snapshots.csv"
    with csv.open("w", encoding="utf-8") as f:
        f.write("event,n_total,n_tradeable,sum_yes_ask,sum_no_taker,"
                "sum_no_maker,edge_taker,edge_maker,bid_depth_usd\n")
        for r in reports:
            s = r.snapshot
            f.write(
                f"{s.title.replace(',', ' ')[:100]},"
                f"{s.n_legs_total},{r.n_tradeable},"
                f"{s.sum_yes_ask:.4f},{r.sum_no_taker:.4f},"
                f"{r.sum_no_maker:.4f},{r.captureable_edge_taker:.4f},"
                f"{r.captureable_edge_maker:.4f},{r.bid_depth_total:.0f}\n"
            )
    print(f"wrote {csv}")

    print()
    print("=" * 80)
    print("STRATEGY 2 / PHASE 0 -- EVENT SUM-OF-YES ARB VALIDATION")
    print("=" * 80)
    print()

    # Top 10 events by maker-fee captureable edge
    print(f"{'Event':<60}  {'#':>3}  {'sum_yes':>7}  {'edge_M':>7}  {'edge_T':>7}  {'liq':>9}")
    print("-" * 100)
    for r in reports[:15]:
        s = r.snapshot
        print(
            f"{s.title[:60]:<60}  "
            f"{r.n_tradeable:>3}  "
            f"{s.sum_yes_ask:>6.2f}   "
            f"{r.captureable_edge_maker:>+6.3f}   "
            f"{r.captureable_edge_taker:>+6.3f}   "
            f"{r.bid_depth_total:>8.0f}"
        )

    # Pass / fail decision
    profitable_at_maker = [r for r in reports
                           if r.captureable_edge_maker >= 0.03
                           and r.bid_depth_total >= 1000]
    profitable_at_taker = [r for r in reports
                           if r.captureable_edge_taker >= 0.03
                           and r.bid_depth_total >= 1000]

    print()
    print(f"Events with >3% edge AS MAKER (0% fees): {len(profitable_at_maker)}")
    print(f"Events with >3% edge AS TAKER (2% fees): {len(profitable_at_taker)}")
    print()

    if len(profitable_at_maker) >= 2:
        print("[GREEN] GREEN-LIGHT Strategy 2 (maker-only): >= 2 events show > 3% maker edge.")
        print("  Build Phase 1 with LIMIT ORDERS that rest as maker.")
        print("  Taker-execution path will lose money -- must enforce maker-only.")
        return 0
    elif len(profitable_at_taker) >= 2:
        print("[GREEN] GREEN-LIGHT Strategy 2 (either path): >= 2 events show > 3% taker edge.")
        return 0
    else:
        print("[RED] NO CLEAR EDGE today. Either:")
        print("    - Check again on different market days (sum-of-YES varies)")
        print("    - The 127% sum-of-YES seen earlier was inflated by placeholders")
        print("    - Strategy may need maker-only execution to be profitable")
        return 3


if __name__ == "__main__":
    sys.exit(main())
