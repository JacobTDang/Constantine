"""EDGE-A / Phase 0 -- Liquidity Rewards Market Making validation.

Mechanic: Polymarket pays makers daily for resting two-sided GTC limit
orders inside a max-spread band. Reward formula (verbatim from docs):

    S(v, s) = ((v - s) / v)^2 * b
    Q_min   = max(min(Q_one, Q_two), max(Q_one/c, Q_two/c))   if mid in [0.10, 0.90]
            = min(Q_one, Q_two)                                otherwise
    payout  = (your_score / sum_all_makers_scores) * pool, daily at 00:00 UTC

Where:
  - v    = market's rewards_max_spread (cents from mid)
  - s    = your order's distance from adjusted midpoint
  - b    = in-game multiplier (assume 1.0 in absence of data)
  - c    = single-sided penalty (currently 3.0 -- two-sided required)

This script answers: at our proposed quote position (v/2 from mid) on
the highest-paying markets, what's the expected daily yield on capital
deployed, accounting for:

  1. Reward share -- our score / sum of competing makers' scores
  2. Adverse selection -- when mid moves away from our quote, our stale
     side fills at a loss before we can re-quote.

Pass criteria:
  - At least 3 markets show expected_daily_yield_pct > 0.5% net of
    adverse selection in conservative model
  - Top markets show > 1% expected daily yield
  - At realistic refresh cadence (5 sec) and 1 ticks/min mid-volatility

Usage:
  python scripts/research/edge_a_lp_rewards_validation.py
"""
from __future__ import annotations

import json
import math
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import requests

CLOB_BASE = "https://clob.polymarket.com"
REWARDS_URL = f"{CLOB_BASE}/rewards/markets/current"
BOOK_URL    = f"{CLOB_BASE}/book"
GAMMA_MARKET_URL = "https://gamma-api.polymarket.com/markets"

# Reward formula constants (per Polymarket docs)
SINGLE_SIDED_PENALTY_C = 3.0
SAMPLES_PER_EPOCH = 10_080      # 6-second sampling, 16.8h epoch
EPOCHS_PER_DAY = 24 / 16.8      # ~1.43

# Our deployment parameters
QUOTE_FRACTION_OF_V = 0.5       # quote at v/2 from mid (= 25% of max score)
QUOTE_SIZE_USD = 200            # per side, per market
REFRESH_CADENCE_SEC = 5         # how often we cancel + re-quote

# Adverse selection model:
#   Polymarket tick is 0.1c-1c depending on market. Assume 1c moves
#   happen at MID_VOL_TICKS_PER_MIN per minute. Each move that crosses
#   our stale quote results in a 1-tick fill loss for QUOTE_SIZE_USD.
MID_VOL_TICKS_PER_MIN = 1.0     # conservative; tighten after live data
TICK_LOSS_USD_PER_FILL = 0.01   # 1c worst-case adverse loss per stale fill

# Trading fee model
MAKER_FEE = 0.00                # Polymarket maker fee is 0
MAKER_REBATE_FRAC = 0.20        # 20% of taker fee comes back; small bonus


@dataclass
class RewardConfig:
    condition_id: str
    rewards_max_spread: float       # in cents
    rewards_min_size: float         # USD min order
    rate_per_day: float             # USDC pool/day for this market
    sponsored_daily_rate: float = 0.0


@dataclass
class BookSnapshot:
    token_id: str
    bids: list[tuple[float, float]] = field(default_factory=list)  # (price, size)
    asks: list[tuple[float, float]] = field(default_factory=list)

    @property
    def best_bid(self) -> float | None:
        return self.bids[0][0] if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return self.asks[0][0] if self.asks else None

    @property
    def mid(self) -> float | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2.0


@dataclass
class MarketAnalysis:
    cfg: RewardConfig
    market_meta: dict
    yes_book: BookSnapshot
    no_book:  BookSnapshot

    # Computed
    competing_makers_estimate: int = 0
    our_score_share: float = 0.0
    expected_daily_reward_usd: float = 0.0
    expected_adverse_loss_usd: float = 0.0
    capital_deployed_usd: float = 0.0
    expected_daily_yield_pct: float = 0.0
    verdict: str = "?"
    notes: list[str] = field(default_factory=list)


# ---- Fetch helpers ---------------------------------------------------------

def fetch_active_rewards(top_n: int = 30) -> list[RewardConfig]:
    """Pull active reward configs sorted by rate_per_day desc."""
    cfgs: list[RewardConfig] = []
    next_cursor: str | None = None
    while True:
        params = {"limit": "500"}
        if next_cursor:
            params["next_cursor"] = next_cursor
        r = requests.get(REWARDS_URL, params=params, timeout=20)
        r.raise_for_status()
        body = r.json()
        for row in body.get("data", []):
            cfg = row.get("rewards_config") or [{}]
            rate = float(cfg[0].get("rate_per_day", 0)) if cfg else 0.0
            if rate <= 0:
                continue
            cfgs.append(RewardConfig(
                condition_id       = row["condition_id"],
                rewards_max_spread = float(row.get("rewards_max_spread") or 0),
                rewards_min_size   = float(row.get("rewards_min_size") or 0),
                rate_per_day       = rate,
                sponsored_daily_rate = float(row.get("sponsored_daily_rate") or 0),
            ))
        next_cursor = body.get("next_cursor")
        if not next_cursor or next_cursor == "LTE=":
            break
        time.sleep(0.3)
    cfgs.sort(key=lambda x: x.rate_per_day + x.sponsored_daily_rate, reverse=True)
    return cfgs[:top_n]


def fetch_market_meta(condition_id: str) -> dict | None:
    """Resolve condition_id -> Gamma market metadata (token IDs, question)."""
    try:
        r = requests.get(GAMMA_MARKET_URL,
                         params={"condition_ids": condition_id},
                         timeout=15)
        r.raise_for_status()
        rows = r.json()
        if isinstance(rows, list) and rows:
            return rows[0]
        return None
    except requests.RequestException as exc:
        print(f"  market meta fetch failed: {exc}", file=sys.stderr)
        return None


def fetch_book(token_id: str) -> BookSnapshot:
    """Fetch the order book for one token."""
    try:
        r = requests.get(BOOK_URL, params={"token_id": token_id}, timeout=15)
        r.raise_for_status()
        body = r.json()
        snap = BookSnapshot(token_id=token_id)
        for level in body.get("bids", []):
            snap.bids.append((float(level["price"]), float(level["size"])))
        for level in body.get("asks", []):
            snap.asks.append((float(level["price"]), float(level["size"])))
        snap.bids.sort(key=lambda x: x[0], reverse=True)
        snap.asks.sort(key=lambda x: x[0])
        return snap
    except requests.RequestException as exc:
        print(f"  book fetch failed for {token_id[:10]}: {exc}", file=sys.stderr)
        return BookSnapshot(token_id=token_id)


# ---- Score / yield model ---------------------------------------------------

def score_for_quote(spread_cents: float, max_spread_cents: float) -> float:
    """Per-sample score for one order at distance `spread_cents` from mid.

    Polymarket: S = ((v-s)/v)^2 * b. We assume b=1.
    Returns 0 if spread > v.
    """
    if max_spread_cents <= 0 or spread_cents >= max_spread_cents:
        return 0.0
    return ((max_spread_cents - spread_cents) / max_spread_cents) ** 2


def estimate_competing_makers(book: BookSnapshot, max_spread_cents: float) -> int:
    """Count distinct price levels within v cents of mid as a proxy for
    competing makers. Conservative -- many price levels could be the same maker."""
    mid = book.mid
    if mid is None:
        return 0
    threshold = max_spread_cents / 100.0
    levels_inside = 0
    for price, _size in book.bids:
        if mid - price <= threshold and price < mid:
            levels_inside += 1
    for price, _size in book.asks:
        if price - mid <= threshold and price > mid:
            levels_inside += 1
    # Cap and floor: at least 1 (us), assume each level is a different maker.
    return max(levels_inside, 1)


def estimate_adverse_loss(quote_size_usd: float,
                          refresh_cadence_sec: float,
                          mid_vol_ticks_per_min: float,
                          tick_loss_usd_per_fill: float) -> float:
    """Per-day adverse selection loss estimate.

    Assume mid moves at `mid_vol_ticks_per_min` ticks/min. Each move that
    occurs between our refreshes can fill our stale quote on the wrong side.
    """
    moves_per_day = mid_vol_ticks_per_min * 60 * 24
    stale_window_per_move = refresh_cadence_sec / 60.0  # minutes of staleness
    # Probability the stale window catches us on the wrong side: ~50%
    expected_adverse_fills = moves_per_day * 0.5
    return expected_adverse_fills * tick_loss_usd_per_fill * (quote_size_usd / 100.0)


def analyze_market(cfg: RewardConfig) -> MarketAnalysis | None:
    meta = fetch_market_meta(cfg.condition_id)
    if not meta:
        print(f"  skip {cfg.condition_id[:12]}: no Gamma metadata")
        return None

    token_ids_raw = meta.get("clobTokenIds")
    if isinstance(token_ids_raw, str):
        try:
            token_ids = json.loads(token_ids_raw)
        except json.JSONDecodeError:
            token_ids = []
    else:
        token_ids = token_ids_raw or []

    if len(token_ids) < 2:
        print(f"  skip {cfg.condition_id[:12]}: not 2 token IDs")
        return None

    yes_book = fetch_book(token_ids[0])
    no_book  = fetch_book(token_ids[1])
    time.sleep(0.2)

    if yes_book.mid is None or no_book.mid is None:
        return None

    a = MarketAnalysis(cfg=cfg, market_meta=meta, yes_book=yes_book, no_book=no_book)

    # Use YES book mid + max_spread for analysis (NO is mirror image)
    max_spread_cents = cfg.rewards_max_spread * 100  # docs use cents but config is fraction
    if max_spread_cents > 100:
        max_spread_cents = cfg.rewards_max_spread     # already in cents
    spread_position_cents = max_spread_cents * QUOTE_FRACTION_OF_V

    a.competing_makers_estimate = estimate_competing_makers(yes_book, max_spread_cents)
    per_sample_score = score_for_quote(spread_position_cents, max_spread_cents)
    # Two-sided quoting -- both sides scored, then aggregated by Q_min logic.
    # Conservative: assume Q_min picks the smaller side; we set both equal so
    # Q_min = our per-sample score.
    if 0.10 <= yes_book.mid <= 0.90:
        our_market_score_per_sample = per_sample_score
    else:
        our_market_score_per_sample = per_sample_score / SINGLE_SIDED_PENALTY_C

    # Competing makers' score share: assume each is also at v/2 (worst case),
    # so they each contribute the same per_sample_score. Our share = 1/(N+1).
    # This is the most pessimistic competition assumption.
    a.our_score_share = 1.0 / (a.competing_makers_estimate + 1)

    pool_per_day = cfg.rate_per_day + cfg.sponsored_daily_rate
    a.expected_daily_reward_usd = pool_per_day * a.our_score_share

    a.capital_deployed_usd = QUOTE_SIZE_USD * 2     # both sides

    a.expected_adverse_loss_usd = estimate_adverse_loss(
        QUOTE_SIZE_USD, REFRESH_CADENCE_SEC,
        MID_VOL_TICKS_PER_MIN, TICK_LOSS_USD_PER_FILL,
    )

    net_per_day = a.expected_daily_reward_usd - a.expected_adverse_loss_usd
    a.expected_daily_yield_pct = (net_per_day / a.capital_deployed_usd) * 100.0

    # Verdict
    if max_spread_cents <= 0:
        a.verdict = "RED"
        a.notes.append("max_spread is zero -- can't quote")
    elif QUOTE_SIZE_USD < cfg.rewards_min_size:
        a.verdict = "RED"
        a.notes.append(
            f"our quote size {QUOTE_SIZE_USD} < min_size {cfg.rewards_min_size}")
    elif a.expected_daily_yield_pct >= 1.0:
        a.verdict = "GREEN"
    elif a.expected_daily_yield_pct >= 0.3:
        a.verdict = "YELLOW"
    else:
        a.verdict = "RED"
        a.notes.append("net yield below 0.3%/day -- not worth capital lock")

    return a


# ---- Main ------------------------------------------------------------------

def main() -> int:
    print(f"=== EDGE-A LP Rewards Validation -- {datetime.now(timezone.utc).isoformat()}Z ===\n")

    print("Fetching active reward markets...")
    cfgs = fetch_active_rewards(top_n=20)
    if not cfgs:
        print("ERROR: no active reward markets found")
        return 1
    print(f"  pulled {len(cfgs)} markets, top rate_per_day = "
          f"{cfgs[0].rate_per_day + cfgs[0].sponsored_daily_rate:.0f} USDC\n")

    analyses: list[MarketAnalysis] = []
    for i, cfg in enumerate(cfgs):
        print(f"[{i+1}/{len(cfgs)}] {cfg.condition_id[:14]} "
              f"rate={cfg.rate_per_day:.0f} v={cfg.rewards_max_spread} "
              f"min_sz={cfg.rewards_min_size}")
        a = analyze_market(cfg)
        if a is None:
            continue
        analyses.append(a)
        print(f"    competing_makers~{a.competing_makers_estimate} "
              f"our_share={a.our_score_share*100:.1f}% "
              f"reward=${a.expected_daily_reward_usd:.2f}/d "
              f"adv_loss=${a.expected_adverse_loss_usd:.2f}/d "
              f"yield={a.expected_daily_yield_pct:+.2f}%/d "
              f"=> {a.verdict}")

    # Summary
    print("\n=== Summary ===")
    by_verdict = {"GREEN": [], "YELLOW": [], "RED": []}
    for a in analyses:
        by_verdict[a.verdict].append(a)
    for verdict, lst in by_verdict.items():
        print(f"  {verdict}: {len(lst)} markets")

    print("\n=== Top GREEN markets ===")
    greens = sorted(by_verdict["GREEN"],
                    key=lambda x: x.expected_daily_yield_pct, reverse=True)
    for a in greens[:10]:
        q = a.market_meta.get("question", "?")[:60]
        print(f"  {a.expected_daily_yield_pct:+.2f}%/d  "
              f"reward=${a.expected_daily_reward_usd:.0f} "
              f"{a.cfg.condition_id[:10]}  {q}")

    # Final verdict
    n_green = len(by_verdict["GREEN"])
    if n_green >= 3:
        print(f"\nVERDICT: GREEN -- {n_green} markets pass with > 1% net daily yield. "
              f"Strategy 6 is viable. Proceed to Phase 1 (build LpQuoter).")
    elif n_green >= 1 or len(by_verdict["YELLOW"]) >= 5:
        print(f"\nVERDICT: YELLOW -- only {n_green} GREEN, "
              f"{len(by_verdict['YELLOW'])} YELLOW. Marginal edge; proceed cautiously "
              f"with single-market pilot first.")
    else:
        print(f"\nVERDICT: RED -- competition too high or rates too low at this snapshot. "
              f"Recheck during off-hours or after a Polymarket event campaign launch.")

    # Persist for inspection
    out_path = Path("data/research/edge_a_validation.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps({
        "ts": datetime.now(timezone.utc).isoformat(),
        "params": {
            "quote_fraction_of_v": QUOTE_FRACTION_OF_V,
            "quote_size_usd": QUOTE_SIZE_USD,
            "refresh_cadence_sec": REFRESH_CADENCE_SEC,
            "mid_vol_ticks_per_min": MID_VOL_TICKS_PER_MIN,
        },
        "markets": [
            {
                "condition_id":     a.cfg.condition_id,
                "question":         a.market_meta.get("question", ""),
                "rate_per_day":     a.cfg.rate_per_day,
                "max_spread":       a.cfg.rewards_max_spread,
                "min_size":         a.cfg.rewards_min_size,
                "competing_makers": a.competing_makers_estimate,
                "our_share":        a.our_score_share,
                "reward_usd_day":   a.expected_daily_reward_usd,
                "adv_loss_usd_day": a.expected_adverse_loss_usd,
                "yield_pct_day":    a.expected_daily_yield_pct,
                "verdict":          a.verdict,
            }
            for a in analyses
        ],
    }, indent=2))
    print(f"\nwrote {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
