"""Strategy 03 / Phase 0 -- Whale-fade edge validator.

Hypothesis (from research/strategies/03_whale_fade.md):
  High-volume / persistently-negative-PnL wallets are systematically
  poor directional traders whose fresh BUY flow can be FADED for
  positive expectancy. The mirror image of EDGE-D (whale-FOLLOW).

This validator answers: when we look at the recent trade history of
wallets that today are identified as loser-whales, what would the
P&L of fading each of their BUYs have been?

A FADE = take the opposite side of their BUY at +1 tick:
  - Whale buys YES at p_w with size s_shares
  - We buy NO at p_n = (1 - p_w) + 0.01 with size s_shares
  - At resolution:
      whale's payoff: 1.0 if YES resolved else 0.0
      our payoff:     1.0 if NO  resolved else 0.0
  - Our P&L per share = our_payoff - p_n  (minus fees on entry)

We only count trades in markets that have RESOLVED — this gives us
clean ground-truth without forward-looking bias on price moves. The
cohort we identify is based on CURRENT leaderboard, which has the
known sample-selection caveat (today's losers may be yesterday's
winners), so we filter trades to those at least 7 days OLDER than
the leaderboard snapshot to avoid "fade the streak that made them
qualify" bias.

Pass criteria (per research/cycles/IMPL_LOOP.md quality bar):
  GREEN:  avg_fade_edge_pct >= 1.5 AND n_fades >= 30
          AND projected_monthly_pct (after fees) >= 1%
  YELLOW: positive edge but below 1.5% OR fewer than 30 fades
  RED:    negative edge OR fewer than 5 fades found

Usage:
  python scripts/research/03_whale_fade_validator.py
"""
from __future__ import annotations

import json
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

# ── Endpoints ───────────────────────────────────────────────────────────────

DATA_API_BASE       = "https://data-api.polymarket.com"
DATA_API_TRADES     = f"{DATA_API_BASE}/trades"
DATA_API_LEADERS    = f"{DATA_API_BASE}/leaderboard"  # falls back to /v1/leaderboard
GAMMA_MARKETS_URL   = "https://gamma-api.polymarket.com/markets"

# ── Cohort filters (from research/strategies/03_whale_fade.md) ──────────────

COHORT_MIN_VOLUME_USD     = 50_000.0
COHORT_MAX_PNL_USD        = -1_000.0    # PnL <= this counts as loser
COHORT_MIN_TRADES         = 30          # statistical floor (slightly relaxed from spec's 50)
COHORT_MIN_VWAP_TRADE_USD = 200.0       # excludes MM-style tight-spread small-size
LEADERBOARD_PAGES         = 5
LEADERBOARD_PAGE_SIZE     = 100

# ── Sampling parameters ─────────────────────────────────────────────────────

LOOKBACK_DAYS             = 60
COHORT_SETTLED_GAP_DAYS   = 7           # exclude trades within last N days (avoid look-ahead)
MAX_TRADES_PER_WALLET     = 200
MIN_FADE_SIZE_USD         = 50.0
MAX_FADE_SIZE_USD         = 5_000.0
HARD_RUNTIME_CAP_SECS     = 270         # 4.5 min

# ── Fee model ───────────────────────────────────────────────────────────────

FEE_TAKER = 0.02                         # 2% taker for fading order (worst case)

SLEEP_BETWEEN_REQS = 0.35


# ── Data classes ────────────────────────────────────────────────────────────

@dataclass
class LeaderEntry:
    wallet:    str
    pnl_usd:   float
    vol_usd:   float
    trades:    int


@dataclass
class FadeOutcome:
    wallet:           str
    market_id:        str
    whale_buy_price:  float          # price at which whale bought the YES side
    fade_entry_price: float          # 1 - whale_buy_price + tick (our NO buy)
    size_shares:      float
    yes_won:          bool           # resolution
    fade_pnl_per_sh:  float          # gross P&L per share before fees
    fade_pnl_net_pct: float          # net % return on capital deployed (after fees)
    ts_ms:            int


@dataclass
class ValidationResult:
    n_leaders_scanned:     int = 0
    n_cohort:              int = 0
    n_wallets_processed:   int = 0
    n_fades_evaluated:     int = 0
    n_wins:                int = 0
    pnl_pct_samples:       list[float] = field(default_factory=list)
    sample_fades:          list[FadeOutcome] = field(default_factory=list)

    @property
    def hit_rate(self) -> float:
        return self.n_wins / self.n_fades_evaluated if self.n_fades_evaluated else 0.0

    @property
    def avg_edge_pct(self) -> float:
        if not self.pnl_pct_samples:
            return 0.0
        return sum(self.pnl_pct_samples) / len(self.pnl_pct_samples)

    def projected_monthly_pct(self) -> float:
        """Scale observed total returns from lookback window to a 30-day month."""
        if not self.pnl_pct_samples:
            return 0.0
        # Sum of per-trade % returns gives total %; scale by 30 / lookback
        total = sum(self.pnl_pct_samples)
        return total * (30.0 / LOOKBACK_DAYS)


# ── Leaderboard ─────────────────────────────────────────────────────────────

def _normalise_leaders(payload) -> list[dict]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for k in ("data", "leaders", "entries", "leaderboard"):
            v = payload.get(k)
            if isinstance(v, list):
                return v
    return []


def fetch_leaderboard() -> list[LeaderEntry]:
    """Fetch a few pages from the public leaderboard endpoint, trying
    the two known URL shapes. Empty list on failure."""
    out: list[LeaderEntry] = []
    for base_url in (DATA_API_LEADERS, f"{DATA_API_BASE}/v1/leaderboard"):
        for page in range(LEADERBOARD_PAGES):
            params = {
                "limit":  str(LEADERBOARD_PAGE_SIZE),
                "offset": str(page * LEADERBOARD_PAGE_SIZE),
                "period": "ALL",
                "order":  "volume",
            }
            try:
                r = requests.get(base_url, params=params, timeout=15)
                r.raise_for_status()
                entries = _normalise_leaders(r.json())
            except (requests.RequestException, ValueError) as exc:
                print(f"  leaderboard error ({base_url} page={page}): {exc}", file=sys.stderr)
                break
            if not entries:
                break
            for e in entries:
                wallet = (e.get("proxyWallet") or e.get("address")
                          or e.get("wallet") or "").lower()
                if not wallet:
                    continue
                try:
                    pnl = float(e.get("pnl") or e.get("profit") or 0)
                    vol = float(e.get("volume") or e.get("vol") or 0)
                    trades = int(e.get("trades") or e.get("trade_count") or 0)
                except (TypeError, ValueError):
                    continue
                out.append(LeaderEntry(wallet=wallet, pnl_usd=pnl,
                                       vol_usd=vol, trades=trades))
            time.sleep(SLEEP_BETWEEN_REQS)
        if out:
            break
    print(f"fetched {len(out)} leaderboard entries")
    return out


def loser_cohort(leaders: list[LeaderEntry]) -> list[LeaderEntry]:
    out: list[LeaderEntry] = []
    for entry in leaders:
        if entry.vol_usd < COHORT_MIN_VOLUME_USD:
            continue
        if entry.pnl_usd > COHORT_MAX_PNL_USD:
            continue
        if entry.trades and entry.trades < COHORT_MIN_TRADES:
            continue
        out.append(entry)
    return out


# ── Trades + market resolution ──────────────────────────────────────────────

def fetch_trades(wallet: str, limit: int = MAX_TRADES_PER_WALLET) -> list[dict]:
    params = {"user": wallet, "limit": str(limit)}
    try:
        r = requests.get(DATA_API_TRADES, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, ValueError) as exc:
        print(f"  trades fetch failed for {wallet[:10]}…: {exc}", file=sys.stderr)
        return []
    if isinstance(data, dict):
        data = data.get("data") or data.get("trades") or []
    return data if isinstance(data, list) else []


_MARKET_RES_CACHE: dict[str, bool | None] = {}

def lookup_market_resolution(condition_id: str) -> bool | None:
    """Returns True if YES side won, False if NO side won, None if not
    resolved or unable to determine."""
    if condition_id in _MARKET_RES_CACHE:
        return _MARKET_RES_CACHE[condition_id]
    if not condition_id:
        return None
    params = {"condition_ids": condition_id}
    try:
        r = requests.get(GAMMA_MARKETS_URL, params=params, timeout=10)
        r.raise_for_status()
        markets = r.json()
    except (requests.RequestException, ValueError):
        _MARKET_RES_CACHE[condition_id] = None
        return None
    if not isinstance(markets, list) or not markets:
        _MARKET_RES_CACHE[condition_id] = None
        return None
    m = markets[0]
    if not m.get("closed"):
        _MARKET_RES_CACHE[condition_id] = None
        return None
    outcome_prices_raw = m.get("outcomePrices")
    try:
        prices = (json.loads(outcome_prices_raw)
                  if isinstance(outcome_prices_raw, str) else outcome_prices_raw)
    except (TypeError, json.JSONDecodeError):
        _MARKET_RES_CACHE[condition_id] = None
        return None
    if not isinstance(prices, list) or len(prices) != 2:
        _MARKET_RES_CACHE[condition_id] = None
        return None
    try:
        yes_p = float(prices[0])
        no_p  = float(prices[1])
    except (ValueError, TypeError):
        _MARKET_RES_CACHE[condition_id] = None
        return None
    if yes_p == no_p:  # ambiguous / not yet resolved at clean 0/1
        _MARKET_RES_CACHE[condition_id] = None
        return None
    yes_won = yes_p > no_p
    _MARKET_RES_CACHE[condition_id] = yes_won
    return yes_won


# ── Backtest ────────────────────────────────────────────────────────────────

def evaluate_wallet_fades(wallet: str, raw_trades: list[dict],
                           lookback_start_ts_ms: int,
                           lookahead_cutoff_ms: int) -> list[FadeOutcome]:
    """For each BUY in the wallet's history within lookback, simulate
    a fade and compute realized P&L based on market resolution."""
    out: list[FadeOutcome] = []
    for t in raw_trades:
        side = (t.get("side") or "").upper()
        if side != "BUY":
            continue
        # outcomeIndex 0 = YES side, 1 = NO side. We define the "whale buy
        # price" as the price on whichever side they bought; the fade
        # entry is on the OTHER side.
        try:
            idx = int(t.get("outcomeIndex", -1))
        except (TypeError, ValueError):
            continue
        if idx not in (0, 1):
            continue
        try:
            price = float(t.get("price") or 0)
            shares = float(t.get("size") or t.get("amount") or 0)
        except (TypeError, ValueError):
            continue
        if price <= 0.01 or price >= 0.99 or shares <= 0:
            continue
        size_usd = price * shares
        if size_usd < MIN_FADE_SIZE_USD or size_usd > MAX_FADE_SIZE_USD:
            continue

        ts_raw = t.get("timestamp") or t.get("matchTime") or t.get("ts")
        try:
            ts = int(ts_raw)
            ts_ms = ts * 1000 if ts < 10**12 else ts
        except (TypeError, ValueError):
            continue
        if ts_ms < lookback_start_ts_ms or ts_ms > lookahead_cutoff_ms:
            continue

        condition_id = (t.get("conditionId") or t.get("market_id") or "")
        yes_won = lookup_market_resolution(condition_id)
        if yes_won is None:
            continue

        # whale bought outcome `idx` at `price`. Fade = we buy opposite at
        # (1 - price + tick). Our payoff = $1 if opposite won, else $0.
        fade_entry = (1.0 - price) + 0.01
        if fade_entry <= 0 or fade_entry >= 1:
            continue
        # Did our side (opposite of whale's) win?
        whale_won = (idx == 0 and yes_won) or (idx == 1 and not yes_won)
        fade_won = not whale_won
        payoff = 1.0 if fade_won else 0.0
        gross_pnl_per_share = payoff - fade_entry
        # ROI: P&L on capital deployed per share, applied symmetric taker fees
        roi = (gross_pnl_per_share - FEE_TAKER * (fade_entry + payoff)) / fade_entry
        roi_pct = roi * 100.0

        out.append(FadeOutcome(
            wallet=           wallet,
            market_id=        condition_id,
            whale_buy_price=  price,
            fade_entry_price= fade_entry,
            size_shares=      shares,
            yes_won=          yes_won,
            fade_pnl_per_sh=  gross_pnl_per_share,
            fade_pnl_net_pct= roi_pct,
            ts_ms=            ts_ms,
        ))
    return out


# ── Main ────────────────────────────────────────────────────────────────────

def main() -> int:
    started = time.time()
    out_dir = Path("data/research")
    out_dir.mkdir(parents=True, exist_ok=True)

    leaders = fetch_leaderboard()
    if not leaders:
        print()
        print("[RED] KILL: could not fetch leaderboard data — endpoint unavailable")
        print("           or network blocked. Cannot evaluate strategy 03 edge.")
        return 3

    cohort = loser_cohort(leaders)
    res = ValidationResult()
    res.n_leaders_scanned = len(leaders)
    res.n_cohort = len(cohort)
    print(f"loser cohort: {len(cohort)} wallets "
          f"(vol >= ${COHORT_MIN_VOLUME_USD/1000:.0f}k, "
          f"pnl <= ${COHORT_MAX_PNL_USD/1000:.1f}k)")

    if not cohort:
        print()
        print("[RED] KILL: leaderboard returned data but zero wallets matched the")
        print("           loser-cohort filters. Either thresholds are too tight,")
        print("           or current Polymarket retail doesn't have qualifying")
        print("           consistent losers at this volume scale.")
        return 3

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    lookback_start_ms = now_ms - LOOKBACK_DAYS * 86400 * 1000
    # exclude last COHORT_SETTLED_GAP_DAYS to avoid the recent streak that
    # qualified them now being the same streak we'd "fade"
    cutoff_ms = now_ms - COHORT_SETTLED_GAP_DAYS * 86400 * 1000

    for i, entry in enumerate(cohort):
        if time.time() - started > HARD_RUNTIME_CAP_SECS:
            print(f"  runtime cap hit; stopped at wallet {i}/{len(cohort)}")
            break
        raw_trades = fetch_trades(entry.wallet)
        if not raw_trades:
            time.sleep(SLEEP_BETWEEN_REQS / 4)
            continue
        # Compute vwap trade size to exclude MM patterns
        sizes = []
        for t in raw_trades:
            try:
                p = float(t.get("price") or 0)
                s = float(t.get("size") or 0)
                if p > 0 and s > 0:
                    sizes.append(p * s)
            except (TypeError, ValueError):
                continue
        if sizes:
            vwap = sum(sizes) / max(len(sizes), 1)
            if vwap < COHORT_MIN_VWAP_TRADE_USD:
                continue   # MM filter
        else:
            continue

        fades = evaluate_wallet_fades(entry.wallet, raw_trades,
                                       lookback_start_ms, cutoff_ms)
        res.n_wallets_processed += 1
        for f in fades:
            res.n_fades_evaluated += 1
            if f.fade_pnl_net_pct > 0:
                res.n_wins += 1
            res.pnl_pct_samples.append(f.fade_pnl_net_pct)
            if len(res.sample_fades) < 15:
                res.sample_fades.append(f)

        if (i + 1) % 10 == 0:
            elapsed = time.time() - started
            print(f"  processed {i+1}/{len(cohort)} wallets "
                  f"(fades={res.n_fades_evaluated} elapsed={elapsed:.0f}s)",
                  flush=True)
        time.sleep(SLEEP_BETWEEN_REQS)

    # Write artefacts
    csv_path = out_dir / "03_whale_fade_phase0.csv"
    with csv_path.open("w", encoding="utf-8") as f:
        f.write("wallet,market_id,whale_buy_price,fade_entry_price,"
                "size_shares,yes_won,fade_pnl_per_share,fade_pnl_net_pct,ts_ms\n")
        for fo in res.sample_fades:
            f.write(f"{fo.wallet},{fo.market_id},{fo.whale_buy_price:.4f},"
                    f"{fo.fade_entry_price:.4f},{fo.size_shares:.2f},"
                    f"{int(fo.yes_won)},{fo.fade_pnl_per_sh:+.4f},"
                    f"{fo.fade_pnl_net_pct:+.2f},{fo.ts_ms}\n")

    summary = {
        "n_leaders_scanned":  res.n_leaders_scanned,
        "n_cohort":           res.n_cohort,
        "n_wallets_processed": res.n_wallets_processed,
        "n_fades_evaluated":  res.n_fades_evaluated,
        "n_wins":             res.n_wins,
        "hit_rate":           round(res.hit_rate, 4),
        "avg_edge_pct":       round(res.avg_edge_pct, 4),
        "projected_monthly_pct": round(res.projected_monthly_pct(), 4),
        "lookback_days":      LOOKBACK_DAYS,
        "fee_taker":          FEE_TAKER,
    }
    json_path = out_dir / "03_whale_fade_phase0.json"
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print()
    print("=" * 64)
    print("STRATEGY 03 / PHASE 0 -- WHALE FADE EDGE VALIDATION")
    print("=" * 64)
    print(f"Leaderboard entries:     {res.n_leaders_scanned}")
    print(f"Loser cohort:            {res.n_cohort}")
    print(f"Wallets w/ trades:       {res.n_wallets_processed}")
    print(f"Fades evaluated:         {res.n_fades_evaluated}")
    print(f"Hit rate:                {res.hit_rate*100:.1f}%")
    print(f"Avg fade edge:           {res.avg_edge_pct:+.2f}% per trade")
    print(f"Projected monthly net:   {res.projected_monthly_pct():+.2f}%")
    print()

    if res.n_fades_evaluated < 5:
        print("[RED] KILL: too few resolved fade samples to draw conclusions.")
        print("            Cohort may be inactive in resolved markets,")
        print("            or data-api/trades pagination cap hit.")
        return 3

    avg = res.avg_edge_pct
    monthly = res.projected_monthly_pct()
    n = res.n_fades_evaluated

    if avg >= 1.5 and n >= 30 and monthly >= 1.0:
        print("[GREEN] GREEN-LIGHT Strategy 03 to Phase 1:")
        print(f"        avg edge {avg:+.2f}% >= 1.5%, n={n} >= 30, "
              f"projected monthly {monthly:+.2f}% >= 1%.")
        return 0
    if avg > 0:
        print("[YELLOW] MARGINAL: positive edge but below promotion bar.")
        print(f"         (avg {avg:+.2f}% / n={n} / monthly {monthly:+.2f}%)")
        print("         Build Phase 1 cautiously; tighten cohort filters first.")
        return 1
    print("[RED] KILL: negative or zero edge across resolved fades.")
    print("           Loser-whale fade premise not supported at current cohort.")
    return 3


if __name__ == "__main__":
    sys.exit(main())
