"""Strategy 05 / Phase 0 -- Cross-market statistical arbitrage validator.

Hypothesis (from research/strategies/05_stat_arb_correlated_baskets.md):
  Polymarket lists logically- and statistically-correlated markets on
  independent order books. When one leg moves and a correlated leg
  doesn't, a beta-weighted spread trade earns a mean-reversion premium.

Phase 0 plan from spec:
  "Hand-pick 5 pairs from the table above. Pull 90 days of price
   history. Compute rolling β + spread z-score. Manually identify
   10 historical |z| > 2 episodes per pair. Pass if median reversion
   within 7 days for > 70% of episodes."

This validator automates the manual step: instead of hand-picking
pairs, it pulls active political markets, computes pairwise Pearson
correlations on their hourly price history, picks the top N highly-
correlated pairs (rho >= 0.6), finds |z-score| > 2 spread deviations,
and measures reversion within 7 calendar days.

Pass criteria (per research/cycles/IMPL_LOOP.md quality bar):
  GREEN:  >= 5 pairs analysed AND median reversion rate >= 70%
          AND avg reversion magnitude <= 0.5 (z back below half stdev)
  YELLOW: reversion rate 50-70% OR positive but mismatched magnitude
  RED:    reversion rate < 50% OR fewer than 5 viable pairs found

Usage:
  python scripts/research/05_stat_arb_validator.py
"""
from __future__ import annotations

import json
import math
import statistics
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
CLOB_HISTORY_URL = "https://clob.polymarket.com/prices-history"

LOOKBACK_DAYS         = 60
MIN_HISTORY_POINTS    = 30
PAGE_SIZE             = 100
MAX_GAMMA_PAGES       = 3
MAX_MARKETS_TO_FETCH  = 40        # cap price-history calls
TOP_PAIRS_BY_RHO      = 25
MIN_CORRELATION       = 0.60
Z_ENTRY_THRESHOLD     = 2.0
Z_EXIT_THRESHOLD      = 0.5
REVERSION_WINDOW_H    = 7 * 24
SLEEP_BETWEEN_REQS    = 0.4
HARD_RUNTIME_SECS     = 270

CATEGORY_TAGS = ("politics", "trump", "world", "geopolitics", "crypto")


@dataclass
class MarketHist:
    slug:    str
    tid:     str
    history: list[tuple[datetime, float]]


@dataclass
class PairResult:
    a_slug:        str
    b_slug:        str
    rho:           float
    beta:          float
    n_episodes:    int
    n_reverted:    int
    reversion_pct: float


@dataclass
class ValidationResult:
    n_markets:            int = 0
    n_with_history:       int = 0
    n_pairs_considered:   int = 0
    n_pairs_viable:       int = 0
    pair_results:         list[PairResult] = field(default_factory=list)

    @property
    def median_reversion_pct(self) -> float:
        if not self.pair_results:
            return 0.0
        return statistics.median(p.reversion_pct for p in self.pair_results)

    @property
    def mean_reversion_pct(self) -> float:
        if not self.pair_results:
            return 0.0
        return statistics.mean(p.reversion_pct for p in self.pair_results)


def fetch_markets() -> list[dict]:
    out: list[dict] = []
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS + 7)
    for tag in CATEGORY_TAGS:
        for page in range(MAX_GAMMA_PAGES):
            params = {
                "limit":     str(PAGE_SIZE),
                "offset":    str(page * PAGE_SIZE),
                "tag_slug":  tag,
                "order":     "endDate",
                "ascending": "false",
            }
            try:
                r = requests.get(GAMMA_EVENTS_URL, params=params, timeout=15)
                r.raise_for_status()
                events = r.json()
            except (requests.RequestException, ValueError):
                break
            if not events:
                break
            for e in events:
                for m in e.get("markets", []):
                    m.setdefault("_event_title", e.get("title", ""))
                    m.setdefault("_tag", tag)
                    out.append(m)
            time.sleep(SLEEP_BETWEEN_REQS)
    # dedupe
    seen: set[str] = set()
    out2: list[dict] = []
    for m in out:
        mid = str(m.get("id") or m.get("conditionId") or m.get("slug"))
        if mid in seen:
            continue
        seen.add(mid)
        out2.append(m)
    return out2[:MAX_MARKETS_TO_FETCH]


def fetch_history(token_id: str) -> list[tuple[datetime, float]]:
    end_ts = int(datetime.now(timezone.utc).timestamp())
    start_ts = end_ts - LOOKBACK_DAYS * 86400
    params = {
        "market":   token_id,
        "interval": "1h",
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


def first_token_id(m: dict) -> str | None:
    raw = m.get("clobTokenIds") or "[]"
    try:
        ids = json.loads(raw) if isinstance(raw, str) else raw
    except json.JSONDecodeError:
        return None
    if not isinstance(ids, list) or not ids:
        return None
    return str(ids[0]) if ids[0] not in (0, "0") else None


def align_pair(a: list[tuple[datetime, float]],
               b: list[tuple[datetime, float]]) -> tuple[list[float], list[float], list[datetime]]:
    """Inner-join two price series on timestamp (1h bucket alignment)."""
    b_map = {ts.replace(minute=0, second=0, microsecond=0): px for ts, px in b}
    xs: list[float] = []
    ys: list[float] = []
    ts: list[datetime] = []
    for at, ap in a:
        key = at.replace(minute=0, second=0, microsecond=0)
        bp = b_map.get(key)
        if bp is None:
            continue
        xs.append(ap)
        ys.append(bp)
        ts.append(key)
    return xs, ys, ts


def pearson_beta(xs: list[float], ys: list[float]) -> tuple[float, float] | None:
    """Returns (pearson_rho, beta) where beta = cov / var(y)."""
    n = len(xs)
    if n < 10:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
    var_x = sum((xs[i] - mx) ** 2 for i in range(n))
    var_y = sum((ys[i] - my) ** 2 for i in range(n))
    if var_x <= 0 or var_y <= 0:
        return None
    rho = num / math.sqrt(var_x * var_y)
    beta = num / var_y
    return rho, beta


def find_reversion_episodes(spread: list[float], ts: list[datetime]) -> tuple[int, int]:
    """Return (n_high_z_episodes, n_reverted_within_window).
    z = (spread - mean) / stdev. Episode: index where |z| crosses
    Z_ENTRY for the first time after returning below Z_EXIT.
    Reverted: within REVERSION_WINDOW_H next hours, |z| drops below
    Z_EXIT at least once."""
    n = len(spread)
    if n < 50:
        return 0, 0
    mean = sum(spread) / n
    var = sum((s - mean) ** 2 for s in spread) / n
    std = math.sqrt(var) if var > 0 else 0.0
    if std <= 0:
        return 0, 0
    zs = [(s - mean) / std for s in spread]

    episodes: list[int] = []
    in_episode = False
    for i, z in enumerate(zs):
        if not in_episode and abs(z) > Z_ENTRY_THRESHOLD:
            episodes.append(i)
            in_episode = True
        elif in_episode and abs(z) < Z_EXIT_THRESHOLD:
            in_episode = False

    n_reverted = 0
    for idx in episodes:
        # look at next REVERSION_WINDOW_H hours (assume 1 bucket = 1h)
        for j in range(idx + 1, min(idx + 1 + REVERSION_WINDOW_H, n)):
            if abs(zs[j]) < Z_EXIT_THRESHOLD:
                n_reverted += 1
                break

    return len(episodes), n_reverted


def main() -> int:
    started = time.time()
    out_dir = Path("data/research")
    out_dir.mkdir(parents=True, exist_ok=True)

    raw_markets = fetch_markets()
    res = ValidationResult()
    res.n_markets = len(raw_markets)
    print(f"fetched {len(raw_markets)} candidate markets")

    histories: list[MarketHist] = []
    for i, m in enumerate(raw_markets):
        if time.time() - started > HARD_RUNTIME_SECS * 0.5:
            print(f"  history fetch cap; stopped at {i}/{len(raw_markets)}")
            break
        tid = first_token_id(m)
        if not tid:
            continue
        h = fetch_history(tid)
        if len(h) < MIN_HISTORY_POINTS:
            time.sleep(SLEEP_BETWEEN_REQS / 4)
            continue
        histories.append(MarketHist(
            slug=m.get("slug", ""), tid=tid, history=h,
        ))
        if (i + 1) % 10 == 0:
            print(f"  fetched history for {len(histories)} markets", flush=True)
        time.sleep(SLEEP_BETWEEN_REQS)

    res.n_with_history = len(histories)
    print(f"computed history for {len(histories)} markets")

    if len(histories) < 5:
        print()
        print("[RED] KILL: too few markets with usable price history.")
        return 3

    # Pairwise correlations
    pair_results_raw: list[tuple[float, float, MarketHist, MarketHist, list[float], list[datetime]]] = []
    for i in range(len(histories)):
        for j in range(i + 1, len(histories)):
            if time.time() - started > HARD_RUNTIME_SECS:
                break
            a = histories[i]
            b = histories[j]
            xs, ys, ts = align_pair(a.history, b.history)
            if len(xs) < MIN_HISTORY_POINTS:
                continue
            pearson = pearson_beta(xs, ys)
            if pearson is None:
                continue
            rho, beta = pearson
            if abs(rho) < MIN_CORRELATION:
                continue
            spread = [xs[k] - beta * ys[k] for k in range(len(xs))]
            res.n_pairs_considered += 1
            pair_results_raw.append((abs(rho), beta, a, b, spread, ts))

    # Top by |rho|
    pair_results_raw.sort(key=lambda t: t[0], reverse=True)
    for rho_abs, beta, a, b, spread, ts in pair_results_raw[:TOP_PAIRS_BY_RHO]:
        n_eps, n_rev = find_reversion_episodes(spread, ts)
        if n_eps < 3:
            continue
        rev_pct = n_rev / n_eps * 100.0
        res.pair_results.append(PairResult(
            a_slug=        a.slug,
            b_slug=        b.slug,
            rho=           rho_abs,
            beta=          beta,
            n_episodes=    n_eps,
            n_reverted=    n_rev,
            reversion_pct= rev_pct,
        ))
    res.n_pairs_viable = len(res.pair_results)

    # Artefacts
    csv_path = out_dir / "05_stat_arb_phase0.csv"
    with csv_path.open("w", encoding="utf-8") as f:
        f.write("a_slug,b_slug,rho,beta,n_episodes,n_reverted,reversion_pct\n")
        for pr in res.pair_results:
            f.write(f"{pr.a_slug},{pr.b_slug},{pr.rho:.4f},{pr.beta:+.4f},"
                    f"{pr.n_episodes},{pr.n_reverted},{pr.reversion_pct:.1f}\n")

    summary = {
        "n_markets":          res.n_markets,
        "n_with_history":     res.n_with_history,
        "n_pairs_considered": res.n_pairs_considered,
        "n_pairs_viable":     res.n_pairs_viable,
        "median_reversion_pct": round(res.median_reversion_pct, 2),
        "mean_reversion_pct":   round(res.mean_reversion_pct, 2),
        "min_correlation":      MIN_CORRELATION,
        "z_entry_threshold":    Z_ENTRY_THRESHOLD,
        "reversion_window_h":   REVERSION_WINDOW_H,
    }
    json_path = out_dir / "05_stat_arb_phase0.json"
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print()
    print("=" * 64)
    print("STRATEGY 05 / PHASE 0 -- STATISTICAL ARBITRAGE VALIDATION")
    print("=" * 64)
    print(f"Markets with history:    {res.n_with_history}")
    print(f"Pairs considered:        {res.n_pairs_considered}")
    print(f"Viable pairs (>=3 eps):  {res.n_pairs_viable}")
    print(f"Median reversion rate:   {res.median_reversion_pct:.1f}%")
    print(f"Mean reversion rate:     {res.mean_reversion_pct:.1f}%")
    print()

    if res.n_pairs_viable < 5:
        print("[RED] KILL: fewer than 5 viable pairs with high correlation")
        print("           and >=3 z-score episodes in the lookback window.")
        return 3

    med = res.median_reversion_pct
    if med >= 70.0:
        print(f"[GREEN] GREEN-LIGHT Strategy 05 to Phase 1:")
        print(f"        median reversion {med:.1f}% >= 70% across "
              f"{res.n_pairs_viable} viable pairs.")
        return 0
    if med >= 50.0:
        print(f"[YELLOW] MARGINAL: median reversion {med:.1f}% in 50-70% band.")
        print("         Tighten correlation threshold; rebuild pair universe.")
        return 1
    print(f"[RED] KILL: median reversion {med:.1f}% < 50% — pairs not")
    print("           mean-reverting at our chosen z thresholds.")
    return 3


if __name__ == "__main__":
    sys.exit(main())
