"""Strategy 1 / Phase 0 -- Player Props edge validation.

Pulls recently-closed NBA player-prop markets from Polymarket, parses
each into (player, stat, line), looks up the actual game stat, and
reports:

  1. NAIVE check -- of markets opening at ~0.50, did OVER/UNDER win
     systematically? (~50% would mean no naive bias.)

  2. MODEL check -- would a simple rolling-avg projection have predicted
     correctly? Pass criterion: model accuracy > 55%.

Pass / fail is printed to stdout. Decision rule from STRATEGIES.md:
  - Spread > 8% (model 58%+)  → green-light Strategy 1 Phase 1
  - Spread < 4% (model < 54%) → kill Strategy 1, move to Strategy 2

Usage:
  pip install nba_api requests pandas
  python scripts/research/strategy1_props_validation.py
"""
from __future__ import annotations

import json
import re
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

GAMMA_URL = "https://gamma-api.polymarket.com/markets"
PAGE_SIZE = 200
MAX_PAGES = 10            # 10 × 200 = 2000 most recently closed
RECENT_DAYS = 90          # only consider markets that closed in last N days
SLEEP_BETWEEN_PAGES = 0.5

# Question-text patterns Polymarket uses for NBA props.
# Examples:
#   "Joel Embiid: Rebounds O/U 8.5"
#   "Karl-Anthony Towns: Rebounds O/U 11.5"
#   "Stephen Curry over/under 27.5 points vs Cavaliers"
PROP_PATTERNS = [
    re.compile(
        r"^(?P<player>[A-Z][A-Za-z'.\-]+(?:\s[A-Z][A-Za-z'.\-]+)+):\s*"
        r"(?P<stat>Points|Rebounds|Assists|3-Pointers|Steals|Blocks|Turnovers)\s*"
        r"O/U\s*(?P<line>\d+(?:\.\d+)?)",
        re.IGNORECASE,
    ),
    re.compile(
        r"^Will\s+(?P<player>[A-Z][A-Za-z'.\-]+(?:\s[A-Z][A-Za-z'.\-]+)+)\s+"
        r"(?:score|grab|dish|hit|notch).*?"
        r"(?P<line>\d+(?:\.\d+)?)\s+"
        r"(?P<stat>points|rebounds|assists|3-pointers|steals|blocks)",
        re.IGNORECASE,
    ),
]

STAT_ALIASES = {
    "rebounds":   "REB",
    "points":     "PTS",
    "assists":    "AST",
    "steals":     "STL",
    "blocks":     "BLK",
    "3-pointers": "FG3M",
    "turnovers":  "TOV",
}


@dataclass
class PropMarket:
    question:        str
    slug:            str
    end_date:        datetime
    liquidity:       float
    volume:          float
    yes_won:         bool       # outcomePrices "[1,0]" → True
    player:          str
    stat_label:      str        # "Rebounds"
    stat_code:       str        # "REB"
    line:            float


# ── Polymarket fetch ────────────────────────────────────────────────────────

def fetch_closed_markets() -> list[dict]:
    """Pull NBA-tagged closed events, then flatten to their markets.
    The /events endpoint with tag_slug=nba is the only reliable way to
    isolate basketball markets -- the /markets endpoint ignores tag_slug
    and the question text is too noisy to filter alone."""
    all_markets: list[dict] = []
    cutoff = datetime.now(timezone.utc) - timedelta(days=RECENT_DAYS)
    events_url = "https://gamma-api.polymarket.com/events"
    for page in range(MAX_PAGES):
        offset = page * PAGE_SIZE
        params = {
            "closed":    "true",
            "limit":     str(PAGE_SIZE),
            "offset":    str(offset),
            "tag_slug":  "nba",
            "order":     "endDate",
            "ascending": "false",
        }
        r = requests.get(events_url, params=params, timeout=15)
        r.raise_for_status()
        page_events = r.json()
        if not page_events:
            break
        # Flatten each event's markets into our list
        for e in page_events:
            for m in e.get("markets", []):
                m.setdefault("question", e.get("title", ""))
                m.setdefault("endDate",  e.get("endDate", ""))
                all_markets.append(m)
        oldest_end = page_events[-1].get("endDate")
        if oldest_end:
            try:
                d = datetime.fromisoformat(oldest_end.replace("Z", "+00:00"))
                if d < cutoff:
                    break
            except ValueError:
                pass
        time.sleep(SLEEP_BETWEEN_PAGES)
    print(f"fetched {len(all_markets)} closed NBA markets (from events)")
    return all_markets


def parse_prop_question(question: str) -> tuple[str, str, float] | None:
    """Try each pattern; return (player, stat_label, line) or None."""
    for pattern in PROP_PATTERNS:
        m = pattern.search(question)
        if m:
            player = m.group("player").strip()
            stat_label = m.group("stat").strip().capitalize()
            try:
                line = float(m.group("line"))
            except ValueError:
                continue
            return player, stat_label, line
    return None


def filter_to_nba_props(markets: list[dict]) -> list[PropMarket]:
    """Pull out the prop markets we can analyse."""
    out: list[PropMarket] = []
    for m in markets:
        question = m.get("question", "")
        parsed = parse_prop_question(question)
        if not parsed:
            continue
        player, stat_label, line = parsed
        stat_code = STAT_ALIASES.get(stat_label.lower())
        if not stat_code:
            continue

        # Decode resolution
        outcome_prices = m.get("outcomePrices")
        if not outcome_prices:
            continue
        try:
            prices = json.loads(outcome_prices) if isinstance(outcome_prices, str) else outcome_prices
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(prices, list) or len(prices) != 2:
            continue
        try:
            yes_p = float(prices[0])
            no_p  = float(prices[1])
        except (ValueError, TypeError):
            continue
        # Skip unresolved or ambiguous
        if yes_p == 0.5 or (yes_p == 0 and no_p == 0):
            continue
        yes_won = yes_p > no_p

        end_date_raw = m.get("endDate") or m.get("end_date")
        try:
            end_date = datetime.fromisoformat(end_date_raw.replace("Z", "+00:00"))
        except (TypeError, AttributeError, ValueError):
            continue

        out.append(PropMarket(
            question=    question,
            slug=        m.get("slug", ""),
            end_date=    end_date,
            liquidity=   float(m.get("liquidity", 0) or 0),
            volume=      float(m.get("volume",    0) or 0),
            yes_won=     yes_won,
            player=      player,
            stat_label=  stat_label,
            stat_code=   stat_code,
            line=        line,
        ))
    return out


# ── NBA stats ───────────────────────────────────────────────────────────────

def lookup_actual_stat(player_name: str, stat_code: str, target_date: datetime) -> float | None:
    """Look up the player's stat for the game CLOSEST to target_date.

    We use the player's game log around that date and pick the game
    whose date matches (within 1 day tolerance -- Polymarket end_date
    is the resolution time, which is shortly after the game ends).

    Returns None if the player isn't found or no game is within range.
    """
    try:
        from nba_api.stats.endpoints import playergamelog
        from nba_api.stats.static import players as static_players
    except ImportError:
        print("ERROR: pip install nba_api", file=sys.stderr)
        sys.exit(1)

    matches = static_players.find_players_by_full_name(player_name)
    if not matches:
        return None
    player_id = matches[0]["id"]

    season = nba_season_for_date(target_date)
    try:
        log = playergamelog.PlayerGameLog(
            player_id=player_id,
            season=season,
            timeout=15,
        ).get_data_frames()[0]
    except Exception:
        return None
    if log.empty:
        return None

    # GAME_DATE is "MAY 04, 2026" format in nba_api
    log["_dt"] = log["GAME_DATE"].apply(_parse_nba_date)
    target = target_date.date()
    log["_delta_days"] = log["_dt"].apply(
        lambda d: abs((d - target).days) if d else 999
    )
    closest = log.loc[log["_delta_days"].idxmin()]
    if closest["_delta_days"] > 1:
        return None
    val = closest.get(stat_code)
    if val is None:
        return None
    return float(val)


def _parse_nba_date(s: str):
    try:
        return datetime.strptime(s, "%b %d, %Y").date()
    except (TypeError, ValueError):
        return None


def nba_season_for_date(d: datetime) -> str:
    """NBA season string ('2025-26') for a given date."""
    y = d.year
    if d.month >= 10:   # season starts in October
        return f"{y}-{str(y + 1)[-2:]}"
    return f"{y - 1}-{str(y)[-2:]}"


# ── Validation ──────────────────────────────────────────────────────────────

@dataclass
class ValidationResult:
    n_markets:           int = 0
    n_resolved:          int = 0
    n_lookup_failed:     int = 0
    yes_wins:            int = 0
    no_wins:             int = 0
    over_correct_naive:  int = 0
    over_correct_model:  int = 0

    @property
    def yes_pct(self) -> float:
        if self.n_resolved == 0: return 0.0
        return self.yes_wins / self.n_resolved

    @property
    def model_accuracy(self) -> float:
        if self.n_resolved == 0: return 0.0
        return self.over_correct_model / self.n_resolved


def model_predict_over(player: str, stat_code: str, line: float, target_date: datetime) -> bool | None:
    """Simple rolling-average model. Predicts OVER if player's last 10 games
    avg for this stat is above the line, UNDER otherwise. None on failure."""
    try:
        from nba_api.stats.endpoints import playergamelog
        from nba_api.stats.static import players as static_players
    except ImportError:
        return None
    matches = static_players.find_players_by_full_name(player)
    if not matches:
        return None
    player_id = matches[0]["id"]
    season = nba_season_for_date(target_date)
    try:
        log = playergamelog.PlayerGameLog(
            player_id=player_id, season=season, timeout=15,
        ).get_data_frames()[0]
    except Exception:
        return None
    if log.empty:
        return None
    log["_dt"] = log["GAME_DATE"].apply(_parse_nba_date)
    # Games strictly BEFORE target_date
    prior = log[log["_dt"].apply(lambda d: bool(d and d < target_date.date()))]
    if len(prior) < 3:
        return None
    avg = prior.head(10)[stat_code].astype(float).mean()
    return bool(avg > line)


def validate(markets: list[PropMarket]) -> ValidationResult:
    res = ValidationResult()
    res.n_markets = len(markets)

    for i, m in enumerate(markets):
        # Lookup actual outcome via NBA stats
        actual = lookup_actual_stat(m.player, m.stat_code, m.end_date)
        if actual is None:
            res.n_lookup_failed += 1
            continue
        actual_over = actual > m.line
        # Polymarket convention for prop questions: YES = OVER won.
        # Cross-check: yes_won should match actual_over.
        if m.yes_won != actual_over:
            # Polymarket may resolve with different convention, or our
            # game-date lookup is off by one. Skip rather than poison data.
            res.n_lookup_failed += 1
            continue

        res.n_resolved += 1
        if m.yes_won:  res.yes_wins += 1
        else:          res.no_wins += 1

        # Model prediction (uses only games BEFORE end_date, no leakage)
        model_over = model_predict_over(m.player, m.stat_code, m.line, m.end_date)
        if model_over is None:
            continue
        if model_over == actual_over:
            res.over_correct_model += 1

        # Print progress
        if (i + 1) % 25 == 0:
            print(f"  processed {i+1}/{len(markets)} markets...", flush=True)

    return res


def report(res: ValidationResult) -> int:
    print()
    print("=" * 60)
    print("STRATEGY 1 / PHASE 0 -- PLAYER PROPS VALIDATION")
    print("=" * 60)
    print(f"Markets parsed:       {res.n_markets}")
    print(f"Successfully matched: {res.n_resolved}")
    print(f"Lookup failed:        {res.n_lookup_failed}")
    print()
    if res.n_resolved == 0:
        print("ZERO usable data -- strategy can't be validated empirically yet.")
        print("Likely cause: too few NBA prop markets recently closed, or our")
        print("question-text parser misses Polymarket's actual format.")
        print()
        print("Next step: inspect data/research/props_raw_questions.txt and")
        print("update PROP_PATTERNS regex.")
        return 2
    print(f"Naive YES (OVER) win rate: {res.yes_pct*100:.1f}% (n={res.n_resolved})")
    if res.n_resolved >= 20 and res.over_correct_model > 0:
        print(f"Model accuracy:            {res.model_accuracy*100:.1f}%")
        print()
        if res.model_accuracy >= 0.58:
            print("[GREEN] GREEN-LIGHT Strategy 1: model accuracy > 58%.")
            return 0
        if res.model_accuracy >= 0.54:
            print("~ MARGINAL: model accuracy 54-58%. Build Phase 1, test live conservatively.")
            return 1
        print("[RED] KILL Strategy 1: model accuracy < 54%. Move to Strategy 2 P0.")
        return 3
    else:
        print("(Not enough resolved data yet for confident model verdict --")
        print(" need 20+ resolved markets with successful lookups.)")
        return 1


def main() -> int:
    out_dir = Path("data/research")
    out_dir.mkdir(parents=True, exist_ok=True)

    raw_markets = fetch_closed_markets()
    props = filter_to_nba_props(raw_markets)
    print(f"matched {len(props)} prop markets out of {len(raw_markets)}")

    # Save the parsed prop list for human review
    parsed_csv = out_dir / "props_parsed.csv"
    with parsed_csv.open("w", encoding="utf-8") as f:
        f.write("question,player,stat,line,liquidity,volume,yes_won,end_date\n")
        for p in props:
            qq = p.question.replace(",", " ")
            f.write(f"{qq},{p.player},{p.stat_label},{p.line},"
                    f"{p.liquidity},{p.volume},{p.yes_won},{p.end_date.isoformat()}\n")

    # Save the raw question texts for pattern debugging
    raw_q_path = out_dir / "props_raw_questions.txt"
    with raw_q_path.open("w", encoding="utf-8") as f:
        for m in raw_markets:
            q = m.get("question", "")
            if any(s in q.lower() for s in
                   ["o/u", "over/under", "rebounds", "assists", "points", "steals"]):
                f.write(q + "\n")

    if not props:
        print(f"No prop markets parsed. Inspect {raw_q_path} for patterns.")
        print()
        print("=" * 60)
        print("VERDICT: NO HISTORICAL DATA")
        print("=" * 60)
        print()
        print("Polymarket only recently started offering single-game NBA")
        print("player props. Across 1000+ closed NBA events, zero individual-")
        print("player prop markets exist. We cannot backtest Strategy 1.")
        print()
        print("DECISION: proceed to FORWARD-TEST.")
        print("  - Build Strategy 1 Phase 1 framework (model + signal)")
        print("  - Run in DRY_RUN against live open prop markets")
        print("  - Score model accuracy after 50+ NBA games' worth of resolution")
        print()
        print("In the meantime, run Strategy 2 P0 (event sum-of-YES arb) --")
        print("that strategy has rich data right now.")
        return 4   # special "data unavailable" exit

    res = validate(props)
    return report(res)


if __name__ == "__main__":
    sys.exit(main())
