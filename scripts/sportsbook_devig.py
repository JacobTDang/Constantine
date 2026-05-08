"""EDGE-C — Sportsbook devig overlay sidecar.

Pulls Pinnacle (and other sharps') moneylines from TheOddsAPI, computes
no-vig fair probabilities, matches against Polymarket sports winner
markets, and writes a JSON snapshot of the cross-platform edge to
`data/sportsbook_devig.json` for the Rust runner to consume.

Mechanic:
  - Pinnacle's lines are the closest thing to "true" odds in sports.
  - Their vig is ~1-3% baked in. Devig: divide each side's implied prob
    by the sum of all sides' implied probs.
  - Polymarket prices reflect retail + Polymarket-specialist flow.
  - When Polymarket diverges from Pinnacle's no-vig line by >= 3%,
    take the side Pinnacle prefers.

Required env (optional — sidecar runs as a no-op if missing):
  THE_ODDS_API_KEY    — get a free tier at https://the-odds-api.com/

Usage:
  python scripts/sportsbook_devig.py            # one-shot
  python scripts/sportsbook_devig.py --watch    # refresh every 5 min
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import requests

THE_ODDS_BASE = "https://api.the-odds-api.com/v4"
GAMMA_EVENTS  = "https://gamma-api.polymarket.com/events"
OUTPUT_PATH   = Path("data/sportsbook_devig.json")

# Sports we cover. Add more by appending TheOddsAPI sport keys.
COVERED_SPORTS = [
    ("basketball_nba", ["nba", "basketball"]),
    ("americanfootball_nfl", ["nfl", "football"]),
    ("baseball_mlb", ["mlb", "baseball"]),
    ("icehockey_nhl", ["nhl", "hockey"]),
]

# Bookmaker rank for fair-odds estimation. Pinnacle is gold; fall back
# to Bet365 / DraftKings if Pinnacle's not posted.
SHARP_BOOKS = ["pinnacle", "bet365", "draftkings"]

MIN_EDGE = 0.03                 # 3% post-devig edge to emit a row
REFRESH_SECS_DEFAULT = 300

log = logging.getLogger("sportsbook_devig")


@dataclass
class GameLine:
    """One game's Pinnacle-derived no-vig probabilities."""
    sport_key:   str             # "basketball_nba"
    home_team:   str
    away_team:   str
    commence_ms: int
    home_prob:   float           # devigged win prob for home
    away_prob:   float
    book_used:   str


@dataclass
class DevigRow:
    """One Polymarket market with a matched Pinnacle line and computed edge."""
    market_id:        str
    condition_id:     str
    question:         str
    sport_key:        str
    home_team:        str
    away_team:        str
    matched_book:     str
    yes_token_id:     str
    no_token_id:      str
    end_date_ms:      int
    yes_ask:          float
    yes_bid:          float
    fair_yes:         float    # devigged Pinnacle prob for YES side
    edge_yes:         float    # fair_yes - yes_ask  (positive = BUY YES)
    edge_no:          float    # (1 - fair_yes) - (1 - yes_bid)
    liquidity:        float
    volume:           float


# ── TheOddsAPI fetch ─────────────────────────────────────────────────────────

def fetch_sportsbook_lines(api_key: str) -> list[GameLine]:
    out: list[GameLine] = []
    for sport_key, _ in COVERED_SPORTS:
        url = f"{THE_ODDS_BASE}/sports/{sport_key}/odds"
        params = {
            "apiKey":     api_key,
            "regions":    "us,eu",
            "markets":    "h2h",          # head-to-head moneyline
            "oddsFormat": "decimal",
        }
        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            games = r.json()
        except (requests.RequestException, ValueError) as exc:
            log.warning("odds fetch failed for %s: %s", sport_key, exc)
            continue
        for g in games:
            line = parse_game(g, sport_key)
            if line:
                out.append(line)
        time.sleep(0.4)
    log.info("pulled %d game lines from TheOddsAPI", len(out))
    return out


def parse_game(g: dict, sport_key: str) -> GameLine | None:
    home = g.get("home_team") or ""
    away = g.get("away_team") or ""
    commence = g.get("commence_time") or ""
    if not (home and away and commence):
        return None
    try:
        commence_ms = int(
            datetime.fromisoformat(commence.replace("Z", "+00:00"))
            .timestamp() * 1000
        )
    except (TypeError, ValueError):
        return None

    # Find the first sharp book that has both home + away h2h prices
    for book_key in SHARP_BOOKS:
        for book in g.get("bookmakers", []):
            if book.get("key", "") != book_key:
                continue
            h2h = next(
                (m for m in book.get("markets", []) if m.get("key") == "h2h"),
                None,
            )
            if not h2h:
                continue
            outcomes = h2h.get("outcomes", [])
            home_dec = next(
                (o.get("price") for o in outcomes if o.get("name") == home),
                None,
            )
            away_dec = next(
                (o.get("price") for o in outcomes if o.get("name") == away),
                None,
            )
            if not (home_dec and away_dec):
                continue
            try:
                home_dec = float(home_dec)
                away_dec = float(away_dec)
            except (TypeError, ValueError):
                continue
            if home_dec <= 1.0 or away_dec <= 1.0:
                continue
            home_imp = 1.0 / home_dec
            away_imp = 1.0 / away_dec
            total = home_imp + away_imp
            if total <= 0.5 or total > 1.20:
                continue
            return GameLine(
                sport_key   = sport_key,
                home_team   = home,
                away_team   = away,
                commence_ms = commence_ms,
                home_prob   = home_imp / total,
                away_prob   = away_imp / total,
                book_used   = book_key,
            )
    return None


# ── Polymarket fetch + matching ──────────────────────────────────────────────

def fetch_polymarket_sports(slugs: list[str]) -> list[dict]:
    """Pull active multi-leg or single-leg sports markets via Gamma."""
    out: list[dict] = []
    for slug in slugs:
        url = GAMMA_EVENTS
        params = {
            "active":    "true",
            "closed":    "false",
            "tag_slug":  slug,
            "limit":     "300",
            "order":     "volume",
            "ascending": "false",
        }
        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            events = r.json()
        except (requests.RequestException, ValueError) as exc:
            log.warning("gamma fetch failed for %s: %s", slug, exc)
            continue
        for e in events:
            for m in e.get("markets", []):
                m["_event_slug"] = slug
                out.append(m)
        time.sleep(0.3)
    log.info("pulled %d candidate Polymarket sports markets", len(out))
    return out


# Heuristic team-name match. Polymarket questions look like
# "Will the Warriors beat the Lakers?" or "Lakers vs Warriors winner".
TEAM_TOKEN_RE = re.compile(r"[A-Za-z']+(?:\s+[A-Za-z']+)*")


def match_market(market: dict, lines: list[GameLine]) -> GameLine | None:
    q = market.get("question", "") or ""
    if not q:
        return None
    q_lower = q.lower()
    end_iso = market.get("endDate") or market.get("end_date_iso") or ""
    try:
        end_ms = int(
            datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
            .timestamp() * 1000
        ) if end_iso else 0
    except (TypeError, ValueError):
        end_ms = 0

    best: GameLine | None = None
    for line in lines:
        # Both team last-words present, end-date within +/- 36h of commence
        home_last = line.home_team.split()[-1].lower()
        away_last = line.away_team.split()[-1].lower()
        if home_last in q_lower and away_last in q_lower:
            if end_ms == 0 or abs(end_ms - line.commence_ms) <= 36 * 3_600_000:
                best = line
                break
    return best


def classify_yes_team(question: str, line: GameLine) -> str | None:
    """Return 'home' or 'away' based on which team the market's YES side
    represents. Look for "Will X win" or "X wins" patterns."""
    q = (question or "").lower()
    home_last = line.home_team.split()[-1].lower()
    away_last = line.away_team.split()[-1].lower()
    # Trivial heuristic: if home name appears first AND with a "win" verb,
    # YES = home. Else if away first, YES = away. Reasonable default for
    # standard "Will <Team> win against ..." Polymarket phrasing.
    home_idx = q.find(home_last)
    away_idx = q.find(away_last)
    if home_idx == -1 or away_idx == -1:
        return None
    if home_idx < away_idx:
        return "home"
    return "away"


# ── Devig + edge ─────────────────────────────────────────────────────────────

def build_rows(lines: list[GameLine], markets: list[dict]) -> list[DevigRow]:
    rows: list[DevigRow] = []
    for m in markets:
        line = match_market(m, lines)
        if not line:
            continue
        side = classify_yes_team(m.get("question", "") or "", line)
        if side is None:
            continue
        fair_yes = line.home_prob if side == "home" else line.away_prob

        # Polymarket prices
        try:
            yes_ask = float(m.get("bestAsk") or 0)
            yes_bid = float(m.get("bestBid") or 0)
        except (TypeError, ValueError):
            yes_ask = yes_bid = 0.0
        if yes_ask <= 0 or yes_ask >= 1 or yes_bid <= 0:
            continue

        token_ids_raw = m.get("clobTokenIds")
        if isinstance(token_ids_raw, str):
            try:
                token_ids = json.loads(token_ids_raw)
            except json.JSONDecodeError:
                continue
        else:
            token_ids = token_ids_raw or []
        if len(token_ids) != 2:
            continue

        edge_yes = fair_yes - yes_ask
        edge_no  = (1.0 - fair_yes) - (1.0 - yes_bid)

        if max(abs(edge_yes), abs(edge_no)) < MIN_EDGE:
            continue

        end_iso = m.get("endDate", "") or ""
        try:
            end_ms = int(datetime.fromisoformat(
                end_iso.replace("Z", "+00:00")
            ).timestamp() * 1000) if end_iso else 0
        except (TypeError, ValueError):
            end_ms = 0

        rows.append(DevigRow(
            market_id    = str(m.get("id", "")),
            condition_id = m.get("conditionId", "") or m.get("condition_id", ""),
            question     = m.get("question", "") or "",
            sport_key    = line.sport_key,
            home_team    = line.home_team,
            away_team    = line.away_team,
            matched_book = line.book_used,
            yes_token_id = str(token_ids[0]),
            no_token_id  = str(token_ids[1]),
            end_date_ms  = end_ms,
            yes_ask      = yes_ask,
            yes_bid      = yes_bid,
            fair_yes     = fair_yes,
            edge_yes     = edge_yes,
            edge_no      = edge_no,
            liquidity    = float(m.get("liquidity") or 0),
            volume       = float(m.get("volume") or 0),
        ))
    log.info("computed %d devig rows above %.0f%% edge", len(rows), MIN_EDGE * 100)
    return rows


# ── IO ───────────────────────────────────────────────────────────────────────

def write_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2))
    tmp.replace(path)


def run_once() -> int:
    api_key = os.environ.get("THE_ODDS_API_KEY", "").strip()
    now_ms  = int(datetime.now(timezone.utc).timestamp() * 1000)

    if not api_key:
        log.warning("THE_ODDS_API_KEY not set — emitting empty snapshot")
        write_atomic(OUTPUT_PATH, {
            "generated_at_ms": now_ms,
            "n_markets":       0,
            "rows":            [],
            "note":            "THE_ODDS_API_KEY not set",
        })
        return 0

    lines = fetch_sportsbook_lines(api_key)
    slugs = [tag for _, slugs in COVERED_SPORTS for tag in slugs]
    markets = fetch_polymarket_sports(slugs)
    rows = build_rows(lines, markets)

    write_atomic(OUTPUT_PATH, {
        "generated_at_ms": now_ms,
        "n_markets":       len(rows),
        "rows":            [asdict(r) for r in rows],
    })
    log.info("wrote %s with %d rows", OUTPUT_PATH, len(rows))
    return 0


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--watch", action="store_true", help="refresh on cadence")
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
