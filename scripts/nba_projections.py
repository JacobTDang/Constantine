"""Strategy 1 — NBA player projection sidecar.

Periodically computes a `p_over` projection for every active player on
every Polymarket NBA prop market we'd consider trading. Writes the
result to `data/nba_projections.json` for the Rust bot to consume.

Model (intentionally simple — see STRATEGIES.md):
  p_over = sigmoid( (rolling_avg(stat, 10g) - line) / sigma )
  where:
    rolling_avg = last-10-game average (capped to season)
    sigma       = stddev of last-30-game stat sample (or 1.0 default)

  Adjustments:
    - Days since last game: heavy discount if < 1 (back-to-back fatigue)
    - Opponent defensive rank: discount when opponent is top-10 vs that stat

This is a baseline. Better models (XGBoost on richer features) are a
Phase 3 optimization.

Usage:
  python scripts/nba_projections.py                    # one-shot
  python scripts/nba_projections.py --watch            # refresh every 5 min
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import re
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

GAMMA_EVENTS = "https://gamma-api.polymarket.com/events"
OUTPUT_PATH  = Path("data/nba_projections.json")

# Polymarket sometimes uses different stat labels; normalise to NBA API
# field names.
STAT_TO_NBA = {
    "points":     "PTS",
    "rebounds":   "REB",
    "assists":    "AST",
    "steals":     "STL",
    "blocks":     "BLK",
    "3-pointers": "FG3M",
    "turnovers":  "TOV",
}

PROP_PATTERN = re.compile(
    r"^(?P<player>[A-Z][A-Za-z'.\-]+(?:\s[A-Z][A-Za-z'.\-]+)+):\s*"
    r"(?P<stat>Points|Rebounds|Assists|3-Pointers|Steals|Blocks|Turnovers)\s*"
    r"O/U\s*(?P<line>\d+(?:\.\d+)?)",
    re.IGNORECASE,
)

ROLLING_GAMES   = 10
SIGMA_GAMES     = 30
DEFAULT_SIGMA   = 4.0
MIN_GAMES       = 5
DEFAULT_OUT_PCT = 0.50    # if model insufficient → no edge claimed

log = logging.getLogger("nba_projections")


# ── Polymarket prop discovery ──────────────────────────────────────────────

@dataclass
class PolyProp:
    market_id:    str
    condition_id: str
    question:     str
    player_name:  str
    stat_label:   str
    stat_code:    str       # NBA API field
    line:         float
    yes_token_id: str
    no_token_id:  str
    yes_ask:      float
    yes_bid:      float
    end_date_ms:  int
    liquidity:    float
    volume:       float


def fetch_open_nba_prop_markets() -> list[PolyProp]:
    """Hit Gamma /events with tag=nba, flatten to per-player prop markets."""
    out: list[PolyProp] = []
    seen = set()
    for offset in range(0, 1000, 200):
        params = {
            "closed":    "false",
            "active":    "true",
            "limit":     200,
            "offset":    offset,
            "tag_slug":  "nba",
            "order":     "endDate",
            "ascending": "true",
        }
        try:
            r = requests.get(GAMMA_EVENTS, params=params, timeout=20)
            r.raise_for_status()
            events = r.json()
        except Exception as e:
            log.warning("gamma fetch failed: %s", e)
            break
        if not events:
            break
        for ev in events:
            for m in ev.get("markets", []):
                p = parse_prop_market(m, ev)
                if p and p.market_id not in seen:
                    seen.add(p.market_id)
                    out.append(p)
        time.sleep(0.4)
    log.info("found %d open NBA prop markets", len(out))
    return out


def parse_prop_market(market: dict, event: dict) -> PolyProp | None:
    question = market.get("question") or event.get("title", "")
    m = PROP_PATTERN.search(question)
    if not m:
        return None
    player_name = m.group("player").strip()
    stat_label  = m.group("stat").strip().capitalize()
    stat_code   = STAT_TO_NBA.get(stat_label.lower())
    if not stat_code:
        return None
    try:
        line = float(m.group("line"))
    except ValueError:
        return None

    token_ids_raw = market.get("clobTokenIds")
    try:
        ids = json.loads(token_ids_raw) if isinstance(token_ids_raw, str) else token_ids_raw
        yes_tok, no_tok = ids[0], ids[1]
    except Exception:
        return None

    end_date_raw = market.get("endDate")
    try:
        end = datetime.fromisoformat(end_date_raw.replace("Z", "+00:00"))
        end_ms = int(end.timestamp() * 1000)
    except (TypeError, AttributeError, ValueError):
        return None

    try:
        yes_ask = float(market.get("bestAsk") or 0)
        yes_bid = float(market.get("bestBid") or 0)
    except (ValueError, TypeError):
        yes_ask = yes_bid = 0.0

    return PolyProp(
        market_id=    market.get("id", "") or market.get("conditionId", ""),
        condition_id= market.get("conditionId", ""),
        question=     question,
        player_name=  player_name,
        stat_label=   stat_label,
        stat_code=    stat_code,
        line=         line,
        yes_token_id= yes_tok,
        no_token_id=  no_tok,
        yes_ask=      yes_ask,
        yes_bid=      yes_bid,
        end_date_ms=  end_ms,
        liquidity=    float(market.get("liquidity", 0) or 0),
        volume=       float(market.get("volume",    0) or 0),
    )


# ── Player projection (delegates to NBA API) ────────────────────────────────

class PlayerStatsCache:
    """In-memory cache. Keyed by (player_id, season). Avoids hammering NBA API."""
    def __init__(self) -> None:
        self._cache: dict[tuple[int, str], dict] = {}

    def get_log(self, player_name: str) -> tuple[int, list[dict]] | None:
        from nba_api.stats.endpoints import playergamelog
        from nba_api.stats.static import players as static_players
        matches = static_players.find_players_by_full_name(player_name)
        if not matches:
            return None
        player_id = matches[0]["id"]
        season = nba_season_for_date(datetime.now(timezone.utc))
        if (player_id, season) in self._cache:
            return player_id, self._cache[(player_id, season)]
        try:
            log_df = playergamelog.PlayerGameLog(
                player_id=player_id, season=season, timeout=20,
            ).get_data_frames()[0]
        except Exception as e:
            log.warning("nba_api fail for %s: %s", player_name, e)
            return None
        # Convert to plain dict list for caching (no pandas in callers)
        rows = log_df.to_dict("records")
        self._cache[(player_id, season)] = rows
        return player_id, rows


def nba_season_for_date(d: datetime) -> str:
    y = d.year
    if d.month >= 10:
        return f"{y}-{str(y + 1)[-2:]}"
    return f"{y - 1}-{str(y)[-2:]}"


def project(prop: PolyProp, cache: PlayerStatsCache) -> tuple[float, dict]:
    """Compute p_over for a prop. Returns (p_over, diagnostics)."""
    diag: dict = {
        "model": "rolling_avg_v1",
        "n_games": 0,
        "rolling_avg": None,
        "sigma": None,
    }
    res = cache.get_log(prop.player_name)
    if res is None:
        return DEFAULT_OUT_PCT, {**diag, "error": "no_player_log"}
    player_id, rows = res
    if len(rows) < MIN_GAMES:
        return DEFAULT_OUT_PCT, {**diag, "error": "insufficient_games", "n_games": len(rows)}

    # Latest first per nba_api convention. Take the most recent N.
    rolling = [float(r[prop.stat_code]) for r in rows[:ROLLING_GAMES] if prop.stat_code in r]
    if len(rolling) < MIN_GAMES:
        return DEFAULT_OUT_PCT, {**diag, "error": "insufficient_stat_data", "n_games": len(rolling)}
    avg = sum(rolling) / len(rolling)

    # Sigma from last 30 games
    sigma_sample = [float(r[prop.stat_code]) for r in rows[:SIGMA_GAMES] if prop.stat_code in r]
    if len(sigma_sample) >= MIN_GAMES:
        m = sum(sigma_sample) / len(sigma_sample)
        var = sum((x - m) ** 2 for x in sigma_sample) / len(sigma_sample)
        sigma = max(math.sqrt(var), 0.5)
    else:
        sigma = DEFAULT_SIGMA

    # p(stat > line) approximated by sigmoid((avg - line) / sigma)
    z = (avg - prop.line) / max(sigma, 0.1)
    p_over = 1.0 / (1.0 + math.exp(-z))

    diag.update({
        "n_games":     len(rolling),
        "rolling_avg": round(avg, 2),
        "sigma":       round(sigma, 2),
        "z":           round(z, 3),
    })
    return p_over, diag


# ── Output ──────────────────────────────────────────────────────────────────

def build_projections(props: list[PolyProp]) -> dict:
    cache = PlayerStatsCache()
    rows = []
    for p in props:
        p_over, diag = project(p, cache)
        rows.append({
            "market_id":    p.market_id,
            "condition_id": p.condition_id,
            "question":     p.question,
            "player":       p.player_name,
            "stat":         p.stat_label,
            "stat_code":    p.stat_code,
            "line":         p.line,
            "yes_token_id": p.yes_token_id,
            "no_token_id":  p.no_token_id,
            "yes_ask":      p.yes_ask,
            "yes_bid":      p.yes_bid,
            "end_date_ms":  p.end_date_ms,
            "liquidity":    p.liquidity,
            "volume":       p.volume,
            "p_over":       round(p_over, 4),
            "edge_over":    round(p_over  - p.yes_ask, 4) if p.yes_ask > 0 else 0.0,
            "edge_under":   round((1.0 - p_over) - (1.0 - p.yes_bid), 4) if p.yes_bid > 0 else 0.0,
            "diagnostics":  diag,
        })
    return {
        "generated_at_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
        "n_markets":       len(rows),
        "projections":     rows,
    }


def write_output(payload: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2))
    tmp.replace(path)
    log.info("wrote %d projections to %s", payload["n_markets"], path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--watch", action="store_true",
                        help="loop forever, refreshing every --interval seconds")
    parser.add_argument("--interval", type=int, default=300,
                        help="refresh interval (seconds) when --watch")
    parser.add_argument("--out", type=Path, default=OUTPUT_PATH,
                        help="output path (default: data/nba_projections.json)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    while True:
        try:
            props = fetch_open_nba_prop_markets()
            if props:
                payload = build_projections(props)
                write_output(payload, args.out)
            else:
                log.info("no open NBA prop markets right now")
        except Exception as e:
            log.exception("projection cycle failed: %s", e)
        if not args.watch:
            return 0
        time.sleep(args.interval)


if __name__ == "__main__":
    sys.exit(main())
