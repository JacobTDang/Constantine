"""Daily Constantine review — LLM-driven analysis (cold path).

Runs once per day (cron / Task Scheduler). Reads the last 24 hours of
trading data, asks Claude to summarize, writes a markdown report to
data/reports/YYYY-MM-DD.md.

This is INTENTIONALLY off the trading hot path. The trading bot is
deterministic Rust; this sidecar exists only to surface insights a
human would otherwise miss in the JSONL logs.

Cost: ~$0.10-0.50/day at typical trade volume (5-50 trades).

Cost: ~$0.10-0.50/day at typical trade volume.
Caches inputs aggressively to skip re-runs that produced the same data.

Usage:
  pip install anthropic python-dotenv
  echo 'ANTHROPIC_API_KEY=sk-ant-...' >> .env
  python scripts/daily_review.py
"""
from __future__ import annotations

import json
import os
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    from anthropic import Anthropic
except ImportError:
    print("ERROR: pip install anthropic", file=sys.stderr)
    sys.exit(1)


REPORT_DIR = Path("data/reports")
DB_DIR     = Path("data/db")
WINDOW_HRS = 24


def load_jsonl_window(path: Path, since_ms: int) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            ts = row.get("event_at_ms") or row.get("fired_at_ms") or 0
            if ts >= since_ms:
                rows.append(row)
    return rows


def summarize_signals(rows: list[dict]) -> dict:
    if not rows:
        return {"n_total": 0}
    by_type     = Counter(r.get("signal_type", "?") for r in rows)
    by_strategy = Counter(r.get("strategy_tag", r.get("signal_type", "?")) for r in rows)
    by_market   = Counter(r.get("market_id", "?") for r in rows)

    avg_edge = sum(r.get("edge", 0) for r in rows) / len(rows)

    # Per-strategy breakdown -- avg edge, n signals, top markets
    by_strategy_detail: dict[str, dict] = {}
    for tag in by_strategy:
        sub = [r for r in rows
               if r.get("strategy_tag", r.get("signal_type")) == tag]
        if not sub:
            continue
        by_strategy_detail[tag] = {
            "n":             len(sub),
            "avg_edge":      round(sum(r.get("edge", 0) for r in sub) / len(sub), 4),
            "avg_confidence": round(
                sum(r.get("confidence", 0) for r in sub) / len(sub), 4
            ),
            "top_markets":   [m for m, _ in
                              Counter(r.get("market_id", "?") for r in sub)
                              .most_common(3)],
        }

    return {
        "n_total":        len(rows),
        "by_signal_type": dict(by_type),
        "by_strategy":    dict(by_strategy),
        "by_strategy_detail": by_strategy_detail,
        "top_markets":    [m for m, _ in by_market.most_common(5)],
        "avg_edge":       round(avg_edge, 4),
    }


def summarize_positions(rows: list[dict]) -> dict:
    by_kind = Counter(r.get("kind", "?") for r in rows)
    settled = [r for r in rows if r.get("kind") == "settle"]
    pnl = sum(r.get("pnl_dollars", 0) for r in settled)
    wins   = sum(1 for r in settled if r.get("pnl_dollars", 0) > 0)
    losses = sum(1 for r in settled if r.get("pnl_dollars", 0) < 0)
    win_rate = wins / max(wins + losses, 1)
    return {
        "n_events":       len(rows),
        "by_kind":        dict(by_kind),
        "n_settled":      len(settled),
        "n_wins":         wins,
        "n_losses":       losses,
        "win_rate":       round(win_rate, 3),
        "realised_pnl":   round(pnl, 2),
    }


def build_prompt(sig_summary: dict, pos_summary: dict) -> str:
    return f"""You are a quant trading analyst reviewing a Polymarket bot's last 24h.
The bot runs up to 8 strategies in parallel (each tagged with strategy_tag):
  - oracle / intramarket  — BTC binary mispricing
  - player_props          — NBA O/U projections (Strategy 1)
  - event_arb             — sum-of-YES exclusive-winner arb (Strategy 2)
  - lp_rewards            — liquidity-rewards quoter (EDGE-A)
  - sportsbook_devig      — Pinnacle no-vig overlay (EDGE-C)
  - whale_follow          — top-PnL wallet copy (EDGE-D)
  - hibernating           — observe-only stale-market scanner (EDGE-E)

Signals data (sliced by strategy_tag where available):
{json.dumps(sig_summary, indent=2)}

Position lifecycle data:
{json.dumps(pos_summary, indent=2)}

Write a concise daily report (<=400 words) covering:
1. Headline result — was the day profitable? By how much?
2. Per-strategy attribution — for each strategy that fired, report n_signals,
   avg_edge, avg_confidence. Note any strategy with > 100 signals OR > 10%
   avg_edge — both warrant scrutiny. If `hibernating` fired, list 1-3
   most-anomalous candidates from top_markets.
3. Anomalies — sudden win-rate drops, spike in rejected orders, unexpected
   strategy distributions, low-quality (< 1% avg_edge) signals from a
   strategy that previously had > 5%.
4. Suggested config tweaks — concrete `.env` changes to consider, e.g.
   "raise MIN_EDGE for strategy X to Y because the noise floor is Z."
   NEVER suggest a change that increases risk irresponsibly.
5. Things to watch tomorrow.

Format as markdown with section headers. Don't pad the report; if nothing
notable happened, say so explicitly. If a strategy emitted zero signals,
note it (one line) but don't speculate."""


def main() -> int:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ANTHROPIC_API_KEY not set; skipping LLM review", file=sys.stderr)
        return 2

    now = datetime.now(timezone.utc)
    since_ms = int((now - timedelta(hours=WINDOW_HRS)).timestamp() * 1000)

    sig_rows = load_jsonl_window(DB_DIR / "signals.jsonl",   since_ms)
    pos_rows = load_jsonl_window(DB_DIR / "positions.jsonl", since_ms)
    sig_summary = summarize_signals(sig_rows)
    pos_summary = summarize_positions(pos_rows)

    print(f"signals: {sig_summary.get('n_total', 0)}, "
          f"position events: {pos_summary.get('n_events', 0)}, "
          f"realised P&L: ${pos_summary.get('realised_pnl', 0):+.2f}")

    if sig_summary.get("n_total", 0) == 0 and pos_summary.get("n_events", 0) == 0:
        print("nothing to review — bot was idle in the last 24h")
        return 0

    client = Anthropic(api_key=api_key)
    prompt = build_prompt(sig_summary, pos_summary)
    msg = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1500,
        messages=[{"role": "user", "content": prompt}],
    )
    body = "".join(b.text for b in msg.content if hasattr(b, "text"))

    out_path = REPORT_DIR / f"{now.date().isoformat()}.md"
    header = (
        f"# Constantine Daily Review — {now.date().isoformat()}\n\n"
        f"_Generated {now.isoformat()} UTC. Window: last {WINDOW_HRS}h._\n\n"
        f"## Aggregates\n\n"
        f"```\n"
        f"{json.dumps({'signals': sig_summary, 'positions': pos_summary}, indent=2)}\n"
        f"```\n\n"
        f"## Analysis (Claude Sonnet 4.6)\n\n"
    )
    out_path.write_text(header + body)
    print(f"wrote {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
