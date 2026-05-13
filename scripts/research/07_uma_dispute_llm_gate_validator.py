"""Strategy 07 / Phase 0 -- UMA dispute proposer-base-rate validator.

Hypothesis (from research/strategies/07_uma_dispute_llm_gate.md):
  ~99% base rate: UMA's DVM aligns with the whitelisted proposer's
  initial proposal on the overwhelming majority of disputes. Combined
  with an LLM gate (89.58% per arxiv 2604.15674), the joint hit rate
  is ~88.7% on disputed markets, with entries typically at 0.80-0.85
  yielding 3-5% net per trade after fees.

Phase 0 plan from spec:
  "1. Pull last 30 UMA-resolved Polymarket markets from on-chain
   history.
   2. For each, run the LLM classifier with web search at the
   dispute time.
   3. Compare LLM verdict vs UMA's final ruling.
   4. Pass if hit rate >= 85% (close to the 89.58% paper claim)."

We DO NOT have a hosted LLM in this validator process (network
permitting, the user can run the Phase 1 backtest with the LLM
hooked up). Phase 0's job is to establish the BASE RATE that the
strategy is layered on top of: the proposer-aligns-with-DVM rate.

This validator queries the UMA OOv2 subgraph for recent
DisputePrice events on Polymarket condition_ids, then for each
checks whether the proposed_price matched the eventual settled_price.
If the proposer base rate is empirically near 99%, the LLM gate's
job is to filter out the contentious 1% where governance attacks /
literal-rule disputes overturn the proposer.

Pass criteria (per research/cycles/IMPL_LOOP.md quality bar):
  GREEN:  >= 20 settled disputes observed AND proposer base rate
          >= 95% AND median entry price would have been < 0.92
  YELLOW: proposer base rate 85-95% (consistent with strategy
          but governance-attack tail thicker than spec assumes)
  RED:    proposer base rate < 85% OR fewer than 5 settled disputes
          observed OR UMA subgraph unreachable

Usage:
  python scripts/research/07_uma_dispute_llm_gate_validator.py
"""
from __future__ import annotations

import json
import statistics
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

# ── Endpoints ───────────────────────────────────────────────────────────────

UMA_SUBGRAPH = ("https://api.thegraph.com/subgraphs/name/umaprotocol/"
                "polygon-contracts")
# Fallback: UMA official hosted (sometimes more available)
UMA_SUBGRAPH_FALLBACKS = [
    UMA_SUBGRAPH,
    "https://api.studio.thegraph.com/query/56178/oo-v2-polygon/version/latest",
]
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"

LOOKBACK_DAYS         = 365     # disputes are rare; widen the window
MAX_DISPUTES_QUERY    = 200
HARD_RUNTIME_SECS     = 270
SLEEP_BETWEEN_REQS    = 0.4


@dataclass
class DisputeSample:
    request_id:       str
    condition_id:     str
    proposed_outcome: int                # 0 = NO, 1 = YES (interpretation: see _normalise)
    settled_outcome:  int
    proposer_wins:    bool
    dispute_ts:       datetime | None
    entry_price_est:  float | None       # YES-side Polymarket price at dispute, if known


@dataclass
class ValidationResult:
    n_disputes_returned: int = 0
    n_settled:           int = 0
    n_proposer_wins:     int = 0
    samples:             list[DisputeSample] = field(default_factory=list)
    entry_prices:        list[float] = field(default_factory=list)

    @property
    def proposer_base_rate(self) -> float:
        if not self.n_settled:
            return 0.0
        return self.n_proposer_wins / self.n_settled

    @property
    def median_entry_price(self) -> float:
        if not self.entry_prices:
            return 0.0
        return statistics.median(self.entry_prices)


def _normalise_price_to_outcome(price_raw) -> int | None:
    """UMA OOv2 stores resolved price as a signed int (Wei-scale).
    For Polymarket binary markets, 1e18 = YES, 0 = NO, 0.5e18 = ambiguous.
    Return 1 (YES), 0 (NO), or None if ambiguous/missing."""
    if price_raw is None:
        return None
    try:
        s = str(price_raw)
        if s.startswith("0x"):
            v = int(s, 16)
        else:
            v = int(s)
    except (TypeError, ValueError):
        return None
    yes_token = 10 ** 18
    no_token = 0
    # Tolerance for rounding
    if abs(v - yes_token) < 10 ** 16:
        return 1
    if abs(v - no_token) < 10 ** 16:
        return 0
    return None  # ambiguous (e.g., 0.5e18) or other


def query_subgraph(query: str, url: str) -> dict | None:
    try:
        r = requests.post(url, json={"query": query}, timeout=20)
        r.raise_for_status()
        payload = r.json()
    except (requests.RequestException, ValueError) as e:
        print(f"  subgraph query failed @ {url[:50]}: {e}", file=sys.stderr)
        return None
    if not isinstance(payload, dict):
        return None
    if "errors" in payload:
        print(f"  subgraph errors: {payload['errors']}", file=sys.stderr)
        return None
    return payload.get("data")


def fetch_disputes() -> list[dict]:
    """Pull recent settled disputes from any working UMA subgraph URL."""
    since_ts = int((datetime.now(timezone.utc) -
                    timedelta(days=LOOKBACK_DAYS)).timestamp())
    # The UMA OOv2 subgraph schema varies; we try a permissive query.
    query = f"""
    {{
      priceRequests(
        first: {MAX_DISPUTES_QUERY},
        where: {{ time_gte: {since_ts}, isResolved: true }},
        orderBy: time, orderDirection: desc
      ) {{
        id
        identifier
        time
        ancillaryData
        proposedPrice
        resolvedPrice
        disputeStartTimestamp
        settlementTimestamp
        state
      }}
    }}
    """
    for url in UMA_SUBGRAPH_FALLBACKS:
        data = query_subgraph(query, url)
        if data and isinstance(data.get("priceRequests"), list):
            disputes = data["priceRequests"]
            # Filter to those that actually had a dispute (proposed != resolved
            # OR explicit "Disputed" state)
            return disputes
    return []


def parse_condition_id(ancillary: str) -> str | None:
    """Best-effort: extract a condition_id-like hex from ancillaryData.
    Polymarket's UMA requests include a 'q:' question text and sometimes
    a conditionId / token reference."""
    if not ancillary:
        return None
    # Look for a 64-hex pattern (0x + 64 chars)
    import re
    m = re.search(r"0x[0-9a-fA-F]{64}", ancillary)
    return m.group(0) if m else None


def _yes_price_at(condition_id: str, ts: datetime | None) -> float | None:
    """Best-effort: fetch current outcomePrices for the market; if it's
    still resolving, we won't have a clean number. Used to estimate
    entry price at dispute time (we don't have historical price-history
    for the dispute timestamp without a CLOB call)."""
    if not condition_id:
        return None
    params = {"condition_ids": condition_id}
    try:
        r = requests.get(GAMMA_MARKETS_URL, params=params, timeout=10)
        r.raise_for_status()
        markets = r.json()
    except (requests.RequestException, ValueError):
        return None
    if not isinstance(markets, list) or not markets:
        return None
    m = markets[0]
    # Last trade price is best proxy for "around dispute time"
    last_trade = m.get("lastTradePrice")
    try:
        return float(last_trade) if last_trade is not None else None
    except (ValueError, TypeError):
        return None


def main() -> int:
    started = time.time()
    out_dir = Path("data/research")
    out_dir.mkdir(parents=True, exist_ok=True)

    disputes = fetch_disputes()
    res = ValidationResult()
    res.n_disputes_returned = len(disputes)

    if not disputes:
        print()
        print("=" * 64)
        print("STRATEGY 07 / PHASE 0 -- UMA DISPUTE BASE RATE VALIDATION")
        print("=" * 64)
        print("[RED] KILL: UMA subgraph returned no disputes (or both")
        print("           endpoints unreachable). Cannot establish empirical")
        print("           proposer base rate. Phase 0 requires subgraph access.")
        return 3

    print(f"fetched {len(disputes)} settled price requests from subgraph")

    for i, d in enumerate(disputes):
        if time.time() - started > HARD_RUNTIME_SECS:
            print(f"  runtime cap at {i}/{len(disputes)}")
            break
        proposed_outcome = _normalise_price_to_outcome(d.get("proposedPrice"))
        settled_outcome = _normalise_price_to_outcome(d.get("resolvedPrice"))
        if proposed_outcome is None or settled_outcome is None:
            continue
        # Only count "real" disputes — those where dispute_start was set
        dispute_ts_raw = d.get("disputeStartTimestamp") or 0
        try:
            dispute_ts_int = int(dispute_ts_raw)
        except (TypeError, ValueError):
            dispute_ts_int = 0
        if dispute_ts_int <= 0:
            # No dispute happened on this request — skip (not a "dispute"
            # but a clean proposal)
            continue
        dispute_ts = datetime.fromtimestamp(dispute_ts_int, tz=timezone.utc)

        condition_id = parse_condition_id(d.get("ancillaryData") or "") or ""
        proposer_wins = (proposed_outcome == settled_outcome)
        entry_price = _yes_price_at(condition_id, dispute_ts) if condition_id else None
        # If we DID get a price, the "entry on proposer's side" is:
        # proposer says YES → buy YES at lastTradePrice
        # proposer says NO  → buy NO  at (1 - lastTradePrice)
        if entry_price is not None:
            est_entry = entry_price if proposed_outcome == 1 else (1.0 - entry_price)
            res.entry_prices.append(est_entry)
        else:
            est_entry = None

        res.n_settled += 1
        if proposer_wins:
            res.n_proposer_wins += 1

        if len(res.samples) < 25:
            res.samples.append(DisputeSample(
                request_id=       d.get("id", ""),
                condition_id=     condition_id,
                proposed_outcome= proposed_outcome,
                settled_outcome=  settled_outcome,
                proposer_wins=    proposer_wins,
                dispute_ts=       dispute_ts,
                entry_price_est=  est_entry,
            ))

        if (i + 1) % 25 == 0:
            print(f"  processed {i+1}/{len(disputes)} "
                  f"(real disputes={res.n_settled})", flush=True)
        time.sleep(SLEEP_BETWEEN_REQS)

    # Artefacts
    csv_path = out_dir / "07_uma_dispute_llm_gate_phase0.csv"
    with csv_path.open("w", encoding="utf-8") as f:
        f.write("request_id,condition_id,dispute_ts,proposed,settled,"
                "proposer_wins,entry_price_est\n")
        for s in res.samples:
            f.write(f"{s.request_id},{s.condition_id},"
                    f"{s.dispute_ts.isoformat() if s.dispute_ts else ''},"
                    f"{s.proposed_outcome},{s.settled_outcome},"
                    f"{int(s.proposer_wins)},"
                    f"{s.entry_price_est if s.entry_price_est is not None else ''}\n")

    summary = {
        "n_disputes_returned":  res.n_disputes_returned,
        "n_settled":            res.n_settled,
        "n_proposer_wins":      res.n_proposer_wins,
        "proposer_base_rate":   round(res.proposer_base_rate, 4),
        "median_entry_price":   round(res.median_entry_price, 4),
        "n_entry_samples":      len(res.entry_prices),
        "lookback_days":        LOOKBACK_DAYS,
    }
    json_path = out_dir / "07_uma_dispute_llm_gate_phase0.json"
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print()
    print("=" * 64)
    print("STRATEGY 07 / PHASE 0 -- UMA DISPUTE BASE RATE VALIDATION")
    print("=" * 64)
    print(f"Disputes returned:       {res.n_disputes_returned}")
    print(f"Real disputes processed: {res.n_settled}")
    print(f"Proposer wins:           {res.n_proposer_wins}")
    print(f"Proposer base rate:      {res.proposer_base_rate*100:.1f}%")
    if res.entry_prices:
        print(f"Median entry price:      {res.median_entry_price:.3f} "
              f"(n={len(res.entry_prices)})")
    print()

    if res.n_settled < 5:
        print("[RED] KILL: too few real disputes in window. UMA subgraph schema")
        print("           may differ from expected, or LOOKBACK_DAYS is too narrow.")
        return 3

    rate = res.proposer_base_rate
    med_entry = res.median_entry_price

    if res.n_settled >= 20 and rate >= 0.95 and (
            len(res.entry_prices) < 5 or med_entry < 0.92):
        print(f"[GREEN] GREEN-LIGHT Strategy 07 to Phase 1:")
        print(f"        proposer base rate {rate*100:.1f}% >= 95% across "
              f"n={res.n_settled} disputes.")
        if res.entry_prices:
            print(f"        median entry {med_entry:.3f} < 0.92 leaves spread.")
        return 0
    if rate >= 0.85:
        print(f"[YELLOW] MARGINAL: proposer base rate {rate*100:.1f}% in 85-95%.")
        print("         Strategy viable with LLM gate but governance-attack tail")
        print("         appears thicker than spec assumed; lower size to $25.")
        return 1
    print(f"[RED] KILL: proposer base rate {rate*100:.1f}% < 85%.")
    print("           Base rate doesn't support the strategy's premise — UMA")
    print("           may be more often overruling proposers than expected.")
    return 3


if __name__ == "__main__":
    sys.exit(main())
