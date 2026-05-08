# Known Gaps + Improvements

Living document. Update as new items surface during build / live runs.

Categories:
- **CRITICAL** — could cause real losses or block live trading
- **IMPORTANT** — degrades performance or limits scope
- **NICE-TO-HAVE** — quality / monitoring / ops
- **DEFERRED** — known but intentionally not addressed yet

Last full review: after Strategy 1 wired into runner (commit `5c86b91`).

---

## Strategy 1 — Player Props

### CRITICAL

- [ ] **Sigmoid model under-confidences extreme lines.** Vassell Points O/U 3.5 → model says p_over 0.84, market says 0.999. Real probability is closer to 0.99 — line is so easy that scoring 4+ in a playoff game is near-certain. Our model treats it as a normal-distribution outcome, which understates lopsided cases. Mitigation: only trade contested markets (yes_ask in [0.20, 0.80]).
- [ ] **No injury suppression.** If a starter is ruled OUT 30 min before tipoff, our projection (based on rolling avg from recent games when they played) is wildly wrong. Need to scrape ESPN injury report and either suppress signal or scale projection to zero.
- [ ] **Forward-test outcome reconciliation untested.** Reconcile path exists but never been exercised against real prop fills. Need to verify: after game ends + UMA resolution + reconcile_with_polymarket runs, prop position transitions correctly.

### IMPORTANT

- [ ] **No opponent defensive ranking.** Embiid vs DPOY-tier center plays differently than Embiid vs unfit team. Public data exists (basketball-reference defensive efficiency by position).
- [ ] **No back-to-back / rest factor.** Players average 5-15% fewer minutes/stats on second night of B2B.
- [ ] **No home/away split.** ~3pp home advantage on most counting stats.
- [ ] **No pace adjustment.** Fast-pace teams produce more counting stats; betting Over on a fast game is structurally biased toward Over.
- [ ] **No prop-fee detection.** Currently using 7% MIN_EDGE which assumes 2% × 2 + slippage. Polymarket fee schedule for props may differ from BTC binary. Verify after first live trade.
- [ ] **Sigma estimation noisy on rookies / role-changes.** Rookie players have < 30 games, sigma defaults to 4.0. Players who recently changed teams have inconsistent sample.

### NICE-TO-HAVE

- [ ] Stat-specific edge thresholds (rebounds easier to forecast than assists).
- [ ] Player-specific volatility (some players are highly consistent, others aren't).
- [ ] Cache stale detection in dashboard view.
- [ ] Per-game P&L grouping (all 8 props from one game settle together).

### DEFERRED

- [ ] XGBoost on richer features (current sigmoid is a baseline).
- [ ] NBA → NFL/MLB extension (different stat sources, different season cycles).
- [ ] Real-time injury feed integration (paid service like Rotowire premium).

---

## Strategy 2 — Event Sum-of-YES Arb

### CRITICAL

- [ ] **Maker-only execution required.** Taker fees (~2%) eat all the 1-3% empirical edge. Bot must place LIMIT ORDERS that rest on the book until filled. Current execute_signal path is take-or-fail; needs full rewrite for this strategy.
- [ ] **Exclusive-winner classification heuristic is brittle.** Current title-keyword filter can misclassify multi-winner events. False positive risk: take an "arb" that doesn't actually arbitrage. Mitigation: dual-check via title keywords + sum-of-YES range; both must pass.
- [ ] **Settlement of multi-leg arb positions.** N-leg trade = N PositionStore entries. If 19/20 fill but 1 doesn't, we have directional exposure. Need atomic-ish: partial fills get auto-cancelled if can't complete the full portfolio within X minutes.
- [ ] **Event mid-stream candidate addition.** Polymarket sometimes adds new candidates after we've placed our portfolio. Our (N-1) payout math breaks if a candidate we don't have NO on wins. Mitigation: monitor event for new candidates; either hedge or close portfolio.

### IMPORTANT

- [ ] **Long-hold capital lockup.** Election arbs hold for months. Need separate `max_long_hold_exposure` cap.
- [ ] **Re-evaluation as prices drift.** Sum-of-YES changes over time. Need periodic re-check of open arbs; if edge has flipped negative, exit early.
- [ ] **Bid depth proxy.** We use Polymarket's `liquidity` field as bid-side depth. Real bid depth from orderbook would be more accurate.
- [ ] **Limit order management.** Need to track placed limit orders, cancel those that don't fill, refresh prices.

### NICE-TO-HAVE

- [ ] Auto-discover new exclusive-winner events as they appear.
- [ ] Event-level position grouping in dashboard.
- [ ] Alert on UMA dispute (could change resolution timing).

### DEFERRED

- [ ] Cross-event correlation (multiple arbs settling same day = settlement-day risk concentration).

---

## Strategy 4 — Geopolitical Fade (KILLED)

Validation showed -81.6% avg ROI. Strategy retired. NOT REOPENING unless:
- We find a different fade mechanic (e.g., fade only LOW-volume "rumor"-driven spikes)
- Sample expanded to 100+ markets shows different pattern

---

## Cross-cutting / infrastructure

### CRITICAL

- [ ] **Reconciliation never run periodically.** Currently runs once on bot startup. If bot runs for 24h+ without restart, fills go unreconciled and PositionStore drifts. Need a periodic reconciler task.
- [ ] **No per-strategy kill switch.** A bug in Strategy 1 could halt Strategy 2 trades via the global kill_switch. Need per-strategy gates.
- [ ] **No live monitoring of sidecar health.** If `nba_projections.py` dies, cache goes stale → prop signals stop firing. We catch this via `is_stale(now_ms)`, but no alert; user only notices via dashboard's heartbeat showing no prop volume.
- [ ] **Wire format never tested live.** Despite verified-vs-rs-clob-client, no real Polymarket order has been submitted. Sprint 9 testnet drill is THE only way to validate. Don't flip EXECUTION_ENABLED=true without it.

### IMPORTANT

- [ ] **Signal log doesn't track strategy source.** signals.jsonl logs all fires but doesn't tag oracle vs prop vs intramarket. Hard to compute per-strategy P&L. Add `strategy_id` field to SignalRow.
- [ ] **Position log lacks strategy attribution.** Same issue: positions.jsonl positions can't be split by strategy. Add `strategy_id` field.
- [ ] **Dashboard doesn't separate strategies.** All positions lumped together. Add per-strategy panels.
- [ ] **No backpressure on JSONL writes.** A spike in trades could fill the disk. Add rotation / size cap.
- [ ] **No `data/db/` size monitoring.** Same as above.

### NICE-TO-HAVE

- [ ] Health check endpoint (HTTP /health) for external monitoring.
- [ ] Metrics export (Prometheus format) for proper observability.
- [ ] Structured signal logs in addition to JSONL (Parquet for analysis).

### DEFERRED

- [ ] G1 settlement timing edge case: if bot was down during close_time, we lack the chainlink history sample. Currently falls back to latest (with warn). Real fix: lookup historical chainlink via Polygon RPC at close_time block.
- [ ] G7 dynamic bankroll edge case: realised_pnl_cents can drift between bot restarts since position log replays settle events. Currently this works correctly due to event replay, but worth a regression test.

---

## Tooling / Dev experience

- [ ] No CI. Should add a GitHub Actions workflow running `cargo test` + `cargo clippy` on PR.
- [ ] No `cargo clippy` baseline. Some lints would surface dead code we haven't audited.
- [ ] No automated formatting check (`cargo fmt --check`).
- [ ] Python scripts use `requests` synchronously. Migrating to `httpx` async would speed up data pulls.
- [ ] Tests for the Python sidecars live nowhere. The validation scripts are de-facto tests but break easily.
- [ ] No load-test on the runner under high signal volume.
- [ ] Dashboard binary doesn't show event-arb portfolios cleanly (when N-leg trades are open).

---

## Empirical questions — TBD by live data

- What fraction of NBA prop signals fire vs the ~137 contested markets per game day?
- How often does the cooldown (60s default) actually save us vs allow the trade?
- Real Polymarket fill latency distribution after submit?
- Average slippage on prop markets at the 50/50 default?
- Do prop markets have a different fee schedule from BTC binaries?
- Distribution of prop OVER/UNDER win rates BY VOLUME bucket — is the 50-200 vol bucket truly unbiased or is sample too small?

---

## Process notes

- After each live trading day: review GAPS.md for items that became obvious. Update ranking by what hurt today.
- After 30 days of live data: cross out CRITICALs that have not in fact caused issues; promote some IMPORTANT to CRITICAL based on real impact.
- Quarterly: archive completed items; start a new section for the quarter.
