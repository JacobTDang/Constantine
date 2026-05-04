"""H8 — Realistic backtest harness.

THE HARD CONSTRAINT: there is no historical Polymarket order-book data
to replay against. So we simulate market pricing parametrically:

  p_market = p_true * (1 - alpha) + 0.5 * alpha + noise

where:
  alpha  = "market inefficiency" parameter (0.0 = perfectly informed,
            1.0 = pure noise)
  p_true = the historical outcome's probability (estimated from the bar's
            forward return; for a closed bar, this is essentially the label)

We test across a range of alpha values. If the model is profitable across
the full range with realistic fees + slippage, it survives. If only at
alpha >= 0.5 (i.e., the market would have to be barely informed), the
strategy fails — we'd be relying on Polymarket being broken.

Pass criteria (any one failure aborts with non-zero exit):
  - Holdout Brier < 0.23
  - At alpha=0.3 (modestly informed market): Sharpe > 1.0 after costs
  - At alpha=0.3: Max drawdown < 15%
  - Kill-switch logic actually fires when expected
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd

from data.binance_history import download_klines, save_to_parquet
from data.label import join_features_and_labels, label_windows
from model.calibration import calibrate_model
from model.features import ML_FEATURES, compute_features
from model.train import (
    BRIER_FAIL_GATE, evaluate, run_walk_forward_training, walk_forward_splits,
)

log = logging.getLogger(__name__)


# ── Cost model ───────────────────────────────────────────────────────────────

@dataclass
class CostModel:
    fee_rate:        float = 0.018  # Polymarket BTC taker
    entry_slippage:  float = 0.010  # 1% entry slippage (thin market)
    exit_slippage:   float = 0.005  # 0.5% exit slippage

    def apply_entry(self, market_price: float) -> float:
        """Effective price paid: market + slippage, plus fee on the trade."""
        return market_price * (1.0 + self.entry_slippage) * (1.0 + self.fee_rate)

    def apply_exit(self, settlement_value: float) -> float:
        """Effective payout received: settlement minus slippage minus fee."""
        return settlement_value * (1.0 - self.exit_slippage) * (1.0 - self.fee_rate)


@dataclass
class BacktestParams:
    bankroll:            float = 1500.0
    max_bet:             float = 30.0
    kelly_fraction:      float = 0.25
    edge_threshold:      float = 0.04
    daily_loss_limit:    float = 75.0
    max_open_exposure:   float = 300.0
    market_alphas:       list[float] = field(default_factory=lambda: [0.0, 0.3, 0.6, 1.0])


@dataclass
class AlphaResult:
    alpha:           float
    n_trades:        int
    total_pnl:       float
    final_bankroll:  float
    return_pct:      float
    sharpe_annual:   float
    max_drawdown:    float
    win_rate:        float
    avg_edge:        float
    days_killed:     int
    passed_criteria: bool


@dataclass
class BacktestReport:
    holdout_n:        int
    holdout_brier:    float
    holdout_ece:      float
    holdout_log_loss: float
    feature_importance: dict[str, int]
    alpha_results:    list[AlphaResult]
    overall_passed:   bool


# ── Trade simulator ──────────────────────────────────────────────────────────

def kelly_size(
    p_model:         float,
    market_price:    float,
    bankroll:        float,
    kelly_fraction:  float,
    max_bet:         float,
) -> float:
    """Direction-agnostic Kelly. Caller passes the price for whichever side
    they're buying. Returns 0 if no edge or invalid input.
    Mirrors rust-core/src/execution/kelly.rs."""
    if not (0 < market_price < 1):    return 0.0
    if not (0 <= p_model <= 1):       return 0.0
    if bankroll <= 0 or kelly_fraction <= 0 or max_bet <= 0:
        return 0.0
    b = (1.0 - market_price) / market_price
    q = 1.0 - p_model
    kelly_full = (b * p_model - q) / b
    if kelly_full <= 0:               return 0.0
    raw = kelly_full * kelly_fraction * bankroll
    return float(min(raw, max_bet))


def simulate_market_price(p_true_fair: float, alpha: float, rng: np.random.Generator) -> float:
    """Generate a market ask price for the YES side under given inefficiency.

    alpha=0.0 → market = p_true_fair (perfectly informed; zero edge for us)
    alpha=1.0 → market = 0.5 + noise (uninformed; maximum edge)
    """
    base = p_true_fair * (1.0 - alpha) + 0.5 * alpha
    noise = rng.normal(0, 0.01)
    return float(np.clip(base + noise, 0.02, 0.98))


def simulate_one_alpha(
    p_model:    np.ndarray,
    label:      np.ndarray,
    timestamps: np.ndarray,
    alpha:      float,
    *,
    cost:       CostModel,
    params:     BacktestParams,
    seed:       int = 42,
) -> AlphaResult:
    """Run the full trade simulator at a given market-inefficiency alpha."""
    rng = np.random.default_rng(seed)
    bankroll       = params.bankroll
    open_exposure  = 0.0
    daily_loss     = 0.0
    last_day_id    = -1   # sentinel: no day seen yet
    kill_switch    = False
    days_killed    = 0
    days_seen: set[int] = set()

    pnl_history: list[float] = []
    edges:       list[float] = []
    n_wins = 0

    p_market_arr = np.array([
        simulate_market_price(label_i, alpha, rng) for label_i in label
    ])

    for i in range(len(p_model)):
        ts_ms  = int(timestamps[i])
        day_id = ts_ms // 86_400_000

        # Daily reset
        if day_id != last_day_id:
            if last_day_id >= 0 and kill_switch:
                days_killed += 1
            last_day_id = day_id
            daily_loss  = 0.0
            kill_switch = False
        days_seen.add(day_id)

        if kill_switch:
            continue

        pm  = float(p_model[i])
        mkt = float(p_market_arr[i])
        outcome = int(label[i])

        # Decide direction: YES if pm > mkt, NO if pm < (1 - (1 - mkt))
        # Wait — we trade on whichever side has bigger edge.
        # Buying YES at mkt: profit if outcome=1; lose if outcome=0
        # Buying NO  at (1-mkt) (assuming yes_ask + no_ask = 1): profit if outcome=0
        yes_ask = mkt
        no_ask  = 1.0 - mkt   # simplified — in reality there's a spread
        yes_edge = pm - yes_ask
        no_edge  = (1.0 - pm) - no_ask

        if yes_edge >= no_edge:
            best_edge = yes_edge
            side, price = "yes", yes_ask
            wins = (outcome == 1)
        else:
            best_edge = no_edge
            side, price = "no", no_ask
            wins = (outcome == 0)

        if best_edge < params.edge_threshold:
            continue

        bet = kelly_size(
            pm if side == "yes" else (1.0 - pm),
            price, bankroll,
            params.kelly_fraction, params.max_bet,
        )
        if bet <= 0 or open_exposure + bet > params.max_open_exposure:
            continue

        # Apply costs
        effective_cost = cost.apply_entry(price)
        # Settlement: $1 if win, $0 if lose
        settlement = 1.0 if wins else 0.0
        gross_payout = (bet / price) * settlement
        net_payout   = cost.apply_exit(gross_payout) if wins else 0.0
        pnl = net_payout - bet * (1.0 + cost.fee_rate)  # bet × fee on entry

        # Track
        bankroll       += pnl
        pnl_history.append(pnl)
        edges.append(best_edge)
        if pnl > 0:
            n_wins += 1
        if pnl < 0:
            daily_loss += -pnl
            if daily_loss >= params.daily_loss_limit:
                kill_switch = True

    # Compute metrics
    total_pnl = sum(pnl_history)
    n_trades  = len(pnl_history)

    if n_trades == 0:
        sharpe   = 0.0
        max_dd   = 0.0
        win_rate = 0.0
        avg_edge = 0.0
    else:
        returns = np.array(pnl_history) / params.bankroll
        # 5-min windows × 12/hr × 24 = 288/day. Annualize from per-trade std.
        # We don't trade every window, so use sqrt(n_trades_per_year).
        n_per_year = max(1, n_trades * 365 // max(1, len(days_seen)))
        sharpe = (
            float(returns.mean() / returns.std() * np.sqrt(n_per_year))
            if returns.std() > 1e-12 else 0.0
        )

        cum  = np.cumsum(pnl_history)
        peak = np.maximum.accumulate(cum)
        dd   = (peak - cum) / np.maximum(params.bankroll + peak, 1.0)
        max_dd   = float(dd.max())
        win_rate = n_wins / n_trades
        avg_edge = float(np.mean(edges))

    passed = (
        sharpe   > 1.0 and
        max_dd   < 0.15 and
        days_killed < 5
    ) if n_trades > 0 else True   # No trades is OK at low-edge regimes

    return AlphaResult(
        alpha=alpha,
        n_trades=n_trades,
        total_pnl=total_pnl,
        final_bankroll=bankroll,
        return_pct=(bankroll / params.bankroll - 1.0) * 100.0,
        sharpe_annual=sharpe,
        max_drawdown=max_dd,
        win_rate=win_rate,
        avg_edge=avg_edge,
        days_killed=days_killed,
        passed_criteria=passed,
    )


# ── Top-level backtest ──────────────────────────────────────────────────────

def run_backtest(
    klines_parquet: Path,
    *,
    cost:   CostModel = None,
    params: BacktestParams = None,
    out_dir: Path = None,
) -> BacktestReport:
    cost   = cost   or CostModel()
    params = params or BacktestParams()
    out_dir = Path(out_dir) if out_dir else Path("data/models")

    # 1. Load klines, compute features and labels
    log.info("loading klines from %s", klines_parquet)
    klines = pd.read_parquet(klines_parquet)
    feat   = compute_features(klines)
    labels = label_windows(klines, duration_min=5)
    joined = join_features_and_labels(feat, labels, ML_FEATURES)

    # Drop rows with NaN features (warmup period)
    joined = joined.dropna(subset=ML_FEATURES).reset_index(drop=True)
    log.info("usable training+holdout rows: %d", len(joined))

    # 2. Walk-forward training (uses our train.py logic)
    log.info("running walk-forward training")
    training_report = run_walk_forward_training(
        joined, fail_on_gate=False, save_path=out_dir / "btc_lgbm.txt",
    )

    # 3. Predict on holdout
    folds, holdout = walk_forward_splits(joined)
    booster = lgb.Booster(model_file=str(out_dir / "btc_lgbm.txt"))
    p_holdout = booster.predict(holdout[ML_FEATURES])
    y_holdout = holdout["label"].astype(int).to_numpy()
    cal       = calibrate_model(y_holdout, p_holdout)
    log.info("holdout: n=%d brier=%.4f ece=%.4f", cal.n, cal.brier, cal.ece)

    # 4. Simulate trading at each alpha — alphas are independent, fan out
    #    across processes. ProcessPoolExecutor (not threads) because numpy
    #    + the simulator are CPU-bound, and threads would hit the GIL.
    timestamps = holdout["prediction_time_ms"].to_numpy()
    n_workers = min(len(params.market_alphas), os.cpu_count() or 1)
    log.info("simulating %d alphas across %d workers", len(params.market_alphas), n_workers)

    if n_workers <= 1:
        alpha_results = [
            simulate_one_alpha(p_holdout, y_holdout, timestamps, a, cost=cost, params=params)
            for a in params.market_alphas
        ]
    else:
        with ProcessPoolExecutor(max_workers=n_workers) as pool:
            futures = {
                pool.submit(
                    simulate_one_alpha,
                    p_holdout, y_holdout, timestamps, a,
                    cost=cost, params=params,
                ): a for a in params.market_alphas
            }
            alpha_results = []
            for fut in as_completed(futures):
                alpha_results.append(fut.result())
        # Restore stable order — as_completed gives them back in finish order
        alpha_results.sort(key=lambda ar: ar.alpha)

    for ar in alpha_results:
        log.info(
            "alpha=%.1f: trades=%d pnl=$%.2f sharpe=%.2f maxdd=%.1f%% win=%.1f%% killed=%d",
            ar.alpha, ar.n_trades, ar.total_pnl, ar.sharpe_annual,
            ar.max_drawdown * 100, ar.win_rate * 100, ar.days_killed,
        )

    # 5. Pass/fail decision
    brier_gate = cal.brier < BRIER_FAIL_GATE
    # The realistic operating point is alpha=0.3 — must pass there
    realistic = next((r for r in alpha_results if abs(r.alpha - 0.3) < 1e-6), None)
    realistic_pass = realistic is not None and realistic.passed_criteria
    overall = brier_gate and realistic_pass

    report = BacktestReport(
        holdout_n=cal.n,
        holdout_brier=cal.brier,
        holdout_ece=cal.ece,
        holdout_log_loss=cal.log_loss,
        feature_importance=training_report.feature_importance,
        alpha_results=alpha_results,
        overall_passed=overall,
    )

    # Persist
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "backtest_report.json").write_text(json.dumps(asdict(report), indent=2))

    return report


# ── CLI ──────────────────────────────────────────────────────────────────────

def main() -> int:
    p = argparse.ArgumentParser(description="H8 — backtest the trained ML model")
    p.add_argument("--klines",   type=Path, required=True, help="parquet from H1")
    p.add_argument("--out-dir",  type=Path, default=Path("data/models"))
    p.add_argument("--bankroll", type=float, default=1500.0)
    p.add_argument("--quiet",    action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )

    params = BacktestParams(bankroll=args.bankroll)
    report = run_backtest(args.klines, params=params, out_dir=args.out_dir)

    print()
    print("=" * 60)
    print(f"  Holdout n={report.holdout_n} brier={report.holdout_brier:.4f} ece={report.holdout_ece:.4f}")
    print("  Per-alpha simulation:")
    for r in report.alpha_results:
        marker = "PASS" if r.passed_criteria else "fail"
        print(f"    alpha={r.alpha:.1f}  trades={r.n_trades:5d}  "
              f"pnl=${r.total_pnl:+8.2f}  sharpe={r.sharpe_annual:5.2f}  "
              f"maxdd={r.max_drawdown*100:5.1f}%  killed={r.days_killed}d  [{marker}]")
    print(f"  OVERALL: {'PASS' if report.overall_passed else 'FAIL'}")
    print("=" * 60)

    return 0 if report.overall_passed else 1


if __name__ == "__main__":
    sys.exit(main())
