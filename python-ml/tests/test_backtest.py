"""H8 — tests for the realistic backtest harness."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# scripts/ is not a package — add it to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))
from backtest import (  # noqa: E402
    AlphaResult,
    BacktestParams,
    CostModel,
    kelly_size,
    simulate_market_price,
    simulate_one_alpha,
)


# ── Cost model ───────────────────────────────────────────────────────────────

def test_cost_apply_entry_increases_price():
    c = CostModel(fee_rate=0.018, entry_slippage=0.01)
    assert c.apply_entry(0.50) > 0.50
    # 0.5 * 1.01 * 1.018 = 0.51409
    assert abs(c.apply_entry(0.50) - 0.51409) < 1e-4


def test_cost_apply_exit_reduces_payout():
    c = CostModel(fee_rate=0.018, exit_slippage=0.005)
    assert c.apply_exit(1.0) < 1.0
    # 1.0 * 0.995 * 0.982 = 0.97709
    assert abs(c.apply_exit(1.0) - 0.97709) < 1e-4


def test_zero_costs_pass_through():
    c = CostModel(fee_rate=0.0, entry_slippage=0.0, exit_slippage=0.0)
    assert c.apply_entry(0.5) == 0.5
    assert c.apply_exit(1.0)  == 1.0


# ── Kelly size (matches Rust impl) ───────────────────────────────────────────

def test_kelly_zero_edge_zero_bet():
    # p_model == price → no edge → zero bet
    assert kelly_size(0.5, 0.5, 1500, 0.25, 30) == 0.0


def test_kelly_negative_edge_zero_bet():
    assert kelly_size(0.4, 0.5, 1500, 0.25, 30) == 0.0


def test_kelly_capped_at_max():
    bet = kelly_size(0.95, 0.05, 100_000, 1.0, 50)
    assert bet == 50.0


def test_kelly_invalid_inputs_zero():
    assert kelly_size(0.5, 0.0,  1500, 0.25, 30) == 0.0
    assert kelly_size(0.5, 1.0,  1500, 0.25, 30) == 0.0
    assert kelly_size(-0.1, 0.5, 1500, 0.25, 30) == 0.0
    assert kelly_size(0.5, 0.5,  -100, 0.25, 30) == 0.0


# ── Market simulation ────────────────────────────────────────────────────────

def test_simulate_market_perfectly_informed_matches_truth():
    rng = np.random.default_rng(0)
    p_true = 0.7
    # alpha=0 → market matches truth (with small noise)
    samples = [simulate_market_price(p_true, alpha=0.0, rng=rng) for _ in range(1000)]
    assert abs(np.mean(samples) - p_true) < 0.02


def test_simulate_market_uninformed_centers_on_half():
    rng = np.random.default_rng(0)
    samples = [simulate_market_price(0.7, alpha=1.0, rng=rng) for _ in range(1000)]
    assert abs(np.mean(samples) - 0.5) < 0.02


def test_simulate_market_clipped_to_valid_range():
    rng = np.random.default_rng(0)
    for _ in range(200):
        v = simulate_market_price(0.99, alpha=0.0, rng=rng)
        assert 0.02 <= v <= 0.98


# ── simulate_one_alpha ──────────────────────────────────────────────────────

def _toy_data(n: int = 200, signal: bool = True, seed: int = 7):
    """Synthetic predictions and labels with optional signal."""
    rng = np.random.default_rng(seed)
    p_model = rng.uniform(0.3, 0.7, n)
    timestamps = np.arange(n) * 5 * 60_000  # 5-min spacing
    if signal:
        # outcome with 70% correlation to p_model
        label = ((p_model > 0.5) ^ (rng.random(n) > 0.85)).astype(int)
    else:
        label = rng.integers(0, 2, n)
    return p_model, label, timestamps


def test_simulate_one_alpha_returns_alpha_result():
    p_model, label, ts = _toy_data(n=300)
    result = simulate_one_alpha(
        p_model, label, ts, alpha=0.5,
        cost=CostModel(), params=BacktestParams(),
    )
    assert isinstance(result, AlphaResult)
    assert result.alpha == 0.5
    assert result.n_trades >= 0
    assert 0 <= result.win_rate <= 1


def test_simulate_one_alpha_no_trades_when_no_edge():
    """alpha=0 means market = truth, so model has no edge → few/zero trades."""
    p_model, label, ts = _toy_data(n=300, signal=False)
    # Force p_model into a tight band so no trades clear the threshold
    p_model = np.full_like(p_model, 0.5)
    result = simulate_one_alpha(
        p_model, label, ts, alpha=0.0,
        cost=CostModel(), params=BacktestParams(edge_threshold=0.04),
    )
    # With p_model=0.5, market also ~0.5 → edge ~ 0 → almost no trades
    assert result.n_trades < 20


def test_simulate_one_alpha_more_trades_when_market_uninformed():
    p_model, label, ts = _toy_data(n=300, signal=True)
    inf  = simulate_one_alpha(p_model, label, ts, alpha=0.0,
                              cost=CostModel(), params=BacktestParams())
    uninf = simulate_one_alpha(p_model, label, ts, alpha=1.0,
                               cost=CostModel(), params=BacktestParams())
    # When market is uninformed, our model has more apparent edge
    assert uninf.n_trades >= inf.n_trades


def test_kill_switch_trips_on_daily_loss():
    """Force losses to exceed the daily limit; verify days_killed > 0."""
    n = 500
    rng = np.random.default_rng(0)
    # Model strongly predicts wrong outcome
    p_model = np.full(n, 0.95)
    label   = np.zeros(n, dtype=int)  # always lose
    timestamps = np.arange(n) * 5 * 60_000

    result = simulate_one_alpha(
        p_model, label, timestamps, alpha=1.0,
        cost=CostModel(),
        params=BacktestParams(daily_loss_limit=20.0),  # tight limit
    )
    # We're losing every trade, alpha=1 makes market underpriced → high edge
    # → many bets → daily loss trips quickly → days_killed > 0
    assert result.days_killed >= 1


def test_pass_criteria_evaluated():
    p_model, label, ts = _toy_data(n=300)
    result = simulate_one_alpha(
        p_model, label, ts, alpha=0.5,
        cost=CostModel(), params=BacktestParams(),
    )
    # Either passes or fails; both are valid responses
    assert isinstance(result.passed_criteria, bool)


def test_no_trades_returns_safe_zero_metrics():
    n = 50
    p_model    = np.full(n, 0.51)
    label      = np.zeros(n, dtype=int)
    timestamps = np.arange(n) * 5 * 60_000
    # alpha=0.0 + threshold=0.50 ensures no trade fires
    result = simulate_one_alpha(
        p_model, label, timestamps, alpha=0.0,
        cost=CostModel(),
        params=BacktestParams(edge_threshold=0.50),
    )
    assert result.n_trades == 0
    assert result.sharpe_annual == 0.0
    assert result.max_drawdown == 0.0


def test_realistic_signal_produces_positive_pnl_at_alpha_1():
    """Sanity: with TRUE signal model + uninformed market → must make money."""
    n = 1000
    rng = np.random.default_rng(123)
    # Strong signal: outcome strongly correlated with p_model
    p_model = rng.uniform(0.2, 0.8, n)
    label = (rng.random(n) < p_model).astype(int)  # outcome generated FROM p_model
    timestamps = np.arange(n) * 5 * 60_000

    result = simulate_one_alpha(
        p_model, label, timestamps, alpha=1.0,
        cost=CostModel(fee_rate=0.0, entry_slippage=0.0, exit_slippage=0.0),
        params=BacktestParams(),
    )
    # No-cost world with truly calibrated model and uninformed market → +PnL
    assert result.total_pnl > 0, f"expected positive pnl, got {result.total_pnl}"
    assert result.win_rate > 0.5
