"""H6 — Hidden Markov Model regime detector.

A 3-state Gaussian HMM fit on (vol_5m, vol_30m) identifies the current
volatility regime. Used by the live signal engine to gate the ML signal:
  - LOW_VOL_RANGING / LOW_VOL_TRENDING → ML signal allowed to fire
  - HIGH_VOL_EVENT  → ML signal blocked (oracle/intramarket arb still allowed)

Belt-and-braces: the live Rust regime gate (signals/regime.rs) uses
simple z-score thresholds. Both must agree before ML fires. HMM can
detect smoother regime transitions than thresholds; thresholds catch
sudden spikes the HMM may have already drifted away from.

Persistence:
  data/models/hmm_regime.pkl
"""
from __future__ import annotations

import logging
import pickle
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from hmmlearn import hmm

log = logging.getLogger(__name__)


class Regime(Enum):
    LOW_VOL_RANGING  = 0
    LOW_VOL_TRENDING = 1
    HIGH_VOL_EVENT   = 2


@dataclass
class RegimeDetector:
    """Wraps a fitted GaussianHMM with state-to-Regime label mapping.

    The mapping is determined post-fit by inspecting the means_ matrix —
    we don't know which numeric state the HMM will assign to "high vol",
    so we identify it by finding the state with the largest vol_5m mean.
    """
    model:               hmm.GaussianHMM
    high_vol_state:      int
    state_to_regime:     dict[int, Regime]
    converged:           bool

    def predict_current(self, vol_5m_30m: np.ndarray) -> Regime:
        """Predict the regime at the most recent observation.

        Args:
          vol_5m_30m: (T, 2) array of [vol_5m, vol_30m] history.
                       Must have at least 1 row; longer is fine.
        """
        if vol_5m_30m.ndim != 2 or vol_5m_30m.shape[1] != 2:
            raise ValueError(f"expected (T, 2) array, got {vol_5m_30m.shape}")
        if len(vol_5m_30m) < 1:
            return Regime.LOW_VOL_RANGING  # safe fallback
        states = self.model.predict(vol_5m_30m)
        return self.state_to_regime.get(int(states[-1]), Regime.LOW_VOL_RANGING)

    def is_tradeable(self, vol_5m_30m: np.ndarray) -> bool:
        return self.predict_current(vol_5m_30m) != Regime.HIGH_VOL_EVENT

    def save(self, path: Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as f:
            pickle.dump(self, f)

    @classmethod
    def load(cls, path: Path) -> "RegimeDetector":
        with open(path, "rb") as f:
            return pickle.load(f)


# ── Fitting ──────────────────────────────────────────────────────────────────

def fit_regime_hmm(
    feature_df:   pd.DataFrame,
    *,
    n_iter:       int = 100,
    random_state: int = 42,
) -> RegimeDetector:
    """Fit a 3-state Gaussian HMM on (vol_5m, vol_30m).

    `feature_df` must contain 'vol_5m' and 'vol_30m' columns.
    Drops rows with NaN before fitting.
    """
    if "vol_5m" not in feature_df.columns or "vol_30m" not in feature_df.columns:
        raise ValueError("feature_df must contain vol_5m and vol_30m")

    obs = feature_df[["vol_5m", "vol_30m"]].dropna().to_numpy()
    if len(obs) < 100:
        raise ValueError(f"need at least 100 rows for HMM, got {len(obs)}")

    model = hmm.GaussianHMM(
        n_components=3,
        covariance_type="full",
        n_iter=n_iter,
        random_state=random_state,
    )
    model.fit(obs)

    if not model.monitor_.converged:
        log.warning("HMM did not converge after %d iterations", n_iter)

    # Identify high-vol state by the largest vol_5m mean (column 0)
    means = model.means_  # shape (3, 2)
    high_vol_state = int(np.argmax(means[:, 0]))

    # Among the two non-high-vol states, the one with HIGHER vol gets RANGING
    # (less directional momentum); the lower-vol one gets TRENDING. This is a
    # heuristic — fine to flip if Brier shows it backwards in practice.
    other_states = [i for i in range(3) if i != high_vol_state]
    other_states_sorted = sorted(other_states, key=lambda s: means[s, 0])
    state_to_regime = {
        other_states_sorted[0]: Regime.LOW_VOL_TRENDING,
        other_states_sorted[1]: Regime.LOW_VOL_RANGING,
        high_vol_state:         Regime.HIGH_VOL_EVENT,
    }

    log.info(
        "HMM fitted: high_vol_state=%d, means=%s, mapping=%s",
        high_vol_state, means.tolist(),
        {s: r.name for s, r in state_to_regime.items()},
    )

    return RegimeDetector(
        model=model,
        high_vol_state=high_vol_state,
        state_to_regime=state_to_regime,
        converged=bool(model.monitor_.converged),
    )
