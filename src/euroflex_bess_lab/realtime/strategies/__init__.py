"""Real-time strategies. Baselines live here; advanced strategies are added as separate modules."""

from __future__ import annotations

from .base import BaseStrategy, IdleStrategy, Strategy
from .forecast_mpc import CachedForecaster, ForecastMPCStrategy, MPCConfig
from .oracle import OracleQHStrategy, solve_qh_arbitrage
from .prism import MechanismSwitchStrategy, PrismStrategy
from .rl import DQNConfig, DQNPolicy, RLStrategy, train_dqn
from .threshold import ThresholdStrategy

__all__ = [
    "BaseStrategy",
    "CachedForecaster",
    "DQNConfig",
    "DQNPolicy",
    "ForecastMPCStrategy",
    "IdleStrategy",
    "MPCConfig",
    "MechanismSwitchStrategy",
    "OracleQHStrategy",
    "PrismStrategy",
    "RLStrategy",
    "Strategy",
    "ThresholdStrategy",
    "solve_qh_arbitrage",
    "train_dqn",
]
