"""Great Britain battery market model: 30-minute settlement, single cash-out price, MID wholesale proxy and the
NESO dynamic response services (DC/DM/DR) procured in the day-ahead EAC auctions.

Self-contained research module (not yet wired into the framework adapters). See ``docs/great_britain.md`` and
``scripts/research/gb_backtest.py``::

    from euroflex_bess_lab.markets.gb import GBMarket, run_strategy

    market = GBMarket.from_public("2025-06-01", "2025-07-01")
    result = run_strategy("combined_stack", market)
    result.ledger[["revenue_wholesale_gbp", "revenue_response_gbp", "pnl_gbp"]].sum()
"""

from __future__ import annotations

from .data import GBMarket
from .engine import GBResult, GBRunConfig, GBSimulator, ReplayPlan, oracle_single_market, oracle_stack
from .forecast import ForecastSettings, PriceForecaster
from .lp import StackProblem, StorageLP, rolling_oracle, solve_stack_lp
from .rules import RESPONSE_PRODUCTS, StackingRules
from .strategies import MAIN_CONFIGS, SENSITIVITY_CONFIGS, STRATEGIES, run_strategy

__all__ = [
    "MAIN_CONFIGS",
    "RESPONSE_PRODUCTS",
    "SENSITIVITY_CONFIGS",
    "STRATEGIES",
    "ForecastSettings",
    "GBMarket",
    "GBResult",
    "GBRunConfig",
    "GBSimulator",
    "PriceForecaster",
    "ReplayPlan",
    "StackProblem",
    "StackingRules",
    "StorageLP",
    "oracle_single_market",
    "oracle_stack",
    "rolling_oracle",
    "run_strategy",
    "solve_stack_lp",
]
