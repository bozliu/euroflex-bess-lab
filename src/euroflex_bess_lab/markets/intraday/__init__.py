"""DE-LU day-ahead + continuous-intraday two-stage battery strategy with a pluggable intraday price source.

Typical use::

    from euroflex_bess_lab.markets.intraday import IntradayConfig, IntradayMarket, run_intraday_backtest

    market = IntradayMarket.from_public("2026-01-01", "2026-01-31")  # DE-LU DA + local ID AEP
    result = run_intraday_backtest(market, IntradayConfig(), days=market.days[35:])
    print(result.summary())

Execution at the TSO ID AEP index is a VWAP-execution *proxy*; plug licensed EPEX data in through
:class:`FrameIntradaySource`. See ``docs/intraday.md``.
"""

from __future__ import annotations

from .backtest import (
    DAY_COLUMNS,
    DaySchedule,
    IntradayBacktest,
    audit_causality,
    committed_decisions,
    run_day,
    run_intraday_backtest,
    run_parallel,
    schedules_to_frame,
    summarize_daily,
)
from .config import (
    DEFAULT_STRATEGIES,
    MAIN_STRATEGY,
    STRATEGIES,
    ForecastSettings,
    IntradayConfig,
    IntradayTiming,
    StrategySpec,
    TradingCosts,
    default_battery,
)
from .data import (
    FrameIntradaySource,
    IdAepIndexSource,
    IntradayMarket,
    IntradayPriceSource,
    SyntheticIntradaySource,
    local_midnight_utc,
)
from .forecast import DaySpreadModel, LiveForecaster, fit_spread_model, persistence_day_ahead
from .lp import DaPlan, IntradayDayLP, JointPlan, LpError, solve_da_schedule, solve_oracle_da_id
from .synthetic import synthetic_day_ahead, synthetic_market

__all__ = [
    "DAY_COLUMNS",
    "DEFAULT_STRATEGIES",
    "MAIN_STRATEGY",
    "STRATEGIES",
    "DaPlan",
    "DaySchedule",
    "DaySpreadModel",
    "ForecastSettings",
    "FrameIntradaySource",
    "IdAepIndexSource",
    "IntradayBacktest",
    "IntradayConfig",
    "IntradayDayLP",
    "IntradayMarket",
    "IntradayPriceSource",
    "IntradayTiming",
    "JointPlan",
    "LiveForecaster",
    "LpError",
    "StrategySpec",
    "SyntheticIntradaySource",
    "TradingCosts",
    "audit_causality",
    "committed_decisions",
    "default_battery",
    "fit_spread_model",
    "local_midnight_utc",
    "persistence_day_ahead",
    "run_day",
    "run_intraday_backtest",
    "run_parallel",
    "schedules_to_frame",
    "solve_da_schedule",
    "solve_oracle_da_id",
    "summarize_daily",
    "synthetic_day_ahead",
    "synthetic_market",
]
