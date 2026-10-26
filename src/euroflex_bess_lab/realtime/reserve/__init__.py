"""Reserve layer of the real-time engine: aFRR capacity + aFRR energy + implicit balancing on one battery.

Per 4-hour delivery block the battery is "virtually split": ``up_mw`` / ``down_mw`` are sold as aFRR capacity
(decided before the D-1 auction gate closure), they follow the TSO's activation minute by minute, and the
remaining power trades implicitly (any core :class:`~euroflex_bess_lab.realtime.strategies.base.Strategy`) under
one shared SOC. See ``docs/realtime_reserves.md`` for the modelling choices and approximations.
"""

from __future__ import annotations

from .activation import (
    ActivationModel,
    ActivationSeries,
    activation_diagnostics,
    build_activation,
    reconstruct_minute_price,
)
from .allocation import (
    SYMMETRIC_CANDIDATES,
    AllocationAudit,
    AllocationDecision,
    ForecastSplitPolicy,
    SplitCandidate,
    check_allocation_causality,
    offers_from_choice,
    oracle_decision,
)
from .capacity import AwardModel, CapacityAwards, DeliveryBlocks, forecast_capacity_prices
from .engine import (
    COMPONENT_COLUMNS,
    ReserveLookaheadAudit,
    ReserveSettings,
    ReserveSimulationResult,
    ReserveSplitStrategy,
    check_reserve_no_lookahead,
    ledger_window,
    revenue_stack,
    settle_reserve_quarter_hours,
    simulate_reserve,
)
from .plan import ReservePlan
from .products import (
    MARI_GO_LIVE_UTC,
    PICASSO_GO_LIVE_UTC,
    AfrrProduct,
    cap_reservation,
    implicit_power_limits,
    max_symmetric_reserve_mw,
    reserve_is_feasible,
    reserve_soc_band,
)
from .study import PolicySuite, ReserveStudyConfig, monthly_allocation, run_policy_suite, save_ledger, stack_table
from .synthetic import synthetic_reserve_inputs

__all__ = [
    "COMPONENT_COLUMNS",
    "MARI_GO_LIVE_UTC",
    "PICASSO_GO_LIVE_UTC",
    "SYMMETRIC_CANDIDATES",
    "ActivationModel",
    "ActivationSeries",
    "AfrrProduct",
    "AllocationAudit",
    "AllocationDecision",
    "AwardModel",
    "CapacityAwards",
    "DeliveryBlocks",
    "ForecastSplitPolicy",
    "PolicySuite",
    "ReserveLookaheadAudit",
    "ReservePlan",
    "ReserveSettings",
    "ReserveSimulationResult",
    "ReserveSplitStrategy",
    "ReserveStudyConfig",
    "SplitCandidate",
    "activation_diagnostics",
    "build_activation",
    "cap_reservation",
    "check_allocation_causality",
    "check_reserve_no_lookahead",
    "forecast_capacity_prices",
    "implicit_power_limits",
    "ledger_window",
    "max_symmetric_reserve_mw",
    "monthly_allocation",
    "offers_from_choice",
    "oracle_decision",
    "reconstruct_minute_price",
    "reserve_is_feasible",
    "reserve_soc_band",
    "revenue_stack",
    "run_policy_suite",
    "save_ledger",
    "settle_reserve_quarter_hours",
    "simulate_reserve",
    "stack_table",
    "synthetic_reserve_inputs",
]
