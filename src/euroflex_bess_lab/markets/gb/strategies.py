"""Named GB strategies (main causal strategies, their perfect-foresight oracles and sensitivities).

All settings were fixed on the December 2023 - June 2024 development window before the full backtest was run:
forecast = 50 % latest published day + 50 % mean of the latest 7 published days (same settlement-period slot)
plus a level correction decaying by 0.9 per SP; planning hurdle 5 GBP/MWh added to the degradation cost inside
the rolling LPs (not charged in settlement); response offers at their expected energy cost.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace

from ...realtime.types import BatteryParams
from .data import GBMarket
from .engine import (
    GBResult,
    GBRunConfig,
    GBSimulator,
    oracle_response_allocation,
    oracle_single_market,
    oracle_stack,
)
from .forecast import ForecastSettings
from .rules import StackingRules

MAIN_FORECAST = ForecastSettings(level_decay_per_sp=0.9, profile_days=7, latest_weight=0.5)
PLANNING_HURDLE_GBP_MWH = 5.0
DEFAULT_BATTERY = BatteryParams(power_mw=1.0, energy_mwh=2.0)


def _cfg(name: str, **kwargs: object) -> GBRunConfig:
    base: dict[str, object] = {"forecast": MAIN_FORECAST, "planning_hurdle_gbp_mwh": PLANNING_HURDLE_GBP_MWH}
    base.update(kwargs)
    return GBRunConfig(name=name, **base)  # type: ignore[arg-type]


MAIN_CONFIGS: dict[str, GBRunConfig] = {
    "wholesale_forecast": _cfg("wholesale_forecast", wholesale="mpc"),
    "niv_chasing": _cfg("niv_chasing", imbalance="mpc"),
    "fr_stack": _cfg("fr_stack", response="forecast", imbalance="soc_rule", planning_hurdle_gbp_mwh=0.0),
    "combined_stack": _cfg("combined_stack", wholesale="mpc", imbalance="mpc", response="forecast"),
}

SENSITIVITY_CONFIGS: dict[str, GBRunConfig] = {
    "niv_chasing_period_start": _cfg("niv_chasing_period_start", imbalance="mpc", imbalance_lead_sps=0),
    "niv_chasing_no_spread_term": _cfg(
        "niv_chasing_no_spread_term", imbalance="mpc", forecast=replace(MAIN_FORECAST, use_spread=False)
    ),
    "wholesale_gate_closure": _cfg("wholesale_gate_closure", wholesale="mpc", wholesale_lead_sps=2),
    "wholesale_naive_persistence": _cfg(
        "wholesale_naive_persistence",
        wholesale="mpc",
        forecast=ForecastSettings(level_decay_per_sp=0.0, profile_days=1, latest_weight=1.0),
        planning_hurdle_gbp_mwh=0.0,
    ),
    "fr_stack_floor_bids": replace(MAIN_CONFIGS["fr_stack"], name="fr_stack_floor_bids", reserve_price="floor"),
    "fr_stack_market_cap_only": replace(
        MAIN_CONFIGS["fr_stack"],
        name="fr_stack_market_cap_only",
        rules=StackingRules(reserve_mode="import_plus_export"),
    ),
    "fr_stack_reserve_export_only": replace(
        MAIN_CONFIGS["fr_stack"],
        name="fr_stack_reserve_export_only",
        rules=StackingRules(reserve_mode="export_only"),
    ),
    "combined_stack_market_cap_only": replace(
        MAIN_CONFIGS["combined_stack"],
        name="combined_stack_market_cap_only",
        rules=StackingRules(reserve_mode="import_plus_export"),
    ),
}


@dataclass(frozen=True)
class StrategyInfo:
    label: str
    kind: str  # "causal" | "oracle" | "sensitivity"
    oracle: str | None = None
    description: str = ""


STRATEGIES: dict[str, StrategyInfo] = {
    "wholesale_forecast": StrategyInfo(
        "Wholesale arbitrage on MID (forecast)",
        "causal",
        "wholesale_oracle",
        "Position of each SP fixed 9 h ahead (opening of the MID trading window) by a rolling 24 h LP on MID "
        "forecasts; settled at the realised MID (traded-price proxy).",
    ),
    "niv_chasing": StrategyInfo(
        "NIV chasing on system prices (gate closure)",
        "causal",
        "imbalance_oracle",
        "Physical baseline of each SP fixed at gate closure (60 min ahead) by a rolling 24 h LP on system-price "
        "forecasts built from published prices only; all energy settled at the single imbalance price.",
    ),
    "fr_stack": StrategyInfo(
        "Frequency-response stacking (DC/DM/DR)",
        "causal",
        "fr_oracle",
        "MW per EFA block and product chosen at the 14:00 EAC gate closure from the previous day's clearing "
        "prices, offered at the expected energy cost; SOC steered back into the required band through the "
        "baseline at gate closure (settled at the imbalance price).",
    ),
    "combined_stack": StrategyInfo(
        "Combined stack (response + wholesale + NIV)",
        "causal",
        "combined_oracle",
        "Response allocation jointly planned with a forecast-MID baseline, wholesale positions fixed 9 h ahead "
        "around the contracted headroom, and a gate-closure re-optimisation of the physical baseline on "
        "system-price forecasts (NIV chasing and SOC recovery).",
    ),
    "wholesale_oracle": StrategyInfo("Wholesale arbitrage on MID (perfect foresight)", "oracle"),
    "imbalance_oracle": StrategyInfo("System-price arbitrage (perfect foresight)", "oracle"),
    "fr_oracle": StrategyInfo("Frequency-response allocation (perfect foresight of clearing prices)", "oracle"),
    "combined_oracle": StrategyInfo("Combined stack (perfect foresight)", "oracle"),
    "niv_chasing_period_start": StrategyInfo(
        "NIV chasing decided at the start of the SP (non-BM asset)", "sensitivity", "imbalance_oracle"
    ),
    "niv_chasing_no_spread_term": StrategyInfo(
        "NIV chasing without the published-spread term", "sensitivity", "imbalance_oracle"
    ),
    "wholesale_gate_closure": StrategyInfo(
        "Wholesale on MID with positions fixed at gate closure", "sensitivity", "wholesale_oracle"
    ),
    "wholesale_naive_persistence": StrategyInfo(
        "Wholesale on MID, naive previous-day persistence, no hurdle", "sensitivity", "wholesale_oracle"
    ),
    "fr_stack_floor_bids": StrategyInfo(
        "Frequency response with offers at the price floor (always accepted)", "sensitivity", "fr_oracle"
    ),
    "fr_stack_market_cap_only": StrategyInfo(
        "Frequency response with only the market stacking cap (offered + reserved MW within 2 MW)",
        "sensitivity",
        "fr_oracle",
    ),
    "fr_stack_reserve_export_only": StrategyInfo(
        "Frequency response, offered + reserved MW of both directions within 1 MW", "sensitivity", "fr_oracle"
    ),
    "combined_stack_market_cap_only": StrategyInfo(
        "Combined stack with only the market stacking cap", "sensitivity", "combined_oracle"
    ),
}


def run_strategy(name: str, market: GBMarket, battery: BatteryParams = DEFAULT_BATTERY) -> GBResult:
    """Run one named strategy (causal, oracle or sensitivity) on ``market``."""

    runners: dict[str, Callable[[], GBResult]] = {
        "wholesale_oracle": lambda: oracle_single_market(market, battery, "mid", "wholesale_oracle"),
        "imbalance_oracle": lambda: oracle_single_market(market, battery, "sip", "imbalance_oracle"),
        "combined_oracle": lambda: oracle_stack(market, battery, name="combined_oracle"),
        "fr_oracle": lambda: _fr_oracle(market, battery),
    }
    if name in runners:
        return runners[name]()
    config = MAIN_CONFIGS.get(name) or SENSITIVITY_CONFIGS.get(name)
    if config is None:
        raise KeyError(f"unknown GB strategy {name!r}; expected one of {sorted(STRATEGIES)}")
    return GBSimulator(market, battery, config).run()


def _fr_oracle(market: GBMarket, battery: BatteryParams) -> GBResult:
    config = replace(MAIN_CONFIGS["fr_stack"], name="fr_oracle")
    contracts = oracle_response_allocation(market, battery, config)
    return GBSimulator(market, battery, config).run(preset_contracts=contracts)
