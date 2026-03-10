from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd
from pyomo.environ import (
    Binary,
    ConcreteModel,
    Constraint,
    NonNegativeReals,
    Objective,
    RangeSet,
    Reals,
    SolverFactory,
    Var,
    maximize,
    value,
)
from pyomo.opt import SolverStatus, TerminationCondition

from ..types import AssetSpec, BatterySpec, SiteSpec


@dataclass
class OptimizationOutput:
    dispatch: pd.DataFrame
    objective_value_eur: float
    solver_name: str
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass
class PortfolioOptimizationOutput:
    site_dispatch: pd.DataFrame
    asset_dispatch: pd.DataFrame
    objective_value_eur: float
    solver_name: str
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class AfrrSeries:
    capacity_up_prices: pd.Series
    capacity_down_prices: pd.Series
    activation_up_prices: pd.Series
    activation_down_prices: pd.Series
    activation_ratio_up: pd.Series
    activation_ratio_down: pd.Series


@dataclass(frozen=True)
class ScenarioMarketSeries:
    scenario_ids: tuple[str, ...]
    weights: pd.Series
    values: pd.DataFrame

    def expected_series(self) -> pd.Series:
        weighted = self.values.mul(self.weights.reindex(self.values.index), axis=0)
        return weighted.sum(axis=0)


@dataclass(frozen=True)
class ScenarioAfrrSeries:
    capacity_up_prices: ScenarioMarketSeries
    capacity_down_prices: ScenarioMarketSeries
    activation_up_prices: ScenarioMarketSeries
    activation_down_prices: ScenarioMarketSeries
    activation_ratio_up: ScenarioMarketSeries
    activation_ratio_down: ScenarioMarketSeries


@dataclass(frozen=True)
class RiskPreference:
    mode: str = "expected_value"
    penalty_lambda: float = 0.0
    tail_alpha: float | None = None


def _validate_scenario_series(reference: ScenarioMarketSeries, *others: ScenarioMarketSeries | None) -> None:
    for other in others:
        if other is None:
            continue
        if other.scenario_ids != reference.scenario_ids:
            raise ValueError("Scenario bundles must use the same scenario_id set across all forecast series")
        left = reference.weights.reindex(reference.scenario_ids)
        right = other.weights.reindex(other.scenario_ids)
        if not left.equals(right):
            raise ValueError("Scenario bundles must use the same scenario weights across all forecast series")


def _zero_scenario_series(reference: ScenarioMarketSeries) -> ScenarioMarketSeries:
    return ScenarioMarketSeries(
        scenario_ids=reference.scenario_ids,
        weights=reference.weights.copy(),
        values=pd.DataFrame(0.0, index=reference.scenario_ids, columns=reference.values.columns),
    )


def ensure_solver_available() -> str:
    for candidate in ("appsi_highs", "highs"):
        solver = SolverFactory(candidate)
        if solver is not None and solver.available(exception_flag=False):
            return candidate
    raise RuntimeError(
        "No supported MILP solver is available. Install `highspy` inside the `dl` conda env to enable Pyomo + HiGHS."
    )


def _solve_single_dispatch_problem(
    *,
    market_frame: pd.DataFrame,
    battery: BatterySpec,
    objective_prices: pd.Series,
    strategy_name: str,
    baseline_net_export_mw: pd.Series | None = None,
    fcr_capacity_prices: pd.Series | None = None,
    fixed_fcr_reserved_mw: pd.Series | None = None,
    afrr_series: AfrrSeries | None = None,
    fixed_afrr_up_reserved_mw: pd.Series | None = None,
    fixed_afrr_down_reserved_mw: pd.Series | None = None,
    reserve_sustain_duration_hours: float | None = None,
    reserve_penalty_eur_per_mw: float = 0.0,
    initial_soc_mwh: float | None = None,
    terminal_soc_mwh: float | None = None,
    degradation_cost_eur_per_mwh: float = 0.0,
) -> OptimizationOutput:
    solver_name = ensure_solver_available()
    frame = market_frame.sort_values("timestamp_utc").reset_index(drop=True).copy()
    frame["timestamp_utc"] = pd.to_datetime(frame["timestamp_utc"], utc=True)
    timestamps = pd.DatetimeIndex(frame["timestamp_utc"])
    availability = battery.availability.to_series(timestamps).astype(float)
    power_limits = battery.effective_power_limit_mw * availability
    dt_hours = float(frame["resolution_minutes"].iloc[0]) / 60.0
    horizon = len(frame)
    baseline = baseline_net_export_mw if baseline_net_export_mw is not None else pd.Series(0.0, index=frame.index)
    reserve_prices = (
        fcr_capacity_prices if fcr_capacity_prices is not None else pd.Series(0.0, index=frame.index, dtype=float)
    )
    fixed_reserve = (
        fixed_fcr_reserved_mw.reset_index(drop=True).astype(float)
        if fixed_fcr_reserved_mw is not None
        else pd.Series(0.0, index=frame.index, dtype=float)
    )
    fixed_afrr_up = (
        fixed_afrr_up_reserved_mw.reset_index(drop=True).astype(float)
        if fixed_afrr_up_reserved_mw is not None
        else pd.Series(0.0, index=frame.index, dtype=float)
    )
    fixed_afrr_down = (
        fixed_afrr_down_reserved_mw.reset_index(drop=True).astype(float)
        if fixed_afrr_down_reserved_mw is not None
        else pd.Series(0.0, index=frame.index, dtype=float)
    )
    reserve_enabled = reserve_sustain_duration_hours is not None or fixed_fcr_reserved_mw is not None
    afrr_enabled = (
        afrr_series is not None or fixed_afrr_up_reserved_mw is not None or fixed_afrr_down_reserved_mw is not None
    )
    reserve_duration_hours = float(reserve_sustain_duration_hours or 0.0)
    if afrr_series is None:
        afrr_series = AfrrSeries(
            capacity_up_prices=pd.Series(0.0, index=frame.index, dtype=float),
            capacity_down_prices=pd.Series(0.0, index=frame.index, dtype=float),
            activation_up_prices=pd.Series(0.0, index=frame.index, dtype=float),
            activation_down_prices=pd.Series(0.0, index=frame.index, dtype=float),
            activation_ratio_up=pd.Series(0.0, index=frame.index, dtype=float),
            activation_ratio_down=pd.Series(0.0, index=frame.index, dtype=float),
        )

    effective_initial_soc = battery.initial_soc_mwh if initial_soc_mwh is None else initial_soc_mwh
    effective_terminal_soc = battery.terminal_soc_mwh if terminal_soc_mwh is None else terminal_soc_mwh

    model = ConcreteModel()
    model.T = RangeSet(0, horizon - 1)
    model.S = RangeSet(0, horizon)
    model.charge = Var(model.T, domain=NonNegativeReals)
    model.discharge = Var(model.T, domain=NonNegativeReals)
    model.fcr_reserved = Var(model.T, domain=NonNegativeReals)
    model.afrr_up_reserved = Var(model.T, domain=NonNegativeReals)
    model.afrr_down_reserved = Var(model.T, domain=NonNegativeReals)
    model.mode = Var(model.T, domain=Binary)
    model.soc = Var(model.S, domain=Reals)

    model.initial_soc = Constraint(expr=model.soc[0] == effective_initial_soc)
    if effective_terminal_soc is not None:
        model.terminal_soc = Constraint(expr=model.soc[horizon] == effective_terminal_soc)

    def soc_bounds_rule(model: ConcreteModel, s: int) -> Constraint:
        return (battery.effective_soc_min_mwh, model.soc[s], battery.effective_soc_max_mwh)

    model.soc_bounds = Constraint(model.S, rule=soc_bounds_rule)

    def charge_limit_rule(model: ConcreteModel, t: int) -> Constraint:
        return model.charge[t] <= float(power_limits.iloc[t]) * model.mode[t]

    def discharge_limit_rule(model: ConcreteModel, t: int) -> Constraint:
        return model.discharge[t] <= float(power_limits.iloc[t]) * (1 - model.mode[t])

    def reserve_limit_rule(model: ConcreteModel, t: int) -> Constraint:
        if not reserve_enabled:
            return model.fcr_reserved[t] == 0.0
        if fixed_fcr_reserved_mw is not None:
            return model.fcr_reserved[t] == float(fixed_reserve.iloc[t])
        return model.fcr_reserved[t] <= float(power_limits.iloc[t])

    def afrr_up_limit_rule(model: ConcreteModel, t: int) -> Constraint:
        if not afrr_enabled:
            return model.afrr_up_reserved[t] == 0.0
        if fixed_afrr_up_reserved_mw is not None:
            return model.afrr_up_reserved[t] == float(fixed_afrr_up.iloc[t])
        return model.afrr_up_reserved[t] <= float(power_limits.iloc[t])

    def afrr_down_limit_rule(model: ConcreteModel, t: int) -> Constraint:
        if not afrr_enabled:
            return model.afrr_down_reserved[t] == 0.0
        if fixed_afrr_down_reserved_mw is not None:
            return model.afrr_down_reserved[t] == float(fixed_afrr_down.iloc[t])
        return model.afrr_down_reserved[t] <= float(power_limits.iloc[t])

    def reserve_charge_headroom_rule(model: ConcreteModel, t: int) -> Constraint:
        return model.charge[t] + model.fcr_reserved[t] + model.afrr_down_reserved[t] <= float(power_limits.iloc[t])

    def reserve_discharge_headroom_rule(model: ConcreteModel, t: int) -> Constraint:
        return model.discharge[t] + model.fcr_reserved[t] + model.afrr_up_reserved[t] <= float(power_limits.iloc[t])

    def reserve_soc_floor_rule(model: ConcreteModel, t: int) -> Constraint:
        if not reserve_enabled:
            return Constraint.Skip
        return (
            model.soc[t] - model.fcr_reserved[t] * reserve_duration_hours / battery.discharge_efficiency
            >= battery.effective_soc_min_mwh
        )

    def reserve_soc_ceiling_rule(model: ConcreteModel, t: int) -> Constraint:
        if not reserve_enabled:
            return Constraint.Skip
        return (
            model.soc[t] + model.fcr_reserved[t] * reserve_duration_hours * battery.charge_efficiency
            <= battery.effective_soc_max_mwh
        )

    def afrr_soc_floor_rule(model: ConcreteModel, t: int) -> Constraint:
        if not afrr_enabled:
            return Constraint.Skip
        return (
            model.soc[t] - model.afrr_up_reserved[t] * reserve_duration_hours / battery.discharge_efficiency
            >= battery.effective_soc_min_mwh
        )

    def afrr_soc_ceiling_rule(model: ConcreteModel, t: int) -> Constraint:
        if not afrr_enabled:
            return Constraint.Skip
        return (
            model.soc[t] + model.afrr_down_reserved[t] * reserve_duration_hours * battery.charge_efficiency
            <= battery.effective_soc_max_mwh
        )

    def soc_transition_rule(model: ConcreteModel, t: int) -> Constraint:
        return (
            model.soc[t + 1]
            == model.soc[t]
            + (
                model.charge[t] * battery.charge_efficiency
                - model.discharge[t] / battery.discharge_efficiency
                - model.afrr_up_reserved[t]
                * float(afrr_series.activation_ratio_up.iloc[t])
                / battery.discharge_efficiency
                + model.afrr_down_reserved[t]
                * float(afrr_series.activation_ratio_down.iloc[t])
                * battery.charge_efficiency
            )
            * dt_hours
        )

    model.charge_limits = Constraint(model.T, rule=charge_limit_rule)
    model.discharge_limits = Constraint(model.T, rule=discharge_limit_rule)
    model.reserve_limits = Constraint(model.T, rule=reserve_limit_rule)
    model.afrr_up_limits = Constraint(model.T, rule=afrr_up_limit_rule)
    model.afrr_down_limits = Constraint(model.T, rule=afrr_down_limit_rule)
    model.reserve_charge_headroom = Constraint(model.T, rule=reserve_charge_headroom_rule)
    model.reserve_discharge_headroom = Constraint(model.T, rule=reserve_discharge_headroom_rule)
    model.reserve_soc_floor = Constraint(model.T, rule=reserve_soc_floor_rule)
    model.reserve_soc_ceiling = Constraint(model.T, rule=reserve_soc_ceiling_rule)
    model.afrr_soc_floor = Constraint(model.T, rule=afrr_soc_floor_rule)
    model.afrr_soc_ceiling = Constraint(model.T, rule=afrr_soc_ceiling_rule)
    model.soc_transition = Constraint(model.T, rule=soc_transition_rule)

    def objective_rule(model: ConcreteModel) -> Objective:
        terms = []
        for t in model.T:
            net_export = model.discharge[t] - model.charge[t]
            revenue_component = (net_export - float(baseline.iloc[t])) * float(objective_prices.iloc[t]) * dt_hours
            reserve_revenue_component = model.fcr_reserved[t] * float(reserve_prices.iloc[t]) * dt_hours
            afrr_capacity_component = (
                model.afrr_up_reserved[t] * float(afrr_series.capacity_up_prices.iloc[t])
                + model.afrr_down_reserved[t] * float(afrr_series.capacity_down_prices.iloc[t])
            ) * dt_hours
            afrr_activation_component = (
                model.afrr_up_reserved[t]
                * float(afrr_series.activation_ratio_up.iloc[t])
                * float(afrr_series.activation_up_prices.iloc[t])
                + model.afrr_down_reserved[t]
                * float(afrr_series.activation_ratio_down.iloc[t])
                * float(afrr_series.activation_down_prices.iloc[t])
            ) * dt_hours
            reserve_penalty_component = (
                reserve_penalty_eur_per_mw
                * (model.fcr_reserved[t] + model.afrr_up_reserved[t] + model.afrr_down_reserved[t])
                * dt_hours
            )
            degradation_component = degradation_cost_eur_per_mwh * (model.charge[t] + model.discharge[t]) * dt_hours
            terms.append(
                revenue_component
                + reserve_revenue_component
                + afrr_capacity_component
                + afrr_activation_component
                - reserve_penalty_component
                - degradation_component
            )
        return sum(terms)

    model.objective = Objective(rule=objective_rule, sense=maximize)
    solver = SolverFactory(solver_name)
    result = solver.solve(model)
    if result.solver.status != SolverStatus.ok or result.solver.termination_condition not in {
        TerminationCondition.optimal,
        TerminationCondition.feasible,
    }:
        raise RuntimeError(
            f"Optimization failed with status={result.solver.status} termination={result.solver.termination_condition}"
        )

    dispatch = frame.copy()
    dispatch["charge_mw"] = [value(model.charge[t]) for t in model.T]
    dispatch["discharge_mw"] = [value(model.discharge[t]) for t in model.T]
    dispatch["soc_start_mwh"] = [value(model.soc[t]) for t in model.T]
    dispatch["soc_mwh"] = [value(model.soc[t + 1]) for t in model.T]
    dispatch["availability_factor"] = availability.values
    dispatch["power_limit_mw"] = power_limits.values
    dispatch["fcr_reserved_mw"] = [value(model.fcr_reserved[t]) for t in model.T]
    dispatch["afrr_up_reserved_mw"] = [value(model.afrr_up_reserved[t]) for t in model.T]
    dispatch["afrr_down_reserved_mw"] = [value(model.afrr_down_reserved[t]) for t in model.T]
    dispatch["reserved_capacity_mw"] = (
        dispatch["fcr_reserved_mw"] + dispatch["afrr_up_reserved_mw"] + dispatch["afrr_down_reserved_mw"]
    )
    dispatch["net_export_mw"] = dispatch["discharge_mw"] - dispatch["charge_mw"]
    dispatch["baseline_net_export_mw"] = baseline.values
    dispatch["imbalance_mw"] = dispatch["net_export_mw"] - dispatch["baseline_net_export_mw"]
    dispatch["reserve_headroom_up_mw"] = (
        dispatch["power_limit_mw"]
        - dispatch["discharge_mw"]
        - dispatch["fcr_reserved_mw"]
        - dispatch["afrr_up_reserved_mw"]
    )
    dispatch["reserve_headroom_down_mw"] = (
        dispatch["power_limit_mw"]
        - dispatch["charge_mw"]
        - dispatch["fcr_reserved_mw"]
        - dispatch["afrr_down_reserved_mw"]
    )
    dispatch["throughput_mwh"] = (dispatch["charge_mw"] + dispatch["discharge_mw"]) * dt_hours
    dispatch["expected_afrr_activated_up_mwh"] = (
        dispatch["afrr_up_reserved_mw"] * afrr_series.activation_ratio_up.reset_index(drop=True) * dt_hours
    )
    dispatch["expected_afrr_activated_down_mwh"] = (
        dispatch["afrr_down_reserved_mw"] * afrr_series.activation_ratio_down.reset_index(drop=True) * dt_hours
    )
    dispatch["strategy_name"] = strategy_name
    objective_value_eur = float(value(model.objective))
    return OptimizationOutput(dispatch=dispatch, objective_value_eur=objective_value_eur, solver_name=solver_name)


def _build_site_dispatch(
    *,
    frame: pd.DataFrame,
    site: SiteSpec,
    asset_dispatch: pd.DataFrame,
    strategy_name: str,
) -> pd.DataFrame:
    grouped = (
        asset_dispatch.groupby(["timestamp_utc", "timestamp_local"], as_index=False)[
            [
                "charge_mw",
                "discharge_mw",
                "net_export_mw",
                "fcr_reserved_mw",
                "afrr_up_reserved_mw",
                "afrr_down_reserved_mw",
                "reserved_capacity_mw",
                "reserve_headroom_up_mw",
                "reserve_headroom_down_mw",
                "throughput_mwh",
                "expected_afrr_activated_up_mwh",
                "expected_afrr_activated_down_mwh",
            ]
        ]
        .sum()
        .sort_values("timestamp_utc")
        .reset_index(drop=True)
    )
    min_soc = (
        asset_dispatch.groupby(["timestamp_utc", "timestamp_local"], as_index=False)["soc_mwh"]
        .sum()
        .sort_values("timestamp_utc")
        .reset_index(drop=True)
    )
    grouped["soc_mwh"] = min_soc["soc_mwh"].values
    grouped["soc_start_mwh"] = (
        asset_dispatch.groupby(["timestamp_utc", "timestamp_local"], as_index=False)["soc_start_mwh"]
        .sum()
        .sort_values("timestamp_utc")
        .reset_index(drop=True)["soc_start_mwh"]
        .values
    )
    grouped["power_limit_mw"] = site.poi_export_limit_mw
    grouped["availability_factor"] = 1.0
    grouped["baseline_net_export_mw"] = 0.0
    grouped["imbalance_mw"] = 0.0
    grouped["strategy_name"] = strategy_name
    grouped["site_id"] = site.id
    grouped["decision_type"] = "site_dispatch"
    return frame.merge(
        grouped[
            [
                "timestamp_utc",
                "timestamp_local",
                "site_id",
                "charge_mw",
                "discharge_mw",
                "soc_start_mwh",
                "soc_mwh",
                "availability_factor",
                "power_limit_mw",
                "fcr_reserved_mw",
                "afrr_up_reserved_mw",
                "afrr_down_reserved_mw",
                "reserved_capacity_mw",
                "net_export_mw",
                "baseline_net_export_mw",
                "imbalance_mw",
                "reserve_headroom_up_mw",
                "reserve_headroom_down_mw",
                "expected_afrr_activated_up_mwh",
                "expected_afrr_activated_down_mwh",
                "strategy_name",
                "decision_type",
            ]
        ],
        on=["timestamp_utc", "timestamp_local"],
        how="left",
    )


def _solve_portfolio_dispatch_problem(
    *,
    market_frame: pd.DataFrame,
    site: SiteSpec,
    assets: list[AssetSpec],
    objective_prices: pd.Series,
    strategy_name: str,
    fcr_capacity_prices: pd.Series | None = None,
    fixed_fcr_reserved_mw_by_asset: dict[str, pd.Series] | None = None,
    afrr_series: AfrrSeries | None = None,
    fixed_afrr_up_reserved_mw_by_asset: dict[str, pd.Series] | None = None,
    fixed_afrr_down_reserved_mw_by_asset: dict[str, pd.Series] | None = None,
    reserve_sustain_duration_hours: float | None = None,
    reserve_penalty_eur_per_mw: float = 0.0,
    degradation_costs_eur_per_mwh: dict[str, float] | None = None,
    initial_soc_mwh_by_asset: dict[str, float] | None = None,
    terminal_soc_mwh_by_asset: dict[str, float | None] | None = None,
) -> PortfolioOptimizationOutput:
    solver_name = ensure_solver_available()
    frame = market_frame.sort_values("timestamp_utc").reset_index(drop=True).copy()
    frame["timestamp_utc"] = pd.to_datetime(frame["timestamp_utc"], utc=True)
    timestamps = pd.DatetimeIndex(frame["timestamp_utc"])
    dt_hours = float(frame["resolution_minutes"].iloc[0]) / 60.0
    horizon = len(frame)
    reserve_enabled = reserve_sustain_duration_hours is not None or fixed_fcr_reserved_mw_by_asset is not None
    afrr_enabled = (
        afrr_series is not None
        or fixed_afrr_up_reserved_mw_by_asset is not None
        or fixed_afrr_down_reserved_mw_by_asset is not None
    )
    reserve_duration_hours = float(reserve_sustain_duration_hours or 0.0)
    reserve_prices = (
        fcr_capacity_prices if fcr_capacity_prices is not None else pd.Series(0.0, index=frame.index, dtype=float)
    )
    if afrr_series is None:
        afrr_series = AfrrSeries(
            capacity_up_prices=pd.Series(0.0, index=frame.index, dtype=float),
            capacity_down_prices=pd.Series(0.0, index=frame.index, dtype=float),
            activation_up_prices=pd.Series(0.0, index=frame.index, dtype=float),
            activation_down_prices=pd.Series(0.0, index=frame.index, dtype=float),
            activation_ratio_up=pd.Series(0.0, index=frame.index, dtype=float),
            activation_ratio_down=pd.Series(0.0, index=frame.index, dtype=float),
        )
    degradation_costs = degradation_costs_eur_per_mwh or {asset.id: 0.0 for asset in assets}
    asset_count = len(assets)

    availability_frames: dict[str, pd.Series] = {}
    power_limit_frames: dict[str, pd.Series] = {}
    for asset in assets:
        availability = asset.battery.availability.to_series(timestamps).astype(float)
        availability_frames[asset.id] = availability
        power_limit_frames[asset.id] = asset.battery.effective_power_limit_mw * availability

    model = ConcreteModel()
    model.A = RangeSet(0, asset_count - 1)
    model.T = RangeSet(0, horizon - 1)
    model.S = RangeSet(0, horizon)
    model.charge = Var(model.A, model.T, domain=NonNegativeReals)
    model.discharge = Var(model.A, model.T, domain=NonNegativeReals)
    model.fcr_reserved = Var(model.A, model.T, domain=NonNegativeReals)
    model.afrr_up_reserved = Var(model.A, model.T, domain=NonNegativeReals)
    model.afrr_down_reserved = Var(model.A, model.T, domain=NonNegativeReals)
    model.mode = Var(model.A, model.T, domain=Binary)
    model.soc = Var(model.A, model.S, domain=Reals)

    asset_lookup = {idx: asset for idx, asset in enumerate(assets)}
    initial_soc_lookup = initial_soc_mwh_by_asset or {asset.id: asset.battery.initial_soc_mwh for asset in assets}
    terminal_soc_lookup = terminal_soc_mwh_by_asset or {asset.id: asset.battery.terminal_soc_mwh for asset in assets}
    fixed_reserve_lookup = {
        asset.id: (
            fixed_fcr_reserved_mw_by_asset[asset.id].reset_index(drop=True).astype(float)
            if fixed_fcr_reserved_mw_by_asset is not None and asset.id in fixed_fcr_reserved_mw_by_asset
            else pd.Series(0.0, index=frame.index, dtype=float)
        )
        for asset in assets
    }
    fixed_afrr_up_lookup = {
        asset.id: (
            fixed_afrr_up_reserved_mw_by_asset[asset.id].reset_index(drop=True).astype(float)
            if fixed_afrr_up_reserved_mw_by_asset is not None and asset.id in fixed_afrr_up_reserved_mw_by_asset
            else pd.Series(0.0, index=frame.index, dtype=float)
        )
        for asset in assets
    }
    fixed_afrr_down_lookup = {
        asset.id: (
            fixed_afrr_down_reserved_mw_by_asset[asset.id].reset_index(drop=True).astype(float)
            if fixed_afrr_down_reserved_mw_by_asset is not None and asset.id in fixed_afrr_down_reserved_mw_by_asset
            else pd.Series(0.0, index=frame.index, dtype=float)
        )
        for asset in assets
    }

    def initial_soc_rule(model: ConcreteModel, a: int) -> Constraint:
        return model.soc[a, 0] == float(initial_soc_lookup[asset_lookup[a].id])

    def terminal_soc_rule(model: ConcreteModel, a: int) -> Constraint:
        battery = asset_lookup[a].battery
        terminal_soc = terminal_soc_lookup.get(asset_lookup[a].id, battery.terminal_soc_mwh)
        if terminal_soc is None:
            return Constraint.Skip
        return model.soc[a, horizon] == float(terminal_soc)

    def soc_bounds_rule(model: ConcreteModel, a: int, s: int) -> Constraint:
        battery = asset_lookup[a].battery
        return (battery.effective_soc_min_mwh, model.soc[a, s], battery.effective_soc_max_mwh)

    def charge_limit_rule(model: ConcreteModel, a: int, t: int) -> Constraint:
        asset = asset_lookup[a]
        return model.charge[a, t] <= float(power_limit_frames[asset.id].iloc[t]) * model.mode[a, t]

    def discharge_limit_rule(model: ConcreteModel, a: int, t: int) -> Constraint:
        asset = asset_lookup[a]
        return model.discharge[a, t] <= float(power_limit_frames[asset.id].iloc[t]) * (1 - model.mode[a, t])

    def reserve_limit_rule(model: ConcreteModel, a: int, t: int) -> Constraint:
        if not reserve_enabled:
            return model.fcr_reserved[a, t] == 0.0
        asset = asset_lookup[a]
        if fixed_fcr_reserved_mw_by_asset is not None:
            return model.fcr_reserved[a, t] == float(fixed_reserve_lookup[asset.id].iloc[t])
        return model.fcr_reserved[a, t] <= float(power_limit_frames[asset.id].iloc[t])

    def afrr_up_limit_rule(model: ConcreteModel, a: int, t: int) -> Constraint:
        if not afrr_enabled:
            return model.afrr_up_reserved[a, t] == 0.0
        asset = asset_lookup[a]
        if fixed_afrr_up_reserved_mw_by_asset is not None:
            return model.afrr_up_reserved[a, t] == float(fixed_afrr_up_lookup[asset.id].iloc[t])
        return model.afrr_up_reserved[a, t] <= float(power_limit_frames[asset.id].iloc[t])

    def afrr_down_limit_rule(model: ConcreteModel, a: int, t: int) -> Constraint:
        if not afrr_enabled:
            return model.afrr_down_reserved[a, t] == 0.0
        asset = asset_lookup[a]
        if fixed_afrr_down_reserved_mw_by_asset is not None:
            return model.afrr_down_reserved[a, t] == float(fixed_afrr_down_lookup[asset.id].iloc[t])
        return model.afrr_down_reserved[a, t] <= float(power_limit_frames[asset.id].iloc[t])

    def reserve_charge_headroom_rule(model: ConcreteModel, a: int, t: int) -> Constraint:
        asset = asset_lookup[a]
        return model.charge[a, t] + model.fcr_reserved[a, t] + model.afrr_down_reserved[a, t] <= float(
            power_limit_frames[asset.id].iloc[t]
        )

    def reserve_discharge_headroom_rule(model: ConcreteModel, a: int, t: int) -> Constraint:
        asset = asset_lookup[a]
        return model.discharge[a, t] + model.fcr_reserved[a, t] + model.afrr_up_reserved[a, t] <= float(
            power_limit_frames[asset.id].iloc[t]
        )

    def reserve_soc_floor_rule(model: ConcreteModel, a: int, t: int) -> Constraint:
        if not reserve_enabled:
            return Constraint.Skip
        battery = asset_lookup[a].battery
        return (
            model.soc[a, t] - model.fcr_reserved[a, t] * reserve_duration_hours / battery.discharge_efficiency
            >= battery.effective_soc_min_mwh
        )

    def reserve_soc_ceiling_rule(model: ConcreteModel, a: int, t: int) -> Constraint:
        if not reserve_enabled:
            return Constraint.Skip
        battery = asset_lookup[a].battery
        return (
            model.soc[a, t] + model.fcr_reserved[a, t] * reserve_duration_hours * battery.charge_efficiency
            <= battery.effective_soc_max_mwh
        )

    def afrr_soc_floor_rule(model: ConcreteModel, a: int, t: int) -> Constraint:
        if not afrr_enabled:
            return Constraint.Skip
        battery = asset_lookup[a].battery
        return (
            model.soc[a, t] - model.afrr_up_reserved[a, t] * reserve_duration_hours / battery.discharge_efficiency
            >= battery.effective_soc_min_mwh
        )

    def afrr_soc_ceiling_rule(model: ConcreteModel, a: int, t: int) -> Constraint:
        if not afrr_enabled:
            return Constraint.Skip
        battery = asset_lookup[a].battery
        return (
            model.soc[a, t] + model.afrr_down_reserved[a, t] * reserve_duration_hours * battery.charge_efficiency
            <= battery.effective_soc_max_mwh
        )

    def soc_transition_rule(model: ConcreteModel, a: int, t: int) -> Constraint:
        battery = asset_lookup[a].battery
        return (
            model.soc[a, t + 1]
            == model.soc[a, t]
            + (
                model.charge[a, t] * battery.charge_efficiency
                - model.discharge[a, t] / battery.discharge_efficiency
                - model.afrr_up_reserved[a, t]
                * float(afrr_series.activation_ratio_up.iloc[t])
                / battery.discharge_efficiency
                + model.afrr_down_reserved[a, t]
                * float(afrr_series.activation_ratio_down.iloc[t])
                * battery.charge_efficiency
            )
            * dt_hours
        )

    def site_import_rule(model: ConcreteModel, t: int) -> Constraint:
        return sum(model.charge[a, t] for a in model.A) <= site.poi_import_limit_mw

    def site_export_rule(model: ConcreteModel, t: int) -> Constraint:
        return sum(model.discharge[a, t] for a in model.A) <= site.poi_export_limit_mw

    def site_import_with_reserve_rule(model: ConcreteModel, t: int) -> Constraint:
        if not reserve_enabled and not afrr_enabled:
            return Constraint.Skip
        return (
            sum(model.charge[a, t] + model.fcr_reserved[a, t] + model.afrr_down_reserved[a, t] for a in model.A)
            <= site.poi_import_limit_mw
        )

    def site_export_with_reserve_rule(model: ConcreteModel, t: int) -> Constraint:
        if not reserve_enabled and not afrr_enabled:
            return Constraint.Skip
        return (
            sum(model.discharge[a, t] + model.fcr_reserved[a, t] + model.afrr_up_reserved[a, t] for a in model.A)
            <= site.poi_export_limit_mw
        )

    model.initial_soc = Constraint(model.A, rule=initial_soc_rule)
    model.terminal_soc = Constraint(model.A, rule=terminal_soc_rule)
    model.soc_bounds = Constraint(model.A, model.S, rule=soc_bounds_rule)
    model.charge_limits = Constraint(model.A, model.T, rule=charge_limit_rule)
    model.discharge_limits = Constraint(model.A, model.T, rule=discharge_limit_rule)
    model.reserve_limits = Constraint(model.A, model.T, rule=reserve_limit_rule)
    model.afrr_up_limits = Constraint(model.A, model.T, rule=afrr_up_limit_rule)
    model.afrr_down_limits = Constraint(model.A, model.T, rule=afrr_down_limit_rule)
    model.reserve_charge_headroom = Constraint(model.A, model.T, rule=reserve_charge_headroom_rule)
    model.reserve_discharge_headroom = Constraint(model.A, model.T, rule=reserve_discharge_headroom_rule)
    model.reserve_soc_floor = Constraint(model.A, model.T, rule=reserve_soc_floor_rule)
    model.reserve_soc_ceiling = Constraint(model.A, model.T, rule=reserve_soc_ceiling_rule)
    model.afrr_soc_floor = Constraint(model.A, model.T, rule=afrr_soc_floor_rule)
    model.afrr_soc_ceiling = Constraint(model.A, model.T, rule=afrr_soc_ceiling_rule)
    model.soc_transition = Constraint(model.A, model.T, rule=soc_transition_rule)
    model.site_import = Constraint(model.T, rule=site_import_rule)
    model.site_export = Constraint(model.T, rule=site_export_rule)
    model.site_import_with_reserve = Constraint(model.T, rule=site_import_with_reserve_rule)
    model.site_export_with_reserve = Constraint(model.T, rule=site_export_with_reserve_rule)

    def objective_rule(model: ConcreteModel) -> Objective:
        terms = []
        for a in model.A:
            asset = asset_lookup[a]
            degradation = float(degradation_costs.get(asset.id, 0.0))
            for t in model.T:
                net_export = model.discharge[a, t] - model.charge[a, t]
                revenue_component = net_export * float(objective_prices.iloc[t]) * dt_hours
                reserve_revenue_component = model.fcr_reserved[a, t] * float(reserve_prices.iloc[t]) * dt_hours
                afrr_capacity_component = (
                    model.afrr_up_reserved[a, t] * float(afrr_series.capacity_up_prices.iloc[t])
                    + model.afrr_down_reserved[a, t] * float(afrr_series.capacity_down_prices.iloc[t])
                ) * dt_hours
                afrr_activation_component = (
                    model.afrr_up_reserved[a, t]
                    * float(afrr_series.activation_ratio_up.iloc[t])
                    * float(afrr_series.activation_up_prices.iloc[t])
                    + model.afrr_down_reserved[a, t]
                    * float(afrr_series.activation_ratio_down.iloc[t])
                    * float(afrr_series.activation_down_prices.iloc[t])
                ) * dt_hours
                reserve_penalty_component = (
                    reserve_penalty_eur_per_mw
                    * (model.fcr_reserved[a, t] + model.afrr_up_reserved[a, t] + model.afrr_down_reserved[a, t])
                    * dt_hours
                )
                degradation_component = degradation * (model.charge[a, t] + model.discharge[a, t]) * dt_hours
                terms.append(
                    revenue_component
                    + reserve_revenue_component
                    + afrr_capacity_component
                    + afrr_activation_component
                    - reserve_penalty_component
                    - degradation_component
                )
        return sum(terms)

    model.objective = Objective(rule=objective_rule, sense=maximize)
    solver = SolverFactory(solver_name)
    result = solver.solve(model)
    if result.solver.status != SolverStatus.ok or result.solver.termination_condition not in {
        TerminationCondition.optimal,
        TerminationCondition.feasible,
    }:
        raise RuntimeError(
            f"Optimization failed with status={result.solver.status} termination={result.solver.termination_condition}"
        )

    asset_rows: list[dict[str, object]] = []
    for a in model.A:
        asset = asset_lookup[int(a)]
        battery = asset.battery
        for t in model.T:
            idx = int(t)
            timestamp_utc = frame.loc[idx, "timestamp_utc"]
            timestamp_local = frame.loc[idx, "timestamp_local"]
            charge = float(value(model.charge[a, t]))
            discharge = float(value(model.discharge[a, t]))
            reserved = float(value(model.fcr_reserved[a, t]))
            afrr_up = float(value(model.afrr_up_reserved[a, t]))
            afrr_down = float(value(model.afrr_down_reserved[a, t]))
            soc_start = float(value(model.soc[a, t]))
            soc_end = float(value(model.soc[a, t + 1]))
            power_limit = float(power_limit_frames[asset.id].iloc[idx])
            asset_rows.append(
                {
                    "timestamp_utc": timestamp_utc,
                    "timestamp_local": timestamp_local,
                    "resolution_minutes": int(frame.loc[idx, "resolution_minutes"]),
                    "market": frame.loc[idx, "market"],
                    "zone": frame.loc[idx, "zone"],
                    "currency": frame.loc[idx, "currency"],
                    "source": frame.loc[idx, "source"],
                    "value_kind": frame.loc[idx, "value_kind"],
                    "asset_id": asset.id,
                    "asset_name": battery.name,
                    "charge_mw": charge,
                    "discharge_mw": discharge,
                    "soc_start_mwh": soc_start,
                    "soc_mwh": soc_end,
                    "availability_factor": float(availability_frames[asset.id].iloc[idx]),
                    "power_limit_mw": power_limit,
                    "fcr_reserved_mw": reserved,
                    "afrr_up_reserved_mw": afrr_up,
                    "afrr_down_reserved_mw": afrr_down,
                    "reserved_capacity_mw": reserved + afrr_up + afrr_down,
                    "net_export_mw": discharge - charge,
                    "baseline_net_export_mw": 0.0,
                    "imbalance_mw": 0.0,
                    "reserve_headroom_up_mw": power_limit - discharge - reserved - afrr_up,
                    "reserve_headroom_down_mw": power_limit - charge - reserved - afrr_down,
                    "throughput_mwh": (charge + discharge) * dt_hours,
                    "expected_afrr_activated_up_mwh": afrr_up
                    * float(afrr_series.activation_ratio_up.iloc[idx])
                    * dt_hours,
                    "expected_afrr_activated_down_mwh": afrr_down
                    * float(afrr_series.activation_ratio_down.iloc[idx])
                    * dt_hours,
                    "strategy_name": strategy_name,
                }
            )
    asset_dispatch = pd.DataFrame(asset_rows)
    site_dispatch = _build_site_dispatch(
        frame=frame, site=site, asset_dispatch=asset_dispatch, strategy_name=strategy_name
    )
    objective_value_eur = float(value(model.objective))
    return PortfolioOptimizationOutput(
        site_dispatch=site_dispatch,
        asset_dispatch=asset_dispatch,
        objective_value_eur=objective_value_eur,
        solver_name=solver_name,
    )


def _solve_single_dispatch_problem_scenario(
    *,
    market_frame: pd.DataFrame,
    battery: BatterySpec,
    objective_prices: ScenarioMarketSeries,
    strategy_name: str,
    risk: RiskPreference,
    baseline_net_export_mw: pd.Series | None = None,
    fcr_capacity_prices: ScenarioMarketSeries | None = None,
    fixed_fcr_reserved_mw: pd.Series | None = None,
    afrr_series: ScenarioAfrrSeries | None = None,
    fixed_afrr_up_reserved_mw: pd.Series | None = None,
    fixed_afrr_down_reserved_mw: pd.Series | None = None,
    reserve_sustain_duration_hours: float | None = None,
    reserve_penalty_eur_per_mw: float = 0.0,
    initial_soc_mwh: float | None = None,
    terminal_soc_mwh: float | None = None,
    degradation_cost_eur_per_mwh: float = 0.0,
) -> OptimizationOutput:
    solver_name = ensure_solver_available()
    frame = market_frame.sort_values("timestamp_utc").reset_index(drop=True).copy()
    frame["timestamp_utc"] = pd.to_datetime(frame["timestamp_utc"], utc=True)
    timestamps = pd.DatetimeIndex(frame["timestamp_utc"])
    availability = battery.availability.to_series(timestamps).astype(float)
    power_limits = battery.effective_power_limit_mw * availability
    dt_hours = float(frame["resolution_minutes"].iloc[0]) / 60.0
    horizon = len(frame)
    baseline = baseline_net_export_mw if baseline_net_export_mw is not None else pd.Series(0.0, index=frame.index)
    reserve_enabled = reserve_sustain_duration_hours is not None or fixed_fcr_reserved_mw is not None
    afrr_enabled = (
        afrr_series is not None or fixed_afrr_up_reserved_mw is not None or fixed_afrr_down_reserved_mw is not None
    )
    reserve_duration_hours = float(reserve_sustain_duration_hours or 0.0)
    fixed_reserve = (
        fixed_fcr_reserved_mw.reset_index(drop=True).astype(float)
        if fixed_fcr_reserved_mw is not None
        else pd.Series(0.0, index=frame.index, dtype=float)
    )
    fixed_afrr_up = (
        fixed_afrr_up_reserved_mw.reset_index(drop=True).astype(float)
        if fixed_afrr_up_reserved_mw is not None
        else pd.Series(0.0, index=frame.index, dtype=float)
    )
    fixed_afrr_down = (
        fixed_afrr_down_reserved_mw.reset_index(drop=True).astype(float)
        if fixed_afrr_down_reserved_mw is not None
        else pd.Series(0.0, index=frame.index, dtype=float)
    )
    reserve_prices = fcr_capacity_prices if fcr_capacity_prices is not None else _zero_scenario_series(objective_prices)
    if afrr_series is None:
        zero = _zero_scenario_series(objective_prices)
        afrr_series = ScenarioAfrrSeries(
            capacity_up_prices=zero,
            capacity_down_prices=zero,
            activation_up_prices=zero,
            activation_down_prices=zero,
            activation_ratio_up=zero,
            activation_ratio_down=zero,
        )
    _validate_scenario_series(
        objective_prices,
        reserve_prices,
        afrr_series.capacity_up_prices,
        afrr_series.capacity_down_prices,
        afrr_series.activation_up_prices,
        afrr_series.activation_down_prices,
        afrr_series.activation_ratio_up,
        afrr_series.activation_ratio_down,
    )

    scenario_ids = list(objective_prices.scenario_ids)
    scenario_weights = objective_prices.weights.reindex(scenario_ids).astype(float)
    effective_initial_soc = battery.initial_soc_mwh if initial_soc_mwh is None else initial_soc_mwh
    effective_terminal_soc = battery.terminal_soc_mwh if terminal_soc_mwh is None else terminal_soc_mwh

    model = ConcreteModel()
    model.T = RangeSet(0, horizon - 1)
    model.S = RangeSet(0, horizon)
    model.K = RangeSet(0, len(scenario_ids) - 1)
    model.charge = Var(model.T, domain=NonNegativeReals)
    model.discharge = Var(model.T, domain=NonNegativeReals)
    model.fcr_reserved = Var(model.T, domain=NonNegativeReals)
    model.afrr_up_reserved = Var(model.T, domain=NonNegativeReals)
    model.afrr_down_reserved = Var(model.T, domain=NonNegativeReals)
    model.mode = Var(model.T, domain=Binary)
    model.soc = Var(model.K, model.S, domain=Reals)

    def initial_soc_rule(model: ConcreteModel, k: int) -> Constraint:
        return model.soc[k, 0] == effective_initial_soc

    def terminal_soc_rule(model: ConcreteModel, k: int) -> Constraint:
        if effective_terminal_soc is None:
            return Constraint.Skip
        return model.soc[k, horizon] == effective_terminal_soc

    def soc_bounds_rule(model: ConcreteModel, k: int, s: int) -> Constraint:
        return (battery.effective_soc_min_mwh, model.soc[k, s], battery.effective_soc_max_mwh)

    def charge_limit_rule(model: ConcreteModel, t: int) -> Constraint:
        return model.charge[t] <= float(power_limits.iloc[t]) * model.mode[t]

    def discharge_limit_rule(model: ConcreteModel, t: int) -> Constraint:
        return model.discharge[t] <= float(power_limits.iloc[t]) * (1 - model.mode[t])

    def reserve_limit_rule(model: ConcreteModel, t: int) -> Constraint:
        if not reserve_enabled:
            return model.fcr_reserved[t] == 0.0
        if fixed_fcr_reserved_mw is not None:
            return model.fcr_reserved[t] == float(fixed_reserve.iloc[t])
        return model.fcr_reserved[t] <= float(power_limits.iloc[t])

    def afrr_up_limit_rule(model: ConcreteModel, t: int) -> Constraint:
        if not afrr_enabled:
            return model.afrr_up_reserved[t] == 0.0
        if fixed_afrr_up_reserved_mw is not None:
            return model.afrr_up_reserved[t] == float(fixed_afrr_up.iloc[t])
        return model.afrr_up_reserved[t] <= float(power_limits.iloc[t])

    def afrr_down_limit_rule(model: ConcreteModel, t: int) -> Constraint:
        if not afrr_enabled:
            return model.afrr_down_reserved[t] == 0.0
        if fixed_afrr_down_reserved_mw is not None:
            return model.afrr_down_reserved[t] == float(fixed_afrr_down.iloc[t])
        return model.afrr_down_reserved[t] <= float(power_limits.iloc[t])

    def reserve_charge_headroom_rule(model: ConcreteModel, t: int) -> Constraint:
        return model.charge[t] + model.fcr_reserved[t] + model.afrr_down_reserved[t] <= float(power_limits.iloc[t])

    def reserve_discharge_headroom_rule(model: ConcreteModel, t: int) -> Constraint:
        return model.discharge[t] + model.fcr_reserved[t] + model.afrr_up_reserved[t] <= float(power_limits.iloc[t])

    def reserve_soc_floor_rule(model: ConcreteModel, k: int, t: int) -> Constraint:
        if not reserve_enabled:
            return Constraint.Skip
        return (
            model.soc[k, t] - model.fcr_reserved[t] * reserve_duration_hours / battery.discharge_efficiency
            >= battery.effective_soc_min_mwh
        )

    def reserve_soc_ceiling_rule(model: ConcreteModel, k: int, t: int) -> Constraint:
        if not reserve_enabled:
            return Constraint.Skip
        return (
            model.soc[k, t] + model.fcr_reserved[t] * reserve_duration_hours * battery.charge_efficiency
            <= battery.effective_soc_max_mwh
        )

    def afrr_soc_floor_rule(model: ConcreteModel, k: int, t: int) -> Constraint:
        if not afrr_enabled:
            return Constraint.Skip
        return (
            model.soc[k, t] - model.afrr_up_reserved[t] * reserve_duration_hours / battery.discharge_efficiency
            >= battery.effective_soc_min_mwh
        )

    def afrr_soc_ceiling_rule(model: ConcreteModel, k: int, t: int) -> Constraint:
        if not afrr_enabled:
            return Constraint.Skip
        return (
            model.soc[k, t] + model.afrr_down_reserved[t] * reserve_duration_hours * battery.charge_efficiency
            <= battery.effective_soc_max_mwh
        )

    def soc_transition_rule(model: ConcreteModel, k: int, t: int) -> Constraint:
        scenario_id = scenario_ids[int(k)]
        return (
            model.soc[k, t + 1]
            == model.soc[k, t]
            + (
                model.charge[t] * battery.charge_efficiency
                - model.discharge[t] / battery.discharge_efficiency
                - model.afrr_up_reserved[t]
                * float(afrr_series.activation_ratio_up.values.loc[scenario_id, t])
                / battery.discharge_efficiency
                + model.afrr_down_reserved[t]
                * float(afrr_series.activation_ratio_down.values.loc[scenario_id, t])
                * battery.charge_efficiency
            )
            * dt_hours
        )

    model.initial_soc = Constraint(model.K, rule=initial_soc_rule)
    model.terminal_soc = Constraint(model.K, rule=terminal_soc_rule)
    model.soc_bounds = Constraint(model.K, model.S, rule=soc_bounds_rule)
    model.charge_limits = Constraint(model.T, rule=charge_limit_rule)
    model.discharge_limits = Constraint(model.T, rule=discharge_limit_rule)
    model.reserve_limits = Constraint(model.T, rule=reserve_limit_rule)
    model.afrr_up_limits = Constraint(model.T, rule=afrr_up_limit_rule)
    model.afrr_down_limits = Constraint(model.T, rule=afrr_down_limit_rule)
    model.reserve_charge_headroom = Constraint(model.T, rule=reserve_charge_headroom_rule)
    model.reserve_discharge_headroom = Constraint(model.T, rule=reserve_discharge_headroom_rule)
    model.reserve_soc_floor = Constraint(model.K, model.T, rule=reserve_soc_floor_rule)
    model.reserve_soc_ceiling = Constraint(model.K, model.T, rule=reserve_soc_ceiling_rule)
    model.afrr_soc_floor = Constraint(model.K, model.T, rule=afrr_soc_floor_rule)
    model.afrr_soc_ceiling = Constraint(model.K, model.T, rule=afrr_soc_ceiling_rule)
    model.soc_transition = Constraint(model.K, model.T, rule=soc_transition_rule)

    def scenario_profit_expr(model: ConcreteModel, k: int):
        scenario_id = scenario_ids[int(k)]
        terms = []
        for t in model.T:
            net_export = model.discharge[t] - model.charge[t]
            revenue_component = (
                (net_export - float(baseline.iloc[t])) * float(objective_prices.values.loc[scenario_id, t]) * dt_hours
            )
            reserve_revenue_component = (
                model.fcr_reserved[t] * float(reserve_prices.values.loc[scenario_id, t]) * dt_hours
            )
            afrr_capacity_component = (
                model.afrr_up_reserved[t] * float(afrr_series.capacity_up_prices.values.loc[scenario_id, t])
                + model.afrr_down_reserved[t] * float(afrr_series.capacity_down_prices.values.loc[scenario_id, t])
            ) * dt_hours
            afrr_activation_component = (
                model.afrr_up_reserved[t]
                * float(afrr_series.activation_ratio_up.values.loc[scenario_id, t])
                * float(afrr_series.activation_up_prices.values.loc[scenario_id, t])
                + model.afrr_down_reserved[t]
                * float(afrr_series.activation_ratio_down.values.loc[scenario_id, t])
                * float(afrr_series.activation_down_prices.values.loc[scenario_id, t])
            ) * dt_hours
            reserve_penalty_component = (
                reserve_penalty_eur_per_mw
                * (model.fcr_reserved[t] + model.afrr_up_reserved[t] + model.afrr_down_reserved[t])
                * dt_hours
            )
            degradation_component = degradation_cost_eur_per_mwh * (model.charge[t] + model.discharge[t]) * dt_hours
            terms.append(
                revenue_component
                + reserve_revenue_component
                + afrr_capacity_component
                + afrr_activation_component
                - reserve_penalty_component
                - degradation_component
            )
        return sum(terms)

    model.scenario_profit = Var(model.K, domain=Reals)

    def scenario_profit_rule(model: ConcreteModel, k: int) -> Constraint:
        return model.scenario_profit[k] == scenario_profit_expr(model, k)

    model.scenario_profit_def = Constraint(model.K, rule=scenario_profit_rule)
    model.shortfall = Var(model.K, domain=NonNegativeReals)
    model.cvar_eta = Var(domain=Reals)
    model.cvar_excess = Var(model.K, domain=NonNegativeReals)

    expected_value_expr = sum(float(scenario_weights.iloc[k]) * model.scenario_profit[k] for k in model.K)

    def downside_shortfall_rule(model: ConcreteModel, k: int) -> Constraint:
        return model.shortfall[k] >= expected_value_expr - model.scenario_profit[k]

    def cvar_excess_rule(model: ConcreteModel, k: int) -> Constraint:
        return model.cvar_excess[k] >= -model.scenario_profit[k] - model.cvar_eta

    model.downside_shortfall = Constraint(model.K, rule=downside_shortfall_rule)
    model.cvar_excess_rule = Constraint(model.K, rule=cvar_excess_rule)

    downside_penalty_expr = sum(float(scenario_weights.iloc[k]) * model.shortfall[k] for k in model.K)
    cvar_penalty_expr = model.cvar_eta + (1.0 / max(1.0 - float(risk.tail_alpha or 0.95), 1e-6)) * sum(
        float(scenario_weights.iloc[k]) * model.cvar_excess[k] for k in model.K
    )

    def objective_rule(model: ConcreteModel) -> Objective:
        if risk.mode == "downside_penalty":
            return expected_value_expr - float(risk.penalty_lambda) * downside_penalty_expr
        if risk.mode == "cvar_lite":
            return expected_value_expr - float(risk.penalty_lambda) * cvar_penalty_expr
        return expected_value_expr

    model.objective = Objective(rule=objective_rule, sense=maximize)
    solver = SolverFactory(solver_name)
    result = solver.solve(model)
    if result.solver.status != SolverStatus.ok or result.solver.termination_condition not in {
        TerminationCondition.optimal,
        TerminationCondition.feasible,
    }:
        raise RuntimeError(
            f"Optimization failed with status={result.solver.status} termination={result.solver.termination_condition}"
        )

    dispatch = frame.copy()
    dispatch["charge_mw"] = [value(model.charge[t]) for t in model.T]
    dispatch["discharge_mw"] = [value(model.discharge[t]) for t in model.T]
    dispatch["soc_start_mwh"] = [value(model.soc[0, t]) for t in model.T]
    dispatch["soc_mwh"] = [value(model.soc[0, t + 1]) for t in model.T]
    dispatch["availability_factor"] = availability.values
    dispatch["power_limit_mw"] = power_limits.values
    dispatch["fcr_reserved_mw"] = [value(model.fcr_reserved[t]) for t in model.T]
    dispatch["afrr_up_reserved_mw"] = [value(model.afrr_up_reserved[t]) for t in model.T]
    dispatch["afrr_down_reserved_mw"] = [value(model.afrr_down_reserved[t]) for t in model.T]
    dispatch["reserved_capacity_mw"] = (
        dispatch["fcr_reserved_mw"] + dispatch["afrr_up_reserved_mw"] + dispatch["afrr_down_reserved_mw"]
    )
    dispatch["net_export_mw"] = dispatch["discharge_mw"] - dispatch["charge_mw"]
    dispatch["baseline_net_export_mw"] = baseline.values
    dispatch["imbalance_mw"] = dispatch["net_export_mw"] - dispatch["baseline_net_export_mw"]
    dispatch["reserve_headroom_up_mw"] = (
        dispatch["power_limit_mw"]
        - dispatch["discharge_mw"]
        - dispatch["fcr_reserved_mw"]
        - dispatch["afrr_up_reserved_mw"]
    )
    dispatch["reserve_headroom_down_mw"] = (
        dispatch["power_limit_mw"]
        - dispatch["charge_mw"]
        - dispatch["fcr_reserved_mw"]
        - dispatch["afrr_down_reserved_mw"]
    )
    dispatch["throughput_mwh"] = (dispatch["charge_mw"] + dispatch["discharge_mw"]) * dt_hours
    expected_ratio_up = afrr_series.activation_ratio_up.expected_series().reset_index(drop=True)
    expected_ratio_down = afrr_series.activation_ratio_down.expected_series().reset_index(drop=True)
    dispatch["expected_afrr_activated_up_mwh"] = dispatch["afrr_up_reserved_mw"] * expected_ratio_up * dt_hours
    dispatch["expected_afrr_activated_down_mwh"] = dispatch["afrr_down_reserved_mw"] * expected_ratio_down * dt_hours
    dispatch["strategy_name"] = strategy_name
    objective_value_eur = float(value(model.objective))
    scenario_profit = {scenario_ids[int(k)]: float(value(model.scenario_profit[k])) for k in model.K}
    return OptimizationOutput(
        dispatch=dispatch,
        objective_value_eur=objective_value_eur,
        solver_name=solver_name,
        metadata={
            "risk_mode": risk.mode,
            "penalty_lambda": risk.penalty_lambda,
            "tail_alpha": risk.tail_alpha,
            "scenario_profit_eur": scenario_profit,
            "expected_value_eur": float(sum(scenario_weights.loc[sid] * scenario_profit[sid] for sid in scenario_ids)),
        },
    )


def _solve_portfolio_dispatch_problem_scenario(
    *,
    market_frame: pd.DataFrame,
    site: SiteSpec,
    assets: list[AssetSpec],
    objective_prices: ScenarioMarketSeries,
    strategy_name: str,
    risk: RiskPreference,
    fcr_capacity_prices: ScenarioMarketSeries | None = None,
    fixed_fcr_reserved_mw_by_asset: dict[str, pd.Series] | None = None,
    afrr_series: ScenarioAfrrSeries | None = None,
    fixed_afrr_up_reserved_mw_by_asset: dict[str, pd.Series] | None = None,
    fixed_afrr_down_reserved_mw_by_asset: dict[str, pd.Series] | None = None,
    reserve_sustain_duration_hours: float | None = None,
    reserve_penalty_eur_per_mw: float = 0.0,
    degradation_costs_eur_per_mwh: dict[str, float] | None = None,
    initial_soc_mwh_by_asset: dict[str, float] | None = None,
    terminal_soc_mwh_by_asset: dict[str, float | None] | None = None,
) -> PortfolioOptimizationOutput:
    solver_name = ensure_solver_available()
    frame = market_frame.sort_values("timestamp_utc").reset_index(drop=True).copy()
    frame["timestamp_utc"] = pd.to_datetime(frame["timestamp_utc"], utc=True)
    timestamps = pd.DatetimeIndex(frame["timestamp_utc"])
    dt_hours = float(frame["resolution_minutes"].iloc[0]) / 60.0
    horizon = len(frame)
    reserve_enabled = reserve_sustain_duration_hours is not None or fixed_fcr_reserved_mw_by_asset is not None
    afrr_enabled = (
        afrr_series is not None
        or fixed_afrr_up_reserved_mw_by_asset is not None
        or fixed_afrr_down_reserved_mw_by_asset is not None
    )
    reserve_duration_hours = float(reserve_sustain_duration_hours or 0.0)
    reserve_prices = fcr_capacity_prices if fcr_capacity_prices is not None else _zero_scenario_series(objective_prices)
    if afrr_series is None:
        zero = _zero_scenario_series(objective_prices)
        afrr_series = ScenarioAfrrSeries(
            capacity_up_prices=zero,
            capacity_down_prices=zero,
            activation_up_prices=zero,
            activation_down_prices=zero,
            activation_ratio_up=zero,
            activation_ratio_down=zero,
        )
    _validate_scenario_series(
        objective_prices,
        reserve_prices,
        afrr_series.capacity_up_prices,
        afrr_series.capacity_down_prices,
        afrr_series.activation_up_prices,
        afrr_series.activation_down_prices,
        afrr_series.activation_ratio_up,
        afrr_series.activation_ratio_down,
    )
    scenario_ids = list(objective_prices.scenario_ids)
    scenario_weights = objective_prices.weights.reindex(scenario_ids).astype(float)
    degradation_costs = degradation_costs_eur_per_mwh or {asset.id: 0.0 for asset in assets}
    asset_count = len(assets)

    availability_frames: dict[str, pd.Series] = {}
    power_limit_frames: dict[str, pd.Series] = {}
    for asset in assets:
        availability = asset.battery.availability.to_series(timestamps).astype(float)
        availability_frames[asset.id] = availability
        power_limit_frames[asset.id] = asset.battery.effective_power_limit_mw * availability

    asset_lookup = {idx: asset for idx, asset in enumerate(assets)}
    initial_soc_lookup = initial_soc_mwh_by_asset or {asset.id: asset.battery.initial_soc_mwh for asset in assets}
    terminal_soc_lookup = terminal_soc_mwh_by_asset or {asset.id: asset.battery.terminal_soc_mwh for asset in assets}
    fixed_reserve_lookup = {
        asset.id: (
            fixed_fcr_reserved_mw_by_asset[asset.id].reset_index(drop=True).astype(float)
            if fixed_fcr_reserved_mw_by_asset is not None and asset.id in fixed_fcr_reserved_mw_by_asset
            else pd.Series(0.0, index=frame.index, dtype=float)
        )
        for asset in assets
    }
    fixed_afrr_up_lookup = {
        asset.id: (
            fixed_afrr_up_reserved_mw_by_asset[asset.id].reset_index(drop=True).astype(float)
            if fixed_afrr_up_reserved_mw_by_asset is not None and asset.id in fixed_afrr_up_reserved_mw_by_asset
            else pd.Series(0.0, index=frame.index, dtype=float)
        )
        for asset in assets
    }
    fixed_afrr_down_lookup = {
        asset.id: (
            fixed_afrr_down_reserved_mw_by_asset[asset.id].reset_index(drop=True).astype(float)
            if fixed_afrr_down_reserved_mw_by_asset is not None and asset.id in fixed_afrr_down_reserved_mw_by_asset
            else pd.Series(0.0, index=frame.index, dtype=float)
        )
        for asset in assets
    }

    model = ConcreteModel()
    model.A = RangeSet(0, asset_count - 1)
    model.T = RangeSet(0, horizon - 1)
    model.S = RangeSet(0, horizon)
    model.K = RangeSet(0, len(scenario_ids) - 1)
    model.charge = Var(model.A, model.T, domain=NonNegativeReals)
    model.discharge = Var(model.A, model.T, domain=NonNegativeReals)
    model.fcr_reserved = Var(model.A, model.T, domain=NonNegativeReals)
    model.afrr_up_reserved = Var(model.A, model.T, domain=NonNegativeReals)
    model.afrr_down_reserved = Var(model.A, model.T, domain=NonNegativeReals)
    model.mode = Var(model.A, model.T, domain=Binary)
    model.soc = Var(model.A, model.K, model.S, domain=Reals)

    def initial_soc_rule(model: ConcreteModel, a: int, k: int) -> Constraint:
        return model.soc[a, k, 0] == float(initial_soc_lookup[asset_lookup[a].id])

    def terminal_soc_rule(model: ConcreteModel, a: int, k: int) -> Constraint:
        battery = asset_lookup[a].battery
        terminal_soc = terminal_soc_lookup.get(asset_lookup[a].id, battery.terminal_soc_mwh)
        if terminal_soc is None:
            return Constraint.Skip
        return model.soc[a, k, horizon] == float(terminal_soc)

    def soc_bounds_rule(model: ConcreteModel, a: int, k: int, s: int) -> Constraint:
        battery = asset_lookup[a].battery
        return (battery.effective_soc_min_mwh, model.soc[a, k, s], battery.effective_soc_max_mwh)

    def charge_limit_rule(model: ConcreteModel, a: int, t: int) -> Constraint:
        asset = asset_lookup[a]
        return model.charge[a, t] <= float(power_limit_frames[asset.id].iloc[t]) * model.mode[a, t]

    def discharge_limit_rule(model: ConcreteModel, a: int, t: int) -> Constraint:
        asset = asset_lookup[a]
        return model.discharge[a, t] <= float(power_limit_frames[asset.id].iloc[t]) * (1 - model.mode[a, t])

    def reserve_limit_rule(model: ConcreteModel, a: int, t: int) -> Constraint:
        if not reserve_enabled:
            return model.fcr_reserved[a, t] == 0.0
        asset = asset_lookup[a]
        if fixed_fcr_reserved_mw_by_asset is not None:
            return model.fcr_reserved[a, t] == float(fixed_reserve_lookup[asset.id].iloc[t])
        return model.fcr_reserved[a, t] <= float(power_limit_frames[asset.id].iloc[t])

    def afrr_up_limit_rule(model: ConcreteModel, a: int, t: int) -> Constraint:
        if not afrr_enabled:
            return model.afrr_up_reserved[a, t] == 0.0
        asset = asset_lookup[a]
        if fixed_afrr_up_reserved_mw_by_asset is not None:
            return model.afrr_up_reserved[a, t] == float(fixed_afrr_up_lookup[asset.id].iloc[t])
        return model.afrr_up_reserved[a, t] <= float(power_limit_frames[asset.id].iloc[t])

    def afrr_down_limit_rule(model: ConcreteModel, a: int, t: int) -> Constraint:
        if not afrr_enabled:
            return model.afrr_down_reserved[a, t] == 0.0
        asset = asset_lookup[a]
        if fixed_afrr_down_reserved_mw_by_asset is not None:
            return model.afrr_down_reserved[a, t] == float(fixed_afrr_down_lookup[asset.id].iloc[t])
        return model.afrr_down_reserved[a, t] <= float(power_limit_frames[asset.id].iloc[t])

    def reserve_charge_headroom_rule(model: ConcreteModel, a: int, t: int) -> Constraint:
        asset = asset_lookup[a]
        return model.charge[a, t] + model.fcr_reserved[a, t] + model.afrr_down_reserved[a, t] <= float(
            power_limit_frames[asset.id].iloc[t]
        )

    def reserve_discharge_headroom_rule(model: ConcreteModel, a: int, t: int) -> Constraint:
        asset = asset_lookup[a]
        return model.discharge[a, t] + model.fcr_reserved[a, t] + model.afrr_up_reserved[a, t] <= float(
            power_limit_frames[asset.id].iloc[t]
        )

    def reserve_soc_floor_rule(model: ConcreteModel, a: int, k: int, t: int) -> Constraint:
        if not reserve_enabled:
            return Constraint.Skip
        battery = asset_lookup[a].battery
        return (
            model.soc[a, k, t] - model.fcr_reserved[a, t] * reserve_duration_hours / battery.discharge_efficiency
            >= battery.effective_soc_min_mwh
        )

    def reserve_soc_ceiling_rule(model: ConcreteModel, a: int, k: int, t: int) -> Constraint:
        if not reserve_enabled:
            return Constraint.Skip
        battery = asset_lookup[a].battery
        return (
            model.soc[a, k, t] + model.fcr_reserved[a, t] * reserve_duration_hours * battery.charge_efficiency
            <= battery.effective_soc_max_mwh
        )

    def afrr_soc_floor_rule(model: ConcreteModel, a: int, k: int, t: int) -> Constraint:
        if not afrr_enabled:
            return Constraint.Skip
        battery = asset_lookup[a].battery
        return (
            model.soc[a, k, t] - model.afrr_up_reserved[a, t] * reserve_duration_hours / battery.discharge_efficiency
            >= battery.effective_soc_min_mwh
        )

    def afrr_soc_ceiling_rule(model: ConcreteModel, a: int, k: int, t: int) -> Constraint:
        if not afrr_enabled:
            return Constraint.Skip
        battery = asset_lookup[a].battery
        return (
            model.soc[a, k, t] + model.afrr_down_reserved[a, t] * reserve_duration_hours * battery.charge_efficiency
            <= battery.effective_soc_max_mwh
        )

    def soc_transition_rule(model: ConcreteModel, a: int, k: int, t: int) -> Constraint:
        scenario_id = scenario_ids[int(k)]
        battery = asset_lookup[a].battery
        return (
            model.soc[a, k, t + 1]
            == model.soc[a, k, t]
            + (
                model.charge[a, t] * battery.charge_efficiency
                - model.discharge[a, t] / battery.discharge_efficiency
                - model.afrr_up_reserved[a, t]
                * float(afrr_series.activation_ratio_up.values.loc[scenario_id, t])
                / battery.discharge_efficiency
                + model.afrr_down_reserved[a, t]
                * float(afrr_series.activation_ratio_down.values.loc[scenario_id, t])
                * battery.charge_efficiency
            )
            * dt_hours
        )

    def site_import_rule(model: ConcreteModel, t: int) -> Constraint:
        return sum(model.charge[a, t] for a in model.A) <= site.poi_import_limit_mw

    def site_export_rule(model: ConcreteModel, t: int) -> Constraint:
        return sum(model.discharge[a, t] for a in model.A) <= site.poi_export_limit_mw

    def site_import_with_reserve_rule(model: ConcreteModel, t: int) -> Constraint:
        if not reserve_enabled and not afrr_enabled:
            return Constraint.Skip
        return (
            sum(model.charge[a, t] + model.fcr_reserved[a, t] + model.afrr_down_reserved[a, t] for a in model.A)
            <= site.poi_import_limit_mw
        )

    def site_export_with_reserve_rule(model: ConcreteModel, t: int) -> Constraint:
        if not reserve_enabled and not afrr_enabled:
            return Constraint.Skip
        return (
            sum(model.discharge[a, t] + model.fcr_reserved[a, t] + model.afrr_up_reserved[a, t] for a in model.A)
            <= site.poi_export_limit_mw
        )

    model.initial_soc = Constraint(model.A, model.K, rule=initial_soc_rule)
    model.terminal_soc = Constraint(model.A, model.K, rule=terminal_soc_rule)
    model.soc_bounds = Constraint(model.A, model.K, model.S, rule=soc_bounds_rule)
    model.charge_limits = Constraint(model.A, model.T, rule=charge_limit_rule)
    model.discharge_limits = Constraint(model.A, model.T, rule=discharge_limit_rule)
    model.reserve_limits = Constraint(model.A, model.T, rule=reserve_limit_rule)
    model.afrr_up_limits = Constraint(model.A, model.T, rule=afrr_up_limit_rule)
    model.afrr_down_limits = Constraint(model.A, model.T, rule=afrr_down_limit_rule)
    model.reserve_charge_headroom = Constraint(model.A, model.T, rule=reserve_charge_headroom_rule)
    model.reserve_discharge_headroom = Constraint(model.A, model.T, rule=reserve_discharge_headroom_rule)
    model.reserve_soc_floor = Constraint(model.A, model.K, model.T, rule=reserve_soc_floor_rule)
    model.reserve_soc_ceiling = Constraint(model.A, model.K, model.T, rule=reserve_soc_ceiling_rule)
    model.afrr_soc_floor = Constraint(model.A, model.K, model.T, rule=afrr_soc_floor_rule)
    model.afrr_soc_ceiling = Constraint(model.A, model.K, model.T, rule=afrr_soc_ceiling_rule)
    model.soc_transition = Constraint(model.A, model.K, model.T, rule=soc_transition_rule)
    model.site_import = Constraint(model.T, rule=site_import_rule)
    model.site_export = Constraint(model.T, rule=site_export_rule)
    model.site_import_with_reserve = Constraint(model.T, rule=site_import_with_reserve_rule)
    model.site_export_with_reserve = Constraint(model.T, rule=site_export_with_reserve_rule)

    model.scenario_profit = Var(model.K, domain=Reals)

    def scenario_profit_rule(model: ConcreteModel, k: int) -> Constraint:
        scenario_id = scenario_ids[int(k)]
        terms = []
        for a in model.A:
            asset = asset_lookup[a]
            degradation = float(degradation_costs.get(asset.id, 0.0))
            for t in model.T:
                net_export = model.discharge[a, t] - model.charge[a, t]
                revenue_component = net_export * float(objective_prices.values.loc[scenario_id, t]) * dt_hours
                reserve_revenue_component = (
                    model.fcr_reserved[a, t] * float(reserve_prices.values.loc[scenario_id, t]) * dt_hours
                )
                afrr_capacity_component = (
                    model.afrr_up_reserved[a, t] * float(afrr_series.capacity_up_prices.values.loc[scenario_id, t])
                    + model.afrr_down_reserved[a, t]
                    * float(afrr_series.capacity_down_prices.values.loc[scenario_id, t])
                ) * dt_hours
                afrr_activation_component = (
                    model.afrr_up_reserved[a, t]
                    * float(afrr_series.activation_ratio_up.values.loc[scenario_id, t])
                    * float(afrr_series.activation_up_prices.values.loc[scenario_id, t])
                    + model.afrr_down_reserved[a, t]
                    * float(afrr_series.activation_ratio_down.values.loc[scenario_id, t])
                    * float(afrr_series.activation_down_prices.values.loc[scenario_id, t])
                ) * dt_hours
                reserve_penalty_component = (
                    reserve_penalty_eur_per_mw
                    * (model.fcr_reserved[a, t] + model.afrr_up_reserved[a, t] + model.afrr_down_reserved[a, t])
                    * dt_hours
                )
                degradation_component = degradation * (model.charge[a, t] + model.discharge[a, t]) * dt_hours
                terms.append(
                    revenue_component
                    + reserve_revenue_component
                    + afrr_capacity_component
                    + afrr_activation_component
                    - reserve_penalty_component
                    - degradation_component
                )
        return model.scenario_profit[k] == sum(terms)

    model.scenario_profit_def = Constraint(model.K, rule=scenario_profit_rule)
    model.shortfall = Var(model.K, domain=NonNegativeReals)
    model.cvar_eta = Var(domain=Reals)
    model.cvar_excess = Var(model.K, domain=NonNegativeReals)
    expected_value_expr = sum(float(scenario_weights.iloc[k]) * model.scenario_profit[k] for k in model.K)

    def downside_shortfall_rule(model: ConcreteModel, k: int) -> Constraint:
        return model.shortfall[k] >= expected_value_expr - model.scenario_profit[k]

    def cvar_excess_rule(model: ConcreteModel, k: int) -> Constraint:
        return model.cvar_excess[k] >= -model.scenario_profit[k] - model.cvar_eta

    model.downside_shortfall = Constraint(model.K, rule=downside_shortfall_rule)
    model.cvar_excess_rule = Constraint(model.K, rule=cvar_excess_rule)
    downside_penalty_expr = sum(float(scenario_weights.iloc[k]) * model.shortfall[k] for k in model.K)
    cvar_penalty_expr = model.cvar_eta + (1.0 / max(1.0 - float(risk.tail_alpha or 0.95), 1e-6)) * sum(
        float(scenario_weights.iloc[k]) * model.cvar_excess[k] for k in model.K
    )

    def objective_rule(model: ConcreteModel) -> Objective:
        if risk.mode == "downside_penalty":
            return expected_value_expr - float(risk.penalty_lambda) * downside_penalty_expr
        if risk.mode == "cvar_lite":
            return expected_value_expr - float(risk.penalty_lambda) * cvar_penalty_expr
        return expected_value_expr

    model.objective = Objective(rule=objective_rule, sense=maximize)
    solver = SolverFactory(solver_name)
    result = solver.solve(model)
    if result.solver.status != SolverStatus.ok or result.solver.termination_condition not in {
        TerminationCondition.optimal,
        TerminationCondition.feasible,
    }:
        raise RuntimeError(
            f"Optimization failed with status={result.solver.status} termination={result.solver.termination_condition}"
        )

    asset_rows: list[dict[str, object]] = []
    expected_ratio_up = afrr_series.activation_ratio_up.expected_series().reset_index(drop=True)
    expected_ratio_down = afrr_series.activation_ratio_down.expected_series().reset_index(drop=True)
    for a in model.A:
        asset = asset_lookup[int(a)]
        battery = asset.battery
        for t in model.T:
            idx = int(t)
            timestamp_utc = frame.loc[idx, "timestamp_utc"]
            timestamp_local = frame.loc[idx, "timestamp_local"]
            charge = float(value(model.charge[a, t]))
            discharge = float(value(model.discharge[a, t]))
            reserved = float(value(model.fcr_reserved[a, t]))
            afrr_up = float(value(model.afrr_up_reserved[a, t]))
            afrr_down = float(value(model.afrr_down_reserved[a, t]))
            soc_start = float(value(model.soc[a, 0, t]))
            soc_end = float(value(model.soc[a, 0, t + 1]))
            power_limit = float(power_limit_frames[asset.id].iloc[idx])
            asset_rows.append(
                {
                    "timestamp_utc": timestamp_utc,
                    "timestamp_local": timestamp_local,
                    "resolution_minutes": int(frame.loc[idx, "resolution_minutes"]),
                    "market": frame.loc[idx, "market"],
                    "zone": frame.loc[idx, "zone"],
                    "currency": frame.loc[idx, "currency"],
                    "source": frame.loc[idx, "source"],
                    "value_kind": frame.loc[idx, "value_kind"],
                    "asset_id": asset.id,
                    "asset_name": battery.name,
                    "charge_mw": charge,
                    "discharge_mw": discharge,
                    "soc_start_mwh": soc_start,
                    "soc_mwh": soc_end,
                    "availability_factor": float(availability_frames[asset.id].iloc[idx]),
                    "power_limit_mw": power_limit,
                    "fcr_reserved_mw": reserved,
                    "afrr_up_reserved_mw": afrr_up,
                    "afrr_down_reserved_mw": afrr_down,
                    "reserved_capacity_mw": reserved + afrr_up + afrr_down,
                    "net_export_mw": discharge - charge,
                    "baseline_net_export_mw": 0.0,
                    "imbalance_mw": 0.0,
                    "reserve_headroom_up_mw": power_limit - discharge - reserved - afrr_up,
                    "reserve_headroom_down_mw": power_limit - charge - reserved - afrr_down,
                    "throughput_mwh": (charge + discharge) * dt_hours,
                    "expected_afrr_activated_up_mwh": afrr_up * float(expected_ratio_up.iloc[idx]) * dt_hours,
                    "expected_afrr_activated_down_mwh": afrr_down * float(expected_ratio_down.iloc[idx]) * dt_hours,
                    "strategy_name": strategy_name,
                }
            )
    asset_dispatch = pd.DataFrame(asset_rows)
    site_dispatch = _build_site_dispatch(
        frame=frame, site=site, asset_dispatch=asset_dispatch, strategy_name=strategy_name
    )
    objective_value_eur = float(value(model.objective))
    scenario_profit = {scenario_ids[int(k)]: float(value(model.scenario_profit[k])) for k in model.K}
    return PortfolioOptimizationOutput(
        site_dispatch=site_dispatch,
        asset_dispatch=asset_dispatch,
        objective_value_eur=objective_value_eur,
        solver_name=solver_name,
        metadata={
            "risk_mode": risk.mode,
            "penalty_lambda": risk.penalty_lambda,
            "tail_alpha": risk.tail_alpha,
            "scenario_profit_eur": scenario_profit,
            "expected_value_eur": float(sum(scenario_weights.loc[sid] * scenario_profit[sid] for sid in scenario_ids)),
        },
    )


def solve_day_ahead_dispatch(
    price_frame: pd.DataFrame,
    battery: BatterySpec,
    *,
    degradation_cost_eur_per_mwh: float = 0.0,
    initial_soc_mwh: float | None = None,
    terminal_soc_mwh: float | None = None,
    fixed_fcr_reserved_mw: pd.Series | None = None,
    fixed_afrr_up_reserved_mw: pd.Series | None = None,
    fixed_afrr_down_reserved_mw: pd.Series | None = None,
    reserve_sustain_duration_minutes: int = 15,
    strategy_name: str = "day_ahead_dispatch",
) -> OptimizationOutput:
    return _solve_single_dispatch_problem(
        market_frame=price_frame,
        battery=battery,
        objective_prices=price_frame["price_eur_per_mwh"],
        strategy_name=strategy_name,
        degradation_cost_eur_per_mwh=degradation_cost_eur_per_mwh,
        initial_soc_mwh=initial_soc_mwh,
        terminal_soc_mwh=terminal_soc_mwh,
        fixed_fcr_reserved_mw=fixed_fcr_reserved_mw,
        fixed_afrr_up_reserved_mw=fixed_afrr_up_reserved_mw,
        fixed_afrr_down_reserved_mw=fixed_afrr_down_reserved_mw,
        reserve_sustain_duration_hours=reserve_sustain_duration_minutes / 60.0
        if fixed_afrr_up_reserved_mw is not None or fixed_afrr_down_reserved_mw is not None
        else None,
    )


def scenario_market_series_from_snapshot(
    snapshot: pd.DataFrame,
    *,
    expected_timestamps: pd.Series,
) -> ScenarioMarketSeries:
    if "scenario_id" not in snapshot.columns or "scenario_weight" not in snapshot.columns:
        raise ValueError("Scenario snapshots must include scenario_id and scenario_weight")
    ordered_timestamps = pd.Index(pd.to_datetime(expected_timestamps, utc=True))
    weights = (
        snapshot[["scenario_id", "scenario_weight"]]
        .drop_duplicates()
        .set_index("scenario_id")["scenario_weight"]
        .sort_index()
        .astype(float)
    )
    matrix = (
        snapshot.pivot(index="scenario_id", columns="delivery_start_utc", values="forecast_price_eur_per_mwh")
        .reindex(index=weights.index, columns=ordered_timestamps)
        .astype(float)
    )
    if matrix.isna().any().any():
        raise ValueError("Scenario snapshot matrix does not cover every requested timestamp")
    matrix.columns = range(len(matrix.columns))
    return ScenarioMarketSeries(
        scenario_ids=tuple(str(scenario_id) for scenario_id in weights.index.tolist()),
        weights=weights,
        values=matrix,
    )


def solve_portfolio_day_ahead_dispatch(
    price_frame: pd.DataFrame,
    site: SiteSpec,
    assets: list[AssetSpec],
    *,
    degradation_costs_eur_per_mwh: dict[str, float] | None = None,
    initial_soc_mwh_by_asset: dict[str, float] | None = None,
    terminal_soc_mwh_by_asset: dict[str, float | None] | None = None,
    fixed_fcr_reserved_mw_by_asset: dict[str, pd.Series] | None = None,
    fixed_afrr_up_reserved_mw_by_asset: dict[str, pd.Series] | None = None,
    fixed_afrr_down_reserved_mw_by_asset: dict[str, pd.Series] | None = None,
    reserve_sustain_duration_minutes: int = 15,
    strategy_name: str = "portfolio_day_ahead_dispatch",
) -> PortfolioOptimizationOutput:
    return _solve_portfolio_dispatch_problem(
        market_frame=price_frame,
        site=site,
        assets=assets,
        objective_prices=price_frame["price_eur_per_mwh"],
        strategy_name=strategy_name,
        degradation_costs_eur_per_mwh=degradation_costs_eur_per_mwh,
        initial_soc_mwh_by_asset=initial_soc_mwh_by_asset,
        terminal_soc_mwh_by_asset=terminal_soc_mwh_by_asset,
        fixed_fcr_reserved_mw_by_asset=fixed_fcr_reserved_mw_by_asset,
        fixed_afrr_up_reserved_mw_by_asset=fixed_afrr_up_reserved_mw_by_asset,
        fixed_afrr_down_reserved_mw_by_asset=fixed_afrr_down_reserved_mw_by_asset,
        reserve_sustain_duration_hours=reserve_sustain_duration_minutes / 60.0
        if fixed_afrr_up_reserved_mw_by_asset is not None or fixed_afrr_down_reserved_mw_by_asset is not None
        else None,
    )


def solve_day_ahead_dispatch_scenario(
    *,
    price_frame: pd.DataFrame,
    price_snapshot: pd.DataFrame,
    battery: BatterySpec,
    risk: RiskPreference,
    degradation_cost_eur_per_mwh: float = 0.0,
    initial_soc_mwh: float | None = None,
    terminal_soc_mwh: float | None = None,
    fixed_fcr_reserved_mw: pd.Series | None = None,
    fixed_afrr_up_reserved_mw: pd.Series | None = None,
    fixed_afrr_down_reserved_mw: pd.Series | None = None,
    reserve_sustain_duration_minutes: int = 15,
    strategy_name: str = "day_ahead_dispatch",
) -> OptimizationOutput:
    scenario_prices = scenario_market_series_from_snapshot(
        price_snapshot, expected_timestamps=price_frame["timestamp_utc"]
    )
    return _solve_single_dispatch_problem_scenario(
        market_frame=price_frame,
        battery=battery,
        objective_prices=scenario_prices,
        strategy_name=strategy_name,
        risk=risk,
        degradation_cost_eur_per_mwh=degradation_cost_eur_per_mwh,
        initial_soc_mwh=initial_soc_mwh,
        terminal_soc_mwh=terminal_soc_mwh,
        fixed_fcr_reserved_mw=fixed_fcr_reserved_mw,
        fixed_afrr_up_reserved_mw=fixed_afrr_up_reserved_mw,
        fixed_afrr_down_reserved_mw=fixed_afrr_down_reserved_mw,
        reserve_sustain_duration_hours=reserve_sustain_duration_minutes / 60.0
        if fixed_afrr_up_reserved_mw is not None or fixed_afrr_down_reserved_mw is not None
        else None,
    )


def solve_portfolio_day_ahead_dispatch_scenario(
    *,
    price_frame: pd.DataFrame,
    price_snapshot: pd.DataFrame,
    site: SiteSpec,
    assets: list[AssetSpec],
    risk: RiskPreference,
    degradation_costs_eur_per_mwh: dict[str, float] | None = None,
    initial_soc_mwh_by_asset: dict[str, float] | None = None,
    terminal_soc_mwh_by_asset: dict[str, float | None] | None = None,
    fixed_fcr_reserved_mw_by_asset: dict[str, pd.Series] | None = None,
    fixed_afrr_up_reserved_mw_by_asset: dict[str, pd.Series] | None = None,
    fixed_afrr_down_reserved_mw_by_asset: dict[str, pd.Series] | None = None,
    reserve_sustain_duration_minutes: int = 15,
    strategy_name: str = "portfolio_day_ahead_dispatch",
) -> PortfolioOptimizationOutput:
    scenario_prices = scenario_market_series_from_snapshot(
        price_snapshot, expected_timestamps=price_frame["timestamp_utc"]
    )
    return _solve_portfolio_dispatch_problem_scenario(
        market_frame=price_frame,
        site=site,
        assets=assets,
        objective_prices=scenario_prices,
        strategy_name=strategy_name,
        risk=risk,
        degradation_costs_eur_per_mwh=degradation_costs_eur_per_mwh,
        initial_soc_mwh_by_asset=initial_soc_mwh_by_asset,
        terminal_soc_mwh_by_asset=terminal_soc_mwh_by_asset,
        fixed_fcr_reserved_mw_by_asset=fixed_fcr_reserved_mw_by_asset,
        fixed_afrr_up_reserved_mw_by_asset=fixed_afrr_up_reserved_mw_by_asset,
        fixed_afrr_down_reserved_mw_by_asset=fixed_afrr_down_reserved_mw_by_asset,
        reserve_sustain_duration_hours=reserve_sustain_duration_minutes / 60.0
        if fixed_afrr_up_reserved_mw_by_asset is not None or fixed_afrr_down_reserved_mw_by_asset is not None
        else None,
    )


def solve_imbalance_overlay_dispatch(
    *,
    day_ahead_frame: pd.DataFrame,
    imbalance_frame: pd.DataFrame,
    battery: BatterySpec,
    baseline_dispatch: pd.DataFrame,
    degradation_cost_eur_per_mwh: float = 0.0,
    initial_soc_mwh: float | None = None,
    terminal_soc_mwh: float | None = None,
    strategy_name: str = "imbalance_overlay_dispatch",
) -> OptimizationOutput:
    aligned = day_ahead_frame[["timestamp_utc"]].merge(
        imbalance_frame[["timestamp_utc", "price_eur_per_mwh"]],
        on="timestamp_utc",
        how="left",
    )
    if aligned["price_eur_per_mwh"].isna().any():
        raise ValueError("Imbalance series must align to every day-ahead interval in the overlay strategy")
    baseline = baseline_dispatch["net_export_mw"].reset_index(drop=True)
    frame = day_ahead_frame.copy()
    frame["price_eur_per_mwh"] = aligned["price_eur_per_mwh"].values
    return _solve_single_dispatch_problem(
        market_frame=frame,
        battery=battery,
        objective_prices=frame["price_eur_per_mwh"],
        strategy_name=strategy_name,
        baseline_net_export_mw=baseline,
        degradation_cost_eur_per_mwh=degradation_cost_eur_per_mwh,
        initial_soc_mwh=initial_soc_mwh,
        terminal_soc_mwh=terminal_soc_mwh,
    )


def solve_day_ahead_fcr_dispatch(
    *,
    day_ahead_frame: pd.DataFrame,
    fcr_capacity_frame: pd.DataFrame,
    battery: BatterySpec,
    degradation_cost_eur_per_mwh: float = 0.0,
    reserve_sustain_duration_minutes: int = 15,
    reserve_penalty_eur_per_mw: float = 0.0,
    initial_soc_mwh: float | None = None,
    terminal_soc_mwh: float | None = None,
    fixed_fcr_reserved_mw: pd.Series | None = None,
    strategy_name: str = "day_ahead_fcr_cooptimization",
) -> OptimizationOutput:
    aligned = day_ahead_frame[["timestamp_utc"]].merge(
        fcr_capacity_frame[["timestamp_utc", "price_eur_per_mwh"]],
        on="timestamp_utc",
        how="left",
    )
    if aligned["price_eur_per_mwh"].isna().any():
        raise ValueError("FCR capacity series must align to every day-ahead interval in the reserve benchmark")
    return _solve_single_dispatch_problem(
        market_frame=day_ahead_frame,
        battery=battery,
        objective_prices=day_ahead_frame["price_eur_per_mwh"],
        strategy_name=strategy_name,
        fcr_capacity_prices=aligned["price_eur_per_mwh"].reset_index(drop=True),
        reserve_sustain_duration_hours=reserve_sustain_duration_minutes / 60.0,
        reserve_penalty_eur_per_mw=reserve_penalty_eur_per_mw,
        degradation_cost_eur_per_mwh=degradation_cost_eur_per_mwh,
        initial_soc_mwh=initial_soc_mwh,
        terminal_soc_mwh=terminal_soc_mwh,
        fixed_fcr_reserved_mw=fixed_fcr_reserved_mw,
    )


def solve_day_ahead_fcr_dispatch_scenario(
    *,
    day_ahead_frame: pd.DataFrame,
    day_ahead_snapshot: pd.DataFrame,
    fcr_capacity_snapshot: pd.DataFrame,
    battery: BatterySpec,
    risk: RiskPreference,
    degradation_cost_eur_per_mwh: float = 0.0,
    reserve_sustain_duration_minutes: int = 15,
    reserve_penalty_eur_per_mw: float = 0.0,
    initial_soc_mwh: float | None = None,
    terminal_soc_mwh: float | None = None,
    fixed_fcr_reserved_mw: pd.Series | None = None,
    strategy_name: str = "day_ahead_fcr_cooptimization",
) -> OptimizationOutput:
    scenario_prices = scenario_market_series_from_snapshot(
        day_ahead_snapshot, expected_timestamps=day_ahead_frame["timestamp_utc"]
    )
    scenario_fcr = scenario_market_series_from_snapshot(
        fcr_capacity_snapshot, expected_timestamps=day_ahead_frame["timestamp_utc"]
    )
    return _solve_single_dispatch_problem_scenario(
        market_frame=day_ahead_frame,
        battery=battery,
        objective_prices=scenario_prices,
        strategy_name=strategy_name,
        risk=risk,
        fcr_capacity_prices=scenario_fcr,
        reserve_sustain_duration_hours=reserve_sustain_duration_minutes / 60.0,
        reserve_penalty_eur_per_mw=reserve_penalty_eur_per_mw,
        degradation_cost_eur_per_mwh=degradation_cost_eur_per_mwh,
        initial_soc_mwh=initial_soc_mwh,
        terminal_soc_mwh=terminal_soc_mwh,
        fixed_fcr_reserved_mw=fixed_fcr_reserved_mw,
    )


def solve_portfolio_day_ahead_fcr_dispatch(
    *,
    day_ahead_frame: pd.DataFrame,
    fcr_capacity_frame: pd.DataFrame,
    site: SiteSpec,
    assets: list[AssetSpec],
    degradation_costs_eur_per_mwh: dict[str, float] | None = None,
    reserve_sustain_duration_minutes: int = 15,
    reserve_penalty_eur_per_mw: float = 0.0,
    initial_soc_mwh_by_asset: dict[str, float] | None = None,
    terminal_soc_mwh_by_asset: dict[str, float | None] | None = None,
    fixed_fcr_reserved_mw_by_asset: dict[str, pd.Series] | None = None,
    strategy_name: str = "portfolio_day_ahead_fcr_cooptimization",
) -> PortfolioOptimizationOutput:
    aligned = day_ahead_frame[["timestamp_utc"]].merge(
        fcr_capacity_frame[["timestamp_utc", "price_eur_per_mwh"]],
        on="timestamp_utc",
        how="left",
    )
    if aligned["price_eur_per_mwh"].isna().any():
        raise ValueError("FCR capacity series must align to every day-ahead interval in the reserve benchmark")
    return _solve_portfolio_dispatch_problem(
        market_frame=day_ahead_frame,
        site=site,
        assets=assets,
        objective_prices=day_ahead_frame["price_eur_per_mwh"],
        strategy_name=strategy_name,
        fcr_capacity_prices=aligned["price_eur_per_mwh"].reset_index(drop=True),
        reserve_sustain_duration_hours=reserve_sustain_duration_minutes / 60.0,
        reserve_penalty_eur_per_mw=reserve_penalty_eur_per_mw,
        degradation_costs_eur_per_mwh=degradation_costs_eur_per_mwh,
        initial_soc_mwh_by_asset=initial_soc_mwh_by_asset,
        terminal_soc_mwh_by_asset=terminal_soc_mwh_by_asset,
        fixed_fcr_reserved_mw_by_asset=fixed_fcr_reserved_mw_by_asset,
    )


def solve_portfolio_day_ahead_fcr_dispatch_scenario(
    *,
    day_ahead_frame: pd.DataFrame,
    day_ahead_snapshot: pd.DataFrame,
    fcr_capacity_snapshot: pd.DataFrame,
    site: SiteSpec,
    assets: list[AssetSpec],
    risk: RiskPreference,
    degradation_costs_eur_per_mwh: dict[str, float] | None = None,
    reserve_sustain_duration_minutes: int = 15,
    reserve_penalty_eur_per_mw: float = 0.0,
    initial_soc_mwh_by_asset: dict[str, float] | None = None,
    terminal_soc_mwh_by_asset: dict[str, float | None] | None = None,
    fixed_fcr_reserved_mw_by_asset: dict[str, pd.Series] | None = None,
    strategy_name: str = "portfolio_day_ahead_fcr_cooptimization",
) -> PortfolioOptimizationOutput:
    scenario_prices = scenario_market_series_from_snapshot(
        day_ahead_snapshot, expected_timestamps=day_ahead_frame["timestamp_utc"]
    )
    scenario_fcr = scenario_market_series_from_snapshot(
        fcr_capacity_snapshot, expected_timestamps=day_ahead_frame["timestamp_utc"]
    )
    return _solve_portfolio_dispatch_problem_scenario(
        market_frame=day_ahead_frame,
        site=site,
        assets=assets,
        objective_prices=scenario_prices,
        strategy_name=strategy_name,
        risk=risk,
        fcr_capacity_prices=scenario_fcr,
        reserve_sustain_duration_hours=reserve_sustain_duration_minutes / 60.0,
        reserve_penalty_eur_per_mw=reserve_penalty_eur_per_mw,
        degradation_costs_eur_per_mwh=degradation_costs_eur_per_mwh,
        initial_soc_mwh_by_asset=initial_soc_mwh_by_asset,
        terminal_soc_mwh_by_asset=terminal_soc_mwh_by_asset,
        fixed_fcr_reserved_mw_by_asset=fixed_fcr_reserved_mw_by_asset,
    )


def solve_day_ahead_afrr_dispatch(
    *,
    day_ahead_frame: pd.DataFrame,
    afrr_capacity_up_frame: pd.DataFrame,
    afrr_capacity_down_frame: pd.DataFrame,
    afrr_activation_price_up_frame: pd.DataFrame,
    afrr_activation_price_down_frame: pd.DataFrame,
    afrr_activation_ratio_up_frame: pd.DataFrame,
    afrr_activation_ratio_down_frame: pd.DataFrame,
    battery: BatterySpec,
    degradation_cost_eur_per_mwh: float = 0.0,
    reserve_sustain_duration_minutes: int = 15,
    reserve_penalty_eur_per_mw: float = 0.0,
    initial_soc_mwh: float | None = None,
    terminal_soc_mwh: float | None = None,
    fixed_afrr_up_reserved_mw: pd.Series | None = None,
    fixed_afrr_down_reserved_mw: pd.Series | None = None,
    strategy_name: str = "day_ahead_afrr_cooptimization",
) -> OptimizationOutput:
    aligned = day_ahead_frame[["timestamp_utc"]].copy()
    for label, frame in {
        "capacity_up": afrr_capacity_up_frame,
        "capacity_down": afrr_capacity_down_frame,
        "activation_up": afrr_activation_price_up_frame,
        "activation_down": afrr_activation_price_down_frame,
        "ratio_up": afrr_activation_ratio_up_frame,
        "ratio_down": afrr_activation_ratio_down_frame,
    }.items():
        aligned = aligned.merge(
            frame[["timestamp_utc", "price_eur_per_mwh"]].rename(columns={"price_eur_per_mwh": label}),
            on="timestamp_utc",
            how="left",
        )
    if aligned.isna().any().any():
        raise ValueError("aFRR series must align to every day-ahead interval in the reserve benchmark")
    afrr_series = AfrrSeries(
        capacity_up_prices=aligned["capacity_up"].reset_index(drop=True),
        capacity_down_prices=aligned["capacity_down"].reset_index(drop=True),
        activation_up_prices=aligned["activation_up"].reset_index(drop=True),
        activation_down_prices=aligned["activation_down"].reset_index(drop=True),
        activation_ratio_up=aligned["ratio_up"].reset_index(drop=True),
        activation_ratio_down=aligned["ratio_down"].reset_index(drop=True),
    )
    return _solve_single_dispatch_problem(
        market_frame=day_ahead_frame,
        battery=battery,
        objective_prices=day_ahead_frame["price_eur_per_mwh"],
        strategy_name=strategy_name,
        afrr_series=afrr_series,
        reserve_sustain_duration_hours=reserve_sustain_duration_minutes / 60.0,
        reserve_penalty_eur_per_mw=reserve_penalty_eur_per_mw,
        degradation_cost_eur_per_mwh=degradation_cost_eur_per_mwh,
        initial_soc_mwh=initial_soc_mwh,
        terminal_soc_mwh=terminal_soc_mwh,
        fixed_afrr_up_reserved_mw=fixed_afrr_up_reserved_mw,
        fixed_afrr_down_reserved_mw=fixed_afrr_down_reserved_mw,
    )


def solve_day_ahead_afrr_dispatch_scenario(
    *,
    day_ahead_frame: pd.DataFrame,
    day_ahead_snapshot: pd.DataFrame,
    afrr_capacity_up_snapshot: pd.DataFrame,
    afrr_capacity_down_snapshot: pd.DataFrame,
    afrr_activation_price_up_snapshot: pd.DataFrame,
    afrr_activation_price_down_snapshot: pd.DataFrame,
    afrr_activation_ratio_up_snapshot: pd.DataFrame,
    afrr_activation_ratio_down_snapshot: pd.DataFrame,
    battery: BatterySpec,
    risk: RiskPreference,
    degradation_cost_eur_per_mwh: float = 0.0,
    reserve_sustain_duration_minutes: int = 15,
    reserve_penalty_eur_per_mw: float = 0.0,
    initial_soc_mwh: float | None = None,
    terminal_soc_mwh: float | None = None,
    fixed_afrr_up_reserved_mw: pd.Series | None = None,
    fixed_afrr_down_reserved_mw: pd.Series | None = None,
    strategy_name: str = "day_ahead_afrr_cooptimization",
) -> OptimizationOutput:
    scenario_prices = scenario_market_series_from_snapshot(
        day_ahead_snapshot, expected_timestamps=day_ahead_frame["timestamp_utc"]
    )
    scenario_afrr = ScenarioAfrrSeries(
        capacity_up_prices=scenario_market_series_from_snapshot(
            afrr_capacity_up_snapshot, expected_timestamps=day_ahead_frame["timestamp_utc"]
        ),
        capacity_down_prices=scenario_market_series_from_snapshot(
            afrr_capacity_down_snapshot, expected_timestamps=day_ahead_frame["timestamp_utc"]
        ),
        activation_up_prices=scenario_market_series_from_snapshot(
            afrr_activation_price_up_snapshot, expected_timestamps=day_ahead_frame["timestamp_utc"]
        ),
        activation_down_prices=scenario_market_series_from_snapshot(
            afrr_activation_price_down_snapshot, expected_timestamps=day_ahead_frame["timestamp_utc"]
        ),
        activation_ratio_up=scenario_market_series_from_snapshot(
            afrr_activation_ratio_up_snapshot, expected_timestamps=day_ahead_frame["timestamp_utc"]
        ),
        activation_ratio_down=scenario_market_series_from_snapshot(
            afrr_activation_ratio_down_snapshot, expected_timestamps=day_ahead_frame["timestamp_utc"]
        ),
    )
    return _solve_single_dispatch_problem_scenario(
        market_frame=day_ahead_frame,
        battery=battery,
        objective_prices=scenario_prices,
        strategy_name=strategy_name,
        risk=risk,
        afrr_series=scenario_afrr,
        reserve_sustain_duration_hours=reserve_sustain_duration_minutes / 60.0,
        reserve_penalty_eur_per_mw=reserve_penalty_eur_per_mw,
        degradation_cost_eur_per_mwh=degradation_cost_eur_per_mwh,
        initial_soc_mwh=initial_soc_mwh,
        terminal_soc_mwh=terminal_soc_mwh,
        fixed_afrr_up_reserved_mw=fixed_afrr_up_reserved_mw,
        fixed_afrr_down_reserved_mw=fixed_afrr_down_reserved_mw,
    )


def solve_portfolio_day_ahead_afrr_dispatch(
    *,
    day_ahead_frame: pd.DataFrame,
    afrr_capacity_up_frame: pd.DataFrame,
    afrr_capacity_down_frame: pd.DataFrame,
    afrr_activation_price_up_frame: pd.DataFrame,
    afrr_activation_price_down_frame: pd.DataFrame,
    afrr_activation_ratio_up_frame: pd.DataFrame,
    afrr_activation_ratio_down_frame: pd.DataFrame,
    site: SiteSpec,
    assets: list[AssetSpec],
    degradation_costs_eur_per_mwh: dict[str, float] | None = None,
    reserve_sustain_duration_minutes: int = 15,
    reserve_penalty_eur_per_mw: float = 0.0,
    initial_soc_mwh_by_asset: dict[str, float] | None = None,
    terminal_soc_mwh_by_asset: dict[str, float | None] | None = None,
    fixed_afrr_up_reserved_mw_by_asset: dict[str, pd.Series] | None = None,
    fixed_afrr_down_reserved_mw_by_asset: dict[str, pd.Series] | None = None,
    strategy_name: str = "portfolio_day_ahead_afrr_cooptimization",
) -> PortfolioOptimizationOutput:
    aligned = day_ahead_frame[["timestamp_utc"]].copy()
    for label, frame in {
        "capacity_up": afrr_capacity_up_frame,
        "capacity_down": afrr_capacity_down_frame,
        "activation_up": afrr_activation_price_up_frame,
        "activation_down": afrr_activation_price_down_frame,
        "ratio_up": afrr_activation_ratio_up_frame,
        "ratio_down": afrr_activation_ratio_down_frame,
    }.items():
        aligned = aligned.merge(
            frame[["timestamp_utc", "price_eur_per_mwh"]].rename(columns={"price_eur_per_mwh": label}),
            on="timestamp_utc",
            how="left",
        )
    if aligned.isna().any().any():
        raise ValueError("aFRR series must align to every day-ahead interval in the reserve benchmark")
    afrr_series = AfrrSeries(
        capacity_up_prices=aligned["capacity_up"].reset_index(drop=True),
        capacity_down_prices=aligned["capacity_down"].reset_index(drop=True),
        activation_up_prices=aligned["activation_up"].reset_index(drop=True),
        activation_down_prices=aligned["activation_down"].reset_index(drop=True),
        activation_ratio_up=aligned["ratio_up"].reset_index(drop=True),
        activation_ratio_down=aligned["ratio_down"].reset_index(drop=True),
    )
    return _solve_portfolio_dispatch_problem(
        market_frame=day_ahead_frame,
        site=site,
        assets=assets,
        objective_prices=day_ahead_frame["price_eur_per_mwh"],
        strategy_name=strategy_name,
        afrr_series=afrr_series,
        reserve_sustain_duration_hours=reserve_sustain_duration_minutes / 60.0,
        reserve_penalty_eur_per_mw=reserve_penalty_eur_per_mw,
        degradation_costs_eur_per_mwh=degradation_costs_eur_per_mwh,
        initial_soc_mwh_by_asset=initial_soc_mwh_by_asset,
        terminal_soc_mwh_by_asset=terminal_soc_mwh_by_asset,
        fixed_afrr_up_reserved_mw_by_asset=fixed_afrr_up_reserved_mw_by_asset,
        fixed_afrr_down_reserved_mw_by_asset=fixed_afrr_down_reserved_mw_by_asset,
    )


def solve_portfolio_day_ahead_afrr_dispatch_scenario(
    *,
    day_ahead_frame: pd.DataFrame,
    day_ahead_snapshot: pd.DataFrame,
    afrr_capacity_up_snapshot: pd.DataFrame,
    afrr_capacity_down_snapshot: pd.DataFrame,
    afrr_activation_price_up_snapshot: pd.DataFrame,
    afrr_activation_price_down_snapshot: pd.DataFrame,
    afrr_activation_ratio_up_snapshot: pd.DataFrame,
    afrr_activation_ratio_down_snapshot: pd.DataFrame,
    site: SiteSpec,
    assets: list[AssetSpec],
    risk: RiskPreference,
    degradation_costs_eur_per_mwh: dict[str, float] | None = None,
    reserve_sustain_duration_minutes: int = 15,
    reserve_penalty_eur_per_mw: float = 0.0,
    initial_soc_mwh_by_asset: dict[str, float] | None = None,
    terminal_soc_mwh_by_asset: dict[str, float | None] | None = None,
    fixed_afrr_up_reserved_mw_by_asset: dict[str, pd.Series] | None = None,
    fixed_afrr_down_reserved_mw_by_asset: dict[str, pd.Series] | None = None,
    strategy_name: str = "portfolio_day_ahead_afrr_cooptimization",
) -> PortfolioOptimizationOutput:
    scenario_prices = scenario_market_series_from_snapshot(
        day_ahead_snapshot, expected_timestamps=day_ahead_frame["timestamp_utc"]
    )
    scenario_afrr = ScenarioAfrrSeries(
        capacity_up_prices=scenario_market_series_from_snapshot(
            afrr_capacity_up_snapshot, expected_timestamps=day_ahead_frame["timestamp_utc"]
        ),
        capacity_down_prices=scenario_market_series_from_snapshot(
            afrr_capacity_down_snapshot, expected_timestamps=day_ahead_frame["timestamp_utc"]
        ),
        activation_up_prices=scenario_market_series_from_snapshot(
            afrr_activation_price_up_snapshot, expected_timestamps=day_ahead_frame["timestamp_utc"]
        ),
        activation_down_prices=scenario_market_series_from_snapshot(
            afrr_activation_price_down_snapshot, expected_timestamps=day_ahead_frame["timestamp_utc"]
        ),
        activation_ratio_up=scenario_market_series_from_snapshot(
            afrr_activation_ratio_up_snapshot, expected_timestamps=day_ahead_frame["timestamp_utc"]
        ),
        activation_ratio_down=scenario_market_series_from_snapshot(
            afrr_activation_ratio_down_snapshot, expected_timestamps=day_ahead_frame["timestamp_utc"]
        ),
    )
    return _solve_portfolio_dispatch_problem_scenario(
        market_frame=day_ahead_frame,
        site=site,
        assets=assets,
        objective_prices=scenario_prices,
        strategy_name=strategy_name,
        risk=risk,
        afrr_series=scenario_afrr,
        reserve_sustain_duration_hours=reserve_sustain_duration_minutes / 60.0,
        reserve_penalty_eur_per_mw=reserve_penalty_eur_per_mw,
        degradation_costs_eur_per_mwh=degradation_costs_eur_per_mwh,
        initial_soc_mwh_by_asset=initial_soc_mwh_by_asset,
        terminal_soc_mwh_by_asset=terminal_soc_mwh_by_asset,
        fixed_afrr_up_reserved_mw_by_asset=fixed_afrr_up_reserved_mw_by_asset,
        fixed_afrr_down_reserved_mw_by_asset=fixed_afrr_down_reserved_mw_by_asset,
    )
