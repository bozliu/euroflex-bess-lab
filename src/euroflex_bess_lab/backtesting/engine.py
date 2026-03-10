from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from ..benchmarks import BenchmarkDefinition, BenchmarkRegistry
from ..config import BacktestConfig, load_config
from ..markets import MarketRegistry
from ..optimization.solver import (
    OptimizationOutput,
    PortfolioOptimizationOutput,
    RiskPreference,
    solve_day_ahead_afrr_dispatch,
    solve_day_ahead_afrr_dispatch_scenario,
    solve_day_ahead_dispatch,
    solve_day_ahead_dispatch_scenario,
    solve_day_ahead_fcr_dispatch,
    solve_imbalance_overlay_dispatch,
    solve_portfolio_day_ahead_afrr_dispatch,
    solve_portfolio_day_ahead_afrr_dispatch_scenario,
    solve_portfolio_day_ahead_dispatch,
    solve_portfolio_day_ahead_dispatch_scenario,
    solve_portfolio_day_ahead_fcr_dispatch,
    solve_portfolio_day_ahead_fcr_dispatch_scenario,
)
from ..types import AssetSpec, OracleComparison, PnLAttribution, RunResult
from .artifacts import make_run_id, write_run_artifacts
from .reasons import assign_reason_codes, assign_site_reason_codes


@dataclass
class DailyArtifacts:
    site_dispatch: pd.DataFrame
    asset_dispatch: pd.DataFrame
    decisions: list[dict[str, object]]
    snapshots: list[pd.DataFrame]
    baseline_schedule: pd.DataFrame | None = None
    revision_schedule: pd.DataFrame | None = None
    schedule_lineage: pd.DataFrame | None = None
    reconciliation_breakdown: pd.DataFrame | None = None


def _validate_market_frame(frame: pd.DataFrame, *, market_name: str, timezone: str) -> pd.DataFrame:
    if frame.empty:
        raise ValueError(f"{market_name} frame cannot be empty")
    if int(frame["resolution_minutes"].iloc[0]) != 15:
        raise ValueError(f"{market_name} frame must use 15-minute resolution")
    if frame["timestamp_utc"].duplicated().any():
        raise ValueError(f"{market_name} frame contains duplicate timestamps")
    normalized = frame.sort_values("timestamp_utc").reset_index(drop=True).copy()
    normalized["timestamp_utc"] = pd.to_datetime(normalized["timestamp_utc"], utc=True)
    normalized["timestamp_local"] = pd.to_datetime(normalized["timestamp_local"], utc=True).dt.tz_convert(timezone)
    return normalized


def _degradation_cost_per_mwh(config: BacktestConfig, asset: AssetSpec) -> float:
    mode = config.degradation.mode
    if mode == "throughput_linear":
        return float(config.degradation.throughput_cost_eur_per_mwh or 0.0)
    if mode == "equivalent_cycle_linear":
        eur_per_cycle = float(config.degradation.eur_per_equivalent_cycle or 0.0)
        usable_energy = asset.battery.usable_energy_mwh
        if usable_energy <= 0.0:
            raise ValueError("equivalent_cycle_linear degradation requires positive usable battery energy")
        return eur_per_cycle / (2.0 * usable_energy)
    if mode == "rainflow_offline":
        return 0.0
    raise ValueError(f"Unsupported degradation mode: {mode}")


def _asset_degradation_costs(config: BacktestConfig) -> dict[str, float]:
    return {asset.id: _degradation_cost_per_mwh(config, asset) for asset in config.assets}


def _execution_config(config: BacktestConfig) -> BacktestConfig:
    if not config.is_revision_workflow:
        return config
    payload = config.model_dump(mode="json")
    payload["workflow"] = config.execution_workflow
    payload["revision"] = None
    return BacktestConfig.model_validate(payload)


def _delivery_dates(config: BacktestConfig) -> list[pd.Timestamp]:
    start = pd.Timestamp(config.timing.delivery_start_date)
    end = pd.Timestamp(config.timing.delivery_end_date)
    return list(pd.date_range(start, end, freq="D"))


def _filter_evaluation_window(frame: pd.DataFrame, config: BacktestConfig) -> pd.DataFrame:
    local_dates = frame["timestamp_local"].dt.date
    mask = (local_dates >= config.timing.delivery_start_date) & (local_dates <= config.timing.delivery_end_date)
    return frame.loc[mask].copy().reset_index(drop=True)


def _validate_evaluation_coverage(frame: pd.DataFrame, config: BacktestConfig, *, market_name: str) -> None:
    start_local = pd.Timestamp(f"{config.timing.delivery_start_date} 00:00:00", tz=config.timing.timezone)
    next_local_date = pd.Timestamp(config.timing.delivery_end_date) + pd.Timedelta(days=1)
    end_local = pd.Timestamp(f"{next_local_date.date()} 00:00:00", tz=config.timing.timezone)
    expected_local = pd.date_range(start_local, end_local, freq="15min", inclusive="left")
    expected_utc = pd.DatetimeIndex(expected_local.tz_convert("UTC"))
    actual_utc = pd.DatetimeIndex(pd.to_datetime(frame["timestamp_utc"], utc=True))
    missing = expected_utc.difference(actual_utc)
    extra = actual_utc.difference(expected_utc)
    if not missing.empty or not extra.empty:
        raise ValueError(
            f"{market_name} evaluation window does not match expected 15-minute delivery coverage; "
            f"missing={len(missing)} extra={len(extra)}"
        )


def _day_frame(frame: pd.DataFrame, delivery_date: pd.Timestamp) -> pd.DataFrame:
    target = delivery_date.date()
    mask = frame["timestamp_local"].dt.date == target
    return frame.loc[mask].copy().reset_index(drop=True)


def _visible_inputs_at(
    decision_time_utc: pd.Timestamp,
    **frames: pd.DataFrame | None,
) -> dict[str, pd.DataFrame]:
    visible: dict[str, pd.DataFrame] = {}
    for name, frame in frames.items():
        if frame is None:
            continue
        timestamps = pd.to_datetime(frame["timestamp_utc"], utc=True)
        clipped = frame.loc[timestamps < decision_time_utc].copy().reset_index(drop=True)
        visible[name] = clipped
    return visible


def _provider_forecast(
    provider,
    *,
    market: str,
    decision_time_utc: pd.Timestamp,
    delivery_frame: pd.DataFrame,
    actual_frame: pd.DataFrame,
    visible_frames: dict[str, pd.DataFrame | None],
) -> pd.DataFrame:
    return provider.get_forecast(
        market=market,
        decision_time_utc=decision_time_utc,
        delivery_frame=delivery_frame,
        actual_frame=actual_frame,
        visible_inputs=_visible_inputs_at(decision_time_utc, **visible_frames),
    )


def _risk_preference(config: BacktestConfig) -> RiskPreference:
    return RiskPreference(
        mode=config.risk.mode,
        penalty_lambda=float(config.risk.penalty_lambda),
        tail_alpha=config.risk.tail_alpha,
    )


def _snapshot_is_scenario(snapshot: pd.DataFrame | None) -> bool:
    return snapshot is not None and "scenario_id" in snapshot.columns and snapshot["scenario_id"].notna().any()


def _expected_snapshot(snapshot: pd.DataFrame) -> pd.DataFrame:
    if not _snapshot_is_scenario(snapshot):
        return snapshot.copy()
    frame = snapshot.copy()
    weighted = (
        frame.assign(weighted_forecast=frame["forecast_price_eur_per_mwh"] * frame["scenario_weight"].astype(float))
        .groupby("delivery_start_utc", as_index=False)["weighted_forecast"]
        .sum()
        .rename(columns={"weighted_forecast": "forecast_price_eur_per_mwh"})
    )
    first_rows = (
        frame.sort_values(["delivery_start_utc", "available_from_utc", "issue_time_utc"])
        .groupby("delivery_start_utc", as_index=False)
        .tail(1)[["delivery_start_utc", "delivery_end_utc", "issue_time_utc", "available_from_utc", "provider_name"]]
    )
    merged = weighted.merge(first_rows, on="delivery_start_utc", how="left")
    merged["market"] = frame["market"].iloc[0]
    merged["scenario_id"] = None
    merged["scenario_weight"] = 1.0
    if "actual_price_eur_per_mwh" in frame.columns:
        actuals = (
            frame[["delivery_start_utc", "actual_price_eur_per_mwh"]]
            .drop_duplicates(subset=["delivery_start_utc"])
            .reset_index(drop=True)
        )
        merged = merged.merge(actuals, on="delivery_start_utc", how="left")
    return merged[
        [
            "market",
            "delivery_start_utc",
            "delivery_end_utc",
            "forecast_price_eur_per_mwh",
            "issue_time_utc",
            "available_from_utc",
            "provider_name",
            "scenario_id",
            "scenario_weight",
            *(["actual_price_eur_per_mwh"] if "actual_price_eur_per_mwh" in merged.columns else []),
        ]
    ].copy()


def _record_snapshot(
    snapshot: pd.DataFrame,
    *,
    decision_id: str,
    decision_time_utc: pd.Timestamp,
    decision_type: str,
    schedule_version: str,
    benchmark_name: str,
    market_id: str,
    workflow_family: str,
    run_scope: str,
    site_id: str,
) -> pd.DataFrame:
    frame = snapshot.copy()
    frame["decision_id"] = decision_id
    frame["decision_time_utc"] = decision_time_utc
    frame["decision_type"] = decision_type
    frame["schedule_version"] = schedule_version
    frame["benchmark_name"] = benchmark_name
    frame["market_id"] = market_id
    frame["workflow_family"] = workflow_family
    frame["run_scope"] = run_scope
    frame["site_id"] = site_id
    return frame


def _merge_market_price(
    dispatch: pd.DataFrame,
    source: pd.DataFrame | None,
    *,
    source_column: str = "price_eur_per_mwh",
    target_column: str,
    default: float = 0.0,
) -> pd.DataFrame:
    frame = dispatch.copy()
    if source is None:
        frame[target_column] = default
        return frame
    merged = frame.merge(
        source[["timestamp_utc", source_column]].rename(columns={source_column: target_column}),
        on="timestamp_utc",
        how="left",
    )
    merged[target_column] = merged[target_column].fillna(default)
    return merged


def _merge_forecast_snapshot(
    dispatch: pd.DataFrame,
    snapshot: pd.DataFrame | None,
    *,
    target_column: str,
    default: float = 0.0,
) -> pd.DataFrame:
    frame = dispatch.copy()
    if snapshot is None:
        frame[target_column] = default
        return frame
    resolved_snapshot = _expected_snapshot(snapshot)
    merged = frame.merge(
        resolved_snapshot[["delivery_start_utc", "forecast_price_eur_per_mwh"]].rename(
            columns={"delivery_start_utc": "timestamp_utc", "forecast_price_eur_per_mwh": target_column}
        ),
        on="timestamp_utc",
        how="left",
    )
    merged[target_column] = merged[target_column].fillna(default)
    return merged


def _attach_optional_imbalance_columns(dispatch: pd.DataFrame, imbalance_frame: pd.DataFrame | None) -> pd.DataFrame:
    frame = dispatch.copy()
    if imbalance_frame is None:
        frame["imbalance_actual_price_eur_per_mwh"] = 0.0
        for optional in (
            "imbalance_shortage_price_eur_per_mwh",
            "imbalance_surplus_price_eur_per_mwh",
            "dispatch_up_price_eur_per_mwh",
            "dispatch_down_price_eur_per_mwh",
            "regulation_state",
            "regulating_condition",
        ):
            frame[optional] = 0.0 if "price" in optional else ""
        return frame

    columns = ["timestamp_utc", "price_eur_per_mwh"]
    rename_map = {"price_eur_per_mwh": "imbalance_actual_price_eur_per_mwh"}
    for optional in (
        "imbalance_shortage_price_eur_per_mwh",
        "imbalance_surplus_price_eur_per_mwh",
        "dispatch_up_price_eur_per_mwh",
        "dispatch_down_price_eur_per_mwh",
        "regulation_state",
        "regulating_condition",
    ):
        if optional in imbalance_frame.columns:
            columns.append(optional)
    return frame.merge(imbalance_frame[columns].rename(columns=rename_map), on="timestamp_utc", how="left")


def _attach_zero_afrr_columns(dispatch: pd.DataFrame) -> pd.DataFrame:
    frame = dispatch.copy()
    for column in (
        "afrr_capacity_up_price_forecast_eur_per_mw_per_h",
        "afrr_capacity_up_price_actual_eur_per_mw_per_h",
        "afrr_capacity_down_price_forecast_eur_per_mw_per_h",
        "afrr_capacity_down_price_actual_eur_per_mw_per_h",
        "afrr_activation_price_up_forecast_eur_per_mwh",
        "afrr_activation_price_up_actual_eur_per_mwh",
        "afrr_activation_price_down_forecast_eur_per_mwh",
        "afrr_activation_price_down_actual_eur_per_mwh",
        "afrr_activation_ratio_up_forecast",
        "afrr_activation_ratio_up_actual",
        "afrr_activation_ratio_down_forecast",
        "afrr_activation_ratio_down_actual",
    ):
        frame[column] = 0.0
    return frame


def _attach_zero_fcr_columns(dispatch: pd.DataFrame) -> pd.DataFrame:
    frame = dispatch.copy()
    frame["fcr_capacity_price_forecast_eur_per_mw_per_h"] = 0.0
    frame["fcr_capacity_price_actual_eur_per_mw_per_h"] = 0.0
    return frame


def _merge_afrr_snapshot_columns(
    dispatch: pd.DataFrame,
    *,
    capacity_up: pd.DataFrame,
    capacity_down: pd.DataFrame,
    activation_price_up: pd.DataFrame,
    activation_price_down: pd.DataFrame,
    activation_ratio_up: pd.DataFrame,
    activation_ratio_down: pd.DataFrame,
) -> pd.DataFrame:
    frame = dispatch.copy()
    frame = _merge_forecast_snapshot(
        frame, capacity_up, target_column="afrr_capacity_up_price_forecast_eur_per_mw_per_h"
    )
    frame = _merge_forecast_snapshot(
        frame, capacity_down, target_column="afrr_capacity_down_price_forecast_eur_per_mw_per_h"
    )
    frame = _merge_forecast_snapshot(
        frame, activation_price_up, target_column="afrr_activation_price_up_forecast_eur_per_mwh"
    )
    frame = _merge_forecast_snapshot(
        frame, activation_price_down, target_column="afrr_activation_price_down_forecast_eur_per_mwh"
    )
    frame = _merge_forecast_snapshot(frame, activation_ratio_up, target_column="afrr_activation_ratio_up_forecast")
    frame = _merge_forecast_snapshot(frame, activation_ratio_down, target_column="afrr_activation_ratio_down_forecast")
    return frame


def _merge_afrr_actual_columns(
    dispatch: pd.DataFrame,
    *,
    capacity_up: pd.DataFrame,
    capacity_down: pd.DataFrame,
    activation_price_up: pd.DataFrame,
    activation_price_down: pd.DataFrame,
    activation_ratio_up: pd.DataFrame,
    activation_ratio_down: pd.DataFrame,
) -> pd.DataFrame:
    frame = dispatch.copy()
    frame = _merge_market_price(
        frame,
        capacity_up,
        target_column="afrr_capacity_up_price_actual_eur_per_mw_per_h",
    )
    frame = _merge_market_price(
        frame,
        capacity_down,
        target_column="afrr_capacity_down_price_actual_eur_per_mw_per_h",
    )
    frame = _merge_market_price(
        frame,
        activation_price_up,
        target_column="afrr_activation_price_up_actual_eur_per_mwh",
    )
    frame = _merge_market_price(
        frame,
        activation_price_down,
        target_column="afrr_activation_price_down_actual_eur_per_mwh",
    )
    frame = _merge_market_price(
        frame,
        activation_ratio_up,
        target_column="afrr_activation_ratio_up_actual",
    )
    frame = _merge_market_price(
        frame,
        activation_ratio_down,
        target_column="afrr_activation_ratio_down_actual",
    )
    return frame


def _ensure_dispatch_columns(dispatch: pd.DataFrame, *, site_id: str, run_scope: str) -> pd.DataFrame:
    frame = dispatch.copy()
    defaults = {
        "site_id": site_id,
        "run_scope": run_scope,
        "baseline_net_export_mw": 0.0,
        "imbalance_mw": 0.0,
        "imbalance_actual_price_eur_per_mwh": 0.0,
        "imbalance_forecast_price_eur_per_mwh": 0.0,
        "fcr_reserved_mw": 0.0,
        "afrr_up_reserved_mw": 0.0,
        "afrr_down_reserved_mw": 0.0,
        "reserved_capacity_mw": frame.get("fcr_reserved_mw", 0.0)
        + frame.get("afrr_up_reserved_mw", 0.0)
        + frame.get("afrr_down_reserved_mw", 0.0),
        "reserve_headroom_up_mw": frame.get("power_limit_mw", pd.Series(0.0, index=frame.index)),
        "reserve_headroom_down_mw": frame.get("power_limit_mw", pd.Series(0.0, index=frame.index)),
        "fcr_capacity_price_actual_eur_per_mw_per_h": 0.0,
        "fcr_capacity_price_forecast_eur_per_mw_per_h": 0.0,
        "afrr_capacity_up_price_actual_eur_per_mw_per_h": 0.0,
        "afrr_capacity_up_price_forecast_eur_per_mw_per_h": 0.0,
        "afrr_capacity_down_price_actual_eur_per_mw_per_h": 0.0,
        "afrr_capacity_down_price_forecast_eur_per_mw_per_h": 0.0,
        "afrr_activation_price_up_actual_eur_per_mwh": 0.0,
        "afrr_activation_price_up_forecast_eur_per_mwh": 0.0,
        "afrr_activation_price_down_actual_eur_per_mwh": 0.0,
        "afrr_activation_price_down_forecast_eur_per_mwh": 0.0,
        "afrr_activation_ratio_up_actual": 0.0,
        "afrr_activation_ratio_up_forecast": 0.0,
        "afrr_activation_ratio_down_actual": 0.0,
        "afrr_activation_ratio_down_forecast": 0.0,
        "expected_afrr_activated_up_mwh": 0.0,
        "expected_afrr_activated_down_mwh": 0.0,
        "schedule_version": "baseline",
        "schedule_state": "locked_realized",
        "lock_state": "locked_realized",
        "throughput_mwh": (frame.get("charge_mw", 0.0) + frame.get("discharge_mw", 0.0))
        * (frame.get("resolution_minutes", pd.Series(15.0, index=frame.index)) / 60.0),
    }
    for column, value in defaults.items():
        if column not in frame.columns:
            frame[column] = value
    return frame


def _site_interval_settlement(
    dispatch: pd.DataFrame,
    *,
    workflow: str,
    degradation_cost_eur_per_mwh: float,
    settlement_engine,
    reserve_penalty_eur_per_mw: float = 0.0,
) -> tuple[pd.DataFrame, PnLAttribution]:
    frame = dispatch.copy()
    dt_hours = frame["resolution_minutes"] / 60.0
    frame["throughput_mwh"] = (frame["charge_mw"] + frame["discharge_mw"]) * dt_hours
    frame["reserve_capacity_revenue_eur"] = 0.0
    frame["reserve_activation_revenue_eur"] = 0.0
    frame["expected_reserve_capacity_revenue_eur"] = 0.0
    frame["expected_reserve_activation_revenue_eur"] = 0.0
    frame["reserve_penalty_eur"] = (
        (frame["fcr_reserved_mw"] + frame["afrr_up_reserved_mw"] + frame["afrr_down_reserved_mw"])
        * reserve_penalty_eur_per_mw
        * dt_hours
    )

    if workflow == "da_plus_imbalance":
        frame["da_revenue_eur"] = (
            frame["baseline_net_export_mw"] * frame["day_ahead_actual_price_eur_per_mwh"] * dt_hours
        )
        frame["expected_da_revenue_eur"] = (
            frame["baseline_net_export_mw"] * frame["day_ahead_forecast_price_eur_per_mwh"] * dt_hours
        )
        frame["imbalance_revenue_eur"] = settlement_engine.settle_imbalance(frame, dt_hours=float(dt_hours.iloc[0]))
        frame["expected_imbalance_revenue_eur"] = (
            frame["imbalance_mw"] * frame["imbalance_forecast_price_eur_per_mwh"].fillna(0.0) * dt_hours
        )
    else:
        frame["da_revenue_eur"] = frame["net_export_mw"] * frame["day_ahead_actual_price_eur_per_mwh"] * dt_hours
        frame["expected_da_revenue_eur"] = (
            frame["net_export_mw"] * frame["day_ahead_forecast_price_eur_per_mwh"] * dt_hours
        )
        frame["imbalance_revenue_eur"] = 0.0
        frame["expected_imbalance_revenue_eur"] = 0.0

    if workflow == "da_plus_fcr":
        frame["reserve_capacity_revenue_eur"] = (
            frame["fcr_reserved_mw"] * frame["fcr_capacity_price_actual_eur_per_mw_per_h"] * dt_hours
        )
        frame["expected_reserve_capacity_revenue_eur"] = (
            frame["fcr_reserved_mw"] * frame["fcr_capacity_price_forecast_eur_per_mw_per_h"] * dt_hours
        )
    elif workflow == "da_plus_afrr":
        frame["reserve_capacity_revenue_eur"] = (
            frame["afrr_up_reserved_mw"] * frame["afrr_capacity_up_price_actual_eur_per_mw_per_h"]
            + frame["afrr_down_reserved_mw"] * frame["afrr_capacity_down_price_actual_eur_per_mw_per_h"]
        ) * dt_hours
        frame["expected_reserve_capacity_revenue_eur"] = (
            frame["afrr_up_reserved_mw"] * frame["afrr_capacity_up_price_forecast_eur_per_mw_per_h"]
            + frame["afrr_down_reserved_mw"] * frame["afrr_capacity_down_price_forecast_eur_per_mw_per_h"]
        ) * dt_hours
        frame["reserve_activation_revenue_eur"] = (
            frame["afrr_up_reserved_mw"]
            * frame["afrr_activation_ratio_up_actual"]
            * frame["afrr_activation_price_up_actual_eur_per_mwh"]
            + frame["afrr_down_reserved_mw"]
            * frame["afrr_activation_ratio_down_actual"]
            * frame["afrr_activation_price_down_actual_eur_per_mwh"]
        ) * dt_hours
        frame["expected_reserve_activation_revenue_eur"] = (
            frame["afrr_up_reserved_mw"]
            * frame["afrr_activation_ratio_up_forecast"]
            * frame["afrr_activation_price_up_forecast_eur_per_mwh"]
            + frame["afrr_down_reserved_mw"]
            * frame["afrr_activation_ratio_down_forecast"]
            * frame["afrr_activation_price_down_forecast_eur_per_mwh"]
        ) * dt_hours

    frame["degradation_cost_eur"] = frame["throughput_mwh"] * degradation_cost_eur_per_mwh
    frame["realized_pnl_eur"] = (
        frame["da_revenue_eur"]
        + frame["imbalance_revenue_eur"]
        + frame["reserve_capacity_revenue_eur"]
        + frame["reserve_activation_revenue_eur"]
        - frame["reserve_penalty_eur"]
        - frame["degradation_cost_eur"]
    )
    frame["expected_pnl_eur"] = (
        frame["expected_da_revenue_eur"]
        + frame["expected_imbalance_revenue_eur"]
        + frame["expected_reserve_capacity_revenue_eur"]
        + frame["expected_reserve_activation_revenue_eur"]
        - frame["reserve_penalty_eur"]
        - frame["degradation_cost_eur"]
    )
    pnl = PnLAttribution(
        da_revenue_eur=float(frame["da_revenue_eur"].sum()),
        imbalance_revenue_eur=float(frame["imbalance_revenue_eur"].sum()),
        reserve_capacity_revenue_eur=float(frame["reserve_capacity_revenue_eur"].sum()),
        reserve_activation_revenue_eur=float(frame["reserve_activation_revenue_eur"].sum()),
        reserve_penalty_eur=float(frame["reserve_penalty_eur"].sum()),
        degradation_cost_eur=float(frame["degradation_cost_eur"].sum()),
        total_pnl_eur=float(frame["realized_pnl_eur"].sum()),
        expected_total_pnl_eur=float(frame["expected_pnl_eur"].sum()),
        metadata={"workflow": workflow},
    )
    return frame, pnl


def _reserve_penalty_eur_per_mw(config: BacktestConfig, *, workflow: str) -> float:
    if workflow == "da_plus_fcr" and config.fcr is not None:
        return float(config.fcr.non_delivery_penalty_eur_per_mw)
    if workflow == "da_plus_afrr" and config.afrr is not None:
        return float(config.afrr.non_delivery_penalty_eur_per_mw)
    return 0.0


def _asset_settlement(
    asset_dispatch: pd.DataFrame,
    *,
    workflow: str,
    degradation_costs_eur_per_mwh: dict[str, float],
    reserve_penalty_eur_per_mw: float = 0.0,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    frame = asset_dispatch.copy()
    dt_hours = frame["resolution_minutes"] / 60.0
    frame["throughput_mwh"] = (frame["charge_mw"] + frame["discharge_mw"]) * dt_hours
    frame["da_revenue_eur"] = frame["net_export_mw"] * frame["day_ahead_actual_price_eur_per_mwh"] * dt_hours
    frame["imbalance_revenue_eur"] = 0.0
    frame["reserve_activation_revenue_eur"] = 0.0
    if workflow == "da_plus_imbalance":
        if {
            "imbalance_surplus_price_eur_per_mwh",
            "imbalance_shortage_price_eur_per_mwh",
        }.issubset(frame.columns):
            effective_imbalance_price = frame["imbalance_surplus_price_eur_per_mwh"].where(
                frame["imbalance_mw"] >= 0.0,
                frame["imbalance_shortage_price_eur_per_mwh"],
            )
            frame["imbalance_revenue_eur"] = frame["imbalance_mw"] * effective_imbalance_price * dt_hours
        else:
            frame["imbalance_revenue_eur"] = (
                frame["imbalance_mw"] * frame["imbalance_actual_price_eur_per_mwh"] * dt_hours
            )
    frame["reserve_capacity_revenue_eur"] = 0.0
    if workflow == "da_plus_fcr":
        frame["reserve_capacity_revenue_eur"] = (
            frame["fcr_reserved_mw"] * frame["fcr_capacity_price_actual_eur_per_mw_per_h"] * dt_hours
        )
    elif workflow == "da_plus_afrr":
        frame["reserve_capacity_revenue_eur"] = (
            frame["afrr_up_reserved_mw"] * frame["afrr_capacity_up_price_actual_eur_per_mw_per_h"]
            + frame["afrr_down_reserved_mw"] * frame["afrr_capacity_down_price_actual_eur_per_mw_per_h"]
        ) * dt_hours
        frame["reserve_activation_revenue_eur"] = (
            frame["afrr_up_reserved_mw"]
            * frame["afrr_activation_ratio_up_actual"]
            * frame["afrr_activation_price_up_actual_eur_per_mwh"]
            + frame["afrr_down_reserved_mw"]
            * frame["afrr_activation_ratio_down_actual"]
            * frame["afrr_activation_price_down_actual_eur_per_mwh"]
        ) * dt_hours
    frame["reserve_penalty_eur"] = (
        (frame["fcr_reserved_mw"] + frame["afrr_up_reserved_mw"] + frame["afrr_down_reserved_mw"])
        * reserve_penalty_eur_per_mw
        * dt_hours
    )
    frame["degradation_cost_eur"] = frame.apply(
        lambda row: row["throughput_mwh"] * float(degradation_costs_eur_per_mwh.get(str(row["asset_id"]), 0.0)),
        axis=1,
    )
    frame["total_pnl_eur"] = (
        frame["da_revenue_eur"]
        + frame["imbalance_revenue_eur"]
        + frame["reserve_capacity_revenue_eur"]
        + frame["reserve_activation_revenue_eur"]
        - frame["reserve_penalty_eur"]
        - frame["degradation_cost_eur"]
    )
    asset_pnl = (
        frame.groupby(["asset_id", "site_id", "market_id", "workflow_family", "run_scope"], as_index=False)[
            [
                "da_revenue_eur",
                "imbalance_revenue_eur",
                "reserve_capacity_revenue_eur",
                "reserve_activation_revenue_eur",
                "reserve_penalty_eur",
                "degradation_cost_eur",
                "total_pnl_eur",
                "throughput_mwh",
                "fcr_reserved_mw",
                "afrr_up_reserved_mw",
                "afrr_down_reserved_mw",
            ]
        ]
        .sum()
        .rename(
            columns={
                "fcr_reserved_mw": "fcr_reserved_mwh_interval_sum",
                "afrr_up_reserved_mw": "afrr_up_reserved_mwh_interval_sum",
                "afrr_down_reserved_mw": "afrr_down_reserved_mwh_interval_sum",
            }
        )
    )
    return frame, asset_pnl


def _forecast_error_metrics(forecast: pd.Series, actual: pd.Series) -> dict[str, float]:
    frame = pd.DataFrame({"forecast": forecast, "actual": actual}).dropna()
    if frame.empty:
        return {"mae": 0.0, "rmse": 0.0, "bias": 0.0}
    error = frame["forecast"] - frame["actual"]
    return {
        "mae": float(error.abs().mean()),
        "rmse": float((error.pow(2).mean()) ** 0.5),
        "bias": float(error.mean()),
    }


def _scenario_forecast_columns_for_workflow(workflow: str) -> dict[str, str]:
    mapping = {
        "day_ahead": "day_ahead_forecast_price_eur_per_mwh",
    }
    if workflow == "da_plus_fcr":
        mapping["fcr_capacity"] = "fcr_capacity_price_forecast_eur_per_mw_per_h"
    elif workflow == "da_plus_afrr":
        mapping.update(
            {
                "afrr_capacity_up": "afrr_capacity_up_price_forecast_eur_per_mw_per_h",
                "afrr_capacity_down": "afrr_capacity_down_price_forecast_eur_per_mw_per_h",
                "afrr_activation_price_up": "afrr_activation_price_up_forecast_eur_per_mwh",
                "afrr_activation_price_down": "afrr_activation_price_down_forecast_eur_per_mwh",
                "afrr_activation_ratio_up": "afrr_activation_ratio_up_forecast",
                "afrr_activation_ratio_down": "afrr_activation_ratio_down_forecast",
            }
        )
    return mapping


def _scenario_settlement_inputs(
    dispatch: pd.DataFrame,
    snapshots: pd.DataFrame,
    *,
    workflow: str,
) -> pd.DataFrame | None:
    scenario_snapshots = snapshots.copy()
    if "scenario_id" not in scenario_snapshots.columns:
        return None
    scenario_snapshots = scenario_snapshots[scenario_snapshots["scenario_id"].notna()].copy()
    if scenario_snapshots.empty:
        return None

    forecast_columns = _scenario_forecast_columns_for_workflow(workflow)
    day_frame = scenario_snapshots[scenario_snapshots["market"] == "day_ahead"][
        ["delivery_start_utc", "schedule_version", "scenario_id", "scenario_weight", "forecast_price_eur_per_mwh"]
    ].rename(
        columns={
            "delivery_start_utc": "timestamp_utc",
            "forecast_price_eur_per_mwh": "day_ahead_forecast_price_eur_per_mwh",
        }
    )
    if day_frame.empty:
        return None

    scenario_dispatch = dispatch.merge(day_frame, on=["timestamp_utc", "schedule_version"], how="left")
    if scenario_dispatch["scenario_id"].isna().any():
        return None

    for market_name, target_column in forecast_columns.items():
        if market_name == "day_ahead":
            continue
        market_frame = scenario_snapshots[scenario_snapshots["market"] == market_name][
            ["delivery_start_utc", "schedule_version", "scenario_id", "forecast_price_eur_per_mwh"]
        ].rename(
            columns={
                "delivery_start_utc": "timestamp_utc",
                "forecast_price_eur_per_mwh": target_column,
            }
        )
        if market_frame.empty:
            scenario_dispatch[target_column] = dispatch.get(target_column, 0.0)
            continue
        scenario_dispatch = scenario_dispatch.merge(
            market_frame,
            on=["timestamp_utc", "schedule_version", "scenario_id"],
            how="left",
            suffixes=("", "__scenario"),
        )
        scenario_column = f"{target_column}__scenario"
        base_series = (
            scenario_dispatch[target_column]
            if target_column in scenario_dispatch.columns
            else pd.Series(dispatch.get(target_column, 0.0), index=scenario_dispatch.index)
        )
        scenario_dispatch[target_column] = scenario_dispatch[scenario_column].fillna(base_series)
        scenario_dispatch = scenario_dispatch.drop(columns=[scenario_column])

    for column in forecast_columns.values():
        if column not in scenario_dispatch.columns:
            scenario_dispatch[column] = dispatch.get(column, 0.0)
    return scenario_dispatch


def _weighted_tail_mean(values: pd.DataFrame, alpha: float) -> float:
    ordered = values.sort_values("total_pnl_eur", ascending=True).reset_index(drop=True)
    remaining = alpha
    weighted_total = 0.0
    weight_used = 0.0
    for row in ordered.itertuples():
        if remaining <= 0.0:
            break
        take = min(float(row.scenario_weight), remaining)
        weighted_total += float(row.total_pnl_eur) * take
        weight_used += take
        remaining -= take
    if weight_used <= 0.0:
        return float(ordered["total_pnl_eur"].min())
    return weighted_total / weight_used


def _scenario_analysis(
    dispatch: pd.DataFrame,
    snapshots: pd.DataFrame,
    *,
    workflow: str,
    degradation_cost_eur_per_mwh: float,
    reserve_penalty_eur_per_mw: float,
    risk: RiskPreference,
    settlement_engine,
    realized_total_pnl_eur: float | None = None,
) -> dict[str, object] | None:
    scenario_dispatch = _scenario_settlement_inputs(dispatch, snapshots, workflow=workflow)
    if scenario_dispatch is None:
        return None
    settled, _ = _site_interval_settlement(
        scenario_dispatch,
        workflow=workflow,
        degradation_cost_eur_per_mwh=degradation_cost_eur_per_mwh,
        settlement_engine=settlement_engine,
        reserve_penalty_eur_per_mw=reserve_penalty_eur_per_mw,
    )
    totals = (
        settled.groupby("scenario_id", as_index=False)[
            [
                "scenario_weight",
                "expected_pnl_eur",
                "expected_da_revenue_eur",
                "expected_imbalance_revenue_eur",
                "expected_reserve_capacity_revenue_eur",
                "expected_reserve_activation_revenue_eur",
            ]
        ]
        .agg(
            {
                "scenario_weight": "first",
                "expected_pnl_eur": "sum",
                "expected_da_revenue_eur": "sum",
                "expected_imbalance_revenue_eur": "sum",
                "expected_reserve_capacity_revenue_eur": "sum",
                "expected_reserve_activation_revenue_eur": "sum",
            }
        )
        .rename(
            columns={
                "expected_pnl_eur": "total_pnl_eur",
                "expected_da_revenue_eur": "da_revenue_eur",
                "expected_imbalance_revenue_eur": "imbalance_revenue_eur",
                "expected_reserve_capacity_revenue_eur": "reserve_capacity_revenue_eur",
                "expected_reserve_activation_revenue_eur": "reserve_activation_revenue_eur",
            }
        )
        .sort_values("scenario_id")
        .reset_index(drop=True)
    )
    if totals.empty:
        return None

    expected_total = float((totals["scenario_weight"] * totals["total_pnl_eur"]).sum())
    best_total = float(totals["total_pnl_eur"].max())
    worst_total = float(totals["total_pnl_eur"].min())
    reserve_total = totals["reserve_capacity_revenue_eur"] + totals["reserve_activation_revenue_eur"]
    downside_penalty = 0.0
    if risk.mode == "downside_penalty":
        downside_penalty = float(
            risk.penalty_lambda
            * ((expected_total - totals["total_pnl_eur"]).clip(lower=0.0) * totals["scenario_weight"]).sum()
        )
    elif risk.mode == "cvar_lite" and risk.tail_alpha is not None:
        tail_mean = _weighted_tail_mean(totals[["scenario_weight", "total_pnl_eur"]], risk.tail_alpha)
        downside_penalty = float(risk.penalty_lambda * max(expected_total - tail_mean, 0.0))

    payload: dict[str, object] = {
        "forecast_mode": "scenario_bundle",
        "risk_mode": risk.mode,
        "scenario_count": int(len(totals)),
        "scenario_expected_total_pnl_eur": expected_total,
        "scenario_best_total_pnl_eur": best_total,
        "scenario_worst_total_pnl_eur": worst_total,
        "scenario_spread_total_pnl_eur": float(best_total - worst_total),
        "downside_penalty_contribution_eur": downside_penalty,
        "reserve_fragility_eur": float(reserve_total.max() - reserve_total.min()),
        "scenario_weights": {
            str(row.scenario_id): float(row.scenario_weight)
            for row in totals[["scenario_id", "scenario_weight"]].itertuples(index=False)
        },
        "scenario_best_id": str(totals.loc[totals["total_pnl_eur"].idxmax(), "scenario_id"]),
        "scenario_worst_id": str(totals.loc[totals["total_pnl_eur"].idxmin(), "scenario_id"]),
    }
    if realized_total_pnl_eur is not None:
        nearest = totals.iloc[(totals["total_pnl_eur"] - realized_total_pnl_eur).abs().argmin()]
        if realized_total_pnl_eur < worst_total:
            envelope_distance = worst_total - realized_total_pnl_eur
            posture = "aggressive"
        elif realized_total_pnl_eur > best_total:
            envelope_distance = realized_total_pnl_eur - best_total
            posture = "conservative"
        else:
            envelope_distance = 0.0
            posture = "within_envelope"
        payload.update(
            {
                "nearest_scenario_id": str(nearest["scenario_id"]),
                "nearest_scenario_total_pnl_eur": float(nearest["total_pnl_eur"]),
                "realized_vs_scenario_envelope_distance_eur": float(envelope_distance),
                "scenario_posture": posture,
            }
        )
    return payload


def _single_asset_to_frames(
    dispatch: pd.DataFrame,
    *,
    config: BacktestConfig,
    decision_type: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    asset = config.primary_asset
    asset_dispatch = dispatch.copy()
    asset_dispatch["site_id"] = config.site.id
    asset_dispatch["asset_id"] = asset.id
    asset_dispatch["asset_name"] = asset.battery.name
    asset_dispatch["run_scope"] = config.run_scope
    if decision_type is not None:
        asset_dispatch["decision_type"] = decision_type

    site_dispatch = dispatch.copy()
    site_dispatch["site_id"] = config.site.id
    site_dispatch["run_scope"] = config.run_scope
    if decision_type is not None:
        site_dispatch["decision_type"] = decision_type
    return site_dispatch, asset_dispatch


def _decorate_portfolio_outputs(
    *,
    site_dispatch: pd.DataFrame,
    asset_dispatch: pd.DataFrame,
    config: BacktestConfig,
    decision_type: str,
    decision_time_utc: pd.Timestamp,
    market_id: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    site_frame = site_dispatch.copy()
    asset_frame = asset_dispatch.copy()
    for frame in (site_frame, asset_frame):
        frame["market_id"] = market_id
        frame["workflow_family"] = config.workflow
        frame["run_scope"] = config.run_scope
        frame["site_id"] = config.site.id
        frame["decision_type"] = decision_type
        frame["decision_time_utc"] = decision_time_utc
    return site_frame, asset_frame


def _annotate_schedule_frame(
    frame: pd.DataFrame,
    *,
    schedule_version: str,
    schedule_state: str,
    lock_state: str,
) -> pd.DataFrame:
    annotated = frame.copy()
    annotated["schedule_version"] = schedule_version
    annotated["schedule_state"] = schedule_state
    annotated["lock_state"] = lock_state
    return annotated


def _lineage_frame(frame: pd.DataFrame, *, entity_type: str) -> pd.DataFrame:
    lineage = frame.copy()
    lineage["entity_type"] = entity_type
    if "asset_id" not in lineage.columns:
        lineage["asset_id"] = None
    return lineage


def _oracle_reference(
    *,
    config: BacktestConfig,
    benchmark: BenchmarkDefinition,
    adapter,
    day_ahead_actual: pd.DataFrame,
    imbalance_actual: pd.DataFrame | None,
    fcr_actual: pd.DataFrame | None,
    afrr_capacity_up_actual: pd.DataFrame | None,
    afrr_capacity_down_actual: pd.DataFrame | None,
    afrr_activation_price_up_actual: pd.DataFrame | None,
    afrr_activation_price_down_actual: pd.DataFrame | None,
    afrr_activation_ratio_up_actual: pd.DataFrame | None,
    afrr_activation_ratio_down_actual: pd.DataFrame | None,
) -> OracleComparison:
    settlement_engine = adapter.settlement_engine(config.workflow)
    reserve_penalty = 0.0
    if config.fcr is not None:
        reserve_penalty = float(config.fcr.non_delivery_penalty_eur_per_mw)
    if config.afrr is not None:
        reserve_penalty = float(config.afrr.non_delivery_penalty_eur_per_mw)
    site_frames: list[pd.DataFrame] = []

    for delivery_date in _delivery_dates(config):
        day_da = _day_frame(day_ahead_actual, delivery_date)
        asset_dispatch: pd.DataFrame | None = None
        if config.workflow == "da_plus_fcr":
            if fcr_actual is None or config.fcr is None:
                raise ValueError("oracle reference requires realized FCR capacity data for da_plus_fcr")
            day_fcr = _day_frame(fcr_actual, delivery_date)
            if config.run_scope == "portfolio":
                portfolio_solution: PortfolioOptimizationOutput = solve_portfolio_day_ahead_fcr_dispatch(
                    day_ahead_frame=day_da,
                    fcr_capacity_frame=day_fcr,
                    site=config.site,
                    assets=config.assets,
                    degradation_costs_eur_per_mwh=_asset_degradation_costs(config),
                    reserve_sustain_duration_minutes=config.fcr.sustain_duration_minutes,
                    reserve_penalty_eur_per_mw=reserve_penalty,
                    strategy_name=benchmark.benchmark_name,
                )
                site_dispatch = portfolio_solution.site_dispatch.copy()
                asset_dispatch = portfolio_solution.asset_dispatch.copy()
            else:
                single_solution: OptimizationOutput = solve_day_ahead_fcr_dispatch(
                    day_ahead_frame=day_da,
                    fcr_capacity_frame=day_fcr,
                    battery=config.primary_asset.battery,
                    degradation_cost_eur_per_mwh=_degradation_cost_per_mwh(config, config.primary_asset),
                    reserve_sustain_duration_minutes=config.fcr.sustain_duration_minutes,
                    reserve_penalty_eur_per_mw=reserve_penalty,
                    initial_soc_mwh=config.primary_asset.battery.initial_soc_mwh,
                    terminal_soc_mwh=config.primary_asset.battery.terminal_soc_mwh,
                    strategy_name=benchmark.benchmark_name,
                )
                site_dispatch, asset_dispatch = _single_asset_to_frames(single_solution.dispatch.copy(), config=config)
            site_dispatch = _merge_forecast_snapshot(
                site_dispatch,
                pd.DataFrame(
                    {
                        "delivery_start_utc": day_da["timestamp_utc"],
                        "forecast_price_eur_per_mwh": day_da["price_eur_per_mwh"],
                    }
                ),
                target_column="day_ahead_forecast_price_eur_per_mwh",
            )
            site_dispatch = _merge_forecast_snapshot(
                site_dispatch,
                pd.DataFrame(
                    {
                        "delivery_start_utc": day_fcr["timestamp_utc"],
                        "forecast_price_eur_per_mwh": day_fcr["price_eur_per_mwh"],
                    }
                ),
                target_column="fcr_capacity_price_forecast_eur_per_mw_per_h",
            )
            site_dispatch = _merge_market_price(
                site_dispatch,
                day_da,
                target_column="day_ahead_actual_price_eur_per_mwh",
            )
            site_dispatch = _merge_market_price(
                site_dispatch,
                day_fcr,
                target_column="fcr_capacity_price_actual_eur_per_mw_per_h",
            )
            site_dispatch = _attach_optional_imbalance_columns(site_dispatch, None)
            site_dispatch = _attach_zero_afrr_columns(site_dispatch)
            if asset_dispatch is None:
                raise ValueError("Oracle FCR reference requires asset-level dispatch")
            asset_dispatch = _merge_forecast_snapshot(
                asset_dispatch,
                pd.DataFrame(
                    {
                        "delivery_start_utc": day_da["timestamp_utc"],
                        "forecast_price_eur_per_mwh": day_da["price_eur_per_mwh"],
                    }
                ),
                target_column="day_ahead_forecast_price_eur_per_mwh",
            )
            asset_dispatch = _merge_forecast_snapshot(
                asset_dispatch,
                pd.DataFrame(
                    {
                        "delivery_start_utc": day_fcr["timestamp_utc"],
                        "forecast_price_eur_per_mwh": day_fcr["price_eur_per_mwh"],
                    }
                ),
                target_column="fcr_capacity_price_forecast_eur_per_mw_per_h",
            )
            asset_dispatch = _merge_market_price(
                asset_dispatch, day_da, target_column="day_ahead_actual_price_eur_per_mwh"
            )
            asset_dispatch = _merge_market_price(
                asset_dispatch, day_fcr, target_column="fcr_capacity_price_actual_eur_per_mw_per_h"
            )
            asset_dispatch = _attach_optional_imbalance_columns(asset_dispatch, None)
            asset_dispatch = _attach_zero_afrr_columns(asset_dispatch)
        elif config.workflow == "da_plus_afrr":
            if (
                afrr_capacity_up_actual is None
                or afrr_capacity_down_actual is None
                or afrr_activation_price_up_actual is None
                or afrr_activation_price_down_actual is None
                or afrr_activation_ratio_up_actual is None
                or afrr_activation_ratio_down_actual is None
                or config.afrr is None
            ):
                raise ValueError("oracle reference requires realized aFRR data for da_plus_afrr")
            day_capacity_up = _day_frame(afrr_capacity_up_actual, delivery_date)
            day_capacity_down = _day_frame(afrr_capacity_down_actual, delivery_date)
            day_activation_up = _day_frame(afrr_activation_price_up_actual, delivery_date)
            day_activation_down = _day_frame(afrr_activation_price_down_actual, delivery_date)
            day_ratio_up = _day_frame(afrr_activation_ratio_up_actual, delivery_date)
            day_ratio_down = _day_frame(afrr_activation_ratio_down_actual, delivery_date)
            if config.run_scope == "portfolio":
                portfolio_solution = solve_portfolio_day_ahead_afrr_dispatch(
                    day_ahead_frame=day_da,
                    afrr_capacity_up_frame=day_capacity_up,
                    afrr_capacity_down_frame=day_capacity_down,
                    afrr_activation_price_up_frame=day_activation_up,
                    afrr_activation_price_down_frame=day_activation_down,
                    afrr_activation_ratio_up_frame=day_ratio_up,
                    afrr_activation_ratio_down_frame=day_ratio_down,
                    site=config.site,
                    assets=config.assets,
                    degradation_costs_eur_per_mwh=_asset_degradation_costs(config),
                    reserve_sustain_duration_minutes=config.afrr.sustain_duration_minutes,
                    reserve_penalty_eur_per_mw=reserve_penalty,
                    strategy_name=benchmark.benchmark_name,
                )
                site_dispatch = portfolio_solution.site_dispatch.copy()
                asset_dispatch = portfolio_solution.asset_dispatch.copy()
            else:
                single_solution = solve_day_ahead_afrr_dispatch(
                    day_ahead_frame=day_da,
                    afrr_capacity_up_frame=day_capacity_up,
                    afrr_capacity_down_frame=day_capacity_down,
                    afrr_activation_price_up_frame=day_activation_up,
                    afrr_activation_price_down_frame=day_activation_down,
                    afrr_activation_ratio_up_frame=day_ratio_up,
                    afrr_activation_ratio_down_frame=day_ratio_down,
                    battery=config.primary_asset.battery,
                    degradation_cost_eur_per_mwh=_degradation_cost_per_mwh(config, config.primary_asset),
                    reserve_sustain_duration_minutes=config.afrr.sustain_duration_minutes,
                    reserve_penalty_eur_per_mw=reserve_penalty,
                    initial_soc_mwh=config.primary_asset.battery.initial_soc_mwh,
                    terminal_soc_mwh=config.primary_asset.battery.terminal_soc_mwh,
                    strategy_name=benchmark.benchmark_name,
                )
                site_dispatch, asset_dispatch = _single_asset_to_frames(single_solution.dispatch.copy(), config=config)
            actual_snapshot = pd.DataFrame(
                {
                    "delivery_start_utc": day_da["timestamp_utc"],
                    "forecast_price_eur_per_mwh": day_da["price_eur_per_mwh"],
                }
            )
            site_dispatch = _merge_forecast_snapshot(
                site_dispatch,
                actual_snapshot,
                target_column="day_ahead_forecast_price_eur_per_mwh",
            )
            site_dispatch = _merge_market_price(
                site_dispatch, day_da, target_column="day_ahead_actual_price_eur_per_mwh"
            )
            site_dispatch = _attach_optional_imbalance_columns(site_dispatch, None)
            site_dispatch = _attach_zero_fcr_columns(site_dispatch)
            site_dispatch = _merge_afrr_snapshot_columns(
                site_dispatch,
                capacity_up=pd.DataFrame(
                    {
                        "delivery_start_utc": day_capacity_up["timestamp_utc"],
                        "forecast_price_eur_per_mwh": day_capacity_up["price_eur_per_mwh"],
                    }
                ),
                capacity_down=pd.DataFrame(
                    {
                        "delivery_start_utc": day_capacity_down["timestamp_utc"],
                        "forecast_price_eur_per_mwh": day_capacity_down["price_eur_per_mwh"],
                    }
                ),
                activation_price_up=pd.DataFrame(
                    {
                        "delivery_start_utc": day_activation_up["timestamp_utc"],
                        "forecast_price_eur_per_mwh": day_activation_up["price_eur_per_mwh"],
                    }
                ),
                activation_price_down=pd.DataFrame(
                    {
                        "delivery_start_utc": day_activation_down["timestamp_utc"],
                        "forecast_price_eur_per_mwh": day_activation_down["price_eur_per_mwh"],
                    }
                ),
                activation_ratio_up=pd.DataFrame(
                    {
                        "delivery_start_utc": day_ratio_up["timestamp_utc"],
                        "forecast_price_eur_per_mwh": day_ratio_up["price_eur_per_mwh"],
                    }
                ),
                activation_ratio_down=pd.DataFrame(
                    {
                        "delivery_start_utc": day_ratio_down["timestamp_utc"],
                        "forecast_price_eur_per_mwh": day_ratio_down["price_eur_per_mwh"],
                    }
                ),
            )
            site_dispatch = _merge_afrr_actual_columns(
                site_dispatch,
                capacity_up=day_capacity_up,
                capacity_down=day_capacity_down,
                activation_price_up=day_activation_up,
                activation_price_down=day_activation_down,
                activation_ratio_up=day_ratio_up,
                activation_ratio_down=day_ratio_down,
            )
            if asset_dispatch is None:
                raise ValueError("Oracle aFRR reference requires asset-level dispatch")
            asset_dispatch = _merge_forecast_snapshot(
                asset_dispatch,
                actual_snapshot,
                target_column="day_ahead_forecast_price_eur_per_mwh",
            )
            asset_dispatch = _merge_market_price(
                asset_dispatch, day_da, target_column="day_ahead_actual_price_eur_per_mwh"
            )
            asset_dispatch = _attach_optional_imbalance_columns(asset_dispatch, None)
            asset_dispatch = _attach_zero_fcr_columns(asset_dispatch)
            asset_dispatch = _merge_afrr_snapshot_columns(
                asset_dispatch,
                capacity_up=pd.DataFrame(
                    {
                        "delivery_start_utc": day_capacity_up["timestamp_utc"],
                        "forecast_price_eur_per_mwh": day_capacity_up["price_eur_per_mwh"],
                    }
                ),
                capacity_down=pd.DataFrame(
                    {
                        "delivery_start_utc": day_capacity_down["timestamp_utc"],
                        "forecast_price_eur_per_mwh": day_capacity_down["price_eur_per_mwh"],
                    }
                ),
                activation_price_up=pd.DataFrame(
                    {
                        "delivery_start_utc": day_activation_up["timestamp_utc"],
                        "forecast_price_eur_per_mwh": day_activation_up["price_eur_per_mwh"],
                    }
                ),
                activation_price_down=pd.DataFrame(
                    {
                        "delivery_start_utc": day_activation_down["timestamp_utc"],
                        "forecast_price_eur_per_mwh": day_activation_down["price_eur_per_mwh"],
                    }
                ),
                activation_ratio_up=pd.DataFrame(
                    {
                        "delivery_start_utc": day_ratio_up["timestamp_utc"],
                        "forecast_price_eur_per_mwh": day_ratio_up["price_eur_per_mwh"],
                    }
                ),
                activation_ratio_down=pd.DataFrame(
                    {
                        "delivery_start_utc": day_ratio_down["timestamp_utc"],
                        "forecast_price_eur_per_mwh": day_ratio_down["price_eur_per_mwh"],
                    }
                ),
            )
            asset_dispatch = _merge_afrr_actual_columns(
                asset_dispatch,
                capacity_up=day_capacity_up,
                capacity_down=day_capacity_down,
                activation_price_up=day_activation_up,
                activation_price_down=day_activation_down,
                activation_ratio_up=day_ratio_up,
                activation_ratio_down=day_ratio_down,
            )
        elif config.workflow == "da_plus_imbalance":
            if imbalance_actual is None:
                raise ValueError("oracle reference requires realized imbalance data")
            day_imb = _day_frame(imbalance_actual, delivery_date)
            baseline = solve_day_ahead_dispatch(
                day_da,
                config.primary_asset.battery,
                degradation_cost_eur_per_mwh=_degradation_cost_per_mwh(config, config.primary_asset),
                initial_soc_mwh=config.primary_asset.battery.initial_soc_mwh,
                terminal_soc_mwh=config.primary_asset.battery.terminal_soc_mwh,
                strategy_name=benchmark.benchmark_name,
            )
            overlay_solution: OptimizationOutput = solve_imbalance_overlay_dispatch(
                day_ahead_frame=day_da,
                imbalance_frame=day_imb,
                battery=config.primary_asset.battery,
                baseline_dispatch=baseline.dispatch,
                degradation_cost_eur_per_mwh=_degradation_cost_per_mwh(config, config.primary_asset),
                initial_soc_mwh=config.primary_asset.battery.initial_soc_mwh,
                terminal_soc_mwh=config.primary_asset.battery.terminal_soc_mwh,
                strategy_name=benchmark.benchmark_name,
            )
            site_dispatch, asset_dispatch = _single_asset_to_frames(overlay_solution.dispatch.copy(), config=config)
            site_dispatch["baseline_net_export_mw"] = baseline.dispatch["net_export_mw"].values
            site_dispatch["imbalance_mw"] = site_dispatch["net_export_mw"] - site_dispatch["baseline_net_export_mw"]
            site_dispatch = _merge_forecast_snapshot(
                site_dispatch,
                pd.DataFrame(
                    {
                        "delivery_start_utc": day_da["timestamp_utc"],
                        "forecast_price_eur_per_mwh": day_da["price_eur_per_mwh"],
                    }
                ),
                target_column="day_ahead_forecast_price_eur_per_mwh",
            )
            site_dispatch = _merge_forecast_snapshot(
                site_dispatch,
                pd.DataFrame(
                    {
                        "delivery_start_utc": day_imb["timestamp_utc"],
                        "forecast_price_eur_per_mwh": day_imb["price_eur_per_mwh"],
                    }
                ),
                target_column="imbalance_forecast_price_eur_per_mwh",
            )
            site_dispatch = _merge_market_price(
                site_dispatch,
                day_da,
                target_column="day_ahead_actual_price_eur_per_mwh",
            )
            site_dispatch = _attach_optional_imbalance_columns(site_dispatch, day_imb)
            site_dispatch = _attach_zero_fcr_columns(site_dispatch)
            site_dispatch = _attach_zero_afrr_columns(site_dispatch)
            asset_dispatch = _merge_forecast_snapshot(
                asset_dispatch,
                pd.DataFrame(
                    {
                        "delivery_start_utc": day_da["timestamp_utc"],
                        "forecast_price_eur_per_mwh": day_da["price_eur_per_mwh"],
                    }
                ),
                target_column="day_ahead_forecast_price_eur_per_mwh",
            )
            asset_dispatch = _merge_forecast_snapshot(
                asset_dispatch,
                pd.DataFrame(
                    {
                        "delivery_start_utc": day_imb["timestamp_utc"],
                        "forecast_price_eur_per_mwh": day_imb["price_eur_per_mwh"],
                    }
                ),
                target_column="imbalance_forecast_price_eur_per_mwh",
            )
            asset_dispatch["baseline_net_export_mw"] = baseline.dispatch["net_export_mw"].values
            asset_dispatch["imbalance_mw"] = asset_dispatch["net_export_mw"] - asset_dispatch["baseline_net_export_mw"]
            asset_dispatch = _merge_market_price(
                asset_dispatch, day_da, target_column="day_ahead_actual_price_eur_per_mwh"
            )
            asset_dispatch = _attach_optional_imbalance_columns(asset_dispatch, day_imb)
            asset_dispatch = _attach_zero_fcr_columns(asset_dispatch)
            asset_dispatch = _attach_zero_afrr_columns(asset_dispatch)
        else:
            if config.run_scope == "portfolio":
                portfolio_solution = solve_portfolio_day_ahead_dispatch(
                    day_da,
                    config.site,
                    config.assets,
                    degradation_costs_eur_per_mwh=_asset_degradation_costs(config),
                    strategy_name=benchmark.benchmark_name,
                )
                site_dispatch = portfolio_solution.site_dispatch.copy()
                asset_dispatch = portfolio_solution.asset_dispatch.copy()
            else:
                single_solution = solve_day_ahead_dispatch(
                    day_da,
                    config.primary_asset.battery,
                    degradation_cost_eur_per_mwh=_degradation_cost_per_mwh(config, config.primary_asset),
                    initial_soc_mwh=config.primary_asset.battery.initial_soc_mwh,
                    terminal_soc_mwh=config.primary_asset.battery.terminal_soc_mwh,
                    strategy_name=benchmark.benchmark_name,
                )
                site_dispatch, asset_dispatch = _single_asset_to_frames(single_solution.dispatch.copy(), config=config)
            site_dispatch = _merge_forecast_snapshot(
                site_dispatch,
                pd.DataFrame(
                    {
                        "delivery_start_utc": day_da["timestamp_utc"],
                        "forecast_price_eur_per_mwh": day_da["price_eur_per_mwh"],
                    }
                ),
                target_column="day_ahead_forecast_price_eur_per_mwh",
            )
            site_dispatch = _merge_market_price(
                site_dispatch,
                day_da,
                target_column="day_ahead_actual_price_eur_per_mwh",
            )
            site_dispatch = _attach_optional_imbalance_columns(site_dispatch, None)
            site_dispatch = _attach_zero_fcr_columns(site_dispatch)
            site_dispatch = _attach_zero_afrr_columns(site_dispatch)
            if asset_dispatch is None:
                raise ValueError("Oracle day-ahead reference requires asset-level dispatch")
            asset_dispatch = _merge_forecast_snapshot(
                asset_dispatch,
                pd.DataFrame(
                    {
                        "delivery_start_utc": day_da["timestamp_utc"],
                        "forecast_price_eur_per_mwh": day_da["price_eur_per_mwh"],
                    }
                ),
                target_column="day_ahead_forecast_price_eur_per_mwh",
            )
            asset_dispatch = _merge_market_price(
                asset_dispatch, day_da, target_column="day_ahead_actual_price_eur_per_mwh"
            )
            asset_dispatch = _attach_optional_imbalance_columns(asset_dispatch, None)
            asset_dispatch = _attach_zero_fcr_columns(asset_dispatch)
            asset_dispatch = _attach_zero_afrr_columns(asset_dispatch)

        site_dispatch = _ensure_dispatch_columns(site_dispatch, site_id=config.site.id, run_scope=config.run_scope)
        site_dispatch["market_id"] = adapter.market_id
        site_dispatch["workflow_family"] = config.workflow
        site_dispatch = assign_site_reason_codes(site_dispatch, config.site)
        settled, _ = _site_interval_settlement(
            site_dispatch,
            workflow=config.workflow,
            degradation_cost_eur_per_mwh=0.0,
            settlement_engine=settlement_engine,
            reserve_penalty_eur_per_mw=reserve_penalty,
        )
        if asset_dispatch is None:
            raise ValueError("Oracle reference requires asset-level dispatch")
        asset_dispatch = _ensure_dispatch_columns(asset_dispatch, site_id=config.site.id, run_scope=config.run_scope)
        asset_dispatch["market_id"] = adapter.market_id
        asset_dispatch["workflow_family"] = config.workflow
        asset_dispatch, _ = _asset_settlement(
            asset_dispatch,
            workflow=config.workflow,
            degradation_costs_eur_per_mwh=_asset_degradation_costs(config),
            reserve_penalty_eur_per_mw=reserve_penalty,
        )
        degradation_by_timestamp = (
            asset_dispatch.groupby("timestamp_utc")["degradation_cost_eur"]
            .sum()
            .reindex(settled["timestamp_utc"])
            .values
        )
        settled["degradation_cost_eur"] = degradation_by_timestamp
        settled["realized_pnl_eur"] = (
            settled["da_revenue_eur"]
            + settled["imbalance_revenue_eur"]
            + settled["reserve_capacity_revenue_eur"]
            + settled["reserve_activation_revenue_eur"]
            - settled["reserve_penalty_eur"]
            - settled["degradation_cost_eur"]
        )
        settled["expected_pnl_eur"] = (
            settled["expected_da_revenue_eur"]
            + settled["expected_imbalance_revenue_eur"]
            + settled["expected_reserve_capacity_revenue_eur"]
            + settled["expected_reserve_activation_revenue_eur"]
            - settled["reserve_penalty_eur"]
            - settled["degradation_cost_eur"]
        )
        site_frames.append(settled)

    oracle_frame = pd.concat(site_frames, ignore_index=True) if site_frames else pd.DataFrame()
    return OracleComparison(
        benchmark_name=benchmark.benchmark_name,
        total_pnl_eur=float(oracle_frame["realized_pnl_eur"].sum()),
        da_revenue_eur=float(oracle_frame["da_revenue_eur"].sum()),
        imbalance_revenue_eur=float(oracle_frame["imbalance_revenue_eur"].sum()),
        reserve_capacity_revenue_eur=float(oracle_frame["reserve_capacity_revenue_eur"].sum()),
        reserve_activation_revenue_eur=float(oracle_frame["reserve_activation_revenue_eur"].sum()),
        reserve_penalty_eur=float(oracle_frame["reserve_penalty_eur"].sum()),
        degradation_cost_eur=float(oracle_frame["degradation_cost_eur"].sum()),
    )


def _portfolio_da_daily(
    *,
    config: BacktestConfig,
    adapter,
    benchmark: BenchmarkDefinition,
    provider,
    delivery_date: pd.Timestamp,
    day_ahead_actual: pd.DataFrame,
    all_day_ahead_actual: pd.DataFrame,
    schedule: pd.DataFrame,
) -> DailyArtifacts:
    day_da = _day_frame(day_ahead_actual, delivery_date)
    if day_da.empty:
        raise ValueError(f"No day-ahead actual data found for delivery_date={delivery_date.date()}")
    schedule_row = schedule[schedule["delivery_date_local"] == str(delivery_date.date())]
    if schedule_row.empty:
        raise ValueError(f"No decision schedule available for delivery_date={delivery_date.date()}")
    decision_time = pd.Timestamp(schedule_row.iloc[0]["day_ahead_gate_closure_utc"])
    visible_frames = {"day_ahead": all_day_ahead_actual}
    day_snapshot = _provider_forecast(
        market="day_ahead",
        decision_time_utc=decision_time,
        delivery_frame=day_da,
        actual_frame=all_day_ahead_actual,
        provider=provider,
        visible_frames=visible_frames,
    )
    if config.forecast_provider.mode == "scenario_bundle":
        solution = solve_portfolio_day_ahead_dispatch_scenario(
            price_frame=day_da,
            price_snapshot=day_snapshot,
            site=config.site,
            assets=config.assets,
            risk=_risk_preference(config),
            degradation_costs_eur_per_mwh=_asset_degradation_costs(config),
            strategy_name=benchmark.benchmark_name,
        )
    else:
        da_input = day_da.copy()
        da_input["price_eur_per_mwh"] = _expected_snapshot(day_snapshot)["forecast_price_eur_per_mwh"].values
        solution = solve_portfolio_day_ahead_dispatch(
            da_input,
            config.site,
            config.assets,
            degradation_costs_eur_per_mwh=_asset_degradation_costs(config),
            strategy_name=benchmark.benchmark_name,
        )
    site_dispatch, asset_dispatch = _decorate_portfolio_outputs(
        site_dispatch=solution.site_dispatch,
        asset_dispatch=solution.asset_dispatch,
        config=config,
        decision_type="day_ahead_nomination",
        decision_time_utc=decision_time,
        market_id=adapter.market_id,
    )
    site_dispatch = _merge_forecast_snapshot(
        site_dispatch, day_snapshot, target_column="day_ahead_forecast_price_eur_per_mwh"
    )
    site_dispatch = _merge_market_price(site_dispatch, day_da, target_column="day_ahead_actual_price_eur_per_mwh")
    site_dispatch = _attach_optional_imbalance_columns(site_dispatch, None)
    site_dispatch["fcr_capacity_price_forecast_eur_per_mw_per_h"] = 0.0
    site_dispatch["fcr_capacity_price_actual_eur_per_mw_per_h"] = 0.0

    asset_dispatch = _merge_forecast_snapshot(
        asset_dispatch, day_snapshot, target_column="day_ahead_forecast_price_eur_per_mwh"
    )
    asset_dispatch = _merge_market_price(asset_dispatch, day_da, target_column="day_ahead_actual_price_eur_per_mwh")
    asset_dispatch = _attach_optional_imbalance_columns(asset_dispatch, None)
    asset_dispatch["fcr_capacity_price_forecast_eur_per_mw_per_h"] = 0.0
    asset_dispatch["fcr_capacity_price_actual_eur_per_mw_per_h"] = 0.0
    asset_dispatch = _ensure_dispatch_columns(asset_dispatch, site_id=config.site.id, run_scope=config.run_scope)
    asset_dispatch = pd.concat(
        [
            assign_reason_codes(
                asset_dispatch[asset_dispatch["asset_id"] == asset.id].copy(),
                asset.battery,
                overlay=False,
            )
            for asset in config.assets
        ],
        ignore_index=True,
    )
    site_dispatch = _ensure_dispatch_columns(site_dispatch, site_id=config.site.id, run_scope=config.run_scope)
    site_dispatch = assign_site_reason_codes(site_dispatch, config.site)

    decision_id = f"{delivery_date.date()}-da-0"
    decisions = [
        {
            "decision_id": decision_id,
            "market_id": adapter.market_id,
            "workflow_family": config.workflow,
            "run_scope": config.run_scope,
            "site_id": config.site.id,
            "decision_time_utc": decision_time,
            "decision_time_local": decision_time.tz_convert(adapter.timezone),
            "decision_type": "day_ahead_nomination",
            "delivery_date_local": str(delivery_date.date()),
            "horizon_start_utc": day_da["timestamp_utc"].iloc[0],
            "horizon_end_utc": day_da["timestamp_utc"].iloc[-1]
            + pd.Timedelta(minutes=config.timing.resolution_minutes),
            "locked_intervals": len(day_da),
            "provider_name": provider.name,
            "benchmark_name": benchmark.benchmark_name,
            "objective_value_eur": solution.objective_value_eur,
            "solver_name": solution.solver_name,
            "schedule_version": "baseline",
        }
    ]
    snapshots = [
        _record_snapshot(
            day_snapshot,
            decision_id=decision_id,
            decision_time_utc=decision_time,
            decision_type="day_ahead_nomination",
            schedule_version="baseline",
            benchmark_name=benchmark.benchmark_name,
            market_id=adapter.market_id,
            workflow_family=config.workflow,
            run_scope=config.run_scope,
            site_id=config.site.id,
        )
    ]
    return DailyArtifacts(
        site_dispatch=site_dispatch, asset_dispatch=asset_dispatch, decisions=decisions, snapshots=snapshots
    )


def _portfolio_fcr_daily(
    *,
    config: BacktestConfig,
    adapter,
    benchmark: BenchmarkDefinition,
    provider,
    delivery_date: pd.Timestamp,
    day_ahead_actual: pd.DataFrame,
    fcr_actual: pd.DataFrame,
    all_day_ahead_actual: pd.DataFrame,
    all_fcr_actual: pd.DataFrame,
    schedule: pd.DataFrame,
) -> DailyArtifacts:
    if config.fcr is None:
        raise ValueError("da_plus_fcr requires an fcr configuration block")
    day_da = _day_frame(day_ahead_actual, delivery_date)
    day_fcr = _day_frame(fcr_actual, delivery_date)
    schedule_row = schedule[schedule["delivery_date_local"] == str(delivery_date.date())]
    if schedule_row.empty:
        raise ValueError(f"No decision schedule available for delivery_date={delivery_date.date()}")
    decision_time = pd.Timestamp(schedule_row.iloc[0]["day_ahead_gate_closure_utc"])
    visible_frames = {
        "day_ahead": all_day_ahead_actual,
        "fcr_capacity": all_fcr_actual,
    }
    day_snapshot = _provider_forecast(
        market="day_ahead",
        decision_time_utc=decision_time,
        delivery_frame=day_da,
        actual_frame=all_day_ahead_actual,
        provider=provider,
        visible_frames=visible_frames,
    )
    fcr_snapshot = _provider_forecast(
        market="fcr_capacity",
        decision_time_utc=decision_time,
        delivery_frame=day_fcr,
        actual_frame=all_fcr_actual,
        provider=provider,
        visible_frames=visible_frames,
    )
    if config.forecast_provider.mode == "scenario_bundle":
        solution = solve_portfolio_day_ahead_fcr_dispatch_scenario(
            day_ahead_frame=day_da,
            day_ahead_snapshot=day_snapshot,
            fcr_capacity_snapshot=fcr_snapshot,
            site=config.site,
            assets=config.assets,
            risk=_risk_preference(config),
            degradation_costs_eur_per_mwh=_asset_degradation_costs(config),
            reserve_sustain_duration_minutes=config.fcr.sustain_duration_minutes,
            reserve_penalty_eur_per_mw=float(config.fcr.non_delivery_penalty_eur_per_mw),
            strategy_name=benchmark.benchmark_name,
        )
    else:
        da_input = day_da.copy()
        da_input["price_eur_per_mwh"] = _expected_snapshot(day_snapshot)["forecast_price_eur_per_mwh"].values
        fcr_input = day_fcr.copy()
        fcr_input["price_eur_per_mwh"] = _expected_snapshot(fcr_snapshot)["forecast_price_eur_per_mwh"].values
        solution = solve_portfolio_day_ahead_fcr_dispatch(
            day_ahead_frame=da_input,
            fcr_capacity_frame=fcr_input,
            site=config.site,
            assets=config.assets,
            degradation_costs_eur_per_mwh=_asset_degradation_costs(config),
            reserve_sustain_duration_minutes=config.fcr.sustain_duration_minutes,
            reserve_penalty_eur_per_mw=float(config.fcr.non_delivery_penalty_eur_per_mw),
            strategy_name=benchmark.benchmark_name,
        )
    site_dispatch, asset_dispatch = _decorate_portfolio_outputs(
        site_dispatch=solution.site_dispatch,
        asset_dispatch=solution.asset_dispatch,
        config=config,
        decision_type="day_ahead_fcr_nomination",
        decision_time_utc=decision_time,
        market_id=adapter.market_id,
    )
    site_dispatch = _merge_forecast_snapshot(
        site_dispatch, day_snapshot, target_column="day_ahead_forecast_price_eur_per_mwh"
    )
    site_dispatch = _merge_forecast_snapshot(
        site_dispatch, fcr_snapshot, target_column="fcr_capacity_price_forecast_eur_per_mw_per_h"
    )
    site_dispatch = _merge_market_price(site_dispatch, day_da, target_column="day_ahead_actual_price_eur_per_mwh")
    site_dispatch = _merge_market_price(
        site_dispatch, day_fcr, target_column="fcr_capacity_price_actual_eur_per_mw_per_h"
    )
    site_dispatch = _attach_optional_imbalance_columns(site_dispatch, None)

    asset_dispatch = _merge_forecast_snapshot(
        asset_dispatch, day_snapshot, target_column="day_ahead_forecast_price_eur_per_mwh"
    )
    asset_dispatch = _merge_forecast_snapshot(
        asset_dispatch, fcr_snapshot, target_column="fcr_capacity_price_forecast_eur_per_mw_per_h"
    )
    asset_dispatch = _merge_market_price(asset_dispatch, day_da, target_column="day_ahead_actual_price_eur_per_mwh")
    asset_dispatch = _merge_market_price(
        asset_dispatch, day_fcr, target_column="fcr_capacity_price_actual_eur_per_mw_per_h"
    )
    asset_dispatch = _attach_optional_imbalance_columns(asset_dispatch, None)
    asset_dispatch = _ensure_dispatch_columns(asset_dispatch, site_id=config.site.id, run_scope=config.run_scope)
    asset_dispatch = pd.concat(
        [
            assign_reason_codes(
                asset_dispatch[asset_dispatch["asset_id"] == asset.id].copy(),
                asset.battery,
                overlay=False,
            )
            for asset in config.assets
        ],
        ignore_index=True,
    )
    site_dispatch = _ensure_dispatch_columns(site_dispatch, site_id=config.site.id, run_scope=config.run_scope)
    site_dispatch = assign_site_reason_codes(site_dispatch, config.site)

    decision_id = f"{delivery_date.date()}-fcr-0"
    decisions = [
        {
            "decision_id": decision_id,
            "market_id": adapter.market_id,
            "workflow_family": config.workflow,
            "run_scope": config.run_scope,
            "site_id": config.site.id,
            "decision_time_utc": decision_time,
            "decision_time_local": decision_time.tz_convert(adapter.timezone),
            "decision_type": "day_ahead_fcr_nomination",
            "delivery_date_local": str(delivery_date.date()),
            "horizon_start_utc": day_da["timestamp_utc"].iloc[0],
            "horizon_end_utc": day_da["timestamp_utc"].iloc[-1]
            + pd.Timedelta(minutes=config.timing.resolution_minutes),
            "locked_intervals": len(day_da),
            "provider_name": provider.name,
            "benchmark_name": benchmark.benchmark_name,
            "objective_value_eur": solution.objective_value_eur,
            "solver_name": solution.solver_name,
            "schedule_version": "baseline",
        }
    ]
    snapshots = [
        _record_snapshot(
            day_snapshot,
            decision_id=decision_id,
            decision_time_utc=decision_time,
            decision_type="day_ahead_fcr_nomination",
            schedule_version="baseline",
            benchmark_name=benchmark.benchmark_name,
            market_id=adapter.market_id,
            workflow_family=config.workflow,
            run_scope=config.run_scope,
            site_id=config.site.id,
        ),
        _record_snapshot(
            fcr_snapshot,
            decision_id=decision_id,
            decision_time_utc=decision_time,
            decision_type="day_ahead_fcr_nomination",
            schedule_version="baseline",
            benchmark_name=benchmark.benchmark_name,
            market_id=adapter.market_id,
            workflow_family=config.workflow,
            run_scope=config.run_scope,
            site_id=config.site.id,
        ),
    ]
    return DailyArtifacts(
        site_dispatch=site_dispatch, asset_dispatch=asset_dispatch, decisions=decisions, snapshots=snapshots
    )


def _portfolio_afrr_daily(
    *,
    config: BacktestConfig,
    adapter,
    benchmark: BenchmarkDefinition,
    provider,
    delivery_date: pd.Timestamp,
    day_ahead_actual: pd.DataFrame,
    afrr_capacity_up_actual: pd.DataFrame,
    afrr_capacity_down_actual: pd.DataFrame,
    afrr_activation_price_up_actual: pd.DataFrame,
    afrr_activation_price_down_actual: pd.DataFrame,
    afrr_activation_ratio_up_actual: pd.DataFrame,
    afrr_activation_ratio_down_actual: pd.DataFrame,
    all_day_ahead_actual: pd.DataFrame,
    all_afrr_capacity_up_actual: pd.DataFrame,
    all_afrr_capacity_down_actual: pd.DataFrame,
    all_afrr_activation_price_up_actual: pd.DataFrame,
    all_afrr_activation_price_down_actual: pd.DataFrame,
    all_afrr_activation_ratio_up_actual: pd.DataFrame,
    all_afrr_activation_ratio_down_actual: pd.DataFrame,
    schedule: pd.DataFrame,
) -> DailyArtifacts:
    if config.afrr is None:
        raise ValueError("da_plus_afrr requires an afrr configuration block")
    day_da = _day_frame(day_ahead_actual, delivery_date)
    day_capacity_up = _day_frame(afrr_capacity_up_actual, delivery_date)
    day_capacity_down = _day_frame(afrr_capacity_down_actual, delivery_date)
    day_activation_up = _day_frame(afrr_activation_price_up_actual, delivery_date)
    day_activation_down = _day_frame(afrr_activation_price_down_actual, delivery_date)
    day_ratio_up = _day_frame(afrr_activation_ratio_up_actual, delivery_date)
    day_ratio_down = _day_frame(afrr_activation_ratio_down_actual, delivery_date)
    schedule_row = schedule[schedule["delivery_date_local"] == str(delivery_date.date())]
    if schedule_row.empty:
        raise ValueError(f"No decision schedule available for delivery_date={delivery_date.date()}")
    decision_time = pd.Timestamp(schedule_row.iloc[0]["day_ahead_gate_closure_utc"])
    visible_frames = {
        "day_ahead": all_day_ahead_actual,
        "afrr_capacity_up": all_afrr_capacity_up_actual,
        "afrr_capacity_down": all_afrr_capacity_down_actual,
        "afrr_activation_price_up": all_afrr_activation_price_up_actual,
        "afrr_activation_price_down": all_afrr_activation_price_down_actual,
        "afrr_activation_ratio_up": all_afrr_activation_ratio_up_actual,
        "afrr_activation_ratio_down": all_afrr_activation_ratio_down_actual,
    }
    day_snapshot = _provider_forecast(
        market="day_ahead",
        decision_time_utc=decision_time,
        delivery_frame=day_da,
        actual_frame=all_day_ahead_actual,
        provider=provider,
        visible_frames=visible_frames,
    )
    capacity_up_snapshot = _provider_forecast(
        market="afrr_capacity_up",
        decision_time_utc=decision_time,
        delivery_frame=day_capacity_up,
        actual_frame=all_afrr_capacity_up_actual,
        provider=provider,
        visible_frames=visible_frames,
    )
    capacity_down_snapshot = _provider_forecast(
        market="afrr_capacity_down",
        decision_time_utc=decision_time,
        delivery_frame=day_capacity_down,
        actual_frame=all_afrr_capacity_down_actual,
        provider=provider,
        visible_frames=visible_frames,
    )
    activation_up_snapshot = _provider_forecast(
        market="afrr_activation_price_up",
        decision_time_utc=decision_time,
        delivery_frame=day_activation_up,
        actual_frame=all_afrr_activation_price_up_actual,
        provider=provider,
        visible_frames=visible_frames,
    )
    activation_down_snapshot = _provider_forecast(
        market="afrr_activation_price_down",
        decision_time_utc=decision_time,
        delivery_frame=day_activation_down,
        actual_frame=all_afrr_activation_price_down_actual,
        provider=provider,
        visible_frames=visible_frames,
    )
    ratio_up_snapshot = _provider_forecast(
        market="afrr_activation_ratio_up",
        decision_time_utc=decision_time,
        delivery_frame=day_ratio_up,
        actual_frame=all_afrr_activation_ratio_up_actual,
        provider=provider,
        visible_frames=visible_frames,
    )
    ratio_down_snapshot = _provider_forecast(
        market="afrr_activation_ratio_down",
        decision_time_utc=decision_time,
        delivery_frame=day_ratio_down,
        actual_frame=all_afrr_activation_ratio_down_actual,
        provider=provider,
        visible_frames=visible_frames,
    )
    if config.run_scope == "portfolio":
        if config.forecast_provider.mode == "scenario_bundle":
            portfolio_solution = solve_portfolio_day_ahead_afrr_dispatch_scenario(
                day_ahead_frame=day_da,
                day_ahead_snapshot=day_snapshot,
                afrr_capacity_up_snapshot=capacity_up_snapshot,
                afrr_capacity_down_snapshot=capacity_down_snapshot,
                afrr_activation_price_up_snapshot=activation_up_snapshot,
                afrr_activation_price_down_snapshot=activation_down_snapshot,
                afrr_activation_ratio_up_snapshot=ratio_up_snapshot,
                afrr_activation_ratio_down_snapshot=ratio_down_snapshot,
                site=config.site,
                assets=config.assets,
                risk=_risk_preference(config),
                degradation_costs_eur_per_mwh=_asset_degradation_costs(config),
                reserve_sustain_duration_minutes=config.afrr.sustain_duration_minutes,
                reserve_penalty_eur_per_mw=float(config.afrr.non_delivery_penalty_eur_per_mw),
                strategy_name=benchmark.benchmark_name,
            )
        else:
            da_input = day_da.copy()
            da_input["price_eur_per_mwh"] = _expected_snapshot(day_snapshot)["forecast_price_eur_per_mwh"].values
            capacity_up_input = day_capacity_up.copy()
            capacity_up_input["price_eur_per_mwh"] = _expected_snapshot(capacity_up_snapshot)[
                "forecast_price_eur_per_mwh"
            ].values
            capacity_down_input = day_capacity_down.copy()
            capacity_down_input["price_eur_per_mwh"] = _expected_snapshot(capacity_down_snapshot)[
                "forecast_price_eur_per_mwh"
            ].values
            activation_up_input = day_activation_up.copy()
            activation_up_input["price_eur_per_mwh"] = _expected_snapshot(activation_up_snapshot)[
                "forecast_price_eur_per_mwh"
            ].values
            activation_down_input = day_activation_down.copy()
            activation_down_input["price_eur_per_mwh"] = _expected_snapshot(activation_down_snapshot)[
                "forecast_price_eur_per_mwh"
            ].values
            ratio_up_input = day_ratio_up.copy()
            ratio_up_input["price_eur_per_mwh"] = _expected_snapshot(ratio_up_snapshot)[
                "forecast_price_eur_per_mwh"
            ].values
            ratio_down_input = day_ratio_down.copy()
            ratio_down_input["price_eur_per_mwh"] = _expected_snapshot(ratio_down_snapshot)[
                "forecast_price_eur_per_mwh"
            ].values
            portfolio_solution = solve_portfolio_day_ahead_afrr_dispatch(
                day_ahead_frame=da_input,
                afrr_capacity_up_frame=capacity_up_input,
                afrr_capacity_down_frame=capacity_down_input,
                afrr_activation_price_up_frame=activation_up_input,
                afrr_activation_price_down_frame=activation_down_input,
                afrr_activation_ratio_up_frame=ratio_up_input,
                afrr_activation_ratio_down_frame=ratio_down_input,
                site=config.site,
                assets=config.assets,
                degradation_costs_eur_per_mwh=_asset_degradation_costs(config),
                reserve_sustain_duration_minutes=config.afrr.sustain_duration_minutes,
                reserve_penalty_eur_per_mw=float(config.afrr.non_delivery_penalty_eur_per_mw),
                strategy_name=benchmark.benchmark_name,
            )
        site_dispatch, asset_dispatch = _decorate_portfolio_outputs(
            site_dispatch=portfolio_solution.site_dispatch,
            asset_dispatch=portfolio_solution.asset_dispatch,
            config=config,
            decision_type="day_ahead_afrr_nomination",
            decision_time_utc=decision_time,
            market_id=adapter.market_id,
        )
        objective_value = portfolio_solution.objective_value_eur
        solver_name = portfolio_solution.solver_name
    else:
        asset = config.primary_asset
        if config.forecast_provider.mode == "scenario_bundle":
            single_solution = solve_day_ahead_afrr_dispatch_scenario(
                day_ahead_frame=day_da,
                day_ahead_snapshot=day_snapshot,
                afrr_capacity_up_snapshot=capacity_up_snapshot,
                afrr_capacity_down_snapshot=capacity_down_snapshot,
                afrr_activation_price_up_snapshot=activation_up_snapshot,
                afrr_activation_price_down_snapshot=activation_down_snapshot,
                afrr_activation_ratio_up_snapshot=ratio_up_snapshot,
                afrr_activation_ratio_down_snapshot=ratio_down_snapshot,
                battery=asset.battery,
                risk=_risk_preference(config),
                degradation_cost_eur_per_mwh=_degradation_cost_per_mwh(config, asset),
                reserve_sustain_duration_minutes=config.afrr.sustain_duration_minutes,
                reserve_penalty_eur_per_mw=float(config.afrr.non_delivery_penalty_eur_per_mw),
                strategy_name=benchmark.benchmark_name,
            )
        else:
            da_input = day_da.copy()
            da_input["price_eur_per_mwh"] = _expected_snapshot(day_snapshot)["forecast_price_eur_per_mwh"].values
            capacity_up_input = day_capacity_up.copy()
            capacity_up_input["price_eur_per_mwh"] = _expected_snapshot(capacity_up_snapshot)[
                "forecast_price_eur_per_mwh"
            ].values
            capacity_down_input = day_capacity_down.copy()
            capacity_down_input["price_eur_per_mwh"] = _expected_snapshot(capacity_down_snapshot)[
                "forecast_price_eur_per_mwh"
            ].values
            activation_up_input = day_activation_up.copy()
            activation_up_input["price_eur_per_mwh"] = _expected_snapshot(activation_up_snapshot)[
                "forecast_price_eur_per_mwh"
            ].values
            activation_down_input = day_activation_down.copy()
            activation_down_input["price_eur_per_mwh"] = _expected_snapshot(activation_down_snapshot)[
                "forecast_price_eur_per_mwh"
            ].values
            ratio_up_input = day_ratio_up.copy()
            ratio_up_input["price_eur_per_mwh"] = _expected_snapshot(ratio_up_snapshot)[
                "forecast_price_eur_per_mwh"
            ].values
            ratio_down_input = day_ratio_down.copy()
            ratio_down_input["price_eur_per_mwh"] = _expected_snapshot(ratio_down_snapshot)[
                "forecast_price_eur_per_mwh"
            ].values
            single_solution = solve_day_ahead_afrr_dispatch(
                day_ahead_frame=da_input,
                afrr_capacity_up_frame=capacity_up_input,
                afrr_capacity_down_frame=capacity_down_input,
                afrr_activation_price_up_frame=activation_up_input,
                afrr_activation_price_down_frame=activation_down_input,
                afrr_activation_ratio_up_frame=ratio_up_input,
                afrr_activation_ratio_down_frame=ratio_down_input,
                battery=asset.battery,
                degradation_cost_eur_per_mwh=_degradation_cost_per_mwh(config, asset),
                reserve_sustain_duration_minutes=config.afrr.sustain_duration_minutes,
                reserve_penalty_eur_per_mw=float(config.afrr.non_delivery_penalty_eur_per_mw),
                strategy_name=benchmark.benchmark_name,
            )
        site_dispatch, asset_dispatch = _single_asset_to_frames(single_solution.dispatch.copy(), config=config)
        site_dispatch["market_id"] = adapter.market_id
        site_dispatch["workflow_family"] = config.workflow
        site_dispatch["decision_type"] = "day_ahead_afrr_nomination"
        site_dispatch["decision_time_utc"] = decision_time
        asset_dispatch["market_id"] = adapter.market_id
        asset_dispatch["workflow_family"] = config.workflow
        asset_dispatch["decision_type"] = "day_ahead_afrr_nomination"
        asset_dispatch["decision_time_utc"] = decision_time
        objective_value = single_solution.objective_value_eur
        solver_name = single_solution.solver_name

    site_dispatch = _merge_forecast_snapshot(
        site_dispatch, day_snapshot, target_column="day_ahead_forecast_price_eur_per_mwh"
    )
    site_dispatch = _merge_market_price(site_dispatch, day_da, target_column="day_ahead_actual_price_eur_per_mwh")
    site_dispatch = _attach_optional_imbalance_columns(site_dispatch, None)
    site_dispatch = _attach_zero_fcr_columns(site_dispatch)
    site_dispatch = _merge_afrr_snapshot_columns(
        site_dispatch,
        capacity_up=capacity_up_snapshot,
        capacity_down=capacity_down_snapshot,
        activation_price_up=activation_up_snapshot,
        activation_price_down=activation_down_snapshot,
        activation_ratio_up=ratio_up_snapshot,
        activation_ratio_down=ratio_down_snapshot,
    )
    site_dispatch = _merge_afrr_actual_columns(
        site_dispatch,
        capacity_up=day_capacity_up,
        capacity_down=day_capacity_down,
        activation_price_up=day_activation_up,
        activation_price_down=day_activation_down,
        activation_ratio_up=day_ratio_up,
        activation_ratio_down=day_ratio_down,
    )

    asset_dispatch = _merge_forecast_snapshot(
        asset_dispatch, day_snapshot, target_column="day_ahead_forecast_price_eur_per_mwh"
    )
    asset_dispatch = _merge_market_price(asset_dispatch, day_da, target_column="day_ahead_actual_price_eur_per_mwh")
    asset_dispatch = _attach_optional_imbalance_columns(asset_dispatch, None)
    asset_dispatch = _attach_zero_fcr_columns(asset_dispatch)
    asset_dispatch = _merge_afrr_snapshot_columns(
        asset_dispatch,
        capacity_up=capacity_up_snapshot,
        capacity_down=capacity_down_snapshot,
        activation_price_up=activation_up_snapshot,
        activation_price_down=activation_down_snapshot,
        activation_ratio_up=ratio_up_snapshot,
        activation_ratio_down=ratio_down_snapshot,
    )
    asset_dispatch = _merge_afrr_actual_columns(
        asset_dispatch,
        capacity_up=day_capacity_up,
        capacity_down=day_capacity_down,
        activation_price_up=day_activation_up,
        activation_price_down=day_activation_down,
        activation_ratio_up=day_ratio_up,
        activation_ratio_down=day_ratio_down,
    )
    asset_dispatch = _ensure_dispatch_columns(asset_dispatch, site_id=config.site.id, run_scope=config.run_scope)
    asset_dispatch = pd.concat(
        [
            assign_reason_codes(
                asset_dispatch[asset_dispatch["asset_id"] == asset.id].copy(),
                asset.battery,
                overlay=False,
            )
            for asset in config.assets
        ],
        ignore_index=True,
    )
    site_dispatch = _ensure_dispatch_columns(site_dispatch, site_id=config.site.id, run_scope=config.run_scope)
    site_dispatch = assign_site_reason_codes(site_dispatch, config.site)

    decision_id = f"{delivery_date.date()}-afrr-0"
    decisions = [
        {
            "decision_id": decision_id,
            "market_id": adapter.market_id,
            "workflow_family": config.workflow,
            "run_scope": config.run_scope,
            "site_id": config.site.id,
            "decision_time_utc": decision_time,
            "decision_time_local": decision_time.tz_convert(adapter.timezone),
            "decision_type": "day_ahead_afrr_nomination",
            "delivery_date_local": str(delivery_date.date()),
            "horizon_start_utc": day_da["timestamp_utc"].iloc[0],
            "horizon_end_utc": day_da["timestamp_utc"].iloc[-1]
            + pd.Timedelta(minutes=config.timing.resolution_minutes),
            "locked_intervals": len(day_da),
            "provider_name": provider.name,
            "benchmark_name": benchmark.benchmark_name,
            "objective_value_eur": objective_value,
            "solver_name": solver_name,
            "schedule_version": "baseline",
        }
    ]
    snapshots = [
        _record_snapshot(
            snapshot,
            decision_id=decision_id,
            decision_time_utc=decision_time,
            decision_type="day_ahead_afrr_nomination",
            schedule_version="baseline",
            benchmark_name=benchmark.benchmark_name,
            market_id=adapter.market_id,
            workflow_family=config.workflow,
            run_scope=config.run_scope,
            site_id=config.site.id,
        )
        for snapshot in (
            day_snapshot,
            capacity_up_snapshot,
            capacity_down_snapshot,
            activation_up_snapshot,
            activation_down_snapshot,
            ratio_up_snapshot,
            ratio_down_snapshot,
        )
    ]
    return DailyArtifacts(
        site_dispatch=site_dispatch, asset_dispatch=asset_dispatch, decisions=decisions, snapshots=snapshots
    )


def _single_asset_imbalance_daily(
    *,
    config: BacktestConfig,
    adapter,
    benchmark: BenchmarkDefinition,
    provider,
    delivery_date: pd.Timestamp,
    day_ahead_actual: pd.DataFrame,
    imbalance_actual: pd.DataFrame,
    all_day_ahead_actual: pd.DataFrame,
    all_imbalance_actual: pd.DataFrame,
    schedule: pd.DataFrame,
) -> DailyArtifacts:
    asset = config.primary_asset
    day_da = _day_frame(day_ahead_actual, delivery_date)
    day_imb = _day_frame(imbalance_actual, delivery_date)
    schedule_row = schedule[schedule["delivery_date_local"] == str(delivery_date.date())]
    if schedule_row.empty:
        raise ValueError(f"No decision schedule available for delivery_date={delivery_date.date()}")
    day_decision_time = pd.Timestamp(schedule_row.iloc[0]["day_ahead_gate_closure_utc"])
    baseline_visible_frames = {
        "day_ahead": all_day_ahead_actual,
        "imbalance": all_imbalance_actual,
    }
    day_snapshot = _provider_forecast(
        market="day_ahead",
        decision_time_utc=day_decision_time,
        delivery_frame=day_da,
        actual_frame=all_day_ahead_actual,
        provider=provider,
        visible_frames=baseline_visible_frames,
    )
    baseline_input = day_da.copy()
    baseline_input["price_eur_per_mwh"] = day_snapshot["forecast_price_eur_per_mwh"].values
    baseline = solve_day_ahead_dispatch(
        baseline_input,
        asset.battery,
        degradation_cost_eur_per_mwh=_degradation_cost_per_mwh(config, asset),
        initial_soc_mwh=asset.battery.initial_soc_mwh,
        terminal_soc_mwh=asset.battery.terminal_soc_mwh,
        strategy_name=benchmark.benchmark_name,
    )
    baseline_dispatch = baseline.dispatch.reset_index(drop=True).copy()

    decision_counter = 0
    decisions: list[dict[str, object]] = [
        {
            "decision_id": f"{delivery_date.date()}-da-{decision_counter}",
            "market_id": adapter.market_id,
            "workflow_family": config.workflow,
            "run_scope": config.run_scope,
            "site_id": config.site.id,
            "decision_time_utc": day_decision_time,
            "decision_time_local": day_decision_time.tz_convert(adapter.timezone),
            "decision_type": "day_ahead_nomination",
            "delivery_date_local": str(delivery_date.date()),
            "horizon_start_utc": day_da["timestamp_utc"].iloc[0],
            "horizon_end_utc": day_da["timestamp_utc"].iloc[-1]
            + pd.Timedelta(minutes=config.timing.resolution_minutes),
            "locked_intervals": len(day_da),
            "provider_name": provider.name,
            "benchmark_name": benchmark.benchmark_name,
            "objective_value_eur": baseline.objective_value_eur,
            "solver_name": baseline.solver_name,
            "schedule_version": "baseline",
            "revision_index": 0,
        }
    ]
    snapshots = [
        _record_snapshot(
            day_snapshot,
            decision_id=str(decisions[0]["decision_id"]),
            decision_time_utc=day_decision_time,
            decision_type="day_ahead_nomination",
            schedule_version="baseline",
            benchmark_name=benchmark.benchmark_name,
            market_id=adapter.market_id,
            workflow_family=config.workflow,
            run_scope=config.run_scope,
            site_id=config.site.id,
        )
    ]

    rebalance_step = max(
        config.timing.rebalance_cadence_minutes // config.timing.resolution_minutes,
        config.timing.execution_lock_intervals,
    )
    executed_rows: list[pd.Series] = []
    current_soc = asset.battery.initial_soc_mwh
    current_plan: pd.DataFrame | None = None
    current_plan_origin = 0
    next_reopt_index = 0

    for idx in range(len(day_da)):
        if current_plan is None or idx >= next_reopt_index:
            decision_counter += 1
            remaining_da = day_da.iloc[idx:].reset_index(drop=True).copy()
            remaining_imb = day_imb.iloc[idx:].reset_index(drop=True).copy()
            decision_time = pd.Timestamp(day_da.iloc[idx]["timestamp_utc"])
            visible_frames = {
                "day_ahead": all_day_ahead_actual,
                "imbalance": all_imbalance_actual,
            }
            imbalance_snapshot = _provider_forecast(
                market="imbalance",
                decision_time_utc=decision_time,
                delivery_frame=remaining_imb,
                actual_frame=all_imbalance_actual,
                provider=provider,
                visible_frames=visible_frames,
            )
            overlay_input = remaining_imb.copy()
            overlay_input["price_eur_per_mwh"] = imbalance_snapshot["forecast_price_eur_per_mwh"].values
            overlay = solve_imbalance_overlay_dispatch(
                day_ahead_frame=remaining_da,
                imbalance_frame=overlay_input,
                battery=asset.battery,
                baseline_dispatch=baseline_dispatch.iloc[idx:].reset_index(drop=True),
                degradation_cost_eur_per_mwh=_degradation_cost_per_mwh(config, asset),
                initial_soc_mwh=current_soc,
                terminal_soc_mwh=asset.battery.terminal_soc_mwh,
                strategy_name=benchmark.benchmark_name,
            )
            current_plan = overlay.dispatch.reset_index(drop=True).copy()
            current_plan["imbalance_forecast_price_eur_per_mwh"] = imbalance_snapshot[
                "forecast_price_eur_per_mwh"
            ].values
            current_plan["day_ahead_forecast_price_eur_per_mwh"] = baseline_dispatch.iloc[idx:][
                "price_eur_per_mwh"
            ].reset_index(drop=True)
            current_plan["fcr_capacity_price_forecast_eur_per_mw_per_h"] = 0.0
            current_plan = _merge_market_price(
                current_plan, remaining_da, target_column="day_ahead_actual_price_eur_per_mwh"
            )
            current_plan = _attach_optional_imbalance_columns(current_plan, remaining_imb)
            current_plan["decision_time_utc"] = decision_time
            current_plan["decision_type"] = "imbalance_rebalance"
            current_plan["market_id"] = adapter.market_id
            current_plan["workflow_family"] = config.workflow
            current_plan_origin = idx
            next_reopt_index = idx + rebalance_step
            decision_id = f"{delivery_date.date()}-imb-{decision_counter}"
            decisions.append(
                {
                    "decision_id": decision_id,
                    "market_id": adapter.market_id,
                    "workflow_family": config.workflow,
                    "run_scope": config.run_scope,
                    "site_id": config.site.id,
                    "decision_time_utc": decision_time,
                    "decision_time_local": decision_time.tz_convert(adapter.timezone),
                    "decision_type": "imbalance_rebalance",
                    "delivery_date_local": str(delivery_date.date()),
                    "horizon_start_utc": remaining_da["timestamp_utc"].iloc[0],
                    "horizon_end_utc": remaining_da["timestamp_utc"].iloc[-1]
                    + pd.Timedelta(minutes=config.timing.resolution_minutes),
                    "locked_intervals": config.timing.execution_lock_intervals,
                    "provider_name": provider.name,
                    "benchmark_name": benchmark.benchmark_name,
                    "objective_value_eur": overlay.objective_value_eur,
                    "solver_name": overlay.solver_name,
                    "schedule_version": "baseline",
                    "revision_index": decision_counter,
                }
            )
            snapshots.append(
                _record_snapshot(
                    imbalance_snapshot,
                    decision_id=decision_id,
                    decision_time_utc=decision_time,
                    decision_type="imbalance_rebalance",
                    schedule_version="baseline",
                    benchmark_name=benchmark.benchmark_name,
                    market_id=adapter.market_id,
                    workflow_family=config.workflow,
                    run_scope=config.run_scope,
                    site_id=config.site.id,
                )
            )

        relative_idx = idx - current_plan_origin
        executed_row = current_plan.iloc[relative_idx].copy()
        current_soc = float(executed_row["soc_mwh"])
        executed_rows.append(executed_row)

    dispatch = pd.DataFrame(executed_rows).reset_index(drop=True)
    dispatch["baseline_net_export_mw"] = baseline_dispatch["net_export_mw"].values
    dispatch["imbalance_mw"] = dispatch["net_export_mw"] - dispatch["baseline_net_export_mw"]
    dispatch = _ensure_dispatch_columns(dispatch, site_id=config.site.id, run_scope=config.run_scope)
    site_dispatch, asset_dispatch = _single_asset_to_frames(dispatch, config=config)
    site_dispatch = assign_reason_codes(site_dispatch, asset.battery, overlay=True)
    asset_dispatch = assign_reason_codes(asset_dispatch, asset.battery, overlay=True)
    return DailyArtifacts(
        site_dispatch=site_dispatch, asset_dispatch=asset_dispatch, decisions=decisions, snapshots=snapshots
    )


def _revision_start_indices(
    *,
    day_frame: pd.DataFrame,
    delivery_date: pd.Timestamp,
    config: BacktestConfig,
    adapter,
) -> list[tuple[pd.Timestamp, int]]:
    if config.revision is None:
        return []
    checkpoints: list[tuple[pd.Timestamp, int]] = []
    for checkpoint_local in config.revision.revision_checkpoints_local:
        decision_time_local = pd.Timestamp(f"{delivery_date.date()} {checkpoint_local}", tz=adapter.timezone)
        decision_time_utc = decision_time_local.tz_convert("UTC")
        locked_candidates = day_frame.index[day_frame["timestamp_utc"] >= decision_time_utc]
        if len(locked_candidates) == 0:
            continue
        first_locked = int(locked_candidates[0])
        start_idx = min(first_locked + config.timing.execution_lock_intervals, len(day_frame))
        if start_idx >= len(day_frame):
            continue
        checkpoints.append((decision_time_utc, start_idx))
    return checkpoints


def _replace_site_plan_slice(current_plan: pd.DataFrame, updated_slice: pd.DataFrame) -> pd.DataFrame:
    result = current_plan.copy()
    incoming = updated_slice.copy()
    key = "timestamp_utc"
    for column in incoming.columns:
        if column not in result.columns:
            result[column] = pd.NA
    for column in result.columns:
        if column not in incoming.columns:
            incoming[column] = result.set_index(key).loc[incoming[key], column].values
    incoming = incoming[result.columns]
    result = result.set_index(key)
    incoming = incoming.set_index(key)
    result.loc[incoming.index, incoming.columns] = incoming
    return result.reset_index()


def _replace_asset_plan_slice(current_plan: pd.DataFrame, updated_slice: pd.DataFrame) -> pd.DataFrame:
    result = current_plan.copy()
    incoming = updated_slice.copy()
    key_columns = ["timestamp_utc", "asset_id"]
    for column in incoming.columns:
        if column not in result.columns:
            result[column] = pd.NA
    indexed_result = result.set_index(key_columns)
    indexed_incoming = incoming.set_index(key_columns)
    for column in indexed_result.columns:
        if column not in indexed_incoming.columns:
            indexed_incoming[column] = indexed_result.loc[indexed_incoming.index, column].values
    indexed_incoming = indexed_incoming[indexed_result.columns]
    indexed_result.loc[indexed_incoming.index, indexed_incoming.columns] = indexed_incoming
    return indexed_result.reset_index()


def _shift_future_site_soc(plan: pd.DataFrame, *, after_timestamp_utc: pd.Timestamp, delta_mwh: float) -> pd.DataFrame:
    if abs(delta_mwh) < 1e-9:
        return plan
    result = plan.copy()
    mask = pd.to_datetime(result["timestamp_utc"], utc=True) > after_timestamp_utc
    for column in ("soc_start_mwh", "soc_mwh"):
        if column in result.columns:
            result.loc[mask, column] = result.loc[mask, column].astype(float) + delta_mwh
    return result


def _shift_future_asset_soc(
    plan: pd.DataFrame, *, after_timestamp_utc: pd.Timestamp, delta_mwh_by_asset: dict[str, float]
) -> pd.DataFrame:
    if not delta_mwh_by_asset:
        return plan
    result = plan.copy()
    timestamps = pd.to_datetime(result["timestamp_utc"], utc=True)
    for asset_id, delta_mwh in delta_mwh_by_asset.items():
        if abs(delta_mwh) < 1e-9:
            continue
        mask = (timestamps > after_timestamp_utc) & (result["asset_id"] == asset_id)
        for column in ("soc_start_mwh", "soc_mwh"):
            if column in result.columns:
                result.loc[mask, column] = result.loc[mask, column].astype(float) + delta_mwh
    return result


def _single_asset_soc_targets(
    plan: pd.DataFrame, asset: AssetSpec, *, start_idx: int, end_idx: int
) -> tuple[float, float | None]:
    initial_soc = asset.battery.initial_soc_mwh if start_idx == 0 else float(plan.iloc[start_idx - 1]["soc_mwh"])
    terminal_soc = asset.battery.terminal_soc_mwh
    if end_idx < len(plan):
        terminal_soc = float(plan.iloc[end_idx - 1]["soc_mwh"])
    return initial_soc, terminal_soc


def _portfolio_soc_targets(
    plan: pd.DataFrame,
    assets: list[AssetSpec],
    *,
    start_idx: int,
    end_idx: int,
) -> tuple[dict[str, float], dict[str, float | None]]:
    initial: dict[str, float] = {}
    terminal: dict[str, float | None] = {}
    ordered = plan.sort_values(["asset_id", "timestamp_utc"]).reset_index(drop=True)
    for asset in assets:
        asset_plan = ordered[ordered["asset_id"] == asset.id].reset_index(drop=True)
        initial[asset.id] = (
            asset.battery.initial_soc_mwh if start_idx == 0 else float(asset_plan.iloc[start_idx - 1]["soc_mwh"])
        )
        terminal[asset.id] = asset.battery.terminal_soc_mwh
        if end_idx < len(asset_plan):
            terminal[asset.id] = float(asset_plan.iloc[end_idx - 1]["soc_mwh"])
    return initial, terminal


def _baseline_day_ahead_single_asset(
    *,
    config: BacktestConfig,
    adapter,
    benchmark: BenchmarkDefinition,
    provider,
    delivery_date: pd.Timestamp,
    day_ahead_actual: pd.DataFrame,
    all_day_ahead_actual: pd.DataFrame,
    schedule: pd.DataFrame,
) -> DailyArtifacts:
    asset = config.primary_asset
    day_da = _day_frame(day_ahead_actual, delivery_date)
    schedule_row = schedule[schedule["delivery_date_local"] == str(delivery_date.date())]
    if schedule_row.empty:
        raise ValueError(f"No decision schedule available for delivery_date={delivery_date.date()}")
    decision_time = pd.Timestamp(schedule_row.iloc[0]["day_ahead_gate_closure_utc"])
    visible_frames = {"day_ahead": all_day_ahead_actual}
    day_snapshot = _provider_forecast(
        market="day_ahead",
        decision_time_utc=decision_time,
        delivery_frame=day_da,
        actual_frame=all_day_ahead_actual,
        provider=provider,
        visible_frames=visible_frames,
    )
    baseline_input = day_da.copy()
    baseline_input["price_eur_per_mwh"] = day_snapshot["forecast_price_eur_per_mwh"].values
    solution = solve_day_ahead_dispatch(
        baseline_input,
        asset.battery,
        degradation_cost_eur_per_mwh=_degradation_cost_per_mwh(config, asset),
        initial_soc_mwh=asset.battery.initial_soc_mwh,
        terminal_soc_mwh=asset.battery.terminal_soc_mwh,
        strategy_name=benchmark.benchmark_name,
    )
    site_dispatch, asset_dispatch = _single_asset_to_frames(solution.dispatch.copy(), config=config)
    site_dispatch["market_id"] = adapter.market_id
    site_dispatch["workflow_family"] = config.execution_workflow
    site_dispatch["decision_type"] = "day_ahead_nomination"
    site_dispatch["decision_time_utc"] = decision_time
    site_dispatch = _merge_forecast_snapshot(
        site_dispatch, day_snapshot, target_column="day_ahead_forecast_price_eur_per_mwh"
    )
    site_dispatch = _merge_market_price(site_dispatch, day_da, target_column="day_ahead_actual_price_eur_per_mwh")
    site_dispatch = _attach_optional_imbalance_columns(site_dispatch, None)
    site_dispatch["fcr_capacity_price_forecast_eur_per_mw_per_h"] = 0.0
    site_dispatch["fcr_capacity_price_actual_eur_per_mw_per_h"] = 0.0
    site_dispatch = _ensure_dispatch_columns(site_dispatch, site_id=config.site.id, run_scope=config.run_scope)
    site_dispatch = assign_reason_codes(site_dispatch, asset.battery, overlay=False)

    asset_dispatch["market_id"] = adapter.market_id
    asset_dispatch["workflow_family"] = config.execution_workflow
    asset_dispatch["decision_type"] = "day_ahead_nomination"
    asset_dispatch["decision_time_utc"] = decision_time
    asset_dispatch = _merge_forecast_snapshot(
        asset_dispatch, day_snapshot, target_column="day_ahead_forecast_price_eur_per_mwh"
    )
    asset_dispatch = _merge_market_price(asset_dispatch, day_da, target_column="day_ahead_actual_price_eur_per_mwh")
    asset_dispatch = _attach_optional_imbalance_columns(asset_dispatch, None)
    asset_dispatch["fcr_capacity_price_forecast_eur_per_mw_per_h"] = 0.0
    asset_dispatch["fcr_capacity_price_actual_eur_per_mw_per_h"] = 0.0
    asset_dispatch = _ensure_dispatch_columns(asset_dispatch, site_id=config.site.id, run_scope=config.run_scope)
    asset_dispatch = assign_reason_codes(asset_dispatch, asset.battery, overlay=False)

    decision_id = f"{delivery_date.date()}-baseline-da-0"
    decisions = [
        {
            "decision_id": decision_id,
            "market_id": adapter.market_id,
            "workflow_family": config.execution_workflow,
            "run_scope": config.run_scope,
            "site_id": config.site.id,
            "decision_time_utc": decision_time,
            "decision_time_local": decision_time.tz_convert(adapter.timezone),
            "decision_type": "day_ahead_nomination",
            "delivery_date_local": str(delivery_date.date()),
            "horizon_start_utc": day_da["timestamp_utc"].iloc[0],
            "horizon_end_utc": day_da["timestamp_utc"].iloc[-1]
            + pd.Timedelta(minutes=config.timing.resolution_minutes),
            "locked_intervals": len(day_da),
            "provider_name": provider.name,
            "benchmark_name": benchmark.benchmark_name,
            "objective_value_eur": solution.objective_value_eur,
            "solver_name": solution.solver_name,
            "schedule_version": "baseline",
            "revision_index": 0,
        }
    ]
    snapshots = [
        _record_snapshot(
            day_snapshot,
            decision_id=decision_id,
            decision_time_utc=decision_time,
            decision_type="day_ahead_nomination",
            schedule_version="baseline",
            benchmark_name=benchmark.benchmark_name,
            market_id=adapter.market_id,
            workflow_family=config.execution_workflow,
            run_scope=config.run_scope,
            site_id=config.site.id,
        )
    ]
    return DailyArtifacts(
        site_dispatch=site_dispatch, asset_dispatch=asset_dispatch, decisions=decisions, snapshots=snapshots
    )


def _schedule_revision_daily(
    *,
    config: BacktestConfig,
    adapter,
    benchmark: BenchmarkDefinition,
    provider,
    delivery_date: pd.Timestamp,
    day_ahead_actual: pd.DataFrame,
    imbalance_actual: pd.DataFrame | None,
    fcr_actual: pd.DataFrame | None,
    afrr_capacity_up_actual: pd.DataFrame | None,
    afrr_capacity_down_actual: pd.DataFrame | None,
    afrr_activation_price_up_actual: pd.DataFrame | None,
    afrr_activation_price_down_actual: pd.DataFrame | None,
    afrr_activation_ratio_up_actual: pd.DataFrame | None,
    afrr_activation_ratio_down_actual: pd.DataFrame | None,
    all_day_ahead_actual: pd.DataFrame,
    all_imbalance_actual: pd.DataFrame | None,
    all_fcr_actual: pd.DataFrame | None,
    all_afrr_capacity_up_actual: pd.DataFrame | None,
    all_afrr_capacity_down_actual: pd.DataFrame | None,
    all_afrr_activation_price_up_actual: pd.DataFrame | None,
    all_afrr_activation_price_down_actual: pd.DataFrame | None,
    all_afrr_activation_ratio_up_actual: pd.DataFrame | None,
    all_afrr_activation_ratio_down_actual: pd.DataFrame | None,
    schedule: pd.DataFrame,
) -> DailyArtifacts:
    if config.revision is None:
        raise ValueError("schedule_revision requires a revision block")
    execution_config = _execution_config(config)
    baseline_benchmark = BenchmarkRegistry.resolve(
        config.market.id,
        config.execution_workflow,
        provider.name,
        run_scope=config.run_scope,
        benchmark_suffix="baseline",
    )
    if config.execution_workflow == "da_plus_imbalance":
        baseline_daily = _baseline_day_ahead_single_asset(
            config=execution_config,
            adapter=adapter,
            benchmark=baseline_benchmark,
            provider=provider,
            delivery_date=delivery_date,
            day_ahead_actual=day_ahead_actual,
            all_day_ahead_actual=all_day_ahead_actual,
            schedule=schedule,
        )
    else:
        baseline_daily = _run_daily_walk_forward(
            config=execution_config,
            adapter=adapter,
            benchmark=baseline_benchmark,
            provider=provider,
            delivery_date=delivery_date,
            day_ahead_actual=day_ahead_actual,
            imbalance_actual=imbalance_actual,
            fcr_actual=fcr_actual,
            afrr_capacity_up_actual=afrr_capacity_up_actual,
            afrr_capacity_down_actual=afrr_capacity_down_actual,
            afrr_activation_price_up_actual=afrr_activation_price_up_actual,
            afrr_activation_price_down_actual=afrr_activation_price_down_actual,
            afrr_activation_ratio_up_actual=afrr_activation_ratio_up_actual,
            afrr_activation_ratio_down_actual=afrr_activation_ratio_down_actual,
            all_day_ahead_actual=all_day_ahead_actual,
            all_imbalance_actual=all_imbalance_actual,
            all_fcr_actual=all_fcr_actual,
            all_afrr_capacity_up_actual=all_afrr_capacity_up_actual,
            all_afrr_capacity_down_actual=all_afrr_capacity_down_actual,
            all_afrr_activation_price_up_actual=all_afrr_activation_price_up_actual,
            all_afrr_activation_price_down_actual=all_afrr_activation_price_down_actual,
            all_afrr_activation_ratio_up_actual=all_afrr_activation_ratio_up_actual,
            all_afrr_activation_ratio_down_actual=all_afrr_activation_ratio_down_actual,
            schedule=schedule,
        )

    baseline_site = _annotate_schedule_frame(
        baseline_daily.site_dispatch,
        schedule_version="baseline",
        schedule_state="baseline_committed",
        lock_state="committed",
    )
    baseline_asset = _annotate_schedule_frame(
        baseline_daily.asset_dispatch,
        schedule_version="baseline",
        schedule_state="baseline_committed",
        lock_state="committed",
    )
    current_site_plan = baseline_site.copy()
    current_asset_plan = baseline_asset.copy()
    lineage_frames = [
        _lineage_frame(baseline_site, entity_type="site"),
        _lineage_frame(baseline_asset, entity_type="asset"),
    ]
    decisions = [dict(row) for row in baseline_daily.decisions]
    snapshots = list(baseline_daily.snapshots)
    revision_counter = 0

    day_da = _day_frame(day_ahead_actual, delivery_date)
    day_imb = _day_frame(imbalance_actual, delivery_date) if imbalance_actual is not None else None
    day_fcr = _day_frame(fcr_actual, delivery_date) if fcr_actual is not None else None
    day_afrr_capacity_up = (
        _day_frame(afrr_capacity_up_actual, delivery_date) if afrr_capacity_up_actual is not None else None
    )
    day_afrr_capacity_down = (
        _day_frame(afrr_capacity_down_actual, delivery_date) if afrr_capacity_down_actual is not None else None
    )
    day_afrr_activation_up = (
        _day_frame(afrr_activation_price_up_actual, delivery_date)
        if afrr_activation_price_up_actual is not None
        else None
    )
    day_afrr_activation_down = (
        _day_frame(afrr_activation_price_down_actual, delivery_date)
        if afrr_activation_price_down_actual is not None
        else None
    )
    day_afrr_ratio_up = (
        _day_frame(afrr_activation_ratio_up_actual, delivery_date)
        if afrr_activation_ratio_up_actual is not None
        else None
    )
    day_afrr_ratio_down = (
        _day_frame(afrr_activation_ratio_down_actual, delivery_date)
        if afrr_activation_ratio_down_actual is not None
        else None
    )
    checkpoints = _revision_start_indices(day_frame=day_da, delivery_date=delivery_date, config=config, adapter=adapter)
    for decision_time, start_idx in checkpoints:
        revision_counter += 1
        end_idx = min(start_idx + config.revision.max_revision_horizon_intervals, len(day_da))
        if start_idx >= end_idx:
            continue
        revision_version = f"revision_{revision_counter:02d}"
        decision_id = f"{delivery_date.date()}-{revision_version}"
        future_site_soc_delta_mwh = 0.0
        future_asset_soc_delta_mwh: dict[str, float] = {}
        revision_boundary_timestamp_utc = day_da.iloc[end_idx - 1]["timestamp_utc"]

        if config.execution_workflow == "da_plus_imbalance":
            if day_imb is None or all_imbalance_actual is None:
                raise ValueError("schedule_revision da_plus_imbalance requires realized imbalance data")
            asset = execution_config.primary_asset
            initial_soc, terminal_soc = _single_asset_soc_targets(
                current_site_plan, asset, start_idx=start_idx, end_idx=end_idx
            )
            remaining_da = day_da.iloc[start_idx:end_idx].reset_index(drop=True).copy()
            remaining_imb = day_imb.iloc[start_idx:end_idx].reset_index(drop=True).copy()
            visible_frames = {
                "day_ahead": all_day_ahead_actual,
                "imbalance": all_imbalance_actual,
            }
            imbalance_snapshot = _provider_forecast(
                market="imbalance",
                decision_time_utc=decision_time,
                delivery_frame=remaining_imb,
                actual_frame=all_imbalance_actual,
                provider=provider,
                visible_frames=visible_frames,
            )
            overlay_input = remaining_imb.copy()
            overlay_input["price_eur_per_mwh"] = imbalance_snapshot["forecast_price_eur_per_mwh"].values
            baseline_slice = baseline_site.iloc[start_idx:end_idx].reset_index(drop=True).copy()
            overlay = solve_imbalance_overlay_dispatch(
                day_ahead_frame=remaining_da,
                imbalance_frame=overlay_input,
                battery=asset.battery,
                baseline_dispatch=baseline_slice,
                degradation_cost_eur_per_mwh=_degradation_cost_per_mwh(execution_config, asset),
                initial_soc_mwh=initial_soc,
                terminal_soc_mwh=terminal_soc,
                strategy_name=benchmark.benchmark_name,
            )
            revised_site, revised_asset = _single_asset_to_frames(overlay.dispatch.copy(), config=execution_config)
            revised_site["market_id"] = adapter.market_id
            revised_site["workflow_family"] = execution_config.workflow
            revised_site["decision_type"] = "schedule_revision"
            revised_site["decision_time_utc"] = decision_time
            revised_site["baseline_net_export_mw"] = baseline_slice["net_export_mw"].values
            revised_site["imbalance_mw"] = revised_site["net_export_mw"] - revised_site["baseline_net_export_mw"]
            revised_site = _merge_forecast_snapshot(
                revised_site,
                snapshots[0][["delivery_start_utc", "forecast_price_eur_per_mwh"]],
                target_column="day_ahead_forecast_price_eur_per_mwh",
            )
            revised_site = _merge_forecast_snapshot(
                revised_site,
                imbalance_snapshot,
                target_column="imbalance_forecast_price_eur_per_mwh",
            )
            revised_site = _merge_market_price(
                revised_site, remaining_da, target_column="day_ahead_actual_price_eur_per_mwh"
            )
            revised_site = _attach_optional_imbalance_columns(revised_site, remaining_imb)
            revised_site["fcr_capacity_price_forecast_eur_per_mw_per_h"] = 0.0
            revised_site["fcr_capacity_price_actual_eur_per_mw_per_h"] = 0.0
            revised_site = _ensure_dispatch_columns(revised_site, site_id=config.site.id, run_scope=config.run_scope)
            revised_site = assign_reason_codes(revised_site, asset.battery, overlay=True)

            revised_asset["market_id"] = adapter.market_id
            revised_asset["workflow_family"] = execution_config.workflow
            revised_asset["decision_type"] = "schedule_revision"
            revised_asset["decision_time_utc"] = decision_time
            revised_asset["baseline_net_export_mw"] = baseline_slice["net_export_mw"].values
            revised_asset["imbalance_mw"] = revised_asset["net_export_mw"] - revised_asset["baseline_net_export_mw"]
            revised_asset = _merge_forecast_snapshot(
                revised_asset,
                snapshots[0][["delivery_start_utc", "forecast_price_eur_per_mwh"]],
                target_column="day_ahead_forecast_price_eur_per_mwh",
            )
            revised_asset = _merge_forecast_snapshot(
                revised_asset,
                imbalance_snapshot,
                target_column="imbalance_forecast_price_eur_per_mwh",
            )
            revised_asset = _merge_market_price(
                revised_asset, remaining_da, target_column="day_ahead_actual_price_eur_per_mwh"
            )
            revised_asset = _attach_optional_imbalance_columns(revised_asset, remaining_imb)
            revised_asset["fcr_capacity_price_forecast_eur_per_mw_per_h"] = 0.0
            revised_asset["fcr_capacity_price_actual_eur_per_mw_per_h"] = 0.0
            revised_asset = _ensure_dispatch_columns(revised_asset, site_id=config.site.id, run_scope=config.run_scope)
            revised_asset = assign_reason_codes(revised_asset, asset.battery, overlay=True)
            objective_value = overlay.objective_value_eur
            solver_name = overlay.solver_name
            snapshots.append(
                _record_snapshot(
                    imbalance_snapshot,
                    decision_id=decision_id,
                    decision_time_utc=decision_time,
                    decision_type="schedule_revision",
                    schedule_version=revision_version,
                    benchmark_name=benchmark.benchmark_name,
                    market_id=adapter.market_id,
                    workflow_family=config.execution_workflow,
                    run_scope=config.run_scope,
                    site_id=config.site.id,
                )
            )
        elif config.execution_workflow == "da_plus_fcr":
            if execution_config.fcr is None or day_fcr is None:
                raise ValueError("schedule_revision da_plus_fcr requires FCR inputs")
            remaining_da = day_da.iloc[start_idx:end_idx].reset_index(drop=True).copy()
            if config.run_scope == "portfolio":
                initial_by_asset, terminal_by_asset = _portfolio_soc_targets(
                    current_asset_plan, execution_config.assets, start_idx=start_idx, end_idx=end_idx
                )
                remaining_fcr = day_fcr.iloc[start_idx:end_idx].reset_index(drop=True).copy()
                remaining_fcr["price_eur_per_mwh"] = current_site_plan.iloc[start_idx:end_idx][
                    "fcr_capacity_price_forecast_eur_per_mw_per_h"
                ].values
                visible_frames = {
                    "day_ahead": all_day_ahead_actual,
                    "fcr_capacity": day_fcr,
                }
                day_snapshot = _provider_forecast(
                    market="day_ahead",
                    decision_time_utc=decision_time,
                    delivery_frame=remaining_da,
                    actual_frame=all_day_ahead_actual,
                    provider=provider,
                    visible_frames=visible_frames,
                )
                fixed_fcr = {
                    asset.id: current_asset_plan[current_asset_plan["asset_id"] == asset.id]
                    .sort_values("timestamp_utc")
                    .reset_index(drop=True)
                    .iloc[start_idx:end_idx]["fcr_reserved_mw"]
                    .reset_index(drop=True)
                    for asset in execution_config.assets
                }
                if config.forecast_provider.mode == "scenario_bundle":
                    solution = solve_portfolio_day_ahead_dispatch_scenario(
                        price_frame=remaining_da,
                        price_snapshot=day_snapshot,
                        site=execution_config.site,
                        assets=execution_config.assets,
                        risk=_risk_preference(config),
                        degradation_costs_eur_per_mwh=_asset_degradation_costs(execution_config),
                        initial_soc_mwh_by_asset=initial_by_asset,
                        terminal_soc_mwh_by_asset=terminal_by_asset,
                        fixed_fcr_reserved_mw_by_asset=fixed_fcr,
                        reserve_sustain_duration_minutes=execution_config.fcr.sustain_duration_minutes,
                        strategy_name=benchmark.benchmark_name,
                    )
                else:
                    remaining_da["price_eur_per_mwh"] = _expected_snapshot(day_snapshot)[
                        "forecast_price_eur_per_mwh"
                    ].values
                    solution = solve_portfolio_day_ahead_fcr_dispatch(
                        day_ahead_frame=remaining_da,
                        fcr_capacity_frame=remaining_fcr,
                        site=execution_config.site,
                        assets=execution_config.assets,
                        degradation_costs_eur_per_mwh=_asset_degradation_costs(execution_config),
                        reserve_sustain_duration_minutes=execution_config.fcr.sustain_duration_minutes,
                        reserve_penalty_eur_per_mw=float(execution_config.fcr.non_delivery_penalty_eur_per_mw),
                        initial_soc_mwh_by_asset=initial_by_asset,
                        terminal_soc_mwh_by_asset=terminal_by_asset,
                        fixed_fcr_reserved_mw_by_asset=fixed_fcr,
                        strategy_name=benchmark.benchmark_name,
                    )
                revised_site, revised_asset = _decorate_portfolio_outputs(
                    site_dispatch=solution.site_dispatch,
                    asset_dispatch=solution.asset_dispatch,
                    config=execution_config,
                    decision_type="schedule_revision",
                    decision_time_utc=decision_time,
                    market_id=adapter.market_id,
                )
            else:
                asset = execution_config.primary_asset
                initial_soc, terminal_soc = _single_asset_soc_targets(
                    current_site_plan, asset, start_idx=start_idx, end_idx=end_idx
                )
                visible_frames = {
                    "day_ahead": all_day_ahead_actual,
                    "fcr_capacity": day_fcr,
                }
                day_snapshot = _provider_forecast(
                    market="day_ahead",
                    decision_time_utc=decision_time,
                    delivery_frame=remaining_da,
                    actual_frame=all_day_ahead_actual,
                    provider=provider,
                    visible_frames=visible_frames,
                )
                remaining_fcr = day_fcr.iloc[start_idx:end_idx].reset_index(drop=True).copy()
                remaining_fcr["price_eur_per_mwh"] = current_site_plan.iloc[start_idx:end_idx][
                    "fcr_capacity_price_forecast_eur_per_mw_per_h"
                ].values
                fixed_fcr = current_site_plan.iloc[start_idx:end_idx]["fcr_reserved_mw"].reset_index(drop=True)
                if config.forecast_provider.mode == "scenario_bundle":
                    single_asset_solution = solve_day_ahead_dispatch_scenario(
                        price_frame=remaining_da,
                        price_snapshot=day_snapshot,
                        battery=asset.battery,
                        risk=_risk_preference(config),
                        degradation_cost_eur_per_mwh=_degradation_cost_per_mwh(execution_config, asset),
                        initial_soc_mwh=initial_soc,
                        terminal_soc_mwh=terminal_soc,
                        fixed_fcr_reserved_mw=fixed_fcr,
                        reserve_sustain_duration_minutes=execution_config.fcr.sustain_duration_minutes,
                        strategy_name=benchmark.benchmark_name,
                    )
                else:
                    remaining_da["price_eur_per_mwh"] = _expected_snapshot(day_snapshot)[
                        "forecast_price_eur_per_mwh"
                    ].values
                    single_asset_solution = solve_day_ahead_fcr_dispatch(
                        day_ahead_frame=remaining_da,
                        fcr_capacity_frame=remaining_fcr,
                        battery=asset.battery,
                        degradation_cost_eur_per_mwh=_degradation_cost_per_mwh(execution_config, asset),
                        reserve_sustain_duration_minutes=execution_config.fcr.sustain_duration_minutes,
                        reserve_penalty_eur_per_mw=float(execution_config.fcr.non_delivery_penalty_eur_per_mw),
                        initial_soc_mwh=initial_soc,
                        terminal_soc_mwh=terminal_soc,
                        fixed_fcr_reserved_mw=fixed_fcr,
                        strategy_name=benchmark.benchmark_name,
                    )
                revised_site, revised_asset = _single_asset_to_frames(
                    single_asset_solution.dispatch.copy(), config=execution_config
                )
                revised_site["market_id"] = adapter.market_id
                revised_site["workflow_family"] = execution_config.workflow
                revised_site["decision_type"] = "schedule_revision"
                revised_site["decision_time_utc"] = decision_time
                revised_asset["market_id"] = adapter.market_id
                revised_asset["workflow_family"] = execution_config.workflow
                revised_asset["decision_type"] = "schedule_revision"
                revised_asset["decision_time_utc"] = decision_time
            revised_site = _merge_forecast_snapshot(
                revised_site, day_snapshot, target_column="day_ahead_forecast_price_eur_per_mwh"
            )
            revised_site["fcr_capacity_price_forecast_eur_per_mw_per_h"] = current_site_plan.iloc[start_idx:end_idx][
                "fcr_capacity_price_forecast_eur_per_mw_per_h"
            ].values
            revised_site = _merge_market_price(
                revised_site, remaining_da, target_column="day_ahead_actual_price_eur_per_mwh"
            )
            revised_site = _merge_market_price(
                revised_site,
                day_fcr.iloc[start_idx:end_idx].reset_index(drop=True),
                target_column="fcr_capacity_price_actual_eur_per_mw_per_h",
            )
            revised_site = _attach_optional_imbalance_columns(revised_site, None)
            revised_site = _ensure_dispatch_columns(revised_site, site_id=config.site.id, run_scope=config.run_scope)
            revised_site = assign_site_reason_codes(revised_site, config.site)

            revised_asset = _merge_forecast_snapshot(
                revised_asset, day_snapshot, target_column="day_ahead_forecast_price_eur_per_mwh"
            )
            revised_asset = revised_asset.merge(
                current_asset_plan[
                    [
                        "timestamp_utc",
                        "asset_id",
                        "fcr_capacity_price_forecast_eur_per_mw_per_h",
                    ]
                ],
                on=["timestamp_utc", "asset_id"],
                how="left",
            )
            revised_asset = _merge_market_price(
                revised_asset, remaining_da, target_column="day_ahead_actual_price_eur_per_mwh"
            )
            revised_asset = _merge_market_price(
                revised_asset,
                day_fcr.iloc[start_idx:end_idx].reset_index(drop=True),
                target_column="fcr_capacity_price_actual_eur_per_mw_per_h",
            )
            revised_asset = _attach_optional_imbalance_columns(revised_asset, None)
            revised_asset = _ensure_dispatch_columns(revised_asset, site_id=config.site.id, run_scope=config.run_scope)
            revised_asset = pd.concat(
                [
                    assign_reason_codes(
                        revised_asset[revised_asset["asset_id"] == asset.id].copy(),
                        asset.battery,
                        overlay=False,
                    )
                    for asset in execution_config.assets
                ],
                ignore_index=True,
            )
            if config.run_scope == "portfolio":
                objective_value = solution.objective_value_eur
                solver_name = solution.solver_name
            else:
                objective_value = single_asset_solution.objective_value_eur
                solver_name = single_asset_solution.solver_name
            snapshots.append(
                _record_snapshot(
                    day_snapshot,
                    decision_id=decision_id,
                    decision_time_utc=decision_time,
                    decision_type="schedule_revision",
                    schedule_version=revision_version,
                    benchmark_name=benchmark.benchmark_name,
                    market_id=adapter.market_id,
                    workflow_family=config.execution_workflow,
                    run_scope=config.run_scope,
                    site_id=config.site.id,
                )
            )
        elif config.execution_workflow == "da_plus_afrr":
            if (
                execution_config.afrr is None
                or day_afrr_capacity_up is None
                or day_afrr_capacity_down is None
                or day_afrr_activation_up is None
                or day_afrr_activation_down is None
                or day_afrr_ratio_up is None
                or day_afrr_ratio_down is None
            ):
                raise ValueError("schedule_revision da_plus_afrr requires aFRR inputs")
            remaining_da_actual = day_da.iloc[start_idx:end_idx].reset_index(drop=True).copy()
            remaining_capacity_up_actual = day_afrr_capacity_up.iloc[start_idx:end_idx].reset_index(drop=True).copy()
            remaining_capacity_down_actual = (
                day_afrr_capacity_down.iloc[start_idx:end_idx].reset_index(drop=True).copy()
            )
            remaining_activation_up_actual = (
                day_afrr_activation_up.iloc[start_idx:end_idx].reset_index(drop=True).copy()
            )
            remaining_activation_down_actual = (
                day_afrr_activation_down.iloc[start_idx:end_idx].reset_index(drop=True).copy()
            )
            remaining_ratio_up_actual = day_afrr_ratio_up.iloc[start_idx:end_idx].reset_index(drop=True).copy()
            remaining_ratio_down_actual = day_afrr_ratio_down.iloc[start_idx:end_idx].reset_index(drop=True).copy()
            visible_frames = {
                "day_ahead": all_day_ahead_actual,
                "afrr_capacity_up": all_afrr_capacity_up_actual,
                "afrr_capacity_down": all_afrr_capacity_down_actual,
                "afrr_activation_price_up": all_afrr_activation_price_up_actual,
                "afrr_activation_price_down": all_afrr_activation_price_down_actual,
                "afrr_activation_ratio_up": all_afrr_activation_ratio_up_actual,
                "afrr_activation_ratio_down": all_afrr_activation_ratio_down_actual,
            }
            day_snapshot = _provider_forecast(
                market="day_ahead",
                decision_time_utc=decision_time,
                delivery_frame=remaining_da_actual,
                actual_frame=all_day_ahead_actual,
                provider=provider,
                visible_frames=visible_frames,
            )
            capacity_up_snapshot = _provider_forecast(
                market="afrr_capacity_up",
                decision_time_utc=decision_time,
                delivery_frame=remaining_capacity_up_actual,
                actual_frame=all_afrr_capacity_up_actual,
                provider=provider,
                visible_frames=visible_frames,
            )
            capacity_down_snapshot = _provider_forecast(
                market="afrr_capacity_down",
                decision_time_utc=decision_time,
                delivery_frame=remaining_capacity_down_actual,
                actual_frame=all_afrr_capacity_down_actual,
                provider=provider,
                visible_frames=visible_frames,
            )
            activation_up_snapshot = _provider_forecast(
                market="afrr_activation_price_up",
                decision_time_utc=decision_time,
                delivery_frame=remaining_activation_up_actual,
                actual_frame=all_afrr_activation_price_up_actual,
                provider=provider,
                visible_frames=visible_frames,
            )
            activation_down_snapshot = _provider_forecast(
                market="afrr_activation_price_down",
                decision_time_utc=decision_time,
                delivery_frame=remaining_activation_down_actual,
                actual_frame=all_afrr_activation_price_down_actual,
                provider=provider,
                visible_frames=visible_frames,
            )
            ratio_up_snapshot = _provider_forecast(
                market="afrr_activation_ratio_up",
                decision_time_utc=decision_time,
                delivery_frame=remaining_ratio_up_actual,
                actual_frame=all_afrr_activation_ratio_up_actual,
                provider=provider,
                visible_frames=visible_frames,
            )
            ratio_down_snapshot = _provider_forecast(
                market="afrr_activation_ratio_down",
                decision_time_utc=decision_time,
                delivery_frame=remaining_ratio_down_actual,
                actual_frame=all_afrr_activation_ratio_down_actual,
                provider=provider,
                visible_frames=visible_frames,
            )
            used_locked_commitment_fallback = False
            if config.run_scope == "portfolio":
                initial_by_asset, terminal_by_asset = _portfolio_soc_targets(
                    current_asset_plan, execution_config.assets, start_idx=start_idx, end_idx=end_idx
                )
                if end_idx < len(day_da):
                    terminal_by_asset = {asset.id: None for asset in execution_config.assets}
                fixed_afrr_up = {
                    asset.id: current_asset_plan[current_asset_plan["asset_id"] == asset.id]
                    .sort_values("timestamp_utc")
                    .reset_index(drop=True)
                    .iloc[start_idx:end_idx]["afrr_up_reserved_mw"]
                    .reset_index(drop=True)
                    for asset in execution_config.assets
                }
                fixed_afrr_down = {
                    asset.id: current_asset_plan[current_asset_plan["asset_id"] == asset.id]
                    .sort_values("timestamp_utc")
                    .reset_index(drop=True)
                    .iloc[start_idx:end_idx]["afrr_down_reserved_mw"]
                    .reset_index(drop=True)
                    for asset in execution_config.assets
                }
                try:
                    if config.forecast_provider.mode == "scenario_bundle":
                        solution = solve_portfolio_day_ahead_afrr_dispatch_scenario(
                            day_ahead_frame=remaining_da_actual,
                            day_ahead_snapshot=day_snapshot,
                            afrr_capacity_up_snapshot=capacity_up_snapshot,
                            afrr_capacity_down_snapshot=capacity_down_snapshot,
                            afrr_activation_price_up_snapshot=activation_up_snapshot,
                            afrr_activation_price_down_snapshot=activation_down_snapshot,
                            afrr_activation_ratio_up_snapshot=ratio_up_snapshot,
                            afrr_activation_ratio_down_snapshot=ratio_down_snapshot,
                            site=execution_config.site,
                            assets=execution_config.assets,
                            risk=_risk_preference(config),
                            degradation_costs_eur_per_mwh=_asset_degradation_costs(execution_config),
                            initial_soc_mwh_by_asset=initial_by_asset,
                            terminal_soc_mwh_by_asset=terminal_by_asset,
                            fixed_afrr_up_reserved_mw_by_asset=fixed_afrr_up,
                            fixed_afrr_down_reserved_mw_by_asset=fixed_afrr_down,
                            reserve_sustain_duration_minutes=execution_config.afrr.sustain_duration_minutes,
                            reserve_penalty_eur_per_mw=float(execution_config.afrr.non_delivery_penalty_eur_per_mw),
                            strategy_name=benchmark.benchmark_name,
                        )
                    else:
                        remaining_da = remaining_da_actual.copy()
                        remaining_da["price_eur_per_mwh"] = _expected_snapshot(day_snapshot)[
                            "forecast_price_eur_per_mwh"
                        ].values
                        remaining_capacity_up = remaining_capacity_up_actual.copy()
                        remaining_capacity_up["price_eur_per_mwh"] = _expected_snapshot(capacity_up_snapshot)[
                            "forecast_price_eur_per_mwh"
                        ].values
                        remaining_capacity_down = remaining_capacity_down_actual.copy()
                        remaining_capacity_down["price_eur_per_mwh"] = _expected_snapshot(capacity_down_snapshot)[
                            "forecast_price_eur_per_mwh"
                        ].values
                        remaining_activation_up = remaining_activation_up_actual.copy()
                        remaining_activation_up["price_eur_per_mwh"] = _expected_snapshot(activation_up_snapshot)[
                            "forecast_price_eur_per_mwh"
                        ].values
                        remaining_activation_down = remaining_activation_down_actual.copy()
                        remaining_activation_down["price_eur_per_mwh"] = _expected_snapshot(activation_down_snapshot)[
                            "forecast_price_eur_per_mwh"
                        ].values
                        remaining_ratio_up = remaining_ratio_up_actual.copy()
                        remaining_ratio_up["price_eur_per_mwh"] = _expected_snapshot(ratio_up_snapshot)[
                            "forecast_price_eur_per_mwh"
                        ].values
                        remaining_ratio_down = remaining_ratio_down_actual.copy()
                        remaining_ratio_down["price_eur_per_mwh"] = _expected_snapshot(ratio_down_snapshot)[
                            "forecast_price_eur_per_mwh"
                        ].values
                        solution = solve_portfolio_day_ahead_afrr_dispatch(
                            day_ahead_frame=remaining_da,
                            afrr_capacity_up_frame=remaining_capacity_up,
                            afrr_capacity_down_frame=remaining_capacity_down,
                            afrr_activation_price_up_frame=remaining_activation_up,
                            afrr_activation_price_down_frame=remaining_activation_down,
                            afrr_activation_ratio_up_frame=remaining_ratio_up,
                            afrr_activation_ratio_down_frame=remaining_ratio_down,
                            site=execution_config.site,
                            assets=execution_config.assets,
                            degradation_costs_eur_per_mwh=_asset_degradation_costs(execution_config),
                            initial_soc_mwh_by_asset=initial_by_asset,
                            terminal_soc_mwh_by_asset=terminal_by_asset,
                            fixed_afrr_up_reserved_mw_by_asset=fixed_afrr_up,
                            fixed_afrr_down_reserved_mw_by_asset=fixed_afrr_down,
                            reserve_sustain_duration_minutes=execution_config.afrr.sustain_duration_minutes,
                            reserve_penalty_eur_per_mw=float(execution_config.afrr.non_delivery_penalty_eur_per_mw),
                            strategy_name=benchmark.benchmark_name,
                        )
                    revised_site, revised_asset = _decorate_portfolio_outputs(
                        site_dispatch=solution.site_dispatch,
                        asset_dispatch=solution.asset_dispatch,
                        config=execution_config,
                        decision_type="schedule_revision",
                        decision_time_utc=decision_time,
                        market_id=adapter.market_id,
                    )
                    objective_value = solution.objective_value_eur
                    solver_name = solution.solver_name
                except RuntimeError:
                    used_locked_commitment_fallback = True
                    slice_timestamps = set(day_da.iloc[start_idx:end_idx]["timestamp_utc"])
                    revised_site = (
                        current_site_plan[current_site_plan["timestamp_utc"].isin(slice_timestamps)]
                        .sort_values("timestamp_utc")
                        .reset_index(drop=True)
                    )
                    revised_asset = (
                        current_asset_plan[current_asset_plan["timestamp_utc"].isin(slice_timestamps)]
                        .sort_values(["asset_id", "timestamp_utc"])
                        .reset_index(drop=True)
                    )
                    revised_site = revised_site.drop(
                        columns=[col for col in revised_site.columns if "forecast" in col or "actual" in col],
                        errors="ignore",
                    )
                    revised_asset = revised_asset.drop(
                        columns=[col for col in revised_asset.columns if "forecast" in col or "actual" in col],
                        errors="ignore",
                    )
                    objective_value = float(revised_site.get("expected_total_pnl_eur", pd.Series(dtype=float)).sum())
                    solver_name = "locked_commitment_fallback"
            else:
                asset = execution_config.primary_asset
                initial_soc, terminal_soc = _single_asset_soc_targets(
                    current_site_plan, asset, start_idx=start_idx, end_idx=end_idx
                )
                if end_idx < len(day_da):
                    terminal_soc = None
                fixed_afrr_up = current_site_plan.iloc[start_idx:end_idx]["afrr_up_reserved_mw"].reset_index(drop=True)
                fixed_afrr_down = current_site_plan.iloc[start_idx:end_idx]["afrr_down_reserved_mw"].reset_index(
                    drop=True
                )
                try:
                    if config.forecast_provider.mode == "scenario_bundle":
                        single_asset_solution = solve_day_ahead_afrr_dispatch_scenario(
                            day_ahead_frame=remaining_da_actual,
                            day_ahead_snapshot=day_snapshot,
                            afrr_capacity_up_snapshot=capacity_up_snapshot,
                            afrr_capacity_down_snapshot=capacity_down_snapshot,
                            afrr_activation_price_up_snapshot=activation_up_snapshot,
                            afrr_activation_price_down_snapshot=activation_down_snapshot,
                            afrr_activation_ratio_up_snapshot=ratio_up_snapshot,
                            afrr_activation_ratio_down_snapshot=ratio_down_snapshot,
                            battery=asset.battery,
                            risk=_risk_preference(config),
                            degradation_cost_eur_per_mwh=_degradation_cost_per_mwh(execution_config, asset),
                            initial_soc_mwh=initial_soc,
                            terminal_soc_mwh=terminal_soc,
                            fixed_afrr_up_reserved_mw=fixed_afrr_up,
                            fixed_afrr_down_reserved_mw=fixed_afrr_down,
                            reserve_sustain_duration_minutes=execution_config.afrr.sustain_duration_minutes,
                            reserve_penalty_eur_per_mw=float(execution_config.afrr.non_delivery_penalty_eur_per_mw),
                            strategy_name=benchmark.benchmark_name,
                        )
                    else:
                        remaining_da = remaining_da_actual.copy()
                        remaining_da["price_eur_per_mwh"] = _expected_snapshot(day_snapshot)[
                            "forecast_price_eur_per_mwh"
                        ].values
                        remaining_capacity_up = remaining_capacity_up_actual.copy()
                        remaining_capacity_up["price_eur_per_mwh"] = _expected_snapshot(capacity_up_snapshot)[
                            "forecast_price_eur_per_mwh"
                        ].values
                        remaining_capacity_down = remaining_capacity_down_actual.copy()
                        remaining_capacity_down["price_eur_per_mwh"] = _expected_snapshot(capacity_down_snapshot)[
                            "forecast_price_eur_per_mwh"
                        ].values
                        remaining_activation_up = remaining_activation_up_actual.copy()
                        remaining_activation_up["price_eur_per_mwh"] = _expected_snapshot(activation_up_snapshot)[
                            "forecast_price_eur_per_mwh"
                        ].values
                        remaining_activation_down = remaining_activation_down_actual.copy()
                        remaining_activation_down["price_eur_per_mwh"] = _expected_snapshot(activation_down_snapshot)[
                            "forecast_price_eur_per_mwh"
                        ].values
                        remaining_ratio_up = remaining_ratio_up_actual.copy()
                        remaining_ratio_up["price_eur_per_mwh"] = _expected_snapshot(ratio_up_snapshot)[
                            "forecast_price_eur_per_mwh"
                        ].values
                        remaining_ratio_down = remaining_ratio_down_actual.copy()
                        remaining_ratio_down["price_eur_per_mwh"] = _expected_snapshot(ratio_down_snapshot)[
                            "forecast_price_eur_per_mwh"
                        ].values
                        single_asset_solution = solve_day_ahead_afrr_dispatch(
                            day_ahead_frame=remaining_da,
                            afrr_capacity_up_frame=remaining_capacity_up,
                            afrr_capacity_down_frame=remaining_capacity_down,
                            afrr_activation_price_up_frame=remaining_activation_up,
                            afrr_activation_price_down_frame=remaining_activation_down,
                            afrr_activation_ratio_up_frame=remaining_ratio_up,
                            afrr_activation_ratio_down_frame=remaining_ratio_down,
                            battery=asset.battery,
                            degradation_cost_eur_per_mwh=_degradation_cost_per_mwh(execution_config, asset),
                            initial_soc_mwh=initial_soc,
                            terminal_soc_mwh=terminal_soc,
                            fixed_afrr_up_reserved_mw=fixed_afrr_up,
                            fixed_afrr_down_reserved_mw=fixed_afrr_down,
                            reserve_sustain_duration_minutes=execution_config.afrr.sustain_duration_minutes,
                            reserve_penalty_eur_per_mw=float(execution_config.afrr.non_delivery_penalty_eur_per_mw),
                            strategy_name=benchmark.benchmark_name,
                        )
                    revised_site, revised_asset = _single_asset_to_frames(
                        single_asset_solution.dispatch.copy(), config=execution_config
                    )
                    revised_site["market_id"] = adapter.market_id
                    revised_site["workflow_family"] = execution_config.workflow
                    revised_site["decision_type"] = "schedule_revision"
                    revised_site["decision_time_utc"] = decision_time
                    revised_asset["market_id"] = adapter.market_id
                    revised_asset["workflow_family"] = execution_config.workflow
                    revised_asset["decision_type"] = "schedule_revision"
                    revised_asset["decision_time_utc"] = decision_time
                    objective_value = single_asset_solution.objective_value_eur
                    solver_name = single_asset_solution.solver_name
                except RuntimeError:
                    used_locked_commitment_fallback = True
                    revised_site = current_site_plan.iloc[start_idx:end_idx].reset_index(drop=True).copy()
                    revised_asset = current_asset_plan.iloc[start_idx:end_idx].reset_index(drop=True).copy()
                    revised_site = revised_site.drop(
                        columns=[col for col in revised_site.columns if "forecast" in col or "actual" in col],
                        errors="ignore",
                    )
                    revised_asset = revised_asset.drop(
                        columns=[col for col in revised_asset.columns if "forecast" in col or "actual" in col],
                        errors="ignore",
                    )
                    objective_value = float(revised_site.get("expected_total_pnl_eur", pd.Series(dtype=float)).sum())
                    solver_name = "locked_commitment_fallback"
            if end_idx < len(day_da) and not used_locked_commitment_fallback:
                previous_site_boundary = float(
                    current_site_plan.loc[
                        current_site_plan["timestamp_utc"] == revision_boundary_timestamp_utc, "soc_mwh"
                    ].iloc[0]
                )
                future_site_soc_delta_mwh = (
                    float(revised_site.sort_values("timestamp_utc").iloc[-1]["soc_mwh"]) - previous_site_boundary
                )
                ordered_revised_asset = revised_asset.sort_values(["asset_id", "timestamp_utc"]).reset_index(drop=True)
                ordered_current_asset = current_asset_plan.sort_values(["asset_id", "timestamp_utc"]).reset_index(
                    drop=True
                )
                for asset in execution_config.assets:
                    new_boundary = float(
                        ordered_revised_asset[ordered_revised_asset["asset_id"] == asset.id].iloc[-1]["soc_mwh"]
                    )
                    old_boundary = float(
                        ordered_current_asset[
                            (ordered_current_asset["asset_id"] == asset.id)
                            & (ordered_current_asset["timestamp_utc"] == revision_boundary_timestamp_utc)
                        ].iloc[0]["soc_mwh"]
                    )
                    future_asset_soc_delta_mwh[asset.id] = new_boundary - old_boundary
            revised_site = _merge_forecast_snapshot(
                revised_site, day_snapshot, target_column="day_ahead_forecast_price_eur_per_mwh"
            )
            revised_site = _merge_market_price(
                revised_site, remaining_da_actual, target_column="day_ahead_actual_price_eur_per_mwh"
            )
            revised_site = _attach_optional_imbalance_columns(revised_site, None)
            revised_site = _attach_zero_fcr_columns(revised_site)
            revised_site = _merge_afrr_snapshot_columns(
                revised_site,
                capacity_up=capacity_up_snapshot,
                capacity_down=capacity_down_snapshot,
                activation_price_up=activation_up_snapshot,
                activation_price_down=activation_down_snapshot,
                activation_ratio_up=ratio_up_snapshot,
                activation_ratio_down=ratio_down_snapshot,
            )
            revised_site = _merge_afrr_actual_columns(
                revised_site,
                capacity_up=remaining_capacity_up_actual,
                capacity_down=remaining_capacity_down_actual,
                activation_price_up=remaining_activation_up_actual,
                activation_price_down=remaining_activation_down_actual,
                activation_ratio_up=remaining_ratio_up_actual,
                activation_ratio_down=remaining_ratio_down_actual,
            )
            revised_site = _ensure_dispatch_columns(revised_site, site_id=config.site.id, run_scope=config.run_scope)
            revised_site = assign_site_reason_codes(revised_site, config.site)

            revised_asset = _merge_forecast_snapshot(
                revised_asset, day_snapshot, target_column="day_ahead_forecast_price_eur_per_mwh"
            )
            revised_asset = _merge_market_price(
                revised_asset, remaining_da_actual, target_column="day_ahead_actual_price_eur_per_mwh"
            )
            revised_asset = _attach_optional_imbalance_columns(revised_asset, None)
            revised_asset = _attach_zero_fcr_columns(revised_asset)
            revised_asset = _merge_afrr_snapshot_columns(
                revised_asset,
                capacity_up=capacity_up_snapshot,
                capacity_down=capacity_down_snapshot,
                activation_price_up=activation_up_snapshot,
                activation_price_down=activation_down_snapshot,
                activation_ratio_up=ratio_up_snapshot,
                activation_ratio_down=ratio_down_snapshot,
            )
            revised_asset = _merge_afrr_actual_columns(
                revised_asset,
                capacity_up=remaining_capacity_up_actual,
                capacity_down=remaining_capacity_down_actual,
                activation_price_up=remaining_activation_up_actual,
                activation_price_down=remaining_activation_down_actual,
                activation_ratio_up=remaining_ratio_up_actual,
                activation_ratio_down=remaining_ratio_down_actual,
            )
            revised_asset = _ensure_dispatch_columns(revised_asset, site_id=config.site.id, run_scope=config.run_scope)
            revised_asset = pd.concat(
                [
                    assign_reason_codes(
                        revised_asset[revised_asset["asset_id"] == asset.id].copy(),
                        asset.battery,
                        overlay=False,
                    )
                    for asset in execution_config.assets
                ],
                ignore_index=True,
            )
            snapshots.append(
                _record_snapshot(
                    day_snapshot,
                    decision_id=decision_id,
                    decision_time_utc=decision_time,
                    decision_type="schedule_revision",
                    schedule_version=revision_version,
                    benchmark_name=benchmark.benchmark_name,
                    market_id=adapter.market_id,
                    workflow_family=config.execution_workflow,
                    run_scope=config.run_scope,
                    site_id=config.site.id,
                )
            )
            for snapshot in (
                capacity_up_snapshot,
                capacity_down_snapshot,
                activation_up_snapshot,
                activation_down_snapshot,
                ratio_up_snapshot,
                ratio_down_snapshot,
            ):
                snapshots.append(
                    _record_snapshot(
                        snapshot,
                        decision_id=decision_id,
                        decision_time_utc=decision_time,
                        decision_type="schedule_revision",
                        schedule_version=revision_version,
                        benchmark_name=benchmark.benchmark_name,
                        market_id=adapter.market_id,
                        workflow_family=config.execution_workflow,
                        run_scope=config.run_scope,
                        site_id=config.site.id,
                    )
                )
        else:
            remaining_da = day_da.iloc[start_idx:end_idx].reset_index(drop=True).copy()
            day_snapshot = _provider_forecast(
                market="day_ahead",
                decision_time_utc=decision_time,
                delivery_frame=remaining_da,
                actual_frame=all_day_ahead_actual,
                provider=provider,
                visible_frames={"day_ahead": all_day_ahead_actual},
            )
            if config.run_scope == "portfolio":
                initial_by_asset, terminal_by_asset = _portfolio_soc_targets(
                    current_asset_plan, execution_config.assets, start_idx=start_idx, end_idx=end_idx
                )
                if config.forecast_provider.mode == "scenario_bundle":
                    solution = solve_portfolio_day_ahead_dispatch_scenario(
                        price_frame=remaining_da,
                        price_snapshot=day_snapshot,
                        site=execution_config.site,
                        assets=execution_config.assets,
                        risk=_risk_preference(config),
                        degradation_costs_eur_per_mwh=_asset_degradation_costs(execution_config),
                        initial_soc_mwh_by_asset=initial_by_asset,
                        terminal_soc_mwh_by_asset=terminal_by_asset,
                        strategy_name=benchmark.benchmark_name,
                    )
                else:
                    remaining_da["price_eur_per_mwh"] = _expected_snapshot(day_snapshot)[
                        "forecast_price_eur_per_mwh"
                    ].values
                    solution = solve_portfolio_day_ahead_dispatch(
                        remaining_da,
                        execution_config.site,
                        execution_config.assets,
                        degradation_costs_eur_per_mwh=_asset_degradation_costs(execution_config),
                        initial_soc_mwh_by_asset=initial_by_asset,
                        terminal_soc_mwh_by_asset=terminal_by_asset,
                        strategy_name=benchmark.benchmark_name,
                    )
                revised_site, revised_asset = _decorate_portfolio_outputs(
                    site_dispatch=solution.site_dispatch,
                    asset_dispatch=solution.asset_dispatch,
                    config=execution_config,
                    decision_type="schedule_revision",
                    decision_time_utc=decision_time,
                    market_id=adapter.market_id,
                )
            else:
                asset = execution_config.primary_asset
                initial_soc, terminal_soc = _single_asset_soc_targets(
                    current_site_plan, asset, start_idx=start_idx, end_idx=end_idx
                )
                if config.forecast_provider.mode == "scenario_bundle":
                    single_asset_solution = solve_day_ahead_dispatch_scenario(
                        price_frame=remaining_da,
                        price_snapshot=day_snapshot,
                        battery=asset.battery,
                        risk=_risk_preference(config),
                        degradation_cost_eur_per_mwh=_degradation_cost_per_mwh(execution_config, asset),
                        initial_soc_mwh=initial_soc,
                        terminal_soc_mwh=terminal_soc,
                        strategy_name=benchmark.benchmark_name,
                    )
                else:
                    remaining_da["price_eur_per_mwh"] = _expected_snapshot(day_snapshot)[
                        "forecast_price_eur_per_mwh"
                    ].values
                    single_asset_solution = solve_day_ahead_dispatch(
                        remaining_da,
                        asset.battery,
                        degradation_cost_eur_per_mwh=_degradation_cost_per_mwh(execution_config, asset),
                        initial_soc_mwh=initial_soc,
                        terminal_soc_mwh=terminal_soc,
                        strategy_name=benchmark.benchmark_name,
                    )
                revised_site, revised_asset = _single_asset_to_frames(
                    single_asset_solution.dispatch.copy(), config=execution_config
                )
                revised_site["market_id"] = adapter.market_id
                revised_site["workflow_family"] = execution_config.workflow
                revised_site["decision_type"] = "schedule_revision"
                revised_site["decision_time_utc"] = decision_time
                revised_asset["market_id"] = adapter.market_id
                revised_asset["workflow_family"] = execution_config.workflow
                revised_asset["decision_type"] = "schedule_revision"
                revised_asset["decision_time_utc"] = decision_time
            revised_site = _merge_forecast_snapshot(
                revised_site, day_snapshot, target_column="day_ahead_forecast_price_eur_per_mwh"
            )
            revised_site = _merge_market_price(
                revised_site, remaining_da, target_column="day_ahead_actual_price_eur_per_mwh"
            )
            revised_site = _attach_optional_imbalance_columns(revised_site, None)
            revised_site["fcr_capacity_price_forecast_eur_per_mw_per_h"] = 0.0
            revised_site["fcr_capacity_price_actual_eur_per_mw_per_h"] = 0.0
            revised_site = _ensure_dispatch_columns(revised_site, site_id=config.site.id, run_scope=config.run_scope)
            revised_site = assign_site_reason_codes(revised_site, config.site)

            revised_asset = _merge_forecast_snapshot(
                revised_asset, day_snapshot, target_column="day_ahead_forecast_price_eur_per_mwh"
            )
            revised_asset = _merge_market_price(
                revised_asset, remaining_da, target_column="day_ahead_actual_price_eur_per_mwh"
            )
            revised_asset = _attach_optional_imbalance_columns(revised_asset, None)
            revised_asset["fcr_capacity_price_forecast_eur_per_mw_per_h"] = 0.0
            revised_asset["fcr_capacity_price_actual_eur_per_mw_per_h"] = 0.0
            revised_asset = _ensure_dispatch_columns(revised_asset, site_id=config.site.id, run_scope=config.run_scope)
            revised_asset = pd.concat(
                [
                    assign_reason_codes(
                        revised_asset[revised_asset["asset_id"] == asset.id].copy(),
                        asset.battery,
                        overlay=False,
                    )
                    for asset in execution_config.assets
                ],
                ignore_index=True,
            )
            if config.run_scope == "portfolio":
                objective_value = solution.objective_value_eur
                solver_name = solution.solver_name
            else:
                objective_value = single_asset_solution.objective_value_eur
                solver_name = single_asset_solution.solver_name
            snapshots.append(
                _record_snapshot(
                    day_snapshot,
                    decision_id=decision_id,
                    decision_time_utc=decision_time,
                    decision_type="schedule_revision",
                    schedule_version=revision_version,
                    benchmark_name=benchmark.benchmark_name,
                    market_id=adapter.market_id,
                    workflow_family=config.execution_workflow,
                    run_scope=config.run_scope,
                    site_id=config.site.id,
                )
            )

        revised_site = _annotate_schedule_frame(
            revised_site,
            schedule_version=revision_version,
            schedule_state="revised_plan",
            lock_state="planned",
        )
        revised_asset = _annotate_schedule_frame(
            revised_asset,
            schedule_version=revision_version,
            schedule_state="revised_plan",
            lock_state="planned",
        )
        current_site_plan = _replace_site_plan_slice(current_site_plan, revised_site)
        current_asset_plan = _replace_asset_plan_slice(current_asset_plan, revised_asset)
        if config.execution_workflow == "da_plus_afrr" and end_idx < len(day_da):
            current_site_plan = _shift_future_site_soc(
                current_site_plan,
                after_timestamp_utc=revision_boundary_timestamp_utc,
                delta_mwh=future_site_soc_delta_mwh,
            )
            current_asset_plan = _shift_future_asset_soc(
                current_asset_plan,
                after_timestamp_utc=revision_boundary_timestamp_utc,
                delta_mwh_by_asset=future_asset_soc_delta_mwh,
            )
        lineage_frames.extend(
            [_lineage_frame(revised_site, entity_type="site"), _lineage_frame(revised_asset, entity_type="asset")]
        )
        decisions.append(
            {
                "decision_id": decision_id,
                "market_id": adapter.market_id,
                "workflow_family": config.execution_workflow,
                "run_scope": config.run_scope,
                "site_id": config.site.id,
                "decision_time_utc": decision_time,
                "decision_time_local": decision_time.tz_convert(adapter.timezone),
                "decision_type": "schedule_revision",
                "delivery_date_local": str(delivery_date.date()),
                "horizon_start_utc": day_da.iloc[start_idx]["timestamp_utc"],
                "horizon_end_utc": day_da.iloc[end_idx - 1]["timestamp_utc"]
                + pd.Timedelta(minutes=config.timing.resolution_minutes),
                "locked_intervals": start_idx,
                "locked_horizon_start_utc": day_da.iloc[0]["timestamp_utc"],
                "locked_horizon_end_utc": day_da.iloc[start_idx - 1]["timestamp_utc"]
                + pd.Timedelta(minutes=config.timing.resolution_minutes)
                if start_idx > 0
                else day_da.iloc[0]["timestamp_utc"],
                "provider_name": provider.name,
                "benchmark_name": benchmark.benchmark_name,
                "objective_value_eur": objective_value,
                "solver_name": solver_name,
                "schedule_version": revision_version,
                "revision_index": revision_counter,
            }
        )

    realized_site = _annotate_schedule_frame(
        current_site_plan,
        schedule_version=current_site_plan["schedule_version"],
        schedule_state="locked_realized",
        lock_state="locked_realized",
    )
    realized_asset = _annotate_schedule_frame(
        current_asset_plan,
        schedule_version=current_asset_plan["schedule_version"],
        schedule_state="locked_realized",
        lock_state="locked_realized",
    )
    reserve_penalty = 0.0
    if config.fcr is not None:
        reserve_penalty = float(config.fcr.non_delivery_penalty_eur_per_mw)
    if config.afrr is not None:
        reserve_penalty = float(config.afrr.non_delivery_penalty_eur_per_mw)
    settlement_engine = adapter.settlement_engine(config.execution_workflow)
    baseline_for_recon = baseline_site.copy()
    baseline_for_recon["workflow_family"] = config.execution_workflow
    baseline_settled, _ = _site_interval_settlement(
        baseline_for_recon,
        workflow=config.execution_workflow,
        degradation_cost_eur_per_mwh=0.0,
        settlement_engine=settlement_engine,
        reserve_penalty_eur_per_mw=reserve_penalty,
    )
    revised_for_recon = realized_site.copy()
    revised_for_recon["workflow_family"] = config.execution_workflow
    revised_settled, _ = _site_interval_settlement(
        revised_for_recon,
        workflow=config.execution_workflow,
        degradation_cost_eur_per_mwh=0.0,
        settlement_engine=settlement_engine,
        reserve_penalty_eur_per_mw=reserve_penalty,
    )
    reconciliation_breakdown = pd.DataFrame(
        {
            "timestamp_utc": revised_settled["timestamp_utc"],
            "site_id": config.site.id,
            "market_id": adapter.market_id,
            "workflow_family": config.execution_workflow,
            "run_scope": config.run_scope,
            "baseline_expected_pnl_eur": baseline_settled["expected_pnl_eur"].values,
            "revised_expected_pnl_eur": revised_settled["expected_pnl_eur"].values,
            "realized_pnl_eur": revised_settled["realized_pnl_eur"].values,
        }
    )
    reconciliation_breakdown["locked_commitment_opportunity_cost_eur"] = (
        reconciliation_breakdown["revised_expected_pnl_eur"] - reconciliation_breakdown["baseline_expected_pnl_eur"]
    )
    if config.execution_workflow == "da_plus_imbalance":
        reconciliation_breakdown["imbalance_settlement_deviation_eur"] = (
            revised_settled["imbalance_revenue_eur"] - revised_settled["expected_imbalance_revenue_eur"]
        )
    else:
        reconciliation_breakdown["imbalance_settlement_deviation_eur"] = 0.0
    if config.execution_workflow == "da_plus_afrr":
        reconciliation_breakdown["activation_settlement_deviation_eur"] = (
            revised_settled["reserve_activation_revenue_eur"]
            - revised_settled["expected_reserve_activation_revenue_eur"]
        )
    else:
        reconciliation_breakdown["activation_settlement_deviation_eur"] = 0.0
    reconciliation_breakdown["reserve_headroom_opportunity_cost_eur"] = 0.0
    reconciliation_breakdown["degradation_cost_drift_eur"] = 0.0
    reconciliation_breakdown["availability_deviation_eur"] = 0.0
    reconciliation_breakdown["forecast_error_eur"] = (
        reconciliation_breakdown["realized_pnl_eur"]
        - reconciliation_breakdown["revised_expected_pnl_eur"]
        - reconciliation_breakdown["imbalance_settlement_deviation_eur"]
        - reconciliation_breakdown["activation_settlement_deviation_eur"]
    )
    reconciliation_breakdown["delta_vs_baseline_expected_eur"] = (
        reconciliation_breakdown["realized_pnl_eur"] - reconciliation_breakdown["baseline_expected_pnl_eur"]
    )
    reconciliation_breakdown["delta_vs_revised_expected_eur"] = (
        reconciliation_breakdown["realized_pnl_eur"] - reconciliation_breakdown["revised_expected_pnl_eur"]
    )

    return DailyArtifacts(
        site_dispatch=realized_site,
        asset_dispatch=realized_asset,
        decisions=decisions,
        snapshots=snapshots,
        baseline_schedule=baseline_site,
        revision_schedule=current_site_plan,
        schedule_lineage=pd.concat(lineage_frames, ignore_index=True),
        reconciliation_breakdown=reconciliation_breakdown,
    )


def _run_daily_walk_forward(
    *,
    config: BacktestConfig,
    adapter,
    benchmark: BenchmarkDefinition,
    provider,
    delivery_date: pd.Timestamp,
    day_ahead_actual: pd.DataFrame,
    imbalance_actual: pd.DataFrame | None,
    fcr_actual: pd.DataFrame | None,
    afrr_capacity_up_actual: pd.DataFrame | None,
    afrr_capacity_down_actual: pd.DataFrame | None,
    afrr_activation_price_up_actual: pd.DataFrame | None,
    afrr_activation_price_down_actual: pd.DataFrame | None,
    afrr_activation_ratio_up_actual: pd.DataFrame | None,
    afrr_activation_ratio_down_actual: pd.DataFrame | None,
    all_day_ahead_actual: pd.DataFrame,
    all_imbalance_actual: pd.DataFrame | None,
    all_fcr_actual: pd.DataFrame | None,
    all_afrr_capacity_up_actual: pd.DataFrame | None,
    all_afrr_capacity_down_actual: pd.DataFrame | None,
    all_afrr_activation_price_up_actual: pd.DataFrame | None,
    all_afrr_activation_price_down_actual: pd.DataFrame | None,
    all_afrr_activation_ratio_up_actual: pd.DataFrame | None,
    all_afrr_activation_ratio_down_actual: pd.DataFrame | None,
    schedule: pd.DataFrame,
) -> DailyArtifacts:
    if config.is_revision_workflow:
        return _schedule_revision_daily(
            config=config,
            adapter=adapter,
            benchmark=benchmark,
            provider=provider,
            delivery_date=delivery_date,
            day_ahead_actual=day_ahead_actual,
            imbalance_actual=imbalance_actual,
            fcr_actual=fcr_actual,
            afrr_capacity_up_actual=afrr_capacity_up_actual,
            afrr_capacity_down_actual=afrr_capacity_down_actual,
            afrr_activation_price_up_actual=afrr_activation_price_up_actual,
            afrr_activation_price_down_actual=afrr_activation_price_down_actual,
            afrr_activation_ratio_up_actual=afrr_activation_ratio_up_actual,
            afrr_activation_ratio_down_actual=afrr_activation_ratio_down_actual,
            all_day_ahead_actual=all_day_ahead_actual,
            all_imbalance_actual=all_imbalance_actual,
            all_fcr_actual=all_fcr_actual,
            all_afrr_capacity_up_actual=all_afrr_capacity_up_actual,
            all_afrr_capacity_down_actual=all_afrr_capacity_down_actual,
            all_afrr_activation_price_up_actual=all_afrr_activation_price_up_actual,
            all_afrr_activation_price_down_actual=all_afrr_activation_price_down_actual,
            all_afrr_activation_ratio_up_actual=all_afrr_activation_ratio_up_actual,
            all_afrr_activation_ratio_down_actual=all_afrr_activation_ratio_down_actual,
            schedule=schedule,
        )
    if config.workflow == "da_plus_imbalance":
        if imbalance_actual is None or all_imbalance_actual is None:
            raise ValueError("da_plus_imbalance requires realized imbalance data")
        return _single_asset_imbalance_daily(
            config=config,
            adapter=adapter,
            benchmark=benchmark,
            provider=provider,
            delivery_date=delivery_date,
            day_ahead_actual=day_ahead_actual,
            imbalance_actual=imbalance_actual,
            all_day_ahead_actual=all_day_ahead_actual,
            all_imbalance_actual=all_imbalance_actual,
            schedule=schedule,
        )
    if config.workflow == "da_plus_fcr":
        if fcr_actual is None or all_fcr_actual is None:
            raise ValueError("da_plus_fcr requires realized FCR capacity data")
        return _portfolio_fcr_daily(
            config=config,
            adapter=adapter,
            benchmark=benchmark,
            provider=provider,
            delivery_date=delivery_date,
            day_ahead_actual=day_ahead_actual,
            fcr_actual=fcr_actual,
            all_day_ahead_actual=all_day_ahead_actual,
            all_fcr_actual=all_fcr_actual,
            schedule=schedule,
        )
    if config.workflow == "da_plus_afrr":
        required_frames = (
            afrr_capacity_up_actual,
            afrr_capacity_down_actual,
            afrr_activation_price_up_actual,
            afrr_activation_price_down_actual,
            afrr_activation_ratio_up_actual,
            afrr_activation_ratio_down_actual,
            all_afrr_capacity_up_actual,
            all_afrr_capacity_down_actual,
            all_afrr_activation_price_up_actual,
            all_afrr_activation_price_down_actual,
            all_afrr_activation_ratio_up_actual,
            all_afrr_activation_ratio_down_actual,
        )
        if any(frame is None for frame in required_frames):
            raise ValueError(
                "da_plus_afrr requires realized aFRR capacity, activation price, and activation ratio data"
            )
        return _portfolio_afrr_daily(
            config=config,
            adapter=adapter,
            benchmark=benchmark,
            provider=provider,
            delivery_date=delivery_date,
            day_ahead_actual=day_ahead_actual,
            afrr_capacity_up_actual=afrr_capacity_up_actual,
            afrr_capacity_down_actual=afrr_capacity_down_actual,
            afrr_activation_price_up_actual=afrr_activation_price_up_actual,
            afrr_activation_price_down_actual=afrr_activation_price_down_actual,
            afrr_activation_ratio_up_actual=afrr_activation_ratio_up_actual,
            afrr_activation_ratio_down_actual=afrr_activation_ratio_down_actual,
            all_day_ahead_actual=all_day_ahead_actual,
            all_afrr_capacity_up_actual=all_afrr_capacity_up_actual,
            all_afrr_capacity_down_actual=all_afrr_capacity_down_actual,
            all_afrr_activation_price_up_actual=all_afrr_activation_price_up_actual,
            all_afrr_activation_price_down_actual=all_afrr_activation_price_down_actual,
            all_afrr_activation_ratio_up_actual=all_afrr_activation_ratio_up_actual,
            all_afrr_activation_ratio_down_actual=all_afrr_activation_ratio_down_actual,
            schedule=schedule,
        )
    return _portfolio_da_daily(
        config=config,
        adapter=adapter,
        benchmark=benchmark,
        provider=provider,
        delivery_date=delivery_date,
        day_ahead_actual=day_ahead_actual,
        all_day_ahead_actual=all_day_ahead_actual,
        schedule=schedule,
    )


def _build_summary(
    *,
    result: RunResult,
    config: BacktestConfig,
    adapter,
    actuals,
) -> dict[str, object]:
    dispatch = result.site_dispatch
    execution_workflow = config.execution_workflow
    reason_counts = dispatch["reason_code"].value_counts().sort_index().to_dict()
    interval_count = int(len(dispatch))
    idle_share = (
        float((dispatch["charge_mw"].eq(0.0) & dispatch["discharge_mw"].eq(0.0)).mean()) if interval_count else 0.0
    )
    data_provenance = {
        "day_ahead": {
            "path": actuals.day_ahead.metadata.get("path"),
            "source": actuals.day_ahead.source,
            "zone": actuals.day_ahead.zone,
        },
        "imbalance": {
            "path": actuals.imbalance.metadata.get("path") if actuals.imbalance is not None else None,
            "source": actuals.imbalance.source if actuals.imbalance is not None else None,
            "zone": actuals.imbalance.zone if actuals.imbalance is not None else None,
        },
        "fcr_capacity": {
            "path": actuals.fcr_capacity.metadata.get("path") if actuals.fcr_capacity is not None else None,
            "source": actuals.fcr_capacity.source if actuals.fcr_capacity is not None else None,
            "zone": actuals.fcr_capacity.zone if actuals.fcr_capacity is not None else None,
        },
        "afrr_capacity_up": {
            "path": actuals.afrr_capacity_up.metadata.get("path") if actuals.afrr_capacity_up is not None else None,
            "source": actuals.afrr_capacity_up.source if actuals.afrr_capacity_up is not None else None,
            "zone": actuals.afrr_capacity_up.zone if actuals.afrr_capacity_up is not None else None,
        },
        "afrr_capacity_down": {
            "path": actuals.afrr_capacity_down.metadata.get("path") if actuals.afrr_capacity_down is not None else None,
            "source": actuals.afrr_capacity_down.source if actuals.afrr_capacity_down is not None else None,
            "zone": actuals.afrr_capacity_down.zone if actuals.afrr_capacity_down is not None else None,
        },
        "afrr_activation_price_up": {
            "path": actuals.afrr_activation_price_up.metadata.get("path")
            if actuals.afrr_activation_price_up is not None
            else None,
            "source": actuals.afrr_activation_price_up.source if actuals.afrr_activation_price_up is not None else None,
            "zone": actuals.afrr_activation_price_up.zone if actuals.afrr_activation_price_up is not None else None,
        },
        "afrr_activation_price_down": {
            "path": actuals.afrr_activation_price_down.metadata.get("path")
            if actuals.afrr_activation_price_down is not None
            else None,
            "source": actuals.afrr_activation_price_down.source
            if actuals.afrr_activation_price_down is not None
            else None,
            "zone": actuals.afrr_activation_price_down.zone if actuals.afrr_activation_price_down is not None else None,
        },
        "afrr_activation_ratio_up": {
            "path": actuals.afrr_activation_ratio_up.metadata.get("path")
            if actuals.afrr_activation_ratio_up is not None
            else None,
            "source": actuals.afrr_activation_ratio_up.source if actuals.afrr_activation_ratio_up is not None else None,
            "zone": actuals.afrr_activation_ratio_up.zone if actuals.afrr_activation_ratio_up is not None else None,
        },
        "afrr_activation_ratio_down": {
            "path": actuals.afrr_activation_ratio_down.metadata.get("path")
            if actuals.afrr_activation_ratio_down is not None
            else None,
            "source": actuals.afrr_activation_ratio_down.source
            if actuals.afrr_activation_ratio_down is not None
            else None,
            "zone": actuals.afrr_activation_ratio_down.zone if actuals.afrr_activation_ratio_down is not None else None,
        },
    }
    gross_revenue = (
        result.pnl.da_revenue_eur
        + result.pnl.imbalance_revenue_eur
        + result.pnl.reserve_capacity_revenue_eur
        + result.pnl.reserve_activation_revenue_eur
    )
    reserve_share = result.pnl.reserve_capacity_revenue_eur / gross_revenue if gross_revenue else 0.0
    asset_rank = result.asset_pnl_attribution.sort_values("total_pnl_eur", ascending=False)[
        ["asset_id", "total_pnl_eur"]
    ].to_dict(orient="records")
    throughput_total = float(dispatch["throughput_mwh"].sum())
    degradation_cost_per_mwh = result.pnl.degradation_cost_eur / throughput_total if throughput_total else 0.0
    reserve_penalty_eur_per_mw = _reserve_penalty_eur_per_mw(config, workflow=execution_workflow)
    scenario_analysis = _scenario_analysis(
        dispatch,
        result.forecast_snapshots,
        workflow=execution_workflow,
        degradation_cost_eur_per_mwh=degradation_cost_per_mwh,
        reserve_penalty_eur_per_mw=reserve_penalty_eur_per_mw,
        risk=_risk_preference(config),
        settlement_engine=adapter.settlement_engine(execution_workflow),
        realized_total_pnl_eur=result.pnl.total_pnl_eur,
    )
    summary: dict[str, object] = {
        "schema_version": config.schema_version,
        "run_id": result.run_id,
        "site_id": result.site_id,
        "run_scope": result.run_scope,
        "asset_count": result.asset_count,
        "poi_import_limit_mw": config.site.poi_import_limit_mw,
        "poi_export_limit_mw": config.site.poi_export_limit_mw,
        "market_id": result.market_id,
        "market_timezone": result.market_timezone,
        "workflow": result.workflow,
        "base_workflow": execution_workflow,
        "benchmark_name": result.benchmark_name,
        "benchmark_family": result.benchmark_family,
        "provider_name": result.provider_name,
        "forecast_mode": config.forecast_provider.mode,
        "risk_mode": config.risk.mode,
        "auditable": result.auditable,
        "delivery_start_date": str(config.timing.delivery_start_date),
        "delivery_end_date": str(config.timing.delivery_end_date),
        "interval_count": interval_count,
        "decision_count": int(len(result.decision_log)),
        "locked_interval_count": int(result.decision_log["locked_intervals"].fillna(0).sum()),
        "da_revenue_eur": result.pnl.da_revenue_eur,
        "imbalance_revenue_eur": result.pnl.imbalance_revenue_eur,
        "reserve_capacity_revenue_eur": result.pnl.reserve_capacity_revenue_eur,
        "reserve_activation_revenue_eur": result.pnl.reserve_activation_revenue_eur,
        "reserve_penalty_eur": result.pnl.reserve_penalty_eur,
        "degradation_cost_eur": result.pnl.degradation_cost_eur,
        "total_pnl_eur": result.pnl.total_pnl_eur,
        "expected_total_pnl_eur": result.pnl.expected_total_pnl_eur,
        "throughput_mwh": throughput_total,
        "idle_share": idle_share,
        "max_site_charge_mw": float(dispatch["charge_mw"].max()),
        "max_site_discharge_mw": float(dispatch["discharge_mw"].max()),
        "reserved_capacity_mw_avg": float(dispatch["reserved_capacity_mw"].mean()),
        "reserved_capacity_mw_max": float(dispatch["reserved_capacity_mw"].max()),
        "afrr_up_reserved_mw_avg": float(dispatch["afrr_up_reserved_mw"].mean()),
        "afrr_up_reserved_mw_max": float(dispatch["afrr_up_reserved_mw"].max()),
        "afrr_down_reserved_mw_avg": float(dispatch["afrr_down_reserved_mw"].mean()),
        "afrr_down_reserved_mw_max": float(dispatch["afrr_down_reserved_mw"].max()),
        "reserve_share_of_total_revenue": float(reserve_share),
        "energy_revenue_eur": result.pnl.da_revenue_eur + result.pnl.imbalance_revenue_eur,
        "reason_code_counts": reason_counts,
        "asset_contribution_ranking": asset_rank,
        "data_provenance": data_provenance,
        "forecast_error": {
            "day_ahead": _forecast_error_metrics(
                dispatch["day_ahead_forecast_price_eur_per_mwh"], dispatch["day_ahead_actual_price_eur_per_mwh"]
            ),
            "imbalance": _forecast_error_metrics(
                dispatch["imbalance_forecast_price_eur_per_mwh"], dispatch["imbalance_actual_price_eur_per_mwh"]
            ),
            "fcr_capacity": _forecast_error_metrics(
                dispatch["fcr_capacity_price_forecast_eur_per_mw_per_h"],
                dispatch["fcr_capacity_price_actual_eur_per_mw_per_h"],
            ),
            "afrr_capacity_up": _forecast_error_metrics(
                dispatch["afrr_capacity_up_price_forecast_eur_per_mw_per_h"],
                dispatch["afrr_capacity_up_price_actual_eur_per_mw_per_h"],
            ),
            "afrr_capacity_down": _forecast_error_metrics(
                dispatch["afrr_capacity_down_price_forecast_eur_per_mw_per_h"],
                dispatch["afrr_capacity_down_price_actual_eur_per_mw_per_h"],
            ),
            "afrr_activation_price_up": _forecast_error_metrics(
                dispatch["afrr_activation_price_up_forecast_eur_per_mwh"],
                dispatch["afrr_activation_price_up_actual_eur_per_mwh"],
            ),
            "afrr_activation_price_down": _forecast_error_metrics(
                dispatch["afrr_activation_price_down_forecast_eur_per_mwh"],
                dispatch["afrr_activation_price_down_actual_eur_per_mwh"],
            ),
            "afrr_activation_ratio_up": _forecast_error_metrics(
                dispatch["afrr_activation_ratio_up_forecast"],
                dispatch["afrr_activation_ratio_up_actual"],
            ),
            "afrr_activation_ratio_down": _forecast_error_metrics(
                dispatch["afrr_activation_ratio_down_forecast"],
                dispatch["afrr_activation_ratio_down_actual"],
            ),
        },
    }
    if scenario_analysis is not None:
        summary["scenario_analysis"] = scenario_analysis
    summary.update(adapter.settlement_metadata(config))
    if result.baseline_schedule is not None and result.reconciliation_breakdown is not None:
        reconciliation_summary: dict[str, object] = {
            "baseline_expected_total_pnl_eur": float(
                result.reconciliation_breakdown["baseline_expected_pnl_eur"].sum()
            ),
            "revised_expected_total_pnl_eur": float(result.reconciliation_breakdown["revised_expected_pnl_eur"].sum()),
            "realized_total_pnl_eur": float(result.reconciliation_breakdown["realized_pnl_eur"].sum()),
            "delta_vs_baseline_expected_eur": float(
                result.reconciliation_breakdown["delta_vs_baseline_expected_eur"].sum()
            ),
            "delta_vs_revised_expected_eur": float(
                result.reconciliation_breakdown["delta_vs_revised_expected_eur"].sum()
            ),
            "forecast_error_eur": float(result.reconciliation_breakdown["forecast_error_eur"].sum()),
            "locked_commitment_opportunity_cost_eur": float(
                result.reconciliation_breakdown["locked_commitment_opportunity_cost_eur"].sum()
            ),
            "reserve_headroom_opportunity_cost_eur": float(
                result.reconciliation_breakdown["reserve_headroom_opportunity_cost_eur"].sum()
            ),
            "degradation_cost_drift_eur": float(result.reconciliation_breakdown["degradation_cost_drift_eur"].sum()),
            "availability_deviation_eur": float(result.reconciliation_breakdown["availability_deviation_eur"].sum()),
            "imbalance_settlement_deviation_eur": float(
                result.reconciliation_breakdown["imbalance_settlement_deviation_eur"].sum()
            ),
            "activation_settlement_deviation_eur": float(
                result.reconciliation_breakdown["activation_settlement_deviation_eur"].sum()
            )
            if "activation_settlement_deviation_eur" in result.reconciliation_breakdown.columns
            else 0.0,
        }
        if scenario_analysis is not None:
            reconciliation_summary["scenario_analysis"] = scenario_analysis
        summary["revision"] = {
            "checkpoint_count": len(config.revision.revision_checkpoints_local) if config.revision is not None else 0,
            "max_revision_horizon_intervals": config.revision.max_revision_horizon_intervals
            if config.revision is not None
            else 0,
            "lock_policy": config.revision.lock_policy if config.revision is not None else None,
        }
        summary["reconciliation"] = reconciliation_summary
    if result.oracle is not None:
        summary["oracle_reference"] = result.oracle.model_dump()
        summary["oracle_gap_total_pnl_eur"] = result.pnl.total_pnl_eur - result.oracle.total_pnl_eur
        summary["oracle_gap_da_revenue_eur"] = result.pnl.da_revenue_eur - result.oracle.da_revenue_eur
        summary["oracle_gap_imbalance_revenue_eur"] = (
            result.pnl.imbalance_revenue_eur - result.oracle.imbalance_revenue_eur
        )
        summary["oracle_gap_reserve_capacity_revenue_eur"] = (
            result.pnl.reserve_capacity_revenue_eur - result.oracle.reserve_capacity_revenue_eur
        )
        summary["oracle_gap_reserve_activation_revenue_eur"] = (
            result.pnl.reserve_activation_revenue_eur - result.oracle.reserve_activation_revenue_eur
        )
        summary["oracle_gap_reserve_penalty_eur"] = result.pnl.reserve_penalty_eur - result.oracle.reserve_penalty_eur
        summary["oracle_gap_degradation_cost_eur"] = (
            result.pnl.degradation_cost_eur - result.oracle.degradation_cost_eur
        )
    return summary


class WalkForwardEngine:
    def __init__(self, config: BacktestConfig, *, benchmark: BenchmarkDefinition, provider) -> None:
        self.config = config
        self.benchmark = benchmark
        self.provider = provider
        self.adapter = MarketRegistry.get(config.market.id)

    def run(self) -> RunResult:
        self.adapter.validate_timing(self.config)
        execution_config = _execution_config(self.config)
        actuals = self.adapter.load_actuals(self.config)
        all_day_ahead = _validate_market_frame(
            actuals.day_ahead.data, market_name="day_ahead", timezone=self.adapter.timezone
        )
        all_imbalance = None
        all_fcr = None
        all_afrr_capacity_up = None
        all_afrr_capacity_down = None
        all_afrr_activation_price_up = None
        all_afrr_activation_price_down = None
        all_afrr_activation_ratio_up = None
        all_afrr_activation_ratio_down = None
        if actuals.imbalance is not None:
            all_imbalance = _validate_market_frame(
                actuals.imbalance.data, market_name="imbalance", timezone=self.adapter.timezone
            )
        if actuals.fcr_capacity is not None:
            all_fcr = _validate_market_frame(
                actuals.fcr_capacity.data, market_name="fcr_capacity", timezone=self.adapter.timezone
            )
        if actuals.afrr_capacity_up is not None:
            all_afrr_capacity_up = _validate_market_frame(
                actuals.afrr_capacity_up.data, market_name="afrr_capacity_up", timezone=self.adapter.timezone
            )
        if actuals.afrr_capacity_down is not None:
            all_afrr_capacity_down = _validate_market_frame(
                actuals.afrr_capacity_down.data, market_name="afrr_capacity_down", timezone=self.adapter.timezone
            )
        if actuals.afrr_activation_price_up is not None:
            all_afrr_activation_price_up = _validate_market_frame(
                actuals.afrr_activation_price_up.data,
                market_name="afrr_activation_price_up",
                timezone=self.adapter.timezone,
            )
        if actuals.afrr_activation_price_down is not None:
            all_afrr_activation_price_down = _validate_market_frame(
                actuals.afrr_activation_price_down.data,
                market_name="afrr_activation_price_down",
                timezone=self.adapter.timezone,
            )
        if actuals.afrr_activation_ratio_up is not None:
            all_afrr_activation_ratio_up = _validate_market_frame(
                actuals.afrr_activation_ratio_up.data,
                market_name="afrr_activation_ratio_up",
                timezone=self.adapter.timezone,
            )
        if actuals.afrr_activation_ratio_down is not None:
            all_afrr_activation_ratio_down = _validate_market_frame(
                actuals.afrr_activation_ratio_down.data,
                market_name="afrr_activation_ratio_down",
                timezone=self.adapter.timezone,
            )

        eval_day_ahead = _filter_evaluation_window(all_day_ahead, self.config)
        _validate_evaluation_coverage(eval_day_ahead, self.config, market_name="day_ahead")
        eval_imbalance = None
        if all_imbalance is not None:
            eval_imbalance = _filter_evaluation_window(all_imbalance, self.config)
            _validate_evaluation_coverage(eval_imbalance, self.config, market_name="imbalance")
        eval_fcr = None
        if all_fcr is not None:
            eval_fcr = _filter_evaluation_window(all_fcr, self.config)
            _validate_evaluation_coverage(eval_fcr, self.config, market_name="fcr_capacity")
        eval_afrr_capacity_up = None
        eval_afrr_capacity_down = None
        eval_afrr_activation_price_up = None
        eval_afrr_activation_price_down = None
        eval_afrr_activation_ratio_up = None
        eval_afrr_activation_ratio_down = None
        if all_afrr_capacity_up is not None:
            eval_afrr_capacity_up = _filter_evaluation_window(all_afrr_capacity_up, self.config)
            _validate_evaluation_coverage(eval_afrr_capacity_up, self.config, market_name="afrr_capacity_up")
        if all_afrr_capacity_down is not None:
            eval_afrr_capacity_down = _filter_evaluation_window(all_afrr_capacity_down, self.config)
            _validate_evaluation_coverage(eval_afrr_capacity_down, self.config, market_name="afrr_capacity_down")
        if all_afrr_activation_price_up is not None:
            eval_afrr_activation_price_up = _filter_evaluation_window(all_afrr_activation_price_up, self.config)
            _validate_evaluation_coverage(
                eval_afrr_activation_price_up, self.config, market_name="afrr_activation_price_up"
            )
        if all_afrr_activation_price_down is not None:
            eval_afrr_activation_price_down = _filter_evaluation_window(all_afrr_activation_price_down, self.config)
            _validate_evaluation_coverage(
                eval_afrr_activation_price_down, self.config, market_name="afrr_activation_price_down"
            )
        if all_afrr_activation_ratio_up is not None:
            eval_afrr_activation_ratio_up = _filter_evaluation_window(all_afrr_activation_ratio_up, self.config)
            _validate_evaluation_coverage(
                eval_afrr_activation_ratio_up, self.config, market_name="afrr_activation_ratio_up"
            )
        if all_afrr_activation_ratio_down is not None:
            eval_afrr_activation_ratio_down = _filter_evaluation_window(all_afrr_activation_ratio_down, self.config)
            _validate_evaluation_coverage(
                eval_afrr_activation_ratio_down, self.config, market_name="afrr_activation_ratio_down"
            )

        schedule = self.adapter.decision_schedule(self.config)
        site_frames: list[pd.DataFrame] = []
        asset_frames: list[pd.DataFrame] = []
        decision_rows: list[dict[str, object]] = []
        snapshot_frames: list[pd.DataFrame] = []
        baseline_schedule_frames: list[pd.DataFrame] = []
        revision_schedule_frames: list[pd.DataFrame] = []
        schedule_lineage_frames: list[pd.DataFrame] = []
        reconciliation_frames: list[pd.DataFrame] = []
        for delivery_date in _delivery_dates(self.config):
            daily = _run_daily_walk_forward(
                config=self.config,
                adapter=self.adapter,
                benchmark=self.benchmark,
                provider=self.provider,
                delivery_date=delivery_date,
                day_ahead_actual=eval_day_ahead,
                imbalance_actual=eval_imbalance,
                fcr_actual=eval_fcr,
                afrr_capacity_up_actual=eval_afrr_capacity_up,
                afrr_capacity_down_actual=eval_afrr_capacity_down,
                afrr_activation_price_up_actual=eval_afrr_activation_price_up,
                afrr_activation_price_down_actual=eval_afrr_activation_price_down,
                afrr_activation_ratio_up_actual=eval_afrr_activation_ratio_up,
                afrr_activation_ratio_down_actual=eval_afrr_activation_ratio_down,
                all_day_ahead_actual=all_day_ahead,
                all_imbalance_actual=all_imbalance,
                all_fcr_actual=all_fcr,
                all_afrr_capacity_up_actual=all_afrr_capacity_up,
                all_afrr_capacity_down_actual=all_afrr_capacity_down,
                all_afrr_activation_price_up_actual=all_afrr_activation_price_up,
                all_afrr_activation_price_down_actual=all_afrr_activation_price_down,
                all_afrr_activation_ratio_up_actual=all_afrr_activation_ratio_up,
                all_afrr_activation_ratio_down_actual=all_afrr_activation_ratio_down,
                schedule=schedule,
            )
            site_frames.append(daily.site_dispatch)
            asset_frames.append(daily.asset_dispatch)
            decision_rows.extend(daily.decisions)
            snapshot_frames.extend(daily.snapshots)
            if daily.baseline_schedule is not None:
                baseline_schedule_frames.append(daily.baseline_schedule)
            if daily.revision_schedule is not None:
                revision_schedule_frames.append(daily.revision_schedule)
            if daily.schedule_lineage is not None:
                schedule_lineage_frames.append(daily.schedule_lineage)
            if daily.reconciliation_breakdown is not None:
                reconciliation_frames.append(daily.reconciliation_breakdown)

        site_dispatch = pd.concat(site_frames, ignore_index=True) if site_frames else pd.DataFrame()
        asset_dispatch = pd.concat(asset_frames, ignore_index=True) if asset_frames else pd.DataFrame()
        site_dispatch = _ensure_dispatch_columns(
            site_dispatch, site_id=self.config.site.id, run_scope=self.config.run_scope
        )
        asset_dispatch = _ensure_dispatch_columns(
            asset_dispatch, site_id=self.config.site.id, run_scope=self.config.run_scope
        )

        site_dispatch, pnl = _site_interval_settlement(
            site_dispatch,
            workflow=self.config.execution_workflow,
            degradation_cost_eur_per_mwh=0.0,
            settlement_engine=self.adapter.settlement_engine(self.config.execution_workflow),
            reserve_penalty_eur_per_mw=float(self.config.fcr.non_delivery_penalty_eur_per_mw)
            if self.config.fcr is not None
            else float(self.config.afrr.non_delivery_penalty_eur_per_mw)
            if self.config.afrr is not None
            else 0.0,
        )
        asset_dispatch, asset_pnl = _asset_settlement(
            asset_dispatch,
            workflow=self.config.execution_workflow,
            degradation_costs_eur_per_mwh=_asset_degradation_costs(execution_config),
            reserve_penalty_eur_per_mw=float(self.config.fcr.non_delivery_penalty_eur_per_mw)
            if self.config.fcr is not None
            else float(self.config.afrr.non_delivery_penalty_eur_per_mw)
            if self.config.afrr is not None
            else 0.0,
        )
        # Replace site-level degradation with asset-summed degradation so portfolio totals remain additive.
        site_dispatch["degradation_cost_eur"] = (
            asset_dispatch.groupby("timestamp_utc")["degradation_cost_eur"]
            .sum()
            .reindex(site_dispatch["timestamp_utc"])
            .values
        )
        site_dispatch["realized_pnl_eur"] = (
            site_dispatch["da_revenue_eur"]
            + site_dispatch["imbalance_revenue_eur"]
            + site_dispatch["reserve_capacity_revenue_eur"]
            + site_dispatch["reserve_activation_revenue_eur"]
            - site_dispatch["reserve_penalty_eur"]
            - site_dispatch["degradation_cost_eur"]
        )
        site_dispatch["expected_pnl_eur"] = (
            site_dispatch["expected_da_revenue_eur"]
            + site_dispatch["expected_imbalance_revenue_eur"]
            + site_dispatch["expected_reserve_capacity_revenue_eur"]
            + site_dispatch["expected_reserve_activation_revenue_eur"]
            - site_dispatch["reserve_penalty_eur"]
            - site_dispatch["degradation_cost_eur"]
        )
        pnl.degradation_cost_eur = float(asset_dispatch["degradation_cost_eur"].sum())
        pnl.total_pnl_eur = float(site_dispatch["realized_pnl_eur"].sum())
        pnl.expected_total_pnl_eur = float(site_dispatch["expected_pnl_eur"].sum())

        settlement_breakdown = site_dispatch[
            [
                "timestamp_utc",
                "site_id",
                "market_id",
                "workflow_family",
                "run_scope",
                "da_revenue_eur",
                "imbalance_revenue_eur",
                "reserve_capacity_revenue_eur",
                "reserve_activation_revenue_eur",
                "reserve_penalty_eur",
                "degradation_cost_eur",
                "realized_pnl_eur",
                "expected_pnl_eur",
            ]
        ].copy()
        decision_log = pd.DataFrame(decision_rows)
        if "schedule_version" not in decision_log.columns:
            decision_log["schedule_version"] = "baseline"
        forecast_snapshots = pd.concat(snapshot_frames, ignore_index=True) if snapshot_frames else pd.DataFrame()
        baseline_schedule = pd.concat(baseline_schedule_frames, ignore_index=True) if baseline_schedule_frames else None
        revision_schedule = pd.concat(revision_schedule_frames, ignore_index=True) if revision_schedule_frames else None
        schedule_lineage = pd.concat(schedule_lineage_frames, ignore_index=True) if schedule_lineage_frames else None
        reconciliation_breakdown = (
            pd.concat(reconciliation_frames, ignore_index=True) if reconciliation_frames else None
        )
        oracle_benchmark = BenchmarkRegistry.resolve(
            self.config.market.id,
            self.config.execution_workflow,
            "perfect_foresight",
            run_scope=self.config.run_scope,
            benchmark_suffix="baseline" if self.config.is_revision_workflow else None,
        )
        oracle = _oracle_reference(
            config=execution_config,
            benchmark=oracle_benchmark,
            adapter=self.adapter,
            day_ahead_actual=eval_day_ahead,
            imbalance_actual=eval_imbalance,
            fcr_actual=eval_fcr,
            afrr_capacity_up_actual=eval_afrr_capacity_up,
            afrr_capacity_down_actual=eval_afrr_capacity_down,
            afrr_activation_price_up_actual=eval_afrr_activation_price_up,
            afrr_activation_price_down_actual=eval_afrr_activation_price_down,
            afrr_activation_ratio_up_actual=eval_afrr_activation_ratio_up,
            afrr_activation_ratio_down_actual=eval_afrr_activation_ratio_down,
        )
        run_id = make_run_id(self.config.run_name)
        result = RunResult(
            run_id=run_id,
            market_id=self.adapter.market_id,
            market_timezone=self.adapter.timezone,
            workflow=self.config.workflow,
            workflow_family=self.config.execution_workflow,
            benchmark_name=self.benchmark.benchmark_name,
            benchmark_family=self.benchmark.benchmark_family,
            provider_name=self.provider.name,
            auditable=self.benchmark.auditable,
            run_scope=self.config.run_scope,
            site_id=self.config.site.id,
            asset_count=len(self.config.assets),
            site_dispatch=site_dispatch,
            asset_dispatch=asset_dispatch,
            asset_pnl_attribution=asset_pnl,
            decision_log=decision_log,
            forecast_snapshots=forecast_snapshots,
            settlement_breakdown=settlement_breakdown,
            baseline_schedule=baseline_schedule,
            revision_schedule=revision_schedule,
            schedule_lineage=schedule_lineage,
            reconciliation_breakdown=reconciliation_breakdown,
            pnl=pnl,
            oracle=oracle,
            metadata={
                "degradation_mode": self.config.degradation.mode,
                "base_workflow": self.config.execution_workflow,
            },
        )
        summary = _build_summary(result=result, config=self.config, adapter=self.adapter, actuals=actuals)
        if "reconciliation" in summary:
            result.reconciliation_summary = summary["reconciliation"]  # type: ignore[assignment]
        run_dir = write_run_artifacts(
            config=self.config,
            run_id=run_id,
            result=result,
            summary=summary,
            day_ahead=actuals.day_ahead,
            imbalance=actuals.imbalance,
            fcr_capacity=actuals.fcr_capacity,
            afrr_capacity_up=actuals.afrr_capacity_up,
            afrr_capacity_down=actuals.afrr_capacity_down,
            afrr_activation_price_up=actuals.afrr_activation_price_up,
            afrr_activation_price_down=actuals.afrr_activation_price_down,
            afrr_activation_ratio_up=actuals.afrr_activation_ratio_up,
            afrr_activation_ratio_down=actuals.afrr_activation_ratio_down,
        )
        result.output_dir = run_dir
        return result


def run_walk_forward(
    config: BacktestConfig | str | Path, *, forecast_provider_override: str | None = None
) -> RunResult:
    resolved_config = load_config(config) if isinstance(config, (str, Path)) else config
    if forecast_provider_override is not None:
        payload = resolved_config.model_dump(mode="json")
        payload["forecast_provider"]["name"] = forecast_provider_override
        resolved_config = BacktestConfig.model_validate(payload)
    benchmark = BenchmarkRegistry.resolve(
        resolved_config.market.id,
        resolved_config.execution_workflow,
        resolved_config.forecast_provider.name,
        run_scope=resolved_config.run_scope,
        benchmark_suffix="revision" if resolved_config.is_revision_workflow else None,
    )
    provider = BenchmarkRegistry.build_provider(resolved_config)
    return WalkForwardEngine(resolved_config, benchmark=benchmark, provider=provider).run()


def run_backtest(config: BacktestConfig | str | Path, *, forecast_provider_override: str | None = None) -> RunResult:
    return run_walk_forward(config, forecast_provider_override=forecast_provider_override)
