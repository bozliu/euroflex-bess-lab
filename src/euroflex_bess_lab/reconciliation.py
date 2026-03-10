from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from .backtesting.engine import (
    _attach_optional_imbalance_columns,
    _ensure_dispatch_columns,
    _merge_market_price,
    _reserve_penalty_eur_per_mw,
    _risk_preference,
    _scenario_analysis,
    _site_interval_settlement,
)
from .config import BacktestConfig, MarketSeriesInput, RevisionRealizedInputsConfig
from .contracts import validate_reconciliation_summary_payload
from .data.io import save_json
from .markets import MarketRegistry


def _load_config_snapshot(run_dir: Path) -> BacktestConfig:
    with (run_dir / "config_snapshot.json").open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return BacktestConfig.model_validate(payload)


def _resolve_realized_inputs(path: Path, config: BacktestConfig) -> RevisionRealizedInputsConfig:
    if path.is_dir():
        candidates = RevisionRealizedInputsConfig(
            day_ahead=MarketSeriesInput(actual_path=path / "day_ahead.parquet")
            if (path / "day_ahead.parquet").exists()
            else None,
            imbalance=MarketSeriesInput(actual_path=path / "imbalance.parquet")
            if (path / "imbalance.parquet").exists()
            else None,
            fcr_capacity=MarketSeriesInput(actual_path=path / "fcr_capacity.parquet")
            if (path / "fcr_capacity.parquet").exists()
            else None,
            afrr_capacity_up=MarketSeriesInput(actual_path=path / "afrr_capacity_up.parquet")
            if (path / "afrr_capacity_up.parquet").exists()
            else None,
            afrr_capacity_down=MarketSeriesInput(actual_path=path / "afrr_capacity_down.parquet")
            if (path / "afrr_capacity_down.parquet").exists()
            else None,
            afrr_activation_price_up=MarketSeriesInput(actual_path=path / "afrr_activation_price_up.parquet")
            if (path / "afrr_activation_price_up.parquet").exists()
            else None,
            afrr_activation_price_down=MarketSeriesInput(actual_path=path / "afrr_activation_price_down.parquet")
            if (path / "afrr_activation_price_down.parquet").exists()
            else None,
            afrr_activation_ratio_up=MarketSeriesInput(actual_path=path / "afrr_activation_ratio_up.parquet")
            if (path / "afrr_activation_ratio_up.parquet").exists()
            else None,
            afrr_activation_ratio_down=MarketSeriesInput(actual_path=path / "afrr_activation_ratio_down.parquet")
            if (path / "afrr_activation_ratio_down.parquet").exists()
            else None,
        )
        return candidates

    with path.open("r", encoding="utf-8") as handle:
        if path.suffix.lower() in {".yaml", ".yml"}:
            payload = yaml.safe_load(handle)
        else:
            payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("Realized input file must be a mapping-like JSON/YAML document")
    candidate: Any
    if (
        "revision" in payload
        and isinstance(payload["revision"], dict)
        and payload["revision"].get("realized_inputs") is not None
    ):
        candidate = payload["revision"]["realized_inputs"]
    elif "data" in payload:
        candidate = payload["data"]
    else:
        candidate = payload
    resolved = RevisionRealizedInputsConfig.model_validate(candidate)
    base_dir = path.parent
    for series in (
        resolved.day_ahead,
        resolved.imbalance,
        resolved.fcr_capacity,
        resolved.afrr_capacity_up,
        resolved.afrr_capacity_down,
        resolved.afrr_activation_price_up,
        resolved.afrr_activation_price_down,
        resolved.afrr_activation_ratio_up,
        resolved.afrr_activation_ratio_down,
    ):
        if series is not None and not series.actual_path.is_absolute():
            series.actual_path = (base_dir / series.actual_path).resolve()
    if resolved.day_ahead is None:
        resolved.day_ahead = config.data.day_ahead
    if resolved.imbalance is None and config.data.imbalance is not None:
        resolved.imbalance = config.data.imbalance
    if resolved.fcr_capacity is None and config.data.fcr_capacity is not None:
        resolved.fcr_capacity = config.data.fcr_capacity
    for field_name in (
        "afrr_capacity_up",
        "afrr_capacity_down",
        "afrr_activation_price_up",
        "afrr_activation_price_down",
        "afrr_activation_ratio_up",
        "afrr_activation_ratio_down",
    ):
        if getattr(resolved, field_name) is None and getattr(config.data, field_name) is not None:
            setattr(resolved, field_name, getattr(config.data, field_name))
    return resolved


def _load_schedule_artifact(path: Path, fallback: pd.DataFrame | None = None) -> pd.DataFrame:
    if path.exists():
        return pd.read_parquet(path)
    if fallback is not None:
        return fallback.copy()
    raise FileNotFoundError(path)


def _load_run_artifacts(
    run_dir: Path,
) -> tuple[dict[str, Any], BacktestConfig, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    with (run_dir / "summary.json").open("r", encoding="utf-8") as handle:
        summary = json.load(handle)
    config = _load_config_snapshot(run_dir)
    site_dispatch = pd.read_parquet(run_dir / "site_dispatch.parquet")
    forecast_snapshots = pd.read_parquet(run_dir / "forecast_snapshots.parquet")
    baseline = _load_schedule_artifact(run_dir / "baseline_schedule.parquet", fallback=site_dispatch)
    revision = _load_schedule_artifact(run_dir / "revision_schedule.parquet", fallback=site_dispatch)
    return summary, config, site_dispatch, baseline, revision, forecast_snapshots


def _apply_actuals(
    frame: pd.DataFrame,
    *,
    adapter,
    config: BacktestConfig,
    day_ahead_actual: pd.DataFrame,
    imbalance_actual: pd.DataFrame | None,
    fcr_actual: pd.DataFrame | None,
    afrr_actuals: dict[str, pd.DataFrame | None],
) -> pd.DataFrame:
    execution_workflow = config.execution_workflow
    resolved = frame.copy()
    resolved = _ensure_dispatch_columns(resolved, site_id=config.site.id, run_scope=config.run_scope)
    resolved["market_id"] = adapter.market_id
    resolved["workflow_family"] = execution_workflow
    for column in (
        "day_ahead_actual_price_eur_per_mwh",
        "imbalance_actual_price_eur_per_mwh",
        "imbalance_shortage_price_eur_per_mwh",
        "imbalance_surplus_price_eur_per_mwh",
        "dispatch_up_price_eur_per_mwh",
        "dispatch_down_price_eur_per_mwh",
        "regulation_state",
        "regulating_condition",
        "fcr_capacity_price_actual_eur_per_mw_per_h",
        "afrr_capacity_up_price_actual_eur_per_mw_per_h",
        "afrr_capacity_down_price_actual_eur_per_mw_per_h",
        "afrr_activation_price_up_actual_eur_per_mwh",
        "afrr_activation_price_down_actual_eur_per_mwh",
        "afrr_activation_ratio_up_actual",
        "afrr_activation_ratio_down_actual",
    ):
        if column in resolved.columns:
            resolved = resolved.drop(columns=[column])
    resolved = _merge_market_price(resolved, day_ahead_actual, target_column="day_ahead_actual_price_eur_per_mwh")
    resolved = _attach_optional_imbalance_columns(resolved, imbalance_actual)
    if fcr_actual is not None:
        resolved = _merge_market_price(resolved, fcr_actual, target_column="fcr_capacity_price_actual_eur_per_mw_per_h")
    else:
        resolved["fcr_capacity_price_actual_eur_per_mw_per_h"] = 0.0
    afrr_mapping = {
        "afrr_capacity_up": "afrr_capacity_up_price_actual_eur_per_mw_per_h",
        "afrr_capacity_down": "afrr_capacity_down_price_actual_eur_per_mw_per_h",
        "afrr_activation_price_up": "afrr_activation_price_up_actual_eur_per_mwh",
        "afrr_activation_price_down": "afrr_activation_price_down_actual_eur_per_mwh",
        "afrr_activation_ratio_up": "afrr_activation_ratio_up_actual",
        "afrr_activation_ratio_down": "afrr_activation_ratio_down_actual",
    }
    for market_name, target_column in afrr_mapping.items():
        actual_frame = afrr_actuals.get(market_name)
        if actual_frame is not None:
            resolved = _merge_market_price(resolved, actual_frame, target_column=target_column)
        else:
            resolved[target_column] = 0.0
    return resolved


def _reconciliation_breakdown(
    *,
    baseline: pd.DataFrame,
    revision: pd.DataFrame,
    config: BacktestConfig,
    adapter,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    execution_workflow = config.execution_workflow
    reserve_penalty = float(config.fcr.non_delivery_penalty_eur_per_mw) if config.fcr is not None else 0.0
    settlement_engine = adapter.settlement_engine(execution_workflow)
    baseline_settled, _ = _site_interval_settlement(
        baseline,
        workflow=execution_workflow,
        degradation_cost_eur_per_mwh=0.0,
        settlement_engine=settlement_engine,
        reserve_penalty_eur_per_mw=reserve_penalty,
    )
    revision_settled, _ = _site_interval_settlement(
        revision,
        workflow=execution_workflow,
        degradation_cost_eur_per_mwh=0.0,
        settlement_engine=settlement_engine,
        reserve_penalty_eur_per_mw=reserve_penalty,
    )
    breakdown = pd.DataFrame(
        {
            "timestamp_utc": revision_settled["timestamp_utc"],
            "site_id": config.site.id,
            "market_id": adapter.market_id,
            "workflow_family": execution_workflow,
            "run_scope": config.run_scope,
            "baseline_expected_pnl_eur": baseline_settled["expected_pnl_eur"].values,
            "revised_expected_pnl_eur": revision_settled["expected_pnl_eur"].values,
            "realized_pnl_eur": revision_settled["realized_pnl_eur"].values,
        }
    )
    breakdown["locked_commitment_opportunity_cost_eur"] = (
        breakdown["revised_expected_pnl_eur"] - breakdown["baseline_expected_pnl_eur"]
    )
    if execution_workflow == "da_plus_imbalance":
        breakdown["imbalance_settlement_deviation_eur"] = (
            revision_settled["imbalance_revenue_eur"] - revision_settled["expected_imbalance_revenue_eur"]
        )
    else:
        breakdown["imbalance_settlement_deviation_eur"] = 0.0
    if execution_workflow == "da_plus_afrr":
        breakdown["activation_settlement_deviation_eur"] = (
            revision_settled["reserve_activation_revenue_eur"]
            - revision_settled["expected_reserve_activation_revenue_eur"]
        )
    else:
        breakdown["activation_settlement_deviation_eur"] = 0.0
    breakdown["reserve_headroom_opportunity_cost_eur"] = 0.0
    breakdown["degradation_cost_drift_eur"] = 0.0
    breakdown["availability_deviation_eur"] = 0.0
    breakdown["forecast_error_eur"] = (
        breakdown["realized_pnl_eur"]
        - breakdown["revised_expected_pnl_eur"]
        - breakdown["imbalance_settlement_deviation_eur"]
        - breakdown["activation_settlement_deviation_eur"]
    )
    breakdown["delta_vs_baseline_expected_eur"] = breakdown["realized_pnl_eur"] - breakdown["baseline_expected_pnl_eur"]
    breakdown["delta_vs_revised_expected_eur"] = breakdown["realized_pnl_eur"] - breakdown["revised_expected_pnl_eur"]
    summary = {
        "baseline_expected_total_pnl_eur": float(breakdown["baseline_expected_pnl_eur"].sum()),
        "revised_expected_total_pnl_eur": float(breakdown["revised_expected_pnl_eur"].sum()),
        "realized_total_pnl_eur": float(breakdown["realized_pnl_eur"].sum()),
        "delta_vs_baseline_expected_eur": float(breakdown["delta_vs_baseline_expected_eur"].sum()),
        "delta_vs_revised_expected_eur": float(breakdown["delta_vs_revised_expected_eur"].sum()),
        "forecast_error_eur": float(breakdown["forecast_error_eur"].sum()),
        "locked_commitment_opportunity_cost_eur": float(breakdown["locked_commitment_opportunity_cost_eur"].sum()),
        "reserve_headroom_opportunity_cost_eur": float(breakdown["reserve_headroom_opportunity_cost_eur"].sum()),
        "degradation_cost_drift_eur": float(breakdown["degradation_cost_drift_eur"].sum()),
        "availability_deviation_eur": float(breakdown["availability_deviation_eur"].sum()),
        "imbalance_settlement_deviation_eur": float(breakdown["imbalance_settlement_deviation_eur"].sum()),
        "activation_settlement_deviation_eur": float(breakdown["activation_settlement_deviation_eur"].sum()),
    }
    return breakdown, summary


def reconcile_run(
    run_dir: str | Path,
    realized_input_path: str | Path,
    *,
    output_dir: str | Path | None = None,
) -> Path:
    target = Path(run_dir).resolve()
    realized_path = Path(realized_input_path).resolve()
    summary, config, _, baseline, revision, forecast_snapshots = _load_run_artifacts(target)
    adapter = MarketRegistry.get(config.market.id)
    realized = _resolve_realized_inputs(realized_path, config)
    day_ahead = adapter.load_input_series(
        path=realized.day_ahead.actual_path if realized.day_ahead is not None else config.data.day_ahead.actual_path,
        name="reconciliation_day_ahead",
        market="day_ahead",
        zone=adapter.day_ahead_zone,
    ).data
    imbalance = None
    if realized.imbalance is not None:
        imbalance = adapter.load_input_series(
            path=realized.imbalance.actual_path,
            name="reconciliation_imbalance",
            market="imbalance",
            zone=adapter.imbalance_zone,
        ).data
    elif config.data.imbalance is not None:
        imbalance = adapter.load_input_series(
            path=config.data.imbalance.actual_path,
            name="reconciliation_imbalance",
            market="imbalance",
            zone=adapter.imbalance_zone,
        ).data
    fcr = None
    if realized.fcr_capacity is not None:
        fcr = adapter.load_input_series(
            path=realized.fcr_capacity.actual_path,
            name="reconciliation_fcr",
            market="fcr_capacity",
            zone=adapter.fcr_zone or adapter.day_ahead_zone,
        ).data
    elif config.data.fcr_capacity is not None:
        fcr = adapter.load_input_series(
            path=config.data.fcr_capacity.actual_path,
            name="reconciliation_fcr",
            market="fcr_capacity",
            zone=adapter.fcr_zone or adapter.day_ahead_zone,
        ).data
    afrr_actuals: dict[str, pd.DataFrame | None] = {}
    afrr_zone = adapter.afrr_zone or adapter.day_ahead_zone
    for field_name in (
        "afrr_capacity_up",
        "afrr_capacity_down",
        "afrr_activation_price_up",
        "afrr_activation_price_down",
        "afrr_activation_ratio_up",
        "afrr_activation_ratio_down",
    ):
        realized_series = getattr(realized, field_name)
        config_series = getattr(config.data, field_name)
        path_like = (
            realized_series.actual_path
            if realized_series is not None
            else config_series.actual_path
            if config_series is not None
            else None
        )
        if path_like is None:
            afrr_actuals[field_name] = None
            continue
        afrr_actuals[field_name] = adapter.load_input_series(
            path=path_like,
            name=f"reconciliation_{field_name}",
            market=field_name,
            zone=afrr_zone,
        ).data
    baseline = _apply_actuals(
        baseline,
        adapter=adapter,
        config=config,
        day_ahead_actual=day_ahead,
        imbalance_actual=imbalance,
        fcr_actual=fcr,
        afrr_actuals=afrr_actuals,
    )
    revision = _apply_actuals(
        revision,
        adapter=adapter,
        config=config,
        day_ahead_actual=day_ahead,
        imbalance_actual=imbalance,
        fcr_actual=fcr,
        afrr_actuals=afrr_actuals,
    )
    breakdown, reconciliation_summary = _reconciliation_breakdown(
        baseline=baseline,
        revision=revision,
        config=config,
        adapter=adapter,
    )
    throughput_total = float(revision["throughput_mwh"].sum()) if "throughput_mwh" in revision.columns else 0.0
    degradation_cost_total = (
        float(revision["degradation_cost_eur"].sum()) if "degradation_cost_eur" in revision.columns else 0.0
    )
    degradation_cost_per_mwh = degradation_cost_total / throughput_total if throughput_total else 0.0
    scenario_analysis = _scenario_analysis(
        revision,
        forecast_snapshots,
        workflow=config.execution_workflow,
        degradation_cost_eur_per_mwh=degradation_cost_per_mwh,
        reserve_penalty_eur_per_mw=_reserve_penalty_eur_per_mw(config, workflow=config.execution_workflow),
        risk=_risk_preference(config),
        settlement_engine=adapter.settlement_engine(config.execution_workflow),
        realized_total_pnl_eur=float(reconciliation_summary["realized_total_pnl_eur"]),
    )
    result_dir = Path(output_dir).resolve() if output_dir is not None else target / "reconciliation"
    result_dir.mkdir(parents=True, exist_ok=True)
    breakdown.to_parquet(result_dir / "reconciliation_breakdown.parquet", index=False)
    breakdown.to_csv(result_dir / "reconciliation_breakdown.csv", index=False)
    payload = {
        **reconciliation_summary,
        "run_id": summary["run_id"],
        "market_id": summary["market_id"],
        "workflow": summary["workflow"],
        "base_workflow": summary.get("base_workflow", summary["workflow"]),
        "source_run_dir": str(target),
        "realized_input_path": str(realized_path),
    }
    if scenario_analysis is not None:
        payload["scenario_analysis"] = scenario_analysis
    validate_reconciliation_summary_payload(payload)
    save_json(payload, result_dir / "reconciliation_summary.json")
    return result_dir
