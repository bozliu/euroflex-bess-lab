from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

from .config import BacktestConfig


class ForecastErrorMetricContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mae: float
    rmse: float
    bias: float


class ForecastErrorContract(BaseModel):
    model_config = ConfigDict(extra="allow")

    day_ahead: ForecastErrorMetricContract
    imbalance: ForecastErrorMetricContract | None = None
    fcr_capacity: ForecastErrorMetricContract | None = None
    afrr_capacity_up: ForecastErrorMetricContract | None = None
    afrr_capacity_down: ForecastErrorMetricContract | None = None
    afrr_activation_price_up: ForecastErrorMetricContract | None = None
    afrr_activation_price_down: ForecastErrorMetricContract | None = None
    afrr_activation_ratio_up: ForecastErrorMetricContract | None = None
    afrr_activation_ratio_down: ForecastErrorMetricContract | None = None


class ScenarioAnalysisContract(BaseModel):
    model_config = ConfigDict(extra="allow")

    forecast_mode: Literal["scenario_bundle"]
    risk_mode: Literal["expected_value", "downside_penalty", "cvar_lite"]
    scenario_count: int
    scenario_expected_total_pnl_eur: float
    scenario_best_total_pnl_eur: float
    scenario_worst_total_pnl_eur: float
    scenario_spread_total_pnl_eur: float
    downside_penalty_contribution_eur: float
    reserve_fragility_eur: float
    scenario_weights: dict[str, float]
    scenario_best_id: str
    scenario_worst_id: str
    nearest_scenario_id: str | None = None
    nearest_scenario_total_pnl_eur: float | None = None
    realized_vs_scenario_envelope_distance_eur: float | None = None
    scenario_posture: Literal["within_envelope", "aggressive", "conservative"] | None = None


class ReconciliationSummaryContract(BaseModel):
    model_config = ConfigDict(extra="allow")

    run_id: str
    market_id: str
    workflow: str
    base_workflow: str
    source_run_dir: str
    realized_input_path: str
    baseline_expected_total_pnl_eur: float
    revised_expected_total_pnl_eur: float
    realized_total_pnl_eur: float
    delta_vs_baseline_expected_eur: float
    delta_vs_revised_expected_eur: float
    forecast_error_eur: float
    locked_commitment_opportunity_cost_eur: float
    reserve_headroom_opportunity_cost_eur: float
    degradation_cost_drift_eur: float
    availability_deviation_eur: float
    imbalance_settlement_deviation_eur: float
    activation_settlement_deviation_eur: float
    scenario_analysis: ScenarioAnalysisContract | None = None


class InlineReconciliationSummaryContract(BaseModel):
    model_config = ConfigDict(extra="allow")

    baseline_expected_total_pnl_eur: float
    revised_expected_total_pnl_eur: float
    realized_total_pnl_eur: float
    delta_vs_baseline_expected_eur: float
    delta_vs_revised_expected_eur: float
    forecast_error_eur: float
    locked_commitment_opportunity_cost_eur: float
    reserve_headroom_opportunity_cost_eur: float
    degradation_cost_drift_eur: float
    availability_deviation_eur: float
    imbalance_settlement_deviation_eur: float
    activation_settlement_deviation_eur: float
    scenario_analysis: ScenarioAnalysisContract | None = None


class SummaryContract(BaseModel):
    model_config = ConfigDict(extra="allow")

    schema_version: Literal[4]
    run_id: str
    site_id: str
    run_scope: Literal["single_asset", "portfolio"]
    asset_count: int
    poi_import_limit_mw: float
    poi_export_limit_mw: float
    market_id: str
    market_timezone: str
    workflow: str
    base_workflow: str
    benchmark_name: str
    benchmark_family: str
    provider_name: str
    forecast_mode: Literal["point", "scenario_bundle"]
    risk_mode: Literal["expected_value", "downside_penalty", "cvar_lite"]
    auditable: bool
    interval_count: int
    decision_count: int
    locked_interval_count: int
    da_revenue_eur: float
    imbalance_revenue_eur: float
    reserve_capacity_revenue_eur: float
    reserve_activation_revenue_eur: float
    reserve_penalty_eur: float
    degradation_cost_eur: float
    total_pnl_eur: float
    expected_total_pnl_eur: float
    throughput_mwh: float
    idle_share: float
    reserve_share_of_total_revenue: float
    energy_revenue_eur: float
    reason_code_counts: dict[str, int]
    asset_contribution_ranking: list[dict[str, Any]]
    data_provenance: dict[str, Any]
    forecast_error: ForecastErrorContract
    scenario_analysis: ScenarioAnalysisContract | None = None
    reconciliation: InlineReconciliationSummaryContract | None = None


class ExportFileContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    path: str
    bytes: int
    sha256: str


class ExportManifestMetadataContract(BaseModel):
    model_config = ConfigDict(extra="allow")

    export_kind: Literal["schedule", "bids", "revision"]
    profile: str
    intended_consumer: str
    benchmark_grade_only: bool
    live_submission_ready: bool
    run_id: str
    site_id: str
    market_id: str
    workflow: str
    run_scope: str
    benchmark_name: str
    market_timezone: str
    generation_time_utc: str
    config_run_name: str


class ExportManifestContract(BaseModel):
    model_config = ConfigDict(extra="allow")

    schema_version: Literal[4]
    created_at_utc: str
    source_run_dir: str
    metadata: ExportManifestMetadataContract
    files: list[ExportFileContract]


def validate_summary_payload(payload: dict[str, Any]) -> SummaryContract:
    return SummaryContract.model_validate(payload)


def validate_export_manifest_payload(payload: dict[str, Any]) -> ExportManifestContract:
    return ExportManifestContract.model_validate(payload)


def validate_reconciliation_summary_payload(payload: dict[str, Any]) -> ReconciliationSummaryContract:
    return ReconciliationSummaryContract.model_validate(payload)


def validate_inline_reconciliation_payload(payload: dict[str, Any]) -> InlineReconciliationSummaryContract:
    return InlineReconciliationSummaryContract.model_validate(payload)


def build_json_schema_bundle() -> dict[str, dict[str, Any]]:
    return {
        "config.v4.json": BacktestConfig.model_json_schema(mode="serialization"),
        "summary.schema.json": SummaryContract.model_json_schema(),
        "export_manifest.schema.json": ExportManifestContract.model_json_schema(),
        "reconciliation_summary.schema.json": ReconciliationSummaryContract.model_json_schema(),
    }


def write_json_schemas(output_dir: str | Path) -> list[Path]:
    target = Path(output_dir)
    target.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for name, schema in build_json_schema_bundle().items():
        path = target / name
        path.write_text(json.dumps(schema, indent=2, sort_keys=True), encoding="utf-8")
        written.append(path)
    return written
