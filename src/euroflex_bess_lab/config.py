from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Literal

import pandas as pd
import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .types import AssetSpec, SiteSpec

MARKETS = ("belgium", "netherlands")
BASE_WORKFLOWS = ("da_only", "da_plus_imbalance", "da_plus_fcr", "da_plus_afrr")
WORKFLOW_FAMILIES = BASE_WORKFLOWS + ("schedule_revision",)
FORECAST_PROVIDERS = ("perfect_foresight", "persistence", "csv", "custom_python")
FORECAST_MODES = ("point", "scenario_bundle")
DEGRADATION_MODES = ("throughput_linear", "equivalent_cycle_linear", "rainflow_offline")
BATCH_FORECAST_MODES = FORECAST_MODES
RISK_MODES = ("expected_value", "downside_penalty", "cvar_lite")
BATCH_STEPS = (
    "validate_config",
    "validate_data",
    "backtest",
    "reconcile",
    "export_schedule",
    "export_bids",
    "export_revision",
)


class MarketConfig(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    id: Literal["belgium", "netherlands"]
    overrides: dict[str, Any] = Field(default_factory=dict)
    live_data_auth_env_var_names: list[str] = Field(default_factory=list)


class MarketSeriesInput(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    actual_path: Path


class DataConfig(BaseModel):
    day_ahead: MarketSeriesInput
    imbalance: MarketSeriesInput | None = None
    fcr_capacity: MarketSeriesInput | None = None
    afrr_capacity_up: MarketSeriesInput | None = None
    afrr_capacity_down: MarketSeriesInput | None = None
    afrr_activation_price_up: MarketSeriesInput | None = None
    afrr_activation_price_down: MarketSeriesInput | None = None
    afrr_activation_ratio_up: MarketSeriesInput | None = None
    afrr_activation_ratio_down: MarketSeriesInput | None = None


class ArtifactConfig(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    root_dir: Path = Path("artifacts")
    save_inputs: bool = True
    save_plots: bool = True
    save_forecast_snapshots: bool = True


class ForecastProviderConfig(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    name: Literal["perfect_foresight", "persistence", "csv", "custom_python"]
    mode: Literal["point", "scenario_bundle"] = "point"
    day_ahead_path: Path | None = None
    imbalance_path: Path | None = None
    fcr_capacity_path: Path | None = None
    afrr_capacity_up_path: Path | None = None
    afrr_capacity_down_path: Path | None = None
    afrr_activation_price_up_path: Path | None = None
    afrr_activation_price_down_path: Path | None = None
    afrr_activation_ratio_up_path: Path | None = None
    afrr_activation_ratio_down_path: Path | None = None
    scenario_id: str | None = None
    module_path: Path | None = None
    class_name: str | None = None
    init_kwargs: dict[str, Any] = Field(default_factory=dict)


class RiskConfig(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    mode: Literal["expected_value", "downside_penalty", "cvar_lite"] = "expected_value"
    penalty_lambda: float = Field(default=0.0, ge=0.0)
    tail_alpha: float | None = Field(default=None, gt=0.0, lt=1.0)

    @model_validator(mode="after")
    def validate_risk_mode(self) -> RiskConfig:
        if self.mode == "cvar_lite" and self.tail_alpha is None:
            raise ValueError("tail_alpha is required when risk.mode is cvar_lite")
        if self.mode != "cvar_lite" and self.tail_alpha is not None:
            raise ValueError("tail_alpha is only valid when risk.mode is cvar_lite")
        return self


class TimingConfig(BaseModel):
    timezone: str
    resolution_minutes: int = Field(default=15, ge=15)
    rebalance_cadence_minutes: int = Field(default=15, ge=15)
    execution_lock_intervals: int = Field(default=1, ge=1)
    day_ahead_gate_closure_local: str = "12:00"
    delivery_start_date: date
    delivery_end_date: date

    @field_validator("resolution_minutes")
    @classmethod
    def validate_resolution(cls, value: int) -> int:
        if value != 15:
            raise ValueError("euroflex_bess_lab currently supports only 15-minute market resolution")
        return value

    @field_validator("rebalance_cadence_minutes")
    @classmethod
    def validate_cadence(cls, value: int) -> int:
        if value % 15 != 0:
            raise ValueError("rebalance_cadence_minutes must be a multiple of 15")
        return value

    @field_validator("day_ahead_gate_closure_local")
    @classmethod
    def validate_gate_closure(cls, value: str) -> str:
        if len(value.split(":")) != 2:
            raise ValueError("day_ahead_gate_closure_local must use HH:MM format")
        return value

    @model_validator(mode="after")
    def validate_window(self) -> TimingConfig:
        if self.delivery_end_date < self.delivery_start_date:
            raise ValueError("delivery_end_date must be on or after delivery_start_date")
        return self


class DegradationConfig(BaseModel):
    mode: Literal["throughput_linear", "equivalent_cycle_linear", "rainflow_offline"] = "throughput_linear"
    throughput_cost_eur_per_mwh: float | None = Field(default=0.0, ge=0.0)
    eur_per_equivalent_cycle: float | None = Field(default=None, ge=0.0)

    @model_validator(mode="after")
    def validate_mode_inputs(self) -> DegradationConfig:
        if self.mode == "throughput_linear" and self.throughput_cost_eur_per_mwh is None:
            raise ValueError("throughput_cost_eur_per_mwh is required for throughput_linear degradation")
        if self.mode == "equivalent_cycle_linear" and self.eur_per_equivalent_cycle is None:
            raise ValueError("eur_per_equivalent_cycle is required for equivalent_cycle_linear degradation")
        return self


class FcrConfig(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    product_id: Literal["fcr_symmetric"] = "fcr_symmetric"
    sustain_duration_minutes: int = Field(default=15, ge=15)
    settlement_mode: Literal["capacity_only"] = "capacity_only"
    activation_mode: Literal["none"] = "none"
    non_delivery_penalty_eur_per_mw: float = Field(default=0.0, ge=0.0)
    simplified_product_logic: Literal[True] = True

    @field_validator("sustain_duration_minutes")
    @classmethod
    def validate_sustain_duration(cls, value: int) -> int:
        if value % 15 != 0:
            raise ValueError("sustain_duration_minutes must be a multiple of 15")
        return value


class AfrrConfig(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    product_id: Literal["afrr_asymmetric"] = "afrr_asymmetric"
    sustain_duration_minutes: int = Field(default=15, ge=15)
    settlement_mode: Literal["capacity_plus_activation_expected_value"] = "capacity_plus_activation_expected_value"
    activation_mode: Literal["expected_value"] = "expected_value"
    non_delivery_penalty_eur_per_mw: float = Field(default=0.0, ge=0.0)
    simplified_product_logic: Literal[True] = True

    @field_validator("sustain_duration_minutes")
    @classmethod
    def validate_sustain_duration(cls, value: int) -> int:
        if value % 15 != 0:
            raise ValueError("sustain_duration_minutes must be a multiple of 15")
        return value


class RevisionRealizedInputsConfig(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    day_ahead: MarketSeriesInput | None = None
    imbalance: MarketSeriesInput | None = None
    fcr_capacity: MarketSeriesInput | None = None
    afrr_capacity_up: MarketSeriesInput | None = None
    afrr_capacity_down: MarketSeriesInput | None = None
    afrr_activation_price_up: MarketSeriesInput | None = None
    afrr_activation_price_down: MarketSeriesInput | None = None
    afrr_activation_ratio_up: MarketSeriesInput | None = None
    afrr_activation_ratio_down: MarketSeriesInput | None = None


class RevisionConfig(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    base_workflow: Literal["da_only", "da_plus_imbalance", "da_plus_fcr", "da_plus_afrr"]
    revision_market_mode: Literal["public_checkpoint_reoptimization"] = "public_checkpoint_reoptimization"
    revision_checkpoints_local: list[str]
    lock_policy: Literal["committed_intervals_only"] = "committed_intervals_only"
    allow_day_ahead_revision: Literal[False] = False
    allow_fcr_revision: bool = False
    allow_afrr_revision: bool = False
    allow_energy_revision: Literal[True] = True
    max_revision_horizon_intervals: int = Field(default=96, ge=1)
    realized_inputs: RevisionRealizedInputsConfig | None = None

    @field_validator("revision_checkpoints_local")
    @classmethod
    def validate_checkpoints(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("revision_checkpoints_local must contain at least one checkpoint")
        normalized: list[str] = []
        for checkpoint in value:
            if len(checkpoint.split(":")) != 2:
                raise ValueError("revision_checkpoints_local entries must use HH:MM format")
            normalized.append(checkpoint)
        if len(set(normalized)) != len(normalized):
            raise ValueError("revision_checkpoints_local entries must be unique")
        return normalized

    @model_validator(mode="after")
    def validate_revision_controls(self) -> RevisionConfig:
        if self.allow_fcr_revision:
            raise ValueError("allow_fcr_revision must remain false in the current release line")
        if self.allow_afrr_revision:
            raise ValueError("allow_afrr_revision must remain false in the current release line")
        checkpoints = [pd.Timestamp(f"2000-01-01 {value}") for value in self.revision_checkpoints_local]
        if checkpoints != sorted(checkpoints):
            raise ValueError("revision_checkpoints_local must be strictly ascending in local clock order")
        return self


class BacktestConfig(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    schema_version: Literal[4]
    run_name: str
    market: MarketConfig
    workflow: Literal["da_only", "da_plus_imbalance", "da_plus_fcr", "da_plus_afrr", "schedule_revision"]
    forecast_provider: ForecastProviderConfig
    timing: TimingConfig
    site: SiteSpec
    assets: list[AssetSpec]
    degradation: DegradationConfig = Field(default_factory=DegradationConfig)
    risk: RiskConfig = Field(default_factory=RiskConfig)
    data: DataConfig
    fcr: FcrConfig | None = None
    afrr: AfrrConfig | None = None
    revision: RevisionConfig | None = None
    artifacts: ArtifactConfig = Field(default_factory=ArtifactConfig)

    @property
    def run_scope(self) -> Literal["single_asset", "portfolio"]:
        return "single_asset" if len(self.assets) == 1 else "portfolio"

    @property
    def primary_asset(self) -> AssetSpec:
        return self.assets[0]

    @property
    def is_revision_workflow(self) -> bool:
        return self.workflow == "schedule_revision"

    @property
    def execution_workflow(self) -> Literal["da_only", "da_plus_imbalance", "da_plus_fcr", "da_plus_afrr"]:
        if self.revision is not None:
            return self.revision.base_workflow
        if self.workflow == "schedule_revision":
            raise ValueError("schedule_revision requires a revision block before resolving execution_workflow")
        return self.workflow

    @model_validator(mode="after")
    def validate_strategy_inputs(self) -> BacktestConfig:
        if not self.assets:
            raise ValueError("assets must contain at least one asset")
        asset_ids = [asset.id for asset in self.assets]
        if len(set(asset_ids)) != len(asset_ids):
            raise ValueError("asset ids must be unique")
        if self.workflow == "schedule_revision" and self.revision is None:
            raise ValueError("revision configuration block is required for schedule_revision")
        if self.workflow != "schedule_revision" and self.revision is not None:
            raise ValueError("revision configuration block is only valid when workflow is schedule_revision")

        execution_workflow = self.execution_workflow
        if self.is_revision_workflow and self.revision is not None:
            if self.revision.base_workflow == "da_plus_imbalance" and self.run_scope == "portfolio":
                raise ValueError("schedule_revision with base_workflow=da_plus_imbalance requires a single asset")
        if execution_workflow == "da_plus_imbalance" and len(self.assets) > 1:
            raise ValueError("Portfolio da_plus_imbalance is out of scope; use a single asset")
        if self.forecast_provider.mode == "scenario_bundle":
            if self.market.id != "belgium":
                raise ValueError(
                    "Scenario forecasting is Belgium-first in the current release line; other markets remain point-only"
                )
            if execution_workflow == "da_plus_imbalance":
                raise ValueError(
                    "Scenario forecasting is not supported for da_plus_imbalance in the current release line"
                )
            if self.forecast_provider.name in {"perfect_foresight", "persistence"}:
                raise ValueError(
                    f"{self.forecast_provider.name} only supports forecast_provider.mode=point in the current release line"
                )
            if self.forecast_provider.scenario_id is not None:
                raise ValueError("scenario_id is only valid for point-mode CSV forecasts, not scenario bundles")
        elif self.risk.mode != "expected_value" or self.risk.penalty_lambda != 0.0 or self.risk.tail_alpha is not None:
            raise ValueError("Risk modes other than expected_value require forecast_provider.mode=scenario_bundle")
        if execution_workflow == "da_plus_imbalance" and self.data.imbalance is None:
            raise ValueError("Imbalance actual input is required for da_plus_imbalance")
        if execution_workflow == "da_plus_fcr":
            if self.data.fcr_capacity is None:
                raise ValueError("FCR capacity actual input is required for da_plus_fcr")
            if self.fcr is None:
                raise ValueError("fcr configuration block is required for da_plus_fcr")
        if execution_workflow == "da_plus_afrr":
            required_inputs = {
                "afrr_capacity_up": self.data.afrr_capacity_up,
                "afrr_capacity_down": self.data.afrr_capacity_down,
                "afrr_activation_price_up": self.data.afrr_activation_price_up,
                "afrr_activation_price_down": self.data.afrr_activation_price_down,
                "afrr_activation_ratio_up": self.data.afrr_activation_ratio_up,
                "afrr_activation_ratio_down": self.data.afrr_activation_ratio_down,
            }
            missing_inputs = [name for name, value in required_inputs.items() if value is None]
            if missing_inputs:
                raise ValueError(
                    "aFRR actual inputs are required for da_plus_afrr: " + ", ".join(sorted(missing_inputs))
                )
            if self.afrr is None:
                raise ValueError("afrr configuration block is required for da_plus_afrr")
        if self.forecast_provider.name == "csv" and self.forecast_provider.day_ahead_path is None:
            raise ValueError("CSV forecast provider requires day_ahead_path")
        if execution_workflow == "da_plus_imbalance" and self.forecast_provider.name == "csv":
            if self.forecast_provider.imbalance_path is None:
                raise ValueError("CSV forecast provider requires imbalance_path for da_plus_imbalance")
        if execution_workflow == "da_plus_fcr" and self.forecast_provider.name == "csv":
            if self.forecast_provider.fcr_capacity_path is None:
                raise ValueError("CSV forecast provider requires fcr_capacity_path for da_plus_fcr")
        if execution_workflow == "da_plus_afrr" and self.forecast_provider.name == "csv":
            csv_requirements = {
                "afrr_capacity_up_path": self.forecast_provider.afrr_capacity_up_path,
                "afrr_capacity_down_path": self.forecast_provider.afrr_capacity_down_path,
                "afrr_activation_price_up_path": self.forecast_provider.afrr_activation_price_up_path,
                "afrr_activation_price_down_path": self.forecast_provider.afrr_activation_price_down_path,
                "afrr_activation_ratio_up_path": self.forecast_provider.afrr_activation_ratio_up_path,
                "afrr_activation_ratio_down_path": self.forecast_provider.afrr_activation_ratio_down_path,
            }
            missing_paths = [name for name, value in csv_requirements.items() if value is None]
            if missing_paths:
                raise ValueError(
                    "CSV forecast provider requires aFRR paths for da_plus_afrr: " + ", ".join(sorted(missing_paths))
                )
        if self.forecast_provider.name == "custom_python":
            if self.forecast_provider.module_path is None:
                raise ValueError("custom_python forecast provider requires module_path")
            if self.forecast_provider.class_name is None:
                raise ValueError("custom_python forecast provider requires class_name")
        return self


class SweepArtifactConfig(BaseModel):
    root_dir: Path = Path("artifacts/sweeps")


class SweepConfig(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    schema_version: Literal[4]
    sweep_name: str
    base_config_path: Path
    matrix: dict[str, list[Any]]
    artifacts: SweepArtifactConfig = Field(default_factory=SweepArtifactConfig)

    @model_validator(mode="after")
    def validate_matrix(self) -> SweepConfig:
        if not self.matrix:
            raise ValueError("matrix must contain at least one override dimension")
        for key, values in self.matrix.items():
            if not values:
                raise ValueError(f"matrix entry {key} must contain at least one value")
        return self


class BatchArtifactConfig(BaseModel):
    root_dir: Path = Path("artifacts/batches")


class BatchJobConfig(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    id: str
    config_path: Path
    market: Literal["belgium", "netherlands"]
    workflow: Literal["da_only", "da_plus_imbalance", "da_plus_fcr", "da_plus_afrr", "schedule_revision"]
    steps: list[
        Literal[
            "validate_config",
            "validate_data",
            "backtest",
            "reconcile",
            "export_schedule",
            "export_bids",
            "export_revision",
        ]
    ]
    forecast_provider: Literal["perfect_foresight", "persistence", "csv", "custom_python"] | None = None
    forecast_mode: Literal["point", "scenario_bundle"] | None = None
    realized_input_path: Path | None = None
    export_schedule_profile: Literal["benchmark", "operator", "submission_candidate"] = "benchmark"
    export_bids_profile: Literal["benchmark", "bid_planning", "submission_candidate"] = "benchmark"
    output_dir: Path | None = None

    @model_validator(mode="after")
    def validate_steps(self) -> BatchJobConfig:
        if not self.steps:
            raise ValueError("Batch job steps must contain at least one operation")
        if "reconcile" in self.steps and self.realized_input_path is None:
            raise ValueError("Batch jobs that include reconcile must define realized_input_path")
        return self


class BatchConfig(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    schema_version: Literal[4]
    batch_name: str
    jobs: list[BatchJobConfig]
    artifacts: BatchArtifactConfig = Field(default_factory=BatchArtifactConfig)

    @model_validator(mode="after")
    def validate_jobs(self) -> BatchConfig:
        if not self.jobs:
            raise ValueError("jobs must contain at least one batch job")
        job_ids = [job.id for job in self.jobs]
        if len(set(job_ids)) != len(job_ids):
            raise ValueError("batch job ids must be unique")
        return self


def _resolve_init_kwargs_paths(values: dict[str, Any], *, base_dir: Path) -> dict[str, Any]:
    resolved: dict[str, Any] = {}
    for key, value in values.items():
        if isinstance(value, dict):
            resolved[key] = _resolve_init_kwargs_paths(value, base_dir=base_dir)
            continue
        if isinstance(value, list):
            items: list[Any] = []
            for item in value:
                if isinstance(item, dict):
                    items.append(_resolve_init_kwargs_paths(item, base_dir=base_dir))
                else:
                    items.append(item)
            resolved[key] = items
            continue
        if isinstance(value, str):
            should_resolve = key.endswith(("_path", "_dir", "_file", "_weights")) or key in {"model_weights"}
            candidate = Path(value)
            if should_resolve and not candidate.is_absolute():
                resolved[key] = str((base_dir / candidate).resolve())
                continue
        resolved[key] = value
    return resolved


def _resolve_paths(config: BacktestConfig, base_dir: Path) -> BacktestConfig:
    if not config.data.day_ahead.actual_path.is_absolute():
        config.data.day_ahead.actual_path = (base_dir / config.data.day_ahead.actual_path).resolve()
    if config.data.imbalance is not None and not config.data.imbalance.actual_path.is_absolute():
        config.data.imbalance.actual_path = (base_dir / config.data.imbalance.actual_path).resolve()
    if config.data.fcr_capacity is not None and not config.data.fcr_capacity.actual_path.is_absolute():
        config.data.fcr_capacity.actual_path = (base_dir / config.data.fcr_capacity.actual_path).resolve()
    for field_name in (
        "afrr_capacity_up",
        "afrr_capacity_down",
        "afrr_activation_price_up",
        "afrr_activation_price_down",
        "afrr_activation_ratio_up",
        "afrr_activation_ratio_down",
    ):
        series = getattr(config.data, field_name)
        if series is not None and not series.actual_path.is_absolute():
            series.actual_path = (base_dir / series.actual_path).resolve()
    if (
        config.forecast_provider.day_ahead_path is not None
        and not config.forecast_provider.day_ahead_path.is_absolute()
    ):
        config.forecast_provider.day_ahead_path = (base_dir / config.forecast_provider.day_ahead_path).resolve()
    if (
        config.forecast_provider.imbalance_path is not None
        and not config.forecast_provider.imbalance_path.is_absolute()
    ):
        config.forecast_provider.imbalance_path = (base_dir / config.forecast_provider.imbalance_path).resolve()
    if (
        config.forecast_provider.fcr_capacity_path is not None
        and not config.forecast_provider.fcr_capacity_path.is_absolute()
    ):
        config.forecast_provider.fcr_capacity_path = (base_dir / config.forecast_provider.fcr_capacity_path).resolve()
    for field_name in (
        "afrr_capacity_up_path",
        "afrr_capacity_down_path",
        "afrr_activation_price_up_path",
        "afrr_activation_price_down_path",
        "afrr_activation_ratio_up_path",
        "afrr_activation_ratio_down_path",
    ):
        series_path = getattr(config.forecast_provider, field_name)
        if series_path is not None and not series_path.is_absolute():
            setattr(config.forecast_provider, field_name, (base_dir / series_path).resolve())
    if config.forecast_provider.module_path is not None and not config.forecast_provider.module_path.is_absolute():
        config.forecast_provider.module_path = (base_dir / config.forecast_provider.module_path).resolve()
    if config.forecast_provider.init_kwargs:
        config.forecast_provider.init_kwargs = _resolve_init_kwargs_paths(
            config.forecast_provider.init_kwargs,
            base_dir=base_dir,
        )
    if config.revision is not None and config.revision.realized_inputs is not None:
        realized = config.revision.realized_inputs
        if realized.day_ahead is not None and not realized.day_ahead.actual_path.is_absolute():
            realized.day_ahead.actual_path = (base_dir / realized.day_ahead.actual_path).resolve()
        if realized.imbalance is not None and not realized.imbalance.actual_path.is_absolute():
            realized.imbalance.actual_path = (base_dir / realized.imbalance.actual_path).resolve()
        if realized.fcr_capacity is not None and not realized.fcr_capacity.actual_path.is_absolute():
            realized.fcr_capacity.actual_path = (base_dir / realized.fcr_capacity.actual_path).resolve()
        for field_name in (
            "afrr_capacity_up",
            "afrr_capacity_down",
            "afrr_activation_price_up",
            "afrr_activation_price_down",
            "afrr_activation_ratio_up",
            "afrr_activation_ratio_down",
        ):
            series = getattr(realized, field_name)
            if series is not None and not series.actual_path.is_absolute():
                series.actual_path = (base_dir / series.actual_path).resolve()
    if not config.artifacts.root_dir.is_absolute():
        config.artifacts.root_dir = (base_dir / config.artifacts.root_dir).resolve()
    return config


def load_config(path: str | Path) -> BacktestConfig:
    config_path = Path(path).resolve()
    with config_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    config = BacktestConfig.model_validate(payload)
    return _resolve_paths(config, config_path.parent)


def load_sweep_config(path: str | Path) -> SweepConfig:
    config_path = Path(path).resolve()
    with config_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    config = SweepConfig.model_validate(payload)
    if not config.base_config_path.is_absolute():
        config.base_config_path = (config_path.parent / config.base_config_path).resolve()
    if not config.artifacts.root_dir.is_absolute():
        config.artifacts.root_dir = (config_path.parent / config.artifacts.root_dir).resolve()
    return config


def load_batch_config(path: str | Path) -> BatchConfig:
    config_path = Path(path).resolve()
    with config_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    config = BatchConfig.model_validate(payload)
    base_dir = config_path.parent
    if not config.artifacts.root_dir.is_absolute():
        config.artifacts.root_dir = (base_dir / config.artifacts.root_dir).resolve()
    for job in config.jobs:
        if not job.config_path.is_absolute():
            job.config_path = (base_dir / job.config_path).resolve()
        if job.realized_input_path is not None and not job.realized_input_path.is_absolute():
            job.realized_input_path = (base_dir / job.realized_input_path).resolve()
        if job.output_dir is not None and not job.output_dir.is_absolute():
            job.output_dir = (base_dir / job.output_dir).resolve()
    return config
