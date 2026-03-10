from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class AvailabilityWindow(BaseModel):
    start: datetime
    end: datetime
    availability_factor: float = Field(default=0.0, ge=0.0, le=1.0)

    @field_validator("start", "end", mode="before")
    @classmethod
    def parse_timestamps(cls, value: object) -> datetime:
        return pd.Timestamp(value).to_pydatetime()

    @model_validator(mode="after")
    def validate_window(self) -> AvailabilityWindow:
        if self.end <= self.start:
            raise ValueError("Availability windows must have end > start")
        return self


class AvailabilityMask(BaseModel):
    outages: list[AvailabilityWindow] = Field(default_factory=list)

    def factor_for(self, timestamp: pd.Timestamp) -> float:
        factor = 1.0
        for outage in self.outages:
            outage_start = pd.Timestamp(outage.start)
            outage_end = pd.Timestamp(outage.end)
            if outage_start <= timestamp < outage_end:
                factor = min(factor, outage.availability_factor)
        return factor

    def to_series(self, index: pd.DatetimeIndex) -> pd.Series:
        values = [self.factor_for(ts) for ts in index]
        return pd.Series(values, index=index, name="availability_factor")


class BatterySpec(BaseModel):
    name: str = "battery"
    power_mw: float = Field(gt=0.0)
    energy_mwh: float = Field(gt=0.0)
    initial_soc_mwh: float = Field(ge=0.0)
    terminal_soc_mwh: float | None = Field(default=None, ge=0.0)
    soc_min_mwh: float = Field(ge=0.0)
    soc_max_mwh: float = Field(gt=0.0)
    charge_efficiency: float = Field(gt=0.0, le=1.0)
    discharge_efficiency: float = Field(gt=0.0, le=1.0)
    connection_limit_mw: float | None = Field(default=None, gt=0.0)
    minimum_headroom_mwh: float = Field(default=0.0, ge=0.0)
    availability: AvailabilityMask = Field(default_factory=AvailabilityMask)

    @model_validator(mode="after")
    def validate_soc_bounds(self) -> BatterySpec:
        if self.soc_min_mwh >= self.soc_max_mwh:
            raise ValueError("soc_min_mwh must be smaller than soc_max_mwh")
        if self.soc_max_mwh > self.energy_mwh:
            raise ValueError("soc_max_mwh cannot exceed energy_mwh")
        if not self.soc_min_mwh <= self.initial_soc_mwh <= self.soc_max_mwh:
            raise ValueError("initial_soc_mwh must be within SOC bounds")
        if self.terminal_soc_mwh is not None and not self.soc_min_mwh <= self.terminal_soc_mwh <= self.soc_max_mwh:
            raise ValueError("terminal_soc_mwh must be within SOC bounds")
        if self.minimum_headroom_mwh * 2 >= self.soc_max_mwh - self.soc_min_mwh:
            raise ValueError("minimum_headroom_mwh leaves no feasible SOC band")
        if not self.effective_soc_min_mwh <= self.initial_soc_mwh <= self.effective_soc_max_mwh:
            raise ValueError("initial_soc_mwh must remain feasible after headroom is applied")
        if (
            self.terminal_soc_mwh is not None
            and not self.effective_soc_min_mwh <= self.terminal_soc_mwh <= self.effective_soc_max_mwh
        ):
            raise ValueError("terminal_soc_mwh must remain feasible after headroom is applied")
        return self

    @property
    def effective_power_limit_mw(self) -> float:
        return min(self.power_mw, self.connection_limit_mw or self.power_mw)

    @property
    def effective_soc_min_mwh(self) -> float:
        return self.soc_min_mwh + self.minimum_headroom_mwh

    @property
    def effective_soc_max_mwh(self) -> float:
        return self.soc_max_mwh - self.minimum_headroom_mwh

    @property
    def usable_energy_mwh(self) -> float:
        return self.effective_soc_max_mwh - self.effective_soc_min_mwh


class SiteSpec(BaseModel):
    id: str
    poi_import_limit_mw: float = Field(gt=0.0)
    poi_export_limit_mw: float = Field(gt=0.0)


class AssetSpec(BaseModel):
    id: str
    kind: Literal["battery"] = "battery"
    battery: BatterySpec

    @model_validator(mode="after")
    def align_battery_name(self) -> AssetSpec:
        if self.battery.name == "battery":
            self.battery.name = self.id
        return self


class BatteryState(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    timestamp: pd.Timestamp
    soc_mwh: float
    available_power_mw: float
    availability_factor: float


class MarketProduct(BaseModel):
    name: str
    market_name: str
    zone: str
    time_resolution_minutes: int
    bid_symmetry_required: bool = False
    gate_closure: str
    settlement_method: str
    penalty_rule: str
    activation_rule: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class PriceSeries(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    market: str
    zone: str
    resolution_minutes: int
    currency: str = "EUR"
    source: str
    value_kind: Literal["actual", "forecast"]
    data: pd.DataFrame
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_data(self) -> PriceSeries:
        required = {
            "timestamp_utc",
            "timestamp_local",
            "market",
            "zone",
            "resolution_minutes",
            "price_eur_per_mwh",
            "currency",
            "source",
            "value_kind",
            "provenance",
        }
        missing = required.difference(self.data.columns)
        if missing:
            raise ValueError(f"PriceSeries is missing required columns: {sorted(missing)}")
        if not self.data["timestamp_utc"].is_monotonic_increasing:
            raise ValueError("PriceSeries timestamps must be sorted ascending")
        return self


class PnLAttribution(BaseModel):
    da_revenue_eur: float = 0.0
    imbalance_revenue_eur: float = 0.0
    reserve_capacity_revenue_eur: float = 0.0
    reserve_activation_revenue_eur: float = 0.0
    reserve_penalty_eur: float = 0.0
    degradation_cost_eur: float = 0.0
    total_pnl_eur: float = 0.0
    expected_total_pnl_eur: float = 0.0
    metadata: dict[str, Any] = Field(default_factory=dict)


class OracleComparison(BaseModel):
    benchmark_name: str
    total_pnl_eur: float
    da_revenue_eur: float
    imbalance_revenue_eur: float
    reserve_capacity_revenue_eur: float = 0.0
    reserve_activation_revenue_eur: float = 0.0
    reserve_penalty_eur: float = 0.0
    degradation_cost_eur: float


class RunResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    run_id: str
    market_id: str
    market_timezone: str
    workflow: str
    workflow_family: str
    benchmark_name: str
    benchmark_family: str
    provider_name: str
    auditable: bool
    run_scope: Literal["single_asset", "portfolio"]
    site_id: str
    asset_count: int
    site_dispatch: pd.DataFrame
    asset_dispatch: pd.DataFrame
    asset_pnl_attribution: pd.DataFrame
    decision_log: pd.DataFrame
    forecast_snapshots: pd.DataFrame
    settlement_breakdown: pd.DataFrame
    baseline_schedule: pd.DataFrame | None = None
    revision_schedule: pd.DataFrame | None = None
    schedule_lineage: pd.DataFrame | None = None
    reconciliation_breakdown: pd.DataFrame | None = None
    reconciliation_summary: dict[str, Any] | None = None
    pnl: PnLAttribution
    oracle: OracleComparison | None = None
    output_dir: Path | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @property
    def realized_dispatch(self) -> pd.DataFrame:
        return self.site_dispatch

    @model_validator(mode="after")
    def validate_frames(self) -> RunResult:
        required_site_dispatch = {
            "timestamp_utc",
            "timestamp_local",
            "site_id",
            "market_id",
            "run_scope",
            "workflow_family",
            "schedule_version",
            "lock_state",
            "charge_mw",
            "discharge_mw",
            "soc_mwh",
            "net_export_mw",
            "reserved_capacity_mw",
            "reason_code",
            "day_ahead_actual_price_eur_per_mwh",
            "day_ahead_forecast_price_eur_per_mwh",
            "fcr_reserved_mw",
            "afrr_up_reserved_mw",
            "afrr_down_reserved_mw",
            "reserve_headroom_up_mw",
            "reserve_headroom_down_mw",
            "fcr_capacity_price_actual_eur_per_mw_per_h",
            "fcr_capacity_price_forecast_eur_per_mw_per_h",
            "afrr_capacity_up_price_actual_eur_per_mw_per_h",
            "afrr_capacity_up_price_forecast_eur_per_mw_per_h",
            "afrr_capacity_down_price_actual_eur_per_mw_per_h",
            "afrr_capacity_down_price_forecast_eur_per_mw_per_h",
            "afrr_activation_price_up_actual_eur_per_mwh",
            "afrr_activation_price_up_forecast_eur_per_mwh",
            "afrr_activation_price_down_actual_eur_per_mwh",
            "afrr_activation_price_down_forecast_eur_per_mwh",
            "afrr_activation_ratio_up_actual",
            "afrr_activation_ratio_up_forecast",
            "afrr_activation_ratio_down_actual",
            "afrr_activation_ratio_down_forecast",
        }
        missing_site_dispatch = required_site_dispatch.difference(self.site_dispatch.columns)
        if missing_site_dispatch:
            raise ValueError(f"RunResult.site_dispatch is missing required columns: {sorted(missing_site_dispatch)}")

        required_asset_dispatch = {
            "timestamp_utc",
            "timestamp_local",
            "site_id",
            "asset_id",
            "market_id",
            "run_scope",
            "workflow_family",
            "schedule_version",
            "lock_state",
            "charge_mw",
            "discharge_mw",
            "soc_mwh",
            "net_export_mw",
            "fcr_reserved_mw",
            "afrr_up_reserved_mw",
            "afrr_down_reserved_mw",
            "availability_factor",
            "reason_code",
        }
        missing_asset_dispatch = required_asset_dispatch.difference(self.asset_dispatch.columns)
        if missing_asset_dispatch:
            raise ValueError(f"RunResult.asset_dispatch is missing required columns: {sorted(missing_asset_dispatch)}")

        required_asset_pnl = {
            "asset_id",
            "site_id",
            "market_id",
            "workflow_family",
            "run_scope",
            "da_revenue_eur",
            "imbalance_revenue_eur",
            "reserve_capacity_revenue_eur",
            "reserve_penalty_eur",
            "degradation_cost_eur",
            "total_pnl_eur",
        }
        missing_asset_pnl = required_asset_pnl.difference(self.asset_pnl_attribution.columns)
        if missing_asset_pnl:
            raise ValueError(
                f"RunResult.asset_pnl_attribution is missing required columns: {sorted(missing_asset_pnl)}"
            )

        required_decisions = {
            "decision_time_utc",
            "decision_type",
            "provider_name",
            "benchmark_name",
            "market_id",
            "workflow_family",
            "run_scope",
            "site_id",
            "schedule_version",
        }
        missing_decisions = required_decisions.difference(self.decision_log.columns)
        if missing_decisions:
            raise ValueError(f"RunResult.decision_log is missing required columns: {sorted(missing_decisions)}")

        required_snapshots = {
            "decision_time_utc",
            "market",
            "delivery_start_utc",
            "forecast_price_eur_per_mwh",
            "issue_time_utc",
            "available_from_utc",
            "provider_name",
            "market_id",
            "workflow_family",
            "run_scope",
            "site_id",
            "schedule_version",
        }
        missing_snapshots = required_snapshots.difference(self.forecast_snapshots.columns)
        if missing_snapshots:
            raise ValueError(f"RunResult.forecast_snapshots is missing required columns: {sorted(missing_snapshots)}")
        if self.baseline_schedule is not None:
            required_schedule = {
                "timestamp_utc",
                "timestamp_local",
                "site_id",
                "market_id",
                "workflow_family",
                "run_scope",
                "schedule_version",
                "schedule_state",
                "lock_state",
                "net_export_mw",
            }
            missing_baseline = required_schedule.difference(self.baseline_schedule.columns)
            if missing_baseline:
                raise ValueError(f"RunResult.baseline_schedule is missing required columns: {sorted(missing_baseline)}")
        if self.revision_schedule is not None:
            required_schedule = {
                "timestamp_utc",
                "timestamp_local",
                "site_id",
                "market_id",
                "workflow_family",
                "run_scope",
                "schedule_version",
                "schedule_state",
                "lock_state",
                "net_export_mw",
            }
            missing_revision = required_schedule.difference(self.revision_schedule.columns)
            if missing_revision:
                raise ValueError(f"RunResult.revision_schedule is missing required columns: {sorted(missing_revision)}")
        if self.schedule_lineage is not None:
            required_lineage = {
                "entity_type",
                "timestamp_utc",
                "site_id",
                "market_id",
                "workflow_family",
                "run_scope",
                "schedule_version",
                "schedule_state",
                "lock_state",
            }
            missing_lineage = required_lineage.difference(self.schedule_lineage.columns)
            if missing_lineage:
                raise ValueError(f"RunResult.schedule_lineage is missing required columns: {sorted(missing_lineage)}")
        if self.reconciliation_breakdown is not None:
            required_reconciliation = {
                "timestamp_utc",
                "site_id",
                "market_id",
                "workflow_family",
                "run_scope",
                "baseline_expected_pnl_eur",
                "revised_expected_pnl_eur",
                "realized_pnl_eur",
            }
            missing_reconciliation = required_reconciliation.difference(self.reconciliation_breakdown.columns)
            if missing_reconciliation:
                raise ValueError(
                    f"RunResult.reconciliation_breakdown is missing required columns: {sorted(missing_reconciliation)}"
                )
        return self


def default_local_frame(data: pd.DataFrame, timezone: str = "Europe/Brussels") -> pd.DataFrame:
    frame = data.copy()
    frame["timestamp_utc"] = pd.to_datetime(frame["timestamp_utc"], utc=True)
    frame["timestamp_local"] = frame["timestamp_utc"].dt.tz_convert(timezone)
    return frame
