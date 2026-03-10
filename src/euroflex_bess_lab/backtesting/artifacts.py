from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from ..analytics.reporting import generate_report
from ..config import BacktestConfig
from ..contracts import validate_inline_reconciliation_payload, validate_summary_payload
from ..data.io import save_json, save_price_series
from ..types import PriceSeries, RunResult


def make_run_id(run_name: str) -> str:
    timestamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    slug = run_name.strip().lower().replace(" ", "-").replace("_", "-")
    return f"{slug}-{timestamp}"


def write_run_artifacts(
    *,
    config: BacktestConfig,
    run_id: str,
    result: RunResult,
    summary: dict[str, object],
    day_ahead: PriceSeries,
    imbalance: PriceSeries | None = None,
    fcr_capacity: PriceSeries | None = None,
    afrr_capacity_up: PriceSeries | None = None,
    afrr_capacity_down: PriceSeries | None = None,
    afrr_activation_price_up: PriceSeries | None = None,
    afrr_activation_price_down: PriceSeries | None = None,
    afrr_activation_ratio_up: PriceSeries | None = None,
    afrr_activation_ratio_down: PriceSeries | None = None,
) -> Path:
    run_dir = config.artifacts.root_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    if config.artifacts.save_inputs:
        input_dir = run_dir / "normalized_inputs"
        save_price_series(day_ahead, input_dir / "day_ahead.parquet")
        if imbalance is not None:
            save_price_series(imbalance, input_dir / "imbalance.parquet")
        if fcr_capacity is not None:
            save_price_series(fcr_capacity, input_dir / "fcr_capacity.parquet")
        if afrr_capacity_up is not None:
            save_price_series(afrr_capacity_up, input_dir / "afrr_capacity_up.parquet")
        if afrr_capacity_down is not None:
            save_price_series(afrr_capacity_down, input_dir / "afrr_capacity_down.parquet")
        if afrr_activation_price_up is not None:
            save_price_series(afrr_activation_price_up, input_dir / "afrr_activation_price_up.parquet")
        if afrr_activation_price_down is not None:
            save_price_series(afrr_activation_price_down, input_dir / "afrr_activation_price_down.parquet")
        if afrr_activation_ratio_up is not None:
            save_price_series(afrr_activation_ratio_up, input_dir / "afrr_activation_ratio_up.parquet")
        if afrr_activation_ratio_down is not None:
            save_price_series(afrr_activation_ratio_down, input_dir / "afrr_activation_ratio_down.parquet")

    result.site_dispatch.to_parquet(run_dir / "site_dispatch.parquet", index=False)
    result.asset_dispatch.to_parquet(run_dir / "asset_dispatch.parquet", index=False)
    result.asset_pnl_attribution.to_parquet(run_dir / "asset_pnl_attribution.parquet", index=False)
    result.decision_log.to_parquet(run_dir / "decision_log.parquet", index=False)
    result.settlement_breakdown.to_parquet(run_dir / "settlement_breakdown.parquet", index=False)
    if result.baseline_schedule is not None:
        result.baseline_schedule.to_parquet(run_dir / "baseline_schedule.parquet", index=False)
    if result.revision_schedule is not None:
        result.revision_schedule.to_parquet(run_dir / "revision_schedule.parquet", index=False)
    if result.schedule_lineage is not None:
        result.schedule_lineage.to_parquet(run_dir / "schedule_lineage.parquet", index=False)
    if result.reconciliation_breakdown is not None:
        result.reconciliation_breakdown.to_parquet(run_dir / "reconciliation_breakdown.parquet", index=False)
    if config.artifacts.save_forecast_snapshots:
        result.forecast_snapshots.to_parquet(run_dir / "forecast_snapshots.parquet", index=False)

    save_json(result.pnl.model_dump(), run_dir / "pnl_attribution.json")
    if result.reconciliation_summary is not None:
        validate_inline_reconciliation_payload(result.reconciliation_summary)
        save_json(result.reconciliation_summary, run_dir / "reconciliation_summary.json")
    save_json(config.model_dump(mode="json"), run_dir / "config_snapshot.json")
    validate_summary_payload(summary)
    save_json(summary, run_dir / "summary.json")
    generate_report(result, summary, run_dir / "report", save_plots=config.artifacts.save_plots)
    return run_dir
