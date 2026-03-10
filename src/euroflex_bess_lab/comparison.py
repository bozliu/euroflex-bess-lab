from __future__ import annotations

import itertools
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from .analytics.reporting import load_report_summary
from .backtesting.engine import run_walk_forward
from .config import BacktestConfig, SweepConfig, load_config
from .data.io import save_json


def _flatten_summary(summary: dict[str, Any]) -> dict[str, Any]:
    flattened = dict(summary)
    forecast_error = flattened.pop("forecast_error", {})
    reason_counts = flattened.pop("reason_code_counts", {})
    scenario_analysis = flattened.pop("scenario_analysis", {})
    reconciliation = flattened.get("reconciliation", {})
    flattened["forecast_error_day_ahead_mae"] = forecast_error.get("day_ahead", {}).get("mae", 0.0)
    flattened["forecast_error_day_ahead_rmse"] = forecast_error.get("day_ahead", {}).get("rmse", 0.0)
    flattened["forecast_error_day_ahead_bias"] = forecast_error.get("day_ahead", {}).get("bias", 0.0)
    flattened["forecast_error_imbalance_mae"] = forecast_error.get("imbalance", {}).get("mae", 0.0)
    flattened["forecast_error_imbalance_rmse"] = forecast_error.get("imbalance", {}).get("rmse", 0.0)
    flattened["forecast_error_imbalance_bias"] = forecast_error.get("imbalance", {}).get("bias", 0.0)
    flattened["forecast_error_fcr_capacity_mae"] = forecast_error.get("fcr_capacity", {}).get("mae", 0.0)
    flattened["forecast_error_fcr_capacity_rmse"] = forecast_error.get("fcr_capacity", {}).get("rmse", 0.0)
    flattened["forecast_error_fcr_capacity_bias"] = forecast_error.get("fcr_capacity", {}).get("bias", 0.0)
    flattened["forecast_error_afrr_capacity_up_mae"] = forecast_error.get("afrr_capacity_up", {}).get("mae", 0.0)
    flattened["forecast_error_afrr_capacity_down_mae"] = forecast_error.get("afrr_capacity_down", {}).get("mae", 0.0)
    if isinstance(scenario_analysis, dict):
        flattened["scenario_expected_total_pnl_eur"] = scenario_analysis.get("expected_total_pnl_eur", 0.0)
        flattened["scenario_best_total_pnl_eur"] = scenario_analysis.get("best_total_pnl_eur", 0.0)
        flattened["scenario_worst_total_pnl_eur"] = scenario_analysis.get("worst_total_pnl_eur", 0.0)
        flattened["scenario_spread_total_pnl_eur"] = scenario_analysis.get("spread_total_pnl_eur", 0.0)
        flattened["scenario_downside_penalty_contribution_eur"] = scenario_analysis.get(
            "downside_penalty_contribution_eur",
            0.0,
        )
        flattened["scenario_reserve_revenue_spread_eur"] = scenario_analysis.get("reserve_revenue_spread_eur", 0.0)
    if isinstance(reconciliation, dict) and isinstance(reconciliation.get("scenario_analysis"), dict):
        recon_scenario = reconciliation["scenario_analysis"]
        flattened["reconciliation_nearest_scenario_id"] = recon_scenario.get("nearest_scenario_id")
        flattened["reconciliation_scenario_envelope_distance_eur"] = recon_scenario.get(
            "realized_vs_scenario_envelope_distance_eur",
            0.0,
        )
    for code, count in reason_counts.items():
        flattened[f"reason_code__{code}"] = count
    return flattened


def _build_grouped_frame(frame: pd.DataFrame, *, group_by: str) -> pd.DataFrame:
    column = "market_id" if group_by == "market" else group_by
    numeric_columns = [
        "total_pnl_eur",
        "expected_total_pnl_eur",
        "oracle_gap_total_pnl_eur",
        "throughput_mwh",
        "idle_share",
        "energy_revenue_eur",
        "reserve_capacity_revenue_eur",
        "reserve_activation_revenue_eur",
        "reserve_penalty_eur",
        "reserve_share_of_total_revenue",
        "reserved_capacity_mw_avg",
        "reserved_capacity_mw_max",
        "forecast_error_day_ahead_mae",
        "forecast_error_imbalance_mae",
        "forecast_error_fcr_capacity_mae",
        "scenario_spread_total_pnl_eur",
        "scenario_downside_penalty_contribution_eur",
        "scenario_reserve_revenue_spread_eur",
        "asset_count",
        "portfolio_uplift_vs_single_asset_eur",
    ]
    available_numeric = [col for col in numeric_columns if col in frame.columns]
    grouped = frame.groupby(column, dropna=False)[available_numeric].mean(numeric_only=True).reset_index()
    grouped = grouped.rename(columns={column: "group"})
    return grouped


def compare_runs(run_dirs: Sequence[str | Path], output_dir: str | Path, *, group_by: str | None = None) -> Path:
    summaries = [load_report_summary(run_dir) for run_dir in run_dirs]
    if not summaries:
        raise ValueError("compare_runs requires at least one run directory")

    frame = (
        pd.DataFrame([_flatten_summary(summary) for summary in summaries])
        .sort_values(["market_id", "run_id"])
        .reset_index(drop=True)
    )
    reference = frame.iloc[0]
    frame["delta_vs_first_total_pnl_eur"] = frame["total_pnl_eur"] - reference["total_pnl_eur"]
    frame["delta_vs_first_expected_pnl_eur"] = frame["expected_total_pnl_eur"] - reference["expected_total_pnl_eur"]
    frame["delta_vs_first_oracle_gap_eur"] = frame["oracle_gap_total_pnl_eur"] - reference["oracle_gap_total_pnl_eur"]
    frame["delta_vs_first_reserve_revenue_eur"] = (
        frame["reserve_capacity_revenue_eur"] - reference["reserve_capacity_revenue_eur"]
    )
    if {"market_id", "workflow", "provider_name", "run_scope", "total_pnl_eur"}.issubset(frame.columns):
        frame["portfolio_uplift_vs_single_asset_eur"] = 0.0
        grouping = frame.groupby(["market_id", "workflow", "provider_name"], dropna=False)
        for _, idx in grouping.groups.items():
            grouped = frame.loc[idx]
            single = grouped[grouped["run_scope"] == "single_asset"]
            portfolio = grouped[grouped["run_scope"] == "portfolio"]
            if not single.empty and not portfolio.empty:
                baseline = float(single.iloc[0]["total_pnl_eur"])
                frame.loc[portfolio.index, "portfolio_uplift_vs_single_asset_eur"] = (
                    frame.loc[portfolio.index, "total_pnl_eur"] - baseline
                )

    target = Path(output_dir)
    target.mkdir(parents=True, exist_ok=True)
    frame.to_csv(target / "comparison.csv", index=False)
    save_json({"runs": frame.to_dict(orient="records")}, target / "comparison.json")
    report_lines = [
        f"# Run Comparison ({len(frame)} runs)",
        "",
        f"- Generated at: `{datetime.now(tz=UTC).isoformat()}`",
        f"- Reference run: `{reference['run_id']}`",
        "",
        "## Totals",
        "",
    ]
    for row in frame.itertuples():
        report_lines.extend(
            [
                f"### {row.run_id}",
                f"- Market: `{row.market_id}`",
                f"- Site: `{getattr(row, 'site_id', 'unknown')}`",
                f"- Run scope: `{getattr(row, 'run_scope', 'single_asset')}`",
                f"- Workflow: `{row.workflow}`",
                f"- Benchmark: `{row.benchmark_name}`",
                f"- Total PnL (EUR): `{row.total_pnl_eur:.2f}`",
                f"- Oracle Gap (EUR): `{row.oracle_gap_total_pnl_eur:.2f}`",
                f"- Reserve Revenue (EUR): `{row.reserve_capacity_revenue_eur:.2f}`",
                f"- Reserve Share: `{row.reserve_share_of_total_revenue:.2%}`",
                f"- Portfolio uplift vs single asset (EUR): `{getattr(row, 'portfolio_uplift_vs_single_asset_eur', 0.0):.2f}`",
                f"- Delta vs first (EUR): `{row.delta_vs_first_total_pnl_eur:.2f}`",
                "",
            ]
        )
    requested_groups = [group_by] if group_by is not None else []
    for default_group in ("market", "workflow"):
        if (
            default_group not in requested_groups
            and ("market_id" if default_group == "market" else default_group) in frame.columns
        ):
            requested_groups.append(default_group)
    for group in requested_groups:
        grouped = _build_grouped_frame(frame, group_by=group)
        grouped.to_csv(target / f"grouped_by_{group}.csv", index=False)
        save_json({"groups": grouped.to_dict(orient="records")}, target / f"grouped_by_{group}.json")
        report_lines.extend(
            [
                f"## Grouped by {group}",
                "",
            ]
        )
        for row in grouped.itertuples():
            report_lines.extend(
                [
                    f"### {row.group}",
                    f"- Mean total PnL (EUR): `{row.total_pnl_eur:.2f}`",
                    f"- Mean oracle gap (EUR): `{row.oracle_gap_total_pnl_eur:.2f}`",
                    f"- Mean reserve revenue (EUR): `{row.reserve_capacity_revenue_eur:.2f}`",
                    f"- Mean reserve share: `{row.reserve_share_of_total_revenue:.2%}`",
                    f"- Mean throughput (MWh): `{row.throughput_mwh:.2f}`",
                    "",
                ]
            )
    (target / "report.md").write_text("\n".join(report_lines), encoding="utf-8")
    return target


def _set_nested_value(payload: dict[str, Any], dotted_key: str, value: Any) -> None:
    parts = dotted_key.split(".")
    cursor = payload
    for part in parts[:-1]:
        if part not in cursor or not isinstance(cursor[part], dict):
            cursor[part] = {}
        cursor = cursor[part]
    cursor[parts[-1]] = value


def _slugify_value(value: Any) -> str:
    text = str(value).replace("/", "-").replace(" ", "-").replace(":", "-")
    return text.lower()


def _resolve_runtime_paths(config: BacktestConfig, *, base_dir: Path) -> BacktestConfig:
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
    if not config.artifacts.root_dir.is_absolute():
        config.artifacts.root_dir = (base_dir / config.artifacts.root_dir).resolve()
    return config


def run_sweep(sweep_config: SweepConfig) -> Path:
    base_config = load_config(sweep_config.base_config_path)
    keys = list(sweep_config.matrix.keys())
    values = [sweep_config.matrix[key] for key in keys]
    sweep_root = sweep_config.artifacts.root_dir / sweep_config.sweep_name
    sweep_root.mkdir(parents=True, exist_ok=True)

    run_dirs: list[Path] = []
    for combination in itertools.product(*values):
        payload = base_config.model_dump(mode="json")
        suffix_parts: list[str] = []
        for key, value in zip(keys, combination, strict=True):
            if key == "__bundle__":
                if not isinstance(value, dict):
                    raise ValueError("__bundle__ sweep dimension must contain mapping values")
                for bundle_key, bundle_value in value.items():
                    _set_nested_value(payload, bundle_key, bundle_value)
                bundle_label = value.get("market.id", value.get("run_label", "bundle"))
                suffix_parts.append(f"bundle-{_slugify_value(bundle_label)}")
                continue
            _set_nested_value(payload, key, value)
            suffix_parts.append(f"{key.split('.')[-1]}-{_slugify_value(value)}")
        payload["run_name"] = f"{base_config.run_name}-{'-'.join(suffix_parts)}"
        payload["artifacts"]["root_dir"] = str(sweep_root / "runs")
        config = _resolve_runtime_paths(
            BacktestConfig.model_validate(payload),
            base_dir=sweep_config.base_config_path.parent,
        )
        result = run_walk_forward(config)
        if result.output_dir is None:
            raise RuntimeError("run_walk_forward did not populate output_dir")
        run_dirs.append(result.output_dir)

    comparison_dir = compare_runs(run_dirs, sweep_root / "comparison", group_by="market")
    save_json(
        {
            "sweep_name": sweep_config.sweep_name,
            "run_dirs": [str(path) for path in run_dirs],
            "comparison_dir": str(comparison_dir),
        },
        sweep_root / "sweep_manifest.json",
    )
    return comparison_dir
