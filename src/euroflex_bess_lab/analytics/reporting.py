from __future__ import annotations

import json
from pathlib import Path
from typing import SupportsFloat, cast

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd

from ..data.io import save_json
from ..types import RunResult
from .rainflow import summarize_rainflow


def _as_float(value: object) -> float:
    return float(cast(SupportsFloat | str, value)) if value is not None else 0.0


def _dispatch_chart(dispatch: pd.DataFrame, output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.step(dispatch["timestamp_local"], dispatch["discharge_mw"], where="post", label="Discharge MW", color="#0f766e")
    ax.step(dispatch["timestamp_local"], -dispatch["charge_mw"], where="post", label="Charge MW", color="#b45309")
    if "fcr_reserved_mw" in dispatch.columns and dispatch["fcr_reserved_mw"].abs().sum() > 0.0:
        ax.step(
            dispatch["timestamp_local"],
            dispatch["fcr_reserved_mw"],
            where="post",
            label="FCR Reserved MW",
            color="#4338ca",
        )
    if "afrr_up_reserved_mw" in dispatch.columns and dispatch["afrr_up_reserved_mw"].abs().sum() > 0.0:
        ax.step(
            dispatch["timestamp_local"],
            dispatch["afrr_up_reserved_mw"],
            where="post",
            label="aFRR Up MW",
            color="#7c3aed",
        )
    if "afrr_down_reserved_mw" in dispatch.columns and dispatch["afrr_down_reserved_mw"].abs().sum() > 0.0:
        ax.step(
            dispatch["timestamp_local"],
            dispatch["afrr_down_reserved_mw"],
            where="post",
            label="aFRR Down MW",
            color="#db2777",
        )
    ax.axhline(0.0, color="#1f2937", linewidth=0.8)
    ax.set_ylabel("MW")
    ax.set_title("Realized Dispatch Schedule")
    ax.legend(loc="upper right")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(output_path, dpi=180)
    plt.close(fig)


def _soc_chart(dispatch: pd.DataFrame, output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.step(dispatch["timestamp_local"], dispatch["soc_mwh"], where="post", label="SoC MWh", color="#1d4ed8")
    ax.set_ylabel("MWh")
    ax.set_title("Battery State of Charge")
    ax.legend(loc="upper right")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(output_path, dpi=180)
    plt.close(fig)


def _forecast_vs_realized_chart(
    dispatch: pd.DataFrame,
    *,
    forecast_column: str,
    actual_column: str,
    title: str,
    output_path: Path,
    y_label: str = "EUR/MWh",
) -> None:
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.step(dispatch["timestamp_local"], dispatch[actual_column], where="post", label="Actual", color="#0f766e")
    ax.step(dispatch["timestamp_local"], dispatch[forecast_column], where="post", label="Forecast", color="#b45309")
    ax.set_ylabel(y_label)
    ax.set_title(title)
    ax.legend(loc="upper right")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(output_path, dpi=180)
    plt.close(fig)


def generate_report(
    result: RunResult,
    summary: dict[str, object],
    output_dir: str | Path,
    *,
    save_plots: bool = True,
) -> dict[str, object]:
    report_dir = Path(output_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    dispatch = result.site_dispatch
    effective_workflow = str(summary.get("base_workflow", result.workflow))
    rainflow = summarize_rainflow(dispatch["soc_mwh"], max(float(dispatch["soc_mwh"].max()), 1.0))
    report_summary = dict(summary)
    report_summary["rainflow"] = rainflow.as_dict()
    if save_plots:
        _dispatch_chart(dispatch, report_dir / "dispatch.png")
        _soc_chart(dispatch, report_dir / "soc.png")
        _forecast_vs_realized_chart(
            dispatch,
            forecast_column="day_ahead_forecast_price_eur_per_mwh",
            actual_column="day_ahead_actual_price_eur_per_mwh",
            title="Day-Ahead Forecast vs Realized",
            output_path=report_dir / "day_ahead_forecast_vs_realized.png",
        )
        if effective_workflow == "da_plus_imbalance":
            _forecast_vs_realized_chart(
                dispatch,
                forecast_column="imbalance_forecast_price_eur_per_mwh",
                actual_column="imbalance_actual_price_eur_per_mwh",
                title="Imbalance Forecast vs Realized",
                output_path=report_dir / "imbalance_forecast_vs_realized.png",
            )
        if effective_workflow == "da_plus_fcr":
            _forecast_vs_realized_chart(
                dispatch,
                forecast_column="fcr_capacity_price_forecast_eur_per_mw_per_h",
                actual_column="fcr_capacity_price_actual_eur_per_mw_per_h",
                title="FCR Capacity Forecast vs Realized",
                output_path=report_dir / "fcr_capacity_forecast_vs_realized.png",
                y_label="EUR/MW/h",
            )
        if effective_workflow == "da_plus_afrr":
            _forecast_vs_realized_chart(
                dispatch,
                forecast_column="afrr_capacity_up_price_forecast_eur_per_mw_per_h",
                actual_column="afrr_capacity_up_price_actual_eur_per_mw_per_h",
                title="aFRR Up Capacity Forecast vs Realized",
                output_path=report_dir / "afrr_capacity_up_forecast_vs_realized.png",
                y_label="EUR/MW/h",
            )
            _forecast_vs_realized_chart(
                dispatch,
                forecast_column="afrr_capacity_down_price_forecast_eur_per_mw_per_h",
                actual_column="afrr_capacity_down_price_actual_eur_per_mw_per_h",
                title="aFRR Down Capacity Forecast vs Realized",
                output_path=report_dir / "afrr_capacity_down_forecast_vs_realized.png",
                y_label="EUR/MW/h",
            )

    save_json(report_summary, report_dir / "report_summary.json")
    forecast_error = report_summary.get("forecast_error", {})
    if not isinstance(forecast_error, dict):
        forecast_error = {}
    day_ahead_error = forecast_error.get("day_ahead", {})
    imbalance_error = forecast_error.get("imbalance", {})
    fcr_error = forecast_error.get("fcr_capacity", {})
    afrr_capacity_up_error = forecast_error.get("afrr_capacity_up", {})
    afrr_capacity_down_error = forecast_error.get("afrr_capacity_down", {})
    if not isinstance(day_ahead_error, dict):
        day_ahead_error = {}
    if not isinstance(imbalance_error, dict):
        imbalance_error = {}
    if not isinstance(fcr_error, dict):
        fcr_error = {}
    if not isinstance(afrr_capacity_up_error, dict):
        afrr_capacity_up_error = {}
    if not isinstance(afrr_capacity_down_error, dict):
        afrr_capacity_down_error = {}
    reason_code_counts = report_summary.get("reason_code_counts", {})
    if not isinstance(reason_code_counts, dict):
        reason_code_counts = {}
    markdown_lines = [
        f"# Run Report: {result.run_id}",
        "",
        f"- Market: `{result.market_id}`",
        f"- Market timezone: `{result.market_timezone}`",
        f"- Site: `{result.site_id}`",
        f"- Run scope: `{result.run_scope}`",
        f"- Asset count: `{result.asset_count}`",
        f"- Benchmark: `{result.benchmark_name}`",
        f"- Workflow: `{result.workflow}`",
        f"- Base workflow: `{summary.get('base_workflow', result.workflow)}`",
        f"- Provider: `{result.provider_name}`",
        f"- Forecast mode: `{summary.get('forecast_mode', 'point')}`",
        f"- Risk mode: `{summary.get('risk_mode', 'expected_value')}`",
        f"- Auditable: `{result.auditable}`",
        f"- Settlement basis: `{report_summary.get('settlement_basis', 'unknown')}`",
        f"- Gate closure: `{report_summary.get('gate_closure_definition', 'unknown')}`",
        f"- Total PnL (EUR): `{result.pnl.total_pnl_eur:.2f}`",
        f"- Expected PnL (EUR): `{result.pnl.expected_total_pnl_eur:.2f}`",
        f"- Oracle Gap (EUR): `{report_summary.get('oracle_gap_total_pnl_eur', 0.0):.2f}`",
        f"- Reserve Revenue (EUR): `{result.pnl.reserve_capacity_revenue_eur:.2f}`",
        f"- Reserve Activation Revenue (EUR): `{result.pnl.reserve_activation_revenue_eur:.2f}`",
        f"- Reserve Penalty (EUR): `{result.pnl.reserve_penalty_eur:.2f}`",
        f"- Equivalent full cycles: `{rainflow.equivalent_full_cycles:.3f}`",
        "",
        "## Portfolio",
        "",
        f"- POI import limit (MW): `{report_summary.get('poi_import_limit_mw', 0.0)}`",
        f"- POI export limit (MW): `{report_summary.get('poi_export_limit_mw', 0.0)}`",
        f"- Reserved capacity avg (MW): `{_as_float(report_summary.get('reserved_capacity_mw_avg', 0.0)):.2f}`",
        f"- Reserved capacity max (MW): `{_as_float(report_summary.get('reserved_capacity_mw_max', 0.0)):.2f}`",
        "",
        "## Forecast Error",
        "",
        f"- Day-ahead MAE: `{float(day_ahead_error.get('mae', 0.0)):.2f}`",
        f"- Day-ahead RMSE: `{float(day_ahead_error.get('rmse', 0.0)):.2f}`",
        f"- Imbalance MAE: `{float(imbalance_error.get('mae', 0.0)):.2f}`",
        f"- Imbalance RMSE: `{float(imbalance_error.get('rmse', 0.0)):.2f}`",
        f"- FCR capacity MAE: `{float(fcr_error.get('mae', 0.0)):.2f}`",
        f"- FCR capacity RMSE: `{float(fcr_error.get('rmse', 0.0)):.2f}`",
        f"- aFRR up capacity MAE: `{float(afrr_capacity_up_error.get('mae', 0.0)):.2f}`",
        f"- aFRR down capacity MAE: `{float(afrr_capacity_down_error.get('mae', 0.0)):.2f}`",
        "",
        "## Reason Codes",
        "",
    ]
    markdown_lines.extend([f"- `{code}`: {count}" for code, count in reason_code_counts.items()])
    if "reconciliation" in report_summary and isinstance(report_summary["reconciliation"], dict):
        reconciliation = report_summary["reconciliation"]
        markdown_lines.append("")
        markdown_lines.append("## Reconciliation")
        markdown_lines.append("")
        markdown_lines.append(
            f"- Baseline expected total (EUR): `{float(reconciliation.get('baseline_expected_total_pnl_eur', 0.0)):.2f}`"
        )
        markdown_lines.append(
            f"- Revised expected total (EUR): `{float(reconciliation.get('revised_expected_total_pnl_eur', 0.0)):.2f}`"
        )
        markdown_lines.append(
            f"- Realized total (EUR): `{float(reconciliation.get('realized_total_pnl_eur', 0.0)):.2f}`"
        )
        markdown_lines.append(
            f"- Delta vs baseline expected (EUR): `{float(reconciliation.get('delta_vs_baseline_expected_eur', 0.0)):.2f}`"
        )
        markdown_lines.append(
            f"- Delta vs revised expected (EUR): `{float(reconciliation.get('delta_vs_revised_expected_eur', 0.0)):.2f}`"
        )
    scenario_analysis = report_summary.get("scenario_analysis")
    if isinstance(scenario_analysis, dict):
        markdown_lines.append("")
        markdown_lines.append("## Scenario Analysis")
        markdown_lines.append("")
        markdown_lines.append(f"- Forecast mode: `{scenario_analysis.get('forecast_mode', 'point')}`")
        markdown_lines.append(f"- Risk mode: `{scenario_analysis.get('risk_mode', 'expected_value')}`")
        markdown_lines.append(f"- Scenario count: `{int(scenario_analysis.get('scenario_count', 0))}`")
        markdown_lines.append(
            f"- Expected total PnL (EUR): `{float(scenario_analysis.get('scenario_expected_total_pnl_eur', 0.0)):.2f}`"
        )
        markdown_lines.append(
            f"- Best total PnL (EUR): `{float(scenario_analysis.get('scenario_best_total_pnl_eur', 0.0)):.2f}`"
        )
        markdown_lines.append(
            f"- Worst total PnL (EUR): `{float(scenario_analysis.get('scenario_worst_total_pnl_eur', 0.0)):.2f}`"
        )
        markdown_lines.append(
            f"- Scenario spread (EUR): `{float(scenario_analysis.get('scenario_spread_total_pnl_eur', 0.0)):.2f}`"
        )
        markdown_lines.append(
            f"- Downside penalty contribution (EUR): `{float(scenario_analysis.get('downside_penalty_contribution_eur', 0.0)):.2f}`"
        )
        markdown_lines.append(
            f"- Reserve fragility (EUR): `{float(scenario_analysis.get('reserve_fragility_eur', 0.0)):.2f}`"
        )
        if scenario_analysis.get("nearest_scenario_id") is not None:
            markdown_lines.append(f"- Nearest scenario: `{scenario_analysis.get('nearest_scenario_id')}`")
            markdown_lines.append(
                f"- Realized vs envelope distance (EUR): `{float(scenario_analysis.get('realized_vs_scenario_envelope_distance_eur', 0.0)):.2f}`"
            )
            markdown_lines.append(f"- Scenario posture: `{scenario_analysis.get('scenario_posture', 'unknown')}`")
    markdown_lines.append("")
    markdown_lines.append("## Asset Contribution")
    markdown_lines.append("")
    for row in result.asset_pnl_attribution.sort_values("total_pnl_eur", ascending=False).itertuples():
        markdown_lines.append(f"- `{row.asset_id}`: `{row.total_pnl_eur:.2f}` EUR")
    markdown_lines.append("")
    markdown_lines.append("Artifacts:")
    markdown_lines.append("- `dispatch.png`")
    markdown_lines.append("- `soc.png`")
    markdown_lines.append("- `day_ahead_forecast_vs_realized.png`")
    if effective_workflow == "da_plus_imbalance":
        markdown_lines.append("- `imbalance_forecast_vs_realized.png`")
    if effective_workflow == "da_plus_fcr":
        markdown_lines.append("- `fcr_capacity_forecast_vs_realized.png`")
    if effective_workflow == "da_plus_afrr":
        markdown_lines.append("- `afrr_capacity_up_forecast_vs_realized.png`")
        markdown_lines.append("- `afrr_capacity_down_forecast_vs_realized.png`")
    markdown_lines.append("- `report_summary.json`")
    (report_dir / "report.md").write_text("\n".join(markdown_lines), encoding="utf-8")
    return report_summary


def load_report_summary(run_dir: str | Path) -> dict[str, object]:
    with (Path(run_dir) / "summary.json").open("r", encoding="utf-8") as handle:
        return json.load(handle)
