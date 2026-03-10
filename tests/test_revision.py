from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import yaml

from euroflex_bess_lab.analytics.reporting import load_report_summary
from euroflex_bess_lab.backtesting.engine import run_walk_forward
from euroflex_bess_lab.config import BacktestConfig
from euroflex_bess_lab.exports import export_revision
from euroflex_bess_lab.reconciliation import reconcile_run


def _load_frame(path: Path, *, timezone: str) -> pd.DataFrame:
    frame = pd.read_csv(path)
    frame["timestamp_utc"] = pd.to_datetime(frame["timestamp_utc"], utc=True)
    frame["timestamp_local"] = pd.to_datetime(frame["timestamp_local"], utc=True).dt.tz_convert(timezone)
    return frame


def _constant_forecast(
    delivery_frame: pd.DataFrame,
    *,
    market: str,
    decision_time_utc: pd.Timestamp,
    value: float,
    provider_name: str = "csv",
) -> pd.DataFrame:
    resolution_minutes = int(delivery_frame["resolution_minutes"].iloc[0])
    return pd.DataFrame(
        {
            "market": market,
            "delivery_start_utc": delivery_frame["timestamp_utc"],
            "delivery_end_utc": delivery_frame["timestamp_utc"] + pd.Timedelta(minutes=resolution_minutes),
            "forecast_price_eur_per_mwh": value,
            "issue_time_utc": decision_time_utc,
            "available_from_utc": decision_time_utc,
            "provider_name": provider_name,
            "scenario_id": None,
        }
    )


def _actual_forecast(
    delivery_frame: pd.DataFrame,
    *,
    market: str,
    decision_time_utc: pd.Timestamp,
    provider_name: str = "csv",
) -> pd.DataFrame:
    resolution_minutes = int(delivery_frame["resolution_minutes"].iloc[0])
    return pd.DataFrame(
        {
            "market": market,
            "delivery_start_utc": delivery_frame["timestamp_utc"],
            "delivery_end_utc": delivery_frame["timestamp_utc"] + pd.Timedelta(minutes=resolution_minutes),
            "forecast_price_eur_per_mwh": delivery_frame["price_eur_per_mwh"].values,
            "issue_time_utc": decision_time_utc,
            "available_from_utc": decision_time_utc,
            "provider_name": provider_name,
            "scenario_id": None,
        }
    )


def _write_revision_forecasts(
    *,
    day_ahead_frame: pd.DataFrame,
    timezone: str,
    tmp_path: Path,
    include_fcr: pd.DataFrame | None = None,
) -> tuple[Path, Path | None]:
    delivery_frame = day_ahead_frame[day_ahead_frame["timestamp_local"].dt.date == pd.Timestamp("2025-06-17").date()]
    gate_closure = pd.Timestamp("2025-06-16 12:00", tz=timezone).tz_convert("UTC")
    revision_one = pd.Timestamp("2025-06-17 06:00", tz=timezone).tz_convert("UTC")
    revision_two = pd.Timestamp("2025-06-17 12:00", tz=timezone).tz_convert("UTC")

    day_ahead_forecast = pd.concat(
        [
            _constant_forecast(delivery_frame, market="day_ahead", decision_time_utc=gate_closure, value=42.0),
            _actual_forecast(delivery_frame, market="day_ahead", decision_time_utc=revision_one),
            _actual_forecast(delivery_frame, market="day_ahead", decision_time_utc=revision_two),
        ],
        ignore_index=True,
    )
    day_ahead_path = tmp_path / "day_ahead_revision_forecasts.csv"
    day_ahead_forecast.to_csv(day_ahead_path, index=False)

    fcr_path: Path | None = None
    if include_fcr is not None:
        fcr_delivery = include_fcr[include_fcr["timestamp_local"].dt.date == pd.Timestamp("2025-06-17").date()]
        fcr_forecast = _actual_forecast(fcr_delivery, market="fcr_capacity", decision_time_utc=gate_closure)
        fcr_path = tmp_path / "fcr_revision_forecasts.csv"
        fcr_forecast.to_csv(fcr_path, index=False)
    return day_ahead_path, fcr_path


def _write_afrr_revision_forecasts(
    *,
    tmp_path: Path,
    timezone: str,
    market_data: dict[str, Path],
) -> dict[str, Path]:
    delivery_date = pd.Timestamp("2025-06-17").date()
    gate_closure = pd.Timestamp("2025-06-16 12:00", tz=timezone).tz_convert("UTC")
    revision_one = pd.Timestamp("2025-06-17 06:00", tz=timezone).tz_convert("UTC")
    revision_two = pd.Timestamp("2025-06-17 12:00", tz=timezone).tz_convert("UTC")
    paths: dict[str, Path] = {}
    for market_name in (
        "afrr_capacity_up",
        "afrr_capacity_down",
        "afrr_activation_price_up",
        "afrr_activation_price_down",
        "afrr_activation_ratio_up",
        "afrr_activation_ratio_down",
    ):
        frame = _load_frame(market_data[market_name], timezone=timezone)
        delivery_frame = frame[
            pd.to_datetime(frame["timestamp_utc"], utc=True).dt.tz_convert(timezone).dt.date == delivery_date
        ].reset_index(drop=True)
        if "activation_ratio" in market_name:
            gate_multiplier = 0.25
            revision_one_multiplier = 0.3
            revision_two_multiplier = 0.35
        else:
            gate_multiplier = 0.75
            revision_one_multiplier = 0.8
            revision_two_multiplier = 0.85
        forecast = pd.concat(
            [
                _constant_forecast(
                    delivery_frame,
                    market=market_name,
                    decision_time_utc=gate_closure,
                    value=float(delivery_frame["price_eur_per_mwh"].mean()) * gate_multiplier,
                ),
                _constant_forecast(
                    delivery_frame,
                    market=market_name,
                    decision_time_utc=revision_one,
                    value=float(delivery_frame["price_eur_per_mwh"].mean()) * revision_one_multiplier,
                ),
                _constant_forecast(
                    delivery_frame,
                    market=market_name,
                    decision_time_utc=revision_two,
                    value=float(delivery_frame["price_eur_per_mwh"].mean()) * revision_two_multiplier,
                ),
            ],
            ignore_index=True,
        )
        path = tmp_path / f"{market_name}_revision_forecasts.csv"
        forecast.to_csv(path, index=False)
        paths[market_name] = path
    return paths


def _write_revision_config(
    *,
    tmp_path: Path,
    market_data: dict[str, Path],
    workflow: str,
    assets: list[dict[str, object]],
    site: dict[str, object],
    day_ahead_forecast_path: Path,
    fcr_forecast_path: Path | None = None,
    afrr_forecast_paths: dict[str, Path] | None = None,
) -> Path:
    payload: dict[str, object] = {
        "schema_version": 4,
        "run_name": f"test-{workflow}",
        "market": {"id": "belgium"},
        "workflow": "schedule_revision",
        "forecast_provider": {
            "name": "csv",
            "day_ahead_path": str(day_ahead_forecast_path),
        },
        "timing": {
            "timezone": "Europe/Brussels",
            "resolution_minutes": 15,
            "rebalance_cadence_minutes": 15,
            "execution_lock_intervals": 1,
            "day_ahead_gate_closure_local": "12:00",
            "delivery_start_date": "2025-06-17",
            "delivery_end_date": "2025-06-17",
        },
        "site": site,
        "assets": assets,
        "degradation": {"mode": "throughput_linear", "throughput_cost_eur_per_mwh": 3.0},
        "data": {"day_ahead": {"actual_path": str(market_data["day_ahead"])}},
        "revision": {
            "base_workflow": workflow,
            "revision_market_mode": "public_checkpoint_reoptimization",
            "revision_checkpoints_local": ["06:00", "12:00"],
            "lock_policy": "committed_intervals_only",
            "allow_day_ahead_revision": False,
            "allow_fcr_revision": False,
            "allow_energy_revision": True,
            "max_revision_horizon_intervals": 16,
        },
        "artifacts": {
            "root_dir": str(tmp_path / "artifacts"),
            "save_inputs": True,
            "save_plots": False,
            "save_forecast_snapshots": True,
        },
    }
    if workflow == "da_plus_fcr":
        payload["data"]["fcr_capacity"] = {"actual_path": str(market_data["fcr_capacity"])}  # type: ignore[index]
        payload["fcr"] = {
            "product_id": "fcr_symmetric",
            "sustain_duration_minutes": 15,
            "settlement_mode": "capacity_only",
            "activation_mode": "none",
            "non_delivery_penalty_eur_per_mw": 0.0,
            "simplified_product_logic": True,
        }
        payload["forecast_provider"]["fcr_capacity_path"] = str(fcr_forecast_path)  # type: ignore[index]
    if workflow == "da_plus_afrr":
        payload["revision"]["max_revision_horizon_intervals"] = 96  # type: ignore[index]
        payload["data"].update(
            {
                "afrr_capacity_up": {"actual_path": str(market_data["afrr_capacity_up"])},
                "afrr_capacity_down": {"actual_path": str(market_data["afrr_capacity_down"])},
                "afrr_activation_price_up": {"actual_path": str(market_data["afrr_activation_price_up"])},
                "afrr_activation_price_down": {"actual_path": str(market_data["afrr_activation_price_down"])},
                "afrr_activation_ratio_up": {"actual_path": str(market_data["afrr_activation_ratio_up"])},
                "afrr_activation_ratio_down": {"actual_path": str(market_data["afrr_activation_ratio_down"])},
            }
        )
        payload["afrr"] = {
            "product_id": "afrr_asymmetric",
            "sustain_duration_minutes": 15,
            "settlement_mode": "capacity_plus_activation_expected_value",
            "activation_mode": "expected_value",
            "non_delivery_penalty_eur_per_mw": 0.0,
            "simplified_product_logic": True,
        }
        if afrr_forecast_paths is None:
            raise ValueError("aFRR revision configs require forecast paths")
        payload["forecast_provider"].update(  # type: ignore[call-arg]
            {
                "afrr_capacity_up_path": str(afrr_forecast_paths["afrr_capacity_up"]),
                "afrr_capacity_down_path": str(afrr_forecast_paths["afrr_capacity_down"]),
                "afrr_activation_price_up_path": str(afrr_forecast_paths["afrr_activation_price_up"]),
                "afrr_activation_price_down_path": str(afrr_forecast_paths["afrr_activation_price_down"]),
                "afrr_activation_ratio_up_path": str(afrr_forecast_paths["afrr_activation_ratio_up"]),
                "afrr_activation_ratio_down_path": str(afrr_forecast_paths["afrr_activation_ratio_down"]),
            }
        )
    config_path = tmp_path / f"{workflow}_revision.yaml"
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return config_path


def _single_asset_payload() -> list[dict[str, object]]:
    return [
        {
            "id": "be-bess-1",
            "kind": "battery",
            "battery": {
                "name": "be_bess_1",
                "power_mw": 1.0,
                "energy_mwh": 2.0,
                "initial_soc_mwh": 1.0,
                "terminal_soc_mwh": 1.0,
                "soc_min_mwh": 0.2,
                "soc_max_mwh": 1.9,
                "charge_efficiency": 0.95,
                "discharge_efficiency": 0.95,
                "connection_limit_mw": 1.0,
                "minimum_headroom_mwh": 0.1,
            },
        }
    ]


def _portfolio_payload() -> list[dict[str, object]]:
    return [
        {
            "id": "be-bess-1",
            "kind": "battery",
            "battery": {
                "name": "be_bess_1",
                "power_mw": 1.0,
                "energy_mwh": 2.0,
                "initial_soc_mwh": 1.3,
                "terminal_soc_mwh": 1.3,
                "soc_min_mwh": 0.2,
                "soc_max_mwh": 1.8,
                "charge_efficiency": 0.95,
                "discharge_efficiency": 0.95,
                "connection_limit_mw": 1.0,
                "minimum_headroom_mwh": 0.1,
            },
        },
        {
            "id": "be-bess-2",
            "kind": "battery",
            "battery": {
                "name": "be_bess_2",
                "power_mw": 1.0,
                "energy_mwh": 1.5,
                "initial_soc_mwh": 1.0,
                "terminal_soc_mwh": 1.0,
                "soc_min_mwh": 0.15,
                "soc_max_mwh": 1.35,
                "charge_efficiency": 0.94,
                "discharge_efficiency": 0.94,
                "connection_limit_mw": 1.0,
                "minimum_headroom_mwh": 0.05,
            },
        },
    ]


def test_schedule_revision_backtest_writes_revision_and_reconciliation_artifacts(
    tmp_path: Path, two_day_market_data: dict[str, Path]
) -> None:
    day_ahead = _load_frame(two_day_market_data["day_ahead"], timezone="Europe/Brussels")
    day_ahead_forecast_path, _ = _write_revision_forecasts(
        day_ahead_frame=day_ahead,
        timezone="Europe/Brussels",
        tmp_path=tmp_path,
    )
    config_path = _write_revision_config(
        tmp_path=tmp_path,
        market_data=two_day_market_data,
        workflow="da_only",
        assets=_single_asset_payload(),
        site={"id": "revision-site", "poi_import_limit_mw": 1.0, "poi_export_limit_mw": 1.0},
        day_ahead_forecast_path=day_ahead_forecast_path,
    )
    config = BacktestConfig.model_validate(yaml.safe_load(config_path.read_text(encoding="utf-8")))
    result = run_walk_forward(config)

    assert result.output_dir is not None
    assert (result.output_dir / "baseline_schedule.parquet").exists()
    assert (result.output_dir / "revision_schedule.parquet").exists()
    assert (result.output_dir / "schedule_lineage.parquet").exists()
    assert (result.output_dir / "reconciliation_breakdown.parquet").exists()
    assert (result.output_dir / "reconciliation_summary.json").exists()
    assert "schedule_revision" in set(result.decision_log["decision_type"])
    assert "revision_01" in set(result.site_dispatch["schedule_version"])
    merged = result.baseline_schedule.merge(
        result.revision_schedule[["timestamp_utc", "net_export_mw"]].rename(columns={"net_export_mw": "revised"}),
        on="timestamp_utc",
        how="inner",
    )
    assert (merged["net_export_mw"] - merged["revised"]).abs().sum() > 0.0
    summary = load_report_summary(result.output_dir)
    assert summary["workflow"] == "schedule_revision"
    assert summary["base_workflow"] == "da_only"
    assert "reconciliation" in summary


def test_schedule_revision_portfolio_fcr_keeps_reserve_locked(
    tmp_path: Path, two_day_market_data: dict[str, Path]
) -> None:
    day_ahead = _load_frame(two_day_market_data["day_ahead"], timezone="Europe/Brussels")
    fcr = _load_frame(two_day_market_data["fcr_capacity"], timezone="Europe/Brussels")
    day_ahead_forecast_path, fcr_forecast_path = _write_revision_forecasts(
        day_ahead_frame=day_ahead,
        timezone="Europe/Brussels",
        tmp_path=tmp_path,
        include_fcr=fcr,
    )
    config_path = _write_revision_config(
        tmp_path=tmp_path,
        market_data=two_day_market_data,
        workflow="da_plus_fcr",
        assets=_portfolio_payload(),
        site={"id": "revision-portfolio", "poi_import_limit_mw": 1.5, "poi_export_limit_mw": 1.5},
        day_ahead_forecast_path=day_ahead_forecast_path,
        fcr_forecast_path=fcr_forecast_path,
    )
    config = BacktestConfig.model_validate(yaml.safe_load(config_path.read_text(encoding="utf-8")))
    result = run_walk_forward(config)

    assert result.output_dir is not None
    baseline = result.baseline_schedule[["timestamp_utc", "fcr_reserved_mw"]].rename(
        columns={"fcr_reserved_mw": "baseline_reserved"}
    )
    realized = result.site_dispatch[["timestamp_utc", "fcr_reserved_mw"]]
    merged = baseline.merge(realized, on="timestamp_utc", how="inner")
    assert (merged["baseline_reserved"] - merged["fcr_reserved_mw"]).abs().max() < 1e-9
    assert "revision_01" in set(result.site_dispatch["schedule_version"])


def test_export_revision_and_reconcile_round_trip(tmp_path: Path, two_day_market_data: dict[str, Path]) -> None:
    day_ahead = _load_frame(two_day_market_data["day_ahead"], timezone="Europe/Brussels")
    day_ahead_forecast_path, _ = _write_revision_forecasts(
        day_ahead_frame=day_ahead,
        timezone="Europe/Brussels",
        tmp_path=tmp_path,
    )
    config_path = _write_revision_config(
        tmp_path=tmp_path,
        market_data=two_day_market_data,
        workflow="da_only",
        assets=_single_asset_payload(),
        site={"id": "revision-site", "poi_import_limit_mw": 1.0, "poi_export_limit_mw": 1.0},
        day_ahead_forecast_path=day_ahead_forecast_path,
    )
    config = BacktestConfig.model_validate(yaml.safe_load(config_path.read_text(encoding="utf-8")))
    result = run_walk_forward(config)

    export_dir = export_revision(result.output_dir)
    manifest = json.loads((export_dir / "manifest.json").read_text(encoding="utf-8"))
    assert {
        "baseline_schedule.csv",
        "baseline_schedule.parquet",
        "baseline_schedule.json",
        "latest_revised_schedule.csv",
        "latest_revised_schedule.parquet",
        "latest_revised_schedule.json",
        "schedule_lineage.csv",
        "schedule_lineage.parquet",
        "schedule_lineage.json",
        "asset_revision_allocation.csv",
        "asset_revision_allocation.parquet",
        "asset_revision_allocation.json",
    } == {entry["path"] for entry in manifest["files"]}

    reconciliation_dir = reconcile_run(result.output_dir, config_path)
    summary = json.loads((reconciliation_dir / "reconciliation_summary.json").read_text(encoding="utf-8"))
    assert "baseline_expected_total_pnl_eur" in summary
    assert "revised_expected_total_pnl_eur" in summary
    assert "realized_total_pnl_eur" in summary


def test_schedule_revision_portfolio_afrr_keeps_reserve_locked(
    tmp_path: Path, two_day_market_data: dict[str, Path]
) -> None:
    day_ahead = _load_frame(two_day_market_data["day_ahead"], timezone="Europe/Brussels")
    day_ahead_forecast_path, _ = _write_revision_forecasts(
        day_ahead_frame=day_ahead,
        timezone="Europe/Brussels",
        tmp_path=tmp_path,
    )
    afrr_forecasts = _write_afrr_revision_forecasts(
        tmp_path=tmp_path,
        timezone="Europe/Brussels",
        market_data=two_day_market_data,
    )
    config_path = _write_revision_config(
        tmp_path=tmp_path,
        market_data=two_day_market_data,
        workflow="da_plus_afrr",
        assets=_portfolio_payload(),
        site={"id": "revision-afrr-portfolio", "poi_import_limit_mw": 1.5, "poi_export_limit_mw": 1.5},
        day_ahead_forecast_path=day_ahead_forecast_path,
        afrr_forecast_paths=afrr_forecasts,
    )
    config = BacktestConfig.model_validate(yaml.safe_load(config_path.read_text(encoding="utf-8")))
    result = run_walk_forward(config)

    baseline = result.baseline_schedule[["timestamp_utc", "afrr_up_reserved_mw", "afrr_down_reserved_mw"]].rename(
        columns={
            "afrr_up_reserved_mw": "baseline_up",
            "afrr_down_reserved_mw": "baseline_down",
        }
    )
    realized = result.site_dispatch[["timestamp_utc", "afrr_up_reserved_mw", "afrr_down_reserved_mw"]]
    merged = baseline.merge(realized, on="timestamp_utc", how="inner")
    assert (merged["baseline_up"] - merged["afrr_up_reserved_mw"]).abs().max() < 1e-9
    assert (merged["baseline_down"] - merged["afrr_down_reserved_mw"]).abs().max() < 1e-9
    assert "revision_01" in set(result.site_dispatch["schedule_version"])
    assert result.reconciliation_breakdown is not None
    assert result.reconciliation_breakdown["activation_settlement_deviation_eur"].abs().sum() > 0.0
