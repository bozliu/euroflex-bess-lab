from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml

from euroflex_bess_lab.config import load_config
from euroflex_bess_lab.validation import doctor, validate_config_file, validate_data_file

EXAMPLE_CONFIG_DIR = Path(__file__).resolve().parents[1] / "examples" / "configs"
INTERNAL_EXAMPLE_CONFIG_DIR = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "example_configs"
EXAMPLE_BASIC_DIR = INTERNAL_EXAMPLE_CONFIG_DIR / "basic"
EXAMPLE_RESERVE_DIR = INTERNAL_EXAMPLE_CONFIG_DIR / "reserve"


def test_validate_config_reports_portfolio_imbalance_scope_failure(tmp_path: Path) -> None:
    config = load_config(EXAMPLE_RESERVE_DIR / "belgium_portfolio_da_plus_fcr_base.yaml")
    payload = config.model_dump(mode="json")
    payload["workflow"] = "da_plus_imbalance"
    payload["data"]["imbalance"] = {
        "actual_path": str(Path(config.data.day_ahead.actual_path).with_name("belgium_imbalance_prices.csv"))
    }
    path = tmp_path / "invalid.yaml"
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    report = validate_config_file(path)
    assert not report.ok
    assert any(check.name == "config_schema" and check.status == "fail" for check in report.checks)


def test_validate_data_detects_duplicate_timestamps(tmp_path: Path) -> None:
    config = load_config(EXAMPLE_BASIC_DIR / "belgium_da_plus_imbalance_base.yaml")
    frame = pd.read_csv(config.data.day_ahead.actual_path)
    duplicated = pd.concat([frame.iloc[[0]], frame], ignore_index=True)
    duplicated_path = tmp_path / "duplicated_day_ahead.csv"
    duplicated.to_csv(duplicated_path, index=False)
    config.data.day_ahead.actual_path = duplicated_path
    path = tmp_path / "invalid-data.yaml"
    path.write_text(yaml.safe_dump(config.model_dump(mode="json"), sort_keys=False), encoding="utf-8")

    report = validate_data_file(path)
    assert not report.ok
    assert any(check.name == "day_ahead_unique_timestamps" and check.status == "fail" for check in report.checks)


def test_validate_data_rejects_lookahead_csv_forecasts(tmp_path: Path) -> None:
    config = load_config(EXAMPLE_BASIC_DIR / "belgium_da_plus_imbalance_base.yaml")
    config.forecast_provider.name = "csv"
    config.forecast_provider.day_ahead_path = tmp_path / "day_ahead_forecast.csv"
    config.forecast_provider.imbalance_path = tmp_path / "imbalance_forecast.csv"

    day_ahead = pd.read_csv(config.data.day_ahead.actual_path)
    imbalance = pd.read_csv(config.data.imbalance.actual_path)  # type: ignore[arg-type]
    day_ahead_forecast = pd.DataFrame(
        {
            "market": "day_ahead",
            "delivery_start_utc": day_ahead["timestamp_utc"],
            "delivery_end_utc": (
                pd.to_datetime(day_ahead["timestamp_utc"], utc=True) + pd.Timedelta(minutes=15)
            ).astype(str),
            "forecast_price_eur_per_mwh": day_ahead["price_eur_per_mwh"],
            "issue_time_utc": "2025-06-16T12:00:00Z",
            "available_from_utc": "2025-06-18T00:00:00Z",
            "provider_name": "csv",
        }
    )
    imbalance_forecast = pd.DataFrame(
        {
            "market": "imbalance",
            "delivery_start_utc": imbalance["timestamp_utc"],
            "delivery_end_utc": (
                pd.to_datetime(imbalance["timestamp_utc"], utc=True) + pd.Timedelta(minutes=15)
            ).astype(str),
            "forecast_price_eur_per_mwh": imbalance["price_eur_per_mwh"],
            "issue_time_utc": "2025-06-17T00:00:00Z",
            "available_from_utc": "2025-06-18T00:00:00Z",
            "provider_name": "csv",
        }
    )
    day_ahead_forecast.to_csv(config.forecast_provider.day_ahead_path, index=False)
    imbalance_forecast.to_csv(config.forecast_provider.imbalance_path, index=False)
    path = tmp_path / "lookahead.yaml"
    path.write_text(yaml.safe_dump(config.model_dump(mode="json"), sort_keys=False), encoding="utf-8")

    report = validate_data_file(path)
    assert not report.ok
    assert any(check.name == "csv_forecasts" and check.status == "fail" for check in report.checks)


def test_validate_config_reports_positive_but_tiny_continuous_reserve_for_portfolio(tmp_path: Path) -> None:
    config = load_config(EXAMPLE_RESERVE_DIR / "belgium_portfolio_da_plus_fcr_base.yaml")
    config.site.poi_import_limit_mw = 0.01
    config.site.poi_export_limit_mw = 0.01
    path = tmp_path / "reserve-poi.yaml"
    path.write_text(yaml.safe_dump(config.model_dump(mode="json"), sort_keys=False), encoding="utf-8")
    report = validate_config_file(path)
    assert report.ok
    reserve_check = next(check for check in report.checks if check.name == "reserve_feasibility")
    assert reserve_check.status == "pass"
    assert reserve_check.context["max_theoretical_site_reserve_mw"] == 0.01


def test_doctor_reports_available_solver_and_dependencies(tmp_path: Path) -> None:
    config = load_config(EXAMPLE_BASIC_DIR / "netherlands_da_only_base.yaml")
    config.artifacts.root_dir = tmp_path / "artifacts"
    path = tmp_path / "doctor.yaml"
    path.write_text(yaml.safe_dump(config.model_dump(mode="json"), sort_keys=False), encoding="utf-8")

    report = doctor(path)
    assert report.ok
    assert any(check.name == "solver" and check.status == "pass" for check in report.checks)
    assert any(check.name == "artifact_root" and check.status == "pass" for check in report.checks)


def test_validate_config_fails_for_missing_custom_provider_module(tmp_path: Path) -> None:
    config = load_config(EXAMPLE_BASIC_DIR / "belgium_da_only_base.yaml")
    config.forecast_provider.name = "custom_python"
    config.forecast_provider.module_path = tmp_path / "missing_provider.py"
    config.forecast_provider.class_name = "MissingForecaster"
    path = tmp_path / "custom-provider.yaml"
    path.write_text(yaml.safe_dump(config.model_dump(mode="json"), sort_keys=False), encoding="utf-8")

    report = validate_config_file(path)
    assert not report.ok
    assert any(check.name == "forecast_provider_supported" and check.status == "fail" for check in report.checks)
