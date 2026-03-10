from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from euroflex_bess_lab.backtesting.engine import run_walk_forward
from euroflex_bess_lab.config import load_config
from euroflex_bess_lab.data.io import load_price_series
from euroflex_bess_lab.forecasts import CsvForecastProvider, PersistenceForecastProvider


def test_persistence_provider_is_deterministic(two_day_market_data: dict[str, Path]) -> None:
    actual = load_price_series(
        two_day_market_data["day_ahead"],
        name="day_ahead_actual",
        market="day_ahead",
        zone="10YBE----------2",
        source="unit_test",
    )
    delivery_frame = actual.data[
        actual.data["timestamp_local"].dt.date == pd.Timestamp("2025-06-17").date()
    ].reset_index(drop=True)
    provider = PersistenceForecastProvider()
    decision_time = pd.Timestamp("2025-06-16T10:00:00Z")

    first = provider.get_forecast(
        market="day_ahead",
        decision_time_utc=decision_time,
        delivery_frame=delivery_frame,
        actual_frame=actual.data,
    )
    second = provider.get_forecast(
        market="day_ahead",
        decision_time_utc=decision_time,
        delivery_frame=delivery_frame,
        actual_frame=actual.data,
    )

    pd.testing.assert_frame_equal(first.reset_index(drop=True), second.reset_index(drop=True))


def test_csv_provider_rejects_lookahead_forecasts(tmp_path: Path, two_day_market_data: dict[str, Path]) -> None:
    actual = load_price_series(
        two_day_market_data["day_ahead"],
        name="day_ahead_actual",
        market="day_ahead",
        zone="10YBE----------2",
        source="unit_test",
    )
    delivery_frame = actual.data[
        actual.data["timestamp_local"].dt.date == pd.Timestamp("2025-06-17").date()
    ].reset_index(drop=True)
    decision_time = pd.Timestamp("2025-06-16T10:00:00Z")
    resolution_minutes = int(delivery_frame["resolution_minutes"].iloc[0])
    hidden = pd.DataFrame(
        {
            "market": "day_ahead",
            "delivery_start_utc": delivery_frame["timestamp_utc"],
            "delivery_end_utc": delivery_frame["timestamp_utc"] + pd.Timedelta(minutes=resolution_minutes),
            "forecast_price_eur_per_mwh": delivery_frame["price_eur_per_mwh"] + 5.0,
            "issue_time_utc": decision_time + pd.Timedelta(hours=1),
            "available_from_utc": decision_time + pd.Timedelta(hours=1),
            "provider_name": "csv",
            "scenario_id": "base",
        }
    )
    forecast_path = tmp_path / "hidden_forecast.csv"
    hidden.to_csv(forecast_path, index=False)
    provider = CsvForecastProvider(day_ahead_path=forecast_path, scenario_id="base")

    with pytest.raises(ValueError, match="lookahead|visible vintages"):
        provider.get_forecast(
            market="day_ahead",
            decision_time_utc=decision_time,
            delivery_frame=delivery_frame,
            actual_frame=actual.data,
        )


def test_persistence_provider_supports_fcr_capacity(two_day_market_data: dict[str, Path]) -> None:
    actual = load_price_series(
        two_day_market_data["fcr_capacity"],
        name="fcr_capacity_actual",
        market="fcr_capacity",
        zone="10YBE----------2",
        source="unit_test",
    )
    delivery_frame = actual.data[
        actual.data["timestamp_local"].dt.date == pd.Timestamp("2025-06-17").date()
    ].reset_index(drop=True)
    provider = PersistenceForecastProvider()
    decision_time = pd.Timestamp("2025-06-16T10:00:00Z")
    first = provider.get_forecast(
        market="fcr_capacity",
        decision_time_utc=decision_time,
        delivery_frame=delivery_frame,
        actual_frame=actual.data,
    )
    second = provider.get_forecast(
        market="fcr_capacity",
        decision_time_utc=decision_time,
        delivery_frame=delivery_frame,
        actual_frame=actual.data,
    )
    pd.testing.assert_frame_equal(first.reset_index(drop=True), second.reset_index(drop=True))


def test_csv_provider_is_deterministic_for_fcr_capacity(tmp_path: Path, two_day_market_data: dict[str, Path]) -> None:
    actual = load_price_series(
        two_day_market_data["fcr_capacity"],
        name="fcr_capacity_actual",
        market="fcr_capacity",
        zone="10YBE----------2",
        source="unit_test",
    )
    delivery_frame = actual.data[
        actual.data["timestamp_local"].dt.date == pd.Timestamp("2025-06-17").date()
    ].reset_index(drop=True)
    decision_time = pd.Timestamp("2025-06-16T10:00:00Z")
    resolution_minutes = int(delivery_frame["resolution_minutes"].iloc[0])
    snapshot = pd.DataFrame(
        {
            "market": "fcr_capacity",
            "delivery_start_utc": delivery_frame["timestamp_utc"],
            "delivery_end_utc": delivery_frame["timestamp_utc"] + pd.Timedelta(minutes=resolution_minutes),
            "forecast_price_eur_per_mwh": delivery_frame["price_eur_per_mwh"] + 1.0,
            "issue_time_utc": decision_time,
            "available_from_utc": decision_time,
            "provider_name": "csv",
            "scenario_id": "base",
        }
    )
    day_ahead_path = tmp_path / "day_ahead_forecast.csv"
    fcr_path = tmp_path / "fcr_forecast.csv"
    snapshot.to_csv(fcr_path, index=False)
    snapshot.assign(market="day_ahead").to_csv(day_ahead_path, index=False)
    provider = CsvForecastProvider(day_ahead_path=day_ahead_path, fcr_capacity_path=fcr_path, scenario_id="base")
    first = provider.get_forecast(
        market="fcr_capacity",
        decision_time_utc=decision_time,
        delivery_frame=delivery_frame,
        actual_frame=actual.data,
    )
    second = provider.get_forecast(
        market="fcr_capacity",
        decision_time_utc=decision_time,
        delivery_frame=delivery_frame,
        actual_frame=actual.data,
    )
    pd.testing.assert_frame_equal(first.reset_index(drop=True), second.reset_index(drop=True))


def test_csv_provider_rejects_scenario_weights_that_do_not_sum_to_one(
    tmp_path: Path, two_day_market_data: dict[str, Path]
) -> None:
    actual = load_price_series(
        two_day_market_data["day_ahead"],
        name="day_ahead_actual",
        market="day_ahead",
        zone="10YBE----------2",
        source="unit_test",
    )
    delivery_frame = actual.data[
        actual.data["timestamp_local"].dt.date == pd.Timestamp("2025-06-17").date()
    ].reset_index(drop=True)
    decision_time = pd.Timestamp("2025-06-16T10:00:00Z")
    resolution_minutes = int(delivery_frame["resolution_minutes"].iloc[0])
    bad_bundle = pd.concat(
        [
            pd.DataFrame(
                {
                    "market": "day_ahead",
                    "delivery_start_utc": delivery_frame["timestamp_utc"],
                    "delivery_end_utc": delivery_frame["timestamp_utc"] + pd.Timedelta(minutes=resolution_minutes),
                    "forecast_price_eur_per_mwh": delivery_frame["price_eur_per_mwh"] + 5.0,
                    "issue_time_utc": decision_time,
                    "available_from_utc": decision_time,
                    "provider_name": "csv",
                    "scenario_id": "upside",
                    "scenario_weight": 0.7,
                }
            ),
            pd.DataFrame(
                {
                    "market": "day_ahead",
                    "delivery_start_utc": delivery_frame["timestamp_utc"],
                    "delivery_end_utc": delivery_frame["timestamp_utc"] + pd.Timedelta(minutes=resolution_minutes),
                    "forecast_price_eur_per_mwh": delivery_frame["price_eur_per_mwh"] - 5.0,
                    "issue_time_utc": decision_time,
                    "available_from_utc": decision_time,
                    "provider_name": "csv",
                    "scenario_id": "downside",
                    "scenario_weight": 0.5,
                }
            ),
        ],
        ignore_index=True,
    )
    forecast_path = tmp_path / "scenario_bundle.csv"
    bad_bundle.to_csv(forecast_path, index=False)
    provider = CsvForecastProvider(day_ahead_path=forecast_path, mode="scenario_bundle")

    with pytest.raises(ValueError, match="sum to 1.0"):
        provider.get_forecast(
            market="day_ahead",
            decision_time_utc=decision_time,
            delivery_frame=delivery_frame,
            actual_frame=actual.data,
        )


def test_csv_provider_rejects_scenario_bundle_with_missing_intervals(
    tmp_path: Path, two_day_market_data: dict[str, Path]
) -> None:
    actual = load_price_series(
        two_day_market_data["day_ahead"],
        name="day_ahead_actual",
        market="day_ahead",
        zone="10YBE----------2",
        source="unit_test",
    )
    delivery_frame = actual.data[
        actual.data["timestamp_local"].dt.date == pd.Timestamp("2025-06-17").date()
    ].reset_index(drop=True)
    decision_time = pd.Timestamp("2025-06-16T10:00:00Z")
    resolution_minutes = int(delivery_frame["resolution_minutes"].iloc[0])
    incomplete = pd.concat(
        [
            pd.DataFrame(
                {
                    "market": "day_ahead",
                    "delivery_start_utc": delivery_frame["timestamp_utc"],
                    "delivery_end_utc": delivery_frame["timestamp_utc"] + pd.Timedelta(minutes=resolution_minutes),
                    "forecast_price_eur_per_mwh": delivery_frame["price_eur_per_mwh"] + 2.0,
                    "issue_time_utc": decision_time,
                    "available_from_utc": decision_time,
                    "provider_name": "csv",
                    "scenario_id": "full",
                    "scenario_weight": 0.5,
                }
            ),
            pd.DataFrame(
                {
                    "market": "day_ahead",
                    "delivery_start_utc": delivery_frame["timestamp_utc"].iloc[:-1],
                    "delivery_end_utc": delivery_frame["timestamp_utc"].iloc[:-1]
                    + pd.Timedelta(minutes=resolution_minutes),
                    "forecast_price_eur_per_mwh": (delivery_frame["price_eur_per_mwh"].iloc[:-1] - 2.0).values,
                    "issue_time_utc": decision_time,
                    "available_from_utc": decision_time,
                    "provider_name": "csv",
                    "scenario_id": "missing_tail",
                    "scenario_weight": 0.5,
                }
            ),
        ],
        ignore_index=True,
    )
    forecast_path = tmp_path / "scenario_missing.csv"
    incomplete.to_csv(forecast_path, index=False)
    provider = CsvForecastProvider(day_ahead_path=forecast_path, mode="scenario_bundle")

    with pytest.raises(ValueError, match="missing one or more delivery intervals"):
        provider.get_forecast(
            market="day_ahead",
            decision_time_utc=decision_time,
            delivery_frame=delivery_frame,
            actual_frame=actual.data,
        )


def test_custom_python_provider_runs_with_visible_inputs_across_workflows(tmp_path: Path) -> None:
    module_path = tmp_path / "custom_provider.py"
    call_log_path = tmp_path / "calls.jsonl"
    module_path.write_text(
        """
from __future__ import annotations

import json
import pandas as pd


class RecordingForecaster:
    def __init__(self, call_log_path: str):
        self.call_log_path = call_log_path

    def initialize(self, *, config, run_context) -> None:
        self.run_context = run_context

    def generate_forecast(self, *, market, decision_time_utc, delivery_frame, visible_inputs):
        for frame in visible_inputs.values():
            if not frame.empty and (pd.to_datetime(frame["timestamp_utc"], utc=True) >= decision_time_utc).any():
                raise ValueError("visible_inputs contains lookahead data")
        with open(self.call_log_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps({"market": market, "keys": sorted(visible_inputs), "decision_time_utc": str(decision_time_utc)}) + "\\n")
        resolution_minutes = int(delivery_frame["resolution_minutes"].iloc[0])
        source = visible_inputs.get(market)
        if source is not None and not source.empty:
            forecast_price = pd.Series([float(source["price_eur_per_mwh"].iloc[-1])] * len(delivery_frame))
        else:
            forecast_price = delivery_frame["price_eur_per_mwh"].astype(float)
        return pd.DataFrame(
            {
                "market": market,
                "delivery_start_utc": delivery_frame["timestamp_utc"],
                "delivery_end_utc": delivery_frame["timestamp_utc"] + pd.Timedelta(minutes=resolution_minutes),
                "forecast_price_eur_per_mwh": forecast_price,
                "issue_time_utc": decision_time_utc,
                "available_from_utc": decision_time_utc,
                "provider_name": "custom_python",
            }
        )
""",
        encoding="utf-8",
    )
    config_paths = [
        Path("tests/fixtures/example_configs/basic/belgium_da_only_base.yaml"),
        Path("tests/fixtures/example_configs/reserve/belgium_da_plus_fcr_base.yaml"),
        Path("tests/fixtures/example_configs/reserve/belgium_da_plus_afrr_base.yaml"),
        Path("tests/fixtures/example_configs/reserve/belgium_schedule_revision_da_plus_afrr_base.yaml"),
    ]
    project_root = Path(__file__).resolve().parents[1]
    for source in config_paths:
        config = load_config(project_root / source)
        config.forecast_provider.name = "custom_python"
        config.forecast_provider.module_path = module_path
        config.forecast_provider.class_name = "RecordingForecaster"
        config.forecast_provider.init_kwargs = {"call_log_path": str(call_log_path)}
        config.artifacts.root_dir = tmp_path / source.stem
        run_walk_forward(config)

    logs = [line for line in call_log_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert logs, "Expected the custom provider to be invoked"
    payloads = [json.loads(line) for line in logs]
    assert any({"day_ahead", "afrr_capacity_up", "afrr_capacity_down"}.issubset(set(row["keys"])) for row in payloads)


def test_custom_python_provider_rejects_missing_class(tmp_path: Path) -> None:
    module_path = tmp_path / "bad_provider.py"
    module_path.write_text("class SomethingElse: pass\n", encoding="utf-8")
    config = load_config(
        Path(__file__).resolve().parents[1] / "tests/fixtures/example_configs/basic/belgium_da_only_base.yaml"
    )
    config.forecast_provider.name = "custom_python"
    config.forecast_provider.module_path = module_path
    config.forecast_provider.class_name = "NotThere"
    with pytest.raises(ValueError, match="was not found"):
        from euroflex_bess_lab.benchmarks import BenchmarkRegistry

        BenchmarkRegistry.build_provider(config)


def test_custom_python_provider_rejects_invalid_snapshot_columns(tmp_path: Path) -> None:
    module_path = tmp_path / "invalid_provider.py"
    module_path.write_text(
        """
import pandas as pd

class InvalidForecaster:
    def initialize(self, *, config, run_context) -> None:
        pass

    def generate_forecast(self, *, market, decision_time_utc, delivery_frame, visible_inputs):
        return pd.DataFrame({"market": [market]})
""",
        encoding="utf-8",
    )
    config = load_config(
        Path(__file__).resolve().parents[1] / "tests/fixtures/example_configs/basic/belgium_da_only_base.yaml"
    )
    config.forecast_provider.name = "custom_python"
    config.forecast_provider.module_path = module_path
    config.forecast_provider.class_name = "InvalidForecaster"
    from euroflex_bess_lab.benchmarks import BenchmarkRegistry

    provider = BenchmarkRegistry.build_provider(config)
    actual = load_price_series(
        config.data.day_ahead.actual_path,
        name="day_ahead_actual",
        market="day_ahead",
        zone="10YBE----------2",
        source="unit_test",
    )
    delivery_frame = actual.data[
        actual.data["timestamp_local"].dt.date == pd.Timestamp("2025-06-17").date()
    ].reset_index(drop=True)
    with pytest.raises(ValueError, match="missing required columns"):
        provider.get_forecast(
            market="day_ahead",
            decision_time_utc=pd.Timestamp("2025-06-16T10:00:00Z"),
            delivery_frame=delivery_frame,
            actual_frame=actual.data,
            visible_inputs={
                "day_ahead": actual.data[actual.data["timestamp_utc"] < pd.Timestamp("2025-06-16T10:00:00Z")]
            },
        )


def test_custom_python_provider_supports_scenario_bundle_runs(tmp_path: Path) -> None:
    module_path = tmp_path / "scenario_provider.py"
    module_path.write_text(
        """
from __future__ import annotations

import pandas as pd


class ScenarioForecaster:
    def initialize(self, *, config, run_context) -> None:
        self.run_context = run_context

    def generate_forecast(self, *, market, decision_time_utc, delivery_frame, visible_inputs):
        resolution_minutes = int(delivery_frame["resolution_minutes"].iloc[0])
        base = delivery_frame["price_eur_per_mwh"].astype(float).reset_index(drop=True)
        optimistic = base + 8.0
        defensive = base - 12.0
        return pd.concat(
            [
                pd.DataFrame(
                    {
                        "market": market,
                        "delivery_start_utc": delivery_frame["timestamp_utc"],
                        "delivery_end_utc": delivery_frame["timestamp_utc"] + pd.Timedelta(minutes=resolution_minutes),
                        "forecast_price_eur_per_mwh": optimistic,
                        "issue_time_utc": decision_time_utc,
                        "available_from_utc": decision_time_utc,
                        "provider_name": "custom_python",
                        "scenario_id": "optimistic",
                        "scenario_weight": 0.55,
                    }
                ),
                pd.DataFrame(
                    {
                        "market": market,
                        "delivery_start_utc": delivery_frame["timestamp_utc"],
                        "delivery_end_utc": delivery_frame["timestamp_utc"] + pd.Timedelta(minutes=resolution_minutes),
                        "forecast_price_eur_per_mwh": defensive,
                        "issue_time_utc": decision_time_utc,
                        "available_from_utc": decision_time_utc,
                        "provider_name": "custom_python",
                        "scenario_id": "defensive",
                        "scenario_weight": 0.45,
                    }
                ),
            ],
            ignore_index=True,
        )
""",
        encoding="utf-8",
    )
    config_paths = [
        Path("tests/fixtures/example_configs/basic/belgium_da_only_base.yaml"),
        Path("tests/fixtures/example_configs/reserve/belgium_da_plus_fcr_base.yaml"),
        Path("tests/fixtures/example_configs/reserve/belgium_da_plus_afrr_base.yaml"),
        Path("tests/fixtures/example_configs/reserve/belgium_schedule_revision_da_plus_afrr_base.yaml"),
    ]
    project_root = Path(__file__).resolve().parents[1]
    for source in config_paths:
        config = load_config(project_root / source)
        config.forecast_provider.name = "custom_python"
        config.forecast_provider.mode = "scenario_bundle"
        config.forecast_provider.module_path = module_path
        config.forecast_provider.class_name = "ScenarioForecaster"
        config.risk.mode = "downside_penalty"
        config.risk.penalty_lambda = 0.5
        config.artifacts.root_dir = tmp_path / source.stem
        result = run_walk_forward(config)
        assert result.output_dir is not None
        summary = json.loads((result.output_dir / "summary.json").read_text(encoding="utf-8"))
        assert summary["scenario_analysis"]["scenario_count"] == 2
        assert summary["forecast_mode"] == "scenario_bundle"
