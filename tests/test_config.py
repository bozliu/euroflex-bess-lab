from __future__ import annotations

from pathlib import Path

import pytest

from euroflex_bess_lab.config import BacktestConfig


def _battery_payload(name: str = "asset") -> dict[str, object]:
    return {
        "id": name,
        "kind": "battery",
        "battery": {
            "name": name,
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


def _base_payload(tmp_path: Path) -> dict[str, object]:
    csv_path = str(tmp_path / "dummy.csv")
    return {
        "schema_version": 4,
        "run_name": "config-test",
        "market": {"id": "belgium"},
        "workflow": "da_only",
        "forecast_provider": {"name": "perfect_foresight"},
        "timing": {
            "timezone": "Europe/Brussels",
            "resolution_minutes": 15,
            "rebalance_cadence_minutes": 15,
            "execution_lock_intervals": 1,
            "day_ahead_gate_closure_local": "12:00",
            "delivery_start_date": "2025-06-17",
            "delivery_end_date": "2025-06-17",
        },
        "site": {
            "id": "test-site",
            "poi_import_limit_mw": 1.0,
            "poi_export_limit_mw": 1.0,
        },
        "assets": [_battery_payload("asset-1")],
        "degradation": {"mode": "throughput_linear", "throughput_cost_eur_per_mwh": 3.0},
        "data": {"day_ahead": {"actual_path": csv_path}},
        "artifacts": {
            "root_dir": str(tmp_path / "artifacts"),
            "save_inputs": False,
            "save_plots": False,
            "save_forecast_snapshots": False,
        },
    }


def test_da_plus_fcr_requires_fcr_data(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path)
    payload["workflow"] = "da_plus_fcr"
    payload["fcr"] = {"product_id": "fcr_symmetric"}
    with pytest.raises(ValueError, match="FCR capacity actual input is required"):
        BacktestConfig.model_validate(payload)


def test_da_plus_fcr_requires_csv_fcr_forecast_path(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path)
    payload["workflow"] = "da_plus_fcr"
    payload["forecast_provider"] = {
        "name": "csv",
        "day_ahead_path": str(tmp_path / "day_ahead.csv"),
    }
    payload["data"]["fcr_capacity"] = {"actual_path": str(tmp_path / "fcr.csv")}  # type: ignore[index]
    payload["fcr"] = {"product_id": "fcr_symmetric"}
    with pytest.raises(ValueError, match="fcr_capacity_path"):
        BacktestConfig.model_validate(payload)


def test_da_plus_afrr_requires_afrr_block(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path)
    payload["workflow"] = "da_plus_afrr"
    payload["data"].update(
        {
            "afrr_capacity_up": {"actual_path": str(tmp_path / "afrr_capacity_up.csv")},
            "afrr_capacity_down": {"actual_path": str(tmp_path / "afrr_capacity_down.csv")},
            "afrr_activation_price_up": {"actual_path": str(tmp_path / "afrr_activation_price_up.csv")},
            "afrr_activation_price_down": {"actual_path": str(tmp_path / "afrr_activation_price_down.csv")},
            "afrr_activation_ratio_up": {"actual_path": str(tmp_path / "afrr_activation_ratio_up.csv")},
            "afrr_activation_ratio_down": {"actual_path": str(tmp_path / "afrr_activation_ratio_down.csv")},
        }
    )
    with pytest.raises(ValueError, match="afrr configuration block is required"):
        BacktestConfig.model_validate(payload)


def test_da_plus_afrr_requires_all_actual_inputs(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path)
    payload["workflow"] = "da_plus_afrr"
    payload["afrr"] = {"product_id": "afrr_asymmetric"}
    payload["data"]["afrr_capacity_up"] = {"actual_path": str(tmp_path / "afrr_capacity_up.csv")}  # type: ignore[index]
    with pytest.raises(ValueError, match="aFRR actual inputs are required"):
        BacktestConfig.model_validate(payload)


def test_da_plus_afrr_requires_csv_forecast_paths(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path)
    payload["workflow"] = "da_plus_afrr"
    payload["afrr"] = {"product_id": "afrr_asymmetric"}
    payload["forecast_provider"] = {
        "name": "csv",
        "day_ahead_path": str(tmp_path / "day_ahead.csv"),
    }
    payload["data"].update(
        {
            "afrr_capacity_up": {"actual_path": str(tmp_path / "afrr_capacity_up.csv")},
            "afrr_capacity_down": {"actual_path": str(tmp_path / "afrr_capacity_down.csv")},
            "afrr_activation_price_up": {"actual_path": str(tmp_path / "afrr_activation_price_up.csv")},
            "afrr_activation_price_down": {"actual_path": str(tmp_path / "afrr_activation_price_down.csv")},
            "afrr_activation_ratio_up": {"actual_path": str(tmp_path / "afrr_activation_ratio_up.csv")},
            "afrr_activation_ratio_down": {"actual_path": str(tmp_path / "afrr_activation_ratio_down.csv")},
        }
    )
    with pytest.raises(ValueError, match="CSV forecast provider requires aFRR paths"):
        BacktestConfig.model_validate(payload)


def test_assets_must_be_non_empty(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path)
    payload["assets"] = []
    with pytest.raises(ValueError, match="assets must contain at least one asset"):
        BacktestConfig.model_validate(payload)


def test_asset_ids_must_be_unique(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path)
    payload["assets"] = [_battery_payload("asset-1"), _battery_payload("asset-1")]
    with pytest.raises(ValueError, match="asset ids must be unique"):
        BacktestConfig.model_validate(payload)


def test_portfolio_da_plus_imbalance_is_rejected(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path)
    payload["workflow"] = "da_plus_imbalance"
    payload["data"]["imbalance"] = {"actual_path": str(tmp_path / "imbalance.csv")}  # type: ignore[index]
    payload["assets"] = [_battery_payload("asset-1"), _battery_payload("asset-2")]
    with pytest.raises(ValueError, match="Portfolio da_plus_imbalance"):
        BacktestConfig.model_validate(payload)


def test_single_asset_run_scope_is_derived(tmp_path: Path) -> None:
    config = BacktestConfig.model_validate(_base_payload(tmp_path))
    assert config.run_scope == "single_asset"


def test_multi_asset_run_scope_is_portfolio(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path)
    payload["workflow"] = "da_plus_fcr"
    payload["data"]["fcr_capacity"] = {"actual_path": str(tmp_path / "fcr.csv")}  # type: ignore[index]
    payload["fcr"] = {"product_id": "fcr_symmetric"}
    payload["site"] = {"id": "site", "poi_import_limit_mw": 1.5, "poi_export_limit_mw": 1.5}
    payload["assets"] = [_battery_payload("asset-1"), _battery_payload("asset-2")]
    config = BacktestConfig.model_validate(payload)
    assert config.run_scope == "portfolio"


def test_schedule_revision_requires_revision_block(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path)
    payload["workflow"] = "schedule_revision"
    with pytest.raises(ValueError, match="revision configuration block is required"):
        BacktestConfig.model_validate(payload)


def test_schedule_revision_rejects_duplicate_checkpoints(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path)
    payload["workflow"] = "schedule_revision"
    payload["revision"] = {
        "base_workflow": "da_only",
        "revision_market_mode": "public_checkpoint_reoptimization",
        "revision_checkpoints_local": ["06:00", "06:00"],
        "lock_policy": "committed_intervals_only",
        "allow_day_ahead_revision": False,
        "allow_fcr_revision": False,
        "allow_energy_revision": True,
        "max_revision_horizon_intervals": 12,
    }
    with pytest.raises(ValueError, match="must be unique"):
        BacktestConfig.model_validate(payload)


def test_schedule_revision_rejects_portfolio_imbalance_scope(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path)
    payload["workflow"] = "schedule_revision"
    payload["data"]["imbalance"] = {"actual_path": str(tmp_path / "imbalance.csv")}  # type: ignore[index]
    payload["assets"] = [_battery_payload("asset-1"), _battery_payload("asset-2")]
    payload["revision"] = {
        "base_workflow": "da_plus_imbalance",
        "revision_market_mode": "public_checkpoint_reoptimization",
        "revision_checkpoints_local": ["06:00", "12:00"],
        "lock_policy": "committed_intervals_only",
        "allow_day_ahead_revision": False,
        "allow_fcr_revision": False,
        "allow_energy_revision": True,
        "max_revision_horizon_intervals": 12,
    }
    with pytest.raises(ValueError, match="requires a single asset"):
        BacktestConfig.model_validate(payload)


def test_schedule_revision_afrr_rejects_allow_afrr_revision(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path)
    payload["workflow"] = "schedule_revision"
    payload["afrr"] = {"product_id": "afrr_asymmetric"}
    payload["data"].update(
        {
            "afrr_capacity_up": {"actual_path": str(tmp_path / "afrr_capacity_up.csv")},
            "afrr_capacity_down": {"actual_path": str(tmp_path / "afrr_capacity_down.csv")},
            "afrr_activation_price_up": {"actual_path": str(tmp_path / "afrr_activation_price_up.csv")},
            "afrr_activation_price_down": {"actual_path": str(tmp_path / "afrr_activation_price_down.csv")},
            "afrr_activation_ratio_up": {"actual_path": str(tmp_path / "afrr_activation_ratio_up.csv")},
            "afrr_activation_ratio_down": {"actual_path": str(tmp_path / "afrr_activation_ratio_down.csv")},
        }
    )
    payload["revision"] = {
        "base_workflow": "da_plus_afrr",
        "revision_market_mode": "public_checkpoint_reoptimization",
        "revision_checkpoints_local": ["06:00", "12:00"],
        "lock_policy": "committed_intervals_only",
        "allow_day_ahead_revision": False,
        "allow_fcr_revision": False,
        "allow_afrr_revision": True,
        "allow_energy_revision": True,
        "max_revision_horizon_intervals": 12,
    }
    with pytest.raises(ValueError, match="allow_afrr_revision must remain false"):
        BacktestConfig.model_validate(payload)


def test_scenario_mode_is_belgium_first(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path)
    payload["market"] = {"id": "netherlands"}
    payload["forecast_provider"] = {
        "name": "csv",
        "mode": "scenario_bundle",
        "day_ahead_path": str(tmp_path / "day_ahead_scenarios.csv"),
    }
    with pytest.raises(ValueError, match="Belgium-first"):
        BacktestConfig.model_validate(payload)


def test_point_only_providers_reject_scenario_mode(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path)
    payload["forecast_provider"] = {"name": "persistence", "mode": "scenario_bundle"}
    with pytest.raises(ValueError, match="only supports forecast_provider.mode=point"):
        BacktestConfig.model_validate(payload)


def test_non_default_risk_requires_scenario_mode(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path)
    payload["risk"] = {"mode": "downside_penalty", "penalty_lambda": 2.0}
    with pytest.raises(ValueError, match="require forecast_provider.mode=scenario_bundle"):
        BacktestConfig.model_validate(payload)
