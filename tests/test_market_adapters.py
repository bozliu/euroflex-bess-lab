from __future__ import annotations

from pathlib import Path

import pytest

from euroflex_bess_lab.config import BacktestConfig
from euroflex_bess_lab.markets import MarketRegistry


def _build_asset(name: str) -> dict[str, object]:
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


def _build_config(
    market_id: str,
    timezone: str,
    market_data: dict[str, Path],
    *,
    workflow: str = "da_plus_imbalance",
    portfolio: bool = False,
) -> BacktestConfig:
    assets = [_build_asset("asset-1")]
    site = {"id": f"{market_id}-site", "poi_import_limit_mw": 1.0, "poi_export_limit_mw": 1.0}
    if portfolio:
        assets.append(_build_asset("asset-2"))
        site = {"id": f"{market_id}-site", "poi_import_limit_mw": 1.4, "poi_export_limit_mw": 1.4}
    payload = {
        "schema_version": 4,
        "run_name": f"adapter-{market_id}",
        "market": {"id": market_id},
        "workflow": workflow,
        "forecast_provider": {"name": "perfect_foresight"},
        "timing": {
            "timezone": timezone,
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
        "artifacts": {
            "root_dir": "artifacts/test",
            "save_inputs": False,
            "save_plots": False,
            "save_forecast_snapshots": False,
        },
    }
    if workflow == "da_plus_imbalance":
        payload["data"]["imbalance"] = {"actual_path": str(market_data["imbalance"])}
    if workflow == "da_plus_fcr":
        payload["data"]["fcr_capacity"] = {"actual_path": str(market_data["fcr_capacity"])}
        payload["fcr"] = {"product_id": "fcr_symmetric"}
    if workflow == "da_plus_afrr":
        for field_name in (
            "afrr_capacity_up",
            "afrr_capacity_down",
            "afrr_activation_price_up",
            "afrr_activation_price_down",
            "afrr_activation_ratio_up",
            "afrr_activation_ratio_down",
        ):
            payload["data"][field_name] = {"actual_path": str(market_data[field_name])}
        payload["afrr"] = {"product_id": "afrr_asymmetric"}
    return BacktestConfig.model_validate(payload)


def test_belgium_adapter_contract(two_day_market_data: dict[str, Path]) -> None:
    config = _build_config("belgium", "Europe/Brussels", two_day_market_data)
    adapter = MarketRegistry.get("belgium")
    adapter.validate_timing(config)
    actuals = adapter.load_actuals(config)
    schedule = adapter.decision_schedule(config)
    assert actuals.day_ahead.data["zone"].eq("10YBE----------2").all()
    assert actuals.imbalance is not None
    assert schedule["market_id"].eq("belgium").all()
    assert adapter.default_benchmarks() == ("perfect_foresight", "persistence", "csv")
    assert adapter.supported_reserve_products() == ("fcr_symmetric", "afrr_asymmetric")


def test_netherlands_adapter_contract(two_day_market_data_nl: dict[str, Path]) -> None:
    config = _build_config("netherlands", "Europe/Amsterdam", two_day_market_data_nl)
    adapter = MarketRegistry.get("netherlands")
    adapter.validate_timing(config)
    actuals = adapter.load_actuals(config)
    schedule = adapter.decision_schedule(config)
    settlement = adapter.settlement_engine("da_plus_imbalance")
    assert actuals.day_ahead.data["zone"].eq("10YNL----------L").all()
    assert actuals.imbalance is not None
    assert "imbalance_shortage_price_eur_per_mwh" in actuals.imbalance.data.columns
    assert schedule["market_id"].eq("netherlands").all()
    assert settlement.settlement_basis == "dual_price_shortage_surplus"
    assert adapter.supported_reserve_products() == ("fcr_symmetric", "afrr_asymmetric")


def test_reserve_adapter_contracts_load_fcr_capacity_for_single_and_portfolio(
    two_day_market_data: dict[str, Path], two_day_market_data_nl: dict[str, Path]
) -> None:
    for market_id, timezone, market_data in (
        ("belgium", "Europe/Brussels", two_day_market_data),
        ("netherlands", "Europe/Amsterdam", two_day_market_data_nl),
    ):
        for portfolio in (False, True):
            config = _build_config(market_id, timezone, market_data, workflow="da_plus_fcr", portfolio=portfolio)
            adapter = MarketRegistry.get(market_id)
            actuals = adapter.load_actuals(config)
            reserve = adapter.build_reserve_product(config)
            assert actuals.fcr_capacity is not None
            assert reserve is not None
            assert reserve.product_id == "fcr_symmetric"


def test_belgium_afrr_adapter_contract(two_day_market_data: dict[str, Path]) -> None:
    config = _build_config("belgium", "Europe/Brussels", two_day_market_data, workflow="da_plus_afrr", portfolio=True)
    adapter = MarketRegistry.get("belgium")
    adapter.validate_timing(config)
    actuals = adapter.load_actuals(config)
    reserve = adapter.build_reserve_product(config)
    assert actuals.afrr_capacity_up is not None
    assert actuals.afrr_capacity_down is not None
    assert actuals.afrr_activation_price_up is not None
    assert actuals.afrr_activation_ratio_down is not None
    assert reserve is not None
    assert reserve.product_id == "afrr_asymmetric"


def test_netherlands_afrr_is_explicitly_rejected(two_day_market_data_nl: dict[str, Path]) -> None:
    config = _build_config("netherlands", "Europe/Amsterdam", two_day_market_data_nl, workflow="da_plus_afrr")
    adapter = MarketRegistry.get("netherlands")
    with pytest.raises(ValueError, match="not yet supported"):
        adapter.validate_timing(config)
