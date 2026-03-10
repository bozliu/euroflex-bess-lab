from __future__ import annotations

from pathlib import Path

from euroflex_bess_lab.analytics.reporting import load_report_summary
from euroflex_bess_lab.backtesting.engine import run_walk_forward
from euroflex_bess_lab.config import BacktestConfig


def _asset_payload(name: str) -> dict[str, object]:
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
    market_data: dict[str, Path],
    tmp_path: Path,
    *,
    market_id: str,
    timezone: str,
    workflow: str = "da_plus_imbalance",
    cadence: int = 15,
    portfolio: bool = False,
) -> BacktestConfig:
    assets = [_asset_payload("asset-1")]
    site = {"id": f"{market_id}-site", "poi_import_limit_mw": 1.0, "poi_export_limit_mw": 1.0}
    if portfolio:
        assets.append(_asset_payload("asset-2"))
        site = {"id": f"{market_id}-site", "poi_import_limit_mw": 1.4, "poi_export_limit_mw": 1.4}
    payload = {
        "schema_version": 4,
        "run_name": f"unit-walk-forward-{market_id}",
        "market": {"id": market_id},
        "workflow": workflow,
        "forecast_provider": {"name": "persistence"},
        "timing": {
            "timezone": timezone,
            "resolution_minutes": 15,
            "rebalance_cadence_minutes": cadence,
            "execution_lock_intervals": 1,
            "day_ahead_gate_closure_local": "12:00",
            "delivery_start_date": "2025-06-17",
            "delivery_end_date": "2025-06-17",
        },
        "site": site,
        "assets": assets,
        "degradation": {"mode": "throughput_linear", "throughput_cost_eur_per_mwh": 3.0},
        "data": {
            "day_ahead": {"actual_path": str(market_data["day_ahead"])},
        },
        "artifacts": {
            "root_dir": str(tmp_path),
            "save_inputs": True,
            "save_plots": False,
            "save_forecast_snapshots": True,
        },
    }
    if workflow == "da_plus_imbalance":
        payload["data"]["imbalance"] = {"actual_path": str(market_data["imbalance"])}
    if workflow == "da_plus_fcr":
        payload["data"]["fcr_capacity"] = {"actual_path": str(market_data["fcr_capacity"])}
        payload["fcr"] = {"product_id": "fcr_symmetric"}
    return BacktestConfig.model_validate(payload)


def test_walk_forward_respects_rebalance_cadence(two_day_market_data: dict[str, Path], tmp_path: Path) -> None:
    config = _build_config(two_day_market_data, tmp_path, market_id="belgium", timezone="Europe/Brussels", cadence=60)
    result = run_walk_forward(config)
    imbalance_decisions = result.decision_log[result.decision_log["decision_type"] == "imbalance_rebalance"]
    assert len(imbalance_decisions) == 24
    unique_decision_times = result.site_dispatch["decision_time_utc"].dropna().nunique()
    assert unique_decision_times == 24


def test_dst_day_runs_with_expected_interval_count(tmp_path: Path) -> None:
    fixture_path = Path(__file__).parent / "fixtures" / "raw" / "entsoe" / "entsoe_day_ahead_belgium_dst.xml"
    config = BacktestConfig.model_validate(
        {
            "schema_version": 4,
            "run_name": "dst-day",
            "market": {"id": "belgium"},
            "workflow": "da_only",
            "forecast_provider": {"name": "perfect_foresight"},
            "timing": {
                "timezone": "Europe/Brussels",
                "resolution_minutes": 15,
                "rebalance_cadence_minutes": 15,
                "execution_lock_intervals": 1,
                "day_ahead_gate_closure_local": "12:00",
                "delivery_start_date": "2024-03-31",
                "delivery_end_date": "2024-03-31",
            },
            "site": {"id": "dst-site", "poi_import_limit_mw": 1.0, "poi_export_limit_mw": 1.0},
            "assets": [_asset_payload("dst-asset")],
            "degradation": {"mode": "rainflow_offline"},
            "data": {"day_ahead": {"actual_path": str(fixture_path)}},
            "artifacts": {
                "root_dir": str(tmp_path),
                "save_inputs": True,
                "save_plots": False,
                "save_forecast_snapshots": True,
            },
        }
    )

    result = run_walk_forward(config)
    summary = load_report_summary(result.output_dir)
    assert summary["interval_count"] == 92
    assert abs(summary["oracle_gap_total_pnl_eur"]) < 1e-6


def test_netherlands_walk_forward_emits_dual_price_settlement(
    two_day_market_data_nl: dict[str, Path], tmp_path: Path
) -> None:
    config = _build_config(
        two_day_market_data_nl,
        tmp_path,
        market_id="netherlands",
        timezone="Europe/Amsterdam",
        workflow="da_plus_imbalance",
    )
    result = run_walk_forward(config)
    assert result.market_id == "netherlands"
    assert result.settlement_breakdown["imbalance_revenue_eur"].abs().sum() > 0.0
    assert result.site_dispatch["workflow_family"].eq("da_plus_imbalance").all()


def test_da_plus_fcr_creates_one_decision_per_delivery_day(
    two_day_market_data: dict[str, Path], tmp_path: Path
) -> None:
    config = _build_config(
        two_day_market_data, tmp_path, market_id="belgium", timezone="Europe/Brussels", workflow="da_plus_fcr"
    )
    result = run_walk_forward(config)
    decision_types = set(result.decision_log["decision_type"])
    assert decision_types == {"day_ahead_fcr_nomination"}
    assert len(result.decision_log) == 1
    assert result.site_dispatch["fcr_reserved_mw"].gt(0.0).any()
    assert result.site_dispatch["imbalance_revenue_eur"].eq(0.0).all()


def test_portfolio_da_plus_fcr_keeps_asset_dispatches(two_day_market_data: dict[str, Path], tmp_path: Path) -> None:
    config = _build_config(
        two_day_market_data,
        tmp_path,
        market_id="belgium",
        timezone="Europe/Brussels",
        workflow="da_plus_fcr",
        portfolio=True,
    )
    result = run_walk_forward(config)
    assert result.run_scope == "portfolio"
    assert result.asset_dispatch["asset_id"].nunique() == 2
    assert result.site_dispatch["fcr_reserved_mw"].gt(0.0).any()


def test_da_plus_fcr_oracle_parity_holds(two_day_market_data_nl: dict[str, Path], tmp_path: Path) -> None:
    config = _build_config(
        two_day_market_data_nl,
        tmp_path,
        market_id="netherlands",
        timezone="Europe/Amsterdam",
        workflow="da_plus_fcr",
    )
    config.forecast_provider.name = "perfect_foresight"
    result = run_walk_forward(config)
    summary = load_report_summary(result.output_dir)
    assert abs(summary["oracle_gap_total_pnl_eur"]) < 1e-6
    assert summary["reserve_product_id"] == "fcr_symmetric"
