from __future__ import annotations

from pathlib import Path

from euroflex_bess_lab.analytics.reporting import load_report_summary
from euroflex_bess_lab.backtesting.engine import run_walk_forward
from euroflex_bess_lab.config import load_config

EXAMPLE_CONFIG_DIR = Path(__file__).resolve().parents[1] / "examples" / "configs"
INTERNAL_EXAMPLE_CONFIG_DIR = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "example_configs"
EXAMPLE_BASIC_DIR = INTERNAL_EXAMPLE_CONFIG_DIR / "basic"
EXAMPLE_RESERVE_DIR = INTERNAL_EXAMPLE_CONFIG_DIR / "reserve"
EXAMPLE_CANONICAL_DIR = EXAMPLE_CONFIG_DIR / "canonical"


def test_example_belgium_backtest_writes_v4_artifacts(tmp_path: Path) -> None:
    config = load_config(EXAMPLE_BASIC_DIR / "belgium_da_plus_imbalance_base.yaml")
    config.artifacts.root_dir = tmp_path
    result = run_walk_forward(config)
    assert result.output_dir is not None
    assert (result.output_dir / "site_dispatch.parquet").exists()
    assert (result.output_dir / "asset_dispatch.parquet").exists()
    assert (result.output_dir / "asset_pnl_attribution.parquet").exists()
    assert (result.output_dir / "decision_log.parquet").exists()
    assert (result.output_dir / "forecast_snapshots.parquet").exists()
    assert (result.output_dir / "summary.json").exists()
    summary = load_report_summary(result.output_dir)
    assert summary["benchmark_name"] == "belgium.da_plus_imbalance.perfect_foresight.single_asset"
    assert summary["market_id"] == "belgium"
    assert summary["run_scope"] == "single_asset"
    assert abs(summary["oracle_gap_total_pnl_eur"]) < 1e-6


def test_example_netherlands_backtest_writes_expected_outputs(tmp_path: Path) -> None:
    config = load_config(EXAMPLE_BASIC_DIR / "netherlands_da_plus_imbalance_base.yaml")
    config.artifacts.root_dir = tmp_path
    result = run_walk_forward(config)
    assert result.output_dir is not None
    dispatch = result.site_dispatch
    assert "imbalance_revenue_eur" in dispatch.columns
    assert "imbalance_shortage_price_eur_per_mwh" in dispatch.columns
    assert dispatch["decision_type"].eq("imbalance_rebalance").any()
    assert (result.output_dir / "normalized_inputs" / "day_ahead.parquet").exists()
    assert (result.output_dir / "normalized_inputs" / "imbalance.parquet").exists()
    summary = load_report_summary(result.output_dir)
    assert summary["market_id"] == "netherlands"
    assert summary["settlement_basis"] == "dual_price_shortage_surplus"


def test_oracle_parity_holds_for_example_da_only_run(tmp_path: Path) -> None:
    config = load_config(EXAMPLE_BASIC_DIR / "netherlands_da_only_base.yaml")
    config.artifacts.root_dir = tmp_path
    result = run_walk_forward(config)
    summary = load_report_summary(result.output_dir)
    assert summary["benchmark_name"] == "netherlands.da_only.perfect_foresight.single_asset"
    assert abs(summary["oracle_gap_total_pnl_eur"]) < 1e-6
    assert abs(result.pnl.total_pnl_eur - result.oracle.total_pnl_eur) < 1e-6


def test_example_fcr_backtest_writes_reserve_outputs(tmp_path: Path) -> None:
    config = load_config(EXAMPLE_RESERVE_DIR / "belgium_da_plus_fcr_base.yaml")
    config.artifacts.root_dir = tmp_path
    result = run_walk_forward(config)
    assert result.output_dir is not None
    dispatch = result.site_dispatch
    assert "fcr_reserved_mw" in dispatch.columns
    assert dispatch["fcr_reserved_mw"].gt(0.0).any()
    summary = load_report_summary(result.output_dir)
    assert summary["benchmark_name"] == "belgium.da_plus_fcr.perfect_foresight.single_asset"
    assert summary["reserve_product_id"] == "fcr_symmetric"
    assert summary["reserve_capacity_revenue_eur"] >= 0.0


def test_portfolio_fcr_backtest_writes_portfolio_outputs(tmp_path: Path) -> None:
    config = load_config(EXAMPLE_RESERVE_DIR / "belgium_portfolio_da_plus_fcr_base.yaml")
    config.artifacts.root_dir = tmp_path
    result = run_walk_forward(config)
    summary = load_report_summary(result.output_dir)
    assert summary["run_scope"] == "portfolio"
    assert summary["asset_count"] == 2
    assert result.asset_dispatch["asset_id"].nunique() == 2
    assert result.site_dispatch["fcr_reserved_mw"].gt(0.0).any()
    assert result.asset_pnl_attribution.shape[0] == 2


def test_example_afrr_backtest_writes_expected_outputs(tmp_path: Path) -> None:
    config = load_config(EXAMPLE_RESERVE_DIR / "belgium_da_plus_afrr_base.yaml")
    config.artifacts.root_dir = tmp_path
    result = run_walk_forward(config)
    assert result.output_dir is not None
    dispatch = result.site_dispatch
    assert "afrr_up_reserved_mw" in dispatch.columns
    assert "afrr_down_reserved_mw" in dispatch.columns
    assert dispatch["afrr_up_reserved_mw"].gt(0.0).any() or dispatch["afrr_down_reserved_mw"].gt(0.0).any()
    summary = load_report_summary(result.output_dir)
    assert summary["benchmark_name"] == "belgium.da_plus_afrr.perfect_foresight.single_asset"
    assert summary["reserve_product_id"] == "afrr_asymmetric"
    assert summary["reserve_activation_revenue_eur"] >= 0.0


def test_portfolio_schedule_revision_afrr_writes_revision_outputs(tmp_path: Path) -> None:
    config = load_config(EXAMPLE_RESERVE_DIR / "belgium_portfolio_schedule_revision_da_plus_afrr_base.yaml")
    config.artifacts.root_dir = tmp_path
    result = run_walk_forward(config)
    assert result.output_dir is not None
    assert (result.output_dir / "baseline_schedule.parquet").exists()
    assert (result.output_dir / "revision_schedule.parquet").exists()
    summary = load_report_summary(result.output_dir)
    assert summary["workflow"] == "schedule_revision"
    assert summary["base_workflow"] == "da_plus_afrr"
    assert summary["run_scope"] == "portfolio"


def test_schedule_revision_backtest_writes_revision_outputs(tmp_path: Path) -> None:
    config = load_config(EXAMPLE_BASIC_DIR / "belgium_da_only_base.yaml")
    payload = config.model_dump(mode="json")
    payload["workflow"] = "schedule_revision"
    payload["revision"] = {
        "base_workflow": "da_only",
        "revision_market_mode": "public_checkpoint_reoptimization",
        "revision_checkpoints_local": ["06:00", "12:00"],
        "lock_policy": "committed_intervals_only",
        "allow_day_ahead_revision": False,
        "allow_fcr_revision": False,
        "allow_energy_revision": True,
        "max_revision_horizon_intervals": 12,
    }
    config = type(config).model_validate(payload)
    config.artifacts.root_dir = tmp_path
    result = run_walk_forward(config)
    assert result.output_dir is not None
    assert (result.output_dir / "baseline_schedule.parquet").exists()
    assert (result.output_dir / "revision_schedule.parquet").exists()
    assert (result.output_dir / "schedule_lineage.parquet").exists()
    assert (result.output_dir / "reconciliation_summary.json").exists()
    summary = load_report_summary(result.output_dir)
    assert summary["workflow"] == "schedule_revision"
    assert summary["base_workflow"] == "da_only"


def test_explicit_point_mode_matches_default_behavior(tmp_path: Path) -> None:
    config = load_config(EXAMPLE_BASIC_DIR / "netherlands_da_only_base.yaml")
    config.artifacts.root_dir = tmp_path / "default"
    baseline = run_walk_forward(config)

    point_config = load_config(EXAMPLE_BASIC_DIR / "netherlands_da_only_base.yaml")
    point_config.forecast_provider.mode = "point"
    point_config.artifacts.root_dir = tmp_path / "point"
    explicit = run_walk_forward(point_config)

    assert baseline.pnl.total_pnl_eur == explicit.pnl.total_pnl_eur
    assert baseline.site_dispatch["net_export_mw"].equals(explicit.site_dispatch["net_export_mw"])


def test_canonical_belgium_full_stack_supports_scenario_mode(tmp_path: Path) -> None:
    module_path = tmp_path / "scenario_provider.py"
    module_path.write_text(
        """
from __future__ import annotations

import pandas as pd


class CanonicalScenarioForecaster:
    def initialize(self, *, config, run_context) -> None:
        self.run_context = run_context

    def generate_forecast(self, *, market, decision_time_utc, delivery_frame, visible_inputs):
        resolution_minutes = int(delivery_frame["resolution_minutes"].iloc[0])
        base = delivery_frame["price_eur_per_mwh"].astype(float).reset_index(drop=True)
        upside = base + 6.0
        downside = base - 10.0
        return pd.concat(
            [
                pd.DataFrame(
                    {
                        "market": market,
                        "delivery_start_utc": delivery_frame["timestamp_utc"],
                        "delivery_end_utc": delivery_frame["timestamp_utc"] + pd.Timedelta(minutes=resolution_minutes),
                        "forecast_price_eur_per_mwh": upside,
                        "issue_time_utc": decision_time_utc,
                        "available_from_utc": decision_time_utc,
                        "provider_name": "custom_python",
                        "scenario_id": "upside",
                        "scenario_weight": 0.6,
                    }
                ),
                pd.DataFrame(
                    {
                        "market": market,
                        "delivery_start_utc": delivery_frame["timestamp_utc"],
                        "delivery_end_utc": delivery_frame["timestamp_utc"] + pd.Timedelta(minutes=resolution_minutes),
                        "forecast_price_eur_per_mwh": downside,
                        "issue_time_utc": decision_time_utc,
                        "available_from_utc": decision_time_utc,
                        "provider_name": "custom_python",
                        "scenario_id": "downside",
                        "scenario_weight": 0.4,
                    }
                ),
            ],
            ignore_index=True,
        )
""",
        encoding="utf-8",
    )
    config = load_config(EXAMPLE_CANONICAL_DIR / "belgium_full_stack.yaml")
    config.forecast_provider.name = "custom_python"
    config.forecast_provider.mode = "scenario_bundle"
    config.forecast_provider.module_path = module_path
    config.forecast_provider.class_name = "CanonicalScenarioForecaster"
    config.risk.mode = "downside_penalty"
    config.risk.penalty_lambda = 0.5
    config.artifacts.root_dir = tmp_path

    result = run_walk_forward(config)

    summary = load_report_summary(result.output_dir)
    assert summary["forecast_mode"] == "scenario_bundle"
    assert summary["risk_mode"] == "downside_penalty"
    assert summary["scenario_analysis"]["scenario_count"] == 2
    assert summary["reconciliation"]["scenario_analysis"]["nearest_scenario_id"] in {"upside", "downside"}
    baseline = result.baseline_schedule[["timestamp_utc", "afrr_up_reserved_mw", "afrr_down_reserved_mw"]]
    revised = result.site_dispatch[["timestamp_utc", "afrr_up_reserved_mw", "afrr_down_reserved_mw"]]
    merged = baseline.merge(revised, on="timestamp_utc", suffixes=("_baseline", "_revised"))
    assert (merged["afrr_up_reserved_mw_baseline"] - merged["afrr_up_reserved_mw_revised"]).abs().max() < 1e-9
    assert (merged["afrr_down_reserved_mw_baseline"] - merged["afrr_down_reserved_mw_revised"]).abs().max() < 1e-9
