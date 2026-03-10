from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from euroflex_bess_lab.backtesting.engine import run_walk_forward
from euroflex_bess_lab.comparison import compare_runs
from euroflex_bess_lab.config import load_config

INTERNAL_EXAMPLE_CONFIG_DIR = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "example_configs"
EXAMPLE_BASIC_DIR = INTERNAL_EXAMPLE_CONFIG_DIR / "basic"
EXAMPLE_RESERVE_DIR = INTERNAL_EXAMPLE_CONFIG_DIR / "reserve"


def test_v4_artifacts_include_stable_market_and_portfolio_fields(tmp_path: Path) -> None:
    config = load_config(EXAMPLE_RESERVE_DIR / "belgium_portfolio_da_plus_fcr_base.yaml")
    config.artifacts.root_dir = tmp_path
    result = run_walk_forward(config)
    run_dir = result.output_dir
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    for key in (
        "market_id",
        "market_timezone",
        "settlement_basis",
        "gate_closure_definition",
        "benchmark_family",
        "data_provenance",
        "reserve_product_id",
        "reserve_capacity_revenue_eur",
        "reserve_penalty_eur",
        "site_id",
        "run_scope",
        "asset_count",
        "poi_import_limit_mw",
        "poi_export_limit_mw",
    ):
        assert key in summary

    decision_log = pd.read_parquet(run_dir / "decision_log.parquet")
    settlement_breakdown = pd.read_parquet(run_dir / "settlement_breakdown.parquet")
    forecast_snapshots = pd.read_parquet(run_dir / "forecast_snapshots.parquet")
    site_dispatch = pd.read_parquet(run_dir / "site_dispatch.parquet")
    asset_dispatch = pd.read_parquet(run_dir / "asset_dispatch.parquet")
    asset_pnl = pd.read_parquet(run_dir / "asset_pnl_attribution.parquet")

    for frame in (decision_log, settlement_breakdown, forecast_snapshots, site_dispatch, asset_dispatch):
        assert "market_id" in frame.columns
        assert "workflow_family" in frame.columns
        assert "run_scope" in frame.columns
    assert "site_id" in site_dispatch.columns
    assert "asset_id" in asset_dispatch.columns
    assert "total_pnl_eur" in asset_pnl.columns
    assert "reserve_capacity_revenue_eur" in settlement_breakdown.columns
    assert "fcr_capacity" in set(forecast_snapshots["market"])


def test_compare_runs_handles_mixed_single_asset_and_portfolio_runs(tmp_path: Path) -> None:
    single_config = load_config(EXAMPLE_RESERVE_DIR / "belgium_da_plus_fcr_base.yaml")
    portfolio_config = load_config(EXAMPLE_RESERVE_DIR / "belgium_portfolio_da_plus_fcr_base.yaml")
    single_config.artifacts.root_dir = tmp_path / "single"
    portfolio_config.artifacts.root_dir = tmp_path / "portfolio"
    single_result = run_walk_forward(single_config)
    portfolio_result = run_walk_forward(portfolio_config)
    comparison_dir = compare_runs(
        [single_result.output_dir, portfolio_result.output_dir], tmp_path / "comparison", group_by="workflow"
    )
    comparison = pd.read_csv(comparison_dir / "comparison.csv")
    assert set(comparison["run_scope"]) == {"single_asset", "portfolio"}
    assert "portfolio_uplift_vs_single_asset_eur" in comparison.columns
