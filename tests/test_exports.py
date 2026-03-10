from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from euroflex_bess_lab.backtesting.engine import run_walk_forward
from euroflex_bess_lab.config import load_config
from euroflex_bess_lab.exports import export_bids, export_revision, export_schedule

EXAMPLE_CONFIG_DIR = Path(__file__).resolve().parents[1] / "examples" / "configs"
INTERNAL_EXAMPLE_CONFIG_DIR = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "example_configs"
EXAMPLE_BASIC_DIR = INTERNAL_EXAMPLE_CONFIG_DIR / "basic"
EXAMPLE_RESERVE_DIR = INTERNAL_EXAMPLE_CONFIG_DIR / "reserve"


def test_export_schedule_writes_site_and_asset_payloads(tmp_path: Path) -> None:
    config = load_config(EXAMPLE_RESERVE_DIR / "belgium_portfolio_da_plus_fcr_base.yaml")
    config.artifacts.root_dir = tmp_path / "runs"
    result = run_walk_forward(config)

    export_dir = export_schedule(result.output_dir)
    manifest = json.loads((export_dir / "manifest.json").read_text(encoding="utf-8"))
    site_schedule = pd.read_csv(export_dir / "site_schedule.csv")
    asset_allocation = pd.read_csv(export_dir / "asset_allocation.csv")

    assert {
        "site_schedule.csv",
        "site_schedule.parquet",
        "site_schedule.json",
        "asset_allocation.csv",
        "asset_allocation.parquet",
        "asset_allocation.json",
    } == {entry["path"] for entry in manifest["files"]}
    assert manifest["metadata"]["export_kind"] == "schedule"
    assert site_schedule["run_scope"].eq("portfolio").all()
    assert "site_id" in site_schedule.columns
    assert "asset_id" in asset_allocation.columns


def test_export_profiles_change_manifest_and_field_sets(tmp_path: Path) -> None:
    config = load_config(EXAMPLE_RESERVE_DIR / "belgium_portfolio_da_plus_afrr_base.yaml")
    config.artifacts.root_dir = tmp_path / "runs"
    result = run_walk_forward(config)

    schedule_dir = export_schedule(result.output_dir, profile="operator")
    bids_dir = export_bids(result.output_dir, profile="bid_planning")
    schedule_manifest = json.loads((schedule_dir / "manifest.json").read_text(encoding="utf-8"))
    bids_manifest = json.loads((bids_dir / "manifest.json").read_text(encoding="utf-8"))
    operator_site = pd.read_csv(schedule_dir / "site_schedule.csv")
    bid_site = pd.read_csv(bids_dir / "site_bids.csv")

    assert schedule_manifest["metadata"]["profile"] == "operator"
    assert schedule_manifest["metadata"]["intended_consumer"] == "scheduler"
    assert "charge_mw" not in operator_site.columns
    assert bids_manifest["metadata"]["profile"] == "bid_planning"
    assert bids_manifest["metadata"]["intended_consumer"] == "trader_or_scheduler"
    assert "day_ahead_nominated_net_export_mw" in bid_site.columns


def test_submission_candidate_profiles_emit_submission_candidate_metadata(tmp_path: Path) -> None:
    config = load_config(EXAMPLE_RESERVE_DIR / "belgium_portfolio_da_plus_afrr_base.yaml")
    config.artifacts.root_dir = tmp_path / "runs"
    result = run_walk_forward(config)

    schedule_dir = export_schedule(result.output_dir, profile="submission_candidate")
    bids_dir = export_bids(result.output_dir, profile="submission_candidate")
    schedule_manifest = json.loads((schedule_dir / "manifest.json").read_text(encoding="utf-8"))
    bids_manifest = json.loads((bids_dir / "manifest.json").read_text(encoding="utf-8"))

    assert schedule_manifest["metadata"]["profile"] == "submission_candidate"
    assert schedule_manifest["metadata"]["live_submission_ready"] is False
    assert bids_manifest["metadata"]["profile"] == "submission_candidate"
    assert bids_manifest["metadata"]["intended_consumer"] == "execution_router_or_scheduler"


def test_export_bids_marks_reserve_assumptions_for_portfolio_da_plus_fcr(tmp_path: Path) -> None:
    config = load_config(EXAMPLE_RESERVE_DIR / "netherlands_portfolio_da_plus_fcr_base.yaml")
    config.artifacts.root_dir = tmp_path / "runs"
    result = run_walk_forward(config)

    export_dir = export_bids(result.output_dir)
    manifest = json.loads((export_dir / "manifest.json").read_text(encoding="utf-8"))
    site_bids = pd.read_csv(export_dir / "site_bids.csv")
    asset_annex = pd.read_csv(export_dir / "asset_reserve_allocation.csv")

    assert {
        "site_bids.csv",
        "site_bids.parquet",
        "site_bids.json",
        "asset_reserve_allocation.csv",
        "asset_reserve_allocation.parquet",
        "asset_reserve_allocation.json",
    } == {entry["path"] for entry in manifest["files"]}
    assert manifest["metadata"]["export_kind"] == "bids"
    assert site_bids["workflow"].eq("da_plus_fcr").all()
    assert "reserved_capacity_mw" in site_bids.columns
    assert "reserve_product_id" in site_bids.columns
    assert asset_annex["asset_id"].nunique() == 2


def test_export_bids_includes_afrr_up_and_down_columns(tmp_path: Path) -> None:
    config = load_config(EXAMPLE_RESERVE_DIR / "belgium_portfolio_da_plus_afrr_base.yaml")
    config.artifacts.root_dir = tmp_path / "runs"
    result = run_walk_forward(config)

    export_dir = export_bids(result.output_dir)
    site_bids = pd.read_csv(export_dir / "site_bids.csv")
    asset_annex = pd.read_csv(export_dir / "asset_reserve_allocation.csv")

    assert site_bids["workflow"].eq("da_plus_afrr").all()
    assert "afrr_up_reserved_mw" in site_bids.columns
    assert "afrr_down_reserved_mw" in site_bids.columns
    assert "afrr_up_reserved_mw" in asset_annex.columns
    assert "afrr_down_reserved_mw" in asset_annex.columns


def test_export_schedule_includes_baseline_and_revision_payloads_for_schedule_revision(tmp_path: Path) -> None:
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
    config.artifacts.root_dir = tmp_path / "runs"
    result = run_walk_forward(config)

    export_dir = export_schedule(result.output_dir)
    manifest = json.loads((export_dir / "manifest.json").read_text(encoding="utf-8"))
    assert "baseline_schedule.csv" in {entry["path"] for entry in manifest["files"]}
    assert "latest_revised_schedule.csv" in {entry["path"] for entry in manifest["files"]}


def test_export_revision_writes_revision_bundle(tmp_path: Path) -> None:
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
    config.artifacts.root_dir = tmp_path / "runs"
    result = run_walk_forward(config)

    export_dir = export_revision(result.output_dir)
    manifest = json.loads((export_dir / "manifest.json").read_text(encoding="utf-8"))
    assert "baseline_schedule.csv" in {entry["path"] for entry in manifest["files"]}
    assert "asset_revision_allocation.csv" in {entry["path"] for entry in manifest["files"]}
