from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import yaml
from typer.testing import CliRunner

from euroflex_bess_lab.cli import app
from euroflex_bess_lab.config import load_config

runner = CliRunner()
PROJECT_ROOT = Path(__file__).resolve().parents[1]
INTERNAL_EXAMPLE_CONFIG_DIR = PROJECT_ROOT / "tests" / "fixtures" / "example_configs"
EXAMPLE_BASIC_DIR = INTERNAL_EXAMPLE_CONFIG_DIR / "basic"
EXAMPLE_RESERVE_DIR = INTERNAL_EXAMPLE_CONFIG_DIR / "reserve"


def _write_temp_config(source: Path, tmp_path: Path) -> Path:
    config = load_config(source)
    config.artifacts.root_dir = tmp_path / "artifacts"
    config_path = tmp_path / source.name
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(yaml.safe_dump(config.model_dump(mode="json"), sort_keys=False), encoding="utf-8")
    return config_path


def test_version_cli_prints_version() -> None:
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert "1.1.0" in result.output


def test_backtest_cli_generic_form_produces_v4_artifacts(tmp_path: Path) -> None:
    config_path = _write_temp_config(EXAMPLE_BASIC_DIR / "belgium_da_plus_imbalance_base.yaml", tmp_path)
    result = runner.invoke(
        app,
        [
            "backtest",
            str(config_path),
            "--market",
            "belgium",
            "--workflow",
            "da_plus_imbalance",
        ],
    )

    assert result.exit_code == 0, result.output
    artifact_dirs = list((tmp_path / "artifacts").iterdir())
    assert len(artifact_dirs) == 1
    run_dir = artifact_dirs[0]
    assert (run_dir / "site_dispatch.parquet").exists()
    assert (run_dir / "asset_dispatch.parquet").exists()
    assert (run_dir / "summary.json").exists()


def test_backtest_cli_portfolio_da_plus_fcr_produces_portfolio_artifacts(tmp_path: Path) -> None:
    config_path = _write_temp_config(EXAMPLE_RESERVE_DIR / "netherlands_portfolio_da_plus_fcr_base.yaml", tmp_path)
    result = runner.invoke(
        app,
        [
            "backtest",
            str(config_path),
            "--market",
            "netherlands",
            "--workflow",
            "da_plus_fcr",
        ],
    )
    assert result.exit_code == 0, result.output
    run_dir = next((tmp_path / "artifacts").iterdir())
    assert (run_dir / "site_dispatch.parquet").exists()
    assert (run_dir / "asset_dispatch.parquet").exists()
    assert (run_dir / "asset_pnl_attribution.parquet").exists()


def test_backtest_cli_belgium_afrr_produces_reserve_outputs(tmp_path: Path) -> None:
    config_path = _write_temp_config(EXAMPLE_RESERVE_DIR / "belgium_da_plus_afrr_base.yaml", tmp_path)
    result = runner.invoke(
        app,
        [
            "backtest",
            str(config_path),
            "--market",
            "belgium",
            "--workflow",
            "da_plus_afrr",
        ],
    )
    assert result.exit_code == 0, result.output
    run_dir = next((tmp_path / "artifacts").iterdir())
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["reserve_product_id"] == "afrr_asymmetric"
    assert (run_dir / "site_dispatch.parquet").exists()


def test_backtest_cli_netherlands_afrr_fails_fast(tmp_path: Path) -> None:
    config = load_config(EXAMPLE_RESERVE_DIR / "belgium_da_plus_afrr_base.yaml")
    payload = config.model_dump(mode="json")
    payload["market"]["id"] = "netherlands"
    payload["timing"]["timezone"] = "Europe/Amsterdam"
    config_path = tmp_path / "netherlands_afrr.yaml"
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    result = runner.invoke(
        app,
        [
            "backtest",
            str(config_path),
            "--market",
            "netherlands",
            "--workflow",
            "da_plus_afrr",
        ],
    )
    assert result.exit_code != 0
    assert "not yet supported" in result.output


def test_compare_cli_grouped_by_market(tmp_path: Path) -> None:
    first = _write_temp_config(EXAMPLE_BASIC_DIR / "belgium_da_only_base.yaml", tmp_path / "first")
    second = _write_temp_config(
        EXAMPLE_RESERVE_DIR / "netherlands_portfolio_da_plus_fcr_base.yaml", tmp_path / "second"
    )
    run_a = runner.invoke(app, ["backtest", str(first), "--market", "belgium", "--workflow", "da_only"])
    run_b = runner.invoke(app, ["backtest", str(second), "--market", "netherlands", "--workflow", "da_plus_fcr"])
    assert run_a.exit_code == 0, run_a.output
    assert run_b.exit_code == 0, run_b.output
    run_dirs = sorted((tmp_path / "first" / "artifacts").iterdir()) + sorted(
        (tmp_path / "second" / "artifacts").iterdir()
    )
    comparison_dir = tmp_path / "comparison"

    compare = runner.invoke(
        app,
        ["compare", str(run_dirs[0]), str(run_dirs[1]), "--output-dir", str(comparison_dir), "--group-by", "market"],
    )
    assert compare.exit_code == 0, compare.output
    assert (comparison_dir / "comparison.csv").exists()
    assert (comparison_dir / "grouped_by_market.csv").exists()


def test_compare_cli_grouped_by_workflow_handles_single_asset_and_portfolio_runs(tmp_path: Path) -> None:
    first = _write_temp_config(EXAMPLE_RESERVE_DIR / "belgium_da_plus_fcr_base.yaml", tmp_path / "first")
    second = _write_temp_config(EXAMPLE_RESERVE_DIR / "belgium_portfolio_da_plus_fcr_base.yaml", tmp_path / "second")
    run_a = runner.invoke(app, ["backtest", str(first), "--market", "belgium", "--workflow", "da_plus_fcr"])
    run_b = runner.invoke(app, ["backtest", str(second), "--market", "belgium", "--workflow", "da_plus_fcr"])
    assert run_a.exit_code == 0, run_a.output
    assert run_b.exit_code == 0, run_b.output
    run_dirs = sorted((tmp_path / "first" / "artifacts").iterdir()) + sorted(
        (tmp_path / "second" / "artifacts").iterdir()
    )
    comparison_dir = tmp_path / "comparison-workflow"
    compare = runner.invoke(
        app,
        ["compare", str(run_dirs[0]), str(run_dirs[1]), "--output-dir", str(comparison_dir), "--group-by", "workflow"],
    )
    assert compare.exit_code == 0, compare.output
    assert (comparison_dir / "grouped_by_workflow.csv").exists()


def test_compare_cli_grouped_by_workflow_handles_mixed_fcr_and_afrr_runs(tmp_path: Path) -> None:
    first = _write_temp_config(EXAMPLE_RESERVE_DIR / "belgium_da_plus_fcr_base.yaml", tmp_path / "first")
    second = _write_temp_config(EXAMPLE_RESERVE_DIR / "belgium_da_plus_afrr_base.yaml", tmp_path / "second")
    run_a = runner.invoke(app, ["backtest", str(first), "--market", "belgium", "--workflow", "da_plus_fcr"])
    run_b = runner.invoke(app, ["backtest", str(second), "--market", "belgium", "--workflow", "da_plus_afrr"])
    assert run_a.exit_code == 0, run_a.output
    assert run_b.exit_code == 0, run_b.output
    run_dirs = sorted((tmp_path / "first" / "artifacts").iterdir()) + sorted(
        (tmp_path / "second" / "artifacts").iterdir()
    )
    comparison_dir = tmp_path / "comparison-reserve"
    compare = runner.invoke(
        app,
        ["compare", str(run_dirs[0]), str(run_dirs[1]), "--output-dir", str(comparison_dir), "--group-by", "workflow"],
    )
    assert compare.exit_code == 0, compare.output
    grouped = pd.read_csv(comparison_dir / "grouped_by_workflow.csv")
    assert {"da_plus_fcr", "da_plus_afrr"}.issubset(set(grouped["group"]))


def test_sweep_cli_materializes_cross_market_bundle(tmp_path: Path) -> None:
    base_config = load_config(EXAMPLE_RESERVE_DIR / "belgium_da_plus_fcr_base.yaml")
    base_config.artifacts.root_dir = tmp_path / "runs"
    base_path = tmp_path / "base.yaml"
    base_path.write_text(yaml.safe_dump(base_config.model_dump(mode="json"), sort_keys=False), encoding="utf-8")
    sweep_payload = {
        "schema_version": 4,
        "sweep_name": "cli-sweep",
        "base_config_path": str(base_path),
        "matrix": {
            "__bundle__": [
                {
                    "run_label": "belgium-single",
                    "market.id": "belgium",
                    "timing.timezone": "Europe/Brussels",
                    "data.day_ahead.actual_path": str(
                        PROJECT_ROOT / "examples" / "data" / "belgium_day_ahead_prices.csv"
                    ),
                    "data.fcr_capacity.actual_path": str(
                        PROJECT_ROOT / "examples" / "data" / "belgium_fcr_capacity_prices.csv"
                    ),
                },
                {
                    "run_label": "netherlands-single",
                    "market.id": "netherlands",
                    "timing.timezone": "Europe/Amsterdam",
                    "data.day_ahead.actual_path": str(
                        PROJECT_ROOT / "examples" / "data" / "netherlands_day_ahead_prices.csv"
                    ),
                    "data.fcr_capacity.actual_path": str(
                        PROJECT_ROOT / "examples" / "data" / "netherlands_fcr_capacity_prices.csv"
                    ),
                },
            ]
        },
        "artifacts": {"root_dir": str(tmp_path / "sweeps")},
    }
    sweep_path = tmp_path / "sweep.yaml"
    sweep_path.write_text(yaml.safe_dump(sweep_payload, sort_keys=False), encoding="utf-8")

    result = runner.invoke(app, ["sweep", str(sweep_path)])
    assert result.exit_code == 0, result.output
    comparison_dir = tmp_path / "sweeps" / "cli-sweep" / "comparison"
    assert (comparison_dir / "comparison.csv").exists()
    assert (comparison_dir / "grouped_by_market.csv").exists()
    assert (comparison_dir / "grouped_by_workflow.csv").exists()
    run_dirs = list((tmp_path / "sweeps" / "cli-sweep" / "runs").iterdir())
    assert len(run_dirs) == 2


def test_validate_config_cli_emits_json_and_human_summary(tmp_path: Path) -> None:
    config_path = _write_temp_config(EXAMPLE_RESERVE_DIR / "belgium_portfolio_da_plus_fcr_base.yaml", tmp_path)
    result = runner.invoke(app, ["validate-config", str(config_path)])
    assert result.exit_code == 0, result.output
    assert "validate-config passed" in result.output
    assert '"report_type": "validate-config"' in result.output


def test_validate_data_cli_fails_on_duplicate_timestamps(tmp_path: Path) -> None:
    config = load_config(EXAMPLE_BASIC_DIR / "belgium_da_plus_imbalance_base.yaml")
    frame = pd.read_csv(config.data.day_ahead.actual_path)
    duplicated = pd.concat([frame.iloc[[0]], frame], ignore_index=True)
    broken_path = tmp_path / "duplicated_day_ahead.csv"
    duplicated.to_csv(broken_path, index=False)
    config.data.day_ahead.actual_path = broken_path
    config.artifacts.root_dir = tmp_path / "artifacts"
    config_path = tmp_path / "broken.yaml"
    config_path.write_text(yaml.safe_dump(config.model_dump(mode="json"), sort_keys=False), encoding="utf-8")
    result = runner.invoke(app, ["validate-data", str(config_path)])
    assert result.exit_code == 1
    assert '"report_type": "validate-data"' in result.output
    assert "Duplicate UTC timestamps found" in result.output


def test_doctor_cli_checks_solver_and_writable_artifacts(tmp_path: Path) -> None:
    config_path = _write_temp_config(EXAMPLE_BASIC_DIR / "netherlands_da_only_base.yaml", tmp_path)
    result = runner.invoke(app, ["doctor", "--config", str(config_path)])
    assert result.exit_code == 0, result.output
    assert '"report_type": "doctor"' in result.output
    assert "Solver available" in result.output


def test_export_schedule_and_bids_cli_write_manifest(tmp_path: Path) -> None:
    config_path = _write_temp_config(EXAMPLE_RESERVE_DIR / "belgium_portfolio_da_plus_fcr_base.yaml", tmp_path)
    run = runner.invoke(app, ["backtest", str(config_path), "--market", "belgium", "--workflow", "da_plus_fcr"])
    assert run.exit_code == 0, run.output
    run_dir = next((tmp_path / "artifacts").iterdir())

    schedule = runner.invoke(app, ["export-schedule", str(run_dir), "--profile", "operator"])
    bids = runner.invoke(app, ["export-bids", str(run_dir), "--profile", "bid_planning"])

    assert schedule.exit_code == 0, schedule.output
    assert bids.exit_code == 0, bids.output
    schedule_manifest = json.loads(
        (run_dir / "exports" / "schedule-operator" / "manifest.json").read_text(encoding="utf-8")
    )
    bids_manifest = json.loads(
        (run_dir / "exports" / "bids-bid_planning" / "manifest.json").read_text(encoding="utf-8")
    )
    assert schedule_manifest["metadata"]["profile"] == "operator"
    assert bids_manifest["metadata"]["profile"] == "bid_planning"
    site_schedule = pd.read_csv(run_dir / "exports" / "schedule-operator" / "site_schedule.csv")
    assert "charge_mw" not in site_schedule.columns


def test_export_bids_cli_supports_submission_candidate_profile(tmp_path: Path) -> None:
    config_path = _write_temp_config(EXAMPLE_RESERVE_DIR / "belgium_da_plus_afrr_base.yaml", tmp_path)
    run = runner.invoke(app, ["backtest", str(config_path), "--market", "belgium", "--workflow", "da_plus_afrr"])
    assert run.exit_code == 0, run.output
    run_dir = next((tmp_path / "artifacts").iterdir())

    export_result = runner.invoke(app, ["export-bids", str(run_dir), "--profile", "submission_candidate"])
    assert export_result.exit_code == 0, export_result.output
    manifest = json.loads(
        (run_dir / "exports" / "bids-submission_candidate" / "manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["metadata"]["profile"] == "submission_candidate"


def test_batch_cli_runs_multistep_job_and_emits_jsonl_log(tmp_path: Path) -> None:
    config_path = _write_temp_config(EXAMPLE_RESERVE_DIR / "belgium_schedule_revision_da_plus_afrr_base.yaml", tmp_path)
    batch_payload = {
        "schema_version": 4,
        "batch_name": "cli-batch",
        "jobs": [
            {
                "id": "be-afrr",
                "config_path": str(config_path),
                "market": "belgium",
                "workflow": "schedule_revision",
                "steps": [
                    "validate_config",
                    "validate_data",
                    "backtest",
                    "reconcile",
                    "export_schedule",
                    "export_bids",
                    "export_revision",
                ],
                "realized_input_path": str(config_path),
                "export_schedule_profile": "operator",
                "export_bids_profile": "bid_planning",
            }
        ],
        "artifacts": {"root_dir": str(tmp_path / "batch-artifacts")},
    }
    batch_path = tmp_path / "batch.yaml"
    batch_path.write_text(yaml.safe_dump(batch_payload, sort_keys=False), encoding="utf-8")
    result = runner.invoke(app, ["batch", str(batch_path)])
    assert result.exit_code == 0, result.output
    batch_root = tmp_path / "batch-artifacts" / "cli-batch"
    assert (batch_root / "batch_summary.json").exists()
    log_lines = (batch_root / "batch.jsonl").read_text(encoding="utf-8").splitlines()
    assert any('"event": "job_completed"' in line for line in log_lines)


def test_export_revision_and_reconcile_cli_for_schedule_revision(tmp_path: Path) -> None:
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
    payload["artifacts"]["root_dir"] = str(tmp_path / "artifacts")
    config_path = tmp_path / "revision.yaml"
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    run = runner.invoke(app, ["backtest", str(config_path), "--market", "belgium", "--workflow", "schedule_revision"])
    assert run.exit_code == 0, run.output
    run_dir = next((tmp_path / "artifacts").iterdir())

    revision = runner.invoke(app, ["export-revision", str(run_dir)])
    reconcile = runner.invoke(app, ["reconcile", str(run_dir), str(config_path)])

    assert revision.exit_code == 0, revision.output
    assert reconcile.exit_code == 0, reconcile.output
    assert (run_dir / "exports" / "revision" / "manifest.json").exists()
    assert (run_dir / "reconciliation" / "reconciliation_summary.json").exists()


def test_tennet_ingest_cli_uses_connector_payload(monkeypatch, tmp_path: Path) -> None:
    payload = json.loads(
        (PROJECT_ROOT / "tests" / "fixtures" / "raw" / "tennet" / "settlement_prices.json").read_text(encoding="utf-8")
    )

    class FakeMetadata:
        def as_dict(self):
            return {"connector_id": "tennet_settlement_prices", "cache_hit": False}

    def fake_fetch(self, *, start, end, **kwargs):
        if kwargs.get("return_metadata"):
            return payload, FakeMetadata()
        return payload

    monkeypatch.setattr("euroflex_bess_lab.cli.TenneTSettlementPricesConnector.fetch", fake_fetch)
    out_raw = tmp_path / "tennet_raw.json"
    out_parquet = tmp_path / "tennet.parquet"
    result = runner.invoke(
        app,
        [
            "ingest",
            "tennet-nl-imbalance",
            "--start",
            "2025-06-20T00:00:00",
            "--end",
            "2025-06-20T02:00:00",
            "--out-raw",
            str(out_raw),
            "--out-parquet",
            str(out_parquet),
        ],
    )
    assert result.exit_code == 0, result.output
    assert out_raw.exists()
    assert (tmp_path / "tennet_raw.json.meta.json").exists()
    assert out_parquet.exists()
