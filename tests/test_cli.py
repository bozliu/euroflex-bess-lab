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


def _build_tennet_payload_from_normalized_frame(frame: pd.DataFrame) -> dict[str, object]:
    points = []
    for row in frame.itertuples(index=False):
        points.append(
            {
                "shortage": f"{float(row.imbalance_shortage_price_eur_per_mwh):.2f}",
                "surplus": f"{float(row.imbalance_surplus_price_eur_per_mwh):.2f}",
                "dispatch_up": f"{float(row.dispatch_up_price_eur_per_mwh):.2f}",
                "dispatch_down": f"{float(row.dispatch_down_price_eur_per_mwh):.2f}",
                "timeInterval_start": pd.Timestamp(row.timestamp_local).strftime("%Y-%m-%dT%H:%M"),
                "timeInterval_end": (pd.Timestamp(row.timestamp_local) + pd.Timedelta(minutes=15)).strftime(
                    "%Y-%m-%dT%H:%M"
                ),
                "regulation_state": row.regulation_state,
                "regulating_condition": row.regulating_condition,
            }
        )
    return {"TimeSeries": [{"Period": {"Points": points}}]}


def _build_day_ahead_frame_from_imbalance(imbalance_frame: pd.DataFrame) -> pd.DataFrame:
    day_ahead = imbalance_frame[["timestamp_utc", "timestamp_local", "zone"]].copy()
    day_ahead["market"] = "day_ahead"
    day_ahead["resolution"] = "PT15M"
    day_ahead["price_eur_per_mwh"] = 48.0
    day_ahead["currency"] = "EUR"
    day_ahead["source"] = "tennet_live_input_cli_fixture"
    day_ahead["is_actual"] = True
    day_ahead["is_forecast"] = False
    day_ahead["quality_status"] = "Validated"
    day_ahead["provenance"] = "day_ahead_cli_fixture"
    return day_ahead[
        [
            "timestamp_utc",
            "timestamp_local",
            "market",
            "resolution",
            "price_eur_per_mwh",
            "currency",
            "zone",
            "source",
            "is_actual",
            "is_forecast",
            "quality_status",
            "provenance",
        ]
    ]


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
    assert "1.2.0" in result.output


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
    run_dir = next(path for path in (tmp_path / "artifacts").iterdir() if path.is_dir())
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
    run_dir = next(path for path in (tmp_path / "artifacts").iterdir() if path.is_dir())
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["reserve_product_id"] == "afrr_asymmetric"
    assert (run_dir / "site_dispatch.parquet").exists()


def test_backtest_cli_netherlands_afrr_produces_reserve_outputs(tmp_path: Path) -> None:
    config_path = _write_temp_config(EXAMPLE_RESERVE_DIR / "netherlands_da_plus_afrr_base.yaml", tmp_path)
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
    assert result.exit_code == 0, result.output
    run_dir = next(path for path in (tmp_path / "artifacts").iterdir() if path.is_dir())
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["reserve_product_id"] == "afrr_asymmetric"
    assert summary["market_id"] == "netherlands"
    assert (run_dir / "site_dispatch.parquet").exists()


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
    run_dir = next(path for path in (tmp_path / "artifacts").iterdir() if path.is_dir())

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
    run_dir = next(path for path in (tmp_path / "artifacts").iterdir() if path.is_dir())

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
    run_dir = next(path for path in (tmp_path / "artifacts").iterdir() if path.is_dir())

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
            return {
                "connector_id": "tennet_settlement_prices",
                "endpoint_id": "publications_v1_settlement_prices",
                "source_operator": "TenneT",
                "auth_mode": "apikey_header_env_var",
                "environment": "acceptance",
                "base_url": "https://api.acc.tennet.eu",
                "cache_hit": False,
            }

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
            "--env",
            "acceptance",
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
    normalized_meta = tmp_path / "tennet.parquet.meta.json"
    assert normalized_meta.exists()
    payload = json.loads(normalized_meta.read_text(encoding="utf-8"))
    assert payload["source_operator"] == "TenneT"
    assert payload["auth_mode"] == "apikey_header_env_var"
    assert payload["environment"] == "acceptance"
    assert payload["normalization_name"] == "normalize_tennet_settlement_prices_json"


def test_tennet_ingest_cli_accepts_utc_z_timestamps(monkeypatch, tmp_path: Path) -> None:
    payload = json.loads(
        (PROJECT_ROOT / "tests" / "fixtures" / "raw" / "tennet" / "settlement_prices.json").read_text(encoding="utf-8")
    )

    class FakeMetadata:
        def as_dict(self):
            return {
                "connector_id": "tennet_settlement_prices",
                "endpoint_id": "publications_v1_settlement_prices",
                "source_operator": "TenneT",
                "auth_mode": "apikey_header_env_var",
                "environment": "acceptance",
                "base_url": "https://api.acc.tennet.eu",
                "cache_hit": False,
            }

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
            "2025-06-20T00:00:00Z",
            "--end",
            "2025-06-20T02:00:00Z",
            "--env",
            "acceptance",
            "--out-raw",
            str(out_raw),
            "--out-parquet",
            str(out_parquet),
        ],
    )
    assert result.exit_code == 0, result.output
    assert out_raw.exists()
    assert out_parquet.exists()


def test_tennet_merit_order_ingest_cli_uses_connector_payload(monkeypatch, tmp_path: Path) -> None:
    payload = json.loads(
        (PROJECT_ROOT / "tests" / "fixtures" / "raw" / "tennet" / "merit_order_list.json").read_text(encoding="utf-8")
    )

    class FakeMetadata:
        def as_dict(self):
            return {
                "connector_id": "tennet_merit_order_list",
                "endpoint_id": "publications_v1_merit_order_list",
                "source_operator": "TenneT",
                "auth_mode": "apikey_header_env_var",
                "environment": "acceptance",
                "base_url": "https://api.acc.tennet.eu",
                "cache_hit": False,
            }

    def fake_fetch(self, *, start, end, **kwargs):
        if kwargs.get("return_metadata"):
            return payload, FakeMetadata()
        return payload

    monkeypatch.setattr("euroflex_bess_lab.cli.TenneTMeritOrderListConnector.fetch", fake_fetch)
    out_raw = tmp_path / "tennet_merit_order_raw.json"
    out_parquet = tmp_path / "tennet_merit_order.parquet"
    result = runner.invoke(
        app,
        [
            "ingest",
            "tennet-nl-merit-order",
            "--start",
            "2025-06-20T00:00:00Z",
            "--end",
            "2025-06-20T00:30:00Z",
            "--env",
            "acceptance",
            "--out-raw",
            str(out_raw),
            "--out-parquet",
            str(out_parquet),
        ],
    )
    assert result.exit_code == 0, result.output
    assert out_raw.exists()
    assert out_parquet.exists()
    normalized_meta = json.loads((tmp_path / "tennet_merit_order.parquet.meta.json").read_text(encoding="utf-8"))
    assert normalized_meta["connector_id"] == "tennet_merit_order_list"
    assert normalized_meta["normalization_name"] == "normalize_tennet_merit_order_list_json"
    assert normalized_meta["table_name"] == "tennet_merit_order_list"


def test_tennet_afrr_activations_ingest_cli_uses_connector_payload(monkeypatch, tmp_path: Path) -> None:
    payload = json.loads(
        (
            PROJECT_ROOT / "tests" / "fixtures" / "raw" / "tennet" / "frequency_restoration_reserve_activations.json"
        ).read_text(encoding="utf-8")
    )

    class FakeMetadata:
        def as_dict(self):
            return {
                "connector_id": "tennet_frequency_restoration_reserve_activations",
                "endpoint_id": "publications_v1_frequency_restoration_reserve_activations",
                "source_operator": "TenneT",
                "auth_mode": "apikey_header_env_var",
                "environment": "acceptance",
                "base_url": "https://api.acc.tennet.eu",
                "cache_hit": False,
            }

    def fake_fetch(self, *, start, end, **kwargs):
        if kwargs.get("return_metadata"):
            return payload, FakeMetadata()
        return payload

    monkeypatch.setattr(
        "euroflex_bess_lab.cli.TenneTFrequencyRestorationReserveActivationsConnector.fetch",
        fake_fetch,
    )
    out_raw = tmp_path / "tennet_afrr_activations_raw.json"
    out_parquet = tmp_path / "tennet_afrr_activations.parquet"
    result = runner.invoke(
        app,
        [
            "ingest",
            "tennet-nl-afrr-activations",
            "--start",
            "2025-06-20T00:00:00Z",
            "--end",
            "2025-06-20T00:30:00Z",
            "--env",
            "acceptance",
            "--out-raw",
            str(out_raw),
            "--out-parquet",
            str(out_parquet),
        ],
    )
    assert result.exit_code == 0, result.output
    assert out_raw.exists()
    assert out_parquet.exists()
    normalized_meta = json.loads((tmp_path / "tennet_afrr_activations.parquet.meta.json").read_text(encoding="utf-8"))
    assert normalized_meta["connector_id"] == "tennet_frequency_restoration_reserve_activations"
    assert normalized_meta["normalization_name"] == "normalize_tennet_frequency_restoration_reserve_activations_json"
    assert normalized_meta["table_name"] == "tennet_frequency_restoration_reserve_activations"


def test_tennet_afrr_derived_cli_builds_live_ready_activation_series(monkeypatch, tmp_path: Path) -> None:
    merit_payload = json.loads(
        (PROJECT_ROOT / "tests" / "fixtures" / "raw" / "tennet" / "merit_order_list.json").read_text(encoding="utf-8")
    )
    activations_payload = json.loads(
        (
            PROJECT_ROOT / "tests" / "fixtures" / "raw" / "tennet" / "frequency_restoration_reserve_activations.json"
        ).read_text(encoding="utf-8")
    )

    class MeritMetadata:
        def as_dict(self):
            return {
                "connector_id": "tennet_merit_order_list",
                "endpoint_id": "publications_v1_merit_order_list",
                "source_operator": "TenneT",
                "auth_mode": "apikey_header_env_var",
                "environment": "production",
                "base_url": "https://api.tennet.eu",
                "cache_hit": False,
            }

    class ActivationMetadata:
        def as_dict(self):
            return {
                "connector_id": "tennet_frequency_restoration_reserve_activations",
                "endpoint_id": "publications_v1_frequency_restoration_reserve_activations",
                "source_operator": "TenneT",
                "auth_mode": "apikey_header_env_var",
                "environment": "production",
                "base_url": "https://api.tennet.eu",
                "cache_hit": False,
            }

    def fake_merit_fetch(self, *, start, end, **kwargs):
        if kwargs.get("return_metadata"):
            return merit_payload, MeritMetadata()
        return merit_payload

    def fake_activation_fetch(self, *, start, end, **kwargs):
        if kwargs.get("return_metadata"):
            return activations_payload, ActivationMetadata()
        return activations_payload

    monkeypatch.setattr("euroflex_bess_lab.cli.TenneTMeritOrderListConnector.fetch", fake_merit_fetch)
    monkeypatch.setattr(
        "euroflex_bess_lab.cli.TenneTFrequencyRestorationReserveActivationsConnector.fetch",
        fake_activation_fetch,
    )

    out_dir = tmp_path / "tennet_afrr_live"
    result = runner.invoke(
        app,
        [
            "ingest",
            "tennet-nl-afrr-derived",
            "--start",
            "2025-06-20T00:00:00Z",
            "--end",
            "2025-06-20T00:30:00Z",
            "--env",
            "production",
            "--out-dir",
            str(out_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert (out_dir / "manifest.json").exists()
    assert (out_dir / "normalized" / "netherlands_merit_order.parquet").exists()
    assert (out_dir / "normalized" / "netherlands_afrr_activations.parquet").exists()
    assert (out_dir / "normalized" / "netherlands_afrr_activation_price_up.parquet").exists()
    assert (out_dir / "normalized" / "netherlands_afrr_activation_price_down.parquet").exists()
    assert (out_dir / "normalized" / "netherlands_afrr_activation_ratio_up.parquet").exists()
    assert (out_dir / "normalized" / "netherlands_afrr_activation_ratio_down.parquet").exists()


def test_dutch_live_input_cli_path_validates_and_exports(monkeypatch, tmp_path: Path) -> None:
    imbalance_frame = pd.read_csv(PROJECT_ROOT / "examples" / "data" / "netherlands_imbalance_prices.csv")
    fixture_payload = _build_tennet_payload_from_normalized_frame(imbalance_frame)

    class FakeMetadata:
        def as_dict(self):
            return {
                "connector_id": "tennet_settlement_prices",
                "endpoint_id": "publications_v1_settlement_prices",
                "source_operator": "TenneT",
                "auth_mode": "apikey_header_env_var",
                "environment": "acceptance",
                "base_url": "https://api.acc.tennet.eu",
                "cache_hit": False,
                "cache_key": "fixture",
                "cache_path": None,
                "timeout_seconds": 30,
                "max_retries": 0,
                "backoff_factor": 0.5,
                "status_code": 200,
                "fetched_at_utc": "2026-03-20T00:00:00+00:00",
                "request_start_utc": "2025-06-16T00:00:00+00:00",
                "request_end_utc": "2025-06-18T00:00:00+00:00",
            }

    def fake_fetch(self, *, start, end, **kwargs):
        if kwargs.get("return_metadata"):
            return fixture_payload, FakeMetadata()
        return fixture_payload

    monkeypatch.setattr("euroflex_bess_lab.cli.TenneTSettlementPricesConnector.fetch", fake_fetch)

    live_dir = tmp_path / "live"
    live_dir.mkdir(parents=True, exist_ok=True)
    out_raw = live_dir / "tennet_raw.json"
    out_parquet = live_dir / "netherlands_imbalance.parquet"
    ingest = runner.invoke(
        app,
        [
            "ingest",
            "tennet-nl-imbalance",
            "--start",
            "2025-06-16T00:00:00",
            "--end",
            "2025-06-18T00:00:00",
            "--env",
            "acceptance",
            "--out-raw",
            str(out_raw),
            "--out-parquet",
            str(out_parquet),
        ],
    )
    assert ingest.exit_code == 0, ingest.output

    day_ahead = _build_day_ahead_frame_from_imbalance(imbalance_frame)
    day_ahead_path = live_dir / "netherlands_day_ahead.parquet"
    day_ahead.to_parquet(day_ahead_path, index=False)

    config = load_config(PROJECT_ROOT / "examples" / "configs" / "basic" / "netherlands_da_only_live_inputs.yaml")
    config.data.day_ahead.actual_path = day_ahead_path
    config.data.imbalance.actual_path = out_parquet  # type: ignore[union-attr]
    config.artifacts.root_dir = tmp_path / "artifacts"
    config_path = tmp_path / "netherlands_live.yaml"
    config_path.write_text(yaml.safe_dump(config.model_dump(mode="json"), sort_keys=False), encoding="utf-8")

    validate_config = runner.invoke(app, ["validate-config", str(config_path)])
    validate_data = runner.invoke(app, ["validate-data", str(config_path)])
    backtest = runner.invoke(app, ["backtest", str(config_path), "--market", "netherlands", "--workflow", "da_only"])
    assert validate_config.exit_code == 0, validate_config.output
    assert validate_data.exit_code == 0, validate_data.output
    assert backtest.exit_code == 0, backtest.output

    run_dir = next(path for path in (tmp_path / "artifacts").iterdir() if path.is_dir())
    reconcile = runner.invoke(app, ["reconcile", str(run_dir), str(config_path)])
    export_schedule = runner.invoke(app, ["export-schedule", str(run_dir), "--profile", "operator"])
    export_bids = runner.invoke(app, ["export-bids", str(run_dir), "--profile", "bid_planning"])

    assert reconcile.exit_code == 0, reconcile.output
    assert export_schedule.exit_code == 0, export_schedule.output
    assert export_bids.exit_code == 0, export_bids.output
    assert (run_dir / "reconciliation" / "reconciliation_summary.json").exists()
    assert (run_dir / "exports" / "schedule-operator" / "site_schedule.json").exists()
    assert (run_dir / "exports" / "bids-bid_planning" / "site_bids.json").exists()
