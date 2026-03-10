from __future__ import annotations

import json
from pathlib import Path

import yaml
from fastapi.testclient import TestClient

from euroflex_bess_lab.config import load_config
from euroflex_bess_lab.run_registry import RunRegistry
from euroflex_bess_lab.service import app

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _write_temp_config(source: Path, tmp_path: Path) -> Path:
    config = load_config(source)
    config.artifacts.root_dir = tmp_path / "artifacts"
    config_path = tmp_path / source.name
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(yaml.safe_dump(config.model_dump(mode="json"), sort_keys=False), encoding="utf-8")
    return config_path


def test_service_endpoints_run_canonical_path_with_registry_and_logs(tmp_path: Path) -> None:
    client = TestClient(app)
    config_path = _write_temp_config(
        PROJECT_ROOT / "examples" / "configs" / "canonical" / "belgium_full_stack.yaml",
        tmp_path,
    )

    validate_response = client.post("/validate", json={"config_path": str(config_path), "kind": "config"})
    assert validate_response.status_code == 200
    assert validate_response.json()["ok"] is True

    data_response = client.post("/validate", json={"config_path": str(config_path), "kind": "data"})
    assert data_response.status_code == 200
    assert data_response.json()["ok"] is True

    backtest_response = client.post(
        "/backtest",
        json={
            "config_path": str(config_path),
            "market": "belgium",
            "workflow": "schedule_revision",
            "forecast_provider": "persistence",
        },
    )
    assert backtest_response.status_code == 200, backtest_response.text
    backtest_payload = backtest_response.json()
    run_dir = Path(backtest_payload["run_dir"])
    registry_path = Path(backtest_payload["registry_path"])
    run_id = backtest_payload["run_id"]

    export_response = client.post(
        "/export",
        json={
            "run_dir": str(run_dir),
            "kind": "bids",
            "profile": "submission_candidate",
        },
    )
    assert export_response.status_code == 200, export_response.text
    export_dir = Path(export_response.json()["export_dir"])
    manifest = json.loads((export_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["metadata"]["profile"] == "submission_candidate"

    reconcile_response = client.post(
        "/reconcile",
        json={
            "run_dir": str(run_dir),
            "settlement_or_realized_input": str(config_path),
        },
    )
    assert reconcile_response.status_code == 200, reconcile_response.text

    reviewed = client.post(
        f"/runs/{run_id}/transition",
        json={"state": "reviewed", "registry_path": str(registry_path)},
    )
    approved = client.post(
        f"/runs/{run_id}/transition",
        json={"state": "approved", "registry_path": str(registry_path)},
    )
    assert reviewed.status_code == 200
    assert approved.status_code == 200

    registry = RunRegistry(registry_path)
    children = registry.children(run_id)
    child_ids = {record.run_id for record in children}
    assert f"{run_id}:baseline" in child_ids
    assert f"{run_id}:revision" in child_ids
    assert any(record.metadata.get("kind") == "export_bids" for record in children)
    assert any(record.metadata.get("kind") == "reconcile" for record in children)
    assert registry.get(run_id).current_state == "approved"

    service_log = run_dir / "service.jsonl"
    assert service_log.exists()
    log_lines = service_log.read_text(encoding="utf-8").splitlines()
    assert any('"event": "run_registered"' in line for line in log_lines)
    assert any('"event": "service_export"' in line for line in log_lines)


def test_service_batch_run_endpoint_executes_canonical_batch(tmp_path: Path) -> None:
    client = TestClient(app)
    config_path = _write_temp_config(
        PROJECT_ROOT / "examples" / "configs" / "canonical" / "belgium_full_stack.yaml",
        tmp_path / "config",
    )
    batch_payload = {
        "schema_version": 4,
        "batch_name": "canonical-service-batch",
        "jobs": [
            {
                "id": "belgium-ga",
                "config_path": str(config_path),
                "market": "belgium",
                "workflow": "schedule_revision",
                "steps": ["validate_config", "validate_data", "backtest", "export_schedule"],
                "export_schedule_profile": "operator",
            }
        ],
        "artifacts": {"root_dir": str(tmp_path / "batch-root")},
    }
    batch_path = tmp_path / "batch.yaml"
    batch_path.write_text(yaml.safe_dump(batch_payload, sort_keys=False), encoding="utf-8")

    response = client.post("/batch/run", json={"batch_config_path": str(batch_path)})
    assert response.status_code == 200, response.text
    batch_dir = Path(response.json()["batch_dir"])
    assert (batch_dir / "batch_summary.json").exists()
