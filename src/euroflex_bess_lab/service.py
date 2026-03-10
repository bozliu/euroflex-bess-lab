from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .backtesting.engine import run_walk_forward
from .batch import run_batch
from .config import BacktestConfig, load_config
from .diagnostics import append_jsonl_event
from .exports import export_bids, export_revision, export_schedule
from .reconciliation import reconcile_run
from .run_registry import (
    RunRegistry,
    default_registry_path,
    register_backtest_result,
    register_derived_artifact,
    registry_path_for_run_dir,
)
from .validation import doctor as run_doctor
from .validation import validate_config_file, validate_data_file


class ValidateRequest(BaseModel):
    config_path: str
    kind: Literal["config", "data", "doctor"] = "config"


class BacktestRequest(BaseModel):
    config_path: str
    market: Literal["belgium", "netherlands"]
    workflow: Literal["da_only", "da_plus_imbalance", "da_plus_fcr", "da_plus_afrr", "schedule_revision"]
    forecast_provider: Literal["perfect_foresight", "persistence", "csv", "custom_python"] | None = None
    launcher: str = "service"


class ReconcileRequest(BaseModel):
    run_dir: str
    settlement_or_realized_input: str
    output_dir: str | None = None
    launcher: str = "service"


class ExportRequest(BaseModel):
    run_dir: str
    kind: Literal["schedule", "bids", "revision"]
    profile: str = "benchmark"
    output_dir: str | None = None
    launcher: str = "service"


class BatchRunRequest(BaseModel):
    batch_config_path: str


class TransitionRequest(BaseModel):
    state: Literal["draft", "reviewed", "approved", "exported", "superseded", "reconciled"]
    registry_path: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


def _service_log_path(*, run_dir: str | Path | None = None, config_path: str | Path | None = None) -> Path:
    if run_dir is not None:
        resolved = Path(run_dir).resolve()
        return resolved / "service.jsonl"
    if config_path is not None:
        config = load_config(config_path)
        return config.artifacts.root_dir.resolve() / "service.jsonl"
    return default_registry_path().with_suffix(".jsonl")


def _registry_log_path(registry_path: str | Path | None) -> Path:
    resolved = Path(registry_path).resolve() if registry_path is not None else default_registry_path()
    return resolved.with_suffix(".jsonl")


def _ensure_parent_run_registered(run_dir: Path, summary: dict[str, Any], registry_path: Path) -> None:
    registry = RunRegistry(registry_path)
    run_id = str(summary["run_id"])
    try:
        registry.get(run_id)
        return
    except KeyError:
        registry.upsert(
            run_id=run_id,
            parent_run_id=None,
            schedule_version="revision_latest" if str(summary["workflow"]) == "schedule_revision" else "baseline",
            market=str(summary["market_id"]),
            workflow=str(summary["workflow"]),
            base_workflow=str(summary["base_workflow"]),
            launcher="service_import",
            current_state="draft",
            artifact_path=run_dir,
            metadata={
                "site_id": summary.get("site_id"),
                "run_scope": summary.get("run_scope"),
                "benchmark_name": summary.get("benchmark_name"),
                "provider_name": summary.get("provider_name"),
            },
        )


def create_app() -> FastAPI:
    app = FastAPI(title="euroflex_bess_lab local service", version="1.1.0")

    @app.post("/validate")
    def validate(request: ValidateRequest) -> dict[str, Any]:
        try:
            if request.kind == "config":
                report = validate_config_file(request.config_path)
            elif request.kind == "data":
                report = validate_data_file(request.config_path)
            else:
                report = run_doctor(request.config_path)
            append_jsonl_event(
                _service_log_path(config_path=request.config_path),
                "service_validate",
                job_id="validate",
                market=report.metadata.get("market_id"),
                workflow=report.metadata.get("workflow"),
                warning_count=sum(1 for check in report.checks if check.status == "warn"),
                failure_category=None if report.ok else "validation",
            )
            return report.as_dict()
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/backtest")
    def backtest(request: BacktestRequest) -> dict[str, Any]:
        try:
            loaded = load_config(request.config_path)
            payload = loaded.model_dump(mode="json")
            payload["market"]["id"] = request.market
            payload["workflow"] = request.workflow
            config = BacktestConfig.model_validate(payload)
            result = run_walk_forward(config, forecast_provider_override=request.forecast_provider)
            if result.output_dir is None:
                raise RuntimeError("backtest completed without output_dir")
            registry_path = register_backtest_result(
                result,
                config,
                launcher=request.launcher,
                log_path=_service_log_path(run_dir=result.output_dir),
            )
            return {
                "run_id": result.run_id,
                "run_dir": str(result.output_dir),
                "registry_path": str(registry_path),
                "market": result.market_id,
                "workflow": result.workflow,
                "base_workflow": result.workflow_family,
            }
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/reconcile")
    def reconcile(request: ReconcileRequest) -> dict[str, Any]:
        try:
            run_dir = Path(request.run_dir).resolve()
            target = reconcile_run(run_dir, request.settlement_or_realized_input, output_dir=request.output_dir)
            summary = (run_dir / "summary.json").read_text(encoding="utf-8")

            run_summary = json.loads(summary)
            registry_path = registry_path_for_run_dir(run_dir)
            _ensure_parent_run_registered(run_dir, run_summary, registry_path)
            record = register_derived_artifact(
                parent_run_id=run_summary["run_id"],
                kind="reconcile",
                market=run_summary["market_id"],
                workflow=run_summary["workflow"],
                base_workflow=run_summary["base_workflow"],
                launcher=request.launcher,
                artifact_path=target,
                registry_path=registry_path,
                schedule_version="revision_latest" if run_summary["workflow"] == "schedule_revision" else "baseline",
                metadata={"realized_input": request.settlement_or_realized_input},
            )
            append_jsonl_event(
                _service_log_path(run_dir=run_dir),
                "service_reconcile",
                run_id=run_summary["run_id"],
                market=run_summary["market_id"],
                workflow=run_summary["workflow"],
                warning_count=0,
            )
            return {
                "reconciliation_dir": str(target),
                "registry_entry": record.run_id,
                "registry_path": str(registry_path),
            }
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/export")
    def export(request: ExportRequest) -> dict[str, Any]:
        try:
            run_dir = Path(request.run_dir).resolve()
            summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
            if request.kind == "schedule":
                target = export_schedule(run_dir, output_dir=request.output_dir, profile=request.profile)
                kind: Literal["export_schedule", "export_bids", "export_revision"] = "export_schedule"
            elif request.kind == "bids":
                target = export_bids(run_dir, output_dir=request.output_dir, profile=request.profile)
                kind = "export_bids"
            else:
                target = export_revision(run_dir, output_dir=request.output_dir)
                kind = "export_revision"
            registry_path = registry_path_for_run_dir(run_dir)
            _ensure_parent_run_registered(run_dir, summary, registry_path)
            record = register_derived_artifact(
                parent_run_id=summary["run_id"],
                kind=kind,
                market=summary["market_id"],
                workflow=summary["workflow"],
                base_workflow=summary["base_workflow"],
                launcher=request.launcher,
                artifact_path=target,
                registry_path=registry_path,
                schedule_version="revision_latest" if summary["workflow"] == "schedule_revision" else "baseline",
                metadata={"profile": request.profile},
            )
            append_jsonl_event(
                _service_log_path(run_dir=run_dir),
                "service_export",
                run_id=summary["run_id"],
                market=summary["market_id"],
                workflow=summary["workflow"],
                export_profile=request.profile,
                warning_count=0,
            )
            return {"export_dir": str(target), "registry_entry": record.run_id, "registry_path": str(registry_path)}
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/batch/run")
    def batch_run(request: BatchRunRequest) -> dict[str, Any]:
        try:
            target = run_batch(request.batch_config_path)
            append_jsonl_event(
                target / "batch.jsonl",
                "service_batch",
                job_id="batch",
                warning_count=0,
            )
            return {"batch_dir": str(target)}
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/runs/{run_id}/transition")
    def transition_run(run_id: str, request: TransitionRequest) -> dict[str, Any]:
        try:
            registry = RunRegistry(request.registry_path or default_registry_path())
            record = registry.transition(run_id, request.state, metadata=request.metadata)
            append_jsonl_event(
                _registry_log_path(request.registry_path),
                "service_transition",
                run_id=run_id,
                market=record.market,
                workflow=record.workflow,
                warning_count=0,
            )
            return {
                "run_id": record.run_id,
                "parent_run_id": record.parent_run_id,
                "current_state": record.current_state,
                "schedule_version": record.schedule_version,
            }
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    return app


app = create_app()
