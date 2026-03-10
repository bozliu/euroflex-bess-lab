from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .backtesting.engine import run_walk_forward
from .config import BacktestConfig, BatchJobConfig, load_batch_config, load_config
from .diagnostics import append_jsonl_event
from .exports import export_bids, export_revision, export_schedule
from .reconciliation import reconcile_run
from .run_registry import RunRegistry, register_backtest_result, register_derived_artifact, registry_path_for_run_dir
from .validation import validate_config_file, validate_data_file


@dataclass
class BatchJobResult:
    job_id: str
    run_dir: Path | None
    reconciliation_dir: Path | None
    export_dirs: dict[str, str]
    completed_steps: list[str]


def _prepare_job_config(job: BatchJobConfig) -> BacktestConfig:
    loaded = load_config(job.config_path)
    payload = loaded.model_dump(mode="json")
    payload["market"]["id"] = job.market
    payload["workflow"] = job.workflow
    if job.forecast_provider is not None:
        payload["forecast_provider"]["name"] = job.forecast_provider
    config = BacktestConfig.model_validate(payload)
    config.artifacts.root_dir = (
        job.output_dir.resolve()
        if job.output_dir is not None
        else (job.config_path.parent / "artifacts" / job.id).resolve()
    )
    return config


def _transition_run_after_export(registry_path: Path, run_id: str, *, profile: str) -> None:
    registry = RunRegistry(registry_path)
    current = registry.get(run_id)
    if current.current_state == "reconciled":
        return
    registry.transition(run_id, "exported", metadata={"profile": profile})


def run_batch(batch_config_path: str | Path) -> Path:
    batch_config = load_batch_config(batch_config_path)
    batch_root = batch_config.artifacts.root_dir / batch_config.batch_name
    batch_root.mkdir(parents=True, exist_ok=True)
    log_path = batch_root / "batch.jsonl"

    results: list[dict[str, Any]] = []
    for job in batch_config.jobs:
        append_jsonl_event(
            log_path,
            "job_started",
            job_id=job.id,
            market=job.market,
            workflow=job.workflow,
            steps=job.steps,
            config_path=str(job.config_path),
        )
        config = _prepare_job_config(job)
        resolved_config_path = batch_root / "resolved_jobs" / f"{job.id}.yaml"
        resolved_config_path.parent.mkdir(parents=True, exist_ok=True)
        resolved_config_path.write_text(
            yaml.safe_dump(config.model_dump(mode="json"), sort_keys=False),
            encoding="utf-8",
        )
        run_dir: Path | None = None
        reconciliation_dir: Path | None = None
        export_dirs: dict[str, str] = {}
        completed_steps: list[str] = []

        for step in job.steps:
            append_jsonl_event(
                log_path,
                "job_step_started",
                job_id=job.id,
                market=job.market,
                workflow=job.workflow,
                step=step,
            )
            if step == "validate_config":
                report = validate_config_file(resolved_config_path)
                if not report.ok:
                    raise ValueError(f"Batch job `{job.id}` failed validate_config")
                completed_steps.append(step)
            elif step == "validate_data":
                report = validate_data_file(resolved_config_path)
                if not report.ok:
                    raise ValueError(f"Batch job `{job.id}` failed validate_data")
                completed_steps.append(step)
            elif step == "backtest":
                result = run_walk_forward(config, forecast_provider_override=job.forecast_provider)
                if result.output_dir is None:
                    raise RuntimeError(f"Batch job `{job.id}` backtest completed without output_dir")
                run_dir = result.output_dir
                register_backtest_result(
                    result,
                    config,
                    launcher=f"batch:{job.id}",
                    log_path=run_dir / "batch.jsonl",
                )
                completed_steps.append(step)
            elif step == "reconcile":
                if run_dir is None:
                    raise ValueError(f"Batch job `{job.id}` cannot reconcile before backtest")
                if job.realized_input_path is None:
                    raise ValueError(f"Batch job `{job.id}` requires realized_input_path for reconcile")
                reconciliation_dir = reconcile_run(run_dir, job.realized_input_path)
                summary_path = run_dir / "summary.json"
                summary = yaml.safe_load(summary_path.read_text(encoding="utf-8"))
                registry_path = registry_path_for_run_dir(run_dir)
                register_derived_artifact(
                    parent_run_id=summary["run_id"],
                    kind="reconcile",
                    market=summary["market_id"],
                    workflow=summary["workflow"],
                    base_workflow=summary["base_workflow"],
                    launcher=f"batch:{job.id}",
                    artifact_path=reconciliation_dir,
                    registry_path=registry_path,
                    schedule_version="revision_latest" if summary["workflow"] == "schedule_revision" else "baseline",
                    metadata={"realized_input": str(job.realized_input_path)},
                )
                RunRegistry(registry_path).transition(summary["run_id"], "reconciled")
                completed_steps.append(step)
            elif step == "export_schedule":
                if run_dir is None:
                    raise ValueError(f"Batch job `{job.id}` cannot export schedule before backtest")
                export_dir = export_schedule(run_dir, profile=job.export_schedule_profile)
                export_dirs["schedule"] = str(export_dir)
                summary = yaml.safe_load((run_dir / "summary.json").read_text(encoding="utf-8"))
                registry_path = registry_path_for_run_dir(run_dir)
                register_derived_artifact(
                    parent_run_id=summary["run_id"],
                    kind="export_schedule",
                    market=summary["market_id"],
                    workflow=summary["workflow"],
                    base_workflow=summary["base_workflow"],
                    launcher=f"batch:{job.id}",
                    artifact_path=export_dir,
                    registry_path=registry_path,
                    schedule_version="revision_latest" if summary["workflow"] == "schedule_revision" else "baseline",
                    metadata={"profile": job.export_schedule_profile},
                )
                _transition_run_after_export(
                    registry_path,
                    summary["run_id"],
                    profile=job.export_schedule_profile,
                )
                completed_steps.append(step)
            elif step == "export_bids":
                if run_dir is None:
                    raise ValueError(f"Batch job `{job.id}` cannot export bids before backtest")
                export_dir = export_bids(run_dir, profile=job.export_bids_profile)
                export_dirs["bids"] = str(export_dir)
                summary = yaml.safe_load((run_dir / "summary.json").read_text(encoding="utf-8"))
                registry_path = registry_path_for_run_dir(run_dir)
                register_derived_artifact(
                    parent_run_id=summary["run_id"],
                    kind="export_bids",
                    market=summary["market_id"],
                    workflow=summary["workflow"],
                    base_workflow=summary["base_workflow"],
                    launcher=f"batch:{job.id}",
                    artifact_path=export_dir,
                    registry_path=registry_path,
                    schedule_version="revision_latest" if summary["workflow"] == "schedule_revision" else "baseline",
                    metadata={"profile": job.export_bids_profile},
                )
                _transition_run_after_export(
                    registry_path,
                    summary["run_id"],
                    profile=job.export_bids_profile,
                )
                completed_steps.append(step)
            elif step == "export_revision":
                if run_dir is None:
                    raise ValueError(f"Batch job `{job.id}` cannot export revision before backtest")
                export_dir = export_revision(run_dir)
                export_dirs["revision"] = str(export_dir)
                summary = yaml.safe_load((run_dir / "summary.json").read_text(encoding="utf-8"))
                registry_path = registry_path_for_run_dir(run_dir)
                register_derived_artifact(
                    parent_run_id=summary["run_id"],
                    kind="export_revision",
                    market=summary["market_id"],
                    workflow=summary["workflow"],
                    base_workflow=summary["base_workflow"],
                    launcher=f"batch:{job.id}",
                    artifact_path=export_dir,
                    registry_path=registry_path,
                    schedule_version="revision_latest",
                    metadata={},
                )
                completed_steps.append(step)
            else:
                raise ValueError(f"Unsupported batch step: {step}")

            append_jsonl_event(
                log_path,
                "job_step_completed",
                job_id=job.id,
                market=job.market,
                workflow=job.workflow,
                step=step,
                run_id=run_dir.name if run_dir is not None else None,
                run_dir=str(run_dir) if run_dir is not None else None,
                reconciliation_dir=str(reconciliation_dir) if reconciliation_dir is not None else None,
                export_dirs=export_dirs,
            )

        result_payload = BatchJobResult(
            job_id=job.id,
            run_dir=run_dir,
            reconciliation_dir=reconciliation_dir,
            export_dirs=export_dirs,
            completed_steps=completed_steps,
        )
        results.append(
            {
                "job_id": result_payload.job_id,
                "run_dir": str(result_payload.run_dir) if result_payload.run_dir is not None else None,
                "reconciliation_dir": str(result_payload.reconciliation_dir)
                if result_payload.reconciliation_dir is not None
                else None,
                "export_dirs": result_payload.export_dirs,
                "completed_steps": result_payload.completed_steps,
            }
        )
        append_jsonl_event(
            log_path,
            "job_completed",
            job_id=job.id,
            market=job.market,
            workflow=job.workflow,
            run_id=run_dir.name if run_dir is not None else None,
            run_dir=str(run_dir) if run_dir is not None else None,
            reconciliation_dir=str(reconciliation_dir) if reconciliation_dir is not None else None,
            export_dirs=export_dirs,
        )

    from .data.io import save_json

    save_json(
        {
            "batch_name": batch_config.batch_name,
            "jobs": results,
        },
        batch_root / "batch_summary.json",
    )
    return batch_root
