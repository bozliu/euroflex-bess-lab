from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import cast

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .analytics.reporting import load_report_summary
from .backtesting.engine import run_walk_forward
from .batch import run_batch
from .comparison import compare_runs, run_sweep
from .config import BacktestConfig, load_config, load_sweep_config
from .data.connectors import (
    ConnectorFetchMetadata,
    EliaImbalanceConnector,
    EntsoeDayAheadConnector,
    TenneTSettlementPricesConnector,
)
from .data.io import save_json, save_price_series
from .data.normalization import (
    normalize_elia_imbalance_json,
    normalize_entsoe_day_ahead_xml,
    normalize_tennet_settlement_prices_json,
)
from .diagnostics import append_jsonl_event
from .exports import export_bids, export_revision, export_schedule
from .reconciliation import reconcile_run
from .run_registry import RunRegistry, register_backtest_result, register_derived_artifact, registry_path_for_run_dir
from .validation import ValidationReport, validate_config_file, validate_data_file
from .validation import doctor as run_doctor

app = typer.Typer(help="Forecast-aware BESS market benchmarking for European flexibility markets.")
ingest_app = typer.Typer(help="Fetch and normalize public market data.")
console = Console()


def _write_raw_payload(path: Path, payload: object, metadata: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(payload, str):
        path.write_text(payload, encoding="utf-8")
    else:
        save_json(payload if isinstance(payload, dict) else {"data": payload}, path)
    save_json(metadata, path.with_name(f"{path.name}.meta.json"))


def _version_callback(value: bool) -> None:
    if value:
        from . import __version__

        console.print(__version__)
        raise typer.Exit()


def _config_log_path(config_path: Path) -> Path:
    try:
        config = load_config(config_path)
    except Exception:
        return Path("artifacts").resolve() / "cli.jsonl"
    return config.artifacts.root_dir.resolve() / "cli.jsonl"


def _run_log_path(run_dir: Path) -> Path:
    return run_dir.resolve() / "cli.jsonl"


def _load_summary(run_dir: Path) -> dict[str, object]:
    return json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))


def _ensure_parent_run_registered(run_dir: Path, summary: dict[str, object], registry_path: Path) -> None:
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
            launcher="cli_import",
            current_state="draft",
            artifact_path=run_dir,
            metadata={
                "site_id": summary.get("site_id"),
                "run_scope": summary.get("run_scope"),
                "benchmark_name": summary.get("benchmark_name"),
                "provider_name": summary.get("provider_name"),
            },
        )


def _transition_run_after_export(registry_path: Path, run_id: str, *, profile: str) -> None:
    registry = RunRegistry(registry_path)
    current = registry.get(run_id)
    if current.current_state == "reconciled":
        return
    registry.transition(run_id, "exported", metadata={"profile": profile})


@app.callback()
def main_callback(
    version: bool | None = typer.Option(
        None,
        "--version",
        callback=_version_callback,
        is_eager=True,
        help="Show the installed euroflex_bess_lab version and exit.",
    ),
) -> None:
    _ = version


def _render_validation_report(report: ValidationReport) -> None:
    title = f"{report.report_type} {'passed' if report.ok else 'failed'}"
    console.print(Panel(title, style="green" if report.ok else "red"))
    table = Table(show_header=True, header_style="bold")
    table.add_column("Check")
    table.add_column("Status")
    table.add_column("Detail")
    for check in report.checks:
        table.add_row(check.name, check.status.upper(), check.detail)
    console.print(table)
    console.print_json(json=json.dumps(report.as_dict()))


@ingest_app.command("entsoe-da")
def ingest_entsoe_da(
    start: datetime = typer.Option(..., help="Start time in ISO-8601."),
    end: datetime = typer.Option(..., help="End time in ISO-8601."),
    out_raw: Path = typer.Option(..., help="Output path for the raw XML payload."),
    out_parquet: Path = typer.Option(..., help="Output path for the standardized Parquet."),
    zone: str = typer.Option("10YBE----------2", help="ENTSO-E bidding zone EIC."),
    timezone: str = typer.Option("Europe/Brussels", help="Local timezone for normalized timestamps."),
    timeout_seconds: int = typer.Option(30, help="Request timeout in seconds."),
    max_retries: int = typer.Option(0, help="Maximum retries for transient upstream failures."),
    backoff_factor: float = typer.Option(0.5, help="Retry backoff factor."),
    cache_dir: Path | None = typer.Option(None, help="Optional connector cache directory."),
    cache_ttl_minutes: int | None = typer.Option(None, help="Optional cache TTL in minutes."),
) -> None:
    connector = EntsoeDayAheadConnector(timeout_seconds=timeout_seconds)
    xml_payload, metadata = cast(
        tuple[str, ConnectorFetchMetadata],
        connector.fetch(
            start=start,
            end=end,
            zone=zone,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            cache_dir=cache_dir,
            cache_ttl_minutes=cache_ttl_minutes,
            return_metadata=True,
        ),
    )
    _write_raw_payload(out_raw, xml_payload, metadata.as_dict())
    series = normalize_entsoe_day_ahead_xml(xml_payload, zone=zone, local_timezone=timezone)
    save_price_series(series, out_parquet)
    console.print(f"Saved raw ENTSO-E XML to [bold]{out_raw}[/bold]")
    console.print(f"Saved normalized day-ahead prices to [bold]{out_parquet}[/bold]")


@ingest_app.command("elia-imbalance")
def ingest_elia_imbalance(
    start: datetime = typer.Option(..., help="Start time in ISO-8601."),
    end: datetime = typer.Option(..., help="End time in ISO-8601."),
    out_raw: Path = typer.Option(..., help="Output path for the raw JSON payload."),
    out_parquet: Path = typer.Option(..., help="Output path for the standardized Parquet."),
    timeout_seconds: int = typer.Option(30, help="Request timeout in seconds."),
    max_retries: int = typer.Option(0, help="Maximum retries for transient upstream failures."),
    backoff_factor: float = typer.Option(0.5, help="Retry backoff factor."),
    cache_dir: Path | None = typer.Option(None, help="Optional connector cache directory."),
    cache_ttl_minutes: int | None = typer.Option(None, help="Optional cache TTL in minutes."),
) -> None:
    connector = EliaImbalanceConnector(timeout_seconds=timeout_seconds)
    payload, metadata = cast(
        tuple[dict[str, object], ConnectorFetchMetadata],
        connector.fetch(
            start=start,
            end=end,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            cache_dir=cache_dir,
            cache_ttl_minutes=cache_ttl_minutes,
            return_metadata=True,
        ),
    )
    _write_raw_payload(out_raw, payload, metadata.as_dict())
    series = normalize_elia_imbalance_json(payload, local_timezone="Europe/Brussels")
    save_price_series(series, out_parquet)
    console.print(f"Saved raw Elia JSON to [bold]{out_raw}[/bold]")
    console.print(f"Saved normalized imbalance prices to [bold]{out_parquet}[/bold]")


@ingest_app.command("tennet-nl-imbalance")
def ingest_tennet_nl_imbalance(
    start: datetime = typer.Option(..., help="Start time in ISO-8601."),
    end: datetime = typer.Option(..., help="End time in ISO-8601."),
    out_raw: Path = typer.Option(..., help="Output path for the raw JSON payload."),
    out_parquet: Path = typer.Option(..., help="Output path for the standardized Parquet."),
    timeout_seconds: int = typer.Option(30, help="Request timeout in seconds."),
    max_retries: int = typer.Option(0, help="Maximum retries for transient upstream failures."),
    backoff_factor: float = typer.Option(0.5, help="Retry backoff factor."),
    cache_dir: Path | None = typer.Option(None, help="Optional connector cache directory."),
    cache_ttl_minutes: int | None = typer.Option(None, help="Optional cache TTL in minutes."),
) -> None:
    connector = TenneTSettlementPricesConnector(timeout_seconds=timeout_seconds)
    payload, metadata = cast(
        tuple[dict[str, object], ConnectorFetchMetadata],
        connector.fetch(
            start=start,
            end=end,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            cache_dir=cache_dir,
            cache_ttl_minutes=cache_ttl_minutes,
            return_metadata=True,
        ),
    )
    _write_raw_payload(out_raw, payload, metadata.as_dict())
    series = normalize_tennet_settlement_prices_json(payload, local_timezone="Europe/Amsterdam")
    save_price_series(series, out_parquet)
    console.print(f"Saved raw TenneT JSON to [bold]{out_raw}[/bold]")
    console.print(f"Saved normalized NL imbalance prices to [bold]{out_parquet}[/bold]")


@app.command()
def backtest(
    config_path: Path = typer.Argument(
        ..., exists=True, dir_okay=False, readable=True, help="Path to a schema v4 config."
    ),
    market: str | None = typer.Option(None, "--market", help="Market adapter id, such as belgium or netherlands."),
    workflow: str | None = typer.Option(
        None,
        "--workflow",
        help="Workflow family: da_only, da_plus_imbalance, da_plus_fcr, da_plus_afrr, or schedule_revision.",
    ),
    forecast_provider: str | None = typer.Option(
        None, "--forecast-provider", help="Override config forecast provider."
    ),
) -> None:
    config_log_path = _config_log_path(config_path)
    if market is None or workflow is None:
        raise typer.BadParameter("--market and --workflow are required; alias workflow forms have been removed")
    loaded = load_config(config_path)
    payload = loaded.model_dump(mode="json")
    payload["market"]["id"] = market
    payload["workflow"] = workflow
    resolved_config = BacktestConfig.model_validate(payload)
    try:
        result = run_walk_forward(resolved_config, forecast_provider_override=forecast_provider)
    except (ValueError, RuntimeError) as exc:
        append_jsonl_event(
            config_log_path,
            "cli_backtest_failed",
            market=market,
            workflow=workflow,
            warning_count=0,
            failure_category="backtest",
        )
        raise typer.BadParameter(str(exc)) from exc
    if result.output_dir is None:
        raise RuntimeError("run_walk_forward completed without an output directory")
    registry_path = register_backtest_result(
        result,
        resolved_config,
        launcher="cli",
        log_path=_run_log_path(result.output_dir),
    )
    summary = load_report_summary(result.output_dir)
    append_jsonl_event(
        _run_log_path(result.output_dir),
        "cli_backtest_completed",
        run_id=result.run_id,
        market=result.market_id,
        workflow=result.workflow,
        warning_count=0,
    )
    console.print(f"Run completed: [bold]{result.run_id}[/bold]")
    console.print(f"Artifacts: [bold]{result.output_dir}[/bold]")
    console.print(f"Registry: [bold]{registry_path}[/bold]")
    console.print(json.dumps(summary, indent=2))


@app.command()
def compare(
    run_dirs: list[Path] = typer.Argument(..., exists=True, file_okay=False, dir_okay=True),
    output_dir: Path | None = typer.Option(None, help="Directory to write the comparison report."),
    group_by: str | None = typer.Option(None, "--group-by", help="Optional grouping, for example `market`."),
) -> None:
    if output_dir is None:
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        output_dir = run_dirs[0].parent / f"comparison-{timestamp}"
    result_dir = compare_runs(run_dirs, output_dir, group_by=group_by)
    console.print(f"Comparison written to [bold]{result_dir}[/bold]")


@app.command()
def sweep(
    config_path: Path = typer.Argument(..., exists=True, readable=True, help="Path to a YAML sweep config."),
) -> None:
    config = load_sweep_config(config_path)
    comparison_dir = run_sweep(config)
    console.print(f"Sweep completed. Comparison artifacts: [bold]{comparison_dir}[/bold]")


@app.command("validate-config")
def validate_config_command(
    config_path: Path = typer.Argument(
        ..., exists=True, dir_okay=False, readable=True, help="Path to a schema v4 config."
    ),
) -> None:
    report = validate_config_file(config_path)
    append_jsonl_event(
        _config_log_path(config_path),
        "cli_validate_config",
        market=report.metadata.get("market_id"),
        workflow=report.metadata.get("workflow"),
        warning_count=sum(1 for check in report.checks if check.status == "warn"),
        failure_category=None if report.ok else "validation",
    )
    _render_validation_report(report)
    if not report.ok:
        raise typer.Exit(code=1)


@app.command("validate-data")
def validate_data_command(
    config_path: Path = typer.Argument(
        ..., exists=True, dir_okay=False, readable=True, help="Path to a schema v4 config."
    ),
) -> None:
    report = validate_data_file(config_path)
    append_jsonl_event(
        _config_log_path(config_path),
        "cli_validate_data",
        market=report.metadata.get("market_id"),
        workflow=report.metadata.get("workflow"),
        warning_count=sum(1 for check in report.checks if check.status == "warn"),
        failure_category=None if report.ok else "validation",
    )
    _render_validation_report(report)
    if not report.ok:
        raise typer.Exit(code=1)


@app.command()
def doctor(
    config_path: Path | None = typer.Option(
        None,
        "--config",
        exists=True,
        dir_okay=False,
        readable=True,
        help="Optional config path for artifact-path and credential checks.",
    ),
) -> None:
    report = run_doctor(config_path)
    append_jsonl_event(
        _config_log_path(config_path) if config_path is not None else (Path("artifacts").resolve() / "cli.jsonl"),
        "cli_doctor",
        market=report.metadata.get("market_id"),
        workflow=report.metadata.get("workflow"),
        warning_count=sum(1 for check in report.checks if check.status == "warn"),
        failure_category=None if report.ok else "diagnostics",
    )
    _render_validation_report(report)
    if not report.ok:
        raise typer.Exit(code=1)


@app.command()
def report(run_dir: Path = typer.Argument(..., exists=True, file_okay=False, dir_okay=True)) -> None:
    summary = load_report_summary(run_dir)
    table = Table(title=f"Run Summary: {summary['run_id']}")
    table.add_column("Metric")
    table.add_column("Value")
    for key in (
        "market_id",
        "workflow",
        "benchmark_name",
        "provider_name",
        "da_revenue_eur",
        "imbalance_revenue_eur",
        "reserve_capacity_revenue_eur",
        "reserve_activation_revenue_eur",
        "reserve_penalty_eur",
        "degradation_cost_eur",
        "total_pnl_eur",
        "oracle_gap_total_pnl_eur",
    ):
        if key in summary:
            table.add_row(key, str(summary[key]))
    console.print(table)
    console.print(f"Detailed report: [bold]{run_dir / 'report' / 'report.md'}[/bold]")


@app.command("export-schedule")
def export_schedule_command(
    run_dir: Path = typer.Argument(..., exists=True, file_okay=False, dir_okay=True, help="Run artifact directory."),
    output_dir: Path | None = typer.Option(None, "--output-dir", help="Optional export directory."),
    profile: str = typer.Option(
        "benchmark",
        "--profile",
        help="Export profile: benchmark, operator, or submission_candidate.",
    ),
) -> None:
    target = export_schedule(run_dir, output_dir=output_dir, profile=profile)
    summary = _load_summary(run_dir)
    registry_path = registry_path_for_run_dir(run_dir)
    _ensure_parent_run_registered(run_dir, summary, registry_path)
    register_derived_artifact(
        parent_run_id=str(summary["run_id"]),
        kind="export_schedule",
        market=str(summary["market_id"]),
        workflow=str(summary["workflow"]),
        base_workflow=str(summary["base_workflow"]),
        launcher="cli",
        artifact_path=target,
        registry_path=registry_path,
        schedule_version="revision_latest" if str(summary["workflow"]) == "schedule_revision" else "baseline",
        metadata={"profile": profile},
    )
    _transition_run_after_export(registry_path, str(summary["run_id"]), profile=profile)
    append_jsonl_event(
        _run_log_path(run_dir),
        "cli_export_schedule",
        run_id=str(summary["run_id"]),
        market=str(summary["market_id"]),
        workflow=str(summary["workflow"]),
        export_profile=profile,
        warning_count=0,
    )
    console.print(f"Schedule export written to [bold]{target}[/bold]")


@app.command("export-bids")
def export_bids_command(
    run_dir: Path = typer.Argument(..., exists=True, file_okay=False, dir_okay=True, help="Run artifact directory."),
    output_dir: Path | None = typer.Option(None, "--output-dir", help="Optional export directory."),
    profile: str = typer.Option(
        "benchmark",
        "--profile",
        help="Export profile: benchmark, bid_planning, or submission_candidate.",
    ),
) -> None:
    target = export_bids(run_dir, output_dir=output_dir, profile=profile)
    summary = _load_summary(run_dir)
    registry_path = registry_path_for_run_dir(run_dir)
    _ensure_parent_run_registered(run_dir, summary, registry_path)
    register_derived_artifact(
        parent_run_id=str(summary["run_id"]),
        kind="export_bids",
        market=str(summary["market_id"]),
        workflow=str(summary["workflow"]),
        base_workflow=str(summary["base_workflow"]),
        launcher="cli",
        artifact_path=target,
        registry_path=registry_path,
        schedule_version="revision_latest" if str(summary["workflow"]) == "schedule_revision" else "baseline",
        metadata={"profile": profile},
    )
    _transition_run_after_export(registry_path, str(summary["run_id"]), profile=profile)
    append_jsonl_event(
        _run_log_path(run_dir),
        "cli_export_bids",
        run_id=str(summary["run_id"]),
        market=str(summary["market_id"]),
        workflow=str(summary["workflow"]),
        export_profile=profile,
        warning_count=0,
    )
    console.print(f"Bid export written to [bold]{target}[/bold]")


@app.command("export-revision")
def export_revision_command(
    run_dir: Path = typer.Argument(..., exists=True, file_okay=False, dir_okay=True, help="Run artifact directory."),
    output_dir: Path | None = typer.Option(None, "--output-dir", help="Optional export directory."),
) -> None:
    target = export_revision(run_dir, output_dir=output_dir)
    summary = _load_summary(run_dir)
    registry_path = registry_path_for_run_dir(run_dir)
    _ensure_parent_run_registered(run_dir, summary, registry_path)
    register_derived_artifact(
        parent_run_id=str(summary["run_id"]),
        kind="export_revision",
        market=str(summary["market_id"]),
        workflow=str(summary["workflow"]),
        base_workflow=str(summary["base_workflow"]),
        launcher="cli",
        artifact_path=target,
        registry_path=registry_path,
        schedule_version="revision_latest",
        metadata={},
    )
    append_jsonl_event(
        _run_log_path(run_dir),
        "cli_export_revision",
        run_id=str(summary["run_id"]),
        market=str(summary["market_id"]),
        workflow=str(summary["workflow"]),
        warning_count=0,
    )
    console.print(f"Revision export written to [bold]{target}[/bold]")


@app.command()
def reconcile(
    run_dir: Path = typer.Argument(..., exists=True, file_okay=False, dir_okay=True, help="Run artifact directory."),
    settlement_or_realized_input: Path = typer.Argument(
        ..., exists=True, readable=True, help="Path to a realized-input config/file or directory."
    ),
    output_dir: Path | None = typer.Option(None, "--output-dir", help="Optional reconciliation directory."),
) -> None:
    target = reconcile_run(run_dir, settlement_or_realized_input, output_dir=output_dir)
    summary = _load_summary(run_dir)
    registry_path = registry_path_for_run_dir(run_dir)
    _ensure_parent_run_registered(run_dir, summary, registry_path)
    register_derived_artifact(
        parent_run_id=str(summary["run_id"]),
        kind="reconcile",
        market=str(summary["market_id"]),
        workflow=str(summary["workflow"]),
        base_workflow=str(summary["base_workflow"]),
        launcher="cli",
        artifact_path=target,
        registry_path=registry_path,
        schedule_version="revision_latest" if str(summary["workflow"]) == "schedule_revision" else "baseline",
        metadata={"realized_input": str(settlement_or_realized_input)},
    )
    RunRegistry(registry_path).transition(str(summary["run_id"]), "reconciled")
    append_jsonl_event(
        _run_log_path(run_dir),
        "cli_reconcile",
        run_id=str(summary["run_id"]),
        market=str(summary["market_id"]),
        workflow=str(summary["workflow"]),
        warning_count=0,
    )
    console.print(f"Reconciliation written to [bold]{target}[/bold]")


@app.command()
def batch(
    batch_config_path: Path = typer.Argument(
        ..., exists=True, dir_okay=False, readable=True, help="Path to a batch YAML config."
    ),
) -> None:
    target = run_batch(batch_config_path)
    append_jsonl_event(
        target / "batch.jsonl",
        "cli_batch_completed",
        job_id=batch_config_path.stem,
        warning_count=0,
    )
    console.print(f"Batch run completed. Artifacts: [bold]{target}[/bold]")


@app.command()
def serve(
    host: str = typer.Option("127.0.0.1", "--host", help="Bind host for the local service."),
    port: int = typer.Option(8000, "--port", help="Bind port for the local service."),
    reload: bool = typer.Option(False, "--reload", help="Enable code reload for local development."),
) -> None:
    import uvicorn

    uvicorn.run("euroflex_bess_lab.service:app", host=host, port=port, reload=reload)


app.add_typer(ingest_app, name="ingest")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
