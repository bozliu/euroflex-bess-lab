#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib
import json
import shlex
import shutil
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))
validation = importlib.import_module("euroflex_bess_lab.validation")
validate_config_file = validation.validate_config_file
validate_data_file = validation.validate_data_file

DEFAULT_CONFIG = REPO_ROOT / "examples" / "configs" / "canonical" / "belgium_full_stack.yaml"
DEFAULT_MARKET = "belgium"
DEFAULT_WORKFLOW = "schedule_revision"
DEFAULT_SCHEDULE_PROFILE = "operator"
DEFAULT_BIDS_PROFILE = "bid_planning"


@dataclass(frozen=True)
class DemoScene:
    prompt_lines: list[str]
    output_lines: list[str]
    duration: float


def _shorten_path(path: str | Path) -> str:
    resolved = Path(path).resolve()
    try:
        return str(resolved.relative_to(REPO_ROOT))
    except ValueError:
        return str(resolved)


def _require_cli() -> None:
    if shutil.which("euroflex") is not None:
        return
    raise RuntimeError("`euroflex` was not found on PATH. Activate the `dl` conda environment before running the demo.")


def _run_shell(command: str) -> str:
    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        shell=True,
        executable="/bin/bash",
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _run_cli(args: list[str]) -> str:
    completed = subprocess.run(
        ["euroflex", *args],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _latest_run_dir() -> Path:
    candidates = [path for path in (REPO_ROOT / "artifacts" / "examples").iterdir() if path.name != ".gitkeep"]
    if not candidates:
        raise RuntimeError("No canonical run directory was created under artifacts/examples")
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def build_demo_payload(
    *,
    config_path: Path,
    market: str,
    workflow: str,
    schedule_profile: str,
    bids_profile: str,
) -> dict[str, Any]:
    _require_cli()
    config_display = _shorten_path(config_path)

    anchor_output = _run_shell(f"printf '%s\\n' {shlex.quote(config_display)}").splitlines()
    config_matches = _run_shell(
        "rg -n "
        + shlex.quote(
            "id: belgium|workflow: schedule_revision|base_workflow: da_plus_afrr|poi_import_limit_mw|poi_export_limit_mw"
        )
        + " "
        + shlex.quote(config_display)
    ).splitlines()

    _run_cli(["validate-config", config_display])
    config_report = validate_config_file(config_path)
    config_lines = [
        "validate-config passed",
        *[
            f"{check.name}: {check.detail}"
            for check in config_report.checks
            if check.status == "pass"
            and check.name in {"config_schema", "asset_count", "run_scope", "workflow_supported", "reserve_product"}
        ],
    ]

    _run_cli(["validate-data", config_display])
    data_report = validate_data_file(config_path)
    data_lines = [
        "validate-data passed",
        *[
            f"{check.name}: {check.detail}"
            for check in data_report.checks
            if check.status in {"pass", "skip"}
            and check.name
            in {
                "day_ahead_fifteen_minute_cadence",
                "day_ahead_delivery_window_coverage",
                "afrr_capacity_up_delivery_window_coverage",
                "afrr_activation_ratio_down_delivery_window_coverage",
                "reserve_feasibility",
            }
        ],
    ]

    _run_cli(["backtest", config_display, "--market", market, "--workflow", workflow])
    run_dir = _latest_run_dir()
    run_dir_display = _shorten_path(run_dir)
    summary = _load_json(run_dir / "summary.json")
    backtest_metrics = [
        f"market={summary['market_id']} workflow={summary['workflow']} base={summary['base_workflow']}",
        f"asset_count={summary['asset_count']} interval_count={summary['interval_count']} checkpoint_count={summary['revision']['checkpoint_count']}",
        f"expected_total_pnl_eur={summary['expected_total_pnl_eur']:.2f}",
        f"reserve_capacity_revenue_eur={summary['reserve_capacity_revenue_eur']:.2f}",
        f"reserve_activation_revenue_eur={summary['reserve_activation_revenue_eur']:.2f}",
    ]
    backtest_lines = [
        f"Run completed: {summary['run_id']}",
        f"Artifacts: {run_dir_display}",
        *backtest_metrics,
    ]

    _run_cli(["reconcile", run_dir_display, config_display])
    reconciliation_dir = run_dir / "reconciliation"
    reconciliation = _load_json(reconciliation_dir / "reconciliation_summary.json")
    reconcile_metrics = [
        f"realized_total_pnl_eur={reconciliation['realized_total_pnl_eur']:.2f}",
        f"delta_vs_revised_expected_eur={reconciliation['delta_vs_revised_expected_eur']:.2f}",
        f"forecast_error_eur={reconciliation['forecast_error_eur']:.2f}",
        f"activation_settlement_deviation_eur={reconciliation['activation_settlement_deviation_eur']:.2f}",
    ]
    reconcile_lines = [
        f"Reconciliation written to {Path(run_dir_display) / 'reconciliation'}",
        *reconcile_metrics,
    ]

    _run_cli(["export-schedule", run_dir_display, "--profile", schedule_profile])
    schedule_dir = run_dir / f"exports/schedule-{schedule_profile}"
    schedule_manifest = _load_json(schedule_dir / "manifest.json")
    schedule_payload = _load_json(schedule_dir / "site_schedule.json")
    schedule_record = schedule_payload["records"][0]
    schedule_metrics = [
        f"profile={schedule_manifest['metadata']['profile']} intended_consumer={schedule_manifest['metadata']['intended_consumer']}",
        f"rows={len(schedule_payload['records'])} schedule_version={schedule_manifest['metadata']['latest_schedule_version']}",
        f"t0 net_export_mw={schedule_record['net_export_mw']:.3f} afrr_up_reserved_mw={schedule_record['afrr_up_reserved_mw']:.3f}",
        f"t0 afrr_down_reserved_mw={schedule_record['afrr_down_reserved_mw']:.3f} soc_mwh={schedule_record['soc_mwh']:.3f}",
    ]
    schedule_lines = [
        f"Schedule export written to {Path(run_dir_display) / 'exports' / f'schedule-{schedule_profile}'}",
        *schedule_metrics,
    ]

    _run_cli(["export-bids", run_dir_display, "--profile", bids_profile])
    bids_dir = run_dir / f"exports/bids-{bids_profile}"
    bids_payload = _load_json(bids_dir / "site_bids.json")
    bid_record = bids_payload["records"][0]
    bid_metrics = [
        f"profile={bids_payload['metadata']['profile']} reserve_product_id={bids_payload['metadata']['reserve_product_id']}",
        f"rows={len(bids_payload['records'])} live_submission_ready={bids_payload['metadata']['live_submission_ready']}",
        f"t0 day_ahead_nominated_net_export_mw={bid_record['day_ahead_nominated_net_export_mw']:.3f}",
        f"t0 reserved_capacity_mw={bid_record['reserved_capacity_mw']:.3f} lock_state={bid_record['lock_state']}",
    ]
    bids_lines = [
        f"Bid export written to {Path(run_dir_display) / 'exports' / f'bids-{bids_profile}'}",
        *bid_metrics,
    ]

    artifact_listing = [
        str(Path(run_dir_display) / "summary.json"),
        str(Path(run_dir_display) / "site_dispatch.parquet"),
        str(Path(run_dir_display) / "reconciliation" / "reconciliation_summary.json"),
        str(Path(run_dir_display) / "exports" / f"schedule-{schedule_profile}" / "site_schedule.json"),
        str(Path(run_dir_display) / "exports" / f"bids-{bids_profile}" / "site_bids.json"),
    ]
    bids_preview = _run_shell(
        f"python -m json.tool {shlex.quote(str((Path(run_dir_display) / 'exports' / f'bids-{bids_profile}' / 'site_bids.json').as_posix()))} | sed -n '1,18p'"
    ).splitlines()

    scenes = [
        DemoScene(
            prompt_lines=[
                f"printf '%s\\n' {config_display}",
                (
                    'rg -n "id: belgium|workflow: schedule_revision|base_workflow: da_plus_afrr|'
                    f'poi_import_limit_mw|poi_export_limit_mw" {config_display}'
                ),
            ],
            output_lines=[*anchor_output, *config_matches],
            duration=2.4,
        ),
        DemoScene(
            prompt_lines=[f"euroflex validate-config {config_display}"],
            output_lines=config_lines,
            duration=2.8,
        ),
        DemoScene(
            prompt_lines=[f"euroflex validate-data {config_display}"],
            output_lines=data_lines,
            duration=3.0,
        ),
        DemoScene(
            prompt_lines=[f"euroflex backtest {config_display} --market {market} --workflow {workflow}"],
            output_lines=backtest_lines,
            duration=6.2,
        ),
        DemoScene(
            prompt_lines=[
                "RUN_DIR=$(ls -td artifacts/examples/* | head -1)",
                f'euroflex reconcile "$RUN_DIR" {config_display}',
            ],
            output_lines=reconcile_lines,
            duration=3.0,
        ),
        DemoScene(
            prompt_lines=[f'euroflex export-schedule "$RUN_DIR" --profile {schedule_profile}'],
            output_lines=schedule_lines,
            duration=2.6,
        ),
        DemoScene(
            prompt_lines=[f'euroflex export-bids "$RUN_DIR" --profile {bids_profile}'],
            output_lines=bids_lines,
            duration=2.4,
        ),
        DemoScene(
            prompt_lines=[
                'find "$RUN_DIR" -maxdepth 3 | rg "summary.json|site_dispatch.parquet|reconciliation_summary.json|site_schedule.json|site_bids.json"',
                f"python -m json.tool \"$RUN_DIR/exports/bids-{bids_profile}/site_bids.json\" | sed -n '1,18p'",
            ],
            output_lines=[*artifact_listing, "", *bids_preview],
            duration=3.8,
        ),
    ]

    return {
        "config_path": str(config_path.resolve()),
        "market": market,
        "workflow": workflow,
        "run_dir": str(run_dir.resolve()),
        "schedule_export_dir": str(schedule_dir.resolve()),
        "bids_export_dir": str(bids_dir.resolve()),
        "reconciliation_dir": str(reconciliation_dir.resolve()),
        "scenes": [asdict(scene) for scene in scenes],
    }


def _play_scenes(payload: dict[str, Any], *, pause_scale: float) -> None:
    for scene in payload["scenes"]:
        for prompt in scene["prompt_lines"]:
            print(f"$ {prompt}")
        for line in scene["output_lines"]:
            print(line)
        print()
        time.sleep(scene["duration"] * pause_scale)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the canonical Belgium full-stack CLI story for demos.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Canonical config path.")
    parser.add_argument("--market", default=DEFAULT_MARKET, help="Market adapter id.")
    parser.add_argument("--workflow", default=DEFAULT_WORKFLOW, help="Workflow family.")
    parser.add_argument("--schedule-profile", default=DEFAULT_SCHEDULE_PROFILE, choices=("benchmark", "operator"))
    parser.add_argument("--bids-profile", default=DEFAULT_BIDS_PROFILE, choices=("benchmark", "bid_planning"))
    parser.add_argument(
        "--write-json", type=Path, default=None, help="Optional path to write the captured demo payload."
    )
    parser.add_argument(
        "--print-transcript",
        action="store_true",
        help="Print the calm terminal transcript with pauses for manual screen recording.",
    )
    parser.add_argument(
        "--pause-scale",
        type=float,
        default=1.0,
        help="Multiplier applied to scene pauses when --print-transcript is used.",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    payload = build_demo_payload(
        config_path=args.config,
        market=args.market,
        workflow=args.workflow,
        schedule_profile=args.schedule_profile,
        bids_profile=args.bids_profile,
    )
    if args.write_json is not None:
        args.write_json.parent.mkdir(parents=True, exist_ok=True)
        args.write_json.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    if args.print_transcript:
        _play_scenes(payload, pause_scale=args.pause_scale)
    else:
        print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
