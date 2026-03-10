from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path
from time import perf_counter
from typing import Any

from euroflex_bess_lab.backtesting.engine import run_walk_forward
from euroflex_bess_lab.config import load_config
from euroflex_bess_lab.exports import export_bids, export_revision, export_schedule
from euroflex_bess_lab.reconciliation import reconcile_run
from euroflex_bess_lab.validation import validate_config_file, validate_data_file

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = REPO_ROOT / "examples" / "configs" / "canonical" / "belgium_full_stack.yaml"


def _assert_report_ok(report: Any, *, report_name: str) -> None:
    if report.ok:
        return
    details = "; ".join(f"{check.name}: {check.detail}" for check in report.checks if check.status == "fail")
    raise RuntimeError(f"{report_name} failed: {details}")


def _timed_call(timings: dict[str, float], name: str, fn, /, *args, **kwargs):
    started = perf_counter()
    result = fn(*args, **kwargs)
    timings[name] = perf_counter() - started
    return result


def run_canonical_pipeline(
    *,
    config_path: Path = DEFAULT_CONFIG,
    market: str = "belgium",
    workflow: str = "schedule_revision",
    schedule_profile: str = "operator",
    bids_profile: str = "bid_planning",
    output_root: Path | None = None,
) -> dict[str, Any]:
    config_path = config_path.resolve()
    resolved_output_root = (
        output_root.resolve() if output_root is not None else Path(tempfile.mkdtemp(prefix="euroflex-canonical-"))
    )

    timings: dict[str, float] = {}
    pipeline_started = perf_counter()

    config_report = _timed_call(timings, "validate_config", validate_config_file, config_path)
    _assert_report_ok(config_report, report_name="validate-config")

    data_report = _timed_call(timings, "validate_data", validate_data_file, config_path)
    _assert_report_ok(data_report, report_name="validate-data")

    loaded_config = load_config(config_path)
    payload = loaded_config.model_dump(mode="json")
    payload["market"]["id"] = market
    payload["workflow"] = workflow
    payload["artifacts"]["root_dir"] = str(resolved_output_root)
    config = loaded_config.__class__.model_validate(payload)

    result = _timed_call(timings, "backtest", run_walk_forward, config)
    if result.output_dir is None:
        raise RuntimeError("Canonical pipeline completed without an artifact directory")

    reconcile_dir = _timed_call(timings, "reconcile", reconcile_run, result.output_dir, config_path)
    schedule_dir = _timed_call(
        timings,
        "export_schedule",
        export_schedule,
        result.output_dir,
        profile=schedule_profile,
    )
    bids_dir = _timed_call(
        timings,
        "export_bids",
        export_bids,
        result.output_dir,
        profile=bids_profile,
    )

    revision_dir: str | None = None
    if (result.output_dir / "revision_schedule.parquet").exists():
        exported_revision_dir = _timed_call(timings, "export_revision", export_revision, result.output_dir)
        revision_dir = str(exported_revision_dir)

    timings["pipeline_total"] = perf_counter() - pipeline_started

    payload = {
        "config_path": str(config_path),
        "market": market,
        "workflow": workflow,
        "run_id": result.run_id,
        "output_root": str(resolved_output_root),
        "run_dir": str(result.output_dir),
        "reconciliation_dir": str(reconcile_dir),
        "schedule_export_dir": str(schedule_dir),
        "bids_export_dir": str(bids_dir),
        "revision_export_dir": revision_dir,
        "timings_seconds": {key: round(value, 6) for key, value in timings.items()},
        "validation": {
            "config": config_report.as_dict(),
            "data": data_report.as_dict(),
        },
    }
    return payload


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the canonical Belgium full-stack pipeline end to end.")
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help="Path to the canonical config to validate and execute.",
    )
    parser.add_argument("--market", default="belgium", help="Market adapter id.")
    parser.add_argument("--workflow", default="schedule_revision", help="Workflow family to run.")
    parser.add_argument(
        "--output-root",
        type=Path,
        default=None,
        help="Optional artifact root directory. Defaults to a temp directory outside the repo.",
    )
    parser.add_argument(
        "--schedule-profile",
        default="operator",
        choices=("benchmark", "operator"),
        help="Profile to use for schedule exports.",
    )
    parser.add_argument(
        "--bids-profile",
        default="bid_planning",
        choices=("benchmark", "bid_planning"),
        help="Profile to use for bid exports.",
    )
    parser.add_argument(
        "--write-json",
        type=Path,
        default=None,
        help="Optional path to persist the pipeline result payload.",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    payload = run_canonical_pipeline(
        config_path=args.config,
        market=args.market,
        workflow=args.workflow,
        schedule_profile=args.schedule_profile,
        bids_profile=args.bids_profile,
        output_root=args.output_root,
    )
    rendered = json.dumps(payload, indent=2, sort_keys=True)
    if args.write_json is not None:
        args.write_json.parent.mkdir(parents=True, exist_ok=True)
        args.write_json.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)


if __name__ == "__main__":
    main()
