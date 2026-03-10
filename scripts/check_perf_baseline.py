from __future__ import annotations

import argparse
import json
from pathlib import Path

from canonical_pipeline import DEFAULT_CONFIG, run_canonical_pipeline

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASELINE = REPO_ROOT / "tests" / "perf_baselines" / "canonical_pipeline.json"


def _allowed_runtime(stage: str, *, baseline_seconds: float, ratio: float, slack_seconds: float) -> float:
    if stage == "pipeline_total":
        return baseline_seconds * ratio + slack_seconds
    return baseline_seconds * ratio + slack_seconds


def main() -> None:
    parser = argparse.ArgumentParser(description="Check canonical pipeline timings against a checked-in baseline.")
    parser.add_argument("--baseline", type=Path, default=DEFAULT_BASELINE, help="Checked-in performance baseline JSON.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Canonical config to benchmark.")
    parser.add_argument("--market", default="belgium", help="Market adapter id.")
    parser.add_argument("--workflow", default="schedule_revision", help="Workflow family to benchmark.")
    parser.add_argument("--write-json", type=Path, default=None, help="Optional path for the measured timing report.")
    args = parser.parse_args()

    baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
    ratio = float(baseline["tolerance_ratio"])
    slack_seconds = float(baseline["slack_seconds"])
    recorded = baseline["stages"]

    payload = run_canonical_pipeline(config_path=args.config, market=args.market, workflow=args.workflow)
    measured = payload["timings_seconds"]

    verdicts: list[dict[str, float | str | bool]] = []
    failures: list[str] = []
    for stage, baseline_seconds in recorded.items():
        actual = float(measured[stage])
        allowed = _allowed_runtime(
            stage, baseline_seconds=float(baseline_seconds), ratio=ratio, slack_seconds=slack_seconds
        )
        passed = actual <= allowed
        verdicts.append(
            {
                "stage": stage,
                "baseline_seconds": float(baseline_seconds),
                "actual_seconds": actual,
                "allowed_seconds": round(allowed, 6),
                "passed": passed,
            }
        )
        if not passed:
            failures.append(
                f"{stage} exceeded tolerance: actual={actual:.3f}s allowed={allowed:.3f}s baseline={baseline_seconds:.3f}s"
            )

    report = {
        "baseline_path": str(args.baseline.resolve()),
        "config_path": str(args.config.resolve()),
        "market": args.market,
        "workflow": args.workflow,
        "tolerance_ratio": ratio,
        "slack_seconds": slack_seconds,
        "verdicts": verdicts,
        "pipeline": payload,
    }
    rendered = json.dumps(report, indent=2, sort_keys=True)
    if args.write_json is not None:
        args.write_json.parent.mkdir(parents=True, exist_ok=True)
        args.write_json.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)

    if failures:
        raise SystemExit("\n".join(failures))


if __name__ == "__main__":
    main()
