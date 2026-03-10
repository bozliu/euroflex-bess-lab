from __future__ import annotations

import json
from pathlib import Path

import jsonschema

from euroflex_bess_lab.backtesting.engine import run_walk_forward
from euroflex_bess_lab.config import load_config
from euroflex_bess_lab.contracts import build_json_schema_bundle, write_json_schemas
from euroflex_bess_lab.exports import export_bids, export_schedule
from euroflex_bess_lab.reconciliation import reconcile_run

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_generated_schema_bundle_matches_checked_in_files(tmp_path: Path) -> None:
    written = write_json_schemas(tmp_path)
    generated = {path.name: json.loads(path.read_text(encoding="utf-8")) for path in written}
    checked_in = {
        path.name: json.loads(path.read_text(encoding="utf-8"))
        for path in sorted((REPO_ROOT / "schemas").glob("*.json"))
    }
    assert generated == checked_in
    assert generated.keys() == build_json_schema_bundle().keys()


def test_canonical_full_stack_output_validates_against_stable_schemas(tmp_path: Path) -> None:
    config = load_config(REPO_ROOT / "examples/configs/canonical/belgium_full_stack.yaml")
    config.artifacts.root_dir = tmp_path / "runs"
    result = run_walk_forward(config)
    assert result.output_dir is not None
    run_dir = result.output_dir

    export_dir = export_schedule(run_dir, profile="operator")
    export_bids(run_dir, profile="bid_planning")
    reconciliation_dir = reconcile_run(run_dir, REPO_ROOT / "examples/configs/canonical/belgium_full_stack.yaml")

    config_schema = json.loads((REPO_ROOT / "schemas" / "config.v4.json").read_text(encoding="utf-8"))
    summary_schema = json.loads((REPO_ROOT / "schemas" / "summary.schema.json").read_text(encoding="utf-8"))
    export_schema = json.loads((REPO_ROOT / "schemas" / "export_manifest.schema.json").read_text(encoding="utf-8"))
    reconciliation_schema = json.loads(
        (REPO_ROOT / "schemas" / "reconciliation_summary.schema.json").read_text(encoding="utf-8")
    )

    jsonschema.validate(
        json.loads((run_dir / "config_snapshot.json").read_text(encoding="utf-8")),
        config_schema,
    )
    jsonschema.validate(json.loads((run_dir / "summary.json").read_text(encoding="utf-8")), summary_schema)
    jsonschema.validate(json.loads((export_dir / "manifest.json").read_text(encoding="utf-8")), export_schema)
    jsonschema.validate(
        json.loads((reconciliation_dir / "reconciliation_summary.json").read_text(encoding="utf-8")),
        reconciliation_schema,
    )
