from __future__ import annotations

import json
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_perf_baseline_file_exists_and_is_well_formed() -> None:
    baseline_path = REPO_ROOT / "tests" / "perf_baselines" / "canonical_pipeline.json"
    payload = json.loads(baseline_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == 1
    assert payload["tolerance_ratio"] > 1.0
    assert payload["slack_seconds"] >= 0.0
    assert "pipeline_total" in payload["stages"]
    assert all(value > 0.0 for value in payload["stages"].values())


def test_ci_workflow_enforces_canonical_pipeline_and_perf_gates() -> None:
    workflow = yaml.safe_load((REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8"))
    jobs = workflow["jobs"]
    assert "canonical-ga-smoke" in jobs
    smoke_steps = "\n".join(
        step.get("run", "") for step in jobs["canonical-ga-smoke"]["steps"] if isinstance(step, dict)
    )
    assert "scripts/canonical_pipeline.py" in smoke_steps
    assert "make perf-check" in smoke_steps

    docker_steps = "\n".join(
        step.get("run", "") for step in jobs["docker-smoke-test"]["steps"] if isinstance(step, dict)
    )
    assert "python scripts/canonical_pipeline.py" in docker_steps
    assert "docker compose up -d notebooks" in docker_steps

    package_steps = "\n".join(step.get("run", "") for step in jobs["package"]["steps"] if isinstance(step, dict))
    assert "python scripts/canonical_pipeline.py" in package_steps


def test_release_workflow_uses_canonical_pipeline_smoke() -> None:
    workflow = yaml.safe_load((REPO_ROOT / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8"))
    steps = "\n".join(
        step.get("run", "") for step in workflow["jobs"]["build-and-publish"]["steps"] if isinstance(step, dict)
    )
    assert "python scripts/canonical_pipeline.py" in steps


def test_docs_nav_exposes_commercial_positioning_page() -> None:
    mkdocs = yaml.safe_load((REPO_ROOT / "mkdocs.yml").read_text(encoding="utf-8"))
    rendered_nav = json.dumps(mkdocs["nav"])
    assert "commercial_positioning.md" in rendered_nav
