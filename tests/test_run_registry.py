from __future__ import annotations

from pathlib import Path

import pytest

from euroflex_bess_lab.run_registry import RunRegistry


def test_run_registry_tracks_transitions_and_children(tmp_path: Path) -> None:
    registry = RunRegistry(tmp_path / "run_registry.sqlite3")
    parent = registry.upsert(
        run_id="run-001",
        parent_run_id=None,
        schedule_version="baseline",
        market="belgium",
        workflow="schedule_revision",
        base_workflow="da_plus_afrr",
        launcher="test",
        current_state="draft",
        artifact_path=tmp_path / "artifacts" / "run-001",
        metadata={"run_scope": "portfolio"},
    )
    child = registry.upsert(
        run_id="run-001:baseline",
        parent_run_id=parent.run_id,
        schedule_version="baseline",
        market="belgium",
        workflow="schedule_revision",
        base_workflow="da_plus_afrr",
        launcher="test",
        current_state="draft",
        artifact_path=tmp_path / "artifacts" / "run-001",
        metadata={"artifact": "baseline_schedule"},
    )

    reviewed = registry.transition(parent.run_id, "reviewed")
    approved = registry.transition(parent.run_id, "approved")
    exported = registry.transition(parent.run_id, "exported")
    reconciled = registry.transition(parent.run_id, "reconciled")

    assert child.parent_run_id == parent.run_id
    assert reviewed.current_state == "reviewed"
    assert approved.current_state == "approved"
    assert exported.current_state == "exported"
    assert reconciled.current_state == "reconciled"
    assert [record.run_id for record in registry.children(parent.run_id)] == [child.run_id]


def test_run_registry_rejects_invalid_transition(tmp_path: Path) -> None:
    registry = RunRegistry(tmp_path / "run_registry.sqlite3")
    registry.upsert(
        run_id="run-002",
        parent_run_id=None,
        schedule_version="baseline",
        market="belgium",
        workflow="schedule_revision",
        base_workflow="da_plus_afrr",
        launcher="test",
        current_state="draft",
        artifact_path=None,
        metadata={},
    )
    registry.transition("run-002", "reviewed")
    with pytest.raises(ValueError, match="Cannot transition"):
        registry.transition("run-002", "draft")
