from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from .config import BacktestConfig
from .diagnostics import append_jsonl_event
from .types import RunResult

RunState = Literal["draft", "reviewed", "approved", "exported", "superseded", "reconciled"]

VALID_STATE_TRANSITIONS: dict[str, set[str]] = {
    "draft": {"reviewed", "approved", "exported", "superseded", "reconciled"},
    "reviewed": {"approved", "exported", "superseded", "reconciled"},
    "approved": {"exported", "superseded", "reconciled"},
    "exported": {"superseded", "reconciled"},
    "superseded": set(),
    "reconciled": {"superseded"},
}


@dataclass(slots=True)
class RegistryRecord:
    run_id: str
    parent_run_id: str | None
    schedule_version: str | None
    market: str
    workflow: str
    base_workflow: str
    launcher: str
    created_at: str
    current_state: RunState
    artifact_path: str | None
    metadata: dict[str, Any]


def default_registry_path(*, artifacts_root: str | Path | None = None) -> Path:
    root = Path(artifacts_root).resolve() if artifacts_root is not None else Path("artifacts").resolve()
    return root / "run_registry.sqlite3"


def registry_path_for_run_dir(run_dir: str | Path) -> Path:
    resolved = Path(run_dir).resolve()
    if len(resolved.parents) >= 2:
        return resolved.parents[1] / "run_registry.sqlite3"
    return default_registry_path()


def _utc_now() -> str:
    return datetime.now(tz=UTC).isoformat()


class RunRegistry:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    parent_run_id TEXT,
                    schedule_version TEXT,
                    market TEXT NOT NULL,
                    workflow TEXT NOT NULL,
                    base_workflow TEXT NOT NULL,
                    launcher TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    current_state TEXT NOT NULL,
                    artifact_path TEXT,
                    metadata_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS run_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    metadata_json TEXT NOT NULL
                )
                """
            )

    def upsert(
        self,
        *,
        run_id: str,
        parent_run_id: str | None,
        schedule_version: str | None,
        market: str,
        workflow: str,
        base_workflow: str,
        launcher: str,
        current_state: RunState,
        artifact_path: str | Path | None,
        metadata: dict[str, Any] | None = None,
    ) -> RegistryRecord:
        payload = metadata or {}
        created_at = _utc_now()
        with self._connect() as connection:
            existing = connection.execute("SELECT created_at FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if existing is not None:
                created_at = str(existing["created_at"])
            connection.execute(
                """
                INSERT INTO runs (
                    run_id, parent_run_id, schedule_version, market, workflow, base_workflow,
                    launcher, created_at, current_state, artifact_path, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    parent_run_id = excluded.parent_run_id,
                    schedule_version = excluded.schedule_version,
                    market = excluded.market,
                    workflow = excluded.workflow,
                    base_workflow = excluded.base_workflow,
                    launcher = excluded.launcher,
                    current_state = excluded.current_state,
                    artifact_path = excluded.artifact_path,
                    metadata_json = excluded.metadata_json
                """,
                (
                    run_id,
                    parent_run_id,
                    schedule_version,
                    market,
                    workflow,
                    base_workflow,
                    launcher,
                    created_at,
                    current_state,
                    str(artifact_path) if artifact_path is not None else None,
                    json.dumps(payload, sort_keys=True),
                ),
            )
        self.add_event(run_id, "upsert", payload)
        return self.get(run_id)

    def add_event(self, run_id: str, event_type: str, metadata: dict[str, Any] | None = None) -> None:
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO run_events (run_id, event_type, created_at, metadata_json) VALUES (?, ?, ?, ?)",
                (run_id, event_type, _utc_now(), json.dumps(metadata or {}, sort_keys=True)),
            )

    def get(self, run_id: str) -> RegistryRecord:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        if row is None:
            raise KeyError(f"Unknown run_id: {run_id}")
        return RegistryRecord(
            run_id=str(row["run_id"]),
            parent_run_id=row["parent_run_id"],
            schedule_version=row["schedule_version"],
            market=str(row["market"]),
            workflow=str(row["workflow"]),
            base_workflow=str(row["base_workflow"]),
            launcher=str(row["launcher"]),
            created_at=str(row["created_at"]),
            current_state=row["current_state"],
            artifact_path=row["artifact_path"],
            metadata=json.loads(str(row["metadata_json"])),
        )

    def children(self, parent_run_id: str) -> list[RegistryRecord]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM runs WHERE parent_run_id = ? ORDER BY created_at, run_id", (parent_run_id,)
            ).fetchall()
        return [
            RegistryRecord(
                run_id=str(row["run_id"]),
                parent_run_id=row["parent_run_id"],
                schedule_version=row["schedule_version"],
                market=str(row["market"]),
                workflow=str(row["workflow"]),
                base_workflow=str(row["base_workflow"]),
                launcher=str(row["launcher"]),
                created_at=str(row["created_at"]),
                current_state=row["current_state"],
                artifact_path=row["artifact_path"],
                metadata=json.loads(str(row["metadata_json"])),
            )
            for row in rows
        ]

    def transition(self, run_id: str, new_state: RunState, *, metadata: dict[str, Any] | None = None) -> RegistryRecord:
        current = self.get(run_id)
        allowed = VALID_STATE_TRANSITIONS[current.current_state]
        if new_state == current.current_state:
            return current
        if new_state not in allowed:
            raise ValueError(f"Cannot transition {run_id} from {current.current_state} to {new_state}")
        updated = self.upsert(
            run_id=current.run_id,
            parent_run_id=current.parent_run_id,
            schedule_version=current.schedule_version,
            market=current.market,
            workflow=current.workflow,
            base_workflow=current.base_workflow,
            launcher=current.launcher,
            current_state=new_state,
            artifact_path=current.artifact_path,
            metadata={**current.metadata, **(metadata or {})},
        )
        self.add_event(run_id, "transition", {"from": current.current_state, "to": new_state, **(metadata or {})})
        return updated


def _derived_run_id(parent_run_id: str, kind: str, suffix: str | None = None) -> str:
    base = f"{parent_run_id}:{kind}"
    return f"{base}:{suffix}" if suffix else base


def register_backtest_result(
    result: RunResult,
    config: BacktestConfig,
    *,
    launcher: str,
    registry_path: str | Path | None = None,
    log_path: str | Path | None = None,
) -> Path:
    path = (
        Path(registry_path).resolve()
        if registry_path is not None
        else default_registry_path(artifacts_root=config.artifacts.root_dir.parent)
    )
    registry = RunRegistry(path)
    registry.upsert(
        run_id=result.run_id,
        parent_run_id=None,
        schedule_version="revision_latest" if result.revision_schedule is not None else "baseline",
        market=result.market_id,
        workflow=result.workflow,
        base_workflow=result.workflow_family,
        launcher=launcher,
        current_state="draft",
        artifact_path=result.output_dir,
        metadata={
            "site_id": result.site_id,
            "run_scope": result.run_scope,
            "benchmark_name": result.benchmark_name,
            "provider_name": result.provider_name,
        },
    )
    if result.baseline_schedule is not None:
        registry.upsert(
            run_id=_derived_run_id(result.run_id, "baseline"),
            parent_run_id=result.run_id,
            schedule_version="baseline",
            market=result.market_id,
            workflow=result.workflow,
            base_workflow=result.workflow_family,
            launcher=launcher,
            current_state="draft",
            artifact_path=result.output_dir,
            metadata={"artifact": "baseline_schedule"},
        )
    if result.revision_schedule is not None:
        registry.upsert(
            run_id=_derived_run_id(result.run_id, "revision"),
            parent_run_id=result.run_id,
            schedule_version="revision_latest",
            market=result.market_id,
            workflow=result.workflow,
            base_workflow=result.workflow_family,
            launcher=launcher,
            current_state="draft",
            artifact_path=result.output_dir,
            metadata={"artifact": "revision_schedule"},
        )
    if log_path is not None:
        append_jsonl_event(
            log_path,
            "run_registered",
            run_id=result.run_id,
            market=result.market_id,
            workflow=result.workflow,
            warning_count=0,
        )
    return path


def register_derived_artifact(
    *,
    parent_run_id: str,
    kind: Literal["reconcile", "export_schedule", "export_bids", "export_revision"],
    market: str,
    workflow: str,
    base_workflow: str,
    launcher: str,
    artifact_path: str | Path,
    registry_path: str | Path,
    schedule_version: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> RegistryRecord:
    registry = RunRegistry(registry_path)
    state: RunState = "reconciled" if kind == "reconcile" else "exported"
    record = registry.upsert(
        run_id=_derived_run_id(parent_run_id, kind, suffix=schedule_version or None),
        parent_run_id=parent_run_id,
        schedule_version=schedule_version,
        market=market,
        workflow=workflow,
        base_workflow=base_workflow,
        launcher=launcher,
        current_state=state,
        artifact_path=artifact_path,
        metadata={"kind": kind, **(metadata or {})},
    )
    return record
