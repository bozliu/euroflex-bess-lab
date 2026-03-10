from __future__ import annotations

import json
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any


def _package_version() -> str:
    try:
        return version("euroflex-bess-lab")
    except PackageNotFoundError:
        return "1.1.0"


class JsonlLogger:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def emit(self, event: str, **payload: Any) -> None:
        payload.setdefault("job_id", None)
        payload.setdefault("run_id", None)
        payload.setdefault("market", None)
        payload.setdefault("workflow", None)
        payload.setdefault("checkpoint_id", None)
        payload.setdefault("export_profile", None)
        payload.setdefault("connector_freshness", None)
        payload.setdefault("warning_count", 0)
        payload.setdefault("failure_category", None)
        record = {
            "event": event,
            "timestamp_utc": datetime.now(tz=UTC).isoformat(),
            "package_version": _package_version(),
            **payload,
        }
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, default=str) + "\n")


def append_jsonl_event(path: str | Path, event: str, **payload: Any) -> Path:
    logger = JsonlLogger(path)
    logger.emit(event, **payload)
    return Path(path)
