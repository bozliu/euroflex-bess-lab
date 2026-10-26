"""Crash-safe persistence of a live run directory.

Layout (all files relative to the run directory, never absolute paths):

- ``config.json``: run settings (strategy, battery, mechanism, publication model, execution adapter);
- ``decisions.jsonl``: one line per decision minute (the journal; source of truth for power and SOC);
- ``qh_ledger.csv``: append-only paper ledger, one line per settlement (corrections append a new revision);
- ``qh_ledger.parquet``: the same ledger, latest revision per quarter-hour, rewritten atomically;
- ``state.json``: snapshot for fast resume (rolled forward from the journal on restart);
- ``status.json``: heartbeat and feed health, rewritten atomically every minute;
- ``feed_log.jsonl``: one line per poll (latency, rows, errors); ``events.jsonl``: start / stop / resume;
- ``archive/raw/<dataset>/<UTC day>.jsonl``: every row version exactly as the upstream API published it
  (new rows, revisions, backfill), with the time it was received; ``archive/normalized/<dataset>/<UTC day>.csv``:
  the same rows in contract form. Nothing that was ever seen live is lost, even after Elia drops it from a
  near-real-time dataset.

JSON files are written to a temporary file and moved into place with ``os.replace``; JSONL and CSV files are
appended line by line. A partial last line left by a crash is truncated on the next open.
"""

from __future__ import annotations

import json
import math
import os
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .feeds import iso_utc

CONFIG = "config.json"
STATE = "state.json"
STATUS = "status.json"
DECISIONS = "decisions.jsonl"
LEDGER_CSV = "qh_ledger.csv"
LEDGER_PARQUET = "qh_ledger.parquet"
FEED_LOG = "feed_log.jsonl"
EVENTS = "events.jsonl"
ARCHIVE = "archive"


def to_jsonable(value: Any) -> Any:
    """Recursively convert numpy / pandas scalars, NaN and timestamps into strict-JSON values."""
    if value is None or isinstance(value, str | bool | int) and not isinstance(value, np.generic):
        return value
    if type(value) is float:
        return None if math.isnan(value) or math.isinf(value) else value
    if isinstance(value, Mapping):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    if isinstance(value, list | tuple | set | frozenset):
        return [to_jsonable(v) for v in value]
    if isinstance(value, pd.Timestamp):
        return iso_utc(value, fractional=True)
    if isinstance(value, np.bool_ | bool):
        return bool(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, float | np.floating):
        number = float(value)
        return None if math.isnan(number) or math.isinf(number) else number
    if isinstance(value, Path):
        return value.as_posix()
    if value is pd.NaT:
        return None
    return value


def _repair_tail(path: Path) -> int:
    """Truncate a partial last line (no trailing newline) left by a crash; returns removed bytes."""
    if not path.exists():
        return 0
    size = path.stat().st_size
    if size == 0:
        return 0
    with path.open("rb+") as handle:
        handle.seek(-1, os.SEEK_END)
        if handle.read(1) == b"\n":
            return 0
        chunk = 4096
        position = size
        cut = 0
        while position > 0:
            step = min(chunk, position)
            position -= step
            handle.seek(position)
            block = handle.read(step)
            index = block.rfind(b"\n")
            if index >= 0:
                cut = position + index + 1
                break
        handle.truncate(cut)
        return size - cut


class RunStore:
    """Reads and writes the files of one live run directory."""

    def __init__(self, run_dir: str | Path, *, fsync: bool = True) -> None:
        self.run_dir = Path(run_dir)
        self.fsync = fsync

    def path(self, name: str) -> Path:
        return self.run_dir / name

    def ensure(self) -> None:
        self.run_dir.mkdir(parents=True, exist_ok=True)

    def exists(self, name: str) -> bool:
        return self.path(name).exists()

    def repair(self) -> dict[str, int]:
        """Remove partial trailing lines from append-only files (after a crash)."""
        out = {name: _repair_tail(self.path(name)) for name in (DECISIONS, LEDGER_CSV, FEED_LOG, EVENTS)}
        archive = self.run_dir / ARCHIVE
        if archive.exists():
            for path in sorted([*archive.rglob("*.jsonl"), *archive.rglob("*.csv")]):
                out[path.relative_to(self.run_dir).as_posix()] = _repair_tail(path)
        return out

    # -- archive --

    def _archive_path(self, kind: str, dataset_id: str, day: str, suffix: str) -> Path:
        path = self.run_dir / ARCHIVE / kind / dataset_id / f"{day}{suffix}"
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def append_archive_raw(self, dataset_id: str, day: str, lines: Iterable[Mapping[str, Any]]) -> None:
        """Append upstream records unchanged (``json.dumps`` of the parsed JSON, key order preserved)."""
        text = "".join(json.dumps(line, separators=(",", ":")) + "\n" for line in lines)
        if not text:
            return
        with self._archive_path("raw", dataset_id, day, ".jsonl").open("a", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            if self.fsync:
                os.fsync(handle.fileno())

    def append_archive_normalized(self, dataset_id: str, day: str, frame: pd.DataFrame) -> None:
        if frame.empty:
            return
        path = self._archive_path("normalized", dataset_id, day, ".csv")
        out = frame.copy()
        for column in out.columns:
            if pd.api.types.is_datetime64_any_dtype(out[column]):
                out[column] = out[column].map(lambda ts: iso_utc(ts))
        header = not path.exists() or path.stat().st_size == 0
        with path.open("a", encoding="utf-8", newline="") as handle:
            out.to_csv(handle, header=header, index=False)
            handle.flush()
            if self.fsync:
                os.fsync(handle.fileno())

    def archived_datasets(self) -> list[str]:
        root = self.run_dir / ARCHIVE / "normalized"
        return sorted(p.name for p in root.iterdir() if p.is_dir()) if root.exists() else []

    def read_archive(
        self, dataset_id: str, *, latest_only: bool = False, time_column: str | None = None
    ) -> pd.DataFrame:
        """All archived versions of ``dataset_id`` rows (normalized); ``latest_only`` keeps the last per row time."""
        root = self.run_dir / ARCHIVE / "normalized" / dataset_id
        files = sorted(root.glob("*.csv")) if root.exists() else []
        if not files:
            return pd.DataFrame()
        frame = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
        for column in frame.columns:
            if column.endswith("_utc"):
                frame[column] = pd.to_datetime(frame[column], utc=True, format="ISO8601")
        if latest_only:
            key = time_column or ("prediction_time_utc" if "prediction_time_utc" in frame.columns else "timestamp_utc")
            frame = frame.sort_values(["received_at_utc"], kind="stable").drop_duplicates(key, keep="last")
            frame = frame.sort_values(key).reset_index(drop=True)
        return frame

    def read_archive_raw(self, dataset_id: str) -> list[dict[str, Any]]:
        root = self.run_dir / ARCHIVE / "raw" / dataset_id
        lines: list[dict[str, Any]] = []
        for path in sorted(root.glob("*.jsonl")) if root.exists() else []:
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    try:
                        lines.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return lines

    # -- JSON --

    def write_json(self, name: str, payload: Mapping[str, Any], *, indent: int | None = 2) -> None:
        target = self.path(name)
        tmp = target.with_name(target.name + ".tmp")
        separators = (",", ":") if indent is None else None
        text = json.dumps(to_jsonable(payload), indent=indent, separators=separators, allow_nan=False) + "\n"
        with tmp.open("w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            if self.fsync:
                os.fsync(handle.fileno())
        os.replace(tmp, target)

    def read_json(self, name: str) -> dict[str, Any] | None:
        target = self.path(name)
        if not target.exists():
            return None
        payload = json.loads(target.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else None

    # -- JSONL --

    def append_jsonl(self, name: str, record: Mapping[str, Any]) -> None:
        self.append_jsonl_many(name, [record])

    def append_jsonl_many(self, name: str, records: Iterable[Mapping[str, Any]]) -> None:
        lines = [json.dumps(to_jsonable(r), separators=(",", ":"), allow_nan=False) for r in records]
        if not lines:
            return
        with self.path(name).open("a", encoding="utf-8") as handle:
            handle.write("\n".join(lines) + "\n")
            handle.flush()
            if self.fsync:
                os.fsync(handle.fileno())

    def read_jsonl(self, name: str) -> list[dict[str, Any]]:
        target = self.path(name)
        if not target.exists():
            return []
        records: list[dict[str, Any]] = []
        with target.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue  # partial line from a crash; repair() removes it before appending
                if isinstance(payload, dict):
                    records.append(payload)
        return records

    # -- ledger --

    def append_ledger(self, rows: pd.DataFrame) -> pd.DataFrame:
        """Append settlement rows to the CSV journal and rewrite the deduplicated parquet ledger."""
        if rows.empty:
            return self.read_ledger()
        csv_path = self.path(LEDGER_CSV)
        out = rows.copy()
        for column in out.columns:
            if pd.api.types.is_datetime64_any_dtype(out[column]):
                out[column] = out[column].map(lambda ts: iso_utc(ts, fractional=True))
        header = not csv_path.exists() or csv_path.stat().st_size == 0
        with csv_path.open("a", encoding="utf-8", newline="") as handle:
            out.to_csv(handle, header=header, index=False)
            handle.flush()
            if self.fsync:
                os.fsync(handle.fileno())
        ledger = self.read_ledger()
        tmp = self.path(LEDGER_PARQUET + ".tmp")
        ledger.to_parquet(tmp, index=False)
        os.replace(tmp, self.path(LEDGER_PARQUET))
        return ledger

    def read_ledger(self, *, all_revisions: bool = False) -> pd.DataFrame:
        csv_path = self.path(LEDGER_CSV)
        if not csv_path.exists() or csv_path.stat().st_size == 0:
            return pd.DataFrame()
        frame = pd.read_csv(csv_path)
        for column in frame.columns:
            if column.endswith("_utc"):
                frame[column] = pd.to_datetime(frame[column], utc=True, format="ISO8601")
        if "price_missing" in frame.columns:
            frame["price_missing"] = frame["price_missing"].astype(str).str.lower().isin({"true", "1"})
        if "provisional" in frame.columns:
            frame["provisional"] = frame["provisional"].astype(str).str.lower().isin({"true", "1"})
        if all_revisions or frame.empty:
            return frame
        frame = frame.sort_values(["timestamp_utc", "revision"], kind="stable")
        return frame.drop_duplicates("timestamp_utc", keep="last").reset_index(drop=True)
