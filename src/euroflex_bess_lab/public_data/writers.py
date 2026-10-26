"""Writers for the public data tree: zstd parquet + ``.meta.json`` sidecar, ``MANIFEST.json`` and verification."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .http import sha256_file

MANIFEST_NAME = "MANIFEST.json"
SIDECAR_SUFFIX = ".meta.json"
SCHEMA_VERSION = 1
MAX_FILE_BYTES = 50 * 1024 * 1024
MAX_TOTAL_BYTES = 200 * 1024 * 1024
ZSTD_LEVEL = 9
ROW_GROUP_SIZE = 131_072
DICTIONARY_MAX_DISTINCT = 16_384


def parquet_write_options(schema: pa.Schema, table: pa.Table | None = None) -> dict[str, Any]:
    """zstd + lossless encodings that compress market time series well.

    Floats use BYTE_STREAM_SPLIT (about half the size of dictionary/plain for noisy float32 series) unless they
    have few distinct values (then dictionary encoding wins), timestamps DELTA_BINARY_PACKED (regular grids shrink
    to almost nothing), strings and integers dictionary encoding.
    """

    floats = [f.name for f in schema if pa.types.is_floating(f.type)]
    if table is not None:
        floats = [c for c in floats if pc.count_distinct(table.column(c)).as_py() > DICTIONARY_MAX_DISTINCT]
    stamps = [f.name for f in schema if pa.types.is_timestamp(f.type)]
    encodings = {**{c: "BYTE_STREAM_SPLIT" for c in floats}, **{c: "DELTA_BINARY_PACKED" for c in stamps}}
    dictionary = [f.name for f in schema if f.name not in encodings]
    return {
        "compression": "zstd",
        "compression_level": ZSTD_LEVEL,
        "row_group_size": ROW_GROUP_SIZE,
        "use_dictionary": dictionary,
        "column_encoding": encodings or None,
        "write_statistics": True,
    }


def sidecar_path(path: Path) -> Path:
    return path.with_name(path.name.removesuffix(".parquet") + SIDECAR_SUFFIX)


def _iso(ts: object) -> str | None:
    if ts is None or (isinstance(ts, float) and pd.isna(ts)):
        return None
    value = pd.Timestamp(ts)
    if pd.isna(value):
        return None
    if value.tzinfo is None:
        value = value.tz_localize("UTC")
    return value.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")


def coverage(
    frame: pd.DataFrame, time_column: str, end_column: str | None = None, step: pd.Timedelta | None = None
) -> tuple[str | None, str | None]:
    """Coverage as ``[start, end)``: end is the max of ``end_column`` or the last timestamp + ``step``."""

    if frame.empty or time_column not in frame.columns:
        return None, None
    start = frame[time_column].min()
    if end_column is not None and end_column in frame.columns:
        end = frame[end_column].max()
    else:
        end = frame[time_column].max()
        if step is not None:
            end = end + step
    return _iso(start), _iso(end)


@dataclass(frozen=True)
class WrittenTable:
    path: Path
    relpath: str
    rows: int
    size_bytes: int
    sidecar: dict[str, Any]


def write_table(
    frame: pd.DataFrame,
    root: Path,
    relpath: str,
    meta: Mapping[str, Any],
    *,
    max_file_bytes: int = MAX_FILE_BYTES,
) -> WrittenTable:
    """Write ``frame`` to ``root/relpath`` (zstd parquet, no index) and its sidecar; return the sidecar dict."""

    path = root / relpath
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = frame.reset_index(drop=True)
    for column in frame.columns:
        if isinstance(frame[column].dtype, pd.DatetimeTZDtype):
            frame[column] = frame[column].dt.tz_convert("UTC").astype("datetime64[ns, UTC]")
    table = pa.Table.from_pandas(frame, preserve_index=False)
    part = path.with_name(path.name + ".tmp")
    pq.write_table(table, part, **parquet_write_options(table.schema, table))
    size = part.stat().st_size
    if size > max_file_bytes:
        part.unlink()
        raise ValueError(f"{relpath} is {size / 1e6:.1f} MB which exceeds the per-file budget; partition it further")
    part.replace(path)
    sidecar: dict[str, Any] = {"schema_version": SCHEMA_VERSION, "path": relpath}
    sidecar.update(dict(meta))
    sidecar["rows"] = int(len(frame))
    sidecar["columns"] = [{"name": f.name, "type": str(f.type)} for f in table.schema]
    sidecar["size_bytes"] = int(size)
    sidecar["sha256"] = sha256_file(path)
    sidecar_path(path).write_text(json.dumps(sidecar, indent=2, sort_keys=False, default=str) + "\n", encoding="utf-8")
    return WrittenTable(path=path, relpath=relpath, rows=int(len(frame)), size_bytes=int(size), sidecar=sidecar)


def write_stub_sidecar(root: Path, relpath: str, meta: Mapping[str, Any]) -> Path:
    """Sidecar for a table that is *not* redistributed (data lives in the raw cache only)."""

    path = root / relpath
    path.parent.mkdir(parents=True, exist_ok=True)
    sidecar: dict[str, Any] = {"schema_version": SCHEMA_VERSION, "path": relpath}
    sidecar.update(dict(meta))
    sidecar["redistributed"] = False
    target = sidecar_path(path)
    target.write_text(json.dumps(sidecar, indent=2, default=str) + "\n", encoding="utf-8")
    return target


def collect_sidecars(root: Path) -> list[dict[str, Any]]:
    sidecars: list[dict[str, Any]] = []
    for meta_path in sorted(root.rglob(f"*{SIDECAR_SUFFIX}")):
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
        payload.setdefault("path", meta_path.relative_to(root).as_posix().removesuffix(SIDECAR_SUFFIX) + ".parquet")
        sidecars.append(payload)
    return sorted(sidecars, key=lambda p: str(p["path"]))


def build_manifest(root: Path) -> dict[str, Any]:
    """Aggregate every sidecar under ``root`` into ``MANIFEST.json`` (deterministic: no wall-clock fields)."""

    sidecars = collect_sidecars(root)
    redistributed = [s for s in sidecars if s.get("redistributed", True)]
    total = int(sum(int(s.get("size_bytes", 0)) for s in redistributed))
    licences: dict[str, list[str]] = {}
    for s in sidecars:
        licences.setdefault(str(s.get("licence", "unknown")), []).append(str(s["path"]))
    retrieved = [str(s["retrieved_at_utc"]) for s in sidecars if s.get("retrieved_at_utc")]
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "description": "Normalized public market data for euroflex-bess-lab; see README.md and LICENSES.md.",
        "data_root": "data/public",
        "latest_retrieval_utc": max(retrieved) if retrieved else None,
        "n_files": len(redistributed),
        "n_not_redistributed": len(sidecars) - len(redistributed),
        "total_size_bytes": total,
        "budget": {"max_total_bytes": MAX_TOTAL_BYTES, "max_file_bytes": MAX_FILE_BYTES},
        "licences": {k: sorted(v) for k, v in sorted(licences.items())},
        "files": sidecars,
    }
    (root / MANIFEST_NAME).write_text(json.dumps(manifest, indent=2, default=str) + "\n", encoding="utf-8")
    return manifest


@dataclass(frozen=True)
class VerifyIssue:
    path: str
    problem: str


def verify_manifest(root: Path, manifest: Mapping[str, Any] | None = None) -> tuple[list[str], list[VerifyIssue]]:
    """Recompute sha256 of every redistributed file listed in the manifest; also flag unlisted parquet files."""

    if manifest is None:
        manifest_path = root / MANIFEST_NAME
        if not manifest_path.exists():
            raise FileNotFoundError(f"No {MANIFEST_NAME} under {root}; run `euroflex data manifest` first.")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    ok: list[str] = []
    issues: list[VerifyIssue] = []
    files: Sequence[Mapping[str, Any]] = manifest.get("files", [])
    listed = set()
    for entry in files:
        rel = str(entry["path"])
        listed.add(rel)
        if not entry.get("redistributed", True):
            continue
        path = root / rel
        if not path.exists():
            issues.append(VerifyIssue(rel, "missing file"))
            continue
        actual = sha256_file(path)
        if actual != entry.get("sha256"):
            issues.append(VerifyIssue(rel, f"sha256 mismatch (manifest {entry.get('sha256')}, actual {actual})"))
            continue
        ok.append(rel)
    for parquet in sorted(root.rglob("*.parquet")):
        rel = parquet.relative_to(root).as_posix()
        if rel not in listed:
            issues.append(VerifyIssue(rel, "parquet file not listed in manifest"))
    total = sum(int(e.get("size_bytes", 0)) for e in files if e.get("redistributed", True))
    if total > MAX_TOTAL_BYTES:
        issues.append(
            VerifyIssue("*", f"total size {total / 1e6:.1f} MB exceeds budget {MAX_TOTAL_BYTES / 1e6:.0f} MB")
        )
    return ok, issues
