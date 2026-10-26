"""Elia Open Data (Opendatasoft Explore v2.1) parquet exports with chunked, cached downloads."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .http import FetchRecord, HttpClient, cached_download, validate_parquet_file

ELIA_API = "https://opendata.elia.be/api/explore/v2.1/catalog/datasets"
ELIA_LICENCE = "CC BY 4.0"
ELIA_LICENCE_URL = "https://opendata.elia.be/pages/licence/"
ELIA_ATTRIBUTION = "Elia Transmission Belgium SA"


def export_url(dataset_id: str) -> str:
    return f"{ELIA_API}/{dataset_id}/exports/parquet"


def dataset_page_url(dataset_id: str) -> str:
    return f"https://opendata.elia.be/explore/dataset/{dataset_id}/"


@dataclass(frozen=True)
class Chunk:
    """A half-open UTC window ``[start, end)``; ``None`` bounds mean "the whole dataset"."""

    start: pd.Timestamp | None
    end: pd.Timestamp | None

    @property
    def label(self) -> str:
        if self.start is None or self.end is None:
            return "all"
        return f"{self.start:%Y-%m-%d}__{self.end:%Y-%m-%d}"


def to_utc(value: str | pd.Timestamp | object) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def plan_chunks(start: pd.Timestamp, end: pd.Timestamp, freq: str) -> list[Chunk]:
    """Cover ``[start, end)`` with calendar-aligned UTC month (``"month"``) or year (``"year"``) chunks.

    Chunks are always aligned to calendar boundaries (the first chunk starts at the month/year containing
    ``start``, the last one ends at the boundary after ``end``) so cached files can be reused across requests.
    """

    start = to_utc(start)
    end = to_utc(end)
    if freq == "all":
        return [Chunk(None, None)]
    if end <= start:
        return []
    if freq == "month":
        first = start.normalize().replace(day=1)
        step = pd.DateOffset(months=1)
    elif freq == "year":
        first = start.normalize().replace(month=1, day=1)
        step = pd.DateOffset(years=1)
    else:
        raise ValueError(f"Unknown chunk frequency {freq!r}")
    chunks: list[Chunk] = []
    cursor = first
    while cursor < end:
        nxt = cursor + step
        chunks.append(Chunk(cursor, nxt))
        cursor = nxt
    return chunks


def where_clause(time_field: str, chunk: Chunk) -> str | None:
    if chunk.start is None or chunk.end is None:
        return None
    return f'{time_field}>="{chunk.start:%Y-%m-%dT%H:%M:%SZ}" and {time_field}<"{chunk.end:%Y-%m-%dT%H:%M:%SZ}"'


def chunk_cache_path(raw_dir: Path, dataset_id: str, chunk: Chunk) -> Path:
    return raw_dir / "elia" / dataset_id / f"{dataset_id}__{chunk.label}.parquet"


@dataclass(frozen=True)
class RawChunk:
    dataset_id: str
    chunk: Chunk
    path: Path
    record: FetchRecord


def fetch_dataset(
    client: HttpClient,
    raw_dir: Path,
    dataset_id: str,
    time_field: str,
    start: pd.Timestamp | None,
    end: pd.Timestamp | None,
    *,
    freq: str = "month",
    refresh: bool = False,
    offline: bool = False,
    frozen_after: pd.Timestamp | None = None,
) -> list[RawChunk]:
    """Download (or reuse) the parquet export of ``dataset_id`` for ``[start, end)`` chunked by ``freq``.

    ``frozen_after`` marks datasets whose content no longer changes after that instant (e.g. the ODS211/ODS212
    simulation of Jan-Jul 2026): their cached chunks are treated as closed and only refreshed with ``refresh``.
    """

    if start is None or end is None or freq == "all":
        chunks = [Chunk(None, None)]
    else:
        chunks = plan_chunks(start, end, freq)
    results: list[RawChunk] = []
    for chunk in chunks:
        params: dict[str, str] = {}
        clause = where_clause(time_field, chunk)
        if clause:
            params["where"] = clause
        dest = chunk_cache_path(raw_dir, dataset_id, chunk)
        record = cached_download(
            client,
            export_url(dataset_id),
            dest,
            params=params,
            validate=validate_parquet_file,
            refresh=refresh,
            period_end_utc=frozen_after or (chunk.end if chunk.end is not None else pd.Timestamp.now(tz="UTC")),
            offline=offline,
        )
        results.append(RawChunk(dataset_id, chunk, dest, record))
    return results


def read_chunks(chunks: Sequence[RawChunk], columns: Sequence[str] | None = None) -> pd.DataFrame:
    """Concatenate cached chunks into one DataFrame (missing optional columns are added as nulls)."""

    tables: list[pa.Table] = []
    for raw in chunks:
        table = pq.read_table(raw.path)
        if columns is not None:
            present = [c for c in columns if c in table.column_names]
            table = table.select(present)
        if table.num_rows:
            tables.append(table)
    if not tables:
        return pd.DataFrame(columns=list(columns or []))
    frames = [t.to_pandas() for t in tables]
    frame = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    if columns is not None:
        for column in columns:
            if column not in frame.columns:
                frame[column] = pd.NA
    return frame


def chunk_provenance(chunks: Sequence[RawChunk]) -> list[dict[str, str | int]]:
    return [
        {
            "dataset_id": raw.dataset_id,
            "chunk": raw.chunk.label,
            "url": raw.record.full_url,
            "retrieved_at_utc": raw.record.retrieved_at_utc,
            "sha256": raw.record.sha256,
            "size_bytes": raw.record.size_bytes,
        }
        for raw in chunks
    ]
