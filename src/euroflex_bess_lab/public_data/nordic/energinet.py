"""Energi Data Service (Energinet) client with a raw cache under ``data/raw/nordic/energinet``.

API facts used here (checked 2026-10-26):

* ``start``/``end`` are Danish local time unless ``timezone=UTC`` is passed; ``end`` is exclusive; ``limit=0``
  returns every record; ``columns=`` and ``filter={"PriceArea": [...]}`` shrink the payload server-side.
* Rate limits are set per dataset and are strict for discontinued datasets (HTTP 429 with a ``Retry-After`` of
  several minutes; repeated excess can block the IP). The client therefore makes few, large requests: one per
  calendar year for active datasets and a single request for discontinued ones, and it honours ``Retry-After``.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from ..http import FetchError, FetchRecord, HttpClient, HttpConfig, cached_download
from .sources import ENERGINET_DATASETS, EnerginetDataset

# Long backoff cap so that a Retry-After of a few minutes is honoured instead of hammering the API.
ENERGINET_HTTP = HttpConfig(
    read_timeout_s=900.0, max_retries=3, backoff_s=30.0, backoff_cap_s=900.0, min_interval_s=2.0
)


@dataclass(frozen=True)
class EnerginetChunk:
    dataset: str
    start: pd.Timestamp  # inclusive (UTC)
    end: pd.Timestamp  # exclusive (UTC)
    path: Path
    record: FetchRecord


def validate_energinet_json(path: Path) -> None:
    """A valid payload is a JSON object with a ``records`` list (the API returns ``{"total", ..., "records"}``)."""

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FetchError(f"Energi Data Service payload is not JSON: {path.name}") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("records"), list):
        raise FetchError(f"Energi Data Service payload has no `records` list: {path.name}")


def _utc(value: str | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def dataset_chunks(
    spec: EnerginetDataset, start: pd.Timestamp, end: pd.Timestamp
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    """Half-open UTC request windows for ``spec`` covering ``[start, end)`` within the dataset's own coverage.

    ``chunk="all"`` gives one window from the dataset start to its end (discontinued datasets); ``"year"`` gives
    calendar-year windows (the open year is requested up to its end; future rows simply do not exist yet).
    """

    lo = max(_utc(start), _utc(spec.first_utc))
    hi = _utc(end)
    if spec.end_utc is not None:
        hi = min(hi, _utc(spec.end_utc))
    if hi <= lo:
        return []
    if spec.chunk == "all":
        first = _utc(spec.first_utc)
        last = _utc(spec.end_utc) if spec.end_utc is not None else hi
        return [(first, last)]
    windows: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    for year in range(lo.year, (hi - pd.Timedelta(microseconds=1)).year + 1):
        y0 = pd.Timestamp(year=year, month=1, day=1, tz="UTC")
        y1 = pd.Timestamp(year=year + 1, month=1, day=1, tz="UTC")
        w0 = max(y0, _utc(spec.first_utc))
        w1 = min(y1, _utc(spec.end_utc)) if spec.end_utc is not None else y1
        if w1 > w0:
            windows.append((w0, w1))
    return windows


def chunk_params(spec: EnerginetDataset, start: pd.Timestamp, end: pd.Timestamp) -> dict[str, str]:
    params = {
        "start": f"{start:%Y-%m-%dT%H:%M}",
        "end": f"{end:%Y-%m-%dT%H:%M}",
        "timezone": "UTC",
        "limit": "0",
        "sort": f"{spec.time_column} asc",
        "columns": ",".join(spec.columns),
    }
    if spec.price_areas:
        params["filter"] = json.dumps({"PriceArea": list(spec.price_areas)}, separators=(",", ":"))
    return params


def chunk_filename(spec: EnerginetDataset, start: pd.Timestamp, end: pd.Timestamp) -> str:
    if spec.chunk == "all":
        return f"{spec.name}_{start:%Y%m%d}_{end:%Y%m%d}.json"
    return f"{spec.name}_{start:%Y}.json"


def fetch_chunk(
    client: HttpClient,
    raw_dir: Path,
    dataset: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    *,
    refresh: bool = False,
    offline: bool = False,
) -> EnerginetChunk:
    spec = ENERGINET_DATASETS[dataset]
    dest = raw_dir / "nordic" / "energinet" / spec.name / chunk_filename(spec, start, end)
    record = cached_download(
        client,
        spec.api_url,
        dest,
        params=chunk_params(spec, start, end),
        validate=validate_energinet_json,
        refresh=refresh,
        period_end_utc=end,
        offline=offline,
    )
    return EnerginetChunk(spec.name, start, end, dest, record)


def fetch_range(
    client: HttpClient,
    raw_dir: Path,
    dataset: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    *,
    refresh: bool = False,
    offline: bool = False,
) -> list[EnerginetChunk]:
    spec = ENERGINET_DATASETS[dataset]
    return [
        fetch_chunk(client, raw_dir, dataset, w0, w1, refresh=refresh, offline=offline)
        for w0, w1 in dataset_chunks(spec, start, end)
    ]


def read_records(chunks: Sequence[EnerginetChunk]) -> pd.DataFrame:
    """Concatenate the ``records`` of cached chunks into one frame (columns as published)."""

    frames: list[pd.DataFrame] = []
    for chunk in chunks:
        payload = json.loads(chunk.path.read_text(encoding="utf-8"))
        records = payload.get("records") or []
        if records:
            frames.append(pd.DataFrame.from_records(records))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def parse_utc(values: pd.Series) -> pd.Series:
    """Energi Data Service ``HourUTC``/``TimeUTC`` strings (naive, UTC) -> tz-aware UTC timestamps."""

    return pd.Series(pd.to_datetime(values, utc=True), index=values.index).astype("datetime64[ns, UTC]")


def chunk_urls(chunks: Sequence[EnerginetChunk]) -> list[str]:
    return [c.record.full_url for c in chunks]


def latest_retrieval(chunks: Sequence[EnerginetChunk]) -> str | None:
    stamps = [c.record.retrieved_at_utc for c in chunks]
    return max(stamps) if stamps else None
