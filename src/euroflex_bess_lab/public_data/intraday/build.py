"""Build the local (not redistributed) ID AEP table and its committed sidecar stub.

Layout (relative to the data roots, mirroring ``germany/fcr_capacity`` from regelleistung.net):

* raw CSV cache: ``data/raw/intraday/netztransparenz/id_aep/id_aep_YYYY-MM.csv`` (+ ``.source.json`` provenance)
* normalized table: ``data/raw/intraday/id_aep_DE-LU.parquet`` + full ``.meta.json`` sidecar (local only)
* committed stub: ``data/public/intraday/id_aep_DE-LU.meta.json`` with ``redistributed: false``
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from ..http import HttpClient, HttpConfig
from ..paths import data_root, raw_root
from ..writers import WrittenTable, coverage, write_stub_sidecar, write_table
from . import netztransparenz as ntp

LOGGER = logging.getLogger(__name__)

ID_AEP_TABLE = "intraday_id_aep_DE-LU"
ID_AEP_RELPATH = "intraday/id_aep_DE-LU.parquet"
ID_AEP_DEFAULT_START = "2024-01-01"
FETCH_COMMAND = f"python -m euroflex_bess_lab.public_data.intraday fetch --start {ID_AEP_DEFAULT_START}"
GENERATOR = "euroflex_bess_lab.public_data.intraday (python -m euroflex_bess_lab.public_data.intraday fetch)"
POLITE_INTERVAL_S = 3.0

TRANSFORMATION_NOTES = [
    "timestamp_utc = start of the quarter-hour delivery product (UTC); the CSV is requested with "
    "ResultTimeZone=utc so DST days carry 92 / 100 rows without ambiguity. resolution_minutes = 15.",
    "id_aep_eur_mwh = ID AEP in EUR/MWh: volume-weighted average price of the most recent continuous intraday "
    "exchange trades (German market area) of the quarter-hour product covering 500 MW, topped up with the hourly "
    "product when the quarter-hour product traded less; NaN where the index is not defined (< 500 MW in both).",
    "Monthly requests (local delivery days, LocalTo exclusive) are concatenated, de-duplicated on timestamp_utc, "
    "reindexed to a complete 15-minute UTC grid (missing quarter-hours are NaN) and cut after the last complete "
    "local delivery day. Values are float32.",
]


@dataclass(frozen=True)
class IntradayBuild:
    local: WrittenTable
    stub_path: Path
    chunks: int
    last_complete_day: str | None


def _complete_days(frame: pd.DataFrame) -> pd.Series:
    local = frame["timestamp_utc"].dt.tz_convert(ntp.LOCAL_TZ)
    rows = frame.groupby(local.dt.date).size()
    expected = {
        day: int(
            (ntp.local_midnight_utc(pd.Timestamp(day) + pd.Timedelta(days=1)) - ntp.local_midnight_utc(day))
            / pd.Timedelta(minutes=15)
        )
        for day in rows.index
    }
    return pd.Series({day: rows[day] >= expected[day] for day in rows.index}, dtype=bool)


def trim_to_complete_days(frame: pd.DataFrame, source_rows: pd.DataFrame) -> tuple[pd.DataFrame, str | None]:
    """Drop trailing local days that are not fully published yet (decided on the source rows, before reindexing)."""

    if source_rows.empty:
        return frame.iloc[0:0], None
    complete = _complete_days(source_rows)
    trailing = complete[::-1]
    last_complete = None
    for day, ok in trailing.items():
        if ok:
            last_complete = day
            break
    if last_complete is None:
        return frame.iloc[0:0], None
    end = ntp.local_midnight_utc(pd.Timestamp(last_complete) + pd.Timedelta(days=1))
    return frame.loc[frame["timestamp_utc"] < end].reset_index(drop=True), f"{last_complete}"


def build_id_aep(
    *,
    start: str = ID_AEP_DEFAULT_START,
    end: str | None = None,
    client: HttpClient | None = None,
    root: Path | None = None,
    raw_dir: Path | None = None,
    refresh: bool = False,
    offline: bool = False,
) -> IntradayBuild:
    """Download (or reuse) monthly ID AEP CSVs for local dates ``[start, end)`` and write the local table.

    ``end`` defaults to tomorrow (local), i.e. everything published so far; incomplete trailing days are dropped.
    """

    public_root = (root or data_root()).resolve()
    raw = (raw_dir or raw_root()).resolve()
    http = client or HttpClient(config=HttpConfig(min_interval_s=POLITE_INTERVAL_S))
    first = pd.Timestamp(start).normalize()
    today_local = pd.Timestamp.now(tz=ntp.LOCAL_TZ).tz_localize(None).normalize()
    last = pd.Timestamp(end).normalize() if end else today_local + pd.Timedelta(days=1)
    months = ntp.month_starts(first, last)
    chunks = []
    for month in months:
        chunk = ntp.fetch_month(http, raw, month, refresh=refresh, offline=offline)
        LOGGER.info("%s %s", chunk.path.name, "cached" if chunk.record.from_cache else "downloaded")
        chunks.append(chunk)
    frame, stats = ntp.normalize_chunks(chunks)
    lo = ntp.local_midnight_utc(first)
    hi = ntp.local_midnight_utc(last)
    source_rows = pd.concat(
        [ntp.parse_id_aep_csv(c.path.read_text(encoding="utf-8-sig"))[0] for c in chunks], ignore_index=True
    ).drop_duplicates("timestamp_utc")
    source_rows = source_rows.loc[(source_rows["timestamp_utc"] >= lo) & (source_rows["timestamp_utc"] < hi)]
    frame = frame.loc[(frame["timestamp_utc"] >= lo) & (frame["timestamp_utc"] < hi)].reset_index(drop=True)
    frame, last_complete = trim_to_complete_days(frame, source_rows)
    days = _complete_days(source_rows) if not source_rows.empty else pd.Series(dtype=bool)
    incomplete = [f"{d}" for d, ok in days.items() if not ok and (last_complete is None or f"{d}" <= last_complete)]
    cov = coverage(frame, "timestamp_utc", step=pd.Timedelta(minutes=15))
    retrieved = [c.record.retrieved_at_utc for c in chunks]
    validation: dict[str, Any] = {
        **stats,
        "rows_after_trim": len(frame),
        "nan_values_after_trim": int(frame[ntp.ID_AEP_COLUMN].isna().sum()) if not frame.empty else 0,
        "months_requested": len(months),
        "last_complete_local_day": last_complete,
        "incomplete_local_days": incomplete[:20],
        "n_incomplete_local_days": len(incomplete),
    }
    meta: dict[str, Any] = {
        "table": ID_AEP_TABLE,
        "title": "German intraday price index ID AEP (TSOs' Index Ausgleichsenergiepreis), quarter-hourly, DE-LU.",
        "source": "netztransparenz.de (German TSOs), public CSV download of the Index Ausgleichsenergiepreis page",
        "dataset_ids": ["netztransparenz_id_aep"],
        "dataset_pages": [ntp.ID_AEP_PAGE_URL],
        "source_url_template": f"{ntp.ID_AEP_CSV_HANDLER_URL}?request=<base64 of JSON {{LocalFrom: YYYY-MM-01, "
        "LocalTo: first day of next month (exclusive), ResultTimeZone: utc, Settings: page chart settings}>",
        "licence": ntp.ID_AEP_LICENCE,
        "licence_url": ntp.ID_AEP_TERMS_URL,
        "attribution": ntp.ID_AEP_ATTRIBUTION,
        "redistributed": False,
        "local_path": f"data/raw/{ID_AEP_RELPATH}",
        "retrieved_at_utc": max(retrieved) if retrieved else None,
        "coverage_start_utc": cov[0],
        "coverage_end_utc": cov[1],
        "snapshot_end_utc": cov[1],
        "time_column": "timestamp_utc",
        "zone": ntp.ID_AEP_ZONE,
        "transformation_notes": TRANSFORMATION_NOTES,
        "validation": validation,
        "inputs": [
            {
                "chunk": f"{c.month:%Y-%m}",
                "url": c.record.full_url,
                "retrieved_at_utc": c.record.retrieved_at_utc,
                "sha256": c.record.sha256,
                "size_bytes": c.record.size_bytes,
            }
            for c in chunks
        ],
        "fetch_command": FETCH_COMMAND,
        "generated_by": GENERATOR,
    }
    local = write_table(frame, raw, ID_AEP_RELPATH, meta)
    stub = {k: v for k, v in local.sidecar.items() if k not in {"schema_version", "path", "inputs"}}
    stub["note"] = (
        "Not redistributed: the netztransparenz.de imprint requires the German TSOs' prior written consent for "
        "any reproduction of the site content. rows/sha256 describe the locally built file at retrieval time; "
        "rebuild it with the fetch command (no account needed)."
    )
    stub_path = write_stub_sidecar(public_root, ID_AEP_RELPATH, stub)
    return IntradayBuild(local=local, stub_path=stub_path, chunks=len(chunks), last_complete_day=last_complete)
