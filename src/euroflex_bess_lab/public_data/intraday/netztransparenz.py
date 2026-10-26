"""German TSOs' intraday price index ID AEP (Index Ausgleichsenergiepreis) from netztransparenz.de.

What the index is (netztransparenz.de, "Index Ausgleichsenergiepreis (ID AEP)"): the volume-weighted average price
of the most recent continuous exchange intraday trades for the German market area in the respective quarter-hour
product, taking trades until 500 MW have been covered. If the quarter-hour product traded less than 500 MW, the
most recent trades of the corresponding hourly product top it up to 500 MW; if both together traded less than
500 MW the index is not defined for that quarter-hour. The TSOs use it to couple the German imbalance price (reBAP)
to the intraday market.

How it is downloaded: the public page offers a "CSV Download" button. In the browser it builds a JSON request
(local delivery dates ``LocalFrom`` / ``LocalTo`` exclusive, the result time zone and the chart settings embedded
in the page), base64-encodes it and issues a plain ``GET`` to ``/DesktopModules/LotesCharts/CsvDownloadHandler.ashx``.
No account, token or cookie is needed. This module sends exactly that request, one calendar month per request with
a polite pause between requests; ``robots.txt`` (checked 2026-10-26) does not disallow the handler path. The
registered Web API (``ds.netztransparenz.de``, OAuth client credentials) is *not* used.

Terms (checked 2026-10-26, https://www.netztransparenz.de/de-de/Impressum, section "Nutzung der Internetseiten"):
content and layout are protected by copyright and any reproduction of the pages or their content requires the
prior written consent of the German TSOs by e-mail, unless reproduction is permitted by law anyway. The index is
therefore **not redistributed**: the raw CSVs and the normalized parquet stay under ``data/raw/intraday/``
(gitignored) and only a sidecar stub with ``redistributed: false`` is committed under ``data/public/intraday/``.
Analysis results derived from the data (backtest revenues, aggregate statistics) are not reproductions of it.
"""

from __future__ import annotations

import base64
import io
import json
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from ..http import FetchError, FetchRecord, HttpClient, cached_download

NETZTRANSPARENZ_BASE = "https://www.netztransparenz.de"
ID_AEP_PAGE_URL = f"{NETZTRANSPARENZ_BASE}/Regelenergie/Ausgleichsenergiepreis/Index-Ausgleichsenergiepreis"
ID_AEP_CSV_HANDLER_URL = f"{NETZTRANSPARENZ_BASE}/DesktopModules/LotesCharts/CsvDownloadHandler.ashx"
ID_AEP_TERMS_URL = f"{NETZTRANSPARENZ_BASE}/de-de/Impressum"
ID_AEP_LICENCE = "proprietary (copyright German TSOs; reproduction needs prior written consent)"
ID_AEP_ATTRIBUTION = "50Hertz, Amprion, TenneT TSO, TransnetBW via netztransparenz.de (Index Ausgleichsenergiepreis)"
ID_AEP_ZONE = "DE-LU"
LOCAL_TZ = "Europe/Berlin"
ID_AEP_COLUMN = "id_aep_eur_mwh"
ID_AEP_COLUMNS = ["timestamp_utc", "resolution_minutes", ID_AEP_COLUMN]

# Settings object the page embeds for this chart (``downloadHandlerConfig.Settings`` on the ID AEP page). The
# handler only needs the data type and route; the full object is sent to mirror the browser request exactly.
ID_AEP_SETTINGS: dict[str, Any] = {
    "DataType": 30,
    "ProduktId": 0,
    "CultureName": "de-DE",
    "Title": "Index Ausgleichsenergiepreis",
    "DiagramType": "line",
    "TimeInterval": 15,
    "DataUnit": " EUR/MWh",
    "CsvColumns": ["ID AEP"],
    "TsoIds": [],
    "NrvDirection": None,
    "WebApiRoute": "IdAep",
    "WebApiBaseUri": "https://lotes-UNB-svc-netzt.corp.transmission-it.de/StatistikApi/",
}
CSV_HEADER_PREFIX = "Datum von;"
UNDEFINED_TOKENS = frozenset({"", "-", "n.a.", "na", "nan", "n/a", "k.a.", "none", "null"})


@dataclass(frozen=True)
class MonthChunk:
    """One cached monthly CSV download (local delivery days of ``month``)."""

    month: pd.Timestamp  # first day of the month (naive local date)
    path: Path
    record: FetchRecord


def encode_request(local_from: str, local_to: str, *, result_time_zone: str = "utc") -> str:
    """Base64 request parameter of the CSV handler (``LocalTo`` is an exclusive local date)."""

    payload = {
        "LocalFrom": local_from,
        "LocalTo": local_to,
        "ResultTimeZone": result_time_zone,
        "Settings": ID_AEP_SETTINGS,
    }
    text = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def decode_request(value: str) -> dict[str, Any]:
    decoded: dict[str, Any] = json.loads(base64.b64decode(value).decode("utf-8"))
    return decoded


def request_params(month: pd.Timestamp) -> dict[str, str]:
    first = pd.Timestamp(year=month.year, month=month.month, day=1)
    following = first + pd.offsets.MonthBegin(1)
    return {"request": encode_request(f"{first:%Y-%m-%d}", f"{following:%Y-%m-%d}")}


def month_starts(start: pd.Timestamp, end: pd.Timestamp) -> list[pd.Timestamp]:
    """First days of every calendar month touched by local dates ``[start, end)``."""

    first = pd.Timestamp(year=start.year, month=start.month, day=1)
    last_day = pd.Timestamp(end) - pd.Timedelta(days=1)
    last = pd.Timestamp(year=last_day.year, month=last_day.month, day=1)
    if last < first:
        return []
    return list(pd.date_range(first, last, freq="MS"))


def validate_id_aep_csv(path: Path) -> None:
    """Reject HTML error pages and truncated downloads (the header line must be the ID AEP CSV header)."""

    head = path.read_bytes()[:400].decode("utf-8-sig", errors="replace")
    if not head.startswith(CSV_HEADER_PREFIX) or "ID AEP" not in head.splitlines()[0]:
        raise FetchError(f"Download is not an ID AEP CSV ({path.name}); first bytes: {head[:120]!r}")


def local_midnight_utc(day: pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(day).normalize().tz_localize(LOCAL_TZ).tz_convert("UTC")


def fetch_month(
    client: HttpClient,
    raw_dir: Path,
    month: pd.Timestamp,
    *,
    refresh: bool = False,
    offline: bool = False,
) -> MonthChunk:
    """Fetch (or reuse from the raw cache) the ID AEP CSV for the local delivery days of one calendar month."""

    first = pd.Timestamp(year=month.year, month=month.month, day=1)
    dest = raw_dir / "intraday" / "netztransparenz" / "id_aep" / f"id_aep_{first:%Y-%m}.csv"
    period_end = local_midnight_utc(first + pd.offsets.MonthBegin(1))
    record = cached_download(
        client,
        ID_AEP_CSV_HANDLER_URL,
        dest,
        params=request_params(first),
        validate=validate_id_aep_csv,
        refresh=refresh,
        period_end_utc=period_end,
        settle=pd.Timedelta(days=2),
        offline=offline,
    )
    return MonthChunk(first, dest, record)


def _parse_number(values: pd.Series) -> tuple[pd.Series, int]:
    """German-formatted decimals (``1.234,56`` / ``-5,19``) to float; undefined tokens become NaN."""

    text = values.astype("string").str.strip()
    undefined = text.isna() | text.str.lower().isin(UNDEFINED_TOKENS)
    cleaned = text.where(~undefined, None)
    has_comma = cleaned.str.contains(",", regex=False).fillna(False)
    cleaned = cleaned.where(~has_comma, cleaned.str.replace(".", "", regex=False).str.replace(",", ".", regex=False))
    numbers = pd.to_numeric(cleaned, errors="coerce")
    unparsed = int((numbers.isna() & ~undefined).sum())
    return numbers.astype("float64"), unparsed


def _parse_date(values: pd.Series) -> pd.Series:
    text = values.astype("string").str.strip()
    iso = text.str.match(r"^\d{4}-\d{2}-\d{2}$").fillna(False)
    parsed = pd.to_datetime(text.where(~iso), format="%d.%m.%Y", errors="coerce")
    parsed_iso = pd.to_datetime(text.where(iso), format="%Y-%m-%d", errors="coerce")
    return parsed.fillna(parsed_iso)


def parse_id_aep_csv(text: str) -> tuple[pd.DataFrame, dict[str, int]]:
    """Parse one CSV from the handler into ``timestamp_utc`` / ``id_aep_eur_mwh`` (requested in UTC).

    Returns the frame (one row per quarter-hour, undefined index values as NaN) and parse statistics.
    """

    body = text.lstrip("﻿")
    if not body.startswith(CSV_HEADER_PREFIX):
        raise ValueError("Not an ID AEP CSV: the header must start with 'Datum von;'")
    raw = pd.read_csv(io.StringIO(body), sep=";", dtype=str, keep_default_na=False)
    if raw.shape[1] < 6:
        raise ValueError(f"ID AEP CSV must have 6 columns, found {raw.shape[1]}: {list(raw.columns)}")
    raw = raw.iloc[:, :6]
    raw.columns = ["date_from", "time_from", "tz_from", "time_to", "tz_to", "value"]
    raw = raw[raw["date_from"].str.strip() != ""].reset_index(drop=True)
    zones = set(raw["tz_from"].str.strip().str.upper()) | set(raw["tz_to"].str.strip().str.upper())
    if raw.empty:
        empty = pd.DataFrame(
            {
                "timestamp_utc": pd.Series(dtype="datetime64[ns, UTC]"),
                "resolution_minutes": pd.Series(dtype="int16"),
                ID_AEP_COLUMN: pd.Series(dtype="float32"),
            }
        )
        return empty, {"rows": 0, "undefined": 0, "unparsed": 0}
    if zones - {"UTC"}:
        raise ValueError(f"ID AEP CSV must be requested in UTC; found time zones {sorted(zones)}")
    dates = _parse_date(raw["date_from"])
    if dates.isna().any():
        bad = raw.loc[dates.isna(), "date_from"].head(3).tolist()
        raise ValueError(f"Unparseable dates in ID AEP CSV: {bad}")
    start = pd.to_datetime(
        dates.dt.strftime("%Y-%m-%d") + " " + raw["time_from"].str.strip(), format="%Y-%m-%d %H:%M", errors="coerce"
    )
    if start.isna().any():
        bad = raw.loc[start.isna(), "time_from"].head(3).tolist()
        raise ValueError(f"Unparseable times in ID AEP CSV: {bad}")
    end_clock = pd.to_datetime(raw["time_to"].str.strip(), format="%H:%M", errors="coerce")
    expected_clock = (start + pd.Timedelta(minutes=15)).dt.strftime("%H:%M")
    mismatched = int((end_clock.dt.strftime("%H:%M") != expected_clock).sum())
    if mismatched:
        raise ValueError(f"{mismatched} ID AEP rows are not 15-minute intervals")
    values, unparsed = _parse_number(raw["value"])
    frame = pd.DataFrame(
        {
            "timestamp_utc": start.dt.tz_localize("UTC").astype("datetime64[ns, UTC]"),
            "resolution_minutes": np.full(len(raw), 15, dtype="int16"),
            ID_AEP_COLUMN: values.to_numpy(dtype="float64"),
        }
    )
    stats = {"rows": len(frame), "undefined": int(frame[ID_AEP_COLUMN].isna().sum()), "unparsed": unparsed}
    return frame, stats


def normalize_chunks(chunks: Sequence[MonthChunk]) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Concatenate monthly CSVs onto a complete UTC quarter-hour grid (missing quarter-hours become NaN rows)."""

    frames: list[pd.DataFrame] = []
    undefined = 0
    unparsed = 0
    rows = 0
    for chunk in chunks:
        frame, chunk_stats = parse_id_aep_csv(chunk.path.read_text(encoding="utf-8-sig"))
        frames.append(frame)
        undefined += chunk_stats["undefined"]
        unparsed += chunk_stats["unparsed"]
        rows += chunk_stats["rows"]
    usable = [f for f in frames if not f.empty]
    if not usable:
        empty = pd.DataFrame(
            {
                "timestamp_utc": pd.Series(dtype="datetime64[ns, UTC]"),
                "resolution_minutes": pd.Series(dtype="int16"),
                ID_AEP_COLUMN: pd.Series(dtype="float32"),
            }
        )
        return empty, {"source_rows": 0, "undefined_values": 0, "unparsed_values": 0, "duplicates_dropped": 0}
    frame = pd.concat(usable, ignore_index=True)
    before = len(frame)
    frame = frame.drop_duplicates("timestamp_utc", keep="last").sort_values("timestamp_utc").reset_index(drop=True)
    duplicates = before - len(frame)
    grid = pd.date_range(frame["timestamp_utc"].iloc[0], frame["timestamp_utc"].iloc[-1], freq="15min")
    indexed = frame.set_index("timestamp_utc").reindex(grid)
    missing_rows = int(indexed["resolution_minutes"].isna().sum())
    out = pd.DataFrame(
        {
            "timestamp_utc": grid.astype("datetime64[ns, UTC]"),
            "resolution_minutes": np.full(len(grid), 15, dtype="int16"),
            ID_AEP_COLUMN: indexed[ID_AEP_COLUMN].to_numpy(dtype="float64").astype("float32"),
        }
    )
    stats: dict[str, Any] = {
        "source_rows": rows,
        "undefined_values": undefined,
        "unparsed_values": unparsed,
        "duplicates_dropped": int(duplicates),
        "grid_rows": len(out),
        "missing_quarter_hours": missing_rows,
        "nan_values_total": int(out[ID_AEP_COLUMN].isna().sum()),
    }
    return out, stats


def complete_local_days(frame: pd.DataFrame) -> pd.Series:
    """Number of defined quarter-hours per local delivery day vs the expected count (92 / 96 / 100)."""

    if frame.empty:
        return pd.Series(dtype="int64")
    local_day = frame["timestamp_utc"].dt.tz_convert(LOCAL_TZ).dt.date
    present = frame.assign(local_day=local_day).groupby("local_day")[ID_AEP_COLUMN].count()
    return present
