"""Energy-Charts (Fraunhofer ISE) day-ahead price API; data CC BY 4.0 from Bundesnetzagentur | SMARD.de."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from .http import FetchRecord, HttpClient, cached_download, validate_json_file

ENERGY_CHARTS_PRICE_API = "https://api.energy-charts.info/price"
ENERGY_CHARTS_LICENCE = "CC BY 4.0"
ENERGY_CHARTS_LICENCE_URL = "https://creativecommons.org/licenses/by/4.0/"
ENERGY_CHARTS_ATTRIBUTION = "Bundesnetzagentur | SMARD.de, via Energy-Charts (Fraunhofer ISE)"
SUPPORTED_ZONES = ("BE", "DE-LU")


@dataclass(frozen=True)
class MonthChunk:
    zone: str
    month: pd.Timestamp  # first day of the month (naive local date)
    path: Path
    record: FetchRecord


def month_starts(start: pd.Timestamp, end: pd.Timestamp) -> list[pd.Timestamp]:
    first = pd.Timestamp(year=start.year, month=start.month, day=1)
    last = pd.Timestamp(year=end.year, month=end.month, day=1)
    return list(pd.date_range(first, last, freq="MS"))


def fetch_month(
    client: HttpClient,
    raw_dir: Path,
    zone: str,
    month: pd.Timestamp,
    *,
    refresh: bool = False,
    offline: bool = False,
) -> MonthChunk:
    """Fetch one calendar month (local delivery days, ``end`` inclusive in the Energy-Charts API)."""

    if zone not in SUPPORTED_ZONES:
        raise ValueError(f"Unsupported bidding zone {zone!r}; expected one of {SUPPORTED_ZONES}")
    last_day = month + pd.offsets.MonthEnd(0)
    params = {"bzn": zone, "start": f"{month:%Y-%m-%d}", "end": f"{last_day:%Y-%m-%d}"}
    dest = raw_dir / "energy_charts" / zone / f"price_{zone}_{month:%Y-%m}.json"
    period_end = (month + pd.offsets.MonthBegin(1)).tz_localize("Europe/Brussels").tz_convert("UTC")
    record = cached_download(
        client,
        ENERGY_CHARTS_PRICE_API,
        dest,
        params=params,
        validate=validate_json_file,
        refresh=refresh,
        period_end_utc=period_end,
        offline=offline,
    )
    return MonthChunk(zone, month, dest, record)


def parse_price_payload(payload: dict[str, object]) -> pd.DataFrame:
    """Parse one Energy-Charts ``/price`` JSON payload into ``timestamp_utc`` / ``price_eur_mwh``."""

    seconds = payload.get("unix_seconds") or []
    prices = payload.get("price") or []
    if not isinstance(seconds, list) or not isinstance(prices, list) or len(seconds) != len(prices):
        raise ValueError("Energy-Charts payload must contain equally long `unix_seconds` and `price` lists")
    frame = pd.DataFrame(
        {
            "timestamp_utc": pd.to_datetime(pd.Series(seconds, dtype="int64"), unit="s", utc=True),
            "price_eur_mwh": pd.to_numeric(pd.Series(prices, dtype="object"), errors="coerce"),
        }
    )
    return frame


def infer_resolution_minutes(timestamps: pd.Series) -> pd.Series:
    """Per-row product length in minutes from the spacing of consecutive timestamps.

    Uses the gap to the next row when it is a standard MTU (15/30/60 min), otherwise the gap to the previous
    row, otherwise the most common gap of the same UTC day.
    """

    ts = pd.Series(pd.to_datetime(timestamps, utc=True)).reset_index(drop=True)
    forward = (ts.shift(-1) - ts).dt.total_seconds() / 60.0
    backward = (ts - ts.shift(1)).dt.total_seconds() / 60.0
    valid = {15.0, 30.0, 60.0}
    resolution = forward.where(forward.isin(valid))
    resolution = resolution.fillna(backward.where(backward.isin(valid)))
    if resolution.isna().any():
        day = ts.dt.floor("D")
        mode = resolution.groupby(day).transform(lambda s: s.mode().iloc[0] if s.notna().any() else np.nan)
        resolution = resolution.fillna(mode)
    return resolution.fillna(60.0).astype("int16")


def normalize_months(chunks: Sequence[MonthChunk]) -> tuple[pd.DataFrame, dict[str, object]]:
    frames: list[pd.DataFrame] = []
    licence_info: set[str] = set()
    for chunk in chunks:
        payload = json.loads(chunk.path.read_text(encoding="utf-8"))
        if isinstance(payload, dict) and payload.get("license_info"):
            licence_info.add(str(payload["license_info"]))
        if isinstance(payload, dict):
            frames.append(parse_price_payload(payload))
    if not frames:
        empty = pd.DataFrame(
            {
                "timestamp_utc": pd.Series(dtype="datetime64[ns, UTC]"),
                "resolution_minutes": pd.Series(dtype="int16"),
                "price_eur_mwh": pd.Series(dtype="float32"),
            }
        )
        return empty, {"licence_info": sorted(licence_info), "null_prices_dropped": 0, "duplicates_dropped": 0}
    frame = pd.concat(frames, ignore_index=True)
    before = len(frame)
    frame = frame.drop_duplicates("timestamp_utc", keep="last").sort_values("timestamp_utc").reset_index(drop=True)
    duplicates = before - len(frame)
    nulls = int(frame["price_eur_mwh"].isna().sum())
    frame = frame.dropna(subset=["price_eur_mwh"]).reset_index(drop=True)
    frame["resolution_minutes"] = infer_resolution_minutes(frame["timestamp_utc"])
    frame["price_eur_mwh"] = frame["price_eur_mwh"].astype("float32")
    frame = frame[["timestamp_utc", "resolution_minutes", "price_eur_mwh"]]
    stats: dict[str, object] = {
        "licence_info": sorted(licence_info),
        "null_prices_dropped": nulls,
        "duplicates_dropped": int(duplicates),
    }
    return frame, stats
